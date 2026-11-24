#![cfg_attr(not(feature = "std"), no_std)]
#![deny(warnings)]
#![forbid(unsafe_code)]
#![allow(clippy::type_complexity)]

extern crate alloc;

use core::{
  borrow::Borrow,
  ops::{Bound, RangeBounds},
  sync::atomic::{AtomicU64, Ordering},
};

use alloc::collections::btree_map::{Iter as BTreeMapIter, Range as BTreeMapRange};

use smallvec_wrapper::OneOrMore;
use txn_core::types::{Entry, EntryData, EntryValue};

use crossbeam_skiplist::SkipMap;

/// The minimum number of tombstones that must be present before an automatic
/// compaction is considered. This keeps tiny databases from compacting on every
/// single deletion.
const MIN_TOMBSTONES_FOR_COMPACTION: u64 = 8;

/// Automatic compaction is triggered once tombstones reach this percentage of
/// the number of live keys. The denominator is the live key count rather than
/// the total outer-map length, so delete-heavy workloads cannot hide behind
/// their own tombstones.
const TOMBSTONE_TO_LIVE_PERCENT: u64 = 50;

pub mod iter;
use iter::*;

pub mod rev_iter;
use rev_iter::*;

pub mod range;
use range::*;

pub mod rev_range;
use rev_range::*;

pub mod types;
use types::*;

#[doc(hidden)]
pub trait Database<K, V>: AsSkipCore<K, V> {}

impl<K, V, T: AsSkipCore<K, V>> Database<K, V> for T {}

#[doc(hidden)]
pub trait AsSkipCore<K, V> {
  // This trait is sealed and cannot be implemented for types outside of this crate.
  // So returning a reference to the inner database is ok.
  fn as_inner(&self) -> &SkipCore<K, V>;
}

pub struct SkipCore<K, V> {
  map: SkipMap<K, Values<V>>,
  last_discard_version: AtomicU64,
  tombstones: AtomicU64,
}

impl<K, V> Default for SkipCore<K, V> {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl<K, V> SkipCore<K, V> {
  #[inline]
  pub fn new() -> Self {
    Self {
      map: SkipMap::new(),
      last_discard_version: AtomicU64::new(0),
      tombstones: AtomicU64::new(0),
    }
  }

  /// Returns the approximate number of keys that have a live value.
  #[inline]
  pub fn live_len(&self) -> usize {
    self
      .map
      .len()
      .saturating_sub(self.tombstones.load(Ordering::Acquire) as usize)
  }

  /// Returns the approximate number of keys whose newest version is a
  /// tombstone.
  #[inline]
  pub fn tombstone_len(&self) -> usize {
    self.tombstones.load(Ordering::Acquire) as usize
  }

  #[inline]
  #[doc(hidden)]
  #[allow(private_interfaces)]
  pub fn __by_ref(&self) -> &SkipMap<K, Values<V>> {
    &self.map
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord,
  V: Send + 'static,
{
  pub fn apply(&self, entries: OneOrMore<Entry<K, V>>) {
    for ent in entries {
      let version = ent.version();
      match ent.data {
        EntryData::Insert { key, value } => {
          let ent = self.map.get_or_insert_with(key, || Values::new());
          let val = ent.value();
          val.lock();
          let previous = val.back().map(|ent| (*ent.key(), ent.value().is_some()));
          val.insert(version, Some(value));

          if let Some((previous_version, false)) = previous {
            if version >= previous_version {
              saturating_decrement(&self.tombstones);
            }
          }

          val.unlock();
        }
        EntryData::Remove(key) => {
          if let Some(values) = self.map.get(&key) {
            let values = values.value();
            if !values.is_empty() {
              let previous = values.back().map(|ent| (*ent.key(), ent.value().is_some()));
              values.insert(version, None);

              match previous {
                None => {
                  self.tombstones.fetch_add(1, Ordering::Relaxed);
                }
                Some((previous_version, is_live)) => {
                  if is_live && version >= previous_version {
                    self.tombstones.fetch_add(1, Ordering::Relaxed);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord,
{
  pub fn get<Q>(&self, key: &Q, version: u64) -> Option<CommittedRef<'_, K, V>>
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    let ent = self.map.get(key)?;
    let version = ent
      .value()
      .upper_bound(Bound::Included(&version))
      .and_then(|v| {
        if v.value().is_some() {
          Some(*v.key())
        } else {
          None
        }
      })?;

    Some(CommittedRef { ent, version })
  }

  pub fn contains_key<Q>(&self, key: &Q, version: u64) -> bool
  where
    K: Borrow<Q>,
    Q: Ord + ?Sized,
  {
    match self.map.get(key) {
      None => false,
      Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
        None => false,
        Some(ent) => ent.value().is_some(),
      },
    }
  }

  pub fn iter(&self, version: u64) -> Iter<'_, K, V> {
    let iter = self.map.iter();
    Iter { iter, version }
  }

  pub fn iter_rev(&self, version: u64) -> RevIter<'_, K, V> {
    let iter = self.map.iter();
    RevIter {
      iter: iter.rev(),
      version,
    }
  }

  pub fn range<Q, R>(&self, range: R, version: u64) -> Range<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    Range {
      range: self.map.range(range),
      version,
    }
  }

  pub fn range_rev<Q, R>(&self, range: R, version: u64) -> RevRange<'_, Q, R, K, V>
  where
    K: Borrow<Q>,
    R: RangeBounds<Q>,
    Q: Ord + ?Sized,
  {
    RevRange {
      range: self.map.range(range).rev(),
      version,
    }
  }
}

impl<K, V> SkipCore<K, V>
where
  K: Ord + Send + 'static,
  V: Send + 'static,
  Values<V>: Send,
{
  /// Applies committed entries and compacts automatically when the tombstone
  /// count has grown too large relative to the number of live keys.
  pub fn apply_and_compact(&self, entries: OneOrMore<Entry<K, V>>, discard_hint: u64) {
    self.apply(entries);
    self.compact_if_needed(discard_hint);
  }

  /// Compacts only when tombstones have reached the configured proportion of
  /// live keys. Returns `true` when a compaction attempt was made.
  #[inline]
  pub fn compact_if_needed(&self, discard_hint: u64) -> bool {
    if !self.compaction_needed() {
      return false;
    }

    self.compact_with_discard_hint(discard_hint);
    true
  }

  #[inline]
  fn compaction_needed(&self) -> bool {
    let tombstones = self.tombstones.load(Ordering::Acquire);
    if tombstones < MIN_TOMBSTONES_FOR_COMPACTION {
      return false;
    }

    let live = (self.map.len() as u64).saturating_sub(tombstones);
    if tombstones.saturating_mul(100) < live.saturating_mul(TOMBSTONE_TO_LIVE_PERCENT) {
      return false;
    }

    true
  }

  /// Compacts using a discard hint from the transaction manager.
  #[inline]
  pub fn compact_with_discard_hint(&self, discard_hint: u64) {
    self.compact(discard_hint);
  }

  pub fn compact(&self, new_discard_version: u64) {
    loop {
      let val = self.last_discard_version.load(Ordering::Acquire);
      if val >= new_discard_version {
        return;
      }

      match self.last_discard_version.compare_exchange_weak(
        val,
        new_discard_version,
        Ordering::SeqCst,
        Ordering::Acquire,
      ) {
        Ok(_) => break,
        // Another thread changed the discard version. Retry with its value; if
        // it is already at least as new as ours, the loop exits above.
        Err(_) => continue,
      }
    }

    for ent in self.map.iter() {
      let values = ent.value();

      // if the oldest version is larger or equal to the new discard version,
      // then nothing to remove.
      if let Some(oldest) = values.front() {
        let oldest_version = *oldest.key();
        if oldest_version >= new_discard_version {
          continue;
        }
      }

      if let Some(newest) = values.back() {
        let newest_version = *newest.key();

        // if the newest version is smaller than the new discard version,
        if newest_version < new_discard_version {
          // if the newest value is none, then we can try to remove the whole key.
          if newest.value().is_none() {
            // try to lock the entry.
            if values.try_lock() {
              // we get the lock, then we can remove the whole key.
              if ent.remove() {
                saturating_decrement(&self.tombstones);
              }

              // unlock the entry.
              values.unlock();
              continue;
            }
          }

          // we leave the current newest value and try to remove previous values.
          let mut prev = newest.prev();
          while let Some(ent) = prev {
            prev = ent.prev();
            ent.remove();
          }
          continue;
        }

        // handle the complex case: we have some values that are larger than the new discard version,
        // and some values that are smaller than the new discard version.

        // Find the newest value that is smaller than the new discard version.
        // It has to stay as a fallback for readers that started before the
        // cutoff, unless it is a tombstone (in which case absence and the
        // tombstone are equivalent).
        if let Some(anchor) = values.upper_bound(Bound::Excluded(&new_discard_version)) {
          let mut prev = anchor.prev();

          if anchor.value().is_none() {
            anchor.remove();
          }

          // Remove everything older than the anchor.
          while let Some(ent) = prev {
            prev = ent.prev();
            ent.remove();
          }
        }
      } else {
        // we do not have any value in the entry, then we can try to remove the whole key.

        // try to lock the entry.
        if values.try_lock() {
          // we get the lock, then we can remove the whole key.
          ent.remove();

          // unlock the entry.
          values.unlock();
        }
      }
    }
  }
}

#[inline]
fn saturating_decrement(counter: &AtomicU64) {
  let mut current = counter.load(Ordering::Relaxed);
  loop {
    if current == 0 {
      return;
    }

    match counter.compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
    {
      Ok(_) => return,
      Err(actual) => current = actual,
    }
  }
}
