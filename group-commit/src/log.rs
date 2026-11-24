//! The persistence boundary underneath the group committer.
//!
//! Two implementations are provided:
//!
//! * [`FileLog`] writes framed records to a real file and persists each batch
//!   with a single `fdatasync`-equivalent ([`std::fs::File::sync_data`]).
//! * [`MemLog`] is an in-memory log with an injectable flush delay and failure
//!   point, so tests and benchmarks can exercise batching deterministically
//!   without touching a disk.

use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration;

use crate::record::{decode_recoverable, Record};

/// A sequential, append-only write-ahead log.
///
/// The trait is intentionally tiny: the group committer owns the
/// implementation exclusively (one background thread, `&mut self` access), so
/// no interior synchronization is needed.
pub trait Log: Send + 'static {
  /// Persistence error returned by [`Log::append`] or [`Log::flush`].
  type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;

  /// Appends an already framed batch. A returned error means none of the
  /// batch may be considered durable.
  fn append(&mut self, batch: &[u8]) -> Result<(), Self::Error>;

  /// Forces every previously appended batch to the persistence point that
  /// durability promises are made against (e.g. `fdatasync`). One call per
  /// committed group, never one per transaction.
  fn flush(&mut self) -> Result<(), Self::Error>;

  /// Total number of successful [`Log::flush`] calls, i.e. the number of
  /// disk syncs actually performed.
  fn syncs(&self) -> u64;
}

/// A [`Log`] backed by a file, persisted with one `sync_data` per group.
pub struct FileLog {
  path: PathBuf,
  file: File,
  syncs: u64,
}

impl FileLog {
  /// Creates a brand new log file. Fails if `path` already exists, so an
  /// existing WAL cannot be silently truncated.
  pub fn create<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
    let path = path.into();
    if let Some(parent) = path.parent() {
      if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)?;
      }
    }
    let file = OpenOptions::new()
      .create_new(true)
      .write(true)
      .read(true)
      .open(&path)?;
    Ok(Self {
      path,
      file,
      syncs: 0,
    })
  }

  /// Opens an existing log for appending, creating it if missing. The file
  /// cursor is positioned at the end.
  pub fn open<P: Into<PathBuf>>(path: P) -> io::Result<Self> {
    let path = path.into();
    if let Some(parent) = path.parent() {
      if !parent.as_os_str().is_empty() {
        std::fs::create_dir_all(parent)?;
      }
    }
    let mut file = OpenOptions::new()
      .create(true)
      .append(true)
      .read(true)
      .open(&path)?;
    file.seek(SeekFrom::End(0))?;
    Ok(Self {
      path,
      file,
      syncs: 0,
    })
  }

  /// Path of the underlying log file.
  #[inline]
  pub fn path(&self) -> &Path {
    &self.path
  }

  /// Current file size in bytes.
  #[inline]
  pub fn file_size(&self) -> io::Result<u64> {
    Ok(self.file.metadata()?.len())
  }

  /// Replays a log after a restart. Intact frames are decoded and returned in
  /// order; a torn tail (partial frame or bad checksum) is truncated away so
  /// the file is immediately reusable for appends.
  ///
  /// Everything returned by this function had previously been acknowledged as
  /// durable; everything dropped had not, and the owning transactions were
  /// never told otherwise.
  pub fn recover<P: AsRef<Path>>(path: P) -> io::Result<Vec<Record>> {
    let mut file = OpenOptions::new()
      .read(true)
      .write(true)
      .open(path.as_ref())?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;

    let (records, valid_bytes) = decode_recoverable(&buf);
    if valid_bytes != buf.len() {
      file.set_len(valid_bytes as u64)?;
      file.sync_all()?;
    }
    Ok(records)
  }

  /// Number of successful `sync_data` calls performed on this handle.
  #[inline]
  pub fn sync_count(&self) -> u64 {
    self.syncs
  }
}

impl Log for FileLog {
  type Error = io::Error;

  #[inline]
  fn append(&mut self, batch: &[u8]) -> io::Result<()> {
    self.file.write_all(batch)
  }

  #[inline]
  fn flush(&mut self) -> io::Result<()> {
    // `sync_data` is fdatasync on Unix: enough for data blocks without
    // flushing unrelated metadata, and strictly cheaper than fsync.
    self.file.sync_data()?;
    self.syncs += 1;
    Ok(())
  }

  #[inline]
  fn syncs(&self) -> u64 {
    self.syncs
  }
}

/// An in-memory [`Log`] for tests and benchmarks.
///
/// `flush` sleeps for the configured delay to emulate disk latency and can be
/// instructed to fail on an Nth sync, letting tests prove that failed batches
/// are never acknowledged.
#[derive(Debug)]
pub struct MemLog {
  buf: Vec<u8>,
  delay: Duration,
  fail_on_sync: Option<u64>,
  syncs: u64,
  failed_syncs: u64,
  appends: u64,
}

impl Default for MemLog {
  fn default() -> Self {
    Self::new()
  }
}

impl MemLog {
  /// Creates a log whose flushes return immediately and never fail.
  pub fn new() -> Self {
    Self {
      buf: Vec::new(),
      delay: Duration::ZERO,
      fail_on_sync: None,
      syncs: 0,
      failed_syncs: 0,
      appends: 0,
    }
  }

  /// Makes every flush block for `delay`, emulating disk sync latency.
  pub fn with_delay(mut self, delay: Duration) -> Self {
    self.delay = delay;
    self
  }

  /// Makes the `n`-th flush (1-based) fail; subsequent flushes succeed again.
  pub fn fail_on_sync(mut self, n: u64) -> Self {
    self.fail_on_sync = Some(n);
    self
  }

  /// All bytes written so far, including frames from every successful append.
  #[inline]
  pub fn written(&self) -> &[u8] {
    &self.buf
  }

  /// Consumes the log and returns its raw contents.
  #[inline]
  pub fn into_inner(self) -> Vec<u8> {
    self.buf
  }

  /// Number of [`Log::append`] calls (one per group).
  #[inline]
  pub fn append_count(&self) -> u64 {
    self.appends
  }

  /// Number of flushes that returned an injected failure.
  #[inline]
  pub fn failed_sync_count(&self) -> u64 {
    self.failed_syncs
  }
}

impl Log for MemLog {
  type Error = io::Error;

  fn append(&mut self, batch: &[u8]) -> io::Result<()> {
    self.buf.extend_from_slice(batch);
    self.appends += 1;
    Ok(())
  }

  fn flush(&mut self) -> io::Result<()> {
    if !self.delay.is_zero() {
      thread::sleep(self.delay);
    }
    let attempt = self.syncs + self.failed_syncs + 1;
    if self.fail_on_sync == Some(attempt) {
      self.failed_syncs += 1;
      return Err(io::Error::other("injected flush failure"));
    }
    self.syncs += 1;
    Ok(())
  }

  #[inline]
  fn syncs(&self) -> u64 {
    self.syncs
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn memlog_counts_appends_and_syncs_and_injects_failure() {
    let mut log = MemLog::new().fail_on_sync(2);
    log.append(b"a").unwrap();
    log.flush().unwrap();
    log.append(b"b").unwrap();
    // The second flush is configured to fail; appends already happened, so
    // `written` still contains b, but no extra successful sync is counted.
    assert!(log.flush().is_err());
    log.append(b"c").unwrap();
    log.flush().unwrap();
    assert_eq!(log.syncs(), 2);
    assert_eq!(log.failed_sync_count(), 1);
    assert_eq!(log.append_count(), 3);
    assert_eq!(log.written(), b"abc");
  }
}
