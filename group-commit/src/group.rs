//! The group committer: one background thread, one flush per batch.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crossbeam_channel::{bounded, Receiver, RecvTimeoutError, Sender, TrySendError};
use parking_lot::Mutex as ParkingMutex;

use crate::error::{CommitError, SubmitError};
use crate::log::Log;
use crate::record::{encode, HEADER_LEN};

/// Tuning knobs for [`GroupCommitter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitOptions {
  /// Maximum number of transactions merged into one flush. A full group is
  /// flushed immediately without waiting for [`Self::max_wait`].
  pub max_batch_records: usize,
  /// Soft upper bound on the encoded batch size in bytes. A record that does
  /// not fit the current group starts its own group, so a single oversized
  /// transaction is never rejected.
  pub max_batch_bytes: usize,
  /// Maximum time the first transaction in a group waits for company before
  /// the group is flushed anyway. Bounds commit latency under low load.
  pub max_wait: Duration,
  /// Capacity of the bounded queue between clients and the committer. A full
  /// queue applies backpressure via [`SubmitError::Full`] instead of growing
  /// memory without limit.
  pub queue_capacity: usize,
}

impl GroupCommitOptions {
  fn validate(&self) -> io::Result<()> {
    if self.max_batch_records == 0 {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "max_batch_records must be > 0",
      ));
    }
    if self.max_batch_bytes < HEADER_LEN {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "max_batch_bytes must fit at least one record header",
      ));
    }
    if self.max_wait.is_zero() {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "max_wait must be > 0",
      ));
    }
    if self.queue_capacity == 0 {
      return Err(io::Error::new(
        io::ErrorKind::InvalidInput,
        "queue_capacity must be > 0",
      ));
    }
    Ok(())
  }
}

/// A successful durability acknowledgement.
///
/// Receiving this value is the commit point: the record has been included in
/// a batch whose single flush has already returned success.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Commit {
  /// Monotonic, gap-free log sequence number assigned in flush order,
  /// starting at 1.
  pub lsn: u64,
  /// 1-based group number; transactions sharing a group share one flush.
  pub group: u64,
  /// Number of payload bytes the transaction contributed.
  pub payload_len: usize,
}

/// One-shot channel on which a queued transaction receives its commit result.
pub type AckReceiver<L> = Receiver<Result<Commit, CommitError<<L as Log>::Error>>>;

struct Pending<E: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static> {
  payload: Vec<u8>,
  ack: Sender<Result<Commit, CommitError<E>>>,
}

type FatalHandle<E> = Mutex<Option<JoinHandle<Option<Arc<E>>>>>;

struct Shared<E> {
  stopped: AtomicBool,
  fatal: ParkingMutex<Option<Arc<E>>>,
}

impl<E> Shared<E> {
  fn mark_fatal(&self, err: Arc<E>) {
    *self.fatal.lock() = Some(err);
  }

  fn fatal_snapshot(&self) -> Option<Arc<E>> {
    self.fatal.lock().clone()
  }
}

#[derive(Debug, Default)]
struct Stats {
  submitted: AtomicU64,
  committed: AtomicU64,
  failed: AtomicU64,
  groups: AtomicU64,
  next_lsn: AtomicU64,
  flush_nanos: AtomicU64,
}

/// Merges transactions from many threads into batched, single-flush groups.
///
/// Cloning is intentionally not provided; wrap it in an [`Arc`] to share it
/// across worker threads.
pub struct GroupCommitter<L: Log> {
  tx: Sender<Pending<L::Error>>,
  shared: Arc<Shared<L::Error>>,
  handle: FatalHandle<L::Error>,
  // Held by the background thread for its whole life; taken back on shutdown.
  log_slot: Arc<Mutex<Option<L>>>,
  opts: GroupCommitOptions,
  stats: Arc<Stats>,
}

impl<L: Log> std::fmt::Debug for GroupCommitter<L> {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    f.debug_struct("GroupCommitter")
      .field("options", &self.opts)
      .field("submitted", &self.stats.submitted.load(Ordering::Relaxed))
      .field("committed", &self.stats.committed.load(Ordering::Relaxed))
      .field("groups", &self.stats.groups.load(Ordering::Relaxed))
      .finish()
  }
}

impl<L: Log> GroupCommitter<L> {
  /// Spawns the background committer thread and starts accepting records.
  pub fn start(log: L, options: GroupCommitOptions) -> io::Result<Self> {
    options.validate()?;

    let (tx, rx) = bounded(options.queue_capacity);
    let shared = Arc::new(Shared {
      stopped: AtomicBool::new(false),
      fatal: ParkingMutex::new(None),
    });
    let stats = Arc::new(Stats::default());
    let log_slot = Arc::new(Mutex::new(Some(log)));

    let thread_shared = shared.clone();
    let thread_stats = stats.clone();
    let thread_log_slot = log_slot.clone();
    let handle = thread::Builder::new()
      .name("group-commit".into())
      .spawn(move || {
        // Take ownership of the log for the thread's whole life so no lock
        // is held while joining; the log is handed back through the slot
        // when the loop exits.
        let mut log = thread_log_slot
          .lock()
          .expect("log slot poisoned")
          .take()
          .expect("log slot empty");
        let fatal = run_committer(&mut log, &rx, &thread_shared, &options, &thread_stats);
        *thread_log_slot.lock().expect("log slot poisoned") = Some(log);
        fatal
      })?;

    Ok(Self {
      tx,
      shared,
      handle: Mutex::new(Some(handle)),
      log_slot,
      opts: options,
      stats,
    })
  }

  /// Queues a record without blocking. On [`SubmitError::Full`] or
  /// [`SubmitError::Closed`] the payload is returned untouched, so the caller
  /// can retry, fall back to a synchronous write, or surface an error.
  pub fn try_submit(&self, payload: Vec<u8>) -> Result<AckReceiver<L>, SubmitError> {
    if self.shared.stopped.load(Ordering::Acquire) || self.shared.fatal_snapshot().is_some() {
      return Err(SubmitError::Closed(payload));
    }

    let (ack_tx, ack_rx) = bounded(1);
    let pending = Pending {
      payload,
      ack: ack_tx,
    };
    match self.tx.try_send(pending) {
      Ok(()) => {
        self.stats.submitted.fetch_add(1, Ordering::Relaxed);
        Ok(ack_rx)
      }
      Err(TrySendError::Full(pending)) => Err(SubmitError::Full(pending.payload)),
      Err(TrySendError::Disconnected(pending)) => Err(SubmitError::Closed(pending.payload)),
    }
  }

  /// Blocks until the record is durable or the committer fails. Applies
  /// backpressure by yielding while the queue is full, so it can only fail
  /// with a durability-level [`CommitError`].
  pub fn commit(&self, payload: Vec<u8>) -> Result<Commit, CommitError<L::Error>> {
    let mut payload = payload;
    let ack_rx = loop {
      match self.try_submit(payload) {
        Ok(ack_rx) => break ack_rx,
        Err(SubmitError::Full(returned)) => {
          payload = returned;
          thread::sleep(Duration::from_micros(200));
        }
        Err(SubmitError::Closed(_)) => return Err(CommitError::Closed),
      }
    };

    // The sender lives inside the committer and always replies, unless the
    // thread panicked; treat that as a closed committer.
    ack_rx.recv().unwrap_or(Err(CommitError::Closed))
  }

  /// Stops accepting work, flushes every queued record, joins the background
  /// thread and returns the log together with the fatal error, if any.
  ///
  /// Every record still in the queue gets either a durability
  /// acknowledgement or a [`CommitError`] before this returns.
  pub fn shutdown(&self) -> (L, Option<CommitError<L::Error>>) {
    self.shared.stopped.store(true, Ordering::Release);
    drop(self.tx.clone());

    let fatal = self
      .handle
      .lock()
      .expect("handle lock poisoned")
      .take()
      .and_then(|handle| handle.join().ok())
      .flatten()
      .map(CommitError::Log);

    let log = self
      .log_slot
      .lock()
      .expect("log slot poisoned")
      .take()
      .expect("log returned exactly once");
    (log, fatal)
  }

  /// Total records accepted into the queue.
  pub fn submitted(&self) -> u64 {
    self.stats.submitted.load(Ordering::Relaxed)
  }

  /// Total records acknowledged as durable.
  pub fn committed(&self) -> u64 {
    self.stats.committed.load(Ordering::Relaxed)
  }

  /// Total records whose batch failed before durability.
  pub fn failed(&self) -> u64 {
    self.stats.failed.load(Ordering::Relaxed)
  }

  /// Total batches flushed, i.e. total log sync calls (disk fsync count).
  pub fn groups(&self) -> u64 {
    self.stats.groups.load(Ordering::Relaxed)
  }

  /// Total time spent inside [`Log::flush`], across all groups.
  pub fn flush_time(&self) -> Duration {
    Duration::from_nanos(self.stats.flush_nanos.load(Ordering::Relaxed))
  }

  /// The options this committer was started with.
  pub fn options(&self) -> GroupCommitOptions {
    self.opts
  }
}

impl<L: Log> Drop for GroupCommitter<L> {
  fn drop(&mut self) {
    self.shared.stopped.store(true, Ordering::Release);
    let handle = self.handle.get_mut().expect("handle lock poisoned").take();
    if let Some(handle) = handle {
      // Disconnecting the queue lets the thread drain and flush whatever is
      // still pending before exiting; errors simply cannot surface here.
      let _ = handle.join();
    }
  }
}

fn run_committer<L: Log>(
  log: &mut L,
  rx: &Receiver<Pending<L::Error>>,
  shared: &Shared<L::Error>,
  options: &GroupCommitOptions,
  stats: &Stats,
) -> Option<Arc<L::Error>> {
  loop {
    // Block until at least one transaction has work, or the queue closes.
    let first = match rx.recv_timeout(options.max_wait) {
      Ok(pending) => pending,
      Err(RecvTimeoutError::Timeout) => {
        // Idle and a shutdown may be pending while other `Sender` clones
        // (e.g. an `Arc<GroupCommitter>`) still exist; re-check the flag.
        if shared.stopped.load(Ordering::Acquire) {
          return None;
        }
        continue;
      }
      Err(RecvTimeoutError::Disconnected) => return None,
    };

    let mut batch = Vec::new();
    let mut batch_bytes = frame_size(&first);
    batch.push(first);
    let mut deadline = Instant::now() + options.max_wait;

    // Coalesce everything that arrives within the batching window, subject
    // to the record/byte limits.
    loop {
      if batch.len() >= options.max_batch_records {
        break;
      }
      let now = Instant::now();
      if now >= deadline {
        break;
      }

      match rx.recv_timeout(deadline - now) {
        Ok(pending) => {
          let frame = frame_size(&pending);
          if batch_bytes + frame > options.max_batch_bytes {
            // Current group is full: make it durable, then the oversized or
            // overflowing record opens the next group.
            if let Some(fatal) = flush_group(log, std::mem::take(&mut batch), stats) {
              shared.mark_fatal(fatal.clone());
              return Some(fatal);
            }
            batch_bytes = 0;
            deadline = Instant::now() + options.max_wait;
          }
          batch_bytes += frame;
          batch.push(pending);
        }
        Err(RecvTimeoutError::Timeout) => break,
        Err(RecvTimeoutError::Disconnected) => break,
      }
    }

    if let Some(fatal) = flush_group(log, batch, stats) {
      shared.mark_fatal(fatal.clone());
      return Some(fatal);
    }
  }
}

fn flush_group<L: Log>(
  log: &mut L,
  batch: Vec<Pending<L::Error>>,
  stats: &Stats,
) -> Option<Arc<L::Error>> {
  let group = stats.groups.fetch_add(1, Ordering::Relaxed) + 1;
  let base_lsn = stats
    .next_lsn
    .fetch_add(batch.len() as u64, Ordering::Relaxed);
  let batch_len = batch.len();

  let started = Instant::now();
  let result = (|| -> Result<(), L::Error> {
    let mut encoded = Vec::new();
    for pending in &batch {
      encoded.extend_from_slice(&encode(&pending.payload));
    }
    log.append(&encoded)?;
    log.flush()
  })();
  stats
    .flush_nanos
    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);

  match result {
    Ok(()) => {
      stats
        .committed
        .fetch_add(batch_len as u64, Ordering::Relaxed);
      for (index, pending) in batch.into_iter().enumerate() {
        let commit = Commit {
          lsn: base_lsn + index as u64 + 1,
          group,
          payload_len: pending.payload.len(),
        };
        // A dropped receiver means the client gave up waiting; the record is
        // still durable, so an error reply would be wrong — just ignore it.
        let _ = pending.ack.send(Ok(commit));
      }
      None
    }
    Err(err) => {
      stats.failed.fetch_add(batch_len as u64, Ordering::Relaxed);
      let shared_err = Arc::new(err);
      for pending in batch {
        let _ = pending.ack.send(Err(CommitError::Log(shared_err.clone())));
      }
      Some(shared_err)
    }
  }
}

#[inline]
fn frame_size<E: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static>(
  pending: &Pending<E>,
) -> usize {
  HEADER_LEN + pending.payload.len()
}
