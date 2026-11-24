//! Group commit for a write-ahead log.
//!
//! Without batching, every transaction pays for a flush (`fsync`/`fdatasync`)
//! on commit. At a few milliseconds per flush the disk serializes all commits,
//! so throughput is capped at roughly `1 / flush_latency` transactions per
//! second regardless of how many clients are concurrent.
//!
//! This crate replaces per-transaction flushing with a single background
//! committer thread:
//!
//! 1. A transaction encodes its WAL record and hands it to
//!    [`GroupCommitter::commit`] instead of flushing itself.
//! 2. The committer drains all currently queued records into one log batch,
//!    using [`GroupCommitOptions::max_batch_records`] and
//!    [`GroupCommitOptions::max_wait`] as the batching boundaries.
//! 3. The whole batch is appended and the log is flushed **once**.
//! 4. Every transaction in the batch is acknowledged only after that single
//!    flush succeeds. A flush failure is delivered to every waiter, and no
//!    transaction is ever told its write is durable before it actually is.
//!
//! The durability contract is therefore identical to per-transaction
//! `fsync`: a successful [`Commit`] means the record has reached the same
//! persistence point the underlying [`Log`] flushes to.
//!
//! # Example
//!
//! ```
//! use group_commit::{FileLog, GroupCommitOptions, GroupCommitter};
//! use std::time::Duration;
//!
//! let dir = tempdir();
//! let log = FileLog::create(dir.join("wal.log")).unwrap();
//! let committer = GroupCommitter::start(
//!   log,
//!   GroupCommitOptions {
//!     max_batch_records: 256,
//!     max_batch_bytes: 1 << 20,
//!     max_wait: Duration::from_millis(2),
//!     queue_capacity: 4096,
//!   },
//! )
//! .unwrap();
//!
//! // Many concurrent transactions share one flush per batch.
//! let commit = committer.commit(b"order 42 -> PAID".to_vec()).unwrap();
//! assert!(commit.lsn >= 1);
//!
//! // `shutdown` flushes everything still queued before returning.
//! let (_log, fatal) = committer.shutdown();
//! assert!(fatal.is_none());
//! # std::fs::remove_dir_all(dir).unwrap();
//! # fn tempdir() -> std::path::PathBuf {
//! #   let p = std::env::temp_dir().join(format!(
//! #     "group-commit-doc-{}",
//! #     std::time::SystemTime::now()
//! #       .duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
//! #   ));
//! #   std::fs::create_dir_all(&p).unwrap();
//! #   p
//! # }
//! ```
#![forbid(unsafe_code)]
#![deny(warnings, missing_docs)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(docsrs, allow(unused_attributes))]

mod error;
pub use error::{CommitError, SubmitError};

mod record;
pub use record::{
  decode_all, decode_one, decode_recoverable, encode, CrcMismatch, FrameError, Record,
};

mod log;
pub use log::{FileLog, Log, MemLog};

mod group;
pub use group::{AckReceiver, Commit, GroupCommitOptions, GroupCommitter};
