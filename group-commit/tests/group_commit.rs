use std::sync::Arc;
use std::thread;
use std::time::Duration;

use group_commit::{
  decode_all, encode, CommitError, FileLog, GroupCommitOptions, GroupCommitter, Log, MemLog,
  SubmitError,
};

fn options() -> GroupCommitOptions {
  GroupCommitOptions {
    max_batch_records: 64,
    max_batch_bytes: 1 << 16,
    max_wait: Duration::from_millis(5),
    queue_capacity: 1024,
  }
}

fn unique_dir(tag: &str) -> std::path::PathBuf {
  let nanos = std::time::SystemTime::now()
    .duration_since(std::time::UNIX_EPOCH)
    .unwrap()
    .as_nanos();
  let dir = std::env::temp_dir().join(format!("group-commit-{tag}-{nanos}-{}", std::process::id()));
  std::fs::create_dir_all(&dir).unwrap();
  dir
}

#[test]
fn batches_many_transactions_into_one_flush() {
  let log = MemLog::new();
  let committer = GroupCommitter::start(log, options()).unwrap();

  let concurrency = 100usize;
  let per_thread = 100usize;
  let total = (concurrency * per_thread) as u64;

  let committer = Arc::new(committer);
  thread::scope(|scope| {
    for thread_id in 0..concurrency {
      let committer = committer.clone();
      scope.spawn(move || {
        for index in 0..per_thread {
          let payload = format!("order {thread_id}-{index}");
          let commit = committer.commit(payload.into_bytes()).unwrap();
          assert!(commit.lsn >= 1 && commit.lsn <= total);
          assert!(commit.group >= 1);
        }
      });
    }
  });

  assert_eq!(committer.committed(), total);
  assert_eq!(committer.failed(), 0);

  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());

  // The whole point of group commit: far fewer fsyncs than transactions.
  assert!(
    log.syncs() < total,
    "expected batching, got {} syncs for {total} txns",
    log.syncs()
  );

  // Every acknowledged transaction is actually in the log, exactly once,
  // framed and checksummed.
  let records = decode_all(log.written()).unwrap();
  assert_eq!(records.len() as u64, total);
}

#[test]
fn acknowledged_commits_survive_a_restart() {
  let dir = unique_dir("restart");
  let path = dir.join("wal.log");

  let committed: Vec<Vec<u8>> = (0..250)
    .map(|i| format!("order {i} -> PAID").into_bytes())
    .collect();

  let log = FileLog::create(&path).unwrap();
  let committer = GroupCommitter::start(log, options()).unwrap();
  for payload in &committed {
    committer.commit(payload.clone()).unwrap();
  }
  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  let before = log.sync_count();
  drop(log);

  // Simulate process restart: replay the file from scratch.
  let recovered = FileLog::recover(&path).unwrap();
  assert_eq!(recovered.len(), committed.len());
  for (record, expected) in recovered.iter().zip(&committed) {
    assert_eq!(record.payload(), expected.as_slice());
  }

  // Reopen and append through a new committer; LSNs restart but the file
  // keeps both eras of records.
  let log = FileLog::open(&path).unwrap();
  let committer = GroupCommitter::start(log, options()).unwrap();
  committer.commit(b"after restart".to_vec()).unwrap();
  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  assert!(log.sync_count() >= 1);
  let all = FileLog::recover(&path).unwrap();
  assert_eq!(all.len(), committed.len() + 1);
  assert_eq!(all.last().unwrap().payload(), b"after restart");
  assert!(before >= 1);

  std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn failed_flush_is_reported_to_every_waiter_and_none_is_acknowledged() {
  // First group succeeds, second group fails: every transaction in the
  // failed group must get an error, none a success.
  let log = MemLog::new().fail_on_sync(2);
  let committer = GroupCommitter::start(log, options()).unwrap();

  let first = committer.commit(b"ok-1".to_vec());
  assert!(first.is_ok());

  let mut waiters = Vec::new();
  for index in 0..32 {
    waiters.push(
      committer
        .try_submit(format!("maybe-{index}").into_bytes())
        .unwrap(),
    );
  }
  let mut failures = 0;
  for waiter in waiters {
    match waiter.recv().unwrap() {
      Err(CommitError::Log(_)) => failures += 1,
      other => panic!("unexpected result after injected failure: {other:?}"),
    }
  }
  assert_eq!(failures, 32);

  // A fatal log error closes the committer; nothing new is accepted.
  let err = committer.commit(b"never".to_vec()).unwrap_err();
  assert!(matches!(err, CommitError::Closed));
  assert!(matches!(
    committer.try_submit(b"never".to_vec()),
    Err(SubmitError::Closed(_))
  ));

  let (_log, fatal) = committer.shutdown();
  assert!(fatal.is_some());
  assert_eq!(failures, 32);
  assert_eq!(committer.failed(), 32);
  assert_eq!(committer.committed(), 1);
}

#[test]
fn shutdown_flushes_everything_queued() {
  let log = MemLog::new();
  let committer = Arc::new(GroupCommitter::start(log, options()).unwrap());

  let producer = committer.clone();
  let handle = thread::spawn(move || {
    for index in 0..1_000 {
      producer
        .commit(format!("order {index}").into_bytes())
        .unwrap();
    }
  });

  // Producers finish first; shutdown must still flush anything buffered in
  // the batching window instead of dropping it.
  handle.join().unwrap();
  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());

  let records = decode_all(log.written()).unwrap();
  assert_eq!(records.len(), 1_000);
}

#[test]
fn shutdown_concurrently_with_producers_rejects_new_work_safely() {
  let log = MemLog::new();
  let committer = Arc::new(GroupCommitter::start(log, options()).unwrap());

  let mut handles = Vec::new();
  for _ in 0..8 {
    let producer = committer.clone();
    handles.push(thread::spawn(move || {
      let mut accepted = 0u64;
      for _ in 0..1_000 {
        match producer.commit(b"order".to_vec()) {
          Ok(_) => accepted += 1,
          // Once shutdown starts, new work is rejected, never silently lost.
          Err(CommitError::Closed) => break,
          Err(other) => panic!("unexpected commit error: {other:?}"),
        }
      }
      accepted
    }));
  }

  // Give the producers a moment to queue work, then shut down concurrently.
  thread::sleep(Duration::from_millis(20));
  let (log, fatal) = committer.shutdown();

  let mut accepted = 0;
  for handle in handles {
    accepted += handle.join().unwrap();
  }
  assert!(fatal.is_none());

  // Every accepted transaction is present; rejected ones were never queued.
  let records = decode_all(log.written()).unwrap();
  assert_eq!(records.len() as u64, accepted);
  assert_eq!(committer.committed(), accepted);
}

#[test]
fn full_queue_applies_backpressure_without_losing_payload() {
  let small = GroupCommitOptions {
    max_batch_records: 2,
    max_batch_bytes: 1 << 16,
    max_wait: Duration::from_secs(1),
    queue_capacity: 1,
  };
  let log = MemLog::new().with_delay(Duration::from_millis(50));
  let committer = GroupCommitter::start(log, small).unwrap();

  // Hold one record in flight while the queue fills up.
  let _first = committer.try_submit(b"in-flight".to_vec()).unwrap();
  let mut returned = 0;
  for _ in 0..16 {
    if let Err(SubmitError::Full(payload)) = committer.try_submit(b"bounced".to_vec()) {
      assert_eq!(payload, b"bounced");
      returned += 1;
    }
  }
  assert!(returned > 0, "bounded queue should exert backpressure");

  // The blocking path simply waits instead of losing the record.
  let commit = committer.commit(b"waited-out".to_vec()).unwrap();
  assert!(commit.lsn >= 1);

  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  let records = decode_all(log.written()).unwrap();
  assert!(records
    .iter()
    .any(|record| record.payload() == b"waited-out"));
  assert!(!records.iter().any(|record| record.payload() == b"bounced"));
}

#[test]
fn lsn_and_group_ordering_are_monotonic() {
  let log = MemLog::new();
  let committer = GroupCommitter::start(
    log,
    GroupCommitOptions {
      max_batch_records: 8,
      max_batch_bytes: 1 << 16,
      max_wait: Duration::from_millis(2),
      queue_capacity: 256,
    },
  )
  .unwrap();

  let mut last_lsn = 0;
  let mut last_group = 0;
  for index in 0..120 {
    let commit = committer.commit(format!("o{index}").into_bytes()).unwrap();
    assert!(commit.lsn > last_lsn, "LSN must be gap-free and increasing");
    assert!(commit.group >= last_group);
    last_lsn = commit.lsn;
    last_group = commit.group;
  }
  assert_eq!(last_lsn, 120);

  let (log, _) = committer.shutdown();
  assert!(log.syncs() >= 1);
  assert!(!log.written().is_empty());
}

#[test]
fn invalid_options_are_rejected_before_a_thread_starts() {
  let base = options();
  let log = MemLog::new();
  assert!(GroupCommitter::start(
    MemLog::new(),
    GroupCommitOptions {
      max_batch_records: 0,
      ..base
    }
  )
  .is_err());
  assert!(GroupCommitter::start(
    MemLog::new(),
    GroupCommitOptions {
      max_batch_bytes: 4,
      ..base
    }
  )
  .is_err());
  assert!(GroupCommitter::start(
    MemLog::new(),
    GroupCommitOptions {
      max_wait: Duration::ZERO,
      ..base
    }
  )
  .is_err());
  assert!(GroupCommitter::start(
    MemLog::new(),
    GroupCommitOptions {
      queue_capacity: 0,
      ..base
    }
  )
  .is_err());
  drop(log);
}

#[test]
fn torn_tail_after_crash_is_ignored_on_recovery() {
  let dir = unique_dir("torn");
  let path = dir.join("wal.log");

  let log = FileLog::create(&path).unwrap();
  let committer = GroupCommitter::start(log, options()).unwrap();
  for index in 0..40 {
    committer
      .commit(format!("durable {index}").into_bytes())
      .unwrap();
  }
  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  drop(log);

  // Simulate a crash mid-append: splice a partial, never-acked frame.
  let mut raw = std::fs::read(&path).unwrap();
  raw.extend_from_slice(&encode(b"never flushed"));
  raw.truncate(raw.len() - 7);
  std::fs::write(&path, &raw).unwrap();

  let recovered = FileLog::recover(&path).unwrap();
  assert_eq!(recovered.len(), 40);

  // The torn bytes were truncated; new appends land right after good data.
  let log = FileLog::open(&path).unwrap();
  let committer = GroupCommitter::start(log, options()).unwrap();
  committer.commit(b"post-recovery".to_vec()).unwrap();
  let (_, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  let again = FileLog::recover(&path).unwrap();
  assert_eq!(again.len(), 41);
  assert_eq!(again.last().unwrap().payload(), b"post-recovery");

  std::fs::remove_dir_all(&dir).unwrap();
}
