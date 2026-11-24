//! Compares per-transaction sync against group commit on the same log.
//!
//! ```text
//! cargo run --release --example tps_compare -- [concurrency] [total] [sync_ms] [baseline_cap]
//! ```
//!
//! `sync_ms` emulates disk sync latency with an in-memory log (the code path
//! is identical to `FileLog`: one flush per durability point). On a real disk
//! `FileLog::flush` costs the same few milliseconds inside `sync_data`, so
//! the batching ratio measured here carries over.

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use group_commit::{encode, GroupCommitOptions, GroupCommitter, Log, MemLog};

fn args() -> (usize, u64, u64, Option<u64>) {
  let mut args = std::env::args().skip(1);
  let concurrency = args
    .next()
    .and_then(|value| value.parse().ok())
    .unwrap_or(100);
  let total = args
    .next()
    .and_then(|value| value.parse().ok())
    .unwrap_or(100_000);
  let sync_ms = args
    .next()
    .and_then(|value| value.parse().ok())
    .unwrap_or(2);
  let baseline_cap = args.next().and_then(|value| value.parse().ok());
  (concurrency, total, sync_ms, baseline_cap)
}

fn main() {
  let (concurrency, total, sync_ms, baseline_cap) = args();
  let delay = Duration::from_millis(sync_ms);
  let payload = b"order 42 -> PAID".to_vec();

  // A full baseline run at ~500 TPS would take minutes; cap it by default and
  // extrapolate, unless the caller asks for a real number.
  let baseline_total = baseline_cap
    .unwrap_or_else(|| (1000 / sync_ms.max(1)).min(total))
    .min(total);

  println!("concurrency={concurrency}, total txns={total}, emulated sync latency={sync_ms}ms");
  println!();

  // Phase A: one sync per transaction, serialized through one log lock.
  let log = Arc::new(Mutex::new(MemLog::new().with_delay(delay)));
  let start = Instant::now();
  thread::scope(|scope| {
    for _ in 0..concurrency {
      let log = log.clone();
      let payload = payload.clone();
      scope.spawn(move || {
        for _ in 0..(baseline_total as usize / concurrency) {
          let frame = encode(&payload);
          let mut guard = log.lock().unwrap();
          guard.append(&frame).unwrap();
          guard.flush().unwrap();
        }
      });
    }
  });
  let elapsed = start.elapsed();
  let baseline_syncs = log.lock().unwrap().syncs();
  let measured_tps = baseline_syncs as f64 / elapsed.as_secs_f64();
  let extrapolated = total as f64 / measured_tps;
  println!("per-transaction sync ({baseline_syncs} measured txns)");
  println!("  measured TPS: {measured_tps:.1}");
  println!("  syncs/txn: 1.00");
  println!("  estimated time for {total} txns: {extrapolated:.2}s");
  println!();

  // Phase B: group commit with the same emulated flush latency.
  let log = MemLog::new().with_delay(delay);
  let committer = Arc::new(
    GroupCommitter::start(
      log,
      GroupCommitOptions {
        max_batch_records: 4096,
        max_batch_bytes: 4 << 20,
        max_wait: Duration::from_millis(2),
        queue_capacity: 16_384,
      },
    )
    .unwrap(),
  );

  let start = Instant::now();
  thread::scope(|scope| {
    for _ in 0..concurrency {
      let committer = committer.clone();
      let payload = payload.clone();
      scope.spawn(move || {
        for _ in 0..(total as usize / concurrency) {
          committer.commit(payload.clone()).unwrap();
        }
      });
    }
  });
  let elapsed = start.elapsed();

  let (log, fatal) = committer.shutdown();
  assert!(fatal.is_none());
  let group_syncs = log.syncs();
  let group_tps = total as f64 / elapsed.as_secs_f64();
  let avg_group_size = total as f64 / group_syncs as f64;

  println!("group commit ({total} txns)");
  println!("  TPS: {group_tps:.1}");
  println!(
    "  disk syncs: {group_syncs} ({:.3} syncs/txn, {avg_group_size:.1} txns/sync)",
    group_syncs as f64 / total as f64
  );
  println!("  measured time: {:.2}s", elapsed.as_secs_f64());
  println!(
    "  total flush time: {:.2}s",
    committer.flush_time().as_secs_f64()
  );
  println!();
  println!(
    "speedup vs per-transaction sync: {:.1}x",
    group_tps / measured_tps
  );
}
