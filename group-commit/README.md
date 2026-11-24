# group-commit

Group commit for a write-ahead log: many small transactions are appended in
one batch and made durable with a single `fdatasync`, instead of paying one
flush per transaction.

## Why

A few milliseconds per `fsync` caps per-transaction durability at a few
hundred TPS no matter how many clients are concurrent, because the disk
serializes every flush. With 100 concurrent writers each holding a ~40 byte
order-status record, flushing each record individually wastes almost all of
that latency on an empty write.

Group commit keeps the same durability contract while amortizing the flush:

1. Clients hand encoded records to a single background committer and wait on
   a one-shot acknowledgement channel.
2. The committer drains the queue up to `max_batch_records` /
   `max_batch_bytes`, or until `max_wait` elapses since the first record.
3. The batch is appended and flushed **once**.
4. Every transaction in the batch is acknowledged only after that flush
   succeeds. A failed flush returns the same error to every waiter and the
   committer stops accepting new work — nothing un-durable is ever acked.

## Durability rules

* A successful `commit` return means the record is on the persistence side
  of `File::sync_data` (fdatasync). There is no "ack early, flush later".
* Failed batches produce errors for all member transactions; the committer
  enters a fatal state because a partially written log position cannot be
  reused safely without recovery.
* Log frames carry a magic header, length and IEEE CRC-32. On restart,
  `FileLog::recover` returns every intact frame and truncates a torn tail
  (partial frame or checksum mismatch) — exactly the records that had never
  been acknowledged.
* `shutdown` drains and flushes the queue before returning the log; dropping
  the committer does the same on a best-effort basis.
* The bounded queue (`queue_capacity`) provides backpressure via
  `SubmitError::Full`; the blocking `commit` simply waits and retries.

## Benchmark

```text
cargo run --release --example tps_compare -- 100 100000 2
```

Reports measured TPS and disk-sync counts for both modes. The baseline
(per-transaction sync) is capped and extrapolated by default because it runs
at roughly `1 / sync_latency` TPS; pass an explicit `baseline_cap` to measure
the full run.
