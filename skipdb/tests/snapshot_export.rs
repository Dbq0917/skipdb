use std::{
  sync::{Arc, Barrier},
  thread,
  time::Duration,
};

use skipdb::{optimistic::OptimisticDb, serializable::SerializableDb};

const KEYS: u64 = 256;

fn assert_send_sync<T: Send + Sync>(_: &T) {}

fn seed_optimistic(db: &OptimisticDb<u64, u64>) {
  let mut txn = db.write();
  for key in 0..KEYS {
    txn.insert(key, 0).unwrap();
  }
  txn.commit().unwrap();
}

fn seed_serializable(db: &SerializableDb<u64, u64>) {
  let mut txn = db.serializable_write();
  for key in 0..KEYS {
    txn.insert(key, 0).unwrap();
  }
  txn.commit().unwrap();
}

#[test]
fn optimistic_snapshot_export_is_stable_while_writes_continue() {
  let db = OptimisticDb::new();
  seed_optimistic(&db);

  let snapshot = db.snapshot();
  assert_send_sync(&snapshot);
  let snapshot_version = snapshot.version();
  let barrier = Arc::new(Barrier::new(2));
  let writer_barrier = barrier.clone();
  let writer_db = db.clone();
  let (committed_tx, committed_rx) = crossbeam_channel::bounded(1);

  let writer = thread::spawn(move || {
    writer_barrier.wait();
    for round in 1..=4 {
      let mut txn = writer_db.write();
      for key in 0..KEYS {
        txn.insert(key, round).unwrap();
      }
      txn.commit().unwrap();
      if round == 1 {
        committed_tx.send(()).unwrap();
      }
    }
  });

  barrier.wait();

  let exported = snapshot
    .iter()
    .enumerate()
    .map(|(idx, entry)| {
      if idx == 32 {
        committed_rx
          .recv_timeout(Duration::from_secs(5))
          .expect("a write committed while the snapshot was being exported");
        thread::sleep(Duration::from_millis(20));
      }
      (*entry.key(), *entry.value())
    })
    .collect::<Vec<_>>();

  writer.join().unwrap();

  assert_eq!(snapshot.version(), snapshot_version);
  assert_eq!(exported.len(), KEYS as usize);
  assert!(exported
    .iter()
    .enumerate()
    .all(|(idx, (key, value))| *key == idx as u64 && *value == 0));

  let latest = db.read();
  assert!((0..KEYS).all(|key| *latest.get(&key).unwrap().value() == 4));
}

#[test]
fn serializable_snapshot_export_is_stable_while_writes_continue() {
  let db = SerializableDb::new();
  seed_serializable(&db);

  let snapshot = db.snapshot();
  assert_send_sync(&snapshot);
  let snapshot_version = snapshot.version();
  let barrier = Arc::new(Barrier::new(2));
  let writer_barrier = barrier.clone();
  let writer_db = db.clone();
  let (committed_tx, committed_rx) = crossbeam_channel::bounded(1);

  let writer = thread::spawn(move || {
    writer_barrier.wait();
    for round in 1..=4 {
      let mut txn = writer_db.serializable_write();
      for key in 0..KEYS {
        txn.insert(key, round).unwrap();
      }
      txn.commit().unwrap();
      if round == 1 {
        committed_tx.send(()).unwrap();
      }
    }
  });

  barrier.wait();

  let exported = snapshot
    .iter()
    .enumerate()
    .map(|(idx, entry)| {
      if idx == 32 {
        committed_rx
          .recv_timeout(Duration::from_secs(5))
          .expect("a write committed while the snapshot was being exported");
        thread::sleep(Duration::from_millis(20));
      }
      (*entry.key(), *entry.value())
    })
    .collect::<Vec<_>>();

  writer.join().unwrap();

  assert_eq!(snapshot.version(), snapshot_version);
  assert_eq!(exported.len(), KEYS as usize);
  assert!(exported
    .iter()
    .enumerate()
    .all(|(idx, (key, value))| *key == idx as u64 && *value == 0));

  let latest = db.read();
  assert!((0..KEYS).all(|key| *latest.get(&key).unwrap().value() == 4));
}

#[test]
fn snapshot_is_not_affected_by_compaction() {
  let db = OptimisticDb::new();
  seed_optimistic(&db);

  let snapshot = db.snapshot();

  let mut txn = db.write();
  for key in 0..KEYS {
    txn.insert(key, 1).unwrap();
  }
  txn.commit().unwrap();
  db.compact();

  assert!((0..KEYS).all(|key| *snapshot.get(&key).unwrap().value() == 0));
  assert!((0..KEYS).all(|key| *db.read().get(&key).unwrap().value() == 1));
}

#[test]
fn snapshot_ignores_later_insert_update_and_remove() {
  let db = OptimisticDb::new();
  seed_optimistic(&db);

  let snapshot = db.snapshot();

  let mut txn = db.write();
  txn.remove(0).unwrap();
  txn.insert(1, 9).unwrap();
  txn.insert(KEYS, 7).unwrap();
  txn.commit().unwrap();

  let exported = snapshot
    .iter()
    .map(|entry| (*entry.key(), *entry.value()))
    .collect::<Vec<_>>();

  assert_eq!(exported.len(), KEYS as usize);
  assert_eq!(exported[0], (0, 0));
  assert_eq!(exported[1], (1, 0));
  assert!(exported.iter().all(|(_, value)| *value == 0));

  let latest = db.read();
  assert!(latest.get(&0).is_none());
  assert_eq!(*latest.get(&1).unwrap().value(), 9);
  assert_eq!(*latest.get(&KEYS).unwrap().value(), 7);
}
