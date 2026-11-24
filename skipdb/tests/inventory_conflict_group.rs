use std::{
  error::Error,
  sync::{
    atomic::{AtomicU32, Ordering},
    Arc, Barrier,
  },
};

use skipdb::optimistic::OptimisticDb;
use txn::error::{TransactionError, WtmError};

type Db = OptimisticDb<String, i64>;

fn stock_key(sku: &str) -> String {
  format!("stock:{sku}")
}

fn group_key(sku: &str) -> String {
  format!("inventory-group:{sku}")
}

fn movement_prefix(sku: &str) -> (String, String) {
  // ':' is immediately before ';' in ASCII, giving a narrow prefix interval.
  (format!("movement:{sku}:"), format!("movement:{sku};"))
}

fn available(db: &Db, sku: &str) -> i64 {
  let txn = db.read();
  let stock = txn
    .get(&stock_key(sku))
    .map(|entry| *entry.value())
    .unwrap_or(0);
  let (start, end) = movement_prefix(sku);
  let movements: i64 = txn.range(start..end).map(|entry| *entry.value()).sum();
  stock + movements
}

fn place_order(db: &Db, sku: &str, order_id: u64) -> Result<bool, Box<dyn Error>> {
  let group = group_key(sku);
  let mut txn = db.write();
  txn.mark_read_group(&group);

  let stock = txn
    .get(&stock_key(sku))?
    .map(|entry| *entry.value())
    .unwrap_or(0);
  let (start, end) = movement_prefix(sku);
  let movements: i64 = txn.range(start..end)?.map(|entry| *entry.value()).sum();

  if stock + movements <= 0 {
    return Ok(false);
  }

  txn.insert(format!("movement:{sku}:{order_id}"), -1)?;
  txn.mark_conflict_group(&group);
  match txn.commit() {
    Ok(()) => Ok(true),
    Err(WtmError::Transaction(TransactionError::Conflict)) => Ok(false),
    Err(other) => Err(Box::new(other)),
  }
}

fn setup_database(skus: &[&str]) -> Db {
  let db = OptimisticDb::new();
  let mut txn = db.write();
  for sku in skus {
    txn.insert(stock_key(sku), 1).unwrap();
  }
  txn.commit().unwrap();
  db
}

#[test]
fn overlapping_transactions_conflict_on_inventory_group() {
  let db = setup_database(&["sku-deterministic"]);
  let group = group_key("sku-deterministic");

  let mut first = db.write();
  let mut second = db.write();
  first.mark_read_group(&group);
  second.mark_read_group(&group);

  assert_eq!(
    *first
      .get(&stock_key("sku-deterministic"))
      .unwrap()
      .unwrap()
      .value(),
    1
  );
  assert_eq!(
    *second
      .get(&stock_key("sku-deterministic"))
      .unwrap()
      .unwrap()
      .value(),
    1
  );

  first
    .insert("movement:sku-deterministic:1".to_owned(), -1)
    .unwrap();
  first.mark_conflict_group(&group);
  first.commit().unwrap();

  second
    .insert("movement:sku-deterministic:2".to_owned(), -1)
    .unwrap();
  second.mark_conflict_group(&group);
  second.commit().unwrap_err();

  assert_eq!(available(&db, "sku-deterministic"), 0);
}

#[test]
fn two_concurrent_orders_for_last_item_deal_exactly_once() {
  for _ in 0..10 {
    let db = setup_database(&["sku-last-item"]);
    let committed = Arc::new(AtomicU32::new(0));
    let barrier = Arc::new(Barrier::new(2));

    let handles = (0..2u64)
      .map(|order_id| {
        let db = db.clone();
        let committed = committed.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
          barrier.wait();
          if place_order(&db, "sku-last-item", order_id).unwrap() {
            committed.fetch_add(1, Ordering::SeqCst);
          }
        })
      })
      .collect::<Vec<_>>();

    for handle in handles {
      handle.join().unwrap();
    }

    assert_eq!(committed.load(Ordering::SeqCst), 1);
    assert_eq!(available(&db, "sku-last-item"), 0);
  }
}

#[test]
fn different_inventory_groups_are_not_table_wide_conflicts() {
  let db = setup_database(&["sku-a", "sku-b"]);
  let committed = Arc::new(AtomicU32::new(0));
  let barrier = Arc::new(Barrier::new(2));

  let handles = ["sku-a", "sku-b"]
    .into_iter()
    .enumerate()
    .map(|(index, sku)| {
      let db = db.clone();
      let committed = committed.clone();
      let barrier = barrier.clone();
      std::thread::spawn(move || {
        barrier.wait();
        if place_order(&db, sku, index as u64).unwrap() {
          committed.fetch_add(1, Ordering::SeqCst);
        }
      })
    })
    .collect::<Vec<_>>();

  for handle in handles {
    handle.join().unwrap();
  }

  assert_eq!(committed.load(Ordering::SeqCst), 2);
  assert_eq!(available(&db, "sku-a"), 0);
  assert_eq!(available(&db, "sku-b"), 0);
}
