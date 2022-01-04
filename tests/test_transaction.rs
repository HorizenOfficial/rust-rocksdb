extern crate rocksdb;

mod util;

use rocksdb::{
    MergeOperands, Options, WriteOptions,
    transactions::{
        ops::*,
        transaction_db::{
            TransactionDB,
            TransactionDBOptions,
            TransactionOptions
        },
        util::TemporaryDBPath
    }
};
use std::convert::TryInto;

#[test]
pub fn test_transaction() {
    let n = TemporaryDBPath::new();
    {
        let db = TransactionDB::open_default(&n).unwrap();

        let trans = db.transaction_default().unwrap();

        trans.put(b"k1", b"v1").unwrap();
        trans.put(b"k2", b"v2").unwrap();
        trans.put(b"k3", b"v3").unwrap();
        trans.put(b"k4", b"v4").unwrap();

        assert_eq!(&*trans.get(b"k1").unwrap().unwrap(), b"v1");
        assert_eq!(&*trans.get(b"k2").unwrap().unwrap(), b"v2");
        assert_eq!(&*trans.get(b"k3").unwrap().unwrap(), b"v3");
        assert_eq!(&*trans.get(b"k4").unwrap().unwrap(), b"v4");

        trans.commit().unwrap();

        let trans2 = db.transaction_default().unwrap();

        let mut iter = trans2.raw_iterator();

        iter.seek_to_first(); // k1

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(b"k1".to_vec().as_slice()));
        assert_eq!(iter.value(), Some(b"v1".to_vec().as_slice()));

        iter.next(); // k2

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(b"k2".to_vec().as_slice()));
        assert_eq!(iter.value(), Some(b"v2".to_vec().as_slice()));

        iter.next(); // k3

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(b"k3".to_vec().as_slice()));
        assert_eq!(iter.value(), Some(b"v3".to_vec().as_slice()));

        iter.next(); // k4

        assert_eq!(iter.valid(), true);
        assert_eq!(iter.key(), Some(b"k4".to_vec().as_slice()));
        assert_eq!(iter.value(), Some(b"v4".to_vec().as_slice()));

        iter.next(); // invalid!

        assert_eq!(iter.valid(), false);
        assert_eq!(iter.key(), None);
        assert_eq!(iter.value(), None);

        let trans3 = db.transaction_default().unwrap();

        assert!(trans2.put(b"k2", b"v5").is_ok());
        // Attempt to change the same key in parallel transaction
        assert!(trans3.put(b"k2", b"v6").is_err());

        trans3.commit().unwrap();
        trans2.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_rollback_savepoint() {
    let path = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = TransactionDB::open(&opts, &path).unwrap();
        let write_options = WriteOptions::default();
        let transaction_options = TransactionOptions::new();

        let trans1 = db.transaction(&write_options, &transaction_options).unwrap();
        let trans2 = db.transaction(&write_options, &transaction_options).unwrap();

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();

        let trans3 = db.transaction(&write_options, &transaction_options).unwrap();

        assert_eq!(&*trans2.get(b"k1").unwrap().unwrap(), b"v1");

        trans3.delete(b"k1").unwrap();
        assert!(trans3.get(b"k1").unwrap().is_none());

        assert_eq!(&*trans2.get(b"k1").unwrap().unwrap(), b"v1");

        trans3.rollback().unwrap();
        assert_eq!(&*trans3.get(b"k1").unwrap().unwrap(), b"v1");

        let trans4 = db.transaction(&write_options, &transaction_options).unwrap();

        assert_eq!(&*trans2.get(b"k1").unwrap().unwrap(), b"v1");

        trans4.delete(b"k1").unwrap();
        trans4.set_savepoint();
        trans4.put(b"k2", b"v2").unwrap();
        trans4.rollback_to_savepoint().unwrap();
        trans4.put(b"k3", b"v3").unwrap();
        trans4.commit().unwrap();

        assert!(trans2.get(b"k1").unwrap().is_none());
        assert!(trans2.get(b"k2").unwrap().is_none());
        assert_eq!(&*trans2.get(b"k3").unwrap().unwrap(), b"v3");

        trans2.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_snapshot() {
    let path = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = TransactionDB::open(&opts, &path).unwrap();

        let write_options = WriteOptions::default();
        let transaction_options = TransactionOptions::new();
        let trans1 = db.transaction(&write_options, &transaction_options).unwrap();

        let mut transaction_options_snapshot = TransactionOptions::new();
        transaction_options_snapshot.set_snapshot(true);
        // create transaction with snapshot
        let trans2 = db.transaction(&write_options, &transaction_options_snapshot).unwrap();

        trans1.put(b"k1", b"v1").unwrap();

        let k1_2 = trans2.get(b"k1").unwrap();
        assert!(k1_2.is_none());

        trans1.commit().unwrap();
        drop(trans1);

        trans2.commit().unwrap();
        drop(trans2);

        let trans3 = db.transaction(&write_options, &transaction_options_snapshot).unwrap();

        let trans4 = db.transaction(&write_options, &transaction_options).unwrap();
        trans4.delete(b"k1").unwrap();
        trans4.commit().unwrap();
        drop(trans4);

        // Transaction returns value according to the current state of DB
        assert!(trans3.get(b"k1").unwrap().is_none());

        // Snapshot inside of transaction returns value according to a state of DB at the moment of trans3 creation
        let k1_3 = trans3.snapshot().get(b"k1").unwrap().unwrap();
        assert_eq!(&*k1_3, b"v1");

        trans3.commit().unwrap();
        drop(trans3);

        let trans5 = db.transaction(&write_options, &transaction_options_snapshot).unwrap();

        let k1_5 = trans5.snapshot().get(b"k1").unwrap();
        assert!(k1_5.is_none());

        trans5.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_get_for_update() {
    let path = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let topts = TransactionDBOptions::default();

        let db = TransactionDB::open_with_descriptor(&opts, &path, topts).unwrap();

        db.put("k1", "v1").expect("failed to put k1 v1");
        let v1 = db
            .get("k1")
            .expect("failed to get k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        let tran1 = db.transaction_default().unwrap();
        let v1 = tran1
            .get_for_update("k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        // k1 is locked for updating outside of the current transaction
        assert!(db.put("k1", "v2").is_err());
        // k1 can be updated within the current transaction
        assert!(tran1.put("k1", "v11").is_ok());

        let v11 = tran1
            .get_for_update("k1")
            .expect("failed to get for update k1")
            .expect("k1 is not exists");
        assert_eq!(&*v11, b"v11");

        tran1.put("k2", "v2").expect("failed to put k2 v2");
        tran1.commit().unwrap();

        assert_eq!(&*db.get(b"k1").unwrap().unwrap(), b"v11");
    }
}

#[test]
pub fn test_transaction_get_for_update_cf() {
    let path = TemporaryDBPath::new();
    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let topts = TransactionDBOptions::default();

        let mut db = TransactionDB::open_with_descriptor(&opts, &path, topts).unwrap();

        db.create_cf("cf1", &opts).expect("failed to create new column family cf1");
        let cf1 = db.cf_handle("cf1").expect("column family does not exist");

        db.put_cf(cf1, "k1", "v1").expect("failed to put k1 v1");
        let v1 = db
            .get_cf(cf1, "k1")
            .expect("failed to get k1")
            .expect("k1 is not exists");
        assert_eq!(&*v1, b"v1");

        let tran1 = db.transaction_default().unwrap();
        let v1 = tran1
            .get_for_update_cf(cf1, "k1")
            .expect("failed to get for update k1")
            .expect("k1 does not exist");
        assert_eq!(&*v1, b"v1");

        assert!(db.put_cf(cf1, "k1", "v2").is_err());

        let v1 = tran1
            .get_for_update_cf(cf1, "k1")
            .expect("failed to get for update k1")
            .expect("k1 does not exist");
        assert_eq!(&*v1, b"v1");

        tran1.put_cf(cf1, "k2", "v2").expect("failed to put k1 v1");
        tran1.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_merge() {
    fn concat_merge(
        _new_key: &[u8],
        existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::new();
        existing_val.map(|v| {
            for e in v {
                result.push(*e)
            }
        });
        for op in operands {
            for e in op {
                result.push(*e)
            }
        }
        Some(result)
    }

    fn test_counting_partial_merge(
        _new_key: &[u8],
        _existing_val: Option<&[u8]>,
        operands: &MergeOperands,
    ) -> Option<Vec<u8>> {
        let mut result: Vec<u8> = Vec::new();
        for op in operands {
            for e in op {
                result.push(*e);
            }
        }
        Some(result)
    }

    let path = TemporaryDBPath::new();

    {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_merge_operator("test operator", concat_merge, test_counting_partial_merge);
        let db = TransactionDB::open(&opts, &path).unwrap();
        let trans1 = db.transaction_default().unwrap();

        trans1.put(b"k1", b"a").unwrap();
        trans1.merge(b"k1", b"b").unwrap();
        trans1.merge(b"k1", b"c").unwrap();
        trans1.merge(b"k1", b"d").unwrap();
        trans1.merge(b"k1", b"efg").unwrap();

        let merged_value = b"abcdefg";

        // transaction contains all the recent updates
        assert_eq!(&*trans1.get(b"k1").unwrap().unwrap(), merged_value);
        // there is no uncommitted value k1 in DB
        assert!(db.get(b"k1").unwrap().is_none());

        trans1.commit().unwrap();

        // DB contains all the recent updates after transaction is committed
        assert_eq!(&*db.get(b"k1").unwrap().unwrap(), merged_value);

        // transaction started for the updated DB also contains all the updates from previously committed transaction
        let trans2 = db.transaction_default().unwrap();
        assert_eq!(&*trans2.get(b"k1").unwrap().unwrap(), merged_value);

        // Empty transaction can be successfully committed
        trans2.commit().unwrap();
    }
}

#[test]
pub fn test_transaction_cfs(){
    let path = TemporaryDBPath::new();

    let mut opts = Options::default();
    opts.create_if_missing(true);
    // let mut db = TransactionDB::open_with_descriptor(&opts, &path, TransactionDBOptions::default()).unwrap();

    // the 'default' cf isn't loaded with non-cf version of 'DB.open'
    // let mut db = TransactionDB::open_default(&path).unwrap();
    let mut db = TransactionDB::open_cf_default(&opts, &path).unwrap();

    db.create_cf("cf1", &opts).expect("failed to create new column family cf1");
    db.create_cf("cf2", &opts).expect("failed to create new column family cf2");
    // db.drop_cf("cf2").expect("failed to drop column family cf2");

    assert!(db.cf_handle("default").is_some());
    assert!(db.cf_handle("cf1").is_some());
    assert!(db.cf_handle("cf2").is_some());

    std::mem::drop(db);

    let db_cf = TransactionDB::open_cf_all(&opts, &path).unwrap();

    assert!(db_cf.cf_handle("default").is_some());
    assert!(db_cf.cf_handle("cf1").is_some());
    assert!(db_cf.cf_handle("cf2").is_some());
}
