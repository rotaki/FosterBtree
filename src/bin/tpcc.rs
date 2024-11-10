use std::sync::atomic::Ordering;

use clap::Parser;
use fbtree::{
    access_method::fbt::{
        READ_HINT_ACCESS, READ_HINT_ASSISTED_ACCESS, READ_NORMAL_ACCESS, WRITE_HINT_ACCESS,
        WRITE_HINT_ASSISTED_ACCESS, WRITE_NORMAL_ACCESS,
    },
    bp::{get_in_mem_pool, get_test_bp, BufferPool},
    prelude::{
        load_all_tables, Customer, CustomerSecondaryKey, NewOrderTxn, OrderStatusTxn, Output,
        PageId, PaymentTxn, ScanOptions, Stat, TPCCConfig, Table, TxnOptions, TxnProfile,
        TxnStorageTrait, DB_ID,
    },
    txn_storage::{self, NoWaitTxnStorage},
};

pub fn main() {
    let config = TPCCConfig::parse();

    let bp = get_test_bp(1000000);
    let txn_storage = NoWaitTxnStorage::new(bp);
    let tbl_info = load_all_tables(&txn_storage, &config);

    let w_id = 1;
    let mut stat = Stat::new();
    let mut out = Output::new();
    for _ in 0..100000 {
        let txn = PaymentTxn::new(&config, w_id);
        txn.run(&config, &txn_storage, &tbl_info, &mut stat, &mut out);
    }

    for _ in 0..100000 {
        let txn = NewOrderTxn::new(&config, w_id);
        txn.run(&config, &txn_storage, &tbl_info, &mut stat, &mut out);
    }

    for _ in 0..100000 {
        let txn = OrderStatusTxn::new(&config, w_id);
        txn.run(&config, &txn_storage, &tbl_info, &mut stat, &mut out);
    }

    println!(
        "Read Hint access: {}",
        READ_HINT_ACCESS.load(Ordering::Relaxed)
    );
    println!(
        "Read Normal access: {}",
        READ_NORMAL_ACCESS.load(Ordering::Relaxed)
    );
    println!(
        "Read Hint Assisted access: {}",
        READ_HINT_ASSISTED_ACCESS.load(Ordering::Relaxed)
    );
    println!(
        "Write Hint access: {}",
        WRITE_HINT_ACCESS.load(Ordering::Relaxed)
    );
    println!(
        "Write Normal access: {}",
        WRITE_NORMAL_ACCESS.load(Ordering::Relaxed)
    );
    println!(
        "Write Hint Assisted access: {}",
        WRITE_HINT_ASSISTED_ACCESS.load(Ordering::Relaxed)
    );

    // Try to print all the values in the secondary index customer record
    // let txn = txn_storage.begin_txn(DB_ID, TxnOptions::default()).unwrap();
    // let cust_iter = txn_storage
    //     .scan_range(
    //         &txn,
    //         tbl_info[Table::CustomerSecondary],
    //         ScanOptions {
    //             lower: vec![],
    //             upper: vec![],
    //             limit: 0,
    //         },
    //     )
    //     .unwrap();
    // while let Ok(Some((s_key, p_value))) = txn_storage.iter_next(&txn, &cust_iter) {
    //     let cust_sec_key = CustomerSecondaryKey::from_bytes(&s_key);
    // }

    println!("{}", stat);
}
