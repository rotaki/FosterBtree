use std::collections::HashSet;

use crate::prelude::{ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait};
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::txn_utils::{not_successful, AbortID, TPCCStatus, TxHelper};

#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::tpcc::TxnTypeStats;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;

#[derive(Default)]
pub struct StockLevelTxnInput {
    pub w_id: u16,
    pub d_id: u8,
    pub threshold: u8,
}

impl StockLevelTxnInput {
    pub fn new(home_w_id: u16, num_warehouses: u16, use_random_warehouse: bool) -> Self {
        let w_id = if use_random_warehouse {
            urand_int(1, num_warehouses as u64) as u16
        } else {
            home_w_id
        };
        let d_id = urand_int(1, District::DISTS_PER_WARE as u8);
        let threshold = urand_int(10, 20) as u8;

        StockLevelTxnInput {
            w_id,
            d_id,
            threshold,
        }
    }

    pub fn print(&self) {
        log_info!(
            "[STOCKLEVEL]: w_id={} d_id={} threshold={}",
            self.w_id,
            self.d_id,
            self.threshold
        );
    }
}

// StockLevel Transaction Output
#[derive(Debug, Clone)]
pub struct StockLevelOutput {
    pub w_id: u16,
    pub d_id: u8,
    pub threshold: i16,
    pub low_stock_count: i32,
}

// Standalone function that takes input and returns output
pub fn run_stocklevel_txn<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    input: &StockLevelTxnInput,
    stats: &mut TxnTypeStats,
) -> Result<StockLevelOutput, TPCCStatus> {
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let start = std::time::SystemTime::now();
    let mut helper = TxHelper::new(txn_storage, stats);

    let w_id = input.w_id;
    let d_id = input.d_id;
    let threshold = input.threshold;

    // Fetch District record
    let d_key = DistrictKey::create_key(w_id, d_id);
    let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::District], d_key.into_bytes());
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::StockLevelGetDistrict as u8));
    }
    let d_bytes = res.unwrap();
    let d = unsafe { District::from_bytes(&d_bytes) };

    // Prepare low and up keys for OrderLine
    let low_key = OrderLineKey::create_key(w_id, d_id, d.d_next_o_id - 20, 1);
    let up_key = OrderLineKey::create_key(w_id, d_id, d.d_next_o_id, 1);

    // Range query over OrderLine
    let mut s_i_ids = HashSet::new();

    let scan_options = ScanOptions {
        lower_inc: low_key.into_bytes().to_vec(),
        upper_exc: up_key.into_bytes().to_vec(),
    };

    let res = txn_storage.scan_range(&txn, tbl_info[TPCCTable::OrderLine], scan_options);
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::StockLevelRangeGetOrderLine as u8));
    }
    let iter = res.unwrap();

    loop {
        let item = txn_storage.iter_next(&txn, &iter);
        match item {
            Ok(Some((_key_bytes, value_bytes))) => {
                let ol = unsafe { OrderLine::from_bytes(&value_bytes) };
                debug_assert_ne!(ol.ol_i_id, { Item::UNUSED_ID });
                s_i_ids.insert(ol.ol_i_id);
            }
            Ok(None) => break,
            Err(e) => {
                let res: Result<(), TxnStorageStatus> = Err(e);
                return Err(helper.kill(&txn, &res, AbortID::StockLevelRangeGetOrderLine as u8));
            }
        }
    }
    drop(iter);

    // Filter s_i_ids based on Stock quantity
    let mut count = 0;
    for &i_id in &s_i_ids {
        let s_key = StockKey::create_key(w_id, i_id);
        let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Stock], s_key.into_bytes());
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::StockLevelGetStock as u8));
        }
        let s_bytes = res.unwrap();
        let s = unsafe { Stock::from_bytes(&s_bytes) };
        if s.s_quantity < threshold as i16 {
            count += 1;
        }
    }

    // Commit transaction
    let commit_status = helper.commit(
        &txn,
        AbortID::StockLevelPrecommit as u8,
        start.elapsed().unwrap().as_nanos() as u64,
    );
    if commit_status != TPCCStatus::Success {
        return Err(commit_status);
    }

    // Build output
    Ok(StockLevelOutput {
        w_id,
        d_id,
        threshold: threshold as i16,
        low_stock_count: count,
    })
}
