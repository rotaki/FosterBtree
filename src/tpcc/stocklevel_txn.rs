use std::collections::HashSet;
use std::time::SystemTime;

use crate::prelude::{ScanOptions, TxnOptions, TxnStorageTrait};
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::txn_utils::*;

#[allow(unused_imports)]
use crate::log;
use crate::{log_info, write_fields};

use super::loader::TPCCTableInfo;
use super::record_definitions::*;

pub struct StockLevelTxn {
    input: StockLevelTxnInput,
}

impl StockLevelTxn {
    pub const NAME: &'static str = "StockLevel";
    pub const ID: TPCCTxnProfileID = TPCCTxnProfileID::StockLevelTxn;
}

impl TPCCTxnProfile for StockLevelTxn {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input = StockLevelTxnInput::new(config, w_id);
        input.print();
        StockLevelTxn { input }
    }

    fn run<T: TxnStorageTrait>(
        &self,
        config: &TPCCConfig,
        txn_storage: &T,
        tbl_info: &TPCCTableInfo,
        stat: &mut TPCCStat,
        out: &mut TPCCOutput,
    ) -> TPCCStatus {
        self.input.print();
        let start = SystemTime::now();
        let mut helper = TxHelper::new(txn_storage, &mut stat[StockLevelTxn::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let w_id = self.input.w_id;
        let d_id = self.input.d_id;
        let threshold = self.input.threshold;

        write_fields!(out, &w_id, &d_id, &threshold);

        // Fetch District record
        let d_key = DistrictKey::create_key(w_id, d_id);
        let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::District], d_key.into_bytes());
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::GetDistrict as u8);
        }
        let d_bytes = res.unwrap();
        let d = unsafe { District::from_bytes(&d_bytes) };

        // Prepare low and up keys for OrderLine
        let low_key = OrderLineKey::create_key(w_id, d_id, d.d_next_o_id - 20, 1);
        let up_key = OrderLineKey::create_key(w_id, d_id, d.d_next_o_id, 1);

        // Range query over OrderLine
        let mut s_i_ids = HashSet::new();

        let scan_options = ScanOptions {
            lower: low_key.into_bytes().to_vec(),
            upper: up_key.into_bytes().to_vec(),
        };

        let res = txn_storage.scan_range(&txn, tbl_info[TPCCTable::OrderLine], scan_options);
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::RangeGetOrderLine as u8);
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
                    return helper.kill::<()>(&txn, &Err(e), AbortID::RangeGetOrderLine as u8);
                }
            }
        }

        // Filter s_i_ids based on Stock quantity
        let mut count = 0;
        for &i_id in &s_i_ids {
            let s_key = StockKey::create_key(w_id, i_id);
            let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Stock], s_key.into_bytes());
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::GetStock as u8);
            }
            let s_bytes = res.unwrap();
            let s = unsafe { Stock::from_bytes(&s_bytes) };
            if s.s_quantity < threshold as i16 {
                count += 1;
            }
        }

        write_fields!(out, &count);

        let duration = start.elapsed().unwrap().as_nanos() as u64;
        helper.commit(&txn, AbortID::Precommit as u8, duration)
    }
}

impl StockLevelTxn {
    pub fn print_abort_details(stat: &[usize]) {
        println!("StockLevelTxn Abort Details:");
        for (i, &count) in stat.iter().enumerate().take(AbortID::Max as usize) {
            println!("        {:<45}: {}", AbortID::from(i as u8).as_str(), count,);
        }
    }
}

#[derive(Default)]
pub struct StockLevelTxnInput {
    w_id: u16,
    d_id: u8,
    threshold: u8,
}

impl StockLevelTxnInput {
    pub fn new(_config: &TPCCConfig, w_id0: u16) -> Self {
        StockLevelTxnInput {
            w_id: w_id0,
            d_id: urand_int(1, District::DISTS_PER_WARE as u64) as u8,
            threshold: urand_int(10, 20) as u8,
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

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum AbortID {
    GetDistrict = 0,
    RangeGetOrderLine = 1,
    GetStock = 2,
    Precommit = 3,
    Max = 4,
}

impl From<u8> for AbortID {
    fn from(val: u8) -> Self {
        match val {
            0 => AbortID::GetDistrict,
            1 => AbortID::RangeGetOrderLine,
            2 => AbortID::GetStock,
            3 => AbortID::Precommit,
            _ => panic!("Invalid AbortID"),
        }
    }
}

impl AbortID {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbortID::GetDistrict => "GET_DISTRICT",
            AbortID::RangeGetOrderLine => "RANGE_GET_ORDERLINE",
            AbortID::GetStock => "GET_STOCK",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}
