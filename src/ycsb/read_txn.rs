use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_error;
use crate::prelude::get_key_bytes;

use crate::{
    prelude::{ScanOptions, TxnOptions, TxnStorageTrait, DB_ID},
    write_fields,
};

use super::{
    loader::{YCSBTable, YCSBTableInfo},
    txn_utils::{
        get_key, TxHelper, YCSBConfig, YCSBOutput, YCSBStat, YCSBStatus, YCSBTxnProfile,
        YCSBTxnProfileID,
    },
};

pub struct ReadTxn {}

impl ReadTxn {
    pub const NAME: &'static str = "Read";
    pub const ID: YCSBTxnProfileID = YCSBTxnProfileID::ReadTxn;
}

impl YCSBTxnProfile for ReadTxn {
    fn new(config: &YCSBConfig) -> Self {
        Self {}
    }

    fn run<T: TxnStorageTrait>(
        &self,
        config: &YCSBConfig,
        txn_storage: &T,
        tbl_info: &YCSBTableInfo,
        stat: &mut YCSBStat,
        out: &mut YCSBOutput,
    ) -> YCSBStatus {
        let start = SystemTime::now();

        let mut helper = TxHelper::new(txn_storage, &mut stat[ReadTxn::ID]);
        let txn = txn_storage.begin_txn(DB_ID, TxnOptions::default()).unwrap();

        let lower = get_key(config.num_keys, config.skew_factor);
        let upper = lower + 1;
        let res = txn_storage.scan_range(
            &txn,
            tbl_info[YCSBTable::Secondary],
            ScanOptions {
                lower: get_key_bytes(lower, config.key_size),
                upper: get_key_bytes(upper, config.key_size),
            },
        );
        if res.is_err() {
            return helper.kill(&txn, &res);
        }
        log_error!("ReadTxn scan_range ok");
        let iter = res.unwrap();

        loop {
            match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((key, value))) => {
                    let len = key.len() + value.len();
                    write_fields!(out, &len);
                    break;
                }
                Ok(None) => {
                    panic!("Key should exist if we are able to scan it unless there is a deletion");
                }
                Err(e) => {
                    return helper.kill::<()>(&txn, &Err(e));
                }
            }
        }
        log_error!("ReadTxn iter_next ok");

        drop(iter);

        log_error!("ReadTxn drop iter ok");

        let elapsed = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, elapsed)
    }
}
