#[allow(unused_imports)]
use crate::log;
use std::time::SystemTime;

use crate::prelude::{TxnOptions, TxnStorageTrait, DB_ID};

use super::{
    loader::{YCSBTable, YCSBTableInfo},
    prelude::get_key_bytes,
    txn_utils::{
        get_key, get_new_value, TxHelper, YCSBConfig, YCSBOutput, YCSBStat, YCSBStatus,
        YCSBTxnProfile, YCSBTxnProfileID,
    },
};

pub struct UpdateTxn {}

impl UpdateTxn {
    pub const NAME: &'static str = "Update";
    pub const ID: YCSBTxnProfileID = YCSBTxnProfileID::UpdateTxn;
}

impl YCSBTxnProfile for UpdateTxn {
    fn new(_config: &YCSBConfig) -> Self {
        Self {}
    }

    fn run<T: TxnStorageTrait>(
        &self,
        config: &YCSBConfig,
        txn_storage: &T,
        tbl_info: &YCSBTableInfo,
        stat: &mut YCSBStat,
        _out: &mut YCSBOutput,
    ) -> YCSBStatus {
        let start = SystemTime::now();

        let mut helper = TxHelper::new(txn_storage, &mut stat[UpdateTxn::ID]);
        let txn = txn_storage.begin_txn(DB_ID, TxnOptions::default()).unwrap();

        let key = get_key(config.num_keys, config.skew_factor);
        let key_bytes = get_key_bytes(key, config.key_size);
        let value = get_new_value(config.value_size);
        let res = txn_storage.update_value(&txn, tbl_info[YCSBTable::Primary], key_bytes, value);

        if res.is_err() {
            return helper.kill(&txn, &res);
        }

        let elapsed = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, elapsed)
    }
}
