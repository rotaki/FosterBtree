use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;

use crate::prelude::ScanOptions;
use crate::prelude::TxnOptions;
use crate::prelude::{TxnStorageStatus, TxnStorageTrait};
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::TxnTypeStats;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::{not_successful, AbortID, TPCCStatus, TxHelper};

/// The Input struct for DeliveryTxn
#[derive(Default)]
pub struct DeliveryTxnInput {
    pub w_id: u16,
    pub o_carrier_id: u8,
    pub ol_delivery_d: Timestamp,
}

impl DeliveryTxnInput {
    pub fn new(home_w_id: u16, num_warehouses: u16, use_random_warehouse: bool) -> Self {
        let w_id = if use_random_warehouse {
            urand_int(1, num_warehouses as u64) as u16
        } else {
            home_w_id
        };
        let o_carrier_id = urand_int(1, 10) as u8;
        let ol_delivery_d = get_timestamp();

        DeliveryTxnInput {
            w_id,
            o_carrier_id,
            ol_delivery_d,
        }
    }

    pub fn print(&self) {
        log_info!(
            "[DELIVERY]: w_id={} o_carrier_id={} ol_delivery_d={}",
            self.w_id,
            self.o_carrier_id,
            self.ol_delivery_d
        );
    }
}

// Delivery Transaction Output
#[derive(Debug, Clone)]
pub struct DeliveryOutput {
    pub w_id: u16,
    pub o_carrier_id: u8,
    pub delivered: Vec<DeliveredOrder>,
}

#[derive(Debug, Clone)]
pub struct DeliveredOrder {
    pub d_id: u8,
    pub o_id: u32,
}

// Standalone function that takes input and returns output
pub fn run_delivery_txn<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    input: &DeliveryTxnInput,
    stats: &mut TxnTypeStats,
) -> Result<DeliveryOutput, TPCCStatus> {
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let start = SystemTime::now();
    let mut helper = TxHelper::new(txn_storage, stats);

    let w_id = input.w_id;
    let o_carrier_id = input.o_carrier_id;
    let ol_delivery_d = input.ol_delivery_d;

    let mut districts = Vec::new();

    'per_district_loop: for d_id in 1..=District::DISTS_PER_WARE as u8 {
        // Get the oldest NewOrder for this district
        let no_low_key = NewOrderKey::create_key(w_id, d_id, 1); // Starting from order id 1
        let no_high_key = NewOrderKey::create_key(w_id, d_id, u32::MAX);

        let res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::NewOrder],
            ScanOptions {
                lower_inc: no_low_key.into_bytes().to_vec(),
                upper_exc: no_high_key.into_bytes().to_vec(),
            },
        );

        if not_successful(&res) {
            return Err(helper.kill(
                &txn,
                &res,
                AbortID::DeliveryGetNewOrderWithSmallestKey as u8,
            ));
        }
        let iter = res.unwrap();

        let (no_key, _) = {
            let first_result = txn_storage.iter_next(&txn, &iter);
            match first_result {
                Ok(Some((key_bytes, value))) => {
                    // Iterate again to ensure phantom protection. TODO fix iter_next to lock not only
                    // the current key but also the next one.
                    let _ = txn_storage.iter_next(&txn, &iter);
                    let no_key = NewOrderKey::from_bytes(&key_bytes);
                    (no_key, value)
                }
                Ok(None) => {
                    // No NewOrder found, so skip to next district
                    continue 'per_district_loop;
                }
                Err(e) => {
                    let res: Result<(), TxnStorageStatus> = Err(e);
                    return Err(helper.kill(
                        &txn,
                        &res,
                        AbortID::DeliveryGetNewOrderWithSmallestKey as u8,
                    ));
                }
            }
        };
        drop(iter);

        let res =
            txn_storage.delete_value(&txn, tbl_info[TPCCTable::NewOrder], no_key.into_bytes());
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::DeliveryDeleteNewOrder as u8));
        }

        // Fetch and update the Order record
        let o_key = OrderKey::create_key(w_id, d_id, no_key.o_id());
        let mut c_id = u32::MAX;
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Order],
            o_key.into_bytes(),
            |bytes| {
                let o = unsafe { Order::from_bytes_mut(bytes) };
                o.o_carrier_id = o_carrier_id;
                c_id = o.o_c_id;
            },
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::DeliveryPrepareUpdateOrder as u8));
        }
        debug_assert_ne!(c_id, u32::MAX);

        // Sum up the total ol_amount and update OrderLine records
        let low_key = OrderLineKey::create_key(w_id, d_id, no_key.o_id(), 1);
        let up_key = OrderLineKey::create_key(w_id, d_id, no_key.o_id() + 1, 1);
        let res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::OrderLine],
            ScanOptions {
                lower_inc: low_key.into_bytes().to_vec(),
                upper_exc: up_key.into_bytes().to_vec(),
            },
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::DeliveryRangeUpdateOrderLine as u8));
        }
        let iter = res.unwrap();
        let mut order_lines = Vec::with_capacity(15); // Assuming max 15 order lines per order
        loop {
            match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((key_bytes, value_bytes))) => {
                    order_lines.push((key_bytes, value_bytes));
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    let res: Result<(), TxnStorageStatus> = Err(e);
                    return Err(helper.kill(
                        &txn,
                        &res,
                        AbortID::DeliveryRangeUpdateOrderLine as u8,
                    ));
                }
            }
        }
        drop(iter);

        let mut total_ol_amount = 0.0;
        for (key_bytes, mut value_bytes) in order_lines {
            let ol = unsafe { OrderLine::from_bytes_mut(&mut value_bytes) };
            ol.ol_delivery_d = ol_delivery_d;
            total_ol_amount += ol.ol_amount;
            let res = txn_storage.update_value(
                &txn,
                tbl_info[TPCCTable::OrderLine],
                &key_bytes,
                value_bytes,
            );
            if not_successful(&res) {
                return Err(helper.kill(&txn, &res, AbortID::DeliveryRangeUpdateOrderLine as u8));
            }
        }

        // Update the Customer record
        let c_key = CustomerKey::create_key(w_id, d_id, c_id);
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Customer],
            c_key.into_bytes(),
            |bytes| {
                let c = unsafe { Customer::from_bytes_mut(bytes) };
                c.c_balance += total_ol_amount;
                c.c_delivery_cnt += 1;
            },
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::DeliveryPrepareUpdateCustomer as u8));
        }

        districts.push(DeliveredOrder {
            d_id,
            o_id: no_key.o_id(),
        });
    }

    // Commit transaction
    let commit_status = helper.commit(
        &txn,
        AbortID::DeliveryPrecommit as u8,
        start.elapsed().unwrap().as_nanos() as u64,
    );
    if commit_status != TPCCStatus::Success {
        return Err(commit_status);
    }

    // Build output
    Ok(DeliveryOutput {
        w_id,
        o_carrier_id,
        delivered: districts,
    })
}
