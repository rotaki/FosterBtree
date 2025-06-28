use std::time::SystemTime;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::{not_successful, AbortID, TPCCStatus, TxHelper};
#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::{ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait};
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::TxnTypeStats;

/// The Input struct for OrderStatusTxn
pub struct OrderStatusTxnInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub by_last_name: bool,
    pub c_last: [u8; Customer::MAX_LAST], // MAX_C_LAST = 16
}

impl OrderStatusTxnInput {
    pub fn new(home_w_id: u16, num_warehouses: u16, use_random_warehouse: bool) -> Self {
        let w_id = if use_random_warehouse {
            urand_int(1, num_warehouses as u64) as u16
        } else {
            home_w_id
        };
        let d_id = urand_int(1, 10) as u8;

        // 60% by last name, 40% by ID
        let by_last_name = urand_int(1, 100) <= 60;
        let (c_id, c_last) = if by_last_name {
            let last_name_num = nurand_int::<255, false>(0, 999) as usize;
            let mut c_last = [0u8; Customer::MAX_LAST];
            make_clast(&mut c_last, last_name_num);
            (Customer::UNUSED_ID, c_last)
        } else {
            let c_id = nurand_int::<1023, false>(1, Customer::CUSTS_PER_DIST as u64) as u32;
            (c_id, [0; Customer::MAX_LAST]) // c_last is not used when by_last_name is false
        };

        let input = OrderStatusTxnInput {
            w_id,
            d_id,
            c_id,
            by_last_name,
            c_last,
        };

        input
    }

    pub fn print(&self) {
        if self.by_last_name {
            log_info!(
                "[ORDERSTATUS]: w_id={} d_id={} by_last_name=t c_last={}",
                self.w_id,
                self.d_id,
                String::from_utf8_lossy(&self.c_last)
            );
        } else {
            log_info!(
                "[ORDERSTATUS]: w_id={} d_id={} by_last_name=f c_id={}",
                self.w_id,
                self.d_id,
                self.c_id
            );
        }
    }
}

// OrderStatus Transaction Output
#[derive(Debug, Clone)]
pub struct OrderStatusOutput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub c_first: [u8; 16], // MAX_FIRST = 16
    pub c_middle: [u8; 2], // MAX_MIDDLE = 2
    pub c_last: [u8; 16],  // MAX_LAST = 16
    pub c_balance: f64,
    pub o_id: u32,
    pub o_entry_d: Timestamp,
    pub o_carrier_id: Option<u8>,
    pub order_lines: Vec<OrderLineInfo>,
}

#[derive(Debug, Clone)]
pub struct OrderLineInfo {
    pub ol_i_id: u32,
    pub ol_supply_w_id: u16,
    pub ol_quantity: u8,
    pub ol_amount: f64,
    pub ol_delivery_d: Option<Timestamp>,
}

impl OrderStatusOutput {
    pub fn c_first_str(&self) -> &str {
        std::str::from_utf8(&self.c_first)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_middle_str(&self) -> &str {
        std::str::from_utf8(&self.c_middle)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_last_str(&self) -> &str {
        std::str::from_utf8(&self.c_last)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }
}

// Standalone function that takes input and returns output
pub fn run_orderstatus_txn<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    input: &OrderStatusTxnInput,
    stats: &mut TxnTypeStats,
) -> Result<OrderStatusOutput, TPCCStatus> {
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let start = SystemTime::now();
    let mut helper = TxHelper::new(txn_storage, stats);

    let c_w_id = input.w_id;
    let c_d_id = input.d_id;
    let mut c_id = input.c_id;
    let c_last = &input.c_last;
    let by_last_name = input.by_last_name;

    if by_last_name {
        debug_assert!(c_id == Customer::UNUSED_ID);
        // Fetch customers with matching last name
        let mut customer_recs = Vec::new();
        let sec_key_start = CustomerSecondaryKey::create_key(c_w_id, c_d_id, c_last, 1);
        let sec_key_start_bytes = sec_key_start.into_bytes();
        let sec_key_end = CustomerSecondaryKey::create_key(c_w_id, c_d_id, c_last, u32::MAX);
        let sec_key_end_bytes = sec_key_end.into_bytes();
        let res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::CustomerSecondary],
            ScanOptions {
                lower_inc: sec_key_start_bytes.to_vec(),
                upper_exc: sec_key_end_bytes.to_vec(),
            },
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::OrderStatusGetCustomerByLastName as u8));
        }
        let iter = res.unwrap();
        loop {
            match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((_, p_value))) => {
                    customer_recs.push(p_value);
                }
                Ok(None) => break,
                Err(e) => {
                    let res: Result<(), TxnStorageStatus> = Err(e);
                    return Err(helper.kill(
                        &txn,
                        &res,
                        AbortID::OrderStatusGetCustomerByLastName as u8,
                    ));
                }
            }
        }
        drop(iter);

        if customer_recs.is_empty() {
            return Err(helper.usr_abort(&txn));
        }

        // Sort the customer records by c_first
        customer_recs.sort_by(|a, b| {
            let a = unsafe { Customer::from_bytes(a) };
            let b = unsafe { Customer::from_bytes(b) };
            a.c_first.cmp(&b.c_first)
        });

        // Select the middle record
        let c = unsafe {
            Customer::from_bytes(customer_recs[customer_recs.len().div_ceil(2) - 1].as_slice())
        };
        c_id = c.c_id;
    }

    // Fetch customer
    let c_key = CustomerKey::create_key(c_w_id, c_d_id, c_id);
    let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Customer], c_key.into_bytes());
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::OrderStatusGetCustomer as u8));
    }
    let c_bytes = res.unwrap();
    let c = unsafe { Customer::from_bytes(&c_bytes) };

    // Get the latest order for the customer
    let os_key_start = OrderSecondaryKey::create_key(c_w_id, c_d_id, c_id, 1);
    let os_key_start_bytes = os_key_start.into_bytes();
    let os_key_end = OrderSecondaryKey::create_key(c_w_id, c_d_id, c_id, u32::MAX);
    let os_key_end_bytes = os_key_end.into_bytes();
    let res = txn_storage.scan_range(
        &txn,
        tbl_info[TPCCTable::OrderSecondary],
        ScanOptions {
            lower_inc: os_key_start_bytes.to_vec(),
            upper_exc: os_key_end_bytes.to_vec(),
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::OrderStatusGetOrderByCustomerId as u8));
    }

    let iter = res.unwrap();
    let mut o_id = 0;

    loop {
        match txn_storage.iter_next(&txn, &iter) {
            Ok(Some((k, _))) => {
                let os_key = OrderSecondaryKey::from_bytes(&k);
                o_id = os_key.o_id();
                // Keep iterating to find the latest (highest o_id)
            }
            Ok(None) => break,
            Err(e) => {
                let res: Result<(), TxnStorageStatus> = Err(e);
                return Err(helper.kill(
                    &txn,
                    &res,
                    AbortID::OrderStatusGetOrderByCustomerId as u8,
                ));
            }
        }
    }
    drop(iter);

    if o_id == 0 {
        return Err(helper.usr_abort(&txn));
    }

    // Fetch the order
    let o_key = OrderKey::create_key(c_w_id, c_d_id, o_id);
    let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Order], o_key.into_bytes());
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::OrderStatusGetOrderByCustomerId as u8));
    }
    let o_bytes = res.unwrap();
    let o = unsafe { Order::from_bytes(&o_bytes) };

    // Fetch order lines using range scan
    let mut order_lines = Vec::new();
    let ol_key_start = OrderLineKey::create_key(c_w_id, c_d_id, o_id, 1);
    let ol_key_end = OrderLineKey::create_key(c_w_id, c_d_id, o_id, u8::MAX);

    let res = txn_storage.scan_range(
        &txn,
        tbl_info[TPCCTable::OrderLine],
        ScanOptions {
            lower_inc: ol_key_start.into_bytes().to_vec(),
            upper_exc: ol_key_end.into_bytes().to_vec(),
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::OrderStatusRangeGetOrderLine as u8));
    }

    let iter = res.unwrap();
    loop {
        match txn_storage.iter_next(&txn, &iter) {
            Ok(Some((_, ol_bytes))) => {
                let ol = unsafe { OrderLine::from_bytes(&ol_bytes) };
                order_lines.push(OrderLineInfo {
                    ol_i_id: ol.ol_i_id,
                    ol_supply_w_id: ol.ol_supply_w_id,
                    ol_quantity: ol.ol_quantity,
                    ol_amount: ol.ol_amount,
                    ol_delivery_d: if ol.ol_delivery_d == 0 {
                        None
                    } else {
                        Some(ol.ol_delivery_d)
                    },
                });
            }
            Ok(None) => break,
            Err(e) => {
                let res: Result<(), TxnStorageStatus> = Err(e);
                return Err(helper.kill(&txn, &res, AbortID::OrderStatusRangeGetOrderLine as u8));
            }
        }
    }
    drop(iter);

    // Commit transaction
    let commit_status = helper.commit(
        &txn,
        AbortID::OrderStatusPrecommit as u8,
        start.elapsed().unwrap().as_nanos() as u64,
    );
    if commit_status != TPCCStatus::Success {
        return Err(commit_status);
    }

    // Build output
    Ok(OrderStatusOutput {
        w_id: c_w_id,
        d_id: c_d_id,
        c_id,
        c_first: c.c_first,
        c_middle: c.c_middle,
        c_last: c.c_last,
        c_balance: c.c_balance,
        o_id,
        o_entry_d: o.o_entry_d,
        o_carrier_id: if o.o_carrier_id == 0 {
            None
        } else {
            Some(o.o_carrier_id)
        },
        order_lines,
    })
}
