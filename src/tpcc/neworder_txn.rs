use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::TxnTypeStats;
use memchr::memmem;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::{not_successful, AbortID, TPCCStatus, TxHelper};

/// The Input struct for NewOrderTxn
#[derive(Default)]
pub struct NewOrderTxnInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub ol_cnt: u8,
    pub o_entry_d: Timestamp,
    pub items: Vec<NewOrderItem>,
}

#[derive(Default)]
pub struct NewOrderItem {
    pub ol_supply_w_id: u16,
    pub ol_i_id: u32,
    pub ol_quantity: u8,
}

impl NewOrderTxnInput {
    pub fn new(home_w_id: u16, num_warehouses: u16, use_random_warehouse: bool) -> Self {
        let w_id = if use_random_warehouse {
            urand_int(1, num_warehouses as u64) as u16
        } else {
            home_w_id
        };
        let d_id = urand_int(1, District::DISTS_PER_WARE as u8);
        let c_id = nurand_int::<1023, false>(1, Customer::CUSTS_PER_DIST as u64) as u32;

        let mut items = Vec::with_capacity(OrderLine::MAX_ORDLINES_PER_ORD);
        let ol_cnt = urand_int(
            OrderLine::MIN_ORDLINES_PER_ORD,
            OrderLine::MAX_ORDLINES_PER_ORD,
        ) as u8;

        // 1% rollback according to TPC-C spec
        let rollback = urand_int(1, 100) == 1;

        for i in 0..ol_cnt {
            let i_id = if rollback && i == ol_cnt - 1 {
                // Last item in a rollback transaction should be invalid
                Item::UNUSED_ID // set to an unused value
            } else {
                nurand_int::<8191, false>(1, Item::ITEMS as u64) as u32
            };
            let quantity = urand_int(1, 10) as u8;

            // 1% chance of remote warehouse
            let supply_w_id = if urand_int(1, 100) == 1 && num_warehouses > 1 {
                let mut w = urand_int(1, num_warehouses as u64) as u16;
                if w == w_id && num_warehouses > 1 {
                    w = if w == num_warehouses { 1 } else { w + 1 };
                }
                w
            } else {
                w_id
            };

            items.push(NewOrderItem {
                ol_i_id: i_id,
                ol_supply_w_id: supply_w_id,
                ol_quantity: quantity,
            });
        }

        NewOrderTxnInput {
            w_id,
            d_id,
            c_id,
            ol_cnt,
            o_entry_d: get_timestamp(),
            items,
        }
    }

    pub fn print(&self) {
        log_info!(
            "[NEWORDER]: w_id={} d_id={} c_id={} rbk={} remote={} ol_cnt={}",
            self.w_id,
            self.d_id,
            self.c_id,
            self.rbk as u8,
            if self.is_remote { "t" } else { "f" },
            self.ol_cnt
        );
        for (_i, _item) in self.items.iter().enumerate() {
            log_info!(
                " ({}): ol_i_id={} ol_supply_w_id={} c_quantity={}",
                _i + 1,
                _item.ol_i_id,
                _item.ol_supply_w_id,
                _item.ol_quantity
            );
        }
    }
}

// NewOrder Transaction Output
#[derive(Debug, Clone)]
pub struct NewOrderOutput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub o_id: u32,
    pub o_entry_d: Timestamp,
    pub c_last: [u8; 16],  // MAX_LAST = 16
    pub c_credit: [u8; 2], // CREDIT = 2
    pub c_discount: f64,
    pub w_tax: f64,
    pub d_tax: f64,
    pub o_ol_cnt: u8,
    pub total: f64,
    pub items: Vec<NewOrderItemOutput>,
}

#[derive(Debug, Clone)]
pub struct NewOrderItemOutput {
    pub ol_i_id: u32,
    pub i_name: [u8; 24], // MAX_NAME = 24
    pub i_price: f64,
    pub ol_quantity: u8,
    pub s_quantity: i16,
    pub brand_generic: char,
    pub ol_amount: f64,
}

// Helper functions to convert byte arrays to strings for display
impl NewOrderOutput {
    pub fn c_last_str(&self) -> &str {
        std::str::from_utf8(&self.c_last)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_credit_str(&self) -> &str {
        std::str::from_utf8(&self.c_credit)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }
}

impl NewOrderItemOutput {
    pub fn i_name_str(&self) -> &str {
        std::str::from_utf8(&self.i_name)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }
}

// Standalone function that takes input and returns output
pub fn run_neworder_txn<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    input: &NewOrderTxnInput,
    stats: &mut TxnTypeStats,
) -> Result<NewOrderOutput, TPCCStatus> {
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let start = SystemTime::now();
    let mut helper = TxHelper::new(txn_storage, stats);

    let w_id = input.w_id;
    let d_id = input.d_id;
    let c_id = input.c_id;
    let ol_cnt = input.ol_cnt;
    let o_entry_d = input.o_entry_d;

    // Fetch warehouse record
    let w_key = WarehouseKey::create_key(w_id);
    let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Warehouse], w_key.into_bytes());
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderGetWarehouse as u8));
    }
    let mut w_bytes = res.unwrap();
    let w = Warehouse::from_bytes_mut(&mut w_bytes);
    let w_tax = w.w_tax;

    // Fetch and update District
    let d_key = DistrictKey::create_key(w_id, d_id);
    let mut o_id = 0;
    let mut d_tax = 0.0;
    let res = txn_storage.update_value_with_func(
        &txn,
        tbl_info[TPCCTable::District],
        d_key.into_bytes(),
        |bytes| {
            let d = unsafe { District::from_bytes_mut(bytes) };
            o_id = d.d_next_o_id;
            d.d_next_o_id += 1;
            d_tax = d.d_tax;
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderUpdateDistrict as u8));
    }

    // Fetch customer record
    let c_key = CustomerKey::create_key(w_id, d_id, c_id);
    let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Customer], c_key.into_bytes());
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderGetCustomer as u8));
    }
    let c_bytes = res.unwrap();
    let c = unsafe { Customer::from_bytes(&c_bytes) };

    // Create Order
    let mut o = Order::generate(w_id, d_id, o_id, c_id);
    o.o_entry_d = o_entry_d;
    o.o_carrier_id = 0;
    o.o_ol_cnt = ol_cnt;
    let o_key = OrderKey::create_key(w_id, d_id, o_id);
    let res = txn_storage.insert_value(
        &txn,
        tbl_info[TPCCTable::Order],
        o_key.into_bytes().to_vec(),
        o.as_bytes().to_vec(),
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderInsertOrder as u8));
    }

    // Create Order Secondary
    let os_key = OrderSecondaryKey::create_key(w_id, d_id, c_id, o_id);
    let res = txn_storage.insert_value(
        &txn,
        tbl_info[TPCCTable::OrderSecondary],
        os_key.into_bytes().to_vec(),
        o_key.into_bytes().to_vec(),
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderInsertOrderSecondary as u8));
    }

    // Create NewOrder
    let no = NewOrder::generate(w_id, d_id, o_id);
    let no_key = NewOrderKey::create_key(w_id, d_id, o_id);
    let res = txn_storage.insert_value(
        &txn,
        tbl_info[TPCCTable::NewOrder],
        no_key.into_bytes().to_vec(),
        no.as_bytes().to_vec(),
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::NewOrderInsertNewOrder as u8));
    }

    // Process order lines
    let mut total = 0.0;
    let mut item_outputs = Vec::with_capacity(input.items.len());

    for (ol_number, item) in input.items.iter().enumerate() {
        let ol_i_id = item.ol_i_id;
        let ol_supply_w_id = item.ol_supply_w_id;
        let ol_quantity = item.ol_quantity;

        // Check for rollback condition
        if ol_i_id == Item::UNUSED_ID {
            return Err(helper.usr_abort(&txn)); // Rollback condition met
        }

        // Get item
        let i_key = ItemKey::create_key(ol_i_id);
        let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Item], i_key.into_bytes());
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::NewOrderGetItem as u8));
        }
        let i_bytes = res.unwrap();
        let i = unsafe { Item::from_bytes(&i_bytes) };

        // Update stock
        let s_key = StockKey::create_key(ol_supply_w_id, ol_i_id);
        let mut s_quantity = 0;
        let mut s_dist = [0u8; 24];
        let mut brand_generic = 'B';

        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Stock],
            s_key.into_bytes(),
            |bytes| {
                let s = unsafe { Stock::from_bytes_mut(bytes) };
                s.s_quantity -= ol_quantity as i16;
                if s.s_quantity < 10 {
                    s.s_quantity += 91;
                }
                s.s_ytd += ol_quantity as u32;
                s.s_order_cnt += 1;
                s.s_remote_cnt += if ol_supply_w_id != w_id { 1 } else { 0 };
                s_quantity = s.s_quantity;
                s_dist.copy_from_slice(&s.s_dist[d_id as usize - 1]);
                brand_generic = if memmem::find(&i.i_data, b"ORIGINAL").is_some()
                    && memmem::find(&s.s_data, b"ORIGINAL").is_some()
                {
                    'B'
                } else {
                    'G'
                };
            },
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::NewOrderUpdateStock as u8));
        }

        let ol_amount = ol_quantity as f64 * i.i_price;
        total += ol_amount;

        // Create order line
        let mut ol = OrderLine::generate(
            w_id,
            d_id,
            o_id,
            ol_number as u8 + 1,
            ol_supply_w_id,
            ol_i_id,
            o_entry_d,
        );
        ol.ol_quantity = ol_quantity;
        ol.ol_amount = ol_amount;
        ol.ol_dist_info = s_dist;
        let ol_key = OrderLineKey::create_key(w_id, d_id, o_id, ol_number as u8 + 1);
        let res = txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::OrderLine],
            ol_key.into_bytes().to_vec(),
            ol.as_bytes().to_vec(),
        );
        if not_successful(&res) {
            return Err(helper.kill(&txn, &res, AbortID::NewOrderInsertOrderLine as u8));
        }

        // Create item output
        item_outputs.push(NewOrderItemOutput {
            ol_i_id,
            i_name: i.i_name,
            i_price: i.i_price,
            ol_quantity,
            s_quantity,
            brand_generic,
            ol_amount,
        });
    }

    // Calculate total with tax
    total = total * (1.0 - c.c_discount) * (1.0 + w_tax + d_tax);

    // Commit transaction
    let commit_status = helper.commit(
        &txn,
        AbortID::NewOrderPrecommit as u8,
        start.elapsed().unwrap().as_nanos() as u64,
    );
    if commit_status != TPCCStatus::Success {
        return Err(commit_status);
    }

    // Build output
    Ok(NewOrderOutput {
        w_id,
        d_id,
        c_id,
        o_id,
        o_entry_d,
        c_last: c.c_last,
        c_credit: c.c_credit,
        c_discount: c.c_discount,
        w_tax,
        d_tax,
        o_ol_cnt: ol_cnt,
        total,
        items: item_outputs,
    })
}
