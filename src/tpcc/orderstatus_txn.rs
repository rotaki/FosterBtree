use std::time::SystemTime;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::*;
#[allow(unused_imports)]
use crate::log;
use crate::prelude::{ScanOptions, TxnOptions, TxnStorageStatus, TxnStorageTrait};
use crate::tpcc::loader::TPCCTable;
use crate::{log_info, write_fields};

pub struct OrderStatusTxn {
    input: OrderStatusTxnInput,
}

impl OrderStatusTxn {
    pub const NAME: &'static str = "OrderStatus";
    pub const ID: TPCCTxnProfileID = TPCCTxnProfileID::OrderStatusTxn;
}

impl TPCCTxnProfile for OrderStatusTxn {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input = OrderStatusTxnInput::new(config, w_id);
        OrderStatusTxn { input }
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
        let mut helper = TxHelper::new(txn_storage, &mut stat[OrderStatusTxn::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let c_w_id = self.input.w_id;
        let c_d_id = self.input.d_id;
        let mut c_id = self.input.c_id;
        let c_last = &self.input.c_last;
        let by_last_name = self.input.by_last_name;

        write_fields!(out, &c_w_id, &c_d_id, &c_id);

        if by_last_name {
            debug_assert!(c_id == Customer::UNUSED_ID);
            // Fetch customers with matching last name
            let mut customer_recs = Vec::new();
            let sec_key = CustomerSecondaryKey::create_key(c_w_id, c_d_id, c_last);
            let sec_key_bytes = sec_key.into_bytes();
            let res = txn_storage.scan_range(
                &txn,
                tbl_info[TPCCTable::CustomerSecondary],
                ScanOptions {
                    lower: sec_key_bytes.to_vec(),
                    upper: vec![],
                },
            );
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::GetCustomerByLastName as u8);
            }
            let iter = res.unwrap();
            loop {
                match txn_storage.iter_next(&txn, &iter) {
                    Ok(Some((s_key, p_value))) => {
                        if s_key != sec_key_bytes {
                            break;
                        } else {
                            customer_recs.push(p_value);
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        return helper.kill::<()>(
                            &txn,
                            &Err(e),
                            AbortID::GetCustomerByLastName as u8,
                        )
                    }
                }
            }
            drop(iter);

            if customer_recs.is_empty() {
                // This should not happen if the iterator is working correctly
                // as we should get at least one record for the matching last name
                return helper.kill::<()>(
                    &txn,
                    &Err(TxnStorageStatus::KeyNotFound),
                    AbortID::GetCustomerByLastName as u8,
                );
            }

            // Sort the customer records by c_first
            customer_recs.sort_by(|a, b| {
                let a = unsafe { Customer::from_bytes(a) };
                let b = unsafe { Customer::from_bytes(b) };
                a.c_first.cmp(&b.c_first)
            });

            // Select the middle record
            let c_bytes = customer_recs[(customer_recs.len() + 1) / 2 - 1].as_slice();
            let c = unsafe { Customer::from_bytes(c_bytes) };
            c_id = c.c_id;
            write_fields!(out, &c.c_first, &c.c_middle, &c.c_last, &c.c_balance);
        } else {
            debug_assert!(c_id != Customer::UNUSED_ID);
            let c_key = CustomerKey::create_key(c_w_id, c_d_id, c_id);
            let res =
                txn_storage.get_value(&txn, tbl_info[TPCCTable::Customer], c_key.into_bytes());
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::GetCustomer as u8);
            }
            let c_bytes = res.unwrap();
            let c = unsafe { Customer::from_bytes(&c_bytes) };
            c_id = c.c_id;
            write_fields!(out, &c.c_first, &c.c_middle, &c.c_last, &c.c_balance);
        }

        // Fetch the last order placed by the customer
        let o_sec_low_key = OrderSecondaryKey::create_key(c_w_id, c_d_id, c_id, 0);
        let o_sec_high_key = OrderSecondaryKey::create_key(c_w_id, c_d_id, c_id + 1, 0);
        let res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::OrderSecondary],
            ScanOptions {
                lower: o_sec_low_key.into_bytes().to_vec(),
                upper: o_sec_high_key.into_bytes().to_vec(),
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::GetOrderByCustomerId as u8);
        }
        let iter = res.unwrap();
        // Find the last order
        let mut last_order = vec![];
        loop {
            match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((sec_key, p_value))) => {
                    debug_assert_eq!(sec_key, o_sec_low_key.into_bytes());
                    last_order = p_value;
                }
                Ok(None) => break,
                Err(e) => {
                    return helper.kill::<()>(&txn, &Err(e), AbortID::GetOrderByCustomerId as u8)
                }
            }
        }
        drop(iter);

        if last_order.is_empty() {
            // This should not happen as we should have at least one order for the customer
            return helper.kill::<()>(
                &txn,
                &Err(TxnStorageStatus::KeyNotFound),
                AbortID::GetOrderByCustomerId as u8,
            );
        }
        let o = unsafe { Order::from_bytes(&last_order) };
        write_fields!(out, &o.o_id, &o.o_entry_d, &o.o_carrier_id);

        // Fetch the order lines for the order
        let low_key = OrderLineKey::create_key(o.o_w_id, o.o_d_id, o.o_id, 1);
        let up_key = OrderLineKey::create_key(o.o_w_id, o.o_d_id, o.o_id + 1, 1);

        let res = txn_storage.scan_range(
            &txn,
            tbl_info[TPCCTable::OrderLine],
            ScanOptions {
                lower: low_key.into_bytes().to_vec(),
                upper: up_key.into_bytes().to_vec(),
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::RangeGetOrderLine as u8);
        }
        let iter = res.unwrap();
        loop {
            match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((_, val))) => {
                    let ol = unsafe { OrderLine::from_bytes(&val) };
                    write_fields!(
                        out,
                        &ol.ol_supply_w_id,
                        &ol.ol_i_id,
                        &ol.ol_quantity,
                        &ol.ol_amount,
                        &ol.ol_delivery_d
                    );
                }
                Ok(None) => break,
                Err(e) => {
                    return helper.kill::<()>(&txn, &Err(e), AbortID::RangeGetOrderLine as u8)
                }
            }
        }
        drop(iter);

        let duration = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, AbortID::Precommit as u8, duration)
    }
}

impl OrderStatusTxn {
    pub fn print_abort_details(stat: &[usize]) {
        println!("OrderStatusTxn Abort Details:");
        for (i, &count) in stat.iter().enumerate().take(AbortID::Max as usize) {
            println!("        {:<45}: {}", AbortID::from(i as u8).as_str(), count,);
        }
    }
}

/// The Input struct for OrderStatusTxn
pub struct OrderStatusTxnInput {
    w_id: u16,
    d_id: u8,
    c_id: u32,
    by_last_name: bool,
    c_last: [u8; Customer::MAX_DATA],
}

impl OrderStatusTxnInput {
    pub fn new(_config: &TPCCConfig, w_id: u16) -> Self {
        let mut input = OrderStatusTxnInput {
            w_id: 0,
            d_id: 0,
            c_id: 0,
            by_last_name: false,
            c_last: [0; Customer::MAX_DATA],
        };
        input.w_id = w_id;
        input.d_id = urand_int(1, District::DISTS_PER_WARE as u64) as u8;
        input.by_last_name = urand_int(1, 100) <= 60;
        if input.by_last_name {
            input.c_id = Customer::UNUSED_ID;
            let num = nurand_int::<255, false>(0, 999);
            make_clast(&mut input.c_last, num as usize);
        } else {
            input.c_id = nurand_int::<1023, false>(1, 3000) as u32;
        }
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

/// Enum representing different abort reasons
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum AbortID {
    GetCustomerByLastName = 0,
    GetCustomer = 1,
    GetOrderByCustomerId = 2,
    RangeGetOrderLine = 3,
    Precommit = 4,
    Max = 5,
}

impl From<u8> for AbortID {
    fn from(val: u8) -> Self {
        match val {
            0 => AbortID::GetCustomerByLastName,
            1 => AbortID::GetCustomer,
            2 => AbortID::GetOrderByCustomerId,
            3 => AbortID::RangeGetOrderLine,
            4 => AbortID::Precommit,
            _ => panic!("Invalid AbortID"),
        }
    }
}

/// Function to get the abort reason as a static string
impl AbortID {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbortID::GetCustomerByLastName => "GET_CUSTOMER_BY_LAST_NAME",
            AbortID::GetCustomer => "GET_CUSTOMER",
            AbortID::GetOrderByCustomerId => "GET_ORDER_BY_CUSTOMER_ID",
            AbortID::RangeGetOrderLine => "RANGE_GET_ORDERLINE",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}
