use super::prelude::TPCCTxnProfileID;

use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;

use crate::prelude::ScanOptions;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::TPCCTable;
use crate::write_fields;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::*;

pub struct DeliveryTxn {
    input: DeliveryTxnInput,
}

impl DeliveryTxn {
    pub const NAME: &'static str = "Delivery";
    pub const ID: TPCCTxnProfileID = TPCCTxnProfileID::DeliveryTxn;
}

impl TPCCTxnProfile for DeliveryTxn {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input = DeliveryTxnInput::new(config, w_id);
        DeliveryTxn { input }
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
        let mut helper = TxHelper::new(txn_storage, &mut stat[DeliveryTxn::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let w_id = self.input.w_id;
        let o_carrier_id = self.input.o_carrier_id;

        write_fields!(out, &w_id, &o_carrier_id);

        'per_district_loop: for d_id in 1..=District::DISTS_PER_WARE as u8 {
            // Get the oldest NewOrder for this district
            let no_low_key = NewOrderKey::create_key(w_id, d_id, 1); // Starting from order id 1

            let res = txn_storage.scan_range(
                &txn,
                tbl_info[TPCCTable::NewOrder],
                ScanOptions {
                    lower: no_low_key.into_bytes().to_vec(),
                    upper: vec![],
                },
            );

            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::GetNewOrderWithSmallestKey as u8);
            }
            let iter = res.unwrap();

            let (no_key, _) = match txn_storage.iter_next(&txn, &iter) {
                Ok(Some((key_bytes, value))) => {
                    let no_key = *unsafe { NewOrderKey::from_bytes(&key_bytes) };
                    if no_key.w_id() == w_id && no_key.d_id() == d_id {
                        (no_key, value)
                    } else {
                        continue 'per_district_loop;
                    }
                }
                Ok(None) => {
                    // Nothing more in iterator, so go to next district
                    continue 'per_district_loop;
                }
                Err(e) => {
                    // Handle error
                    return helper.kill::<()>(
                        &txn,
                        &Err(e),
                        AbortID::GetNewOrderWithSmallestKey as u8,
                    );
                }
            };
            drop(iter);

            let res =
                txn_storage.delete_value(&txn, tbl_info[TPCCTable::NewOrder], no_key.into_bytes());
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::DeleteNewOrder as u8);
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
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::PrepareUpdateOrder as u8);
            }
            debug_assert_ne!(c_id, u32::MAX);

            // Sum up the total ol_amount and update OrderLine records
            let low_key = OrderLineKey::create_key(w_id, d_id, no_key.o_id(), 1);
            let up_key = OrderLineKey::create_key(w_id, d_id, no_key.o_id() + 1, 1);
            let res = txn_storage.scan_range(
                &txn,
                tbl_info[TPCCTable::OrderLine],
                ScanOptions {
                    lower: low_key.into_bytes().to_vec(),
                    upper: up_key.into_bytes().to_vec(),
                },
            );
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::RangeUpdateOrderLine as u8);
            }
            let iter = res.unwrap();
            let mut order_lines = vec![];
            loop {
                match txn_storage.iter_next(&txn, &iter) {
                    Ok(Some((key_bytes, value_bytes))) => {
                        order_lines.push((key_bytes, value_bytes));
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) => {
                        return helper.kill::<()>(
                            &txn,
                            &Err(e),
                            AbortID::RangeUpdateOrderLine as u8,
                        );
                    }
                }
            }
            drop(iter);

            let mut total_ol_amount = 0.0;
            for (key_bytes, mut value_bytes) in order_lines {
                let ol = unsafe { OrderLine::from_bytes_mut(&mut value_bytes) };
                ol.ol_delivery_d = self.input.ol_delivery_d;
                total_ol_amount += ol.ol_amount;
                let res = txn_storage.update_value(
                    &txn,
                    tbl_info[TPCCTable::OrderLine],
                    &key_bytes,
                    value_bytes,
                );
                if not_successful(config, &res) {
                    return helper.kill(&txn, &res, AbortID::RangeUpdateOrderLine as u8);
                }
            }

            // Update the Customer record
            let c_key = CustomerKey::create_key(w_id, d_id, c_id);
            let res = txn_storage.update_value_with_func(
                &txn,
                tbl_info[TPCCTable::Customer],
                c_key.into_bytes(),
                |bytes| {
                    let c = Customer::from_bytes_mut(bytes);
                    c.c_balance += total_ol_amount;
                    c.c_delivery_cnt += 1;
                },
            );
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::PrepareUpdateCustomer as u8);
            }

            write_fields!(out, &d_id, &no_key.o_id());
        }

        let duration = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, AbortID::Precommit as u8, duration)
    }
}

impl DeliveryTxn {
    pub fn print_abort_details(stat: &[usize]) {
        println!("DeliveryTxn Abort Details:");
        for (i, &count) in stat.iter().enumerate().take(AbortID::Max as usize) {
            println!("        {:<45}: {}", AbortID::from(i as u8).as_str(), count);
        }
    }
}

/// The Input struct for DeliveryTxn
#[derive(Default)]
pub struct DeliveryTxnInput {
    w_id: u16,
    o_carrier_id: u8,
    ol_delivery_d: Timestamp,
}

impl DeliveryTxnInput {
    pub fn new(_config: &TPCCConfig, w_id: u16) -> Self {
        DeliveryTxnInput {
            w_id,
            o_carrier_id: urand_int(1, 10) as u8,
            ol_delivery_d: get_timestamp(),
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

/// Enum representing different abort reasons
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum AbortID {
    GetNewOrderWithSmallestKey = 0,
    DeleteNewOrder = 1,
    FinishDeleteNewOrder = 2,
    PrepareUpdateOrder = 3,
    FinishUpdateOrder = 4,
    RangeUpdateOrderLine = 5,
    PrepareUpdateCustomer = 6,
    FinishUpdateCustomer = 7,
    Precommit = 8,
    Max = 9,
}

impl From<u8> for AbortID {
    fn from(val: u8) -> Self {
        match val {
            0 => AbortID::GetNewOrderWithSmallestKey,
            1 => AbortID::DeleteNewOrder,
            2 => AbortID::FinishDeleteNewOrder,
            3 => AbortID::PrepareUpdateOrder,
            4 => AbortID::FinishUpdateOrder,
            5 => AbortID::RangeUpdateOrderLine,
            6 => AbortID::PrepareUpdateCustomer,
            7 => AbortID::FinishUpdateCustomer,
            8 => AbortID::Precommit,
            _ => panic!("Invalid AbortID"),
        }
    }
}

/// Function to get the abort reason as a static string
impl AbortID {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbortID::GetNewOrderWithSmallestKey => "GET_NEWORDER_WITH_SMALLEST_KEY",
            AbortID::DeleteNewOrder => "DELETE_NEWORDER",
            AbortID::FinishDeleteNewOrder => "FINISH_DELETE_NEWORDER",
            AbortID::PrepareUpdateOrder => "PREPARE_UPDATE_ORDER",
            AbortID::FinishUpdateOrder => "FINISH_UPDATE_ORDER",
            AbortID::RangeUpdateOrderLine => "RANGE_UPDATE_ORDERLINE",
            AbortID::PrepareUpdateCustomer => "PREPARE_UPDATE_CUSTOMER",
            AbortID::FinishUpdateCustomer => "FINISH_UPDATE_CUSTOMER",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}
