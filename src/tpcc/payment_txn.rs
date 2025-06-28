use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::ScanOptions;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::TPCCTable;
use crate::tpcc::TxnTypeStats;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::{not_successful, AbortID, TPCCStatus, TxHelper};

/// The Input struct for PaymentTx
#[derive(Default)]
pub struct PaymentTxnInput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub c_w_id: u16,
    pub c_d_id: u8,
    pub h_amount: f64,
    pub h_date: Timestamp,
    pub by_last_name: bool,
    pub c_last: [u8; Customer::MAX_LAST],
}

impl PaymentTxnInput {
    pub fn new(home_w_id: u16, num_warehouses: u16, use_random_warehouse: bool) -> Self {
        let w_id = if use_random_warehouse {
            urand_int(1, num_warehouses as u64) as u16
        } else {
            home_w_id
        };
        let d_id = urand_int(1, District::DISTS_PER_WARE as u64) as u8;
        let h_amount = urand_double(100, 500000, 100);

        // 85% local, 15% remote
        let (c_w_id, c_d_id) = if urand_int(1, 100) <= 85 {
            (w_id, d_id)
        } else {
            let c_w_id = if num_warehouses > 1 {
                let mut w = urand_int(1, num_warehouses as u64) as u16;
                if w == w_id {
                    w = if w == num_warehouses { 1 } else { w + 1 };
                }
                w
            } else {
                w_id
            };
            (c_w_id, urand_int(1, District::DISTS_PER_WARE as u64) as u8)
        };

        // 60% by last name, 40% by ID
        let by_last_name = urand_int(1, 100) <= 60;
        let (c_id, c_last) = if by_last_name {
            let last_name_num = nurand_int::<255, false>(0, 999) as usize;
            let mut c_last = [0u8; 16];
            make_clast(&mut c_last, last_name_num);
            (Customer::UNUSED_ID, c_last)
        } else {
            let c_last = [0u8; 16]; // Unused when not searching by last name
                                    // Generate a random customer ID between 1 and CUSTS_PER_DIST
            let c_id = nurand_int::<1023, false>(1, Customer::CUSTS_PER_DIST as u64) as u32;
            (c_id, c_last)
        };

        PaymentTxnInput {
            w_id,
            d_id,
            c_id,
            c_w_id,
            c_d_id,
            h_amount,
            h_date: get_timestamp(),
            by_last_name,
            c_last,
        }
    }

    pub fn print(&self) {
        if self.by_last_name {
            log_info!(
                "[PAYMENT] w_id={} d_id={} c_w_id={} c_d_id={} h_amount={:.2} h_date={} by_last_name=t c_last={}",
                self.w_id,
                self.d_id,
                self.c_w_id,
                self.c_d_id,
                self.h_amount,
                self.h_date,
                String::from_utf8_lossy(&self.c_last)
            );
        } else {
            log_info!(
                "[PAYMENT] w_id={} d_id={} c_w_id={} c_d_id={} h_amount={:.2} h_date={} by_last_name=f c_id={}",
                self.w_id, self.d_id, self.c_w_id, self.c_d_id, self.h_amount, self.h_date, self.c_id
            );
        }
    }
}

// Payment Transaction Output
#[derive(Debug, Clone)]
pub struct PaymentOutput {
    pub w_id: u16,
    pub d_id: u8,
    pub c_id: u32,
    pub c_w_id: u16,
    pub c_d_id: u8,
    pub c_first: [u8; 16],    // MAX_FIRST = 16
    pub c_middle: [u8; 2],    // MAX_MIDDLE = 2
    pub c_last: [u8; 16],     // MAX_LAST = 16
    pub c_street_1: [u8; 20], // MAX_STREET = 20
    pub c_street_2: [u8; 20], // MAX_STREET = 20
    pub c_city: [u8; 20],     // MAX_CITY = 20
    pub c_state: [u8; 2],     // STATE = 2
    pub c_zip: [u8; 9],       // ZIP = 9
    pub c_phone: [u8; 16],    // PHONE = 16
    pub c_since: Timestamp,
    pub c_credit: [u8; 2], // CREDIT = 2
    pub c_credit_lim: f64,
    pub c_discount: f64,
    pub c_balance: f64,
    pub c_data: Option<[u8; 500]>, // MAX_DATA = 500, only for bad credit customers
                                   // pub h_amount: f64,
                                   // pub h_date: u32,
}

impl PaymentOutput {
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

    pub fn c_street_1_str(&self) -> &str {
        std::str::from_utf8(&self.c_street_1)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_street_2_str(&self) -> &str {
        std::str::from_utf8(&self.c_street_2)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_city_str(&self) -> &str {
        std::str::from_utf8(&self.c_city)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_state_str(&self) -> &str {
        std::str::from_utf8(&self.c_state)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_zip_str(&self) -> &str {
        std::str::from_utf8(&self.c_zip)
            .unwrap_or("")
            .trim_end_matches('\0')
            .trim_end()
    }

    pub fn c_phone_str(&self) -> &str {
        std::str::from_utf8(&self.c_phone)
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

    pub fn c_data_str(&self) -> Option<&str> {
        self.c_data.as_ref().map(|data| {
            std::str::from_utf8(data)
                .unwrap_or("")
                .trim_end_matches('\0')
                .trim_end()
        })
    }
}

// Standalone function that takes input and returns output
pub fn run_payment_txn<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    input: &PaymentTxnInput,
    stats: &mut TxnTypeStats,
) -> Result<PaymentOutput, TPCCStatus> {
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let start = SystemTime::now();
    let mut helper = TxHelper::new(txn_storage, stats);

    let w_id = input.w_id;
    let d_id = input.d_id;
    let mut c_id = input.c_id;
    let c_w_id = input.c_w_id;
    let c_d_id = input.c_d_id;
    let h_amount = input.h_amount;
    let _h_date = input.h_date;
    let c_last = &input.c_last;
    let by_last_name = input.by_last_name;

    // Fetch and update Warehouse
    let w_key = WarehouseKey::create_key(w_id);
    let res = txn_storage.update_value_with_func(
        &txn,
        tbl_info[TPCCTable::Warehouse],
        w_key.into_bytes(),
        |bytes| {
            let w = Warehouse::from_bytes_mut(bytes);
            w.w_ytd += h_amount;
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::PaymentUpdateWarehouse as u8));
    }

    // Fetch and update District
    let d_key = DistrictKey::create_key(w_id, d_id);
    let res = txn_storage.update_value_with_func(
        &txn,
        tbl_info[TPCCTable::District],
        d_key.into_bytes(),
        |bytes| {
            let d = unsafe { District::from_bytes_mut(bytes) };
            d.d_ytd += h_amount;
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::PaymentUpdateDistrict as u8));
    }

    // Fetch and update Customer
    let c_key = if by_last_name {
        debug_assert!(c_id == Customer::UNUSED_ID);

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
            return Err(helper.kill(&txn, &res, AbortID::PaymentScanCustomerByLastName as u8));
        }

        let iter = res.unwrap();
        loop {
            let res = txn_storage.iter_next(&txn, &iter);
            match res {
                Ok(Some((key_bytes, value_bytes))) => {
                    customer_recs.push((key_bytes, value_bytes));
                }
                Ok(None) => break,
                Err(_) => {
                    return Err(helper.kill(
                        &txn,
                        &res,
                        AbortID::PaymentScanCustomerByLastName as u8,
                    ));
                }
            }
        }
        drop(iter);

        if customer_recs.is_empty() {
            panic!(
                "No customer found with last name: {}",
                String::from_utf8_lossy(c_last)
            );
        }

        let mid = customer_recs.len() / 2;
        let (_, v) = &customer_recs[mid];
        let cs = unsafe { Customer::from_bytes(v) };
        c_id = cs.c_id;
        CustomerKey::create_key(c_w_id, c_d_id, c_id)
    } else {
        CustomerKey::create_key(c_w_id, c_d_id, c_id)
    };

    // Update customer and get customer data
    let mut c_data_str = None;
    let mut customer_info = None;

    let res = txn_storage.update_value_with_func(
        &txn,
        tbl_info[TPCCTable::Customer],
        c_key.into_bytes(),
        |bytes| {
            let c = unsafe { Customer::from_bytes_mut(bytes) };
            c.c_balance -= h_amount;
            c.c_ytd_payment += h_amount;
            c.c_payment_cnt += 1;

            // Store customer info for output
            customer_info = Some((
                c.c_first,
                c.c_middle,
                c.c_last,
                c.c_address.street_1,
                c.c_address.street_2,
                c.c_address.city,
                c.c_address.state,
                c.c_address.zip,
                c.c_phone,
                c.c_since,
                c.c_credit,
                c.c_credit_lim,
                c.c_discount,
                c.c_balance,
            ));

            if c.c_credit[0] == b'B' && c.c_credit[1] == b'C' {
                // Format new c_data
                let c_data_fmt = format!(
                    "{:5} {:2} {:2} {:2} {:2} {:9.2} | ",
                    c_id, c_d_id, c_w_id, d_id, w_id, h_amount
                );
                let c_data_len = c_data_fmt.len();
                let move_len = Customer::MAX_DATA - c_data_len;

                // Shift existing c_data and prepend new data
                c.c_data.copy_within(0..move_len, c_data_len);
                c.c_data[..c_data_len].copy_from_slice(c_data_fmt.as_bytes());

                c_data_str = Some(c.c_data.clone());
            }
        },
    );
    if not_successful(&res) {
        return Err(helper.kill(&txn, &res, AbortID::PaymentUpdateCustomer as u8));
    }

    // Insert History record
    // let mut h = History::generate(c_w_id, c_d_id, c_id, w_id, d_id);
    // h.h_date = h_date;
    // h.h_amount = h_amount;
    // let h_key = HistoryKey::create_key(c_w_id, c_d_id, c_id);
    // let res = txn_storage.insert_value(&txn, tbl_info[TPCCTable::History], h_key.into_bytes(), unsafe {
    //     h.as_bytes().to_vec()
    // });
    // if res.is_err() {
    //     txn_storage.abort_txn(&txn).unwrap();
    //     return Err(TPCCStatus::SystemAbort);
    // }

    // Commit transaction
    let commit_status = helper.commit(
        &txn,
        AbortID::PaymentPrecommit as u8,
        start.elapsed().unwrap().as_nanos() as u64,
    );
    if commit_status != TPCCStatus::Success {
        return Err(commit_status);
    }

    // Build output
    let (
        c_first,
        c_middle,
        c_last,
        c_street_1,
        c_street_2,
        c_city,
        c_state,
        c_zip,
        c_phone,
        c_since,
        c_credit,
        c_credit_lim,
        c_discount,
        c_balance,
    ) = customer_info.unwrap();

    Ok(PaymentOutput {
        w_id,
        d_id,
        c_id,
        c_w_id,
        c_d_id,
        c_first,
        c_middle,
        c_last,
        c_street_1,
        c_street_2,
        c_city,
        c_state,
        c_zip,
        c_phone,
        c_since,
        c_credit,
        c_credit_lim,
        c_discount,
        c_balance,
        c_data: c_data_str,
        // h_amount,
        // h_date,
    })
}
