use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::ScanOptions;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageStatus;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::TPCCTable;
use crate::write_fields;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::*;

pub struct PaymentTxn {
    input: PaymentTxnInput,
}

impl PaymentTxn {
    pub const NAME: &'static str = "Payment";
    pub const ID: TPCCTxnProfileID = TPCCTxnProfileID::PaymentTxn;
}

impl TPCCTxnProfile for PaymentTxn {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input: PaymentTxnInput = PaymentTxnInput::new(config, w_id);
        PaymentTxn { input }
    }
    /// The run method translated into Rust
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
        let mut helper = TxHelper::new(txn_storage, &mut stat[PaymentTxn::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let w_id = self.input.w_id;
        let d_id = self.input.d_id;
        let mut c_id = self.input.c_id;
        let c_w_id = self.input.c_w_id;
        let c_d_id = self.input.c_d_id;
        let h_amount = self.input.h_amount;
        let _h_date = self.input.h_date;
        let c_last = &self.input.c_last;
        let by_last_name = self.input.by_last_name;

        write_fields!(out, &w_id, &d_id);

        // Fetch and update Warehouse
        let w_key = WarehouseKey::create_key(w_id);
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Warehouse],
            w_key.into_bytes(),
            |bytes| {
                let w = Warehouse::from_bytes_mut(bytes);
                write_fields!(out, &w.w_name);
                w.w_ytd += h_amount;
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::UpdateWarehouse as u8);
        }

        // Fetch and update District
        let d_key = DistrictKey::create_key(w_id, d_id);
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::District],
            d_key.into_bytes(),
            |bytes| {
                let d = unsafe { District::from_bytes_mut(bytes) };
                write_fields!(out, &d.d_name);
                d.d_ytd += h_amount;
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::UpdateDistrict as u8);
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
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::ScanCustomerByLastName as u8);
            }
            let iter = res.unwrap();
            let fe_res = txn_storage.iter_for_each(&txn, &iter, &mut |_key, value| {
                customer_recs.push(value.to_vec());
                true
            });
            if let Err(e) = fe_res {
                drop(iter);
                return helper.kill::<()>(&txn, &Err(e), AbortID::ScanCustomerByLastName as u8);
            }
            drop(iter);

            if customer_recs.is_empty() {
                // This should not happen if iteration is correct as we should have at least one record
                // with any pattern of last name.
                return helper.kill::<()>(
                    &txn,
                    &Err(TxnStorageStatus::KeyNotFound),
                    AbortID::ScanCustomerByLastName as u8,
                );
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
            CustomerKey::create_key_from_customer(c)
        } else {
            // Customer is selected based on customer number
            CustomerKey::create_key(c_w_id, c_d_id, c_id)
        };
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[TPCCTable::Customer],
            c_key.into_bytes(),
            |bytes| {
                let c = Customer::from_bytes_mut(bytes);
                write_fields!(
                    out,
                    &c.c_first,
                    &c.c_middle,
                    &c.c_last,
                    &c.c_address,
                    &c.c_phone,
                    &c.c_since,
                    &c.c_credit,
                    &c.c_credit_lim,
                    &c.c_discount,
                    &c.c_balance
                );
                c_id = c.c_id;
                c.c_balance -= h_amount;
                c.c_ytd_payment += h_amount;
                c.c_payment_cnt += 1;
                if c.c_credit.starts_with(b"BC") {
                    write_fields!(out, &c.c_data);
                    self.modify_customer_data(c, w_id, d_id, c_w_id, c_d_id, h_amount);
                }
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::UpdateCustomer as u8);
        }

        // Insert History
        // Ignore it for now as it is not used for the output.

        let duration = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, AbortID::Precommit as u8, duration)
    }

    // Create history record
    // fn create_history(
    //     &self,
    //     h: &mut History,
    //     w_id: u16,
    //     d_id: u8,
    //     c_id: u32,
    //     c_w_id: u16,
    //     c_d_id: u8,
    //     h_amount: f64,
    //     w_name: &str,
    //     d_name: &str,
    // ) {
    //     h.h_c_id = c_id;
    //     h.h_c_d_id = c_d_id;
    //     h.h_c_w_id = c_w_id;
    //     h.h_d_id = d_id;
    //     h.h_w_id = w_id;
    //     h.h_date = get_timestamp();
    //     h.h_amount = h_amount;
    //     let data = format!("{:<10.10}    {:<10.10}", w_name, d_name);
    //     let data_bytes = data.as_bytes();
    //     h.h_data[..data_bytes.len()].copy_from_slice(data_bytes);
    // }
}

impl PaymentTxn {
    // Modify customer data
    fn modify_customer_data(
        &self,
        c: &mut Customer,
        w_id: u16,
        d_id: u8,
        c_w_id: u16,
        c_d_id: u8,
        h_amount: f64,
    ) {
        let new_data = format!(
            "| {:4} {:2} {:4} {:2} {:4} ${:7.2}",
            c.c_id, c_d_id, c_w_id, d_id, w_id, h_amount
        );
        let new_data_bytes = new_data.as_bytes();
        let len = new_data_bytes.len();
        c.c_data.copy_within(0..Customer::MAX_DATA - len, len);
        c.c_data[..len].copy_from_slice(new_data_bytes);
    }

    pub fn print_abort_details(stat: &[usize]) {
        println!("PaymentTxn Abort Details:");
        for (i, &count) in stat.iter().enumerate().take(AbortID::Max as usize) {
            println!("        {:<45}: {}", AbortID::from(i as u8).as_str(), count,);
        }
    }
}

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
    pub c_last: [u8; Customer::MAX_LAST + 1],
}

impl PaymentTxnInput {
    #[allow(clippy::field_reassign_with_default)]
    pub fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let num_warehouses = config.num_warehouses;
        let mut this = PaymentTxnInput::default();
        this.w_id = w_id;
        this.d_id = urand_int(1, District::DISTS_PER_WARE as u64) as u8;
        this.h_amount = urand_double(100, 500000, 100);
        this.h_date = get_timestamp();
        if num_warehouses == 1 || urand_int(1, 100) <= 85 {
            this.c_w_id = this.w_id;
            this.c_d_id = this.d_id;
        } else {
            loop {
                this.c_w_id = urand_int(1, num_warehouses as u64) as u16;
                if this.c_w_id != this.w_id {
                    break;
                }
            }
            debug_assert!(this.c_w_id != this.w_id);
            this.c_d_id = urand_int(1, District::DISTS_PER_WARE as u64) as u8;
        }
        this.by_last_name = urand_int(1, 100) <= 60;
        if this.by_last_name {
            this.c_id = Customer::UNUSED_ID;
            let num = nurand_int::<255, false>(0, 999);
            make_clast(&mut this.c_last, num as usize);
        } else {
            this.c_id = nurand_int::<1023, false>(1, 3000) as u32;
        }
        this
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

/// Enum representing different abort reasons
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum AbortID {
    UpdateWarehouse = 0,
    UpdateDistrict = 1,
    ScanCustomerByLastName = 2,
    UpdateCustomer = 3,
    Precommit = 4,
    Max = 5,
}

impl From<u8> for AbortID {
    fn from(val: u8) -> Self {
        match val {
            0 => AbortID::UpdateWarehouse,
            1 => AbortID::UpdateDistrict,
            2 => AbortID::ScanCustomerByLastName,
            3 => AbortID::UpdateCustomer,
            4 => AbortID::Precommit,
            _ => panic!("Invalid AbortID"),
        }
    }
}

/// Function to get the abort reason as a static string
impl AbortID {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbortID::UpdateWarehouse => "UPDATE_WAREHOUSE",
            AbortID::UpdateDistrict => "UPDATE_DISTRICT",
            AbortID::ScanCustomerByLastName => "SCAN_CUSTOMER_BY_LAST_NAME",
            AbortID::UpdateCustomer => "UPDATE_CUSTOMER",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}
