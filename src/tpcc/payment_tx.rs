use std::time::SystemTime;

use crate::log_info;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageStatus;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::Table;
use rand::Rng;

use super::loader::TableInfo;
use super::record_definitions::*;
use super::tx_utils::*;

macro_rules! write_fields {
    ($out:expr, $($field:expr),*) => {
        $(
            $out.write($field);
        )*
    };
}

/// The PaymentTx struct
pub struct PaymentTx {
    input: PaymentTxInput,
}

impl PaymentTx {
    pub const NAME: &'static str = "Payment";
    pub const ID: TxnProfileID = TxnProfileID::PaymentTx;

    /// Function to get the abort reason as a static string
    pub fn abort_reason(a: AbortID) -> &'static str {
        match a {
            AbortID::PrepareUpdateWarehouse => "PREPARE_UPDATE_WAREHOUSE",
            AbortID::FinishUpdateWarehouse => "FINISH_UPDATE_WAREHOUSE",
            AbortID::PrepareUpdateDistrict => "PREPARE_UPDATE_DISTRICT",
            AbortID::FinishUpdateDistrict => "FINISH_UPDATE_DISTRICT",
            AbortID::PrepareUpdateCustomerByLastName => "PREPARE_UPDATE_CUSTOMER_BY_LAST_NAME",
            AbortID::PrepareUpdateCustomer => "PREPARE_UPDATE_CUSTOMER",
            AbortID::FinishUpdateCustomer => "FINISH_UPDATE_CUSTOMER",
            AbortID::PrepareInsertHistory => "PREPARE_INSERT_HISTORY",
            AbortID::FinishInsertHistory => "FINISH_INSERT_HISTORY",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}

impl TxnProfile for PaymentTx {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input: PaymentTxInput = PaymentTxInput::new(config, w_id);
        PaymentTx { input }
    }
    /// The run method translated into Rust
    fn run<T: TxnStorageTrait>(
        &self,
        config: &TPCCConfig,
        txn_storage: &T,
        tbl_info: &TableInfo,
        stat: &mut Stat,
        out: &mut Output,
    ) -> TPCCStatus {
        let start = SystemTime::now();
        let mut res: TxnStorageStatus;
        let mut helper = TxHelper::new(txn_storage, &mut stat[PaymentTx::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let w_id = self.input.w_id;
        let d_id = self.input.d_id;
        let mut c_id = self.input.c_id;
        let c_w_id = self.input.c_w_id;
        let c_d_id = self.input.c_d_id;
        let h_amount = self.input.h_amount;
        let h_date = self.input.h_date;
        let c_last = &self.input.c_last;
        let by_last_name = self.input.by_last_name;

        write_fields!(out, &w_id, &d_id);

        // Fetch and update Warehouse
        let w_key = WarehouseKey::create_key(w_id);
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[Table::Warehouse],
            &w_key.into_bytes(),
            |bytes| {
                let w = Warehouse::from_bytes_mut(bytes);
                write_fields!(out, &w.w_name);
                w.w_ytd += h_amount;
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishUpdateWarehouse as u8);
        }

        // Fetch and update District
        let d_key = DistrictKey::create_key(w_id, d_id);
        let res = txn_storage.update_value_with_func(
            &txn,
            tbl_info[Table::District],
            &d_key.into_bytes(),
            |bytes| {
                let d = District::from_bytes_mut(bytes);
                write_fields!(out, &d.d_name);
                d.d_ytd += h_amount;
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishUpdateDistrict as u8);
        }

        // Fetch and update Customer
        /*
        let res = if by_last_name {
            todo!("Implement get_customer_by_last_name_and_prepare_for_update");
        } else {
            // Customer is selected based on customer number
            let c_key = CustomerKey::create_key(c_w_id, c_d_id, c_id);
            txn_storage.update_value_with_func(
                &txn,
                &(TableID::Customer as u16),
                &c_key.into_bytes(),
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
                        todo!("Implement modify_customer_data");
                    }
                },
            )
        };
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishUpdateCustomer as u8);
        }
        */

        // Insert History

        let duration = start.elapsed().unwrap().as_nanos() as u64;
        return helper.commit(&txn, AbortID::Precommit as u8, duration);
    }

    // Modify customer data
    // fn modify_customer_data(
    //     &self,
    //     c: &mut Customer,
    //     w_id: u16,
    //     d_id: u8,
    //     c_w_id: u16,
    //     c_d_id: u8,
    //     h_amount: f64,
    // ) {
    //     let new_data = format!(
    //         "| {:4} {:2} {:4} {:2} {:4} ${:7.2}",
    //         c.c_id, c_d_id, c_w_id, d_id, w_id, h_amount
    //     );
    //     let new_data_bytes = new_data.as_bytes();
    //     let len = new_data_bytes.len();
    //     let total_len = len + c.c_data.len();
    //     if total_len <= c.c_data.len() {
    //         c.c_data[..len].copy_from_slice(new_data_bytes);
    //     }
    // }

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

/// The Input struct for PaymentTx
#[derive(Default)]
pub struct PaymentTxInput {
    w_id: u16,
    d_id: u8,
    c_id: u32,
    c_w_id: u16,
    c_d_id: u8,
    h_amount: f64,
    h_date: Timestamp,
    by_last_name: bool,
    c_last: [u8; Customer::MAX_LAST + 1],
}

impl PaymentTxInput {
    pub fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let num_warehouses = config.num_warehouses;
        let mut this = PaymentTxInput::default();
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
                "pay: w_id={} d_id={} c_w_id={} c_d_id={} h_amount={:.2} h_date={} by_last_name=t c_last={}",
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
                "pay: w_id={} d_id={} c_w_id={} c_d_id={} h_amount={:.2} h_date={} by_last_name=f c_id={}",
                self.w_id, self.d_id, self.c_w_id, self.c_d_id, self.h_amount, self.h_date, self.c_id
            );
        }
    }
}

/// Enum representing different abort reasons
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum AbortID {
    PrepareUpdateWarehouse = 0,
    FinishUpdateWarehouse = 1,
    PrepareUpdateDistrict = 2,
    FinishUpdateDistrict = 3,
    PrepareUpdateCustomerByLastName = 4,
    PrepareUpdateCustomer = 5,
    FinishUpdateCustomer = 6,
    PrepareInsertHistory = 7,
    FinishInsertHistory = 8,
    Precommit = 9,
    Max = 10,
}

#[cfg(test)]
mod tests {
    use crate::{bp::get_test_bp, tpcc::loader::load_all_tables, txn_storage::NoWaitTxnStorage};

    use super::*;

    fn create_nowait_txn_storage() -> impl TxnStorageTrait {
        let bp = get_test_bp(100);
        let txn_storage = NoWaitTxnStorage::new(bp);
        txn_storage
    }

    #[test]
    fn test_payment_tx() {
        let txn_storage = create_nowait_txn_storage();
        let config = TPCCConfig {
            num_warehouses: 1,
            num_threads: 1,
            random_abort: false,
            fixed_warehouse_per_thread: false,
        };
        let tbl_info = load_all_tables(&txn_storage, &config);
        println!("Loaded all tables");

        let w_id = 1;
        let payment_tx = PaymentTx::new(&config, w_id);
        let mut stat = Stat::new();
        let mut out = Output::new();
        payment_tx.run(&config, &txn_storage, &tbl_info, &mut stat, &mut out);
    }
}
