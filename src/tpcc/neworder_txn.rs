use super::prelude::TPCCTxnProfileID;

use std::time::SystemTime;

#[allow(unused_imports)]
use crate::log;
use crate::log_info;
use crate::prelude::TxnOptions;
use crate::prelude::TxnStorageTrait;
use crate::tpcc::loader::TPCCTable;
use crate::write_fields;
use memchr::memmem;

use super::loader::TPCCTableInfo;
use super::record_definitions::*;
use super::txn_utils::*;

pub struct NewOrderTxn {
    input: NewOrderTxnInput,
}

impl NewOrderTxn {
    pub const NAME: &'static str = "NewOrder";
    pub const ID: TPCCTxnProfileID = TPCCTxnProfileID::NewOrderTxn;
}

impl TPCCTxnProfile for NewOrderTxn {
    fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let input = NewOrderTxnInput::new(config, w_id);
        NewOrderTxn { input }
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
        let mut helper = TxHelper::new(txn_storage, &mut stat[NewOrderTxn::ID]);
        let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();

        let is_remote = self.input.is_remote;
        let w_id = self.input.w_id;
        let d_id = self.input.d_id;
        let c_id = self.input.c_id;
        let ol_cnt = self.input.ol_cnt;
        let o_entry_d = self.input.o_entry_d;

        write_fields!(out, &w_id, &d_id, &c_id);

        // Fetch warehouse record
        let w_key = WarehouseKey::create_key(w_id);
        let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Warehouse], w_key.into_bytes());
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::GetWarehouse as u8);
        }
        let mut w_bytes = res.unwrap();
        let w = Warehouse::from_bytes_mut(&mut w_bytes);
        write_fields!(out, &w.w_tax);

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
                write_fields!(out, &d.d_tax, &d.d_next_o_id);
                d_tax = d.d_tax;
            },
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishUpdateDistrict as u8);
        }

        // Fetch customer record
        let c_key = CustomerKey::create_key(w_id, d_id, c_id);
        let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Customer], c_key.into_bytes());
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::GetCustomer as u8);
        }
        let c_bytes = res.unwrap();
        let c = unsafe { Customer::from_bytes(&c_bytes) };

        // Output data
        write_fields!(
            out,
            &c.c_last,
            &c.c_credit,
            &c.c_discount,
            &ol_cnt,
            &o_entry_d
        );

        // Insert NewOrder record
        let no_key = NewOrderKey::create_key(w_id, d_id, o_id);
        let mut no = NewOrder::new();
        self.create_neworder(&mut no, w_id, d_id, o_id);
        let res = txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::NewOrder],
            no_key.into_bytes().to_vec(),
            no.as_bytes().to_vec(),
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishInsertNewOrder as u8);
        }

        // Insert Order record
        let o_key = OrderKey::create_key(w_id, d_id, o_id);
        let mut o_rec = Order::new();
        self.create_order(&mut o_rec, w_id, d_id, c_id, o_id, ol_cnt, is_remote);
        let res = txn_storage.insert_value(
            &txn,
            tbl_info[TPCCTable::Order],
            o_key.into_bytes().to_vec(),
            o_rec.as_bytes().to_vec(),
        );
        if not_successful(config, &res) {
            return helper.kill(&txn, &res, AbortID::FinishInsertOrder as u8);
        }

        let mut total = 0.0;

        for (ol_num, item) in self.input.items.iter().enumerate() {
            let ol_num = (ol_num + 1) as u8;
            let ol_supply_w_id = item.ol_supply_w_id;
            let ol_i_id = item.ol_i_id;
            let ol_quantity = item.ol_quantity;

            if ol_i_id == Item::UNUSED_ID {
                return helper.usr_abort(&txn);
            }

            // Fetch Item record
            let i_key = ItemKey::create_key(ol_i_id);
            let res = txn_storage.get_value(&txn, tbl_info[TPCCTable::Item], i_key.into_bytes());
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::GetItem as u8);
            }
            let i_bytes = res.unwrap();
            let i = unsafe { Item::from_bytes(&i_bytes) };

            // Fetch and update Stock record. Create OrderLine record while updating Stock
            let s_key = StockKey::create_key(ol_supply_w_id, ol_i_id);
            let mut brand_generic = 'G';
            let mut ol = OrderLine::new();
            let ol_amount = ol_quantity as f64 * i.i_price;
            total += ol_amount;
            let res = txn_storage.update_value_with_func(
                &txn,
                tbl_info[TPCCTable::Stock],
                s_key.into_bytes(),
                |bytes| {
                    let s = unsafe { Stock::from_bytes_mut(bytes) };
                    // Modify stock
                    if memmem::find(&s.s_data, b"ORIGINAL").is_some()
                        && memmem::find(&i.i_data, b"ORIGINAL").is_some()
                    {
                        brand_generic = 'B';
                    }
                    self.modify_stock(s, ol_quantity, is_remote);
                    self.create_orderline(
                        &mut ol,
                        w_id,
                        d_id,
                        o_id,
                        ol_num,
                        ol_i_id,
                        ol_supply_w_id,
                        ol_quantity,
                        ol_amount,
                        s,
                    );
                    write_fields!(out, &s.s_quantity);
                },
            );
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::FinishUpdateStock as u8);
            }

            // Insert OrderLine record
            let ol_key = OrderLineKey::create_key(w_id, d_id, o_id, ol_num);
            let res = txn_storage.insert_value(
                &txn,
                tbl_info[TPCCTable::OrderLine],
                ol_key.into_bytes().to_vec(),
                ol.as_bytes().to_vec(),
            );
            if not_successful(config, &res) {
                return helper.kill(&txn, &res, AbortID::FinishInsertOrderLine as u8);
            }

            write_fields!(
                out,
                &ol_supply_w_id,
                &ol_i_id,
                &i.i_name,
                &ol_quantity,
                &brand_generic,
                &i.i_price,
                &ol_amount
            );
        }

        total *= (1.0 - c.c_discount) * (1.0 + w.w_tax + d_tax);
        out.write(&total);

        let duration = start.elapsed().unwrap().as_micros() as u64;
        helper.commit(&txn, AbortID::Precommit as u8, duration)
    }
}

impl NewOrderTxn {
    fn modify_stock(&self, s: &mut Stock, ol_quantity: u8, is_remote: bool) {
        if s.s_quantity > ol_quantity as i16 + 10 {
            s.s_quantity -= ol_quantity as i16;
        } else {
            s.s_quantity = s.s_quantity - ol_quantity as i16 + 91;
        }
        s.s_ytd += ol_quantity as u32;
        s.s_order_cnt += 1;
        if is_remote {
            s.s_remote_cnt += 1;
        }
    }

    fn create_neworder(&self, no: &mut NewOrder, w_id: u16, d_id: u8, o_id: u32) {
        no.no_w_id = w_id;
        no.no_d_id = d_id;
        no.no_o_id = o_id;
    }

    #[allow(clippy::too_many_arguments)]
    fn create_order(
        &self,
        o: &mut Order,
        w_id: u16,
        d_id: u8,
        c_id: u32,
        o_id: u32,
        ol_cnt: u8,
        is_remote: bool,
    ) {
        o.o_w_id = w_id;
        o.o_d_id = d_id;
        o.o_c_id = c_id;
        o.o_id = o_id;
        o.o_carrier_id = 0;
        o.o_ol_cnt = ol_cnt;
        o.o_all_local = !is_remote as u8;
        o.o_entry_d = get_timestamp();
    }

    #[allow(clippy::too_many_arguments)]
    fn create_orderline(
        &self,
        ol: &mut OrderLine,
        w_id: u16,
        d_id: u8,
        o_id: u32,
        ol_num: u8,
        ol_i_id: u32,
        ol_supply_w_id: u16,
        ol_quantity: u8,
        ol_amount: f64,
        s: &Stock,
    ) {
        ol.ol_w_id = w_id;
        ol.ol_d_id = d_id;
        ol.ol_o_id = o_id;
        ol.ol_number = ol_num;
        ol.ol_i_id = ol_i_id;
        ol.ol_supply_w_id = ol_supply_w_id;
        ol.ol_delivery_d = 0;
        ol.ol_quantity = ol_quantity;
        ol.ol_amount = ol_amount;
        ol.ol_dist_info
            .copy_from_slice(&s.s_dist[(d_id - 1) as usize]);
    }

    pub fn print_abort_details(stat: &[usize]) {
        println!("NewOrderTxn Abort Details:");
        for (i, &count) in stat.iter().enumerate().take(AbortID::Max as usize) {
            println!("        {:<45}: {}", AbortID::from(i as u8).as_str(), count);
        }
    }
}

/// The Input struct for NewOrderTxn
#[derive(Default)]
pub struct NewOrderTxnInput {
    w_id: u16,
    d_id: u8,
    c_id: u32,
    ol_cnt: u8,
    o_entry_d: Timestamp,
    rbk: bool,
    is_remote: bool,
    items: Vec<NewOrderItem>,
}

#[derive(Default)]
pub struct NewOrderItem {
    ol_supply_w_id: u16,
    ol_i_id: u32,
    ol_quantity: u8,
}

impl NewOrderTxnInput {
    pub fn new(config: &TPCCConfig, w_id: u16) -> Self {
        let mut input = NewOrderTxnInput::default();
        let num_warehouses = config.num_warehouses;
        input.w_id = w_id;
        input.d_id = urand_int(1, District::DISTS_PER_WARE as u8);
        input.c_id = nurand_int::<1023, false>(1, Customer::CUSTS_PER_DIST as u64) as u32;
        input.ol_cnt = urand_int(
            OrderLine::MIN_ORDLINES_PER_ORD as u8,
            OrderLine::MAX_ORDLINES_PER_ORD as u8,
        );
        input.o_entry_d = get_timestamp();
        input.rbk = urand_int(1, 100) == 1;
        input.is_remote = urand_int(1, 100) == 1;
        input.items = Vec::with_capacity(OrderLine::MAX_ORDLINES_PER_ORD);
        for i in 1..=input.ol_cnt {
            let mut item = NewOrderItem::default();
            if i == input.ol_cnt && input.rbk {
                item.ol_i_id = Item::UNUSED_ID; /* set to an unused value */
            } else {
                item.ol_i_id = nurand_int::<8191, false>(1, Item::ITEMS as u64) as u32;
            }
            if input.is_remote && num_warehouses > 1 {
                let mut remote_w_id;
                loop {
                    remote_w_id = urand_int(1, num_warehouses as u64) as u16;
                    if remote_w_id != w_id {
                        break;
                    }
                }
                assert!(remote_w_id != w_id);
                item.ol_supply_w_id = remote_w_id;
            } else {
                item.ol_supply_w_id = w_id;
            }
            item.ol_quantity = urand_int(1, 10) as u8;
            input.items.push(item);
        }
        input
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
        for _item in self.items.iter() {
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

/// Enum representing different abort reasons
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
enum AbortID {
    GetWarehouse = 0,
    PrepareUpdateDistrict = 1,
    FinishUpdateDistrict = 2,
    GetCustomer = 3,
    PrepareInsertNewOrder = 4,
    FinishInsertNewOrder = 5,
    PrepareInsertOrder = 6,
    FinishInsertOrder = 7,
    GetItem = 8,
    PrepareUpdateStock = 9,
    FinishUpdateStock = 10,
    PrepareInsertOrderLine = 11,
    FinishInsertOrderLine = 12,
    Precommit = 13,
    Max = 14,
}

impl From<u8> for AbortID {
    fn from(val: u8) -> Self {
        match val {
            0 => AbortID::GetWarehouse,
            1 => AbortID::PrepareUpdateDistrict,
            2 => AbortID::FinishUpdateDistrict,
            3 => AbortID::GetCustomer,
            4 => AbortID::PrepareInsertNewOrder,
            5 => AbortID::FinishInsertNewOrder,
            6 => AbortID::PrepareInsertOrder,
            7 => AbortID::FinishInsertOrder,
            8 => AbortID::GetItem,
            9 => AbortID::PrepareUpdateStock,
            10 => AbortID::FinishUpdateStock,
            11 => AbortID::PrepareInsertOrderLine,
            12 => AbortID::FinishInsertOrderLine,
            13 => AbortID::Precommit,
            _ => panic!("Invalid AbortID"),
        }
    }
}

/// Function to get the abort reason as a static string
impl AbortID {
    pub fn as_str(&self) -> &'static str {
        match self {
            AbortID::GetWarehouse => "GET_WAREHOUSE",
            AbortID::PrepareUpdateDistrict => "PREPARE_UPDATE_DISTRICT",
            AbortID::FinishUpdateDistrict => "FINISH_UPDATE_DISTRICT",
            AbortID::GetCustomer => "GET_CUSTOMER",
            AbortID::PrepareInsertNewOrder => "PREPARE_INSERT_NEWORDER",
            AbortID::FinishInsertNewOrder => "FINISH_INSERT_NEWORDER",
            AbortID::PrepareInsertOrder => "PREPARE_INSERT_ORDER",
            AbortID::FinishInsertOrder => "FINISH_INSERT_ORDER",
            AbortID::GetItem => "GET_ITEM",
            AbortID::PrepareUpdateStock => "PREPARE_UPDATE_STOCK",
            AbortID::FinishUpdateStock => "FINISH_UPDATE_STOCK",
            AbortID::PrepareInsertOrderLine => "PREPARE_INSERT_ORDERLINE",
            AbortID::FinishInsertOrderLine => "FINISH_INSERT_ORDERLINE",
            AbortID::Precommit => "PRECOMMIT",
            _ => panic!("Invalid AbortID"),
        }
    }
}
