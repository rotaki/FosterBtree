use criterion::black_box;
use rayon::prelude::*;
use std::{collections::HashMap, ops::Index};

use crate::{
    bp::ContainerId,
    prelude::{ContainerDS, ContainerOptions, DBOptions, ScanOptions, TxnOptions, TxnStorageTrait},
    utils::Permutation,
};

use super::record_definitions::{
    get_timestamp, urand_int, Customer, CustomerKey, CustomerSecondaryKey, District, DistrictKey,
    Item, ItemKey, NewOrder, NewOrderKey, Order, OrderKey, OrderLine, OrderLineKey,
    OrderSecondaryKey, Stock, StockKey, Timestamp, Warehouse, WarehouseKey,
};

pub const DB_ID: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TPCCTable {
    Item,
    Warehouse,
    Stock,
    District,
    Customer,
    CustomerSecondary,
    Order,
    OrderSecondary,
    OrderLine,
    NewOrder,
    History,
}

pub struct TPCCTableInfo {
    map: HashMap<TPCCTable, ContainerId>,
}

impl Default for TPCCTableInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl TPCCTableInfo {
    pub fn new() -> Self {
        TPCCTableInfo {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, table: TPCCTable, c_id: ContainerId) {
        self.map.insert(table, c_id);
    }
}

impl Index<TPCCTable> for TPCCTableInfo {
    type Output = ContainerId;

    fn index(&self, table: TPCCTable) -> &Self::Output {
        self.map.get(&table).unwrap()
    }
}

fn create_and_insert_item_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    i_id: u32,
) {
    let key = ItemKey::create_key(i_id);
    let value = Item::generate(i_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::Item],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

fn create_and_insert_warehouse_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    w_id: u16,
) {
    let key = WarehouseKey::create_key(w_id);
    let value = Warehouse::generate(w_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::Warehouse],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

fn create_and_insert_stock_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    w_id: u16,
    s_i_id: u32,
) {
    let key = StockKey::create_key(w_id, s_i_id);
    let value = Stock::generate(w_id, s_i_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::Stock],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

fn create_and_insert_district_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    d_w_id: u16,
    d_id: u8,
) {
    let key = DistrictKey::create_key(d_w_id, d_id);
    let value = District::generate(d_w_id, d_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::District],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

fn create_and_insert_customer_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    c_w_id: u16,
    c_d_id: u8,
    c_id: u32,
    t: Timestamp,
) {
    let key = CustomerKey::create_key(c_w_id, c_d_id, c_id);
    let value = Customer::generate(c_w_id, c_d_id, c_id, t);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::Customer],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();

    let sec_key = CustomerSecondaryKey::create_key_from_customer(&value);
    let sec_value = key.into_bytes();
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::CustomerSecondary],
            sec_key.into_bytes().to_vec(),
            sec_value.to_vec(),
        )
        .unwrap();
}

fn create_and_insert_history_record(
    _txn_storage: &impl TxnStorageTrait,
    _table_info: &TPCCTableInfo,
    _h_c_w_id: u16,
    _h_c_d_id: u8,
    _h_c_id: u32,
    _h_w_id: u16,
    _h_d_id: u8,
) {
    // ignore history record for now
}

fn create_and_insert_order_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    o_w_id: u16,
    o_d_id: u8,
    o_id: u32,
    o_c_id: u32,
) -> (Timestamp, u8) {
    let key = OrderKey::create_key(o_w_id, o_d_id, o_id);
    let value = Order::generate(o_w_id, o_d_id, o_id, o_c_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::Order],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();

    let sec_key = OrderSecondaryKey::create_key_from_order(&value);
    let sec_value = key.into_bytes();
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::OrderSecondary],
            sec_key.into_bytes().to_vec(),
            sec_value.to_vec(),
        )
        .unwrap();
    (value.o_entry_d, value.o_ol_cnt)
}

#[allow(clippy::too_many_arguments)]
fn create_and_insert_orderline_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    ol_w_id: u16,
    ol_d_id: u8,
    ol_o_id: u32,
    ol_number: u8,
    ol_supply_w_id: u16,
    ol_i_id: u32,
    o_entry_d: Timestamp,
) {
    let key = OrderLineKey::create_key(ol_w_id, ol_d_id, ol_o_id, ol_number);
    let value = OrderLine::generate(
        ol_w_id,
        ol_d_id,
        ol_o_id,
        ol_number,
        ol_supply_w_id,
        ol_i_id,
        o_entry_d,
    );
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::OrderLine],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

fn create_and_insert_neworder_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    no_w_id: u16,
    no_d_id: u8,
    no_o_id: u32,
) {
    let key = NewOrderKey::create_key(no_w_id, no_d_id, no_o_id);
    let value = NewOrder::generate(no_w_id, no_d_id, no_o_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[TPCCTable::NewOrder],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

/// Table Dependencies
/// - Item: 100K
/// - Warehouse: N
///   - Stock: N * 100K
///   - District: N * 10
///     - Customer: N * 10 * 3000
///       - History
///     - Order: N * 10 * 3000
///       - OrderLine: N * 10 * 3000 * 10 (average 10 orderlines per order)
///       - NewOrders
fn load_item_table(txn_storage: &impl TxnStorageTrait, table_info: &TPCCTableInfo) {
    println!("Loading item table");
    (1..Item::ITEMS + 1)
        .into_par_iter()
        .chunks(1000)
        .for_each(|chunk| {
            for i_id in chunk {
                create_and_insert_item_record(txn_storage, table_info, i_id as u32);
            }
        });
}

fn load_warehouse_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    num_warehouses: u16,
) {
    println!("Loading warehouse table");
    (1..num_warehouses + 1).into_par_iter().for_each(|w_id| {
        create_and_insert_warehouse_record(txn_storage, table_info, w_id);
        load_stock_table(txn_storage, table_info, w_id);
        load_district_table(txn_storage, table_info, w_id);
    });
}

fn load_stock_table(txn_storage: &impl TxnStorageTrait, table_info: &TPCCTableInfo, w_id: u16) {
    (1..Stock::STOCKS_PER_WARE + 1)
        .into_par_iter()
        .chunks(1000)
        .for_each(|chunk| {
            for s_i_id in chunk {
                create_and_insert_stock_record(txn_storage, table_info, w_id, s_i_id as u32);
            }
        });
}

fn load_district_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    d_w_id: u16,
) {
    (1..District::DISTS_PER_WARE + 1)
        .into_par_iter()
        .for_each(|d_id| {
            create_and_insert_district_record(txn_storage, table_info, d_w_id, d_id as u8);
            load_customer_table(txn_storage, table_info, d_w_id, d_id as u8);
            load_order_table(txn_storage, table_info, d_w_id, d_id as u8);
        })
}

fn load_customer_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    c_w_id: u16,
    c_d_id: u8,
) {
    let t = get_timestamp();
    (1..Customer::CUSTS_PER_DIST + 1)
        .into_par_iter()
        .chunks(1000)
        .for_each(|chunk| {
            for c_id in chunk {
                create_and_insert_customer_record(
                    txn_storage,
                    table_info,
                    c_w_id,
                    c_d_id,
                    c_id as u32,
                    t,
                );
                load_history_table(txn_storage, table_info, c_w_id, c_d_id, c_id as u32);
            }
        })
}

fn load_history_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    w_id: u16,
    d_id: u8,
    c_id: u32,
) {
    create_and_insert_history_record(txn_storage, table_info, w_id, d_id, c_id, w_id, d_id);
}

fn load_order_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    o_w_id: u16,
    o_d_id: u8,
) {
    let p = Permutation::new(1, Order::ORDS_PER_DIST);
    (1..Order::ORDS_PER_DIST + 1)
        .into_par_iter()
        .chunks(1000)
        .for_each(|chunk| {
            for o_id in chunk {
                let o_c_id = p[o_id - 1];
                let (o_entry_d, ol_cnt) = create_and_insert_order_record(
                    txn_storage,
                    table_info,
                    o_w_id,
                    o_d_id,
                    o_id as u32,
                    o_c_id as u32,
                );
                load_orderline_table(
                    txn_storage,
                    table_info,
                    ol_cnt,
                    o_w_id,
                    o_d_id,
                    o_id as u32,
                    o_entry_d,
                );
                if o_id > 2100 {
                    load_neworder_table(txn_storage, table_info, o_w_id, o_d_id, o_id as u32);
                }
            }
        })
}

fn load_orderline_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    ol_cnt: u8,
    ol_w_id: u16,
    ol_d_id: u8,
    ol_o_id: u32,
    o_entry_d: Timestamp,
) {
    for ol_number in 1..ol_cnt {
        let ol_i_id = urand_int(1, 100000);
        create_and_insert_orderline_record(
            txn_storage,
            table_info,
            ol_w_id,
            ol_d_id,
            ol_o_id,
            ol_number,
            ol_w_id,
            ol_i_id,
            o_entry_d,
        )
    }
}

fn load_neworder_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TPCCTableInfo,
    no_w_id: u16,
    no_d_id: u8,
    no_o_id: u32,
) {
    create_and_insert_neworder_record(txn_storage, table_info, no_w_id, no_d_id, no_o_id);
}

pub fn tpcc_load_schema(txn_storage: &impl TxnStorageTrait) -> TPCCTableInfo {
    let containers = txn_storage.list_containers(DB_ID).unwrap();
    let mut table_info = TPCCTableInfo::new();
    for (c_id, options) in containers {
        let table = match options.name().as_str() {
            "item" => TPCCTable::Item,
            "warehouse" => TPCCTable::Warehouse,
            "stock" => TPCCTable::Stock,
            "district" => TPCCTable::District,
            "customer" => TPCCTable::Customer,
            "customer_secondary" => TPCCTable::CustomerSecondary,
            "order" => TPCCTable::Order,
            "order_secondary" => TPCCTable::OrderSecondary,
            "orderline" => TPCCTable::OrderLine,
            "neworder" => TPCCTable::NewOrder,
            "history" => TPCCTable::History,
            _ => {
                println!("Unknown table: {}", options.name());
                continue;
            }
        };
        table_info.insert(table, c_id);
    }
    println!("======== Containers ========");
    println!("Item container id: {}", table_info[TPCCTable::Item]);
    println!(
        "Warehouse container id: {}",
        table_info[TPCCTable::Warehouse]
    );
    println!("Stock container id: {}", table_info[TPCCTable::Stock]);
    println!("District container id: {}", table_info[TPCCTable::District]);
    println!("Customer container id: {}", table_info[TPCCTable::Customer]);
    println!(
        "Customer secondary container id: {}",
        table_info[TPCCTable::CustomerSecondary]
    );
    println!("Orders container id: {}", table_info[TPCCTable::Order]);
    println!(
        "Orders secondary container id: {}",
        table_info[TPCCTable::OrderSecondary]
    );
    println!(
        "OrderLine container id: {}",
        table_info[TPCCTable::OrderLine]
    );
    println!("NewOrder container id: {}", table_info[TPCCTable::NewOrder]);
    println!("History container id: {}", table_info[TPCCTable::History]);
    table_info
}

pub fn tpcc_gen_all_tables(
    txn_storage: &impl TxnStorageTrait,
    num_warehouses: u16,
) -> TPCCTableInfo {
    let mut table_info = TPCCTableInfo::new();
    let db_id = txn_storage.open_db(DBOptions::new("tpcc")).unwrap();
    assert_eq!(db_id, DB_ID);

    println!("======== Containers ========");

    // Item table
    let c_id = txn_storage
        .create_container(DB_ID, ContainerOptions::primary("item", ContainerDS::BTree))
        .unwrap();
    table_info.insert(TPCCTable::Item, c_id);
    println!("Item container id: {}", c_id);

    // Warehouse table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("warehouse", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::Warehouse, c_id);
    println!("Warehouse container id: {}", c_id);

    // Stock table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("stock", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::Stock, c_id);
    println!("Stock container id: {}", c_id);

    // District table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("district", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::District, c_id);
    println!("District container id: {}", c_id);

    // Customer table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("customer", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::Customer, c_id);
    println!("Customer container id: {}", c_id);

    // Customer secondary index
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::secondary("customer_secondary", ContainerDS::BTree, c_id),
        )
        .unwrap();
    table_info.insert(TPCCTable::CustomerSecondary, c_id);
    println!("Customer secondary container id: {}", c_id);

    // Orders table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("order", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::Order, c_id);
    println!("Orders container id: {}", c_id);

    // Orders secondary index
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::secondary("order_secondary", ContainerDS::BTree, c_id),
        )
        .unwrap();
    table_info.insert(TPCCTable::OrderSecondary, c_id);
    println!("Orders secondary container id: {}", c_id);

    // OrderLine table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("orderline", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::OrderLine, c_id);
    println!("OrderLine container id: {}", c_id);

    // NewOrder table
    let c_id: u16 = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("neworder", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::NewOrder, c_id);
    println!("NewOrder container id: {}", c_id);

    // History table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("history", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(TPCCTable::History, c_id);
    println!("History container id: {}", c_id);

    println!("======== Start Loading Tables ========");

    let time = std::time::Instant::now();
    load_item_table(txn_storage, &table_info);
    load_warehouse_table(txn_storage, &table_info, num_warehouses);
    let elapsed = time.elapsed();
    println!(
        "======== Finished Loading Tables ========\nElapsed time: {:?}",
        elapsed
    );

    // Traverse the secondary index to make sure that the physical address is correct
    let txn = txn_storage.begin_txn(0, TxnOptions::default()).unwrap();
    let res = txn_storage.scan_range(
        &txn,
        table_info[TPCCTable::CustomerSecondary],
        ScanOptions {
            lower: vec![],
            upper: vec![],
        },
    );
    let iter = res.unwrap();
    let mut cnt = 0;
    loop {
        match txn_storage.iter_next(&txn, &iter) {
            Ok(Some((s_key, p_value))) => {
                cnt += 1;
                black_box((s_key, p_value));
            }
            Ok(None) => break,
            Err(e) => {
                panic!("Error iterating over customer secondary index");
            }
        }
    }
    println!("Scanned {} records in customer secondary index", cnt);
    drop(iter);

    let res = txn_storage.scan_range(
        &txn,
        table_info[TPCCTable::OrderSecondary],
        ScanOptions {
            lower: vec![],
            upper: vec![],
        },
    );
    let iter = res.unwrap();
    let mut cnt = 0;
    loop {
        match txn_storage.iter_next(&txn, &iter) {
            Ok(Some((s_key, p_value))) => {
                cnt += 1;
                black_box((s_key, p_value));
            }
            Ok(None) => break,
            Err(e) => {
                panic!("Error iterating over order secondary index");
            }
        }
    }
    println!("Scanned {} records in order secondary index", cnt);
    drop(iter);
    txn_storage.commit_txn(&txn, false).unwrap();

    table_info
}

pub fn tpcc_show_table_stats(txn_storage: &impl TxnStorageTrait, table_info: &TPCCTableInfo) {
    // Show the stats by the order of c_id
    let mut table_info_ordered = table_info.map.iter().collect::<Vec<_>>();
    table_info_ordered.sort_by_key(|(_, c_id)| **c_id);
    for (table, c_id) in table_info_ordered {
        let stats = txn_storage.get_container_stats(DB_ID, *c_id).unwrap();
        println!("========= {:?} (c_id: {}) =========", table, c_id);
        println!("{}", stats);
    }
}
