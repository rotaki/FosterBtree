use std::{collections::HashMap, ops::Index};

use crate::{
    bp::ContainerId,
    prelude::{ContainerDS, ContainerOptions, DBOptions, TxnStorageTrait},
};

use super::{
    record_definitions::{
        get_timestamp, urand_int, Customer, CustomerKey, CustomerSecondaryKey, District,
        DistrictKey, Item, ItemKey, NewOrder, NewOrderKey, Order, OrderKey, OrderLine,
        OrderLineKey, OrderSecondaryKey, Stock, StockKey, Timestamp, Warehouse, WarehouseKey,
    },
    tx_utils::{Permutation, TPCCConfig},
};

pub const DB_ID: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Table {
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

pub struct TableInfo {
    map: HashMap<Table, ContainerId>,
}

impl Default for TableInfo {
    fn default() -> Self {
        Self::new()
    }
}

impl TableInfo {
    pub fn new() -> Self {
        TableInfo {
            map: HashMap::new(),
        }
    }

    pub fn insert(&mut self, table: Table, c_id: ContainerId) {
        self.map.insert(table, c_id);
    }
}

impl Index<Table> for TableInfo {
    type Output = ContainerId;

    fn index(&self, table: Table) -> &Self::Output {
        self.map.get(&table).unwrap()
    }
}

pub fn create_and_insert_item_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    i_id: u32,
) {
    let key = ItemKey::create_key(i_id);
    let value = Item::generate(i_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::Item],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_warehouse_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    w_id: u16,
) {
    let key = WarehouseKey::create_key(w_id);
    let value = Warehouse::generate(w_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::Warehouse],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_stock_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    w_id: u16,
    s_i_id: u32,
) {
    let key = StockKey::create_key(w_id, s_i_id);
    let value = Stock::generate(w_id, s_i_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::Stock],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_district_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    d_w_id: u16,
    d_id: u8,
) {
    let key = DistrictKey::create_key(d_w_id, d_id);
    let value = District::generate(d_w_id, d_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::District],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_customer_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
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
            table_info[Table::Customer],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();

    let sec_key = CustomerSecondaryKey::create_key_from_customer(&value);
    let sec_value = key.into_bytes();
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::CustomerSecondary],
            sec_key.into_bytes().to_vec(),
            sec_value.to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_history_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    h_c_w_id: u16,
    h_c_d_id: u8,
    h_c_id: u32,
    h_w_id: u16,
    h_d_id: u8,
) {
    // ignore history record for now
}

pub fn create_and_insert_order_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
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
            table_info[Table::Order],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();

    let sec_key = OrderSecondaryKey::create_key_from_order(&value);
    let sec_value = key.into_bytes();
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::OrderSecondary],
            sec_key.into_bytes().to_vec(),
            sec_value.to_vec(),
        )
        .unwrap();
    (value.o_entry_d, value.o_ol_cnt)
}

pub fn create_and_insert_orderline_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
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
            table_info[Table::OrderLine],
            key.into_bytes().to_vec(),
            value.as_bytes().to_vec(),
        )
        .unwrap();
}

pub fn create_and_insert_neworder_record(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    no_w_id: u16,
    no_d_id: u8,
    no_o_id: u32,
) {
    let key = NewOrderKey::create_key(no_w_id, no_d_id, no_o_id);
    let value = NewOrder::generate(no_w_id, no_d_id, no_o_id);
    txn_storage
        .raw_insert_value(
            DB_ID,
            table_info[Table::NewOrder],
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

pub fn load_item_table(txn_storage: &impl TxnStorageTrait, table_info: &TableInfo) {
    println!("Loading item table");
    for i_id in 1..=Item::ITEMS {
        create_and_insert_item_record(txn_storage, table_info, i_id as u32);
    }
}

pub fn load_warehouse_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    config: &TPCCConfig,
) {
    let num_warehouses = config.num_warehouses;
    for w_id in 1..=num_warehouses {
        println!("Loading warehouse and other tables: {}", w_id);
        create_and_insert_warehouse_record(txn_storage, table_info, w_id);
        load_stock_table(txn_storage, table_info, w_id);
        load_district_table(txn_storage, table_info, w_id);
    }
}

pub fn load_stock_table(txn_storage: &impl TxnStorageTrait, table_info: &TableInfo, w_id: u16) {
    for s_i_id in 1..=Stock::STOCKS_PER_WARE {
        create_and_insert_stock_record(txn_storage, table_info, w_id, s_i_id as u32);
    }
}

pub fn load_district_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    d_w_id: u16,
) {
    for d_id in 1..=District::DISTS_PER_WARE {
        create_and_insert_district_record(txn_storage, table_info, d_w_id, d_id as u8);
        load_customer_table(txn_storage, table_info, d_w_id, d_id as u8);
        load_order_table(txn_storage, table_info, d_w_id, d_id as u8);
    }
}

pub fn load_customer_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    c_w_id: u16,
    c_d_id: u8,
) {
    let t = get_timestamp();
    for c_id in 1..=Customer::CUSTS_PER_DIST {
        create_and_insert_customer_record(txn_storage, table_info, c_w_id, c_d_id, c_id as u32, t);
        load_history_table(txn_storage, table_info, c_w_id, c_d_id, c_id as u32);
    }
}

pub fn load_history_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    w_id: u16,
    d_id: u8,
    c_id: u32,
) {
    create_and_insert_history_record(txn_storage, table_info, w_id, d_id, c_id, w_id, d_id);
}

pub fn load_order_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    o_w_id: u16,
    o_d_id: u8,
) {
    let p = Permutation::new(1, Order::ORDS_PER_DIST);
    for o_id in 1..=Order::ORDS_PER_DIST {
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
}

pub fn load_orderline_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
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

pub fn load_neworder_table(
    txn_storage: &impl TxnStorageTrait,
    table_info: &TableInfo,
    no_w_id: u16,
    no_d_id: u8,
    no_o_id: u32,
) {
    create_and_insert_neworder_record(txn_storage, table_info, no_w_id, no_d_id, no_o_id);
}

pub fn load_all_tables(txn_storage: &impl TxnStorageTrait, config: &TPCCConfig) -> TableInfo {
    let mut table_info = TableInfo::new();
    let db_id = txn_storage.open_db(DBOptions::new("tpcc")).unwrap();
    assert_eq!(db_id, DB_ID);

    println!("======== Containers ========");

    // Item table
    let c_id = txn_storage
        .create_container(DB_ID, ContainerOptions::primary("item", ContainerDS::BTree))
        .unwrap();
    table_info.insert(Table::Item, c_id);
    println!("Item container id: {}", c_id);

    // Warehouse table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("warehouse", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::Warehouse, c_id);
    println!("Warehouse container id: {}", c_id);

    // Stock table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("stock", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::Stock, c_id);
    println!("Stock container id: {}", c_id);

    // District table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("district", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::District, c_id);
    println!("District container id: {}", c_id);

    // Customer table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("customer", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::Customer, c_id);
    println!("Customer container id: {}", c_id);

    // Customer secondary index
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::secondary("customer_secondary", ContainerDS::BTree, c_id),
        )
        .unwrap();
    table_info.insert(Table::CustomerSecondary, c_id);
    println!("Customer secondary container id: {}", c_id);

    // Orders table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("order", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::Order, c_id);
    println!("Orders container id: {}", c_id);

    // Orders secondary index
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::secondary("order_secondary", ContainerDS::BTree, c_id),
        )
        .unwrap();
    table_info.insert(Table::OrderSecondary, c_id);
    println!("Orders secondary container id: {}", c_id);

    // OrderLine table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("orderline", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::OrderLine, c_id);
    println!("OrderLine container id: {}", c_id);

    // NewOrder table
    let c_id: u16 = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("neworder", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::NewOrder, c_id);
    println!("NewOrder container id: {}", c_id);

    // History table
    let c_id = txn_storage
        .create_container(
            DB_ID,
            ContainerOptions::primary("history", ContainerDS::BTree),
        )
        .unwrap();
    table_info.insert(Table::History, c_id);
    println!("History container id: {}", c_id);

    println!("======== Start Loading Tables ========");

    let time = std::time::Instant::now();
    load_item_table(txn_storage, &table_info);
    load_warehouse_table(txn_storage, &table_info, config);
    let elapsed = time.elapsed();
    println!(
        "======== Finished Loading Tables ========\nElapsed time: {:?}",
        elapsed
    );

    table_info
}
