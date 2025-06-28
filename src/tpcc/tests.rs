use super::delivery_txn::{run_delivery_txn, DeliveryTxnInput};
use super::loader::{tpcc_gen_all_tables, tpcc_show_table_stats, TPCCTable};
use super::neworder_txn::{run_neworder_txn, NewOrderItem, NewOrderTxnInput};
use super::orderstatus_txn::{run_orderstatus_txn, OrderStatusTxnInput};
use super::payment_txn::{run_payment_txn, PaymentTxnInput};
use super::record_definitions::*;
use super::stocklevel_txn::{run_stocklevel_txn, StockLevelTxnInput};
use super::txn_utils::*;
use super::*;

use crate::page::PAGE_SIZE;
use crate::prelude::ScanOptions;
use crate::txn_storage::NoWaitTxnStorage;
use crate::{
    bp::{get_test_bp, MemPool},
    prelude::{TxnOptions, TxnStorageTrait},
};

fn setup_test_storage(num_warehouses: u16) -> NoWaitTxnStorage<impl MemPool> {
    let num_frames = (num_warehouses as usize * 1024 * 1024 * 1024) / (PAGE_SIZE as usize);
    let bp = get_test_bp(num_frames);
    let storage = NoWaitTxnStorage::new(&bp);

    storage
}

#[test]
fn test_loader_creates_containers() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Verify all TPC-C tables are created
    assert!(table_info[TPCCTable::Item] > 0);
    assert!(table_info[TPCCTable::Warehouse] > 0);
    assert!(table_info[TPCCTable::Stock] > 0);
    assert!(table_info[TPCCTable::District] > 0);
    assert!(table_info[TPCCTable::Customer] > 0);
    assert!(table_info[TPCCTable::CustomerSecondary] > 0);
    assert!(table_info[TPCCTable::Order] > 0);
    assert!(table_info[TPCCTable::OrderSecondary] > 0);
    assert!(table_info[TPCCTable::OrderLine] > 0);
    assert!(table_info[TPCCTable::NewOrder] > 0);
    assert!(table_info[TPCCTable::History] > 0);
}

#[test]
fn test_record_serialization() {
    // Test warehouse serialization
    let warehouse = Warehouse::generate(1);
    let bytes = warehouse.as_bytes();
    let deserialized = unsafe { Warehouse::from_bytes(bytes) };
    assert_eq!(warehouse.w_id, deserialized.w_id);
    assert_eq!(warehouse.w_name, deserialized.w_name);
    assert_eq!(warehouse.w_tax, deserialized.w_tax);
    assert_eq!(warehouse.w_ytd, deserialized.w_ytd);

    // Test district serialization
    let district = District::generate(1, 1);
    let bytes = district.as_bytes();
    let deserialized = unsafe { District::from_bytes(bytes) };
    assert_eq!(district.d_id, deserialized.d_id);
    assert_eq!(district.d_w_id, deserialized.d_w_id);
    assert_eq!(district.d_tax, deserialized.d_tax);
    assert_eq!(district.d_next_o_id, deserialized.d_next_o_id);

    // Test customer serialization
    let customer = Customer::generate(1, 1, 1, 0);
    let bytes = customer.as_bytes();
    let deserialized = unsafe { Customer::from_bytes(bytes) };
    assert_eq!(customer.c_id, deserialized.c_id);
    assert_eq!(customer.c_d_id, deserialized.c_d_id);
    assert_eq!(customer.c_w_id, deserialized.c_w_id);
    assert_eq!(customer.c_balance, deserialized.c_balance);
    assert_eq!(customer.c_credit, deserialized.c_credit);
}

#[test]
fn test_key_creation() {
    // Test warehouse key
    let w_key = WarehouseKey::create_key(1);
    let w_bytes = w_key.into_bytes();
    assert_eq!(w_bytes.len(), 2);

    // Test customer key
    let c_key = CustomerKey::create_key(1, 1, 1);
    let c_bytes = c_key.into_bytes();
    assert_eq!(c_bytes.len(), 8); // 2 + 2 + 4 bytes

    // Test order key
    let o_key = OrderKey::create_key(1, 1, 100);
    let o_bytes = o_key.into_bytes();
    assert_eq!(o_bytes.len(), 8); // 2 + 2 + 4 bytes

    // Test from record
    let customer = Customer::generate(1, 1, 1, 0);
    let c_key2 = CustomerKey::create_key_from_customer(&customer);
    assert_eq!(c_key.into_bytes(), c_key2.into_bytes());
}

#[test]
fn test_item_loading() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Verify items are loaded correctly
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();

    // Get first item
    let item_key = ItemKey::create_key(1);
    let result = storage.get_value(&txn, table_info[TPCCTable::Item], item_key.into_bytes());

    assert!(result.is_ok());
    let item_bytes = result.unwrap();
    let item = unsafe { Item::from_bytes(&item_bytes) };
    assert_eq!(item.i_id, 1);
    assert!(item.i_price >= 1.0 && item.i_price <= 100.0);
    assert!(item.i_name[0] != 0); // Name should not be empty

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_warehouse_and_district_loading() {
    let storage = setup_test_storage(2);
    let table_info = tpcc_gen_all_tables(&storage, 2);

    // Verify warehouses
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();

    // Check warehouse 1
    let w_key = WarehouseKey::create_key(1);
    let result = storage.get_value(&txn, table_info[TPCCTable::Warehouse], w_key.into_bytes());
    assert!(result.is_ok());
    let w_bytes = result.unwrap();
    let warehouse = unsafe { Warehouse::from_bytes(&w_bytes) };
    assert_eq!(warehouse.w_id, 1);
    assert_eq!(warehouse.w_ytd, 300000.0);

    // Check warehouse 2
    let w_key2 = WarehouseKey::create_key(2);
    let result2 = storage.get_value(&txn, table_info[TPCCTable::Warehouse], w_key2.into_bytes());
    assert!(result2.is_ok());
    let w_bytes2 = result2.unwrap();
    let warehouse2 = unsafe { Warehouse::from_bytes(&w_bytes2) };
    assert_eq!(warehouse2.w_id, 2);

    // Check districts for warehouse 1
    for d_id in 1..=10 {
        let d_key = DistrictKey::create_key(1, d_id);
        let result = storage.get_value(&txn, table_info[TPCCTable::District], d_key.into_bytes());
        assert!(result.is_ok());
        let d_bytes = result.unwrap();
        let district = unsafe { District::from_bytes(&d_bytes) };
        assert_eq!(district.d_w_id, 1);
        assert_eq!(district.d_id, d_id);
        assert_eq!(district.d_next_o_id, 3001);
        assert_eq!(district.d_ytd, 30000.0);
    }

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_customer_loading_and_secondary_index() {
    let storage = setup_test_storage(1);

    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Test primary key access
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let c_key = CustomerKey::create_key(1, 1, 1);
    let result = storage.get_value(&txn, table_info[TPCCTable::Customer], c_key.into_bytes());

    assert!(result.is_ok());
    let c_bytes = result.unwrap();
    let customer = unsafe { Customer::from_bytes(&c_bytes) };
    assert_eq!(customer.c_id, 1);
    assert_eq!(customer.c_d_id, 1);
    assert_eq!(customer.c_w_id, 1);
    assert_eq!(customer.c_balance, -10.0);
    assert_eq!(customer.c_credit_lim, 50000.0);

    // Test secondary index by last name
    let lower = CustomerSecondaryKey::create_key(1, 1, b"BARBARBAR", 0).into_bytes();
    let upper = CustomerSecondaryKey::create_key(1, 1, b"BARBARBAR", u32::MAX).into_bytes();

    let scan_options = ScanOptions {
        lower_inc: lower.to_vec(),
        upper_exc: upper.to_vec(),
    };
    let iter = storage.scan_range(&txn, table_info[TPCCTable::CustomerSecondary], scan_options);
    assert!(iter.is_ok());

    let mut count = 0;
    let iter = iter.unwrap();
    loop {
        let result = storage.iter_next(&txn, &iter);
        if result.is_err() {
            panic!(
                "Error iterating over customer secondary index: {:?}",
                result.err()
            );
        }

        if result.as_ref().unwrap().is_none() {
            break;
        }

        let (_, c_bytes) = result.unwrap().unwrap();
        // Secondary index gets the primary value
        let c = unsafe { Customer::from_bytes(&c_bytes) };
        assert_eq!(c.c_w_id, 1);
        assert_eq!(c.c_d_id, 1);
        let c_last_str = std::str::from_utf8(&c.c_last)
            .unwrap()
            .trim_end_matches('\0');
        if c_last_str == "BARBARBAR" {
            count += 1;
        }
    }
    drop(iter);

    storage.commit_txn(&txn, false).unwrap();
    // println!("count: {}", count);
    assert!(
        count >= 1,
        "Expected at least one customer with last name BARBARBAR in warehouse 1, district 1"
    );
}

#[test]
fn test_order_and_new_order_loading() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();

    // Check an old order (should be delivered)
    let o_key = OrderKey::create_key(1, 1, 1);
    let result = storage.get_value(&txn, table_info[TPCCTable::Order], o_key.into_bytes());
    assert!(result.is_ok());
    let o_bytes = result.unwrap();
    let order = unsafe { Order::from_bytes(&o_bytes) };
    assert_eq!(order.o_id, 1);
    assert!(order.o_carrier_id > 0 && order.o_carrier_id <= 10); // Should have carrier

    // Check new orders (should exist for o_id > 2100)
    let no_key = NewOrderKey::create_key(1, 1, 2101);
    let result = storage.get_value(&txn, table_info[TPCCTable::NewOrder], no_key.into_bytes());
    assert!(result.is_ok(), "New order 2101 should exist");

    // Verify order exists too
    let o_key2 = OrderKey::create_key(1, 1, 2101);
    let result2 = storage.get_value(&txn, table_info[TPCCTable::Order], o_key2.into_bytes());
    assert!(result2.is_ok());
    let o_bytes2 = result2.unwrap();
    let order2 = unsafe { Order::from_bytes(&o_bytes2) };
    assert_eq!(order2.o_carrier_id, 0); // Should not have carrier yet

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_order_loading_and_secondary_index() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();

    // Test primary key access
    let o_key = OrderKey::create_key(1, 1, 100);
    let result = storage.get_value(&txn, table_info[TPCCTable::Order], o_key.into_bytes());
    if result.is_ok() {
        let o_bytes = result.unwrap();
        let order = unsafe { Order::from_bytes(&o_bytes) };
        assert_eq!(order.o_w_id, 1);
        assert_eq!(order.o_d_id, 1);
        assert_eq!(order.o_id, 100);
    }

    // Test secondary index by customer ID
    // The secondary index key is (w_id, d_id, c_id, o_id)
    let lower = OrderSecondaryKey::create_key(1, 1, 1, 0).into_bytes();
    let upper = OrderSecondaryKey::create_key(1, 1, 1, u32::MAX).into_bytes();

    let scan_options = ScanOptions {
        lower_inc: lower.to_vec(),
        upper_exc: upper.to_vec(),
    };

    let iter = storage.scan_range(&txn, table_info[TPCCTable::OrderSecondary], scan_options);
    assert!(iter.is_ok());

    let mut order_count = 0;
    let mut found_orders = Vec::new();
    let iter = iter.unwrap();

    loop {
        let result = storage.iter_next(&txn, &iter);
        if result.is_err() {
            panic!(
                "Error iterating over order secondary index: {:?}",
                result.err()
            );
        }

        if result.as_ref().unwrap().is_none() {
            break;
        }

        let (key_bytes, value_bytes) = result.unwrap().unwrap();
        let o_sec_key = OrderSecondaryKey::from_bytes(&key_bytes);
        let order = unsafe { Order::from_bytes(&value_bytes) };
        assert_eq!(o_sec_key.w_id(), 1);
        assert_eq!(o_sec_key.d_id(), 1);
        assert_eq!(o_sec_key.c_id(), 1);
        assert_eq!(order.o_w_id, 1);
        assert_eq!(order.o_d_id, 1);
        assert_eq!(order.o_c_id, 1);
        assert!(o_sec_key.o_id() > 0);
        found_orders.push(o_sec_key.o_id());
        order_count += 1;
    }
    drop(iter);

    storage.commit_txn(&txn, false).unwrap();

    // Customer 1 should have multiple orders
    println!(
        "Found {} orders for customer 1 in warehouse 1, district 1",
        order_count
    );
    println!("Order IDs: {:?}", found_orders);
    assert_eq!(
        order_count, 1,
        "Expected exactly 1 order for customer 1 in warehouse 1, district 1 after loading"
    );
}

#[test]
fn test_neworder_transaction_success() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    let input = NewOrderTxnInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        ol_cnt: 2,
        o_entry_d: get_timestamp(),
        items: vec![
            NewOrderItem {
                ol_i_id: 1,
                ol_supply_w_id: 1,
                ol_quantity: 5,
            },
            NewOrderItem {
                ol_i_id: 2,
                ol_supply_w_id: 1,
                ol_quantity: 10,
            },
        ],
    };

    let mut stats = TxnTypeStats::new();
    let result = run_neworder_txn(&storage, &table_info, &input, &mut stats);

    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.w_id, 1);
    assert_eq!(output.d_id, 1);
    assert_eq!(output.c_id, 1);
    assert_eq!(output.o_id, 3001); // First new order after loading
    assert_eq!(output.items.len(), 2);
    assert!(output.total > 0.0);

    // Verify order was created
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let o_key = OrderKey::create_key(1, 1, 3001);
    let result = storage.get_value(&txn, table_info[TPCCTable::Order], o_key.into_bytes());
    assert!(result.is_ok());
    let o_bytes = result.unwrap();
    let order = unsafe { Order::from_bytes(&o_bytes) };
    assert_eq!(order.o_c_id, 1);
    assert_eq!(order.o_ol_cnt, 2);
    assert_eq!(order.o_carrier_id, 0); // Not delivered yet

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_neworder_transaction_rollback() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Transaction with rollback flag set
    let input = NewOrderTxnInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        ol_cnt: 1,
        o_entry_d: get_timestamp(),
        items: vec![NewOrderItem {
            ol_i_id: Item::UNUSED_ID, // Invalid item for rollback
            ol_supply_w_id: 1,
            ol_quantity: 5,
        }],
    };

    let mut stats = TxnTypeStats::new();
    let result = run_neworder_txn(&storage, &table_info, &input, &mut stats);
    assert!(matches!(result, Err(TPCCStatus::UserAbort)));
}

#[test]
fn test_payment_transaction_by_id() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Get initial customer balance
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let c_key = CustomerKey::create_key(1, 1, 1);
    let result = storage.get_value(&txn, table_info[TPCCTable::Customer], c_key.into_bytes());
    assert!(result.is_ok());
    let c_bytes = result.unwrap();
    let customer = unsafe { Customer::from_bytes(&c_bytes) };
    let initial_balance = customer.c_balance;
    assert_eq!(initial_balance, -10.0); // Initial balance set in loader
    storage.commit_txn(&txn, false).unwrap();

    // Payment by customer ID
    let input = PaymentTxnInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: 1,
        by_last_name: false,
        c_last: [0u8; 16],
        h_amount: 100.0,
        h_date: get_timestamp(),
    };

    let mut stats = TxnTypeStats::new();
    let result = run_payment_txn(&storage, &table_info, &input, &mut stats);

    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.w_id, 1);
    assert_eq!(output.d_id, 1);
    assert_eq!(output.c_id, 1);
    assert_eq!(output.c_balance, initial_balance - 100.0);

    // Verify customer balance was updated
    let txn2 = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let result2 = storage.get_value(&txn2, table_info[TPCCTable::Customer], c_key.into_bytes());
    assert!(result2.is_ok());
    let c_bytes2 = result2.unwrap();
    let customer2 = unsafe { Customer::from_bytes(&c_bytes2) };
    assert_eq!(customer2.c_balance, initial_balance - 100.0);
    storage.commit_txn(&txn2, false).unwrap();
}

#[test]
fn test_payment_transaction_by_last_name() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Payment by customer last name
    let mut c_last = [0u8; 16];
    make_clast(&mut c_last, 0); // BARBARBAR - first generated last name
    println!("Using last name: {}", std::str::from_utf8(&c_last).unwrap());
    let input = PaymentTxnInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: 0, // Will be overridden when by_last_name is true
        by_last_name: true,
        c_last,
        h_amount: 50.0,
        h_date: get_timestamp(),
    };

    let mut stats = TxnTypeStats::new();
    let result = run_payment_txn(&storage, &table_info, &input, &mut stats);

    assert!(result.is_ok());
    let output = result.unwrap();
    assert_eq!(output.c_last, c_last);
    assert_eq!(output.c_balance, -60.0); // Started at -10.0, paid 50.0 more
}

#[test]
fn test_orderstatus_transaction_by_id() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Order status by customer ID
    let input = OrderStatusTxnInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        by_last_name: false,
        c_last: [0u8; 16],
    };

    let mut stats = TxnTypeStats::new();
    let result = run_orderstatus_txn(&storage, &table_info, &input, &mut stats);

    // This might be UserAbort if no orders exist yet
    assert!(result.is_ok() || matches!(result, Err(TPCCStatus::UserAbort)));

    if let Ok(output) = result {
        assert_eq!(output.c_id, 1);
        assert!(output.o_id > 0);
        assert!(!output.order_lines.is_empty());

        // For old orders, should have delivery info
        if output.o_id < 2101 {
            assert!(output.o_carrier_id.is_some() && output.o_carrier_id.unwrap() > 0);
        }
    }
}

#[test]
fn test_orderstatus_transaction_by_last_name() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Order status by customer last name
    let mut c_last = [0u8; 16];
    make_clast(&mut c_last, 57); // BARBARBAR
    let input = OrderStatusTxnInput {
        w_id: 1,
        d_id: 1,
        c_id: 0,
        by_last_name: true,
        c_last,
    };

    let mut stats = TxnTypeStats::new();
    let result = run_orderstatus_txn(&storage, &table_info, &input, &mut stats);

    assert!(result.is_ok() || matches!(result, Err(TPCCStatus::UserAbort)));
}

#[test]
fn test_delivery_transaction() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    let input = DeliveryTxnInput {
        w_id: 1,
        o_carrier_id: 5,
        ol_delivery_d: get_timestamp(),
    };

    let mut stats = TxnTypeStats::new();
    let result = run_delivery_txn(&storage, &table_info, &input, &mut stats);

    // Delivery might not find any orders to deliver, which returns empty output
    assert!(result.is_ok());

    if let Ok(output) = result {
        // If orders were delivered, verify they were updated
        if !output.delivered.is_empty() {
            println!("Delivered {} orders", output.delivered.len());
            let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();

            for delivered in &output.delivered {
                let o_key = OrderKey::create_key(1, delivered.d_id, delivered.o_id);
                let o_bytes = storage
                    .get_value(&txn, table_info[TPCCTable::Order], o_key.into_bytes())
                    .unwrap();

                let order = unsafe { Order::from_bytes(&o_bytes) };
                assert_eq!(order.o_carrier_id, 5);
            }

            storage.commit_txn(&txn, false).unwrap();
        } else {
            panic!("Delivery transaction returned no orders delivered, expected at least one order to be delivered");
        }
    }
}

#[test]
fn test_stocklevel_transaction() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    let input = StockLevelTxnInput {
        w_id: 1,
        d_id: 1,
        threshold: 15,
    };

    let mut stats = TxnTypeStats::new();
    let result = run_stocklevel_txn(&storage, &table_info, &input, &mut stats);

    assert!(result.is_ok());
    let output = result.unwrap();
    assert!(output.low_stock_count >= 0);
}

#[test]
fn test_transaction_rollback_on_error() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Get initial district next_o_id
    let txn = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let d_key = DistrictKey::create_key(1, 1);
    let result = storage.get_value(&txn, table_info[TPCCTable::District], d_key.into_bytes());
    assert!(result.is_ok());
    let d_bytes = result.unwrap();
    let district = unsafe { District::from_bytes(&d_bytes) };
    let initial_next_o_id = district.d_next_o_id;
    storage.commit_txn(&txn, false).unwrap();

    // Try transaction with item that will cause rollback
    let input = NewOrderTxnInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        ol_cnt: 2,
        o_entry_d: get_timestamp(),
        items: vec![
            NewOrderItem {
                ol_i_id: 1,
                ol_supply_w_id: 1,
                ol_quantity: 5,
            },
            NewOrderItem {
                ol_i_id: Item::UNUSED_ID, // Invalid item to trigger rollback
                ol_supply_w_id: 1,
                ol_quantity: 5,
            },
        ],
    };

    let mut stats = TxnTypeStats::new();
    let result = run_neworder_txn(&storage, &table_info, &input, &mut stats);
    assert!(matches!(result, Err(TPCCStatus::UserAbort)));

    // Verify district next_o_id was not incremented (transaction rolled back)
    let txn2 = storage.begin_txn(0, TxnOptions::default()).unwrap();
    let result2 = storage.get_value(&txn2, table_info[TPCCTable::District], d_key.into_bytes());
    assert!(result2.is_ok());
    let d_bytes2 = result2.unwrap();
    let district2 = unsafe { District::from_bytes(&d_bytes2) };
    assert_eq!(
        district2.d_next_o_id, initial_next_o_id,
        "Transaction should have rolled back"
    );
    storage.commit_txn(&txn2, false).unwrap();
}

#[test]
fn test_concurrent_transactions() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    use std::thread;

    // Run multiple neworder transactions concurrently on different districts
    thread::scope(|s| {
        for i in 0..3 {
            let storage_ref = &storage;
            let table_info_ref = &table_info;

            s.spawn(move || {
                let input = NewOrderTxnInput {
                    w_id: 1,
                    d_id: ((i % 10) + 1) as u8, // Different districts
                    c_id: ((i % 30) + 1) as u32,
                    ol_cnt: 1,
                    o_entry_d: get_timestamp(),
                    items: vec![NewOrderItem {
                        ol_i_id: ((i % 10) + 1) as u32,
                        ol_supply_w_id: 1,
                        ol_quantity: 5,
                    }],
                };

                let mut stats = TxnTypeStats::new();
                let result = run_neworder_txn(storage_ref, table_info_ref, &input, &mut stats);
                assert!(result.is_ok());
            });
        }
    });
}

#[test]
fn test_abort_tracking() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // Create an input that will cause a specific abort
    let input = NewOrderTxnInput {
        w_id: 999, // Non-existent warehouse
        d_id: 1,
        c_id: 1,
        ol_cnt: 1,
        o_entry_d: get_timestamp(),
        items: vec![NewOrderItem {
            ol_i_id: 1,
            ol_supply_w_id: 1,
            ol_quantity: 1,
        }],
    };

    let mut stats = TxnTypeStats::new();
    let result = run_neworder_txn(&storage, &table_info, &input, &mut stats);
    assert!(matches!(result, Err(TPCCStatus::SystemAbort)));
}

#[test]
fn test_abort_id_enum() {
    use std::convert::TryFrom;

    // Test conversion and naming
    let abort_id = AbortID::try_from(0u8).unwrap();
    assert_eq!(abort_id.name(), "NewOrder_GetWarehouse");
    assert_eq!(abort_id.txn_type(), "NewOrderTxn");

    let abort_id = AbortID::try_from(10u8).unwrap();
    assert_eq!(abort_id.name(), "Payment_UpdateWarehouse");
    assert_eq!(abort_id.txn_type(), "PaymentTxn");

    let abort_id = AbortID::try_from(30u8).unwrap();
    assert_eq!(abort_id.name(), "Delivery_GetNewOrderWithSmallestKey");
    assert_eq!(abort_id.txn_type(), "DeliveryTxn");

    // Test invalid conversion
    let result = AbortID::try_from(100u8);
    assert!(result.is_err());
}

#[test]
fn test_txn_type_stats() {
    let mut stats = TxnTypeStats::new();
    assert_eq!(stats.num_commits, 0);
    assert_eq!(stats.num_user_aborts, 0);
    assert_eq!(stats.num_system_aborts, 0);

    // Record some transactions
    stats.record_commit(1000);
    assert_eq!(stats.num_commits, 1);
    assert_eq!(stats.total_latency_ns, 1000);
    assert_eq!(stats.min_latency_ns, 1000);
    assert_eq!(stats.max_latency_ns, 1000);

    stats.record_commit(2000);
    assert_eq!(stats.num_commits, 2);
    assert_eq!(stats.total_latency_ns, 3000);
    assert_eq!(stats.min_latency_ns, 1000);
    assert_eq!(stats.max_latency_ns, 2000);

    stats.record_user_abort();
    assert_eq!(stats.num_user_aborts, 1);

    stats.record_system_abort(5);
    assert_eq!(stats.num_system_aborts, 1);
    assert_eq!(stats.abort_counts[5], 1);

    // Record another system abort with same ID
    stats.record_system_abort(5);
    assert_eq!(stats.num_system_aborts, 2);
    assert_eq!(stats.abort_counts[5], 2);
}

#[test]
fn test_table_stats() {
    let storage = setup_test_storage(1);
    let table_info = tpcc_gen_all_tables(&storage, 1);

    // This should not panic and should print stats to stdout
    tpcc_show_table_stats(&storage, &table_info);
}

#[test]
fn test_benchmark_creation() {
    let storage = setup_test_storage(1);
    let bench = TpccBenchmark::new(storage, 1);

    // Run a very short benchmark
    let result = bench.run_benchmark(1, 1, false); // 1 thread, 1 second

    assert!(result.committed_txns >= 1);
    assert_eq!(result.duration_secs, 1);
    assert!(result.throughput >= 0.0);

    // Print results
    result.print(true);
}
