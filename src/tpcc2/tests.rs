use super::delivery_txn::{run_delivery_txn, DeliveryInput};
use super::loader::{TpccContainerIds, TpccLoader};
use super::neworder_txn::{run_neworder_txn, NewOrderInput, NewOrderItem};
use super::orderstatus_txn::{run_orderstatus_txn, OrderStatusInput};
use super::payment_txn::{run_payment_txn, PaymentInput};
use super::record_definitions::*;
use super::stocklevel_txn::{run_stocklevel_txn, StockLevelInput};
use super::txn_utils::*;
use super::*;

use crate::{
    bp::{get_test_bp_lru, BufferPoolLRU, DatabaseId},
    tpcc::record_definitions::Address,
    txn_storage2::{
        field::Field,
        field_level_storage_trait::{FieldLeveLStorageTrait, ScanOptions, TxnOptions},
        schema::Schema,
        transactional_storage::TransactionalStorage,
    },
};
use std::sync::Arc;

type TestBP = BufferPoolLRU;

use super::loader::PartitionMode;

fn setup_test_warehouse(
    num_warehouses: u16,
) -> (
    Arc<TransactionalStorage<TestBP>>,
    DatabaseId,
    TpccContainerIds,
) {
    setup_test_warehouse_with_mode(num_warehouses, PartitionMode::HotCold)
}

fn setup_test_warehouse_with_mode(
    num_warehouses: u16,
    mode: PartitionMode,
) -> (
    Arc<TransactionalStorage<TestBP>>,
    DatabaseId,
    TpccContainerIds,
) {
    let bp = get_test_bp_lru(1000);
    let loader = TpccLoader::with_partition_mode(bp, mode);
    let storage: Arc<TransactionalStorage<BufferPoolLRU>> = loader.get_storage();
    let db_id = loader.get_db_id();
    let containers = loader.get_container_ids();

    // Load minimal test data
    loader.load_items(100); // Load only 100 items for tests
    for w_id in 1..=num_warehouses {
        loader.load_warehouse(w_id);
    }

    (storage, db_id, containers)
}

#[test]
fn test_loader_creates_containers_hotcold() {
    let bp = get_test_bp_lru(100);
    let loader = TpccLoader::with_partition_mode(bp, PartitionMode::HotCold);
    let storage = loader.get_storage();
    let db_id = loader.get_db_id();

    let container_list = storage.list_containers(db_id).unwrap();
    assert_eq!(container_list.len(), 12); // 12 tables (includes customer_hot)

    let names: Vec<String> = container_list
        .iter()
        .map(|(_, opts)| opts.name().clone())
        .collect();
    assert!(names.contains(&"customer".to_string()));
    assert!(names.contains(&"customer_hot".to_string()));
}

#[test]
fn test_loader_creates_containers_fullrow() {
    let bp = get_test_bp_lru(100);
    let loader = TpccLoader::with_partition_mode(bp, PartitionMode::FullRow);
    let storage = loader.get_storage();
    let db_id = loader.get_db_id();

    let container_list = storage.list_containers(db_id).unwrap();
    assert_eq!(container_list.len(), 11); // 11 tables (no customer_hot)
}

#[test]
fn test_item_loading() {
    let (storage, db_id, containers) = setup_test_warehouse(0);

    // Verify items were loaded
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Check first item
    let key = vec![Field::Uint32(Some(1))];
    let (fields, _) = storage
        .get_fields(&txn, containers.item_cid, key, &[0, 2, 3], None)
        .unwrap();

    assert_eq!(fields.len(), 3);
    if let Field::Uint32(Some(i_id)) = &fields[0] {
        assert_eq!(*i_id, 1);
    } else {
        panic!("Expected Uint32 for i_id");
    }

    // Verify price is in valid range
    if let Field::Float64(Some(price)) = &fields[1] {
        assert!(*price >= 1.0 && *price <= 100.0);
    } else {
        panic!("Expected Float64 for i_price");
    }

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_warehouse_and_district_loading() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Check warehouse
    let w_key = vec![Field::Uint16(Some(1))];
    let (w_fields, _) = storage
        .get_fields(&txn, containers.warehouse_cid, w_key, &[0, 2], None)
        .unwrap();

    if let Field::Uint16(Some(w_id)) = &w_fields[0] {
        assert_eq!(*w_id, 1);
    }
    if let Field::Float64(Some(w_ytd)) = &w_fields[1] {
        assert_eq!(*w_ytd, 300000.0);
    }

    // Check districts
    for d_id in 1..=10 {
        let d_key = vec![Field::Uint16(Some(1)), Field::Uint8(Some(d_id))];
        let (d_fields, _) = storage
            .get_fields(&txn, containers.district_cid, d_key, &[1, 2], None)
            .unwrap();

        if let Field::Uint8(Some(id)) = &d_fields[0] {
            assert_eq!(*id, d_id);
        }
        if let Field::Uint32(Some(next_o_id)) = &d_fields[1] {
            assert_eq!(*next_o_id, 3001);
        }
    }

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_customer_loading_and_secondary_index() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Check customer by primary key — cold container has keys + stable fields
    let c_key = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::Uint32(Some(1)),
    ];
    let (c_cold_fields, _) = storage
        .get_fields(
            &txn,
            containers.customer_cid,
            c_key.clone(),
            &[2], // c_id in cold schema
            None,
        )
        .unwrap();

    if let Field::Uint32(Some(c_id)) = &c_cold_fields[0] {
        assert_eq!(*c_id, 1);
    }

    // Check customer hot container for balance
    let (c_hot_fields, _) = storage
        .get_fields(
            &txn,
            containers.customer_hot_cid,
            c_key,
            &[3], // c_balance in hot schema
            None,
        )
        .unwrap();

    if let Field::Float64(Some(balance)) = &c_hot_fields[0] {
        assert_eq!(*balance, -10.0);
    }

    // Test secondary index lookup
    let last_name = "BARBARBAR"; // First generated last name
    let scan_start = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::String(Some(last_name.to_string())),
        Field::Uint32(Some(0)),
    ];
    let scan_end = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::String(Some(last_name.to_string())),
        Field::Uint32(Some(u32::MAX)),
    ];

    let iter = storage
        .scan_range(
            &txn,
            containers.customer_secondary_cid,
            ScanOptions::new(&[]).with_bounds(scan_start, scan_end),
        )
        .unwrap();

    let mut found = false;
    while let Ok(Some((key_fields, _, _))) = storage.iter_next(&txn, &iter) {
        if key_fields.len() >= 4 {
            if let Field::Uint32(Some(c_id)) = &key_fields[3] {
                assert_eq!(*c_id, 1); // First customer should have this last name
                found = true;
                break;
            }
        }
    }
    assert!(
        found,
        "Customer with last name {} not found in secondary index",
        last_name
    );

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_order_and_new_order_loading() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // Check an order
    let o_key = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::Uint32(Some(1)),
    ];
    let (o_fields, _) = storage
        .get_fields(&txn, containers.order_cid, o_key, &[2, 4, 5], None)
        .unwrap();

    if let Field::Uint32(Some(o_id)) = &o_fields[0] {
        assert_eq!(*o_id, 1);
    }
    if let Field::Uint8(Some(carrier)) = &o_fields[1] {
        assert!(*carrier >= 1 && *carrier <= 10); // Should have carrier for old orders
    }

    // Check new orders (should exist for o_id > 2100)
    let no_key = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::Uint32(Some(2101)),
    ];

    let no_result = storage.get_fields(&txn, containers.new_order_cid, no_key, &[2], None);
    assert!(no_result.is_ok(), "New order 2101 should exist");

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_neworder_transaction_success() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = NewOrderInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        items: vec![
            NewOrderItem {
                i_id: 1,
                supply_w_id: 1,
                quantity: 5,
            },
            NewOrderItem {
                i_id: 2,
                supply_w_id: 1,
                quantity: 10,
            },
        ],
        rollback: false,
    };

    let result = run_neworder_txn(&storage, db_id, &containers, &input).unwrap();

    println!("New order transaction completed successfully!");
    // Verify output
    assert_eq!(result.o_id, 3001); // First new order after loading
    assert_eq!(result.items.len(), 2);
    assert!(result.total_amount > 0.0);

    println!("Verifying order creation...");
    // Verify order was created
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let o_key = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::Uint32(Some(3001)),
    ];
    let (o_fields, _) = storage
        .get_fields(&txn, containers.order_cid, o_key, &[3, 5], None)
        .unwrap();

    if let Field::Uint32(Some(c_id)) = &o_fields[0] {
        assert_eq!(*c_id, 1);
    }
    if let Field::Uint8(Some(ol_cnt)) = &o_fields[1] {
        assert_eq!(*ol_cnt, 2);
    }

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_neworder_transaction_invalid_item() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = NewOrderInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        items: vec![
            NewOrderItem {
                i_id: 99999,
                supply_w_id: 1,
                quantity: 5,
            }, // Invalid item
        ],
        rollback: false,
    };

    let result = run_neworder_txn(&storage, db_id, &containers, &input);
    assert!(result.is_err());
}

#[test]
fn test_payment_transaction_by_id() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    // Get initial customer balance from hot container
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let c_key = vec![
        Field::Uint16(Some(1)),
        Field::Uint8(Some(1)),
        Field::Uint32(Some(1)),
    ];
    let (c_fields, _) = storage
        .get_fields(
            &txn,
            containers.customer_hot_cid,
            c_key.clone(),
            &[3], // c_balance in hot schema
            None,
        )
        .unwrap();
    let initial_balance = if let Field::Float64(Some(bal)) = &c_fields[0] {
        bal
    } else {
        panic!("Expected Float64 for balance");
    };
    storage.commit_txn(&txn, false).unwrap();

    // Run payment
    let input = PaymentInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: Some(1),
        c_last: None,
        h_amount: 100.0,
    };

    let result = run_payment_txn(&storage, db_id, &containers, &input).unwrap();

    assert_eq!(result.c_id, 1);
    assert_eq!(result.c_balance, *initial_balance - 100.0);

    // // Verify history record was created
    // let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    // // Scan history table for our record
    // let iter = storage
    //     .scan_range(&txn2, containers.history_cid, ScanOptions::new())
    //     .unwrap();
    // let mut found_history = false;

    // while let Ok(Some((_, fields, _))) = storage.iter_next(&txn2, &iter) {
    //     if fields.len() >= 7 {
    //         if let (Field::Uint32(Some(c_id)), Field::Float64(Some(amount))) =
    //             (&fields[2], &fields[6])
    //         {
    //             if *c_id == 1 && *amount == 100.0 {
    //                 found_history = true;
    //                 break;
    //             }
    //         }
    //     }
    // }

    // assert!(found_history, "History record not found");
    // storage.commit_txn(&txn2, false).unwrap();
}

#[test]
fn test_payment_transaction_by_last_name() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = PaymentInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: None,
        c_last: Some("BARBARBAR".to_string()), // First generated last name
        h_amount: 50.0,
    };

    let result = run_payment_txn(&storage, db_id, &containers, &input).unwrap();

    assert_eq!(result.c_last, "BARBARBAR");
    assert!(result.c_balance < 0.0); // Started at -10.0, paid 50.0
}

#[test]
fn test_orderstatus_transaction() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = OrderStatusInput {
        w_id: 1,
        d_id: 1,
        c_id: Some(1),
        c_last: None,
    };

    let result = run_orderstatus_txn(&storage, db_id, &containers, &input).unwrap();

    assert_eq!(result.c_id, 1);
    assert!(result.o_id > 0);
    assert!(!result.order_lines.is_empty());

    // For old orders, should have delivery info
    if result.o_id < 2101 {
        assert!(result.o_carrier_id.is_some());
        assert!(result.order_lines[0].ol_delivery_d.is_some());
    }
}

#[test]
fn test_delivery_transaction() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = DeliveryInput {
        w_id: 1,
        o_carrier_id: 5,
    };

    let result = run_delivery_txn(&storage, db_id, &containers, &input).unwrap();

    // Should deliver orders from multiple districts
    assert!(!result.delivered_orders.is_empty());

    // Verify orders were updated
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    for delivered in &result.delivered_orders {
        let o_key = vec![
            Field::Uint16(Some(1)),
            Field::Uint8(Some(delivered.d_id)),
            Field::Uint32(Some(delivered.o_id)),
        ];

        let (o_fields, _) = storage
            .get_fields(&txn, containers.order_cid, o_key, &[4], None)
            .unwrap();

        if let Field::Uint8(Some(carrier)) = &o_fields[0] {
            assert_eq!(*carrier, 5);
        }
    }

    storage.commit_txn(&txn, false).unwrap();
}

#[test]
fn test_stocklevel_transaction() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    let input = StockLevelInput {
        w_id: 1,
        d_id: 1,
        threshold: 15,
    };

    let _ = run_stocklevel_txn(&storage, db_id, &containers, &input).unwrap();

    // Should find some low stock items
    // assert!(result.low_stock_count >= 0);
}

#[test]
fn test_concurrent_transactions() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    use std::thread;

    let storage = Arc::new(storage);
    let mut handles = vec![];

    // Run multiple transactions concurrently
    for i in 0..3 {
        let storage_clone = Arc::clone(&storage);
        let containers_copy = containers;

        let handle = thread::spawn(move || {
            let input = NewOrderInput {
                w_id: 1,
                d_id: ((i % 10) + 1) as u8,
                c_id: ((i % 30) + 1) as u32,
                items: vec![NewOrderItem {
                    i_id: ((i % 10) + 1) as u32,
                    supply_w_id: 1,
                    quantity: 5,
                }],
                rollback: false,
            };

            run_neworder_txn(&storage_clone, db_id, &containers_copy, &input)
        });

        handles.push(handle);
    }

    // All should succeed without conflicts (different districts)
    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result.is_ok());
    }
}

#[test]
fn test_transaction_rollback_on_error() {
    let (storage, db_id, containers) = setup_test_warehouse(1);

    // Get initial district next_o_id
    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let d_key = vec![Field::Uint16(Some(1)), Field::Uint8(Some(1))];
    let (d_fields, _) = storage
        .get_fields(&txn, containers.district_cid, d_key.clone(), &[2], None)
        .unwrap();
    let initial_next_o_id = if let Field::Uint32(Some(id)) = &d_fields[0] {
        id
    } else {
        panic!("Expected Uint32 for next_o_id");
    };
    storage.commit_txn(&txn, false).unwrap();

    // Try to create order with invalid item
    let input = NewOrderInput {
        w_id: 1,
        d_id: 1,
        c_id: 1,
        items: vec![
            NewOrderItem {
                i_id: 1,
                supply_w_id: 1,
                quantity: 5,
            },
            NewOrderItem {
                i_id: 99999,
                supply_w_id: 1,
                quantity: 10,
            }, // Invalid
        ],
        rollback: false,
    };

    let result = run_neworder_txn(&storage, db_id, &containers, &input);
    assert!(result.is_err());

    // Verify district next_o_id was not incremented
    let txn2 = storage.begin_txn(db_id, TxnOptions::default()).unwrap();
    let (d_fields2, _) = storage
        .get_fields(&txn2, containers.district_cid, d_key, &[2], None)
        .unwrap();
    let final_next_o_id = if let Field::Uint32(Some(id)) = &d_fields2[0] {
        id
    } else {
        panic!("Expected Uint32 for next_o_id");
    };
    storage.commit_txn(&txn2, false).unwrap();

    assert_eq!(
        *initial_next_o_id, *final_next_o_id,
        "Transaction should have rolled back"
    );
}

#[test]
fn test_benchmark_hotcold() {
    let bp = get_test_bp_lru(500);
    let bench = TpccBenchmark::with_partition_mode(bp, 1, PartitionMode::HotCold);
    let result = bench.run_benchmark(2, 1, false);
    assert!(result.committed_txns > 0);
    assert!(result.throughput > 0.0);
}

#[test]
fn test_benchmark_fullrow() {
    let bp = get_test_bp_lru(500);
    let bench = TpccBenchmark::with_partition_mode(bp, 1, PartitionMode::FullRow);
    let result = bench.run_benchmark(2, 1, false);
    assert!(result.committed_txns > 0);
    assert!(result.throughput > 0.0);
}

#[test]
fn test_benchmark_field_level() {
    let bp = get_test_bp_lru(500);
    let bench = TpccBenchmark::with_partition_mode(bp, 1, PartitionMode::FieldLevel);
    let result = bench.run_benchmark(2, 1, false);
    assert!(result.committed_txns > 0);
    assert!(result.throughput > 0.0);
}

#[test]
fn test_payment_fullrow() {
    let (storage, db_id, containers) = setup_test_warehouse_with_mode(1, PartitionMode::FullRow);

    let input = PaymentInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: Some(1),
        c_last: None,
        h_amount: 100.0,
    };

    let result = run_payment_txn(&storage, db_id, &containers, &input).unwrap();
    assert_eq!(result.c_id, 1);
    assert_eq!(result.c_balance, -10.0 - 100.0);
}

#[test]
fn test_payment_field_level() {
    let (storage, db_id, containers) = setup_test_warehouse_with_mode(1, PartitionMode::FieldLevel);

    let input = PaymentInput {
        w_id: 1,
        d_id: 1,
        c_w_id: 1,
        c_d_id: 1,
        c_id: Some(1),
        c_last: None,
        h_amount: 100.0,
    };

    let result = run_payment_txn(&storage, db_id, &containers, &input).unwrap();
    assert_eq!(result.c_id, 1);
    assert_eq!(result.c_balance, -10.0 - 100.0);
}

#[test]
fn test_neworder_profile() {
    use super::neworder_txn::{run_neworder_txn_profiled, NewOrderProfile};
    use super::txn_helper::TxnTypeStats;
    use crate::bp::get_test_bp_clock;
    use std::sync::{atomic::AtomicBool, Arc};
    use std::thread;
    use std::time::Duration;

    // Use Clock BP (same as release benchmark) instead of LRU
    let num_frames = 1024 * 1024 * 1024 / crate::prelude::PAGE_SIZE; // 1GB
    let bp = get_test_bp_clock(num_frames);
    let loader = TpccLoader::with_partition_mode(bp, PartitionMode::FieldLevel);
    let storage = loader.get_storage();
    let db_id = loader.get_db_id();
    let containers = loader.get_container_ids();
    loader.load_items(crate::tpcc::Item::ITEMS);
    loader.load_warehouse(1);
    loader.load_warehouse(2);

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = vec![];

    for _t_id in 0..4 {
        let storage = storage.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            let mut stats = TxnTypeStats::new();
            let mut prof = NewOrderProfile::default();
            let home_w_id = (_t_id % 2 + 1) as u16;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let d_id = crate::tpcc::record_definitions::urand_int(1u8, 10);
                let c_id = crate::tpcc::record_definitions::urand_int(1u32, 3000);
                let ol_cnt = crate::tpcc::record_definitions::urand_int(5u8, 15);
                let items: Vec<_> = (0..ol_cnt)
                    .map(|_| super::neworder_txn::NewOrderItem {
                        i_id: crate::tpcc::record_definitions::urand_int(1u32, 100000),
                        supply_w_id: home_w_id,
                        quantity: 5,
                    })
                    .collect();
                let input = super::neworder_txn::NewOrderInput {
                    w_id: home_w_id,
                    d_id,
                    c_id,
                    items,
                    rollback: false,
                };
                run_neworder_txn_profiled(
                    &storage, db_id, &containers, &input,
                    Some(&mut stats), &mut prof,
                );
            }
            (stats, prof)
        }));
    }

    thread::sleep(Duration::from_secs(5));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);

    let mut total_prof = NewOrderProfile::default();
    let mut total_commits = 0u64;
    for h in handles {
        let (stats, prof) = h.join().unwrap();
        total_commits += stats.num_commits;
        total_prof.merge(&prof);
    }
    println!("\n=== NewOrder Profile (4 threads, 2 warehouses, FieldLevel) ===");
    total_prof.print(total_commits);

    // Print get_fields internal breakdown
    use crate::txn_storage2::transactional_storage::*;
    let count = GET_FIELDS_COUNT.load(std::sync::atomic::Ordering::Relaxed);
    if count > 0 {
        let traverse = GET_FIELDS_TRAVERSE_NS.load(std::sync::atomic::Ordering::Relaxed);
        let lock = GET_FIELDS_LOCK_NS.load(std::sync::atomic::Ordering::Relaxed);
        let deser = GET_FIELDS_DESER_NS.load(std::sync::atomic::Ordering::Relaxed);
        let rwset = GET_FIELDS_RWSET_NS.load(std::sync::atomic::Ordering::Relaxed);
        let key_search = GET_FIELDS_KEY_SERIAL_NS.load(std::sync::atomic::Ordering::Relaxed);
        let total = traverse + key_search + lock + deser + rwset;
        let avg = |v: u64| v / count;  // ns per call
        let pct = |v: u64| v as f64 / total as f64 * 100.0;
        println!("\n  get_fields internal breakdown ({} first-access calls, avg {}ns/call):", count, total / count);
        println!("    btree_traverse:   {:>6}ns  ({:>5.1}%)", avg(traverse), pct(traverse));
        println!("    upper_bound:      {:>6}ns  ({:>5.1}%)", avg(key_search), pct(key_search));
        println!("    field_lock:       {:>6}ns  ({:>5.1}%)", avg(lock), pct(lock));
        println!("    deserialize:      {:>6}ns  ({:>5.1}%)", avg(deser), pct(deser));
        println!("    rwset+clone:      {:>6}ns  ({:>5.1}%)", avg(rwset), pct(rwset));
    }
}

#[test]
fn test_schema_serialization() {
    // Test that schemas can be serialized and deserialized correctly
    let schema = item_schema();
    let bytes = schema.to_bytes();
    let deserialized = Schema::from_bytes(&bytes);

    assert_eq!(schema.cols().len(), deserialized.cols().len());
    assert_eq!(schema.key_indices(), deserialized.key_indices());
}

#[test]
fn test_address_serialization() {
    let mut addr = Address::new();
    addr.street_1[..4].copy_from_slice(b"1234");
    addr.city[..8].copy_from_slice(b"TestCity");
    addr.state[..2].copy_from_slice(b"TS");
    addr.zip[..5].copy_from_slice(b"12345");

    let serialized = address_to_string(&addr);
    let deserialized = string_to_address(&serialized);

    assert_eq!(&addr.street_1[..4], &deserialized.street_1[..4]);
    assert_eq!(&addr.city[..8], &deserialized.city[..8]);
    assert_eq!(&addr.state[..2], &deserialized.state[..2]);
    assert_eq!(&addr.zip[..5], &deserialized.zip[..5]);
}

#[test]
fn test_district_info_helpers() {
    let dist_str = "info1|info2|info3|info4|info5|info6|info7|info8|info9|info10";

    assert_eq!(get_district_info(dist_str, 1), "info1");
    assert_eq!(get_district_info(dist_str, 5), "info5");
    assert_eq!(get_district_info(dist_str, 10), "info10");

    let updated = update_district_info(dist_str, 3, "newinfo");
    assert_eq!(get_district_info(&updated, 3), "newinfo");
    assert_eq!(get_district_info(&updated, 1), "info1"); // Others unchanged
}
