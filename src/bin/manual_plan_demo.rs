//! Demo binary showing the manual-plan embedded record store API.
//!
//! This demonstrates:
//! 1. Defining logical schema (what columns exist) and physical schema (what indexes exist)
//! 2. Registering tables in a persistent catalog
//! 3. Inserting records with automatic secondary index maintenance
//! 4. Querying with explicit access-path selection (the key differentiator)

use std::sync::Arc;

use fbtree::{
    bp::get_test_bp,
    field, logical_schema,
    txn_storage2::{
        catalog::Catalog,
        field::Field,
        field_level_storage_trait::{DBOptions, FieldLeveLStorageTrait, TxnOptions},
        index_def::{IndexDef, PhysicalSchema},
        managed_table::ManagedTable,
        query_builder::QueryBuilder,
        transactional_storage::TransactionalStorage,
    },
};

/// Shorthand helpers for unsigned field types (the field! macro only covers signed).
fn u32f(v: u32) -> Field { Field::Uint32(Some(v)) }
fn u64f(v: u64) -> Field { Field::Uint64(Some(v)) }

fn main() {
    println!("=== Manual-Plan Embedded Record Store Demo ===\n");

    // =========================================================================
    // 1. Set up storage
    // =========================================================================
    let bp = get_test_bp(10_000);
    let storage = Arc::new(TransactionalStorage::new(bp));
    let db_id = storage.open_db(DBOptions::new("demo")).unwrap();

    // =========================================================================
    // 2. Define logical schema — what data exists
    // =========================================================================
    let logical = logical_schema! {
        "user_id" => Uint32,
        "ts"      => Uint64,
        "amount"  => Float64,
        "reason"  => String nullable,
    };

    println!("Logical schema:");
    for col in logical.columns() {
        println!(
            "  {} : {:?}{}",
            col.name,
            col.data_type,
            if col.nullable { " (nullable)" } else { "" }
        );
    }
    println!();

    // =========================================================================
    // 3. Define physical schema — how data can be accessed
    // =========================================================================
    //
    // Primary index: ordered by (user_id, ts) — good for "all events for a user"
    // Secondary index "by_ts": ordered by (ts) — good for "recent events across all users"
    //
    // The programmer CHOOSES which index to use. No optimizer decides for you.

    let physical = PhysicalSchema::new(
        IndexDef::primary("pk", vec![0, 1]), // key: (user_id, ts)
        vec![
            IndexDef::secondary("by_ts", vec![1], false), // key: (ts), non-unique
        ],
    );

    println!("Physical schema:");
    for idx in physical.all_indexes() {
        println!(
            "  index {:?} on columns {:?} (unique={})",
            idx.name, idx.key_columns, idx.unique
        );
    }
    println!();

    // =========================================================================
    // 4. Register table in the catalog (persisted)
    // =========================================================================
    let mut catalog = Catalog::open(storage.clone(), db_id).unwrap();
    let meta = catalog
        .create_table("events", logical, physical)
        .unwrap()
        .clone();

    println!("Table 'events' registered in catalog.");
    println!(
        "  Primary container ID: {}",
        meta.primary_container_id
    );
    for (i, cid) in meta.secondary_container_ids.iter().enumerate() {
        println!(
            "  Secondary '{}' container ID: {}",
            meta.physical_schema.secondary_indexes[i].name, cid
        );
    }
    println!();

    // =========================================================================
    // 5. Insert records — secondary indexes maintained automatically
    // =========================================================================
    let table = ManagedTable::new(catalog.storage(), db_id, &meta);

    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    let events = vec![
        vec![u32f(1), u64f(100), field!(Float64 25.50), field!(String "purchase")],
        vec![u32f(2), u64f(101), field!(Float64 10.00), field!(String "refund")],
        vec![u32f(1), u64f(102), field!(Float64 5.75),  field!(String "fee")],
        vec![u32f(3), u64f(103), field!(Float64 100.0), field!(String "purchase")],
        vec![u32f(2), u64f(104), field!(Float64 42.00), field!(String "purchase")],
        vec![u32f(1), u64f(200), field!(Float64 8.00),  field!(String "fee")],
    ];

    println!("Inserting {} records...", events.len());
    for event in events {
        table.insert(&txn, event).unwrap();
    }
    storage.commit_txn(&txn, false).unwrap();
    storage.drop_txn(txn).unwrap();
    println!("Done. Secondary index 'by_ts' was auto-maintained.\n");

    // =========================================================================
    // 6. Query using PRIMARY index — "all events for user 1"
    // =========================================================================
    println!("--- Query 1: All events for user_id=1 (using PRIMARY index 'pk') ---");

    let txn = storage.begin_txn(db_id, TxnOptions::default()).unwrap();

    let results = QueryBuilder::new(&table, &txn)
        .using("pk")
        .prefix_eq("user_id", u32f(1))
        .execute()
        .unwrap();

    println!("  Found {} rows:", results.len());
    for row in &results {
        println!(
            "    user_id={}, ts={}, amount={}, reason={}",
            row[0], row[1], row[2], row[3]
        );
    }
    println!();

    // =========================================================================
    // 7. Query using SECONDARY index — "events with ts in [101, 104)"
    // =========================================================================
    println!("--- Query 2: Events with ts in [101, 104) (using SECONDARY index 'by_ts') ---");
    println!("  The programmer explicitly chose 'by_ts'. No optimizer involved.\n");

    let results = QueryBuilder::new(&table, &txn)
        .using("by_ts")
        .lower_bound("ts", u64f(101))
        .upper_bound("ts", u64f(104))
        .execute()
        .unwrap();

    println!("  Found {} rows:", results.len());
    for row in &results {
        println!(
            "    user_id={}, ts={}, amount={}, reason={}",
            row[0], row[1], row[2], row[3]
        );
    }
    println!();

    // =========================================================================
    // 8. Point lookup — get a single field by primary key
    // =========================================================================
    println!("--- Query 3: Point lookup for (user_id=2, ts=101) ---");

    let (fields, _hint) = table
        .get_by_pk(
            &txn,
            vec![u32f(2), u64f(101)],
            &[2, 3], // amount, reason
            None,
        )
        .unwrap();
    println!("  amount={}, reason={}", fields[0], fields[1]);
    println!();

    // =========================================================================
    // 9. Update with automatic secondary index maintenance
    // =========================================================================
    println!("--- Update: Change reason for (user_id=1, ts=100) ---");

    table
        .update_field(
            &txn,
            vec![u32f(1), u64f(100)],
            3, // reason column
            field!(String "subscription"),
            None,
        )
        .unwrap();

    // Verify
    let (fields, _) = table
        .get_by_pk(&txn, vec![u32f(1), u64f(100)], &[3], None)
        .unwrap();
    println!("  Updated reason: {}", fields[0]);
    println!();

    // =========================================================================
    // 10. Delete with automatic secondary index maintenance
    // =========================================================================
    println!("--- Delete: Remove event (user_id=3, ts=103) ---");

    table
        .delete(&txn, vec![u32f(3), u64f(103)])
        .unwrap();

    // Verify user 3 has no events
    let results = QueryBuilder::new(&table, &txn)
        .using("pk")
        .prefix_eq("user_id", u32f(3))
        .execute()
        .unwrap();
    println!("  Events for user_id=3 after delete: {} rows", results.len());
    println!();

    // =========================================================================
    // 11. Show catalog persistence
    // =========================================================================
    println!("--- Catalog ---");
    let tables = catalog.list_tables();
    println!("  Registered tables: {:?}", tables);

    storage.commit_txn(&txn, false).unwrap();
    storage.drop_txn(txn).unwrap();

    println!("\n=== Demo complete ===");
    println!("Key takeaway: the programmer chooses the access path, not an optimizer.");
    println!("  .using(\"pk\")    → scan the primary B-tree");
    println!("  .using(\"by_ts\") → scan the secondary B-tree");
    println!("Secondary indexes are maintained automatically on insert/update/delete.");
}
