use chrono::NaiveDate;
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{
    bp::{ContainerId, DatabaseId, MemPool},
    txn_storage2::{
        field_level_storage_trait::{ContainerDS, ContainerOptions, DBOptions, ScanOptions},
        DataType, FieldLeveLStorageTrait, NonTransactionalStorage, Schema,
    },
};

use tpchgen::distribution::Distributions;
use tpchgen::generators::LineItemGenerator;
use tpchgen::text::TextPool;

// Schema definition for lineitem table

pub fn lineitem_schema() -> Schema {
    Schema::with_primary_key(
        vec![
            (false, DataType::Int32),    // l_orderkey (part of primary key)
            (false, DataType::Int32),    // l_partkey
            (false, DataType::Int32),    // l_suppkey
            (false, DataType::Int32),    // l_linenumber (part of primary key)
            (false, DataType::Float64),  // l_quantity
            (false, DataType::Float64),  // l_extendedprice
            (false, DataType::Float64),  // l_discount
            (false, DataType::Float64),  // l_tax
            (false, DataType::String),   // l_returnflag
            (false, DataType::String),   // l_linestatus
            (false, DataType::DateTime), // l_shipdate
            (false, DataType::DateTime), // l_commitdate
            (false, DataType::DateTime), // l_receiptdate
            (false, DataType::String),   // l_shipinstruct
            (false, DataType::String),   // l_shipmode
            (false, DataType::String),   // l_comment
            (false, DataType::String),   // l_dummy
        ],
        vec![0, 3], // (l_orderkey, l_linenumber) is the primary key
    )
}

pub struct TpchLoader<M: MemPool> {
    storage: Arc<NonTransactionalStorage<M>>,
    db_id: DatabaseId,
    line_item_cid: ContainerId,
}

impl<M: MemPool> TpchLoader<M> {
    pub fn new(mem_pool: Arc<M>) -> Self {
        let storage = Arc::new(NonTransactionalStorage::new(mem_pool));
        let db_id = storage.open_db(DBOptions::new("tpch")).unwrap();

        // Create container for lineitem table
        let line_item_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new("lineitem", ContainerDS::BTree, lineitem_schema()),
            )
            .unwrap();

        Self {
            storage,
            db_id,
            line_item_cid,
        }
    }

    pub fn get_storage(&self) -> Arc<NonTransactionalStorage<M>> {
        self.storage.clone()
    }

    pub fn get_db_id(&self) -> DatabaseId {
        self.db_id
    }

    /// Loads the lineitem table with the given scale factor using parallel generation
    pub fn load_lineitem(&self, scale_factor: f64, num_threads: usize) {
        println!(
            "Loading lineitem table (SF={}) with {} threads",
            scale_factor, num_threads
        );

        // Initialize distributions and text pool (these are thread-safe singletons)
        let start = std::time::Instant::now();
        Distributions::static_default();
        TextPool::get_or_init_default();
        println!(
            "Initialized distributions and text pool in {:?}",
            start.elapsed()
        );

        // Calculate number of parts for parallel generation
        // Following tpchgen-cli's logic: target ~15MB chunks
        let (num_parts, parts) = self.calculate_lineitem_parts(scale_factor);
        println!("Generating {} parts in parallel", num_parts);

        // Set up rayon thread pool
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        let storage = self.storage.clone();
        let db_id = self.db_id;
        let line_item_cid = self.line_item_cid;

        let gen_start = std::time::Instant::now();
        let row_count = Arc::new(AtomicU64::new(0));

        // Generate lineitem data in parallel
        pool.install(|| {
            parts.into_par_iter().for_each(|part| {
                // Create generator for this partition
                let generator = LineItemGenerator::new(scale_factor, part, num_parts);

                let mut local_count = 0;
                // Generate and insert lineitems for this partition
                for lineitem in generator {
                    let record = self.lineitem_to_record(&lineitem);
                    storage
                        .raw_insert_record(db_id, line_item_cid, record)
                        .unwrap();
                    local_count += 1;
                }

                row_count.fetch_add(local_count, Ordering::Relaxed);
                println!("Completed partition {} ({} rows)", part, local_count);
            });
        });

        let total_rows = row_count.load(Ordering::Relaxed);
        let elapsed = gen_start.elapsed();
        println!("Lineitem loading complete!");
        println!("Total rows: {}", total_rows);
        println!("Time: {:?}", elapsed);
        println!(
            "Rate: {:.0} rows/sec",
            total_rows as f64 / elapsed.as_secs_f64()
        );
    }

    /// Calculate the number of parts for parallel lineitem generation
    fn calculate_lineitem_parts(&self, scale_factor: f64) -> (i32, Vec<i32>) {
        // LineItem has about 4x the rows of Orders
        // Orders base is 1,500,000 rows at SF=1
        let orders_base = 1_500_000;
        let row_count = 4 * (orders_base as f64 * scale_factor) as i64;

        // Average lineitem row size in TBL format is ~128 bytes
        let avg_row_size_bytes = 128;

        // Target chunks of about 15MB
        let target_chunk_size_bytes = 15 * 1024 * 1024;
        let num_parts = ((row_count * avg_row_size_bytes) / target_chunk_size_bytes + 1) as i32;

        // Cap at a reasonable number
        let num_parts = num_parts.min(1000);

        (num_parts, (1..=num_parts).collect())
    }

    /// Convert a tpchgen LineItem to a storage Record
    fn lineitem_to_record(
        &self,
        lineitem: &tpchgen::generators::LineItem,
    ) -> crate::txn_storage2::field::Record {
        use crate::txn_storage2::field::Field;

        crate::txn_storage2::field::Record {
            fields: vec![
                Field::Int32(Some(lineitem.l_orderkey as i32)),
                Field::Int32(Some(lineitem.l_partkey as i32)),
                Field::Int32(Some(lineitem.l_suppkey as i32)),
                Field::Int32(Some(lineitem.l_linenumber)),
                Field::Float64(Some(lineitem.l_quantity as f64)),
                Field::Float64(Some(lineitem.l_extendedprice.as_f64())),
                Field::Float64(Some(lineitem.l_discount.as_f64())),
                Field::Float64(Some(lineitem.l_tax.as_f64())),
                Field::String(Some(lineitem.l_returnflag.to_string())),
                Field::String(Some(lineitem.l_linestatus.to_string())),
                Field::Date(Some(
                    NaiveDate::from_num_days_from_ce_opt(
                        lineitem.l_shipdate.to_unix_epoch() + 719163,
                    )
                    .unwrap(),
                )), // Unix epoch + days since 0000-01-01
                Field::Date(Some(
                    NaiveDate::from_num_days_from_ce_opt(
                        lineitem.l_commitdate.to_unix_epoch() + 719163,
                    )
                    .unwrap(),
                )),
                Field::Date(Some(
                    NaiveDate::from_num_days_from_ce_opt(
                        lineitem.l_receiptdate.to_unix_epoch() + 719163,
                    )
                    .unwrap(),
                )),
                Field::String(Some(lineitem.l_shipinstruct.to_string())),
                Field::String(Some(lineitem.l_shipmode.to_string())),
                Field::String(Some(lineitem.l_comment.to_string())),
                Field::String(Some(String::new())), // l_dummy
            ],
        }
    }

    /// Scan the lineitem table and return all records
    pub fn scan_lineitem(&self) -> Vec<crate::txn_storage2::field::Record> {
        use crate::txn_storage2::field_level_storage_trait::{FieldLeveLStorageTrait, TxnOptions};

        // Begin a transaction
        let txn = self
            .storage
            .begin_txn(self.db_id, TxnOptions::default())
            .unwrap();

        // Create scan options to retrieve all columns (0-16 for lineitem's 17 fields)
        let cols: Vec<usize> = (0..17).collect();
        let scan_options = ScanOptions::new(&cols);

        // Get iterator handle
        let iterator = self
            .storage
            .scan_range(&txn, self.line_item_cid, scan_options)
            .unwrap();

        // Collect all records
        let mut records = Vec::new();
        while let Ok(Some((_key_fields, value_fields, _ptr))) =
            self.storage.iter_next(&txn, &iterator)
        {
            // The value_fields contain the full record (all 17 fields)
            // The key_fields are just extracted views of fields at positions 0 and 3
            // So we only need the value_fields
            records.push(crate::txn_storage2::field::Record {
                fields: value_fields,
            });
        }

        // Clean up
        self.storage.drop_iterator_handle(iterator).unwrap();
        self.storage.commit_txn(&txn, false).unwrap();

        records
    }

    /// Get the lineitem container ID
    pub fn get_lineitem_cid(&self) -> ContainerId {
        self.line_item_cid
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bp::InMemPool;

    #[test]
    fn test_tpch_loader_lineitem() {
        let mem_pool = Arc::new(InMemPool::new());
        let loader = TpchLoader::new(mem_pool);

        // Load a small scale factor for testing
        let scale_factor = 0.001; // SF=0.001 for quick testing
        let num_threads = 4;

        loader.load_lineitem(scale_factor, num_threads);

        // Verify we loaded some data
        let records = loader.scan_lineitem();
        assert!(!records.is_empty());
        println!("Loaded {} lineitem records", records.len());

        // Check the first record has the expected number of fields
        // if let Some(first_record) = records.first() {
        //     assert_eq!(first_record.fields.len(), 17); // lineitem has 17 fields
        //     println!("{}", first_record);
        // }
        for (i, record) in records.iter().enumerate().take(5) {
            println!("{}: {}", i, record);
        }
    }
}

/// Example usage for benchmarking
pub fn benchmark_lineitem_sort<M: MemPool>(mem_pool: Arc<M>, scale_factor: f64) {
    let loader = TpchLoader::new(mem_pool);

    // Load lineitem table
    let load_start = std::time::Instant::now();
    loader.load_lineitem(scale_factor, 10);
    println!("Loading took: {:?}", load_start.elapsed());

    // Scan and sort by orderkey (first field)
    let sort_start = std::time::Instant::now();
    let mut records = loader.scan_lineitem();
    records.sort_by(|a, b| match (&a.fields[0], &b.fields[0]) {
        (
            crate::txn_storage2::field::Field::Int32(Some(a_val)),
            crate::txn_storage2::field::Field::Int32(Some(b_val)),
        ) => a_val.cmp(b_val),
        _ => std::cmp::Ordering::Equal,
    });
    println!(
        "Sorting {} records took: {:?}",
        records.len(),
        sort_start.elapsed()
    );
}
