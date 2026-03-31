use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[allow(unused_imports)]
use crate::log;

use crate::{
    bp::{DatabaseId, MemPool},
    tpcc::record_definitions::{get_timestamp, nurand_int, urand_int},
    txn_storage2::{
        field::{Field, Record},
        field_level_storage_trait::{
            ContainerDS, ContainerOptions, DBOptions, FieldLeveLStorageTrait,
        },
        transactional_storage::TransactionalStorage,
    },
};

use super::record_definitions::*;

/// Controls how the CUSTOMER table is partitioned and locked.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionMode {
    /// Single customer container, record-level locking (baseline)
    FullRow,
    /// Two containers (cold + hot), record-level locking per container
    HotCold,
    /// Single customer container, field-level locking
    FieldLevel,
}

pub struct TpccLoader<M: MemPool> {
    storage: Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    partition_mode: PartitionMode,
    // Container IDs
    item_cid: u16,
    warehouse_cid: u16,
    stock_cid: u16,
    district_cid: u16,
    customer_cid: u16, // Primary customer container (full-row or cold, depending on mode)
    customer_hot_cid: u16, // Hot container (only used in HotCold mode, 0 otherwise)
    customer_secondary_cid: u16,
    history_cid: u16,
    order_cid: u16,
    order_secondary_cid: u16,
    new_order_cid: u16,
    order_line_cid: u16,
    // For generating unique history IDs
    #[allow(dead_code)]
    history_id_counter: AtomicU64,
}

impl<M: MemPool> TpccLoader<M> {
    pub fn new(mem_pool: Arc<M>) -> Self {
        Self::with_partition_mode(mem_pool, PartitionMode::HotCold)
    }

    pub fn with_partition_mode(mem_pool: Arc<M>, partition_mode: PartitionMode) -> Self {
        let storage = Arc::new(TransactionalStorage::new(mem_pool));
        let db_id = storage.open_db(DBOptions::new("tpcc")).unwrap();

        // Create all containers
        let item_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(ITEM_TABLE, ContainerDS::BTree, item_schema()),
            )
            .unwrap();

        let warehouse_opts =
            ContainerOptions::new(WAREHOUSE_TABLE, ContainerDS::BTree, warehouse_schema());
        let warehouse_cid = storage
            .create_container(
                db_id,
                if partition_mode == PartitionMode::FieldLevel {
                    warehouse_opts.with_field_level_locking()
                } else {
                    warehouse_opts
                },
            )
            .unwrap();

        let stock_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(STOCK_TABLE, ContainerDS::BTree, stock_schema()),
            )
            .unwrap();

        let district_opts =
            ContainerOptions::new(DISTRICT_TABLE, ContainerDS::BTree, district_schema());
        let district_cid = storage
            .create_container(
                db_id,
                if partition_mode == PartitionMode::FieldLevel {
                    district_opts.with_field_level_locking()
                } else {
                    district_opts
                },
            )
            .unwrap();

        // Customer container(s) depend on partition mode
        let (customer_cid, customer_hot_cid) = match partition_mode {
            PartitionMode::FullRow => {
                let cid = storage
                    .create_container(
                        db_id,
                        ContainerOptions::new(
                            CUSTOMER_TABLE,
                            ContainerDS::BTree,
                            customer_schema(),
                        ),
                    )
                    .unwrap();
                (cid, 0) // hot_cid unused
            }
            PartitionMode::HotCold => {
                let cold_cid = storage
                    .create_container(
                        db_id,
                        ContainerOptions::new(
                            CUSTOMER_TABLE,
                            ContainerDS::BTree,
                            customer_cold_schema(),
                        ),
                    )
                    .unwrap();
                let hot_cid = storage
                    .create_container(
                        db_id,
                        ContainerOptions::new(
                            CUSTOMER_HOT_TABLE,
                            ContainerDS::BTree,
                            customer_hot_schema(),
                        ),
                    )
                    .unwrap();
                (cold_cid, hot_cid)
            }
            PartitionMode::FieldLevel => {
                let cid = storage
                    .create_container(
                        db_id,
                        ContainerOptions::new(
                            CUSTOMER_TABLE,
                            ContainerDS::BTree,
                            customer_schema(),
                        )
                        .with_field_level_locking(),
                    )
                    .unwrap();
                (cid, 0) // hot_cid unused
            }
        };

        let customer_secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(
                    CUSTOMER_SECONDARY_TABLE,
                    ContainerDS::BTree,
                    customer_secondary_schema(),
                ),
            )
            .unwrap();

        let history_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(HISTORY_TABLE, ContainerDS::BTree, history_schema()),
            )
            .unwrap();

        let order_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(ORDER_TABLE, ContainerDS::BTree, order_schema()),
            )
            .unwrap();

        let order_secondary_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(
                    ORDER_SECONDARY_TABLE,
                    ContainerDS::BTree,
                    order_secondary_schema(),
                ),
            )
            .unwrap();

        let new_order_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(NEW_ORDER_TABLE, ContainerDS::BTree, new_order_schema()),
            )
            .unwrap();

        let order_line_cid = storage
            .create_container(
                db_id,
                ContainerOptions::new(ORDER_LINE_TABLE, ContainerDS::BTree, order_line_schema()),
            )
            .unwrap();

        Self {
            storage,
            db_id,
            partition_mode,
            item_cid,
            warehouse_cid,
            stock_cid,
            district_cid,
            customer_cid,
            customer_hot_cid,
            customer_secondary_cid,
            history_cid,
            order_cid,
            order_secondary_cid,
            new_order_cid,
            order_line_cid,
            history_id_counter: AtomicU64::new(1),
        }
    }

    pub fn get_storage(&self) -> Arc<TransactionalStorage<M>> {
        self.storage.clone()
    }

    pub fn get_db_id(&self) -> DatabaseId {
        self.db_id
    }

    pub fn get_container_ids(&self) -> TpccContainerIds {
        TpccContainerIds {
            partition_mode: self.partition_mode,
            item_cid: self.item_cid,
            warehouse_cid: self.warehouse_cid,
            stock_cid: self.stock_cid,
            district_cid: self.district_cid,
            customer_cid: self.customer_cid,
            customer_hot_cid: self.customer_hot_cid,
            customer_secondary_cid: self.customer_secondary_cid,
            history_cid: self.history_cid,
            order_cid: self.order_cid,
            order_secondary_cid: self.order_secondary_cid,
            new_order_cid: self.new_order_cid,
            order_line_cid: self.order_line_cid,
        }
    }

    #[allow(dead_code)]
    fn get_next_history_id(&self) -> u64 {
        self.history_id_counter.fetch_add(1, Ordering::SeqCst)
    }

    pub fn load_items(&self, num_items: usize) {
        println!("Loading {} items", num_items);

        // Use raw insert for efficiency (bypasses transaction overhead)
        // Parallelize item loading using rayon
        (1..=num_items).into_par_iter().for_each(|i| {
            let item = crate::tpcc::Item::generate(i as u32);
            let record = item_to_record(&item);
            self.storage
                .raw_insert_record(self.db_id, self.item_cid, record)
                .unwrap();
        });

        println!("Items loaded");
    }

    pub fn load_warehouse(&self, w_id: u16) {
        println!("Loading warehouse {}", w_id);

        // Load warehouse
        let warehouse = crate::tpcc::Warehouse::generate(w_id);
        self.storage
            .raw_insert_record(
                self.db_id,
                self.warehouse_cid,
                warehouse_to_record(&warehouse),
            )
            .unwrap();

        // Load districts
        for d_id in 1..=crate::tpcc::District::DISTS_PER_WARE {
            let district = crate::tpcc::District::generate(w_id, d_id as u8);
            self.storage
                .raw_insert_record(self.db_id, self.district_cid, district_to_record(&district))
                .unwrap();
        }

        // Load stock
        self.load_stock(w_id);

        // Load customers and their data in parallel across districts
        (1..=crate::tpcc::District::DISTS_PER_WARE)
            .into_par_iter()
            .for_each(|d_id| {
                self.load_customers(w_id, d_id as u8);
            });

        println!("Warehouse {} loaded", w_id);
    }

    fn load_stock(&self, w_id: u16) {
        // println!("Loading stock for warehouse {}", w_id);

        // Parallelize stock loading using rayon
        (1..=crate::tpcc::Item::ITEMS)
            .into_par_iter()
            .for_each(|i| {
                let stock = crate::tpcc::Stock::generate(w_id, i as u32);
                let record = stock_to_record(&stock);
                self.storage
                    .raw_insert_record(self.db_id, self.stock_cid, record)
                    .unwrap();
            });
    }

    fn load_customers(&self, w_id: u16, d_id: u8) {
        // println!("Loading customers for warehouse {} district {}", w_id, d_id);

        (1..=crate::tpcc::Customer::CUSTS_PER_DIST)
            .into_par_iter()
            .for_each(|c_id| {
                let customer =
                    crate::tpcc::Customer::generate(w_id, d_id, c_id as u32, get_timestamp());

                let customer_ptr = match self.partition_mode {
                    PartitionMode::HotCold => {
                        // Insert cold fields into primary (cold) container
                        let ptr = self
                            .storage
                            .raw_insert_record(
                                self.db_id,
                                self.customer_cid,
                                customer_cold_to_record(&customer),
                            )
                            .unwrap();
                        // Insert hot fields into hot container
                        self.storage
                            .raw_insert_record(
                                self.db_id,
                                self.customer_hot_cid,
                                customer_hot_to_record(&customer),
                            )
                            .unwrap();
                        ptr
                    }
                    PartitionMode::FullRow | PartitionMode::FieldLevel => {
                        // Insert full row into single container
                        self.storage
                            .raw_insert_record(
                                self.db_id,
                                self.customer_cid,
                                customer_to_record(&customer),
                            )
                            .unwrap()
                    }
                };

                let secondary_key = Record {
                    fields: vec![
                        Field::Uint16(Some(w_id)),
                        Field::Uint8(Some(d_id)),
                        Field::String(Some(
                            String::from_utf8_lossy(&customer.c_last)
                                .trim_end_matches('\0')
                                .to_string(),
                        )),
                        Field::Uint32(Some(c_id as u32)),
                        Field::Pointer(Some(customer_ptr)),
                    ],
                };
                self.storage
                    .raw_insert_record(self.db_id, self.customer_secondary_cid, secondary_key)
                    .unwrap();
            });

        // Load orders (sequential for now due to order dependencies)
        self.load_orders(w_id, d_id);
    }

    fn load_orders(&self, w_id: u16, d_id: u8) {
        // println!("Loading orders for warehouse {} district {}", w_id, d_id);

        // Generate customer IDs based on order - simplified approach for now
        // TODO: Use proper random permutation when RandomKVs is fixed for 4-byte keys

        // Collect all order data first
        (1..=crate::tpcc::Order::ORDS_PER_DIST)
            .into_par_iter()
            .for_each(|o_id| {
                let c_id = ((o_id - 1) % crate::tpcc::Customer::CUSTS_PER_DIST + 1) as u32;
                let order = crate::tpcc::Order::generate(w_id, d_id, o_id as u32, c_id);

                // Insert into primary order table and capture the record pointer
                let order_ptr = self
                    .storage
                    .raw_insert_record(self.db_id, self.order_cid, order_to_record(&order))
                    .unwrap();

                // Insert into secondary index (by customer) with pointer to primary record
                let secondary_key = Record {
                    fields: vec![
                        Field::Uint16(Some(w_id)),
                        Field::Uint8(Some(d_id)),
                        Field::Uint32(Some(c_id)),
                        Field::Uint32(Some(o_id as u32)),
                        Field::Pointer(Some(order_ptr)),
                    ],
                };
                self.storage
                    .raw_insert_record(self.db_id, self.order_secondary_cid, secondary_key)
                    .unwrap();

                // Insert new order if applicable
                if o_id > 2100 {
                    let new_order = crate::tpcc::NewOrder::generate(w_id, d_id, o_id as u32);
                    self.storage
                        .raw_insert_record(
                            self.db_id,
                            self.new_order_cid,
                            new_order_to_record(&new_order),
                        )
                        .unwrap();
                }

                // Generate and insert order lines in parallel
                let order_lines: Vec<_> = (1..=order.o_ol_cnt)
                    .map(|ol_number| {
                        let ol_i_id = if o_id < 2101 {
                            urand_int(1, crate::tpcc::Item::ITEMS as u32)
                        } else {
                            nurand_int::<8191, true>(1, crate::tpcc::Item::ITEMS as u64) as u32
                        };

                        crate::tpcc::OrderLine::generate(
                            w_id,
                            d_id,
                            o_id as u32,
                            ol_number,
                            w_id, // ol_supply_w_id = w_id for simplicity
                            ol_i_id,
                            order.o_entry_d,
                        )
                    })
                    .collect();

                // Insert order lines
                order_lines.into_iter().for_each(|order_line| {
                    self.storage
                        .raw_insert_record(
                            self.db_id,
                            self.order_line_cid,
                            order_line_to_record(&order_line),
                        )
                        .unwrap();
                });
            });
    }
}

// Container IDs struct for passing to transactions
#[derive(Clone, Copy, Debug)]
pub struct TpccContainerIds {
    pub partition_mode: PartitionMode,
    pub item_cid: u16,
    pub warehouse_cid: u16,
    pub stock_cid: u16,
    pub district_cid: u16,
    pub customer_cid: u16, // Primary customer container (full-row, cold, or field-level)
    pub customer_hot_cid: u16, // Hot container (only used in HotCold mode)
    pub customer_secondary_cid: u16,
    pub history_cid: u16,
    pub order_cid: u16,
    pub order_secondary_cid: u16,
    pub new_order_cid: u16,
    pub order_line_cid: u16,
}
