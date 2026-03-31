pub mod delivery_txn;
pub mod loader;
pub mod neworder_txn;
pub mod orderstatus_txn;
pub mod payment_txn;
pub mod record_definitions;
pub mod stocklevel_txn;
pub mod txn_helper;
pub mod txn_utils;

#[cfg(test)]
mod tests;

#[allow(unused_imports)]
use crate::log;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use crate::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    bp::{DatabaseId, MemPool},
    random::gen_truncated_randomized_exponential_backoff,
    txn_storage2::{
        field_level_storage_trait::TxnStorageStatus, transactional_storage::TransactionalStorage,
    },
};

pub use delivery_txn::{run_delivery_txn, run_delivery_txn_with_stats, DeliveryInput};
pub use loader::{PartitionMode, TpccContainerIds, TpccLoader};
pub use neworder_txn::{
    run_neworder_txn, run_neworder_txn_with_stats, NewOrderInput, NewOrderItem,
};
pub use orderstatus_txn::{run_orderstatus_txn, run_orderstatus_txn_with_stats, OrderStatusInput};
pub use payment_txn::{run_payment_txn, run_payment_txn_with_stats, PaymentInput};
pub use stocklevel_txn::{run_stocklevel_txn, run_stocklevel_txn_with_stats, StockLevelInput};

use crate::tpcc::record_definitions::{make_clast, nurand_int, urand_int};
use crate::tpcc2::txn_helper::{print_abort_details, TPCCStatus, TxnTypeStats};

// Define a constant for invalid item ID
const INVALID_ITEM_ID: u32 = 99999;

pub struct TpccBenchmark<M: MemPool + 'static> {
    storage: Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: TpccContainerIds,
    num_warehouses: u16,
}

impl<M: MemPool + 'static> TpccBenchmark<M> {
    pub fn new(mem_pool: Arc<M>, num_warehouses: u16) -> Self {
        Self::with_partition_mode(mem_pool, num_warehouses, PartitionMode::HotCold)
    }

    pub fn with_partition_mode(
        mem_pool: Arc<M>,
        num_warehouses: u16,
        partition_mode: PartitionMode,
    ) -> Self {
        let loader = TpccLoader::with_partition_mode(mem_pool, partition_mode);
        let storage = loader.get_storage();
        let db_id = loader.get_db_id();
        let containers = loader.get_container_ids();

        // Load data
        println!("Loading TPC-C data for {} warehouses", num_warehouses);
        loader.load_items(crate::tpcc::Item::ITEMS);

        for w_id in 1..=num_warehouses {
            loader.load_warehouse(w_id);
        }

        println!("TPC-C data loading complete");

        Self {
            storage,
            db_id,
            containers,
            num_warehouses,
        }
    }

    pub fn run_benchmark(
        &self,
        num_threads: usize,
        duration_secs: u64,
        use_random_warehouse: bool,
    ) -> BenchmarkResult {
        let num_cores_minus_one = get_total_cpus() - 1; // Reserve one core for the main thread
        if num_threads > num_cores_minus_one {
            panic!(
                "Number of worker threads {} exceeds available cores (total cores - 1 for main thread) {}",
                num_threads, num_cores_minus_one
            );
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let committed_txns = Arc::new(AtomicU64::new(0));
        let aborted_txns = Arc::new(AtomicU64::new(0));

        let mut handles = vec![];

        for thread_id in 0..num_threads {
            let storage = self.storage.clone();
            let db_id = self.db_id;
            let containers = self.containers;
            let num_warehouses = self.num_warehouses;
            let stop_flag = stop_flag.clone();
            let committed_txns = committed_txns.clone();
            let aborted_txns = aborted_txns.clone();

            let handle = thread::spawn(move || {
                with_affinity(thread_id, || {
                    let current_cpu = get_current_cpu();
                    println!("Thread {} pinned to CPU {}", thread_id, current_cpu);
                    let mut local_committed = 0u64;
                    let mut local_aborted = 0u64;

                    // Thread-local stats for each transaction type
                    let mut neworder_stats = TxnTypeStats::new();
                    let mut payment_stats = TxnTypeStats::new();
                    let mut orderstatus_stats = TxnTypeStats::new();
                    let mut delivery_stats = TxnTypeStats::new();
                    let mut stocklevel_stats = TxnTypeStats::new();

                    // Assign home warehouse or use random selection
                    let home_w_id = if !use_random_warehouse {
                        (thread_id % num_warehouses as usize) as u16 + 1
                    } else {
                        0 // Will be randomly selected in each transaction
                    };

                    while !stop_flag.load(Ordering::Relaxed) {
                        // TPC-C transaction mix
                        let txn_type = urand_int(1, 100);

                        let result = if txn_type <= 45 {
                            // 45% NewOrder
                            run_transaction_with_retry(|| {
                                run_neworder_transaction(
                                    &storage,
                                    db_id,
                                    &containers,
                                    home_w_id,
                                    num_warehouses,
                                    use_random_warehouse,
                                    &mut neworder_stats,
                                )
                            })
                        } else if txn_type <= 88 {
                            // 43% Payment
                            run_transaction_with_retry(|| {
                                run_payment_transaction(
                                    &storage,
                                    db_id,
                                    &containers,
                                    home_w_id,
                                    num_warehouses,
                                    use_random_warehouse,
                                    &mut payment_stats,
                                )
                            })
                        } else if txn_type <= 92 {
                            // 4% OrderStatus
                            run_transaction_with_retry(|| {
                                run_orderstatus_transaction(
                                    &storage,
                                    db_id,
                                    &containers,
                                    home_w_id,
                                    num_warehouses,
                                    use_random_warehouse,
                                    &mut orderstatus_stats,
                                )
                            })
                        } else if txn_type <= 96 {
                            // 4% Delivery
                            run_transaction_with_retry(|| {
                                run_delivery_transaction(
                                    &storage,
                                    db_id,
                                    &containers,
                                    home_w_id,
                                    num_warehouses,
                                    use_random_warehouse,
                                    &mut delivery_stats,
                                )
                            })
                        } else {
                            // 4% StockLevel
                            run_transaction_with_retry(|| {
                                run_stocklevel_transaction(
                                    &storage,
                                    db_id,
                                    &containers,
                                    home_w_id,
                                    num_warehouses,
                                    use_random_warehouse,
                                    &mut stocklevel_stats,
                                )
                            })
                        };

                        match result {
                            Ok(_) => local_committed += 1,
                            Err(_) => local_aborted += 1,
                        }
                    }

                    committed_txns.fetch_add(local_committed, Ordering::Relaxed);
                    aborted_txns.fetch_add(local_aborted, Ordering::Relaxed);

                    // Return thread-local stats
                    (
                        neworder_stats,
                        payment_stats,
                        orderstatus_stats,
                        delivery_stats,
                        stocklevel_stats,
                    )
                })
                .unwrap()
            });

            handles.push(handle);
        }

        // Run for specified duration
        thread::sleep(Duration::from_secs(duration_secs));
        stop_flag.store(true, Ordering::Relaxed);

        // Wait for all threads to finish and collect stats
        let mut merged_neworder_stats = TxnTypeStats::new();
        let mut merged_payment_stats = TxnTypeStats::new();
        let mut merged_orderstatus_stats = TxnTypeStats::new();
        let mut merged_delivery_stats = TxnTypeStats::new();
        let mut merged_stocklevel_stats = TxnTypeStats::new();

        for handle in handles {
            let (neworder, payment, orderstatus, delivery, stocklevel) = handle.join().unwrap();
            merge_stats(&mut merged_neworder_stats, neworder);
            merge_stats(&mut merged_payment_stats, payment);
            merge_stats(&mut merged_orderstatus_stats, orderstatus);
            merge_stats(&mut merged_delivery_stats, delivery);
            merge_stats(&mut merged_stocklevel_stats, stocklevel);
        }

        let total_committed = committed_txns.load(Ordering::Relaxed);
        let total_aborted = aborted_txns.load(Ordering::Relaxed);

        BenchmarkResult {
            duration_secs,
            committed_txns: total_committed,
            aborted_txns: total_aborted,
            throughput: total_committed as f64 / duration_secs as f64,
            neworder_stats: merged_neworder_stats,
            payment_stats: merged_payment_stats,
            orderstatus_stats: merged_orderstatus_stats,
            delivery_stats: merged_delivery_stats,
            stocklevel_stats: merged_stocklevel_stats,
        }
    }
}

pub struct BenchmarkResult {
    pub duration_secs: u64,
    pub committed_txns: u64,
    pub aborted_txns: u64,
    pub throughput: f64,
    pub neworder_stats: TxnTypeStats,
    pub payment_stats: TxnTypeStats,
    pub orderstatus_stats: TxnTypeStats,
    pub delivery_stats: TxnTypeStats,
    pub stocklevel_stats: TxnTypeStats,
}

impl BenchmarkResult {
    pub fn print(&self, verbose: bool) {
        // Calculate total transactions for percentage calculations
        let _total_txns = self.committed_txns + self.aborted_txns;
        let total_commits = self.neworder_stats.num_commits
            + self.payment_stats.num_commits
            + self.orderstatus_stats.num_commits
            + self.delivery_stats.num_commits
            + self.stocklevel_stats.num_commits;
        let total_user_aborts = self.neworder_stats.num_user_aborts
            + self.payment_stats.num_user_aborts
            + self.orderstatus_stats.num_user_aborts
            + self.delivery_stats.num_user_aborts
            + self.stocklevel_stats.num_user_aborts;
        let total_system_aborts = self.neworder_stats.num_system_aborts
            + self.payment_stats.num_system_aborts
            + self.orderstatus_stats.num_system_aborts
            + self.delivery_stats.num_system_aborts
            + self.stocklevel_stats.num_system_aborts;

        println!("    commits: {}", total_commits);
        println!("    usr_aborts: {}", total_user_aborts);
        println!("    sys_aborts: {}", total_system_aborts);
        println!("Throughput: {:.1} txns/s", self.throughput);
        println!();
        println!("Details:");

        // Print each transaction type in the specified format
        print_txn_type_line("NewOrderTxn", &self.neworder_stats, total_commits);
        print_txn_type_line("PaymentTxn", &self.payment_stats, total_commits);
        print_txn_type_line("OrderStatusTxn", &self.orderstatus_stats, total_commits);
        print_txn_type_line("DeliveryTxn", &self.delivery_stats, total_commits);
        print_txn_type_line("StockLevelTxn", &self.stocklevel_stats, total_commits);

        if verbose && total_system_aborts > 0 {
            println!("\nSystem Abort Details:");
            print_abort_details("NewOrderTxn", &self.neworder_stats);
            print_abort_details("PaymentTxn", &self.payment_stats);
            print_abort_details("OrderStatusTxn", &self.orderstatus_stats);
            print_abort_details("DeliveryTxn", &self.delivery_stats);
            print_abort_details("StockLevelTxn", &self.stocklevel_stats);
        }
    }
}

fn print_txn_type_line(name: &str, stats: &TxnTypeStats, total_commits: u64) {
    let total_txns = stats.num_commits + stats.num_user_aborts + stats.num_system_aborts;
    let commit_pct = if total_commits > 0 {
        (stats.num_commits as f64 / total_commits as f64) * 100.0
    } else {
        0.0
    };
    let txn_commit_pct = if total_txns > 0 {
        (stats.num_commits as f64 / total_txns as f64) * 100.0
    } else {
        0.0
    };
    let txn_ua_pct = if total_txns > 0 {
        (stats.num_user_aborts as f64 / total_txns as f64) * 100.0
    } else {
        0.0
    };
    let txn_sa_pct = if total_txns > 0 {
        (stats.num_system_aborts as f64 / total_txns as f64) * 100.0
    } else {
        0.0
    };

    let avg_latency = if stats.num_commits > 0 {
        (stats.total_latency_ns / stats.num_commits) / 1000 // Convert to microseconds
    } else {
        0
    };
    let min_latency = if stats.num_commits > 0 {
        stats.min_latency_ns / 1000
    } else {
        0
    };
    let max_latency = stats.max_latency_ns / 1000;

    println!("    {:<15} c[{:>6.2}%]: {:>7}({:>6.2}%)   ua: {:>7}({:>6.2}%)  sa: {:>7}({:>6.2}%)  avgl: {:>6}  minl: {:>6}  maxl: {:>6}",
        name,
        commit_pct,
        stats.num_commits,
        txn_commit_pct,
        stats.num_user_aborts,
        txn_ua_pct,
        stats.num_system_aborts,
        txn_sa_pct,
        avg_latency,
        min_latency,
        max_latency
    );
}

fn merge_stats(target: &mut TxnTypeStats, source: TxnTypeStats) {
    target.num_commits += source.num_commits;
    target.num_user_aborts += source.num_user_aborts;
    target.num_system_aborts += source.num_system_aborts;
    target.total_latency_ns += source.total_latency_ns;

    if source.min_latency_ns < target.min_latency_ns {
        target.min_latency_ns = source.min_latency_ns;
    }
    if source.max_latency_ns > target.max_latency_ns {
        target.max_latency_ns = source.max_latency_ns;
    }

    for i in 0..target.abort_counts.len() {
        target.abort_counts[i] += source.abort_counts[i];
    }
}

// Helper functions for generating transaction inputs

fn run_transaction_with_retry<F>(mut run_fn: F) -> Result<(), TxnStorageStatus>
where
    F: FnMut() -> Result<(), TxnStorageStatus>,
{
    let mut attempts = 0;
    loop {
        match run_fn() {
            Ok(()) => return Ok(()),
            Err(TxnStorageStatus::Aborted) => {
                // System abort - retry with exponential backoff
                thread::sleep(Duration::from_nanos(
                    gen_truncated_randomized_exponential_backoff(attempts),
                ));
                attempts += 1;
                // Continue to retry
            }
            Err(e) => {
                // Other errors - don't retry
                return Err(e);
            }
        }
    }
}

fn run_neworder_transaction<M: MemPool + 'static>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TxnStorageStatus> {
    let w_id = if use_random_warehouse {
        urand_int(1, num_warehouses)
    } else {
        home_w_id
    };
    let d_id = urand_int(1, 10);
    let c_id = nurand_int::<1023, false>(1, crate::tpcc::Customer::CUSTS_PER_DIST as u64) as u32;

    let mut items = Vec::new();
    let ol_cnt = urand_int(5, 15);

    // 1% rollback according to TPC-C spec
    let rollback = urand_int(1, 100) == 1;

    for i in 0..ol_cnt {
        let i_id = if rollback && i == ol_cnt - 1 {
            // Last item in a rollback transaction should be invalid
            INVALID_ITEM_ID
        } else {
            nurand_int::<8191, false>(1, crate::tpcc::Item::ITEMS as u64) as u32
        };
        let quantity = urand_int(1, 10);

        // 1% chance of remote warehouse
        let supply_w_id = if urand_int(1, 100) == 1 && num_warehouses > 1 {
            let mut w = urand_int(1, num_warehouses);
            if w == w_id && num_warehouses > 1 {
                w = if w == num_warehouses { 1 } else { w + 1 };
            }
            w
        } else {
            w_id
        };

        items.push(NewOrderItem {
            i_id,
            supply_w_id,
            quantity,
        });
    }

    let input = NewOrderInput {
        w_id,
        d_id,
        c_id,
        items,
        rollback,
    };

    let (status, _) = run_neworder_txn_with_stats(storage, db_id, containers, &input, Some(stats));
    match status {
        TPCCStatus::Success => Ok(()),
        TPCCStatus::UserAbort => Ok(()), // User aborts are expected (1% rollback)
        TPCCStatus::SystemAbort => Err(TxnStorageStatus::Aborted),
    }
}

fn run_payment_transaction<M: MemPool + 'static>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TxnStorageStatus> {
    let w_id = if use_random_warehouse {
        urand_int(1, num_warehouses)
    } else {
        home_w_id
    };
    let d_id = urand_int(1, 10);
    let h_amount = urand_int(1, 5000) as f64;

    // 85% local, 15% remote
    let (c_w_id, c_d_id) = if urand_int(1, 100) <= 85 {
        (w_id, d_id)
    } else {
        let c_w_id = if num_warehouses > 1 {
            let mut w = urand_int(1, num_warehouses);
            if w == w_id {
                w = if w == num_warehouses { 1 } else { w + 1 };
            }
            w
        } else {
            w_id
        };
        (c_w_id, urand_int(1, 10))
    };

    // 60% by last name, 40% by ID
    let (c_id, c_last) = if urand_int(1, 100) <= 60 {
        let last_name_num = nurand_int::<255, false>(0, 999) as usize;
        let mut c_last = vec![0u8; 16];
        make_clast(&mut c_last, last_name_num);
        (
            None,
            Some(
                String::from_utf8_lossy(&c_last)
                    .trim_end_matches('\0')
                    .to_string(),
            ),
        )
    } else {
        (
            Some(nurand_int::<1023, false>(1, crate::tpcc::Customer::CUSTS_PER_DIST as u64) as u32),
            None,
        )
    };

    let input = PaymentInput {
        w_id,
        d_id,
        c_w_id,
        c_d_id,
        c_id,
        c_last,
        h_amount,
    };

    let (status, _) = run_payment_txn_with_stats(storage, db_id, containers, &input, Some(stats));
    match status {
        TPCCStatus::Success => Ok(()),
        TPCCStatus::UserAbort => Ok(()), // User aborts are expected
        TPCCStatus::SystemAbort => Err(TxnStorageStatus::Aborted),
    }
}

fn run_orderstatus_transaction<M: MemPool + 'static>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TxnStorageStatus> {
    let w_id = if use_random_warehouse {
        urand_int(1, num_warehouses)
    } else {
        home_w_id
    };
    let d_id = urand_int(1, 10);

    // 60% by last name, 40% by ID
    let (c_id, c_last) = if urand_int(1, 100) <= 60 {
        let last_name_num = nurand_int::<255, false>(0, 999) as usize;
        let mut c_last = vec![0u8; 16];
        make_clast(&mut c_last, last_name_num);
        (
            None,
            Some(
                String::from_utf8_lossy(&c_last)
                    .trim_end_matches('\0')
                    .to_string(),
            ),
        )
    } else {
        (
            Some(nurand_int::<1023, false>(1, crate::tpcc::Customer::CUSTS_PER_DIST as u64) as u32),
            None,
        )
    };

    let input = OrderStatusInput {
        w_id,
        d_id,
        c_id,
        c_last,
    };

    let (status, _) =
        run_orderstatus_txn_with_stats(storage, db_id, containers, &input, Some(stats));
    match status {
        TPCCStatus::Success => Ok(()),
        TPCCStatus::UserAbort => Ok(()), // User aborts are expected
        TPCCStatus::SystemAbort => Err(TxnStorageStatus::Aborted),
    }
}

fn run_delivery_transaction<M: MemPool + 'static>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TxnStorageStatus> {
    let w_id = if use_random_warehouse {
        urand_int(1, num_warehouses)
    } else {
        home_w_id
    };
    let o_carrier_id = urand_int(1, 10);

    let input = DeliveryInput { w_id, o_carrier_id };

    let (status, _) = run_delivery_txn_with_stats(storage, db_id, containers, &input, Some(stats));
    match status {
        TPCCStatus::Success => Ok(()),
        TPCCStatus::UserAbort => Ok(()), // User aborts are expected
        TPCCStatus::SystemAbort => Err(TxnStorageStatus::Aborted),
    }
}

fn run_stocklevel_transaction<M: MemPool + 'static>(
    storage: &Arc<TransactionalStorage<M>>,
    db_id: DatabaseId,
    containers: &TpccContainerIds,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TxnStorageStatus> {
    let w_id = if use_random_warehouse {
        urand_int(1, num_warehouses)
    } else {
        home_w_id
    };
    let d_id = urand_int(1, 10);
    let threshold = urand_int(10, 20);

    let input = StockLevelInput {
        w_id,
        d_id,
        threshold,
    };

    let (status, _) =
        run_stocklevel_txn_with_stats(storage, db_id, containers, &input, Some(stats));
    match status {
        TPCCStatus::Success => Ok(()),
        TPCCStatus::UserAbort => Ok(()), // User aborts are expected
        TPCCStatus::SystemAbort => Err(TxnStorageStatus::Aborted),
    }
}
