mod delivery_txn;
mod loader;
mod neworder_txn;
mod orderstatus_txn;
mod payment_txn;
pub mod record_definitions;
mod stocklevel_txn;
mod txn_utils;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod record_definitions_tests;

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::Duration;

pub use record_definitions::{
    Customer, District, History, Item, NewOrder, Order, OrderLine, Stock, Warehouse,
};

use crate::{
    affinity::{get_current_cpu, get_total_cpus, with_affinity},
    event_tracer::trace_txn,
    prelude::{tpcc_gen_all_tables, tpcc_load_schema, TxnStorageTrait},
    random::gen_truncated_randomized_exponential_backoff,
};

// Re-export key items from submodules
pub use delivery_txn::{run_delivery_txn, DeliveryOutput, DeliveryTxnInput};
pub use loader::{TPCCTable, TPCCTableInfo};
pub use neworder_txn::{run_neworder_txn, NewOrderItem, NewOrderOutput, NewOrderTxnInput};
pub use orderstatus_txn::{run_orderstatus_txn, OrderStatusOutput, OrderStatusTxnInput};
pub use payment_txn::{run_payment_txn, PaymentOutput, PaymentTxnInput};
pub use stocklevel_txn::{run_stocklevel_txn, StockLevelOutput, StockLevelTxnInput};
pub use txn_utils::{
    print_abort_details, AbortID, TPCCConfig, TPCCStatus, TxnTypeStats, INVALID_ITEM_ID,
};

use record_definitions::urand_int;

pub mod prelude {
    pub use super::delivery_txn::*;
    pub use super::loader::*;
    pub use super::neworder_txn::*;
    pub use super::orderstatus_txn::*;
    pub use super::payment_txn::*;
    pub use super::record_definitions::*;
    pub use super::stocklevel_txn::*;
    pub use super::txn_utils::*;
}

#[repr(u8)]
#[derive(Debug, Clone, Copy)]
enum TPCCTxnType {
    NewOrder = 0,
    Payment = 1,
    OrderStatus = 2,
    Delivery = 3,
    StockLevel = 4,
}

impl TPCCTxnType {
    fn as_u8(&self) -> u8 {
        *self as u8
    }
}

pub struct TpccBenchmark<T: TxnStorageTrait + Send + Sync + 'static> {
    txn_storage: T,
    tbl_info: TPCCTableInfo,
    num_warehouses: u16,
}

impl<T: TxnStorageTrait + Send + Sync + 'static> TpccBenchmark<T> {
    pub fn new(txn_storage: T, num_warehouses: u16) -> Self {
        let tbl_info = tpcc_gen_all_tables(&txn_storage, num_warehouses);

        Self {
            txn_storage,
            tbl_info,
            num_warehouses,
        }
    }

    pub fn load(txn_storage: T, num_warehouses: u16) -> Self {
        let tbl_info = tpcc_load_schema(&txn_storage);
        Self {
            txn_storage,
            tbl_info,
            num_warehouses,
        }
    }

    pub fn run_benchmark(
        &self,
        num_threads: usize,
        duration_secs: u64,
        use_random_warehouse: bool,
    ) -> BenchmarkResult {
        let num_cores_minus_one = get_total_cpus() - 1; // keep one core for the main thread
        if num_threads > num_cores_minus_one {
            panic!(
                "Number of worker threads {} exceeds available cores (total cores - 1 for main thread) {}",
                num_threads, num_cores_minus_one
            );
        }

        /* -------- shared, thread-safe state (plain Atomics) -------- */
        let stop_flag = AtomicBool::new(false);
        let committed_txns = AtomicU64::new(0);
        let aborted_txns = AtomicU64::new(0);

        // Create a barrier to synchronize thread startup
        let start_barrier = Arc::new(Barrier::new(num_threads + 1));

        /* -------- run workers inside a thread::scope -------- */
        let (
            merged_neworder_stats,
            merged_payment_stats,
            merged_orderstatus_stats,
            merged_delivery_stats,
            merged_stocklevel_stats,
        ) = thread::scope(|scope| {
            // one handle per worker so we can collect per-thread stats
            let mut handles = Vec::with_capacity(num_threads);

            for thread_id in 0..num_threads {
                // plain references — no Arc needed
                let txn_storage = &self.txn_storage;
                let tbl_info = self.tbl_info.clone();
                let num_warehouses = self.num_warehouses;
                let stop_flag = &stop_flag;
                let committed_txns = &committed_txns;
                let aborted_txns = &aborted_txns;
                let barrier = Arc::clone(&start_barrier);

                // spawn a scoped thread
                handles.push(scope.spawn(move || {
                    with_affinity(thread_id, || {
                        println!(
                            "Thread {id} pinned to CPU {}",
                            get_current_cpu(),
                            id = thread_id
                        );

                        let mut local_committed = 0;
                        let mut local_aborted = 0;

                        // per-txn-type stats
                        let mut neworder_stats = TxnTypeStats::new();
                        let mut payment_stats = TxnTypeStats::new();
                        let mut orderstatus_stats = TxnTypeStats::new();
                        let mut delivery_stats = TxnTypeStats::new();
                        let mut stocklevel_stats = TxnTypeStats::new();

                        // choose the worker’s “home” warehouse
                        let home_w_id = if !use_random_warehouse {
                            (thread_id % num_warehouses as usize) as u16 + 1
                        } else {
                            0
                        };

                        // Wait for all threads to be ready before starting transactions
                        barrier.wait();

                        // run until the main thread flips the stop flag
                        while !stop_flag.load(Ordering::Relaxed) {
                            let txn_type = urand_int(1, 100);
                            let res = if txn_type <= 45 {
                                /* NewOrder 45 % */
                                run_transaction_with_retry(|| {
                                    run_neworder_transaction(
                                        txn_storage,
                                        &tbl_info,
                                        home_w_id,
                                        num_warehouses,
                                        use_random_warehouse,
                                        &mut neworder_stats,
                                    )
                                })
                                .map(|_| TPCCTxnType::NewOrder)
                            } else if txn_type <= 88 {
                                /* Payment 43 % */
                                run_transaction_with_retry(|| {
                                    run_payment_transaction(
                                        txn_storage,
                                        &tbl_info,
                                        home_w_id,
                                        num_warehouses,
                                        use_random_warehouse,
                                        &mut payment_stats,
                                    )
                                })
                                .map(|_| TPCCTxnType::Payment)
                            } else if txn_type <= 92 {
                                /* OrderStatus 4 % */
                                run_transaction_with_retry(|| {
                                    run_orderstatus_transaction(
                                        txn_storage,
                                        &tbl_info,
                                        home_w_id,
                                        num_warehouses,
                                        use_random_warehouse,
                                        &mut orderstatus_stats,
                                    )
                                })
                                .map(|_| TPCCTxnType::OrderStatus)
                            } else if txn_type <= 96 {
                                /* Delivery 4 % */
                                run_transaction_with_retry(|| {
                                    run_delivery_transaction(
                                        txn_storage,
                                        &tbl_info,
                                        home_w_id,
                                        num_warehouses,
                                        use_random_warehouse,
                                        &mut delivery_stats,
                                    )
                                })
                                .map(|_| TPCCTxnType::Delivery)
                            } else {
                                /* StockLevel 4 % */
                                run_transaction_with_retry(|| {
                                    run_stocklevel_transaction(
                                        txn_storage,
                                        &tbl_info,
                                        home_w_id,
                                        num_warehouses,
                                        use_random_warehouse,
                                        &mut stocklevel_stats,
                                    )
                                })
                                .map(|_| TPCCTxnType::StockLevel)
                            };

                            match res {
                                Ok(kind) => {
                                    local_committed += 1;
                                    trace_txn(kind.as_u8());
                                }
                                Err(_) => local_aborted += 1,
                            }
                        }

                        committed_txns.fetch_add(local_committed, Ordering::Relaxed);
                        aborted_txns.fetch_add(local_aborted, Ordering::Relaxed);

                        // return all per-thread stats
                        (
                            neworder_stats,
                            payment_stats,
                            orderstatus_stats,
                            delivery_stats,
                            stocklevel_stats,
                        )
                    })
                    .expect("thread affinity failed")
                }));
            }

            /* -------- let workers run for the requested duration -------- */
            // Note: The duration includes thread setup time. For more accurate measurement,
            // consider starting the timer after all threads pass the barrier.
            start_barrier.wait(); // Ensure all threads are ready before starting the timer
            thread::sleep(Duration::from_secs(duration_secs));
            stop_flag.store(true, Ordering::Relaxed);

            /* -------- merge per-thread stats before leaving the scope -------- */
            let mut merged_neworder = TxnTypeStats::new();
            let mut merged_payment = TxnTypeStats::new();
            let mut merged_orderstatus = TxnTypeStats::new();
            let mut merged_delivery = TxnTypeStats::new();
            let mut merged_stocklevel = TxnTypeStats::new();

            for handle in handles {
                let (n, p, o, d, s) = handle.join().unwrap();
                merge_stats(&mut merged_neworder, n);
                merge_stats(&mut merged_payment, p);
                merge_stats(&mut merged_orderstatus, o);
                merge_stats(&mut merged_delivery, d);
                merge_stats(&mut merged_stocklevel, s);
            }

            (
                merged_neworder,
                merged_payment,
                merged_orderstatus,
                merged_delivery,
                merged_stocklevel,
            )
        }); // -- all worker threads are joined here

        /* -------- build the BenchmarkResult -------- */
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

fn run_transaction_with_retry<F>(mut run_fn: F) -> Result<(), TPCCStatus>
where
    F: FnMut() -> Result<(), TPCCStatus>,
{
    let mut attempts = 0;
    loop {
        match run_fn() {
            Ok(()) => return Ok(()),
            Err(TPCCStatus::UserAbort) => {
                return Err(TPCCStatus::UserAbort); // User abort - do not retry
            }
            Err(TPCCStatus::SystemAbort) => {
                // System abort - retry with exponential backoff
                thread::sleep(Duration::from_nanos(
                    gen_truncated_randomized_exponential_backoff(attempts),
                ));
                attempts += 1;
                // Continue to retry
            }
            Err(e) => {
                panic!("Unexpected TPCCStatus: {:?}", e);
            }
        }
    }
}

fn run_neworder_transaction<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TPCCStatus> {
    let input = NewOrderTxnInput::new(home_w_id, num_warehouses, use_random_warehouse);
    run_neworder_txn(&*txn_storage, tbl_info, &input, stats)?;
    Ok(())
}

fn run_payment_transaction<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TPCCStatus> {
    let input = PaymentTxnInput::new(home_w_id, num_warehouses, use_random_warehouse);
    run_payment_txn(&*txn_storage, tbl_info, &input, stats)?;
    Ok(())
}

fn run_orderstatus_transaction<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TPCCStatus> {
    let input = OrderStatusTxnInput::new(home_w_id, num_warehouses, use_random_warehouse);
    run_orderstatus_txn(&*txn_storage, tbl_info, &input, stats)?;
    Ok(())
}

fn run_delivery_transaction<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TPCCStatus> {
    let input = DeliveryTxnInput::new(home_w_id, num_warehouses, use_random_warehouse);
    run_delivery_txn(&*txn_storage, tbl_info, &input, stats)?;
    Ok(())
}

fn run_stocklevel_transaction<T: TxnStorageTrait>(
    txn_storage: &T,
    tbl_info: &TPCCTableInfo,
    home_w_id: u16,
    num_warehouses: u16,
    use_random_warehouse: bool,
    stats: &mut TxnTypeStats,
) -> Result<(), TPCCStatus> {
    let input = StockLevelTxnInput::new(home_w_id, num_warehouses, use_random_warehouse);
    run_stocklevel_txn(&*txn_storage, tbl_info, &input, stats)?;
    Ok(())
}
