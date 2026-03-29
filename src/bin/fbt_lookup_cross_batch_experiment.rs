use std::{
    cmp::Ordering,
    collections::{hash_map::Entry, BTreeMap, HashMap, VecDeque},
    hint::black_box,
    sync::Arc,
    time::Instant,
};

use clap::{Parser, ValueEnum};
use fbtree::{
    access_method::fbt::{BTreeKey, FosterBtreePage},
    bp::{
        get_in_mem_pool,
        prelude::{ContainerId, FrameReadGuard, MemPool, MemPoolStatus, PageRef},
    },
    prelude::{FosterBtree, Page, PageVisitor},
};
use rand::{rngs::SmallRng, seq::SliceRandom, Rng, SeedableRng};

#[cfg(test)]
use fbtree::bp::prelude::InMemPool;

const SMALL_LEAF_BINARY_SEARCH_THRESHOLD: usize = 8;
const DEFAULT_QUEUE_SORT_THRESHOLD: usize = 32;
const NEVER_SORT_THRESHOLD: usize = usize::MAX;
const DEFAULT_QUERY_COUNTS: &str = "1024,4096,16384,65536";
const DEFAULT_Q3_SURVIVAL_FRACTION: f64 = 0.486;
const DEFAULT_Q3_LINEITEMS_PER_ORDER: f64 = 4.0;

#[derive(ValueEnum, Debug, Clone, Copy, Eq, PartialEq)]
enum Workload {
    Uniform,
    Q3,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Foster B-tree lookup experiment with node queueing")]
struct Config {
    /// Number of existing keys inserted into the tree.
    #[arg(short = 'n', long, default_value_t = 1_000_000)]
    num_entries: usize,

    /// Comma-separated query counts.
    #[arg(short = 'b', long = "query-counts", default_value = DEFAULT_QUERY_COUNTS)]
    query_counts: String,

    /// Workload model used for generating probe keys.
    #[arg(long, value_enum, default_value_t = Workload::Uniform)]
    workload: Workload,

    /// Number of warmup runs per method and query count.
    #[arg(short = 'w', long, default_value_t = 3)]
    warmups: usize,

    /// Number of measured runs per method and query count.
    #[arg(short = 'r', long, default_value_t = 5)]
    runs: usize,

    /// Fixed-width key size in bytes. Must be at least 8.
    #[arg(long, default_value_t = 10)]
    key_size: usize,

    /// Fixed-width value size in bytes. Must be at least 8.
    #[arg(long, default_value_t = 100)]
    val_size: usize,

    /// Sort a node-local inbox once it reaches this many queries.
    #[arg(long, default_value_t = DEFAULT_QUEUE_SORT_THRESHOLD)]
    queue_sort_threshold: usize,

    /// Simulate chunk-at-a-time probe arrival with this chunk size.
    /// When omitted, the full query set is materialized and processed as one batch.
    #[arg(long)]
    chunk_size: Option<usize>,

    /// Surviving fraction for the Q3-style orders predicate.
    #[arg(long, default_value_t = DEFAULT_Q3_SURVIVAL_FRACTION)]
    q3_survival_fraction: f64,

    /// Expected lineitems per surviving orderkey for Q3 reporting.
    #[arg(long, default_value_t = DEFAULT_Q3_LINEITEMS_PER_ORDER)]
    q3_lineitems_per_order: f64,

    /// Seed for deterministic query generation.
    #[arg(long, default_value_t = 7)]
    seed: u64,
}

#[derive(Clone, Debug)]
struct Query {
    key: Vec<u8>,
    qid: usize,
}

#[derive(Clone, Copy, Debug, Default)]
struct TimingSummary {
    sort_ns: u128,
    exec_ns: u128,
    total_ns: u128,
    page_reads: u64,
    leaf_page_reads: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum QueuePolicy {
    AllNodes,
    InternalNodesOnly,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SortedBatchedPolicy {
    Binary,
    Merge,
}

struct NodeTask {
    page_ref: PageRef,
    query_ids: Vec<usize>,
    is_sorted: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct AccessStats {
    page_reads: u64,
    leaf_page_reads: u64,
}

#[derive(Default)]
struct TreeStructureStats {
    per_level: BTreeMap<u8, usize>,
}

impl TreeStructureStats {
    fn num_levels(&self) -> usize {
        self.per_level
            .keys()
            .max()
            .map(|level| *level as usize + 1)
            .unwrap_or(0)
    }

    fn total_pages(&self) -> usize {
        self.per_level.values().sum()
    }
}

impl PageVisitor for TreeStructureStats {
    fn visit_pre(&mut self, page: &Page) {
        *self.per_level.entry(page.level()).or_insert(0) += 1;
    }

    fn visit_post(&mut self, _page: &Page) {}
}

fn encode_num(num: usize, size: usize) -> Vec<u8> {
    assert!(
        size >= 8,
        "fixed-width numeric encoding requires at least 8 bytes"
    );
    let mut bytes = vec![0; size];
    bytes[size - 8..].copy_from_slice(&(num as u64).to_be_bytes());
    bytes
}

fn parse_query_counts(raw: &str) -> Vec<usize> {
    let sizes: Vec<_> = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid query count: {part}"))
        })
        .collect();
    assert!(!sizes.is_empty(), "at least one query count is required");
    sizes
}

fn build_tree<M: MemPool>(config: &Config, mem_pool: Arc<M>) -> Arc<FosterBtree<M>> {
    let tree = Arc::new(FosterBtree::new(ContainerId::new(0, 0), mem_pool));
    let start = Instant::now();
    for i in 0..config.num_entries {
        let key = encode_num(i, config.key_size);
        let value = encode_num(i, config.val_size);
        tree.insert_kv(&key, &value, false).unwrap();
    }
    println!(
        "built {} entries in {:.3} ms",
        config.num_entries,
        start.elapsed().as_secs_f64() * 1_000.0
    );
    print_tree_structure_stats(&collect_tree_structure_stats(&tree));
    tree
}

fn collect_tree_structure_stats<M: MemPool>(tree: &Arc<FosterBtree<M>>) -> TreeStructureStats {
    let mut stats = TreeStructureStats::default();
    tree.page_traverser().visit(&mut stats);
    stats
}

fn print_tree_structure_stats(stats: &TreeStructureStats) {
    println!(
        "tree_structure: levels={} total_pages={}",
        stats.num_levels(),
        stats.total_pages()
    );

    let headers = ["level", "page_count"];
    let rows: Vec<[String; 2]> = stats
        .per_level
        .iter()
        .rev()
        .map(|(level, count)| [level.to_string(), count.to_string()])
        .collect();

    let mut widths = headers.map(str::len);
    for row in &rows {
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell.len());
        }
    }

    print_small_row(&headers.map(str::to_string), &widths);
    print_small_separator(&widths);
    for row in &rows {
        print_small_row(row, &widths);
    }
}

fn print_small_row(row: &[String; 2], widths: &[usize; 2]) {
    println!(
        "| {:>w0$} | {:>w1$} |",
        row[0],
        row[1],
        w0 = widths[0],
        w1 = widths[1],
    );
}

fn print_small_separator(widths: &[usize; 2]) {
    println!("|-{}-|-{}-|", "-".repeat(widths[0]), "-".repeat(widths[1]),);
}

fn generate_queries(config: &Config, query_count: usize) -> Vec<Query> {
    let mut rng = SmallRng::seed_from_u64(config.seed ^ query_count as u64);
    (0..query_count)
        .map(|qid| {
            let key_num = rng.random_range(0..config.num_entries);
            Query {
                key: encode_num(key_num, config.key_size),
                qid,
            }
        })
        .collect()
}

fn generate_q3_queries(config: &Config) -> Vec<Query> {
    let mut rng = SmallRng::seed_from_u64(config.seed);
    let mut key_nums = Vec::with_capacity(
        (config.num_entries as f64 * config.q3_survival_fraction).round() as usize,
    );

    for key_num in 0..config.num_entries {
        if rng.random::<f64>() < config.q3_survival_fraction {
            key_nums.push(key_num);
        }
    }

    assert!(
        !key_nums.is_empty(),
        "Q3 workload generated no surviving orderkeys; increase num_entries or q3_survival_fraction"
    );

    key_nums.shuffle(&mut rng);
    key_nums
        .into_iter()
        .enumerate()
        .map(|(qid, key_num)| Query {
            key: encode_num(key_num, config.key_size),
            qid,
        })
        .collect()
}

fn sort_queries(queries: &[Query]) -> Vec<Query> {
    let mut sorted = queries.to_vec();
    sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
    sorted
}

fn effective_chunk_size(queries: &[Query], chunk_size: Option<usize>) -> usize {
    chunk_size.unwrap_or_else(|| queries.len().max(1))
}

fn query_chunks<'a>(
    queries: &'a [Query],
    chunk_size: Option<usize>,
) -> std::slice::Chunks<'a, Query> {
    queries.chunks(effective_chunk_size(queries, chunk_size))
}

fn run_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    for query in queries {
        results[query.qid] = tree.get_kv(&query.key).ok().map(|(_, value)| value);
    }
}

fn run_lookup_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
) {
    for query in queries {
        results[query.qid] = point_lookup_counted(tree, &query.key, access_stats);
    }
}

fn run_sorted_lookup_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    sorted_queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
) {
    run_lookup_counted(tree, sorted_queries, results, access_stats);
}

fn run_sorted_batched_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    sorted_queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    if sorted_queries.is_empty() {
        return;
    }

    debug_assert!(sorted_queries
        .windows(2)
        .all(|window| window[0].key <= window[1].key));
    process_sorted_batched_page(
        tree,
        tree.root_key,
        sorted_queries,
        results,
        access_stats,
        policy,
    );
}

fn run_lookup_arrival_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    chunk_size: Option<usize>,
) {
    for chunk in query_chunks(queries, chunk_size) {
        run_lookup_counted(tree, chunk, results, access_stats);
    }
}

fn run_sorted_lookup_arrival_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    chunk_size: Option<usize>,
) {
    for chunk in query_chunks(queries, chunk_size) {
        let sorted = sort_queries(chunk);
        run_sorted_lookup_counted(tree, &sorted, results, access_stats);
    }
}

fn run_sorted_batched_arrival_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    chunk_size: Option<usize>,
    policy: SortedBatchedPolicy,
) {
    for chunk in query_chunks(queries, chunk_size) {
        let sorted = sort_queries(chunk);
        run_sorted_batched_counted(tree, &sorted, results, access_stats, policy);
    }
}

fn run_node_queue_arrival_with_policy<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
    chunk_size: Option<usize>,
) {
    for chunk in query_chunks(queries, chunk_size) {
        run_node_queue_with_policy(
            tree,
            chunk,
            results,
            access_stats,
            policy,
            queue_sort_threshold,
        );
    }
}

fn run_node_queue_with_policy<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    if queries.is_empty() {
        return;
    }

    let mut ready = VecDeque::new();
    ready.push_back(NodeTask {
        page_ref: tree.root_key,
        query_ids: (0..queries.len()).collect(),
        is_sorted: false,
    });

    while let Some(task) = ready.pop_front() {
        process_page_task(
            tree,
            task,
            queries,
            results,
            access_stats,
            &mut ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn sort_query_ids_by_key(query_ids: &mut [usize], queries: &[Query]) {
    query_ids.sort_unstable_by(|lhs, rhs| {
        queries[*lhs]
            .key
            .cmp(&queries[*rhs].key)
            .then(queries[*lhs].qid.cmp(&queries[*rhs].qid))
    });
}

fn maybe_sort_node_task(task: &mut NodeTask, queries: &[Query], queue_sort_threshold: usize) {
    if !task.is_sorted && task.query_ids.len() >= queue_sort_threshold {
        sort_query_ids_by_key(&mut task.query_ids, queries);
        task.is_sorted = true;
    }
}

fn process_page_task<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    mut task: NodeTask,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    if task.query_ids.is_empty() {
        return;
    }

    maybe_sort_node_task(&mut task, queries, queue_sort_threshold);

    let page = read_page_counted(tree, task.page_ref, access_stats);
    if page.is_leaf() {
        process_leaf_task(
            tree,
            &page,
            task,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    } else {
        process_internal_task(
            tree,
            &page,
            task,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_internal_task<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    task: NodeTask,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    if task.is_sorted {
        process_internal_task_sorted(
            tree,
            page,
            task.query_ids,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    } else {
        process_internal_task_unsorted(
            tree,
            page,
            task.query_ids,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_internal_task_sorted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    query_ids: Vec<usize>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    let mut remaining = query_ids;

    for slot_id in 1..page.high_fence_slot_id() {
        if remaining.is_empty() {
            break;
        }

        let upper_bound = page.get_btree_key(slot_id + 1);
        let end = remaining.partition_point(|query_id| {
            compare_query_to_key(&queries[*query_id].key, &upper_bound) == Ordering::Less
        });
        if end == 0 {
            continue;
        }

        let child_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
        let tail = remaining.split_off(end);
        let child_query_ids = std::mem::replace(&mut remaining, tail);
        dispatch_child_task(
            tree,
            page,
            slot_id,
            child_ref,
            child_query_ids,
            true,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }

    debug_assert!(remaining.is_empty());
}

fn process_internal_task_unsorted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    query_ids: Vec<usize>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    let mut child_groups: HashMap<u32, Vec<usize>> = HashMap::new();
    let mut child_order = Vec::new();

    for query_id in query_ids {
        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&queries[query_id].key)) - 1;
        debug_assert!(slot_id > page.low_fence_slot_id());

        match child_groups.entry(slot_id) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().push(query_id);
            }
            Entry::Vacant(entry) => {
                child_order.push(slot_id);
                entry.insert(vec![query_id]);
            }
        }
    }

    for slot_id in child_order {
        let child_query_ids = child_groups
            .remove(&slot_id)
            .expect("child group should exist for touched slot");
        let child_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
        dispatch_child_task(
            tree,
            page,
            slot_id,
            child_ref,
            child_query_ids,
            false,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_leaf_task<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    task: NodeTask,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    if task.is_sorted {
        process_leaf_task_sorted(
            tree,
            page,
            task.query_ids,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    } else {
        process_leaf_task_unsorted(
            tree,
            page,
            task.query_ids,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_leaf_task_sorted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    mut query_ids: Vec<usize>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    let local_end_slot;
    let foster_query_ids;

    if page.has_foster_child() {
        let foster_key = BTreeKey::new(page.get_foster_key());
        let foster_split = query_ids.partition_point(|query_id| {
            compare_query_to_key(&queries[*query_id].key, &foster_key) == Ordering::Less
        });
        foster_query_ids = query_ids.split_off(foster_split);
        local_end_slot = page.foster_child_slot_id();
    } else {
        foster_query_ids = Vec::new();
        local_end_slot = page.high_fence_slot_id();
    }

    process_leaf_local_query_ids_sorted(page, &query_ids, queries, local_end_slot, results);

    if !foster_query_ids.is_empty() {
        let foster_ref = decode_page_ref(tree.container_id, page.get_foster_val());
        dispatch_child_task(
            tree,
            page,
            page.foster_child_slot_id(),
            foster_ref,
            foster_query_ids,
            true,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_leaf_task_unsorted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    query_ids: Vec<usize>,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    let local_end_slot;
    let mut foster_query_ids = Vec::new();

    if page.has_foster_child() {
        let foster_key = BTreeKey::new(page.get_foster_key());
        local_end_slot = page.foster_child_slot_id();
        for query_id in query_ids {
            if compare_query_to_key(&queries[query_id].key, &foster_key) == Ordering::Less {
                process_leaf_query_binary(page, query_id, queries, local_end_slot, results);
            } else {
                foster_query_ids.push(query_id);
            }
        }
    } else {
        local_end_slot = page.high_fence_slot_id();
        for query_id in query_ids {
            process_leaf_query_binary(page, query_id, queries, local_end_slot, results);
        }
    }

    if !foster_query_ids.is_empty() {
        let foster_ref = decode_page_ref(tree.container_id, page.get_foster_val());
        dispatch_child_task(
            tree,
            page,
            page.foster_child_slot_id(),
            foster_ref,
            foster_query_ids,
            false,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn process_leaf_local_query_ids_sorted(
    page: &FrameReadGuard,
    query_ids: &[usize],
    queries: &[Query],
    local_end_slot: u32,
    results: &mut [Option<Vec<u8>>],
) {
    if query_ids.len() <= SMALL_LEAF_BINARY_SEARCH_THRESHOLD {
        process_leaf_local_query_ids_binary(page, query_ids, queries, local_end_slot, results);
    } else {
        process_leaf_local_query_ids_merge(page, query_ids, queries, local_end_slot, results);
    }
}

fn process_leaf_local_query_ids_binary(
    page: &FrameReadGuard,
    query_ids: &[usize],
    queries: &[Query],
    local_end_slot: u32,
    results: &mut [Option<Vec<u8>>],
) {
    for query_id in query_ids {
        process_leaf_query_binary(page, *query_id, queries, local_end_slot, results);
    }
}

fn process_leaf_query_binary(
    page: &FrameReadGuard,
    query_id: usize,
    queries: &[Query],
    local_end_slot: u32,
    results: &mut [Option<Vec<u8>>],
) {
    let query = &queries[query_id];
    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&query.key)) - 1;
    if slot_id != page.low_fence_slot_id()
        && slot_id < local_end_slot
        && page.get_raw_key(slot_id) == query.key.as_slice()
    {
        results[query.qid] = Some(page.get_val(slot_id).to_vec());
    } else {
        results[query.qid] = None;
    }
}

fn process_leaf_query_binary_by_ref(
    page: &FrameReadGuard,
    query: &Query,
    local_end_slot: u32,
    results: &mut [Option<Vec<u8>>],
) {
    let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&query.key)) - 1;
    if slot_id != page.low_fence_slot_id()
        && slot_id < local_end_slot
        && page.get_raw_key(slot_id) == query.key.as_slice()
    {
        results[query.qid] = Some(page.get_val(slot_id).to_vec());
    } else {
        results[query.qid] = None;
    }
}

fn process_leaf_local_query_ids_merge(
    page: &FrameReadGuard,
    query_ids: &[usize],
    queries: &[Query],
    local_end_slot: u32,
    results: &mut [Option<Vec<u8>>],
) {
    let mut query_idx = 0;
    let mut slot_id = 1;

    while query_idx < query_ids.len() && slot_id < local_end_slot {
        let slot_key = page.get_raw_key(slot_id);
        let query = &queries[query_ids[query_idx]];
        match query.key.as_slice().cmp(slot_key) {
            Ordering::Less => {
                results[query.qid] = None;
                query_idx += 1;
            }
            Ordering::Equal => {
                results[query.qid] = Some(page.get_val(slot_id).to_vec());
                query_idx += 1;
            }
            Ordering::Greater => {
                slot_id += 1;
            }
        }
    }

    while query_idx < query_ids.len() {
        let query = &queries[query_ids[query_idx]];
        results[query.qid] = None;
        query_idx += 1;
    }
}

fn process_sorted_batched_page<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page_ref: PageRef,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    if queries.is_empty() {
        return;
    }

    let page = read_page_counted(tree, page_ref, access_stats);
    if page.is_leaf() {
        match policy {
            SortedBatchedPolicy::Binary => process_sorted_batched_leaf_binary(
                tree,
                &page,
                queries,
                results,
                access_stats,
                policy,
            ),
            SortedBatchedPolicy::Merge => process_sorted_batched_leaf_merge(
                tree,
                &page,
                queries,
                results,
                access_stats,
                policy,
            ),
        }
    } else {
        match policy {
            SortedBatchedPolicy::Binary => process_sorted_batched_internal_binary(
                tree,
                &page,
                queries,
                results,
                access_stats,
                policy,
            ),
            SortedBatchedPolicy::Merge => process_sorted_batched_internal_merge(
                tree,
                &page,
                queries,
                results,
                access_stats,
                policy,
            ),
        }
    }
}

fn process_sorted_batched_internal_binary<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    let mut begin = 0;

    while begin < queries.len() {
        let slot_id = page.upper_bound_slot_id(&BTreeKey::new(&queries[begin].key)) - 1;
        debug_assert!(slot_id > page.low_fence_slot_id());

        let child_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
        let mut end = begin + 1;
        while end < queries.len() {
            let next_slot_id = page.upper_bound_slot_id(&BTreeKey::new(&queries[end].key)) - 1;
            if next_slot_id != slot_id {
                break;
            }
            end += 1;
        }

        process_sorted_batched_page(
            tree,
            child_ref,
            &queries[begin..end],
            results,
            access_stats,
            policy,
        );
        begin = end;
    }
}

fn process_sorted_batched_internal_merge<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    let mut begin = 0;

    for slot_id in 1..page.high_fence_slot_id() {
        if begin == queries.len() {
            break;
        }

        let upper_bound = page.get_btree_key(slot_id + 1);
        let child_begin = begin;
        while begin < queries.len()
            && compare_query_to_key(&queries[begin].key, &upper_bound) == Ordering::Less
        {
            begin += 1;
        }
        if child_begin == begin {
            continue;
        }

        let child_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
        process_sorted_batched_page(
            tree,
            child_ref,
            &queries[child_begin..begin],
            results,
            access_stats,
            policy,
        );
    }

    debug_assert_eq!(begin, queries.len());
}

fn process_sorted_batched_leaf_binary<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    let local_end_slot = if page.has_foster_child() {
        page.foster_child_slot_id()
    } else {
        page.high_fence_slot_id()
    };

    let foster_split = if page.has_foster_child() {
        let foster_key = BTreeKey::new(page.get_foster_key());
        queries.partition_point(|query| {
            compare_query_to_key(&query.key, &foster_key) == Ordering::Less
        })
    } else {
        queries.len()
    };

    for query in &queries[..foster_split] {
        process_leaf_query_binary_by_ref(page, query, local_end_slot, results);
    }

    if foster_split < queries.len() {
        let foster_ref = decode_page_ref(tree.container_id, page.get_foster_val());
        process_sorted_batched_page(
            tree,
            foster_ref,
            &queries[foster_split..],
            results,
            access_stats,
            policy,
        );
    }
}

fn process_sorted_batched_leaf_merge<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    policy: SortedBatchedPolicy,
) {
    let local_end_slot = if page.has_foster_child() {
        page.foster_child_slot_id()
    } else {
        page.high_fence_slot_id()
    };
    let mut query_idx = 0;
    let mut slot_id = 1;

    while query_idx < queries.len() && slot_id < local_end_slot {
        let slot_key = page.get_raw_key(slot_id);
        match queries[query_idx].key.as_slice().cmp(slot_key) {
            Ordering::Less => {
                results[queries[query_idx].qid] = None;
                query_idx += 1;
            }
            Ordering::Equal => {
                results[queries[query_idx].qid] = Some(page.get_val(slot_id).to_vec());
                query_idx += 1;
            }
            Ordering::Greater => {
                slot_id += 1;
            }
        }
    }

    if page.has_foster_child() {
        let foster_key = BTreeKey::new(page.get_foster_key());
        while query_idx < queries.len()
            && compare_query_to_key(&queries[query_idx].key, &foster_key) == Ordering::Less
        {
            results[queries[query_idx].qid] = None;
            query_idx += 1;
        }

        if query_idx < queries.len() {
            let foster_ref = decode_page_ref(tree.container_id, page.get_foster_val());
            process_sorted_batched_page(
                tree,
                foster_ref,
                &queries[query_idx..],
                results,
                access_stats,
                policy,
            );
        }
    } else {
        while query_idx < queries.len() {
            results[queries[query_idx].qid] = None;
            query_idx += 1;
        }
    }
}

fn dispatch_child_task<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    slot_id: u32,
    child_ref: PageRef,
    query_ids: Vec<usize>,
    is_sorted: bool,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
    access_stats: &mut AccessStats,
    ready: &mut VecDeque<NodeTask>,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
) {
    if query_ids.is_empty() {
        return;
    }

    let child_task = NodeTask {
        page_ref: child_ref,
        query_ids,
        is_sorted,
    };

    if should_queue_child(page, slot_id, policy) {
        ready.push_back(child_task);
    } else {
        process_page_task(
            tree,
            child_task,
            queries,
            results,
            access_stats,
            ready,
            policy,
            queue_sort_threshold,
        );
    }
}

fn should_queue_child(page: &FrameReadGuard, slot_id: u32, policy: QueuePolicy) -> bool {
    match policy {
        QueuePolicy::AllNodes => true,
        QueuePolicy::InternalNodesOnly => {
            if page.has_foster_child() && slot_id == page.foster_child_slot_id() {
                true
            } else {
                page.level() > 1
            }
        }
    }
}

fn benchmark_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        run_lookup_arrival_counted(tree, queries, &mut results, &mut access_stats, chunk_size);
        black_box(access_stats);
        black_box(&results);
    }

    let mut samples = Vec::with_capacity(runs);
    let mut counts = None;
    for _ in 0..runs {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        let start = Instant::now();
        run_lookup_arrival_counted(tree, queries, &mut results, &mut access_stats, chunk_size);
        samples.push(start.elapsed().as_nanos());
        record_access_stats(&mut counts, access_stats);
    }

    let exec_ns = median_ns(&mut samples);
    let counts = counts.unwrap_or_default();
    TimingSummary {
        sort_ns: 0,
        exec_ns,
        total_ns: exec_ns,
        page_reads: counts.page_reads,
        leaf_page_reads: counts.leaf_page_reads,
    }
}

fn benchmark_sorted_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        run_sorted_lookup_arrival_counted(
            tree,
            queries,
            &mut results,
            &mut access_stats,
            chunk_size,
        );
        black_box(access_stats);
        black_box(&results);
    }

    let mut sort_samples = Vec::with_capacity(runs);
    let mut exec_samples = Vec::with_capacity(runs);
    let mut total_samples = Vec::with_capacity(runs);
    let mut counts = None;

    for _ in 0..runs {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        let mut sort_ns = 0;
        let mut exec_ns = 0;
        for chunk in query_chunks(queries, chunk_size) {
            let sort_start = Instant::now();
            let sorted = sort_queries(chunk);
            sort_ns += sort_start.elapsed().as_nanos();

            let exec_start = Instant::now();
            run_sorted_lookup_counted(tree, &sorted, &mut results, &mut access_stats);
            exec_ns += exec_start.elapsed().as_nanos();
        }

        sort_samples.push(sort_ns);
        exec_samples.push(exec_ns);
        total_samples.push(sort_ns + exec_ns);
        record_access_stats(&mut counts, access_stats);
    }

    let counts = counts.unwrap_or_default();
    TimingSummary {
        sort_ns: median_ns(&mut sort_samples),
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut total_samples),
        page_reads: counts.page_reads,
        leaf_page_reads: counts.leaf_page_reads,
    }
}

fn benchmark_sorted_batched<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
    policy: SortedBatchedPolicy,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        run_sorted_batched_arrival_counted(
            tree,
            queries,
            &mut results,
            &mut access_stats,
            chunk_size,
            policy,
        );
        black_box(access_stats);
        black_box(&results);
    }

    let mut sort_samples = Vec::with_capacity(runs);
    let mut exec_samples = Vec::with_capacity(runs);
    let mut total_samples = Vec::with_capacity(runs);
    let mut counts = None;

    for _ in 0..runs {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        let mut sort_ns = 0;
        let mut exec_ns = 0;
        for chunk in query_chunks(queries, chunk_size) {
            let sort_start = Instant::now();
            let sorted = sort_queries(chunk);
            sort_ns += sort_start.elapsed().as_nanos();

            let exec_start = Instant::now();
            run_sorted_batched_counted(tree, &sorted, &mut results, &mut access_stats, policy);
            exec_ns += exec_start.elapsed().as_nanos();
        }

        sort_samples.push(sort_ns);
        exec_samples.push(exec_ns);
        total_samples.push(sort_ns + exec_ns);
        record_access_stats(&mut counts, access_stats);
    }

    let counts = counts.unwrap_or_default();
    TimingSummary {
        sort_ns: median_ns(&mut sort_samples),
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut total_samples),
        page_reads: counts.page_reads,
        leaf_page_reads: counts.leaf_page_reads,
    }
}

fn benchmark_sorted_batched_binary<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    benchmark_sorted_batched(
        tree,
        queries,
        warmups,
        runs,
        chunk_size,
        SortedBatchedPolicy::Binary,
    )
}

fn benchmark_sorted_batched_merge<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    benchmark_sorted_batched(
        tree,
        queries,
        warmups,
        runs,
        chunk_size,
        SortedBatchedPolicy::Merge,
    )
}

fn benchmark_all_node_queue<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    queue_sort_threshold: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    benchmark_node_queue_with_policy(
        tree,
        queries,
        warmups,
        runs,
        QueuePolicy::AllNodes,
        queue_sort_threshold,
        chunk_size,
    )
}

fn benchmark_internal_node_queue<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    queue_sort_threshold: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    benchmark_node_queue_with_policy(
        tree,
        queries,
        warmups,
        runs,
        QueuePolicy::InternalNodesOnly,
        queue_sort_threshold,
        chunk_size,
    )
}

fn benchmark_internal_node_queue_binary<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    benchmark_node_queue_with_policy(
        tree,
        queries,
        warmups,
        runs,
        QueuePolicy::InternalNodesOnly,
        NEVER_SORT_THRESHOLD,
        chunk_size,
    )
}

fn benchmark_node_queue_with_policy<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
    policy: QueuePolicy,
    queue_sort_threshold: usize,
    chunk_size: Option<usize>,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        run_node_queue_arrival_with_policy(
            tree,
            queries,
            &mut results,
            &mut access_stats,
            policy,
            queue_sort_threshold,
            chunk_size,
        );
        black_box(access_stats);
        black_box(&results);
    }

    let mut exec_samples = Vec::with_capacity(runs);
    let mut counts = None;

    for _ in 0..runs {
        let mut results = vec![None; queries.len()];
        let mut access_stats = AccessStats::default();
        let exec_start = Instant::now();
        run_node_queue_arrival_with_policy(
            tree,
            queries,
            &mut results,
            &mut access_stats,
            policy,
            queue_sort_threshold,
            chunk_size,
        );
        let exec_ns = exec_start.elapsed().as_nanos();

        exec_samples.push(exec_ns);
        record_access_stats(&mut counts, access_stats);
    }

    let exec_ns = median_ns(&mut exec_samples);
    let counts = counts.unwrap_or_default();
    TimingSummary {
        sort_ns: 0,
        exec_ns,
        total_ns: exec_ns,
        page_reads: counts.page_reads,
        leaf_page_reads: counts.leaf_page_reads,
    }
}

fn point_lookup_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    key: &[u8],
    access_stats: &mut AccessStats,
) -> Option<Vec<u8>> {
    let search_key = BTreeKey::new(key);
    let mut page_ref = tree.root_key;

    loop {
        let page = read_page_counted(tree, page_ref, access_stats);
        if page.is_leaf() {
            if page.has_foster_child() && search_key >= BTreeKey::new(page.get_foster_key()) {
                page_ref = decode_page_ref(tree.container_id, page.get_foster_val());
                continue;
            }

            let slot_id = page.upper_bound_slot_id(&search_key) - 1;
            if slot_id == page.low_fence_slot_id() {
                return None;
            }
            if page.get_raw_key(slot_id) == key {
                return Some(page.get_val(slot_id).to_vec());
            }
            return None;
        }

        let slot_id = page.upper_bound_slot_id(&search_key) - 1;
        debug_assert!(slot_id > page.low_fence_slot_id());
        page_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
    }
}

fn format_ms(ns: u128) -> String {
    format!("{:.3}", ns as f64 / 1_000_000.0)
}

fn record_access_stats(slot: &mut Option<AccessStats>, access_stats: AccessStats) {
    if let Some(existing) = slot {
        debug_assert_eq!(*existing, access_stats);
    } else {
        *slot = Some(access_stats);
    }
}

fn median_ns(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    let mid = samples.len() / 2;
    if samples.len() % 2 == 1 {
        samples[mid]
    } else {
        (samples[mid - 1] + samples[mid]) / 2
    }
}

fn format_ns_per_query(ns: u128, query_count: usize) -> String {
    format!("{:.2}", ns as f64 / query_count as f64)
}

fn format_mlookups_per_sec(ns: u128, query_count: usize) -> String {
    let seconds = ns as f64 / 1_000_000_000.0;
    format!("{:.3}", query_count as f64 / seconds / 1_000_000.0)
}

fn validate_methods<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    queue_sort_threshold: usize,
    chunk_size: Option<usize>,
) {
    let mut res_lookup = vec![None; queries.len()];
    run_lookup(tree, queries, &mut res_lookup);

    let mut res_lookup_counted = vec![None; queries.len()];
    let mut lookup_access_stats = AccessStats::default();
    run_lookup_arrival_counted(
        tree,
        queries,
        &mut res_lookup_counted,
        &mut lookup_access_stats,
        chunk_size,
    );
    assert_eq!(
        res_lookup, res_lookup_counted,
        "tree.get_kv and counted lookup differ"
    );

    let mut res_sorted = vec![None; queries.len()];
    let mut sorted_access_stats = AccessStats::default();
    run_sorted_lookup_arrival_counted(
        tree,
        queries,
        &mut res_sorted,
        &mut sorted_access_stats,
        chunk_size,
    );
    assert_eq!(res_lookup, res_sorted, "lookup and sorted_lookup differ");

    let mut res_sorted_batched_binary = vec![None; queries.len()];
    let mut sorted_batched_binary_access_stats = AccessStats::default();
    run_sorted_batched_arrival_counted(
        tree,
        queries,
        &mut res_sorted_batched_binary,
        &mut sorted_batched_binary_access_stats,
        chunk_size,
        SortedBatchedPolicy::Binary,
    );
    assert_eq!(
        res_lookup, res_sorted_batched_binary,
        "lookup and sorted_batched_binary differ"
    );

    let mut res_sorted_batched_merge = vec![None; queries.len()];
    let mut sorted_batched_merge_access_stats = AccessStats::default();
    run_sorted_batched_arrival_counted(
        tree,
        queries,
        &mut res_sorted_batched_merge,
        &mut sorted_batched_merge_access_stats,
        chunk_size,
        SortedBatchedPolicy::Merge,
    );
    assert_eq!(
        res_lookup, res_sorted_batched_merge,
        "lookup and sorted_batched_merge differ"
    );

    let mut res_all_queue = vec![None; queries.len()];
    let mut all_queue_access_stats = AccessStats::default();
    run_node_queue_arrival_with_policy(
        tree,
        queries,
        &mut res_all_queue,
        &mut all_queue_access_stats,
        QueuePolicy::AllNodes,
        queue_sort_threshold,
        chunk_size,
    );
    assert_eq!(
        res_lookup, res_all_queue,
        "lookup and all_node_queue differ"
    );

    let mut res_internal_queue = vec![None; queries.len()];
    let mut internal_queue_access_stats = AccessStats::default();
    run_node_queue_arrival_with_policy(
        tree,
        queries,
        &mut res_internal_queue,
        &mut internal_queue_access_stats,
        QueuePolicy::InternalNodesOnly,
        queue_sort_threshold,
        chunk_size,
    );
    assert_eq!(
        res_lookup, res_internal_queue,
        "lookup and internal_node_queue differ"
    );

    let mut res_internal_queue_binary = vec![None; queries.len()];
    let mut internal_queue_binary_access_stats = AccessStats::default();
    run_node_queue_arrival_with_policy(
        tree,
        queries,
        &mut res_internal_queue_binary,
        &mut internal_queue_binary_access_stats,
        QueuePolicy::InternalNodesOnly,
        NEVER_SORT_THRESHOLD,
        chunk_size,
    );
    assert_eq!(
        res_lookup, res_internal_queue_binary,
        "lookup and internal_node_queue_binary differ"
    );
}

fn print_summary(
    query_count: usize,
    lookup: TimingSummary,
    sorted: TimingSummary,
    sorted_batched_binary: TimingSummary,
    sorted_batched_merge: TimingSummary,
    all_queue: TimingSummary,
    internal_queue: TimingSummary,
    internal_queue_binary: TimingSummary,
) {
    println!("query_count={query_count}");
    let headers = [
        "method",
        "sort_ms",
        "exec_ms",
        "total_ms",
        "exec_ns/query",
        "total_ns/query",
        "Mlookups/s",
        "page_reads",
        "leaf_page_reads",
    ];
    let rows = [
        summary_row("random_order", lookup, query_count),
        summary_row("sort_input", sorted, query_count),
        summary_row("sorted_batched_binary", sorted_batched_binary, query_count),
        summary_row("sorted_batched_merge", sorted_batched_merge, query_count),
        summary_row("all_node_queue", all_queue, query_count),
        summary_row("internal_node_queue", internal_queue, query_count),
        summary_row(
            "internal_node_queue_binary",
            internal_queue_binary,
            query_count,
        ),
    ];

    let mut widths = headers.map(str::len);
    for row in &rows {
        for (idx, cell) in row.iter().enumerate() {
            widths[idx] = widths[idx].max(cell.len());
        }
    }

    print_aligned_row(&headers.map(str::to_string), &widths);
    print_separator(&widths);
    for row in &rows {
        print_aligned_row(row, &widths);
    }
}

fn summary_row(method: &str, summary: TimingSummary, query_count: usize) -> [String; 9] {
    [
        method.to_string(),
        format_ms(summary.sort_ns),
        format_ms(summary.exec_ns),
        format_ms(summary.total_ns),
        format_ns_per_query(summary.exec_ns, query_count),
        format_ns_per_query(summary.total_ns, query_count),
        format_mlookups_per_sec(summary.total_ns, query_count),
        summary.page_reads.to_string(),
        summary.leaf_page_reads.to_string(),
    ]
}

fn print_aligned_row(row: &[String; 9], widths: &[usize; 9]) {
    println!(
        "| {:<w0$} | {:>w1$} | {:>w2$} | {:>w3$} | {:>w4$} | {:>w5$} | {:>w6$} | {:>w7$} | {:>w8$} |",
        row[0],
        row[1],
        row[2],
        row[3],
        row[4],
        row[5],
        row[6],
        row[7],
        row[8],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
        w4 = widths[4],
        w5 = widths[5],
        w6 = widths[6],
        w7 = widths[7],
        w8 = widths[8],
    );
}

fn print_separator(widths: &[usize; 9]) {
    println!(
        "|-{}-|-{}-|-{}-|-{}-|-{}-|-{}-|-{}-|-{}-|-{}-|",
        "-".repeat(widths[0]),
        "-".repeat(widths[1]),
        "-".repeat(widths[2]),
        "-".repeat(widths[3]),
        "-".repeat(widths[4]),
        "-".repeat(widths[5]),
        "-".repeat(widths[6]),
        "-".repeat(widths[7]),
        "-".repeat(widths[8]),
    );
}

fn print_q3_workload_info(config: &Config, query_count: usize) {
    let estimated_matches = (query_count as f64 * config.q3_lineitems_per_order).round() as u64;
    println!(
        "q3_workload: survival_fraction={:.3} surviving_orderkeys={} estimated_lineitem_matches≈{}",
        config.q3_survival_fraction, query_count, estimated_matches
    );
}

fn print_probe_arrival_info(config: &Config) {
    match config.chunk_size {
        Some(chunk_size) => println!("probe_arrival: chunked chunk_size={chunk_size}"),
        None => println!("probe_arrival: materialized_full_batch"),
    }
}

fn run_experiment<M: MemPool>(config: &Config, tree: &Arc<FosterBtree<M>>) {
    print_probe_arrival_info(config);
    match config.workload {
        Workload::Uniform => {
            let query_counts = parse_query_counts(&config.query_counts);
            for query_count in query_counts {
                let queries = generate_queries(config, query_count);
                validate_methods(
                    tree,
                    &queries,
                    config.queue_sort_threshold,
                    config.chunk_size,
                );

                let lookup = benchmark_lookup(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.chunk_size,
                );
                let sorted = benchmark_sorted_lookup(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.chunk_size,
                );
                let sorted_batched_binary = benchmark_sorted_batched_binary(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.chunk_size,
                );
                let sorted_batched_merge = benchmark_sorted_batched_merge(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.chunk_size,
                );
                let all_queue = benchmark_all_node_queue(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.queue_sort_threshold,
                    config.chunk_size,
                );
                let internal_queue = benchmark_internal_node_queue(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.queue_sort_threshold,
                    config.chunk_size,
                );
                let internal_queue_binary = benchmark_internal_node_queue_binary(
                    tree,
                    &queries,
                    config.warmups,
                    config.runs,
                    config.chunk_size,
                );

                print_summary(
                    query_count,
                    lookup,
                    sorted,
                    sorted_batched_binary,
                    sorted_batched_merge,
                    all_queue,
                    internal_queue,
                    internal_queue_binary,
                );
            }
        }
        Workload::Q3 => {
            let queries = generate_q3_queries(config);
            let query_count = queries.len();
            print_q3_workload_info(config, query_count);
            validate_methods(
                tree,
                &queries,
                config.queue_sort_threshold,
                config.chunk_size,
            );

            let lookup = benchmark_lookup(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.chunk_size,
            );
            let sorted = benchmark_sorted_lookup(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.chunk_size,
            );
            let sorted_batched_binary = benchmark_sorted_batched_binary(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.chunk_size,
            );
            let sorted_batched_merge = benchmark_sorted_batched_merge(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.chunk_size,
            );
            let all_queue = benchmark_all_node_queue(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.queue_sort_threshold,
                config.chunk_size,
            );
            let internal_queue = benchmark_internal_node_queue(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.queue_sort_threshold,
                config.chunk_size,
            );
            let internal_queue_binary = benchmark_internal_node_queue_binary(
                tree,
                &queries,
                config.warmups,
                config.runs,
                config.chunk_size,
            );

            print_summary(
                query_count,
                lookup,
                sorted,
                sorted_batched_binary,
                sorted_batched_merge,
                all_queue,
                internal_queue,
                internal_queue_binary,
            );
        }
    }
}

fn main() {
    let config = Config::parse();
    assert!(config.num_entries > 0, "num_entries must be > 0");
    assert!(config.warmups > 0, "warmups must be > 0");
    assert!(config.runs > 0, "runs must be > 0");
    assert!(config.key_size >= 8, "key_size must be at least 8");
    assert!(config.val_size >= 8, "val_size must be at least 8");
    assert!(
        (0.0..=1.0).contains(&config.q3_survival_fraction),
        "q3_survival_fraction must be in [0, 1]"
    );
    assert!(
        config.q3_lineitems_per_order > 0.0,
        "q3_lineitems_per_order must be > 0"
    );
    assert!(
        config.queue_sort_threshold > 0,
        "queue_sort_threshold must be > 0"
    );
    assert!(
        config.chunk_size.unwrap_or(1) > 0,
        "chunk_size must be > 0 when provided"
    );

    println!("config: {:?}", config);
    let tree = build_tree(&config, get_in_mem_pool());
    run_experiment(&config, &tree);
}

fn read_page<M: MemPool>(tree: &Arc<FosterBtree<M>>, page_ref: PageRef) -> FrameReadGuard {
    loop {
        match tree.mem_pool.get_page_for_read(
            page_ref.container_id(),
            page_ref.page_id(),
            page_ref.frame_hint(),
        ) {
            Ok(page) => return page,
            Err(MemPoolStatus::FrameReadLatchGrantFailed)
            | Err(MemPoolStatus::FrameWriteLatchGrantFailed)
            | Err(MemPoolStatus::CannotEvictPage) => std::hint::spin_loop(),
            Err(err) => panic!("unexpected page read error for {page_ref}: {err:?}"),
        }
    }
}

fn read_page_counted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page_ref: PageRef,
    access_stats: &mut AccessStats,
) -> FrameReadGuard {
    let page = read_page(tree, page_ref);
    access_stats.page_reads += 1;
    if page.is_leaf() {
        access_stats.leaf_page_reads += 1;
    }
    page
}

fn decode_page_ref(container_id: ContainerId, value: &[u8]) -> PageRef {
    assert_eq!(
        value.len(),
        8,
        "internal child pointers must encode page_id + frame_id"
    );
    let page_id = u32::from_be_bytes(value[0..4].try_into().unwrap());
    let frame_id = u32::from_be_bytes(value[4..8].try_into().unwrap());
    PageRef::new_with_frame_id(container_id, page_id, frame_id)
}

fn compare_query_to_key(query: &[u8], key: &BTreeKey<'_>) -> Ordering {
    BTreeKey::new(query).cmp(key)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn build_test_tree(
        nums: &[usize],
        key_size: usize,
        val_size: usize,
    ) -> Arc<FosterBtree<InMemPool>> {
        let tree = Arc::new(FosterBtree::new(ContainerId::new(0, 0), get_in_mem_pool()));
        for &num in nums {
            let key = encode_num(num, key_size);
            let value = encode_num(num, val_size);
            tree.insert_kv(&key, &value, false).unwrap();
        }
        tree
    }

    fn build_queries(nums: &[usize], key_size: usize) -> Vec<Query> {
        nums.iter()
            .enumerate()
            .map(|(qid, num)| Query {
                key: encode_num(*num, key_size),
                qid,
            })
            .collect()
    }

    #[test]
    fn internal_node_queue_matches_lookup_on_duplicates_and_misses() {
        let tree = build_test_tree(&(0..4096).map(|i| i * 2).collect::<Vec<_>>(), 10, 16);
        let queries = build_queries(&[0, 1, 2, 2, 3, 8, 9, 10, 777, 778, 4094, 4095], 10);
        validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, None);
        validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, Some(4));
    }

    #[test]
    fn internal_node_queue_matches_lookup_on_large_randomized_workload() {
        let key_size = 10;
        let val_size = 32;
        let keys: Vec<_> = (0..50_000).map(|i| i * 2).collect();
        let tree = build_test_tree(&keys, key_size, val_size);

        for seed in [1_u64, 7, 42] {
            let config = Config {
                num_entries: 50_000,
                query_counts: "2048".to_string(),
                workload: Workload::Uniform,
                warmups: 1,
                runs: 1,
                key_size,
                val_size,
                queue_sort_threshold: DEFAULT_QUEUE_SORT_THRESHOLD,
                chunk_size: None,
                q3_survival_fraction: DEFAULT_Q3_SURVIVAL_FRACTION,
                q3_lineitems_per_order: DEFAULT_Q3_LINEITEMS_PER_ORDER,
                seed,
            };
            let queries = generate_queries(&config, 2048);
            validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, None);
            validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, Some(256));
        }
    }

    #[test]
    fn internal_node_queue_handles_empty_batch() {
        let tree = build_test_tree(&(0..1024).collect::<Vec<_>>(), 10, 16);
        let queries = Vec::new();
        validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, None);
        validate_methods(&tree, &queries, DEFAULT_QUEUE_SORT_THRESHOLD, Some(128));
    }
}
