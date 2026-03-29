use std::{
    cmp::Ordering,
    hint::black_box,
    sync::Arc,
    time::{Duration, Instant},
};

use clap::{Parser, ValueEnum};
use fbtree::{
    access_method::fbt::{BTreeKey, FosterBtreePage},
    access_method::OrderedUniqueKeyIndex,
    bp::{
        get_bp, get_in_mem_pool,
        prelude::{ContainerId, FrameReadGuard, MemPool, MemPoolStatus, PageRef},
    },
    container::ContainerManager,
    prelude::FosterBtree,
};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use tempfile::tempdir;

#[cfg(test)]
use fbtree::bp::prelude::InMemPool;

const DEFAULT_BATCH_SIZES: &str = "1024,4096,16384,65536,262144";

#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum StorageMode {
    InMemory,
    Disk,
}

#[derive(Parser, Debug, Clone)]
#[command(version, about = "Run batched Foster B-tree lookup experiments")]
struct Config {
    /// Number of existing keys to insert into the tree before running the experiment.
    #[arg(short = 'n', long, default_value_t = 1_000_000)]
    num_entries: usize,

    /// Comma-separated batch sizes.
    #[arg(short = 'b', long, default_value = DEFAULT_BATCH_SIZES)]
    batch_sizes: String,

    /// Number of warmup runs per method and batch size.
    #[arg(short = 'w', long, default_value_t = 5)]
    warmups: usize,

    /// Number of measured runs per method and batch size.
    #[arg(short = 'r', long, default_value_t = 10)]
    runs: usize,

    /// Fixed-width key size in bytes. Must be at least 8.
    #[arg(long, default_value_t = 10)]
    key_size: usize,

    /// Fixed-width value size in bytes. Must be at least 8.
    #[arg(long, default_value_t = 100)]
    val_size: usize,

    /// Seed for deterministic query generation.
    #[arg(long, default_value_t = 7)]
    seed: u64,

    /// Storage backend used to build the benchmark tree.
    #[arg(long, value_enum, default_value_t = StorageMode::InMemory)]
    storage: StorageMode,

    /// Number of buffer-pool frames when using --storage disk.
    #[arg(long, default_value_t = 8 * 1024)]
    bp_frames: usize,
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

fn parse_batch_sizes(raw: &str) -> Vec<usize> {
    let sizes: Vec<_> = raw
        .split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            part.parse::<usize>()
                .unwrap_or_else(|_| panic!("invalid batch size: {part}"))
        })
        .collect();
    assert!(!sizes.is_empty(), "at least one batch size is required");
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
    tree
}

fn generate_queries(config: &Config, batch_size: usize) -> Vec<Query> {
    let mut rng = SmallRng::seed_from_u64(config.seed ^ batch_size as u64);
    (0..batch_size)
        .map(|qid| {
            let key_num = rng.random_range(0..config.num_entries);
            Query {
                key: encode_num(key_num, config.key_size),
                qid,
            }
        })
        .collect()
}

fn sort_queries(queries: &[Query]) -> Vec<Query> {
    let mut sorted = queries.to_vec();
    sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
    sorted
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

fn run_lookup_sorted<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    sorted_queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    run_lookup(tree, sorted_queries, results);
}

fn run_lookup_sorted_vectorized<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    sorted_queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    // Method 3 from experiment.md:
    // keep the batch globally sorted and push contiguous query slices down the tree.
    // Each node receives only the subrange whose keys fall into that node's key range.
    debug_assert!(sorted_queries.windows(2).all(|w| w[0].key <= w[1].key));
    process_page(tree, tree.root_key, sorted_queries, results);
}

fn run_lookup_sorted_scan_merge_join<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    sorted_queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    // Candidate 4 treats batched lookups as a join:
    // sort the probe side (queries), range-scan the build side (tree) over the covered key
    // interval, then do a merge join between the two sorted streams.
    if sorted_queries.is_empty() {
        return;
    }

    debug_assert!(sorted_queries.windows(2).all(|w| w[0].key <= w[1].key));

    let start_key = &sorted_queries[0].key;
    let end_key = lexicographic_successor(&sorted_queries[sorted_queries.len() - 1].key);
    let mut scan = match end_key.as_ref() {
        Some(end_key) => tree.scan_range(start_key, end_key),
        None => tree.scan_range(start_key, &[]),
    };
    let mut current = scan.next();

    for query in sorted_queries {
        loop {
            match current.as_ref() {
                Some((scan_key, scan_val)) => match query.key.as_slice().cmp(scan_key.as_slice()) {
                    Ordering::Less => {
                        results[query.qid] = None;
                        break;
                    }
                    Ordering::Equal => {
                        results[query.qid] = Some(scan_val.clone());
                        break;
                    }
                    Ordering::Greater => {
                        current = scan.next();
                    }
                },
                None => {
                    results[query.qid] = None;
                    break;
                }
            }
        }
    }
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

fn process_page<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page_ref: PageRef,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    if queries.is_empty() {
        return;
    }

    let page = read_page(tree, page_ref);
    if page.is_leaf() {
        process_leaf_page(tree, &page, queries, results);
    } else {
        process_internal_page(tree, &page, queries, results);
    }
}

fn process_internal_page<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    // Internal-node batching is a merge-style partitioning pass, not per-query lookup.
    // Because `queries` is already sorted, `begin` only moves forward. For each child, we
    // advance `end` until the child's upper fence is reached, then recurse on that slice.
    let mut begin = 0;

    for slot_id in 1..page.high_fence_slot_id() {
        let child_ref = decode_page_ref(tree.container_id, page.get_val(slot_id));
        let upper_bound = page.get_btree_key(slot_id + 1);

        let mut end = begin;
        while end < queries.len()
            && compare_query_to_key(&queries[end].key, &upper_bound) == Ordering::Less
        {
            end += 1;
        }

        if begin < end {
            process_page(tree, child_ref, &queries[begin..end], results);
        }

        begin = end;
        if begin == queries.len() {
            break;
        }
    }

    debug_assert_eq!(begin, queries.len());
}

fn process_leaf_page<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    page: &FrameReadGuard,
    queries: &[Query],
    results: &mut [Option<Vec<u8>>],
) {
    // Leaves do the actual merge between two sorted streams:
    // `queries[..foster_split]` and the leaf's sorted slots. This avoids a binary search for
    // every query. Any tail at or beyond the foster key is forwarded to the foster child.
    let (local_end_slot, foster_split) = if page.has_foster_child() {
        let foster_key = BTreeKey::new(page.get_foster_key());
        let mut split = 0;
        while split < queries.len()
            && compare_query_to_key(&queries[split].key, &foster_key) == Ordering::Less
        {
            split += 1;
        }
        (page.foster_child_slot_id(), split)
    } else {
        (page.high_fence_slot_id(), queries.len())
    };

    let mut query_idx = 0;
    let mut slot_id = 1;

    while query_idx < foster_split && slot_id < local_end_slot {
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

    while query_idx < foster_split {
        results[queries[query_idx].qid] = None;
        query_idx += 1;
    }

    if foster_split < queries.len() {
        let foster_ref = decode_page_ref(tree.container_id, page.get_foster_val());
        process_page(tree, foster_ref, &queries[foster_split..], results);
    }
}

fn benchmark_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut results = vec![None; queries.len()];
        run_lookup(tree, queries, &mut results);
        black_box(&results);
    }

    let mut exec_samples = Vec::with_capacity(runs);
    for _ in 0..runs {
        let mut results = vec![None; queries.len()];
        let start = Instant::now();
        run_lookup(tree, queries, &mut results);
        exec_samples.push(start.elapsed().as_nanos());
        black_box(&results);
    }

    TimingSummary {
        sort_ns: 0,
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut exec_samples),
    }
}

fn benchmark_sorted_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut sorted = queries.to_vec();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let mut results = vec![None; queries.len()];
        run_lookup_sorted(tree, &sorted, &mut results);
        black_box(&results);
    }

    let mut sort_samples = Vec::with_capacity(runs);
    let mut exec_samples = Vec::with_capacity(runs);
    let mut total_samples = Vec::with_capacity(runs);

    for _ in 0..runs {
        let mut sorted = queries.to_vec();
        let sort_start = Instant::now();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let sort_ns = sort_start.elapsed().as_nanos();

        let mut results = vec![None; queries.len()];
        let exec_start = Instant::now();
        run_lookup_sorted(tree, &sorted, &mut results);
        let exec_ns = exec_start.elapsed().as_nanos();

        sort_samples.push(sort_ns);
        exec_samples.push(exec_ns);
        total_samples.push(sort_ns + exec_ns);
        black_box(&results);
    }

    TimingSummary {
        sort_ns: median_ns(&mut sort_samples),
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut total_samples),
    }
}

fn benchmark_vectorized_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut sorted = queries.to_vec();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let mut results = vec![None; queries.len()];
        run_lookup_sorted_vectorized(tree, &sorted, &mut results);
        black_box(&results);
    }

    let mut sort_samples = Vec::with_capacity(runs);
    let mut exec_samples = Vec::with_capacity(runs);
    let mut total_samples = Vec::with_capacity(runs);

    for _ in 0..runs {
        let mut sorted = queries.to_vec();
        let sort_start = Instant::now();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let sort_ns = sort_start.elapsed().as_nanos();

        let mut results = vec![None; queries.len()];
        let exec_start = Instant::now();
        run_lookup_sorted_vectorized(tree, &sorted, &mut results);
        let exec_ns = exec_start.elapsed().as_nanos();

        sort_samples.push(sort_ns);
        exec_samples.push(exec_ns);
        total_samples.push(sort_ns + exec_ns);
        black_box(&results);
    }

    TimingSummary {
        sort_ns: median_ns(&mut sort_samples),
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut total_samples),
    }
}

fn benchmark_scan_merge_join_lookup<M: MemPool>(
    tree: &Arc<FosterBtree<M>>,
    queries: &[Query],
    warmups: usize,
    runs: usize,
) -> TimingSummary {
    for _ in 0..warmups {
        let mut sorted = queries.to_vec();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let mut results = vec![None; queries.len()];
        run_lookup_sorted_scan_merge_join(tree, &sorted, &mut results);
        black_box(&results);
    }

    let mut sort_samples = Vec::with_capacity(runs);
    let mut exec_samples = Vec::with_capacity(runs);
    let mut total_samples = Vec::with_capacity(runs);

    for _ in 0..runs {
        let mut sorted = queries.to_vec();
        let sort_start = Instant::now();
        sorted.sort_unstable_by(|lhs, rhs| lhs.key.cmp(&rhs.key).then(lhs.qid.cmp(&rhs.qid)));
        let sort_ns = sort_start.elapsed().as_nanos();

        let mut results = vec![None; queries.len()];
        let exec_start = Instant::now();
        run_lookup_sorted_scan_merge_join(tree, &sorted, &mut results);
        let exec_ns = exec_start.elapsed().as_nanos();

        sort_samples.push(sort_ns);
        exec_samples.push(exec_ns);
        total_samples.push(sort_ns + exec_ns);
        black_box(&results);
    }

    TimingSummary {
        sort_ns: median_ns(&mut sort_samples),
        exec_ns: median_ns(&mut exec_samples),
        total_ns: median_ns(&mut total_samples),
    }
}

fn lexicographic_successor(key: &[u8]) -> Option<Vec<u8>> {
    let mut next = key.to_vec();
    for idx in (0..next.len()).rev() {
        if next[idx] != u8::MAX {
            next[idx] += 1;
            next[idx + 1..].fill(0);
            return Some(next);
        }
    }
    None
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

fn format_ms(ns: u128) -> String {
    format!("{:.3}", ns as f64 / 1_000_000.0)
}

fn format_ns_per_query(ns: u128, batch_size: usize) -> String {
    format!("{:.2}", ns as f64 / batch_size as f64)
}

fn format_mlookups_per_sec(ns: u128, batch_size: usize) -> String {
    let seconds = Duration::from_nanos(ns as u64).as_secs_f64();
    format!("{:.3}", batch_size as f64 / seconds / 1_000_000.0)
}

fn validate_methods<M: MemPool>(tree: &Arc<FosterBtree<M>>, queries: &[Query]) {
    let sorted = sort_queries(queries);

    let mut res_lookup = vec![None; queries.len()];
    run_lookup(tree, queries, &mut res_lookup);

    let mut res_sorted = vec![None; queries.len()];
    run_lookup_sorted(tree, &sorted, &mut res_sorted);
    assert_eq!(res_lookup, res_sorted, "lookup and lookup_sorted differ");

    let mut res_vectorized = vec![None; queries.len()];
    run_lookup_sorted_vectorized(tree, &sorted, &mut res_vectorized);
    assert_eq!(
        res_lookup, res_vectorized,
        "lookup and lookup_sorted_vectorized differ"
    );

    let mut res_scan_merge = vec![None; queries.len()];
    run_lookup_sorted_scan_merge_join(tree, &sorted, &mut res_scan_merge);
    assert_eq!(
        res_lookup, res_scan_merge,
        "lookup and lookup_sorted_scan_merge_join differ"
    );
}

fn print_summary(
    batch_size: usize,
    lookup: TimingSummary,
    sorted: TimingSummary,
    vector: TimingSummary,
    scan_merge: TimingSummary,
) {
    println!("batch_size={batch_size}");
    println!(
        "  lookup:                  exec_ms={} total_ms={} ns/query={} Mlookups/s={}",
        format_ms(lookup.exec_ns),
        format_ms(lookup.total_ns),
        format_ns_per_query(lookup.total_ns, batch_size),
        format_mlookups_per_sec(lookup.total_ns, batch_size),
    );
    println!(
        "  lookup_sorted:           sort_ms={} exec_ms={} total_ms={} exec_ns/query={} total_ns/query={} total_Mlookups/s={}",
        format_ms(sorted.sort_ns),
        format_ms(sorted.exec_ns),
        format_ms(sorted.total_ns),
        format_ns_per_query(sorted.exec_ns, batch_size),
        format_ns_per_query(sorted.total_ns, batch_size),
        format_mlookups_per_sec(sorted.total_ns, batch_size),
    );
    println!(
        "  lookup_sorted_vectorized sort_ms={} exec_ms={} total_ms={} exec_ns/query={} total_ns/query={} total_Mlookups/s={}",
        format_ms(vector.sort_ns),
        format_ms(vector.exec_ns),
        format_ms(vector.total_ns),
        format_ns_per_query(vector.exec_ns, batch_size),
        format_ns_per_query(vector.total_ns, batch_size),
        format_mlookups_per_sec(vector.total_ns, batch_size),
    );
    println!(
        "  lookup_sorted_scan_merge sort_ms={} exec_ms={} total_ms={} exec_ns/query={} total_ns/query={} total_Mlookups/s={}",
        format_ms(scan_merge.sort_ns),
        format_ms(scan_merge.exec_ns),
        format_ms(scan_merge.total_ns),
        format_ns_per_query(scan_merge.exec_ns, batch_size),
        format_ns_per_query(scan_merge.total_ns, batch_size),
        format_mlookups_per_sec(scan_merge.total_ns, batch_size),
    );
}

fn run_experiment<M: MemPool>(config: &Config, tree: &Arc<FosterBtree<M>>) {
    let batch_sizes = parse_batch_sizes(&config.batch_sizes);
    for batch_size in batch_sizes {
        let queries = generate_queries(&config, batch_size);
        validate_methods(tree, &queries);

        let lookup = benchmark_lookup(tree, &queries, config.warmups, config.runs);
        let sorted = benchmark_sorted_lookup(tree, &queries, config.warmups, config.runs);
        let vectorized = benchmark_vectorized_lookup(tree, &queries, config.warmups, config.runs);
        let scan_merge =
            benchmark_scan_merge_join_lookup(tree, &queries, config.warmups, config.runs);

        print_summary(batch_size, lookup, sorted, vectorized, scan_merge);
    }
}

fn main() {
    let config = Config::parse();
    assert!(config.num_entries > 0, "num_entries must be > 0");
    assert!(config.warmups > 0, "warmups must be > 0");
    assert!(config.runs > 0, "runs must be > 0");
    assert!(config.key_size >= 8, "key_size must be at least 8");
    assert!(config.val_size >= 8, "val_size must be at least 8");
    assert!(config.bp_frames > 0, "bp_frames must be > 0");

    let max_batch_size = *parse_batch_sizes(&config.batch_sizes).iter().max().unwrap();
    assert!(
        config.num_entries >= max_batch_size,
        "num_entries ({}) must be >= max batch size ({max_batch_size})",
        config.num_entries
    );

    println!("config: {:?}", config);

    match config.storage {
        StorageMode::InMemory => {
            let tree = build_tree(&config, get_in_mem_pool());
            run_experiment(&config, &tree);
        }
        StorageMode::Disk => {
            let temp_dir = tempdir().unwrap();
            let cm = Arc::new(ContainerManager::new(temp_dir.path(), false, false).unwrap());
            let tree = build_tree(&config, get_bp(config.bp_frames, cm));
            run_experiment(&config, &tree);
        }
    }
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
            .map(|(qid, &num)| Query {
                key: encode_num(num, key_size),
                qid,
            })
            .collect()
    }

    #[test]
    fn vectorized_matches_lookup_with_duplicates_and_in_range_misses() {
        let key_size = 10;
        let val_size = 16;
        let keys: Vec<_> = (0..4096).map(|i| i * 2).collect();
        let tree = build_test_tree(&keys, key_size, val_size);

        let queries = build_queries(
            &[
                0, 1, 2, 2, 3, 8, 9, 10, 777, 778, 779, 4094, 4095, 4096, 8190, 8191, 9000,
            ],
            key_size,
        );

        validate_methods(&tree, &queries);
    }

    #[test]
    fn vectorized_matches_lookup_on_large_randomized_batches() {
        let key_size = 10;
        let val_size = 32;
        let keys: Vec<_> = (0..50_000).map(|i| i * 2).collect();
        let tree = build_test_tree(&keys, key_size, val_size);

        for seed in [1_u64, 7, 42] {
            let mut rng = SmallRng::seed_from_u64(seed);
            let query_nums: Vec<_> = (0..20_000).map(|_| rng.random_range(0..100_000)).collect();
            let queries = build_queries(&query_nums, key_size);
            validate_methods(&tree, &queries);
        }
    }

    #[test]
    fn vectorized_handles_empty_batch() {
        let tree = build_test_tree(&[0, 2, 4, 6], 10, 16);
        let queries = Vec::new();
        let mut results = Vec::new();

        run_lookup_sorted_vectorized(&tree, &queries, &mut results);

        assert!(results.is_empty());
    }

    #[test]
    fn scan_merge_join_handles_duplicates_and_empty_successor() {
        let key_size = 10;
        let val_size = 16;
        let tree = build_test_tree(&[0, 1, 2, 255, 256, 257], key_size, val_size);
        let queries = vec![
            Query {
                key: vec![u8::MAX; key_size],
                qid: 0,
            },
            Query {
                key: vec![u8::MAX; key_size],
                qid: 1,
            },
        ];
        let sorted = sort_queries(&queries);
        let mut results = vec![None; sorted.len()];

        run_lookup_sorted_scan_merge_join(&tree, &sorted, &mut results);

        assert_eq!(results, vec![None, None]);
    }
}
