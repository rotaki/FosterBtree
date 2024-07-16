use std::process::Command;

use criterion::{criterion_group, criterion_main, Criterion};
use fbtree::{bench_utils::*, random::RandomKVs};

fn bench_random_insertion(c: &mut Criterion) {
    let kvs = RandomKVs::new(true, false, 1, 200_000, 100, 50, 100);
    let bp_size = 200_000;

    let mut group = c.benchmark_group("Random Insertion");
    group.sample_size(20);

    // Generate hash maps
    let phm = gen_paged_hash_map_in_mem();
    let phm2 = gen_paged_hash_map_on_disk(bp_size);
    let mut rhm = gen_rust_hash_map();

    clear_cache();
    // Benchmark insertion functions
    group.bench_function("In memory Paged Hash Map Insertion", |p| {
        p.iter(|| insert_into_paged_hash_map(&phm, &kvs))
    });

    clear_cache();
    group.bench_function("On disk Paged Hash Map Insertion", |p| {
        p.iter(|| insert_into_paged_hash_map(&phm2, &kvs))
    });

    clear_cache();
    group.bench_function("Rust HashMap Insertion", |p| {
        p.iter(|| insert_into_rust_hash_map(&mut rhm, &kvs))
    });

    clear_cache();
    // Benchmark lookup functions
    group.bench_function("In memory Paged Hash Map Lookup (multi-thread)", |p| {
        p.iter(|| get_from_paged_hash_map(&phm, &kvs))
    });

    clear_cache();
    group.bench_function("On disk Paged Hash Map Lookup (multi-thread)", |p| {
        p.iter(|| get_from_paged_hash_map(&phm2, &kvs))
    });

    clear_cache();
    group.bench_function("Rust HashMap Lookup (multi-thread)", |p| {
        p.iter(|| get_from_rust_hash_map(&rhm, &kvs))
    });

    group.finish();
}

criterion_group!(benches, bench_random_insertion);
criterion_main!(benches);

fn clear_cache() {
    // Unix-based system cache clearing
    let _ = Command::new("sync").status();
    let _ = Command::new("echo 3 > /proc/sys/vm/drop_caches").status();
}

// group.bench_function("In memory Foster BTree Initial Allocation", |b| {
//     b.iter(|| {
//         let tree = gen_foster_btree_in_mem();
//         black_box(tree);
//     });
// });

// group.bench_function("In memory Foster BTree Insertion", |b| {
//     b.iter(|| insert_into_foster_tree(gen_foster_btree_in_mem(), &kvs));
// });

// group.bench_function("In memory Foster BTree Insertion Parallel", |b| {
//     b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_in_mem(), &kvs));
// });

// group.bench_function("On disk Foster BTree Initial Allocation", |b| {
//     b.iter(|| {
//         let tree = gen_foster_btree_on_disk(bp_size);
//         black_box(tree);
//     });
// });

// group.bench_function("On disk Foster BTree Insertion", |b| {
//     b.iter(|| insert_into_foster_tree(gen_foster_btree_on_disk(bp_size), &kvs));
// });

// group.bench_function("On disk Foster BTree Insertion Parallel", |b| {
//     b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_on_disk(bp_size), &kvs));
// });

// group.bench_function("BTreeMap Insertion", |b| {
//     b.iter(|| insert_into_btree_map(BTreeMap::new(), &kvs));
// });
