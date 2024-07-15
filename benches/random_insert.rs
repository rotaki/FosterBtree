use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use fbtree::{bench_utils::*, random::RandomKVs};

fn bench_random_insertion(c: &mut Criterion) {
    let kvs = RandomKVs::new(true, false, 3, 500000, 100, 50, 100);
    let bp_size = 10000;

    let mut group = c.benchmark_group("Random Insertion");
    group.sample_size(10);

    group.bench_function("In memory Foster BTree Initial Allocation", |b| {
        b.iter(|| {
            let tree = gen_foster_btree_in_mem();
            black_box(tree);
        });
    });

    group.bench_function("In memory Foster BTree Insertion", |b| {
        b.iter(|| insert_into_foster_tree(gen_foster_btree_in_mem(), &kvs));
    });

    group.bench_function("In memory Foster BTree Insertion Parallel", |b| {
        b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_in_mem(), &kvs));
    });

    group.bench_function("On disk Foster BTree Initial Allocation", |b| {
        b.iter(|| {
            let tree = gen_foster_btree_on_disk(bp_size);
            black_box(tree);
        });
    });

    group.bench_function("On disk Foster BTree Insertion", |b| {
        b.iter(|| insert_into_foster_tree(gen_foster_btree_on_disk(bp_size), &kvs));
    });

    group.bench_function("On disk Foster BTree Insertion Parallel", |b| {
        b.iter(|| insert_into_foster_tree_parallel(gen_foster_btree_on_disk(bp_size), &kvs));
    });

    group.bench_function("BTreeMap Insertion", |b| {
        b.iter(|| insert_into_btree_map(BTreeMap::new(), &kvs));
    });

    group.finish();
}

criterion_group!(benches, bench_random_insertion);
criterion_main!(benches);
