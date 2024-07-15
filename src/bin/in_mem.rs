use clap::Parser;
use fbtree::{bench_utils::*, random::RandomKVs};
fn main() {
    let bench_params = BenchParams::parse();
    println!("{}", bench_params);

    let tree = gen_foster_btree_in_mem();

    let kvs = RandomKVs::new(
        bench_params.unique_keys,
        false,
        bench_params.num_threads,
        bench_params.num_keys,
        bench_params.key_size,
        bench_params.val_min_size,
        bench_params.val_max_size,
    );

    run_bench(bench_params, kvs, tree.clone());

    #[cfg(feature = "stat")]
    {
        println!("Btree op stats: ");
        println!("{}", tree.op_stats());
        println!("Btree page stats: ");
        println!("{}", tree.page_stats(false));
    }
}
