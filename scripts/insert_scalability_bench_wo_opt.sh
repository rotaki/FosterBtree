#!/bin/bash

cargo build --release --bin fbt_insert_bench --features "no_bp_hint,no_tree_hint"

for threads in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20
do
    # Remove previous results file if it exists
    rm -f fbt_noopt_insert_${threads}.txt

    # Run the binary 3 times for each thread configuration
    for i in {1..3}
    do
        ./target/release/fbt_insert_bench -t $threads >> fbt_noopt_insert_${threads}.txt
    done
    echo "Benchmark completed for $threads threads"
done
