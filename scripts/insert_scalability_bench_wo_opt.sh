#!/bin/bash

cargo build --release --bin fbt_insert_bench --features "no_bp_hint,no_tree_hint"

# Loop from 3 to 18 threads, incrementing by 3
for threads in {1..20..3}
do
    # Run the binary and redirect the output to a file
    ./target/release/fbt_insert_bench -t $threads > output_threads_${threads}.txt
    echo \"Benchmark completed for $threads threads\"
done
