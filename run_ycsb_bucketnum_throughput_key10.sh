#!/bin/bash

# Usage: ./run_experiments.sh <number_of_experiments>
# Example: ./run_experiments.sh 3

cargo build --release --bin ycsb --features ycsb_hash_chain && mv ./target/release/ycsb ./ycsb_hash_chain
cargo build --release --bin ycsb --features ycsb_hash_fbt && mv ./target/release/ycsb ./ycsb_hash_fbt
cargo build --release --bin ycsb --features ycsb_hash_bloom_chain && mv ./target/release/ycsb ./ycsb_hash_bloom_chain

NUM_EXPERIMENTS=$1

if [ -z "$NUM_EXPERIMENTS" ]; then
    echo "Please provide the number of experiments."
    exit 1
fi

LOG_FILE="experiment_output.log"

# Initialize the CSV headers
HEADER="p"
for (( i=0; i<$NUM_EXPERIMENTS; i++ ))
do
    HEADER="$HEADER,throughput$i"
done

# Define the set of p values to iterate over
P_VALUES=(1000 2000 3000 4000 5000 6000 7000 8000 9000 10000 20000 30000 40000 50000)
# Define the set of skew factors to iterate over
SKEW_VALUES=(0.0 0.8 0.9)
# SKEW_VALUES=(0.0 0.8)

run_experiment() {
    BINARY=$1
    SKEW=$2
    OUTPUT_FILE=$3

    echo $HEADER > $OUTPUT_FILE

    for p in "${P_VALUES[@]}"
    do
        LINE="$p"
        for (( i=0; i<$NUM_EXPERIMENTS; i++ ))
        do
            echo "Running $BINARY with p=$p and skew=$SKEW (experiment $i)..."
            OUTPUT=$(./$BINARY -t 20 -k 10 -p $p -w A -s $SKEW 2>&1 | tee -a $LOG_FILE)
            THROUGHPUT=$(echo "$OUTPUT" | grep 'Throughput' | awk '{print $3}')
            LINE="$LINE,$THROUGHPUT"
        done
        echo $LINE >> $OUTPUT_FILE
    done
}

# Run experiments for both binaries and all skew values
for SKEW in "${SKEW_VALUES[@]}"
do
    run_experiment "ycsb_hash_bloom_chain" $SKEW "bucketnum_throughput_results_ycsb_hash_bloom_chain_skew_$SKEW.csv"
    run_experiment "ycsb_hash_chain" $SKEW "bucketnum_throughput_results_ycsb_hash_chain_skew_$SKEW.csv"
    run_experiment "ycsb_hash_fbt" $SKEW "bucketnum_throughput_results_ycsb_hash_fbt_skew_$SKEW.csv"
done
