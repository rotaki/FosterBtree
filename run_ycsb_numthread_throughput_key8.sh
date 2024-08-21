#!/bin/bash

# Usage: ./run_experiments.sh <number_of_experiments>
# Example: ./run_experiments.sh 3

cargo build --release --bin ycsb --features ycsb_hash_chain && mv ./target/release/ycsb ./ycsb_hash_chain
cargo build --release --bin ycsb --features ycsb_fbt && mv ./target/release/ycsb ./ycsb_fbt
cargo build --release --bin ycsb --features ycsb_hash_fbt && mv ./target/release/ycsb ./ycsb_hash_fbt
cargo build --release --bin ycsb --features ycsb_hash_bloom_chain && mv ./target/release/ycsb ./ycsb_hash_bloom_chain

NUM_EXPERIMENTS=$1

if [ -z "$NUM_EXPERIMENTS" ]; then
    echo "Please provide the number of experiments."
    exit 1
fi

LOG_FILE="experiment_output.log"

# Initialize the CSV headers
HEADER="t"
for (( i=0; i<$NUM_EXPERIMENTS; i++ ))
do
    HEADER="$HEADER,throughput$i"
done

# Define the set of t values to iterate over
T_VALUES=(1 2 3 4 5 6 7 8 9 10 13 16 19 22 25 28 31 34 37 40)
# T_VALUES=(1 2)
# Define the set of skew factors to iterate over
SKEW_VALUES=(0.0 0.8 0.9)
# SKEW_VALUES=(0.0 0.8)

run_experiment() {
    BINARY=$1
    SKEW=$2
    OUTPUT_FILE=$3

    echo $HEADER > $OUTPUT_FILE

    for t in "${T_VALUES[@]}"
    do
        LINE="$t"
        for (( i=0; i<$NUM_EXPERIMENTS; i++ ))
        do
            echo "Running $BINARY with t=$t and skew=$SKEW (experiment $i)..."
            OUTPUT=$(./$BINARY -t $t -k 8 -p 10000 -w A -s $SKEW 2>&1 | tee -a $LOG_FILE)
            THROUGHPUT=$(echo "$OUTPUT" | grep 'Throughput' | awk '{print $3}')
            LINE="$LINE,$THROUGHPUT"
        done
        echo $LINE >> $OUTPUT_FILE
    done
}

# Run experiments for both binaries and all skew values
for SKEW in "${SKEW_VALUES[@]}"
do
    run_experiment "ycsb_hash_bloom_chain" $SKEW "throughput_results_ycsb_hash_bloom_chain_skew_$SKEW.csv"
    run_experiment "ycsb_hash_chain" $SKEW "throughput_results_ycsb_hash_chain_skew_$SKEW.csv"
    run_experiment "ycsb_fbt" $SKEW "throughput_results_ycsb_fbt_skew_$SKEW.csv"
    run_experiment "ycsb_hash_fbt" $SKEW "throughput_results_ycsb_hash_fbt_skew_$SKEW.csv"
done
