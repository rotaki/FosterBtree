#!/bin/bash

# Figure 3. TPCC Benchmark
set -euxo pipefail

# Define page sizes and feature sets
PAGE_SIZES=(
    # "4k_page" 
    "16k_page" 
    # "64k_page"
)
FEATURE_SETS=(
    ""                      # All optimizations
    "no_tree_hint"          # Without tree hint
    "no_bp_hint"            # Without bp hint
    "no_tree_hint no_bp_hint"  # Without tree hint and bp hint
)
FEATURE_SUFFIXES=(
    "lipah"
    "no_tree_hint"
    "no_bp_hint"
    "no_tree_hint_no_bp_hint"
)
THREAD_COUNTS=(1 2 3 4 5 10 15 20 25 30 35 40)
ITERATIONS=(1 2 3)  # Number of iterations per benchmark
# NUM_WAREHOUSES=1    # Set the number of warehouses

# Loop through each feature set
for i in "${!FEATURE_SETS[@]}"; do
    FEATURES="${FEATURE_SETS[$i]}"
    SUFFIX="${FEATURE_SUFFIXES[$i]}"

    # Loop through each page size
    for PAGE_SIZE in "${PAGE_SIZES[@]}"; do
        # Build the binary with the specified features
        cargo build --release --bin tpcc --features "$PAGE_SIZE $FEATURES"
        
        # Create a unique binary name
        PAGE_SIZE_SUFFIX=$(echo "$PAGE_SIZE" | sed 's/_page//')
        BINARY_NAME="tpcc_${SUFFIX}_${PAGE_SIZE_SUFFIX}"
        
        # Move the built binary to the new name
        mv ./target/release/tpcc "$BINARY_NAME"
        
        # Create a directory for the binary's results
        RESULT_DIR="Result-${BINARY_NAME}"
        mkdir -p "$RESULT_DIR"

        # Run benchmarks for each thread count and iteration
        for THREAD_COUNT in "${THREAD_COUNTS[@]}"; do

            NUM_WAREHOUSES=${THREAD_COUNT}

            for ITERATION in "${ITERATIONS[@]}"; do
                OUTPUT_FILE="${RESULT_DIR}/${BINARY_NAME}_w${NUM_WAREHOUSES}_t${THREAD_COUNT}.dat${ITERATION}"
                
                # Run the benchmark and redirect output to the file
                ./"$BINARY_NAME" -w "$NUM_WAREHOUSES" -t "$THREAD_COUNT" -d 20 -D 60 > "$OUTPUT_FILE"
            done
        done
    done
done
