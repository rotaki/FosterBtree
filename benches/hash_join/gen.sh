#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# Remove existing CSV files
echo "Deleting existing CSV files..."
rm -f data.csv txs.csv ops.csv

# Run gen_data.py
echo "Running gen_data.py..."
python3 gen_data.py "$@"

# Check if data.csv was generated
if [ -f "data.csv" ]; then
    echo "data.csv generated successfully."
else
    echo "Error: data.csv not found. gen_data.py may have failed."
    exit 1
fi

# Run gen_txs.py
echo "Running gen_txs.py..."
python3 gen_txs.py "$@"

# Check if txs.csv and ops.csv were generated
if [ -f "txs.csv" ] && [ -f "ops.csv" ]; then
    echo "txs.csv and ops.csv generated successfully."
else
    echo "Error: txs.csv or ops.csv not found. gen_txs.py may have failed."
    exit 1
fi

echo "Data and transactions generated successfully."