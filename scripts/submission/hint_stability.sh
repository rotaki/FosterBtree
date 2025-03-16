set -euxo

cargo build --release --bin secondary_index_stability_hint

# Figure 6. Hint Hit Ratio In-Memory
# ./target/release/secondary_index_stability_hint -b 60000 -n 1000000 -k 10 -r 100 > hint_stability_t1_n1000000_k10_r100_b60000.txt

# Figure 7. Hint Hit Ratio On-Disk
./target/release/secondary_index_stability_hint -b 16580 -n 1000000 -k 10 -r 100 > ondisk_hint_stability_t1_n1000000_k10_r100_b60000.txt