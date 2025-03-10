set -euxo

cargo build --release --bin secondary_index_stability_hint

# ./target/release/secondary_index_stability_hint -b 60000 -n 1000000 -k 10 -r 100 > hint_stability_t1_n1000000_k10_r100_b60000.txt
./target/release/secondary_index_stability_hint -b 16580 -n 1000000 -k 10 -r 100 > ondisk_hint_stability_t1_n1000000_k10_r100_b60000.txt