set -euxo 

cargo build --release --bin secondary_index_scan_with_insert

./target/release/secondary_index_scan_with_insert -b 600000 -n 1000000 -k 10 -r 100 --repair-type 0    > scan_t1_n1000000_k10_r100_b600000_full_repair.txt
./target/release/secondary_index_scan_with_insert -b 600000 -n 1000000 -k 10 -r 100 --repair-type 1    > scan_t1_n1000000_k10_r100_b600000_ignore_slot_repair.txt
./target/release/secondary_index_scan_with_insert -b 600000 -n 1000000 -k 10 -r 100 --repair-type 2    > scan_t1_n1000000_k10_r100_b600000_no_repair.txt