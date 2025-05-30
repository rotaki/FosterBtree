set -euxo

# Figure 4. Secondary Index Lookup Latency with Different Hint Types Under Varying Record Size
cargo build --release --bin secondary_index_bench_ptr --features "bp_clock"
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 100    > ptr_t1_n1000000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 300    > ptr_t1_n1000000_k10_r300_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 500    > ptr_t1_n1000000_k10_r500_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 1000   > ptr_t1_n1000000_k10_r1000_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 1500   > ptr_t1_n1000000_k10_r1500_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 2000   > ptr_t1_n1000000_k10_r2000_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 2500   > ptr_t1_n1000000_k10_r2500_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 3000   > ptr_t1_n1000000_k10_r3000_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 3500   > ptr_t1_n1000000_k10_r3500_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 4000   > ptr_t1_n1000000_k10_r4000_b600000.txt
echo "Done"