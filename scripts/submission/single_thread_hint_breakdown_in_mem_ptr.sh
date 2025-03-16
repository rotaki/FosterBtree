set -euxo

# Figure 4. Secondary Index Lookup Latency with Different Hint Types Under Varying Record Size
cargo build --release --bin secondary_index_bench_ptr
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 10     > ptr_t1_n1000000_k10_r10_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 100    > ptr_t1_n1000000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 500    > ptr_t1_n1000000_k10_r500_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 1000   > ptr_t1_n1000000_k10_r1000_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 2000   > ptr_t1_n1000000_k10_r2000_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 4000   > ptr_t1_n1000000_k10_r4000_b600000.txt
echo "Done"