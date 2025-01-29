set -euxo

cargo build --release --bin secondary_index_bench_ptr_worst_case
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 1000   > worst_case_ptr_t1_n1000000_k10_r100_b1000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 2000   > worst_case_ptr_t1_n1000000_k10_r100_b2000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 4000   > worst_case_ptr_t1_n1000000_k10_r100_b4000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 8000   > worst_case_ptr_t1_n1000000_k10_r100_b8000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 16000  > worst_case_ptr_t1_n1000000_k10_r100_b16000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 1000000 -k 10 -r 100 -b 32000  > worst_case_ptr_t1_n1000000_k10_r100_b32000.txt
echo "Done"