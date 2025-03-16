set -euxo

cargo build --release --bin secondary_index_bench_ptr
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 1000   > ptr_t1_n1000000_k10_r100_b1000.txt
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 2000   > ptr_t1_n1000000_k10_r100_b2000.txt
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 4000   > ptr_t1_n1000000_k10_r100_b4000.txt
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 8000   > ptr_t1_n1000000_k10_r100_b8000.txt
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 16000  > ptr_t1_n1000000_k10_r100_b16000.txt
./target/release/secondary_index_bench_ptr -n 1000000 -k 10 -r 100 -b 32000  > ptr_t1_n1000000_k10_r100_b32000.txt
echo "Done"