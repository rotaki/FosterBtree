set -euxo

cargo build --release --bin secondary_index_bench_ptr_worst_case
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 1      > worst_case_ptr_t1_n100000_k100_r1_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 5      > worst_case_ptr_t1_n100000_k100_r5_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 10     > worst_case_ptr_t1_n100000_k100_r10_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 50     > worst_case_ptr_t1_n100000_k100_r50_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 100    > worst_case_ptr_t1_n100000_k100_r100_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 500    > worst_case_ptr_t1_n100000_k100_r500_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 1000   > worst_case_ptr_t1_n100000_k100_r1000_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 2000   > worst_case_ptr_t1_n100000_k100_r2000_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 3000   > worst_case_ptr_t1_n100000_k100_r3000_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 4000   > worst_case_ptr_t1_n100000_k100_r4000_b100000.txt
./target/release/secondary_index_bench_ptr_worst_case -n 100000 -k 100 -r 5000   > worst_case_ptr_t1_n100000_k100_r5000_b100000.txt
echo "Done"