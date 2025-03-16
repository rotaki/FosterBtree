set -euxo 

# Figure 5. Secondary Index Lookup Latency Under Varying Number of Stale Hints

cargo build --release --bin secondary_index_bench_ptr_with_updates

./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 -h 0 > ptr_t1_n1000000_k10_r100_b600000_no_hint.txt

./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 0    > ptr_t1_n1000000_k10_r100_b600000_p0.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 10   > ptr_t1_n1000000_k10_r100_b600000_p10.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 20   > ptr_t1_n1000000_k10_r100_b600000_p20.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 30   > ptr_t1_n1000000_k10_r100_b600000_p30.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 40   > ptr_t1_n1000000_k10_r100_b600000_p40.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 50   > ptr_t1_n1000000_k10_r100_b600000_p50.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 60   > ptr_t1_n1000000_k10_r100_b600000_p60.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 70   > ptr_t1_n1000000_k10_r100_b600000_p70.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 80   > ptr_t1_n1000000_k10_r100_b600000_p80.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 90   > ptr_t1_n1000000_k10_r100_b600000_p90.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --page-hint-correctness 100  > ptr_t1_n1000000_k10_r100_b600000_p100.txt

./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 0   > ptr_t1_n1000000_k10_r100_b600000_f0.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 10  > ptr_t1_n1000000_k10_r100_b600000_f10.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 20  > ptr_t1_n1000000_k10_r100_b600000_f20.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 30  > ptr_t1_n1000000_k10_r100_b600000_f30.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 40  > ptr_t1_n1000000_k10_r100_b600000_f40.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 50  > ptr_t1_n1000000_k10_r100_b600000_f50.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 60  > ptr_t1_n1000000_k10_r100_b600000_f60.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 70  > ptr_t1_n1000000_k10_r100_b600000_f70.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 80  > ptr_t1_n1000000_k10_r100_b600000_f80.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 90  > ptr_t1_n1000000_k10_r100_b600000_f90.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --frame-hint-correctness 100 > ptr_t1_n1000000_k10_r100_b600000_f100.txt

./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 0    > ptr_t1_n1000000_k10_r100_b600000_s0.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 10   > ptr_t1_n1000000_k10_r100_b600000_s10.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 20   > ptr_t1_n1000000_k10_r100_b600000_s20.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 30   > ptr_t1_n1000000_k10_r100_b600000_s30.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 40   > ptr_t1_n1000000_k10_r100_b600000_s40.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 50   > ptr_t1_n1000000_k10_r100_b600000_s50.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 60   > ptr_t1_n1000000_k10_r100_b600000_s60.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 70   > ptr_t1_n1000000_k10_r100_b600000_s70.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 80   > ptr_t1_n1000000_k10_r100_b600000_s80.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 90   > ptr_t1_n1000000_k10_r100_b600000_s90.txt
./target/release/secondary_index_bench_ptr_with_updates -b 600000 -n 1000000 -k 10 -r 100 --slot-hint-correctness 100  > ptr_t1_n1000000_k10_r100_b600000_s100.txt