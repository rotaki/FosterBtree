cargo build --release --bin secondary_index_bench_ptr
./target/release/secondary_index_bench_ptr -b 600000 -n 10000 -k 10 -r 100      > numkeys_ptr_t1_n10000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 100000 -k 10 -r 100     > numkeys_ptr_t1_n100000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 500000 -k 10 -r 100     > numkeys_ptr_t1_n500000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 1000000 -k 10 -r 100    > numkeys_ptr_t1_n1000000_k10_r100_b600000.txt
./target/release/secondary_index_bench_ptr -b 600000 -n 5000000 -k 10 -r 100    > numkeys_ptr_t1_n5000000_k10_r100_b600000.txt
echo "Done"