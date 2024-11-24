set -euxo

cargo build --release --bin secondary_index_bench
./target/release/secondary_index_bench -n 100000 -k 100 -r 1      > t1_n100000_k100_r1_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 5      > t1_n100000_k100_r5_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 10     > t1_n100000_k100_r10_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 50     > t1_n100000_k100_r50_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 100    > t1_n100000_k100_r100_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 500    > t1_n100000_k100_r500_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000   > t1_n100000_k100_r1000_b100000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 5000   > t1_n100000_k100_r5000_b100000.txt
echo "Done"