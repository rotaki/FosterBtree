set -euxo

cargo build --release --bin secondary_index_bench
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 500    > t1_n100000_k100_r1000_b500.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 1000   > t1_n100000_k100_r1000_b1000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 2000   > t1_n100000_k100_r1000_b2000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 3000   > t1_n100000_k100_r1000_b3000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 4000   > t1_n100000_k100_r1000_b4000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 5000   > t1_n100000_k100_r1000_b5000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 10000  > t1_n100000_k100_r1000_b10000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 15000  > t1_n100000_k100_r1000_b15000.txt
./target/release/secondary_index_bench -n 100000 -k 100 -r 1000 -b 20000  > t1_n100000_k100_r1000_b20000.txt
echo "Done"