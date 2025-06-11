set -euxo 

cargo build --release --bin fbt_scan_latency
# 
./target/release/fbt_scan_latency -r 50   > fbt_scan_latency_50.txt
./target/release/fbt_scan_latency -r 100  > fbt_scan_latency_100.txt
./target/release/fbt_scan_latency -r 200  > fbt_scan_latency_200.txt
./target/release/fbt_scan_latency -r 500  > fbt_scan_latency_500.txt
./target/release/fbt_scan_latency -r 1000 > fbt_scan_latency_1000.txt
./target/release/fbt_scan_latency -r 2000 > fbt_scan_latency_2000.txt
./target/release/fbt_scan_latency -r 3000 > fbt_scan_latency_3000.txt
./target/release/fbt_scan_latency -r 4000 > fbt_scan_latency_4000.txt
# 
# 
cargo build --release --bin hs_scan_latency
# 
./target/release/hs_scan_latency -r 50   > hs_scan_latency_50.txt
./target/release/hs_scan_latency -r 100  > hs_scan_latency_100.txt
./target/release/hs_scan_latency -r 200  > hs_scan_latency_200.txt
./target/release/hs_scan_latency -r 500  > hs_scan_latency_500.txt
./target/release/hs_scan_latency -r 1000 > hs_scan_latency_1000.txt
./target/release/hs_scan_latency -r 2000 > hs_scan_latency_2000.txt
./target/release/hs_scan_latency -r 3000 > hs_scan_latency_3000.txt
./target/release/hs_scan_latency -r 4000 > hs_scan_latency_4000.txt
# 
# 
cargo build --release --bin aps_scan_latency
# 
./target/release/aps_scan_latency -r 50   > aps_scan_latency_50.txt
./target/release/aps_scan_latency -r 100  > aps_scan_latency_100.txt
./target/release/aps_scan_latency -r 200  > aps_scan_latency_200.txt
./target/release/aps_scan_latency -r 500  > aps_scan_latency_500.txt
./target/release/aps_scan_latency -r 1000 > aps_scan_latency_1000.txt
./target/release/aps_scan_latency -r 2000 > aps_scan_latency_2000.txt
./target/release/aps_scan_latency -r 3000 > aps_scan_latency_3000.txt
./target/release/aps_scan_latency -r 4000 > aps_scan_latency_4000.txt
# 
# 
cargo build --release --bin fss_scan_latency
# 
./target/release/fss_scan_latency -r 50   > fss_scan_latency_50.txt
./target/release/fss_scan_latency -r 100  > fss_scan_latency_100.txt
./target/release/fss_scan_latency -r 200  > fss_scan_latency_200.txt
./target/release/fss_scan_latency -r 500  > fss_scan_latency_500.txt
./target/release/fss_scan_latency -r 1000 > fss_scan_latency_1000.txt
./target/release/fss_scan_latency -r 2000 > fss_scan_latency_2000.txt
./target/release/fss_scan_latency -r 3000 > fss_scan_latency_3000.txt
./target/release/fss_scan_latency -r 4000 > fss_scan_latency_4000.txt


cargo build --release --bin vec_scan_latency

./target/release/vec_scan_latency -r 50   > vec_scan_latency_50.txt
./target/release/vec_scan_latency -r 100  > vec_scan_latency_100.txt
./target/release/vec_scan_latency -r 200  > vec_scan_latency_200.txt
./target/release/vec_scan_latency -r 500  > vec_scan_latency_500.txt
./target/release/vec_scan_latency -r 1000 > vec_scan_latency_1000.txt
./target/release/vec_scan_latency -r 2000 > vec_scan_latency_2000.txt
./target/release/vec_scan_latency -r 3000 > vec_scan_latency_3000.txt
./target/release/vec_scan_latency -r 4000 > vec_scan_latency_4000.txt
