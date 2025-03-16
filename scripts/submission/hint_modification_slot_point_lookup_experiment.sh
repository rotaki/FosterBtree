set -euxo 
cargo build --release --bin secondary_index_point_read_with_slot_hint_modification

# Figure 8. Latency of Key Lookup Operations Mixed With Hint Modification Operations

################ CHECK THE second_index_point_read_with_slot_hint_modification.rs for the 
################ correct parameters of hint modification (Page 50 %, Frame 10 %, Slot 40 %)
##################################### SKEW 0.0 #####################################

./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 0  > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m0.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 20 > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m20.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 40 > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m40.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 60 > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m60.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 80 > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m80.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -m 99 > slot_modification_read_t1_n1000000_k10_r100_b60000_s0_m99.txt
 
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 0  > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m0.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 20 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m20.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 40 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m40.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 60 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m60.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 80 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m80.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 --repair-type 2 -m 99 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m99.txt
# 
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 0  > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m0.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 20 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m20.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 40 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m40.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 60 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m60.txt
./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 80 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m80.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0 -h 0 -m 99 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s0_m99.txt


##################################### SKEW 0.99 #####################################

# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 0  > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m0.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 20 > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m20.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 40 > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m40.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 60 > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m60.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 80 > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m80.txt
# # ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -m 99 > slot_modification_read_t1_n1000000_k10_r100_b60000_s99_m99.txt

# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 0  > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s99_m0.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 20 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s99_m20.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 40 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s99_m40.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 60 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s99_m60.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 80 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s99_m80.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 --repair-type 2 -m 99 > slot_modification_read_no_repair_t1_n1000000_k10_r100_b60000_s0_m99.txt

# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 0  > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m0.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 20 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m20.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 40 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m40.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 60 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m60.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 80 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m80.txt
# ./target/release/secondary_index_point_read_with_slot_hint_modification -b 60000 -n 1000000 -k 10 -r 100 -w 20 -e 60 -s 0.99 -h 0 -m 99 > slot_modification_read_no_hint_t1_n1000000_k10_r100_b60000_s99_m99.txt