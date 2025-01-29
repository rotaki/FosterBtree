set -euxo 

cargo build --release --bin secondary_index_multithread
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 1 >  ondisk_b5000_n100000_k100_r100_t1.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 4 >  ondisk_b5000_n100000_k100_r100_t4.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 8 >  ondisk_b5000_n100000_k100_r100_t8.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 12 > ondisk_b5000_n100000_k100_r100_t12.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 16 > ondisk_b5000_n100000_k100_r100_t16.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 20 > ondisk_b5000_n100000_k100_r100_t20.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 24 > ondisk_b5000_n100000_k100_r100_t24.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 28 > ondisk_b5000_n100000_k100_r100_t28.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 32 > ondisk_b5000_n100000_k100_r100_t32.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 36 > ondisk_b5000_n100000_k100_r100_t36.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 40 > ondisk_b5000_n100000_k100_r100_t40.txt1
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 44 > ondisk_b5000_n100000_k100_r100_t44.txt1
# 
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 1 >  ondisk_b5000_n100000_k100_r100_t1.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 4 >  ondisk_b5000_n100000_k100_r100_t4.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 8 >  ondisk_b5000_n100000_k100_r100_t8.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 12 > ondisk_b5000_n100000_k100_r100_t12.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 16 > ondisk_b5000_n100000_k100_r100_t16.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 20 > ondisk_b5000_n100000_k100_r100_t20.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 24 > ondisk_b5000_n100000_k100_r100_t24.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 28 > ondisk_b5000_n100000_k100_r100_t28.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 32 > ondisk_b5000_n100000_k100_r100_t32.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 36 > ondisk_b5000_n100000_k100_r100_t36.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 40 > ondisk_b5000_n100000_k100_r100_t40.txt2
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 44 > ondisk_b5000_n100000_k100_r100_t44.txt2
# 
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 1 >  ondisk_b5000_n100000_k100_r100_t1.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 4 >  ondisk_b5000_n100000_k100_r100_t4.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 8 >  ondisk_b5000_n100000_k100_r100_t8.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 12 > ondisk_b5000_n100000_k100_r100_t12.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 16 > ondisk_b5000_n100000_k100_r100_t16.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 20 > ondisk_b5000_n100000_k100_r100_t20.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 24 > ondisk_b5000_n100000_k100_r100_t24.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 28 > ondisk_b5000_n100000_k100_r100_t28.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 32 > ondisk_b5000_n100000_k100_r100_t32.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 36 > ondisk_b5000_n100000_k100_r100_t36.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 40 > ondisk_b5000_n100000_k100_r100_t40.txt3
./target/release/secondary_index_multithread -n 100000 -k 100 -r 100 -b 5000 -t 44 > ondisk_b5000_n100000_k100_r100_t44.txt3
