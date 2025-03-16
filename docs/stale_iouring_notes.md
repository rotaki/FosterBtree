# IO-Uring Performance Notes

## IO-Uring without optimization (3 threads)

```text
 Running benches/random_insert.rs (target/release/deps/random_insert-681113c3e980476b)
Benchmarking Random Insertion/In memory Foster BTree Initial Allocation: Collecting 10 samples in estima
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [202.47 ns 203.03 ns 203.95 ns]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.4s.
Benchmarking Random Insertion/In memory Foster BTree Insertion: Collecting 10 samples in estimated 5.428
Random Insertion/In memory Foster BTree Insertion
                        time:   [533.28 ms 538.21 ms 544.52 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Collecting 10 samples in estima
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [296.20 ms 298.47 ms 301.01 ms]
Benchmarking Random Insertion/On disk Foster BTree Initial Allocation: Collecting 10 samples in estimate
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [15.590 ms 15.625 ms 15.701 ms]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 12.1s.
Benchmarking Random Insertion/On disk Foster BTree Insertion: Collecting 10 samples in estimated 12.057 
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.2048 s 1.2131 s 1.2242 s]
Found 3 outliers among 10 measurements (30.00%)
  2 (20.00%) low mild
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 9.7s.
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Collecting 10 samples in estimate
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [967.89 ms 974.13 ms 981.94 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 10 samples in estimated 8.7656 s (20 iterat
Random Insertion/BTreeMap Insertion
                        time:   [456.05 ms 481.94 ms 515.54 ms]
```


## IO-Uring with kernel thread polling (3-threads)

```text
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [201.42 ns 201.97 ns 202.87 ns]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.3s.
Benchmarking Random Insertion/In memory Foster BTree Insertion: Collecting 10 s
Random Insertion/In memory Foster BTree Insertion
                        time:   [525.33 ms 528.20 ms 531.24 ms]
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Warmin
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Collec
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Analyz
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [293.73 ms 295.66 ms 297.32 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) low mild
Benchmarking Random Insertion/On disk Foster BTree Initial Allocation: Warming 
Benchmarking Random Insertion/On disk Foster BTree Initial Allocation: Collecti
Benchmarking Random Insertion/On disk Foster BTree Initial Allocation: Analyzin
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [15.505 ms 15.549 ms 15.633 ms]
Found 2 outliers among 10 measurements (20.00%)
  2 (20.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 11.2s.
Benchmarking Random Insertion/On disk Foster BTree Insertion: Collecting 10 sam
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.0895 s 1.0994 s 1.1071 s]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) low mild
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 8.8s.
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Collecti
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Analyzin
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [880.78 ms 890.11 ms 899.93 ms]
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 10 samples in esti
Random Insertion/BTreeMap Insertion
                        time:   [446.31 ms 448.79 ms 451.56 ms]
```


## IO-Uring with kernel thread polling + Registering file and buffer (3-threads)

```text
     Running benches/random_insert.rs (target/release/deps/random_insert-0f5779134b006542)
Benchmarking Random Insertion/In memory Foster BTree Initial Allocation: Collecting 10 samples in estima
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [196.35 ns 196.84 ns 197.85 ns]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.3s.
Benchmarking Random Insertion/In memory Foster BTree Insertion: Collecting 10 samples in estimated 5.320
Random Insertion/In memory Foster BTree Insertion
                        time:   [513.16 ms 519.29 ms 527.97 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion Parallel: Collecting 10 samples in estima
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [289.05 ms 291.80 ms 294.54 ms]
Benchmarking Random Insertion/On disk Foster BTree Initial Allocation: Collecting 10 samples in estimate
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [15.573 ms 15.604 ms 15.642 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 10.6s.
Benchmarking Random Insertion/On disk Foster BTree Insertion: Collecting 10 samples in estimated 10.553 
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.0344 s 1.0441 s 1.0540 s]
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 8.3s.
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Collecting 10 samples in estimate
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [831.90 ms 841.22 ms 852.22 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high mild
Benchmarking Random Insertion/BTreeMap Insertion: Collecting 10 samples in estimated 8.5908 s (20 iterat
Random Insertion/BTreeMap Insertion
                        time:   [440.30 ms 443.21 ms 446.98 ms]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
```


## IO-Uring with Registering file and buffer (3-threads)

```text
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [194.06 ns 194.51 ns 195.27 ns]
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.4s.
Random Insertion/In memory Foster BTree Insertion
                        time:   [520.10 ms 523.73 ms 527.68 ms]
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [295.38 ms 297.36 ms 299.65 ms]
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [15.419 ms 15.443 ms 15.491 ms]
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 12.0s.
Random Insertion/On disk Foster BTree Insertion
                        time:   [1.1960 s 1.2067 s 1.2174 s]
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 9.5s.
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [938.91 ms 946.70 ms 955.47 ms]
Random Insertion/BTreeMap Insertion
                        time:   [437.69 ms 439.52 ms 441.55 ms]
```