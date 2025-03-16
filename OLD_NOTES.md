# TODO

## Buffer pool

* [x] Add reader-writer latch
* [x] Add container file mapping
* [x] Write the evicted page to disk if dirty
* [x] Add logger for debugging
* [x] Add cache replacement implementations
  * [x] Add more eviction algo (SIEVE)
* [x] Add write-ahead log, page lsn
* [x] Add pointer swizzling

## Foster B-Tree

* [x] Add Foster B-tree Page
  * [x] Add insert operator
  * [x] Add get operator
  * [x] Add delete operator
  * [x] Add upsert operator
  * [x] Add statistics for each structure modification
    * [x] Revise the split decision
  * [x] Add thread-unsafe page traversal operator
    * [x] Add consistency checker
  * [x] Fix the buffer pool free frame allocation bug
  * [x] Add memory usage profiler
    * [x] Heaptrack for memory profiling
  * [x] Test with bp (limited memory pool with disk offloading)
  * [x] Add range scan operator
  * [] Reuse the removed page in merge
  * [] Improve page binary search
  * [] Add prefix compression in pages
  * [] Asynchronous page read/write
  * [x] Add page split into three pages
  * [] Add better latch for bp
  * [] Add transactional support
  * [] Optimistic lock coupling with hybrid latches with page versioning.
  * [] Add ghost record support for transaction support

### Open Questions

* [] How many threads are needed to get comparable performance with a single-threaded execution?

## Add Logging

* [x] Add log buffer with append and flush capabilities



## Benchmarking notes

```sh
cargo bench
```

### Result (temp)

#### Test scenario

Measured the time taken to insert kvs.

```text
unique_keys: true,
num_threads: 3 (when run in parallel),
num_keys: 500000,
key_size: 100,
val_min_size: 50,
val_max_size: 100,
bp_size: 10000 (when run on-disk)
```

```text
Random Insertion/In memory Foster BTree Initial Allocation
                        time:   [206.71 ns 207.27 ns 208.37 ns]
                        change: [+2.9012% +5.2609% +7.6219%] (p = 0.00 < 0.05)
                        Performance has regressed.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Benchmarking Random Insertion/In memory Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 5.3s.
Random Insertion/In memory Foster BTree Insertion
                        time:   [526.47 ms 529.48 ms 533.39 ms]
                        change: [-0.0672% +0.5119% +1.3174%] (p = 0.22 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) high mild
  1 (10.00%) high severe
Random Insertion/In memory Foster BTree Insertion Parallel
                        time:   [285.02 ms 287.67 ms 290.63 ms]
                        change: [-2.6766% -1.5267% -0.4052%] (p = 0.02 < 0.05)
                        Change within noise threshold.
Random Insertion/On disk Foster BTree Initial Allocation
                        time:   [14.635 ms 14.659 ms 14.687 ms]
                        change: [-4.6366% -1.4302% +1.7827%] (p = 0.45 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 9.8s.
Random Insertion/On disk Foster BTree Insertion
                        time:   [960.74 ms 975.66 ms 1.0008 s]
                        change: [-4.6045% -2.4195% +0.9489%] (p = 0.10 > 0.05)
                        No change in performance detected.
Found 1 outliers among 10 measurements (10.00%)
  1 (10.00%) high severe
Benchmarking Random Insertion/On disk Foster BTree Insertion Parallel: Warming up for 3.0000 s
Warning: Unable to complete 10 samples in 5.0s. You may wish to increase target time to 7.3s.
Random Insertion/On disk Foster BTree Insertion Parallel
                        time:   [717.28 ms 724.93 ms 732.74 ms]
                        change: [-2.4263% -0.8722% +0.5856%] (p = 0.30 > 0.05)
                        No change in performance detected.
Random Insertion/BTreeMap Insertion
                        time:   [434.31 ms 440.89 ms 450.57 ms]
                        change: [-1.1270% +0.4669% +2.7757%] (p = 0.73 > 0.05)
                        No change in performance detected.
Found 2 outliers among 10 measurements (20.00%)
  1 (10.00%) low mild
  1 (10.00%) high severe
```
