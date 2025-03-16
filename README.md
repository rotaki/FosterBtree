# Storage Engine in Rust

This is a research project to build a persistent storage engine in Rust. The goal is to build a storage engine that is fast and efficient for modern hardware.

## Design

The storage engine is designed to be modular and extensible. The storage engine is composed of the following components from the bottom up:

* **File Manager**: Manages files on disk and provides direct I/O for reading and writing.
* **Buffer Pool**: Manages pages in memory and provides a cache for disk pages.
* **Access Method**: Provides different access methods for data storage.
  * **Foster B-Tree**: A write-optimized B-Tree that supports efficient concurrent operations.
  * **Hash Index**: A hash index that supports efficient point lookups.
* **Transaction Manager**: Provides transaction support for the storage engine.
  * **2PL**: Two-phase locking with no-wait (immediate abort).
* **Benchmarking**: Provides TPC-C and YCSB benchmarking for the storage engine.

The storage engine incorporates an optimization called **LIPAH (Logical ID with Physical Address Hinting)** to reduce the overhead of indirection layers in the storage engine.

## Features

### File Manager

* [x] pread/pwrite with/without direct I/O
* [x] iouring with/without direct I/O

### Buffer Pool

* [x] Buffer pool with LRU eviction policy
* [x] Concurrent page access with reader-writer latch
* [x] LIPAH integration for buffer pool

### Foster B-Tree

* [x] Foster B-Tree with page split and merge
* [x] Concurrent operations with latch coupling (crabbing)
* [x] LIPAH integration for foster b-tree
* [x] Visualization of foster b-tree with wasm

### Hash Index

* [x] Hash index with chaining
* [x] LIPAH integration for hash index

### Transaction

* [x] 2PL with no-wait (immediate abort)
* [x] TPC-C and YCSB benchmarking
* [x] LIPAH integration for transaction
* [ ] Logging and recovery

## Testing, Debugging, Profiling, and Benchmarking

To run tests, run `cargo test`. To run a binary, build with `cargo build --release --bin <binary>` and run with `./target/release/<binary>`. To benchmark, see `scripts` directory. There are scripts for running TPC-C and YCSB benchmarks. The easiest way to run is to create a symlink to the shell script in the `scripts` directory in the root directory. Some scripts might not work as expected due to lack of maintenance.

### Visualize Foster B-Tree

```sh
wasm-pack build --target web
python3 -m http.server
```

Then open `http://localhost:8000` in your browser.
May need to comment out `criterion` in `Cargo.toml` to build for wasm.
This is NOT working right now due to the incompatibility of wasm and io-uring library.

### Multi-thread logger

See `logger.rs` and

```sh
cargo build --features "log_trace" # or log_debug, log_info, log_warn, log_error
```

### Stats

```sh
cargo build --features stat
```

### Perf

```sh
cargo build --target x86_64-unknown-linux-gnu --release --bin on_disk
perf record -e cycles -g --call-graph dwarf ./target/x86_64-unknown-linux-gnu/release/main
hotspot perf.data
```

if sudo is needed, set `echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid` and `echo 0 | sudo tee /proc/sys/kernel/kptr_restrict`.
To install `perf`, `apt-get install linux-tools-common linux-tools-generic linux-tools-`uname -r``
To install `hotspot`, `apt-get install hotspot`

### Heaptrack

Heaptrack is a heap memory profiler. To install, `apt-get install heaptrack heaptrack-gui`.
To profile, `heaptrack <binary> <my params>`. This will open a gui to analyze the heap memory usage.
To open the gui later, `heaptrack_gui <heaptrack.log>`
