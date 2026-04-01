Yes. That is the right **minimal experiment**.

You want exactly these three:

1. **Lookup**
2. **Lookup Sorted**
3. **Lookup Sorted + Vectorized Execution (Ours)**

This isolates:

* benefit of batching alone,
* benefit of sorting alone,
* benefit of your execution model beyond sorting.

---

# Goal of the experiment

Measure whether:

> sorting helps, and whether your vectorized batched traversal helps **beyond** sorting.

That is the cleanest first result.

---

# Method 1: Lookup

## Definition

Use your existing tree’s normal lookup path.

For each query:

* call standard lookup independently
* do not sort queries

## Pseudocode

```rust id="bv7i6r"
for q in &queries {
    results[q.qid] = tree.lookup(q.key);
}
```

## What it measures

Baseline cost of ordinary point lookups.

---

# Method 2: Lookup Sorted

## Definition

Sort the query batch by key first, but still perform **ordinary lookup** for each query independently.

So this is still:

* one query at a time
* one root-to-leaf traversal at a time
* one binary search per node per query

Only the order of queries changes.

## Pseudocode

```rust id="nkh7zt"
sorted_queries.sort_by_key(|q| q.key);

for q in &sorted_queries {
    results[q.qid] = tree.lookup(q.key);
}
```

## What it measures

How much gain comes from:

* better cache locality,
* repeated traversal of nearby paths,
* branch predictor effects,

**without** changing the lookup algorithm.

This is a very important baseline.

---

# Method 3: Lookup Sorted + Vectorized Execution (Ours)

## Definition

Sort queries by key, then traverse the tree in **ranges**, not one query at a time.

At each node:

* input is a **contiguous sorted slice** of queries
* node partitions that slice across children using a merge-like pass
* recurse on each nonempty child range

At leaves:

* merge leaf keys with query slice to produce answers

This is your method.

---

# What to implement

Use a query struct like:

```rust id="gud5l3"
#[derive(Clone, Copy)]
struct Query<K> {
    key: K,
    qid: usize,
}
```

And result buffer:

```rust id="mjm9kl"
let mut results: Vec<Option<Value>> = vec![None; queries.len()];
```

---

# Benchmark driver

For each batch size:

* generate queries
* run method 1
* run method 2
* run method 3
* assert outputs are equal
* print timing

---

# Exact experiment plan

## Step 1: Pick batch sizes

Start with:

* 1K
* 4K
* 16K
* 64K
* 256K

That is enough for the first pass.

## Step 2: Pick query types

Use one easy workload first:

* random existing keys only

Then optionally:

* 50% misses
* clustered keys

But for the first experiment, just use existing keys.

## Step 3: Repeat each run

Run each method maybe:

* 5 warmups
* 10 measured runs

Take median or average.

---

# What to measure

For each method measure:

* total elapsed time
* ns/query
* lookups/sec

For methods 2 and 3, measure sorting separately too:

* `sort_time`
* `execution_time`
* `sort + execution`

This matters because otherwise method 3 can look artificially good if you ignore sorting.

So report:

### Method 1

* lookup time

### Method 2

* sort time
* sorted lookup time
* total time

### Method 3

* sort time
* vectorized traversal time
* total time

---

# What the results mean

## If 2 beats 1

Sorting alone helps.

## If 3 beats 2

Your execution model adds value beyond sorting.

## If 3 beats 2 only when excluding sort

Then your traversal is better, but sorting overhead may make the full method unattractive unless queries are already sorted or naturally batched.

## If 3 does not beat 2

Then your vectorized traversal is probably not worthwhile.

---

# How to explain each line

A nice interpretation is:

* **1 → 2** = benefit of ordering
* **2 → 3** = benefit of vectorized traversal

That is exactly what you want.

---

# Simplest implementation of method 3

Assume your tree gives access to:

* whether node is leaf
* internal separators
* child pointers
* leaf keys / values

Then implement:

```rust id="9em97b"
fn process_node(
    node: &Node,
    queries: &[Query<Key>],
    begin: usize,
    end: usize,
    results: &mut [Option<Value>],
) {
    if node.is_leaf() {
        process_leaf(node, queries, begin, end, results);
    } else {
        process_internal(node, queries, begin, end, results);
    }
}
```

## Internal node

Partition sorted `queries[begin..end)` across children.

## Leaf

Merge sorted query slice with sorted leaf keys.

That is all you need.

---

# Minimal benchmark skeleton

```rust id="ekh54i"
fn run_benchmark(tree: &Tree, keys: Vec<Key>) {
    let queries: Vec<Query<Key>> = keys.into_iter()
        .enumerate()
        .map(|(qid, key)| Query { key, qid })
        .collect();

    let mut res1 = vec![None; queries.len()];
    let mut res2 = vec![None; queries.len()];
    let mut res3 = vec![None; queries.len()];

    // 1. lookup
    let t1 = Instant::now();
    for q in &queries {
        res1[q.qid] = tree.lookup(q.key);
    }
    let time1 = t1.elapsed();

    // 2. lookup sorted
    let mut sorted2 = queries.clone();
    let t2s = Instant::now();
    sorted2.sort_by_key(|q| q.key);
    let sort2 = t2s.elapsed();

    let t2 = Instant::now();
    for q in &sorted2 {
        res2[q.qid] = tree.lookup(q.key);
    }
    let exec2 = t2.elapsed();

    // 3. lookup sorted + vectorized
    let mut sorted3 = queries.clone();
    let t3s = Instant::now();
    sorted3.sort_by_key(|q| q.key);
    let sort3 = t3s.elapsed();

    let t3 = Instant::now();
    process_node(tree.root(), &sorted3, 0, sorted3.len(), &mut res3, tree);
    let exec3 = t3.elapsed();

    assert_eq!(res1, res2);
    assert_eq!(res1, res3);

    println!("1 lookup: {:?}", time1);
    println!("2 lookup sorted: sort={:?}, exec={:?}, total={:?}", sort2, exec2, sort2 + exec2);
    println!("3 ours: sort={:?}, exec={:?}, total={:?}", sort3, exec3, sort3 + exec3);
}
```

---

# What to call method 3

I would not call it just “vectorized execution” in code, because that can sound like SIMD.

A safer internal name is:

* `lookup_sorted_partitioned`
* `lookup_sorted_batched`
* `lookup_sorted_vectorized`

For the paper, you can say “vectorized batched traversal,” but in code I’d prefer `partitioned`.

---

# Final experiment table

You want a table like this:

| batch size | lookup | lookup sorted | lookup sorted + ours |
| ---------- | -----: | ------------: | -------------------: |
| 1K         |    ... |           ... |                  ... |
| 4K         |    ... |           ... |                  ... |
| 16K        |    ... |           ... |                  ... |
| 64K        |    ... |           ... |                  ... |
| 256K       |    ... |           ... |                  ... |

And ideally show both:

* execution only
* total including sort

---

# Strongest expected outcome

You hope to see:

* `lookup sorted` slightly better than `lookup`
* `lookup sorted + ours` significantly better than `lookup sorted`

That would be the cleanest evidence.

---

# Recommendation

Start with:

* single-threaded
* all keys exist
* batch sizes 1K to 256K
* measure total time and total+sort

That is the simplest experiment worth doing.

If you want, next I can write the exact `process_internal` and `process_leaf` pseudocode for method 3.

Yes — for method 3, define it so that **every node is processed with a merge-style pass**, not per-key search.

That means method 3 should be:

> **Lookup Sorted + Vectorized Execution**
> sorted queries enter the root, and at **every internal node and leaf** you process the node with a merge-like scan over two sorted sequences.

Here is the clean experiment definition.

---

# The 3 methods

## 1. Lookup

Unsorted queries, normal lookup.

For each query:

* start at root
* binary search each internal node
* binary search leaf

This is your baseline.

---

## 2. Lookup Sorted

Sort queries first, but still do normal lookup.

For each sorted query:

* start at root
* binary search each internal node
* binary search leaf

This isolates the benefit of sorting alone.

---

## 3. Lookup Sorted + Vectorized Execution (Ours)

Sort queries first, then process them **as ranges**.

At every node:

* node keys/separators are sorted
* incoming query slice is sorted
* do a **merge-style pass**
* emit child query ranges

At every leaf:

* leaf keys are sorted
* incoming query slice is sorted
* do a **merge-style pass**
* fill results

So yes: **each node is merge join**.

---

# Exact rule for method 3

## Internal node = merge-partition

Inputs:

* sorted query slice `Q[begin..end)`
* sorted separators `S[0..m-1]`

Operation:

* scan `Q` once
* scan `S` once
* partition `Q` into contiguous subranges for each child

This is the internal-node “merge join.”

It is really a partitioning merge:

* all queries `< S[0]` go to child 0
* all queries `>= S[0] && < S[1]` go to child 1
* ...
* all remaining go to last child

No binary search allowed here.

## Leaf = merge probe

Inputs:

* sorted query slice
* sorted leaf keys

Operation:

* scan both left to right
* if query key < leaf key: result is miss
* if query key > leaf key: advance leaf
* if equal: write match

No binary search allowed here either.

So the whole method is consistently merge-based.

---

# Minimal pseudocode

## Driver

```rust
fn lookup_sorted_vectorized(tree: &Tree, queries: &mut [Query], results: &mut [Option<Value>]) {
    queries.sort_by_key(|q| q.key);
    process_node(tree.root(), queries, 0, queries.len(), results, tree);
}
```

---

## Internal node: merge join / partition

```rust
fn process_internal(
    node: &InternalNode,
    queries: &[Query],
    begin: usize,
    end: usize,
    results: &mut [Option<Value>],
    tree: &Tree,
) {
    let seps = &node.separators;
    let children = &node.children;

    let mut qi = begin;

    for i in 0..seps.len() {
        let start = qi;
        while qi < end && queries[qi].key < seps[i] {
            qi += 1;
        }

        if start < qi {
            process_node(children[i], queries, start, qi, results, tree);
        }
    }

    if qi < end {
        process_node(children[seps.len()], queries, qi, end, results, tree);
    }
}
```

That is the internal-node merge operation.

---

## Leaf node: merge join / probe

```rust
fn process_leaf(
    leaf: &LeafNode,
    queries: &[Query],
    begin: usize,
    end: usize,
    results: &mut [Option<Value>],
) {
    let keys = &leaf.keys;
    let vals = &leaf.values;

    let mut qi = begin;
    let mut li = 0;

    while qi < end && li < keys.len() {
        if queries[qi].key < keys[li] {
            results[queries[qi].qid] = None;
            qi += 1;
        } else if queries[qi].key > keys[li] {
            li += 1;
        } else {
            results[queries[qi].qid] = Some(vals[li].clone());
            qi += 1;
        }
    }

    while qi < end {
        results[queries[qi].qid] = None;
        qi += 1;
    }
}
```

That is the leaf merge operation.

---

## Dispatcher

```rust
fn process_node(
    node_id: NodeId,
    queries: &[Query],
    begin: usize,
    end: usize,
    results: &mut [Option<Value>],
    tree: &Tree,
) {
    match tree.node(node_id) {
        Node::Internal(node) => process_internal(node, queries, begin, end, results, tree),
        Node::Leaf(leaf) => process_leaf(leaf, queries, begin, end, results),
    }
}
```

---

# What to benchmark

For each batch size:

* generate queries
* run method 1
* run method 2
* run method 3
* assert same results

Use:

* 1K
* 4K
* 16K
* 64K
* 256K

Start with existing keys only.

---

# What to measure

Measure:

## Method 1

* total lookup time

## Method 2

* sort time
* lookup time
* total

## Method 3

* sort time
* vectorized traversal time
* total

This gives:

* `1 -> 2`: benefit of sorting
* `2 -> 3`: benefit of merge-at-each-node execution

---

# Important implementation constraint

For method 3, be disciplined:

* **no binary search inside any node**
* internal nodes must use merge partition
* leaves must use merge probe

Otherwise the comparison gets blurry.

---

# Best naming

I would name the three methods:

* `lookup`
* `lookup_sorted`
* `lookup_sorted_merge`

or

* `lookup`
* `lookup_sorted`
* `lookup_sorted_vectorized`

If you want to emphasize your point, `lookup_sorted_merge` is the clearest.

---

# The exact experiment statement

You can describe it like this:

> We compare three lookup modes:
> (1) ordinary per-key lookup,
> (2) sorted per-key lookup, and
> (3) sorted vectorized lookup, where each internal node and leaf processes its incoming query batch using a merge-style pass over sorted queries and sorted node contents.

That is precise and matches what you want.

If you want, I can next write a version that maps directly onto a typical Rust B-tree node API.
