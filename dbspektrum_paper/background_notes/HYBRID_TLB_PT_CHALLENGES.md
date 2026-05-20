# Hybrid TLB+PT: Implementation Challenges

## The Naive Idea

```rust
fn get_page(page_key: PageKey) -> Frame {
    // L1: TLB
    if let Some(frame) = tlb_probe(page_key) {
        return frame;
    }

    // L2: PT preferred frame
    let pref = preferred_frame(page_key);
    if frame_has_page(pref, page_key) {
        tlb_insert(page_key, pref);
        return pref;
    }

    // L3: Overflow
    overflow.get(page_key)
}
```

**Sounds simple, right?**

## The Hard Problems

### Problem 1: Who Owns the Frame State?

**TLB-BP architecture**:
```
TLB: Caches (page_key → frame_id) mappings
Overflow table: Source of truth for page locations
Frames: Managed by FrameManager (shared)
```

**PT architecture**:
```
Preferred frames: Each page "wants" to be at specific frame
Overflow table: Pages that couldn't fit at preferred frame
Frames: Managed by FrameManager (shared)
Promotion: Moves pages from overflow → preferred frame
```

**Hybrid would need**:
```
TLB: Cache layer (page_key → frame_id)
PT preferred frames: Pages at "home" location
Overflow: Pages not at preferred frame
Frames: Shared FrameManager

Question: When TLB caches a frame, does it affect PT promotion logic?
```

### Problem 2: TLB Invalidation on Eviction

**Scenario**:
```
1. Thread 1: TLB caches (page_100 → frame_50)
2. Thread 2: Evicts frame_50 (to make room for page_200)
3. Thread 1: TLB still says page_100 is at frame_50 (STALE!)
4. Thread 1: Reads garbage data from frame_50 (now has page_200)
```

**Current TLB-BP solution**:
```rust
// TLB validation on every hit
if entry_tag(entry) == tag {
    let frame_id = entry_frame(entry);
    // Still need to check if frame actually has this page!
    if frame_meta(frame_id).key() == page_key {
        return Some(frame_id);
    }
}
```

**Problem**: This validation is EXPENSIVE! It's basically doing the same work as PT bucket validation.

If we validate TLB hits, we lose the "zero overhead L1 cache" benefit.

### Problem 3: Promotion Breaks TLB Consistency

**Scenario**:
```
Initial state:
- page_100 at frame_1000 (overflow)
- Thread 1 TLB: (page_100 → frame_1000)
- Thread 1 TLB: (page_100 → frame_1000)

PT promotion happens:
- Thread 2 promotes page_100 from frame_1000 → frame_50 (preferred)
- Updates overflow table: page_100 now at frame_50

Thread 1 accesses page_100:
- TLB hit! Says frame_1000
- But page_100 is now at frame_50
- TLB cache is STALE
```

**Need to invalidate TLB entries across ALL threads when promotion happens.**

**How?**

Option 1: **Global TLB shootdown** (like CPU TLB invalidation)
```rust
fn promote_page(page_key: PageKey, from: u32, to: u32) {
    // Move page from → to
    move_page(from, to);

    // Invalidate TLB on ALL threads
    for thread_id in 0..NUM_THREADS {
        send_tlb_shootdown(thread_id, page_key);
    }
}
```

**Problem**: This is SUPER expensive! Cross-thread communication, cache coherence traffic.

Option 2: **Generation counter** (like CPU)
```rust
static TLB_GENERATION: AtomicU64 = AtomicU64::new(0);

thread_local! {
    static TLB_GEN: Cell<u64> = Cell::new(0);
}

fn promote_page(...) {
    move_page(from, to);
    TLB_GENERATION.fetch_add(1, Ordering::Release);  // Invalidate all TLBs
}

fn tlb_probe(page_key) {
    if TLB_GEN.get() != TLB_GENERATION.load(Ordering::Acquire) {
        tlb_flush();  // Our TLB is stale
        TLB_GEN.set(TLB_GENERATION.load(Ordering::Acquire));
    }
    // ... normal lookup
}
```

**Problem**: Atomic load on EVERY access! This kills performance.

Option 3: **Validate on TLB hit** (current TLB-BP approach)
```rust
fn tlb_probe(page_key) {
    if entry_tag == tag {
        let frame = entry_frame;
        // Validate frame still has this page
        if frame_meta(frame).key() == page_key {
            return Some(frame);
        }
        // Stale entry, remove it
        tlb_invalidate(entry);
    }
}
```

**Problem**: This is what TLB-BP already does! No performance gain over standalone TLB-BP.

### Problem 4: Two Overflow Tables or One?

**Option A: Share overflow table**
```rust
struct Hybrid {
    tlb: ThreadLocal<TLB>,
    pt: PT,  // Has preferred frames + overflow table
}
```

**Problem**: PT's overflow table stores pages NOT at preferred frame. But with TLB cache, some pages might be:
- Not in TLB
- Not at preferred frame
- In overflow table

When TLB caches an overflow page, does it stay in overflow? What if PT tries to promote it later?

**Option B: Separate overflow tables**
```rust
struct Hybrid {
    tlb: ThreadLocal<TLB>,
    tlb_overflow: CongeeMap,  // Pages not in TLB
    pt_preferred: Vec<Frame>,  // PT preferred frames
    pt_overflow: CongeeMap,    // Pages not at preferred
}
```

**Problem**: Now you have TWO overflow tables to check! And pages can be in:
- TLB (cached)
- PT preferred frame
- PT overflow
- TLB overflow

**This is getting complicated...**

### Problem 5: Where Do New Pages Go?

**When creating a new page**:

**Option A: Put at PT preferred frame**
```rust
fn create_new_page(page_key) {
    let pref = preferred_frame(page_key);
    if pref is empty {
        place_at_preferred(pref);
    } else {
        place_in_overflow();
    }
}
```

**Question**: Should we also insert into TLB?
- YES → TLB filled with pages that might not be accessed (waste)
- NO → Next access will miss TLB (then hit PT preferred frame, then cache in TLB)

**Option B: Put in overflow, let TLB/PT discover**
```rust
fn create_new_page(page_key) {
    place_in_overflow();
    // Don't insert into TLB or PT preferred frame yet
    // Wait for first access to decide
}
```

**Question**: When does it get promoted to PT preferred frame?
- On first access? (might not have room)
- Gradually? (same convergence problem we're trying to solve)

### Problem 6: Eviction Policy

**When frame needs to be evicted**:

**TLB-BP**: Evict based on clock/LRU, no concept of "preferred"

**PT**: Prefer to evict from overflow, keep preferred frames occupied

**Hybrid**: Which policy?
- If we prefer to keep preferred frames → TLB cache is mostly overflow pages (low value)
- If we treat all frames equal → PT preferred frames get evicted (breaks PT)

### Problem 7: Thread-Local TLB, Shared PT

**TLB is thread-local**:
```
Thread 1 TLB: [page_1, page_5, page_10, ...]
Thread 2 TLB: [page_2, page_6, page_11, ...]
```

**PT preferred frames are shared**:
```
Preferred frame 100: page_100 (visible to all threads)
```

**Problem**: Different threads have different "view" of which pages are hot:
- Thread 1 thinks page_1, page_5, page_10 are hot (in its TLB)
- Thread 2 thinks page_2, page_6, page_11 are hot (in its TLB)

PT promotion is based on global access patterns, but TLB is local.

**How do we coordinate**?
- If Thread 1's TLB has page_100, should PT avoid promoting page_200 to frame_100?
- If PT promotes page_100 away, should it notify Thread 1's TLB?

**This coordination is expensive!**

## The Reality Check

### What We'd Actually Need

For a correct implementation:

1. **TLB validation on every hit** (to catch stale entries after promotion/eviction)
2. **Cross-thread coordination** (TLB shootdown or generation counter)
3. **Unified eviction policy** (that respects both TLB and PT invariants)
4. **Clear ownership model** (who decides where pages live?)

### Performance Impact

**Original claim**: 30% TLB hit at 10ns, 50% PT hit at 30ns, 20% overflow at 50ns → 28ns average

**Reality with validation**:
- 30% TLB hit: 10ns (lookup) + 5ns (validation) = 15ns
- 50% PT hit: 30ns
- 20% overflow: 50ns
- **Average: 0.30×15 + 0.50×30 + 0.20×50 = 4.5 + 15 + 10 = 29.5ns**

**Plus**:
- Generation counter check on every access: +2ns
- Occasional TLB flush: +variable

**New average**: ~32ns

**vs PT alone**: 36ns

**Improvement**: Only ~11%, not 22%

**And we haven't accounted for**:
- Complexity cost (bugs, maintenance)
- TLB shootdown overhead
- Coordination overhead

## Is It Worth It?

### Complexity Analysis

**Standalone PT**:
- One promotion algorithm
- One overflow table
- Clear ownership (preferred frame or overflow)

**Standalone TLB-BP**:
- Thread-local cache
- One overflow table
- Simple eviction

**Hybrid TLB+PT**:
- Two caching layers (TLB + PT)
- Coordination between layers
- TLB invalidation protocol
- Complex eviction policy
- Unclear ownership

**Complexity**: 3-4× higher

**Benefit**: Maybe 11% performance improvement (if we're lucky)

### The Honest Assessment

**You're right - it's hard to mix.**

The fundamental problem:
- **TLB wants to be a fast, thread-local, uncoordinated cache**
- **PT wants to be a global, coordinated, preferred-frame system**

Combining them requires:
- Making TLB coordinated (loses speed)
- Or making TLB uncoordinated but validated (loses speed)
- Complex invalidation protocol (adds overhead)

**The juice isn't worth the squeeze.**

## Alternative: Simpler Hybrids

### Option 1: TLB for Small Working Sets, PT for Large

```rust
fn choose_bp(working_set_estimate: usize) -> Arc<dyn BP> {
    if working_set_estimate < 10_000 {
        Arc::new(TlbBP::new(...))  // TLB hit rate > 40%
    } else {
        Arc::new(PT::new(...))      // Better coverage after convergence
    }
}
```

**Benefit**: No coordination needed, just pick the right tool

**Problem**: Need to estimate working set, can't switch mid-run

### Option 2: TLB Only for Preferred Frames

```rust
// TLB only caches entries that are at their preferred frame
fn tlb_insert_if_preferred(page_key, frame) {
    if frame == preferred_frame(page_key) {
        tlb_insert(page_key, frame);
    }
}
```

**Benefit**: No invalidation needed! Preferred frames are stable.

**Problem**: TLB only caches 4% of pages (the ones already at preferred frame - not useful)

### Option 3: Give Up on Hybrid

Just pick one:
- **Small working sets**: TLB-BP (high hit rate)
- **Large working sets**: PT-ophash (after convergence)
- **Cloud-native**: TLB-BP (no convergence delay)
- **Traditional**: PT-ophash (better equilibrium)

**Benefit**: Simple, easy to reason about

**Problem**: No "best of both worlds"

## Conclusion

**You're absolutely right.** Mixing TLB and PT is hard because:

1. **Invalidation problem**: TLB caches stale after promotion/eviction
2. **Coordination overhead**: Need cross-thread communication
3. **Complexity**: 3-4× more complex than either alone
4. **Diminishing returns**: Maybe 11% gain after accounting for overhead

**The hybrid idea looked good on paper (22% faster) but falls apart when you think about the implementation details.**

**Better approach**:
- Fix PT to converge faster (dual-hash + aggressive promotion)
- Use TLB-BP for specific scenarios (small working sets, multi-tenant isolation)
- Don't try to combine them

Thanks for pushing back on this - it saved me from going down a rabbit hole!
