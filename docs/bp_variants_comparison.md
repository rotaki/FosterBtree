# Buffer Pool Variants: LIPAH, PT-FP-1, TLB-BP

A visual comparison of the three buffer-pool translation strategies.

The core question for any BP: given a `PageKey = (container, page_id)`, **how
do you find the frame that holds it?** Each variant answers differently.

```
                ┌──────────────┐
                │   PageKey    │   "I want page (c=9, p=12345)"
                └──────┬───────┘
                       │
                       ▼
                  translation
                       │
                       ▼
                ┌──────────────┐
                │  frame_id    │   "It's in frame 4321"
                └──────────────┘
```

The differentiator: **where the `frame_id` hint comes from** and what happens
when the hint is stale.

---

## 1. LIPAH — keep (page_id, frame_id) together

The upper layer (B-tree or any access method) stores `PageFrameKey =
(page_id, frame_id)` pairs — both fields kept side by side. The page_id is
always authoritative; the frame_id is a cached hint into the BP. The hint
comes "for free" because the calling code already has it loaded.

```
┌───────────────────────────────────────────────────────────────┐
│  B-tree internal node                                         │
│ ┌─────────────────────────────────────────────────────────┐   │
│ │ key0 │ key1 │ key2 │ key3 │ ...                         │   │
│ ├─────────────────────────────────────────────────────────┤   │
│ │ Each slot: PageFrameKey = (page_id, frame_id_hint)      │   │
│ │  ┌──────────┬──────────┐  ┌──────────┬──────────┐  ...  │   │
│ │  │ page_id  │ frame_id │  │ page_id  │ frame_id │       │   │
│ │  │   123    │   4321   │  │   124    │    982   │       │   │
│ │  └──────────┴──────────┘  └──────────┴──────────┘       │   │
│ └────┬─────────────────┬──────────────────────────────────┘   │
└──────┼─────────────────┼──────────────────────────────────────┘
       │ page_id         │ frame_id_hint  (kept alongside)
       │ (authoritative) │
       │                 ▼
       │      ┌────────────────────────────────────────────┐
       │      │  Global Frame Array  (validates the hint)  │
       │      │  ┌────┬────┬────┬────┬────┬────┬────┬───┐  │
       │      │  │meta│meta│meta│meta│meta│meta│meta│...│  │
       │      │  └────┴────┴────┴────┴────┴────┴────┴───┘  │
       │      │                  ▲                         │
       │      │              frame[hint]                   │
       │      └────────────────────────────────────────────┘
       │                          │
       │                          ▼
       └────────────────▶ meta.key == page_id?
                                  │
                        ┌─────────┴─────────┐
                       YES                  NO  (hint stale)
                        │                    │
                   ┌─────────┐         ┌──────────────────────┐
                   │ FAST    │         │  SLOW PATH           │
                   │ PATH    │         │  DashMap.get(page_id)│
                   │         │         │     │                │
                   │ no      │         │     ▼                │
                   │ extra   │         │  fresh frame_id      │
                   │ work    │         │     │                │
                   │         │         │     ▼                │
                   └─────────┘         │  caller updates the  │
                                       │  frame_id field of   │
                                       │  its PageFrameKey    │
                                       └──────────────────────┘
```

**Key idea**: translation work is *zero* on the fast path — the hint is
already next to the page_id in the caller's data. After eviction, one
DashMap lookup gives a fresh frame_id; the caller writes it back into its
own `PageFrameKey` so subsequent accesses are fast again.

---

## 2. PT-FP-1 — hint computed by hash function

No swizzling. The hint is *computed* deterministically from the page key via
a hash. The page is *expected* to be in its preferred frame (placed there at
creation time and via promotion).

```
PageKey
   │
   ├─hash─▶ preferred_frame = hash(key) % num_frames
   │            │
   │            ▼
   │   ┌──────────────────────────────────────────────────────────┐
   │   │  Global Frame Array                                      │
   │   │  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐    │
   │   │  │meta│meta│meta│meta│meta│meta│meta│meta│meta│... │    │
   │   │  └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘    │
   │   │             ▲                                            │
   │   │       frame[preferred]                                   │
   │   └──────────────────────────────────────────────────────────┘
   │                │
   │                ▼
   └──────▶ meta.key == PageKey?
                    │
            ┌───────┴───────┐
           YES             NO  (page was displaced into overflow)
            │               │
       ┌─────────┐    ┌─────────────────────────────────────┐
       │ FAST    │    │  SLOW PATH                          │
       │ PATH    │    │  ┌───────────────────────────────┐  │
       │         │    │  │ Overflow Hash Table (chained) │  │
       │ 1 hash  │    │  └─────────────┬─────────────────┘  │
       │ + 1 meta│    │                ▼                    │
       │ access  │    │           frame_id                  │
       └─────────┘    │                ▼                    │
                      │      meta.key validation            │
                      │                ▼                    │
                      │      ┌───────────────────────────┐  │
                      │      │ promote? (1/50 or 1/512)  │  │
                      │      │ → COPY 16 KB page back    │  │
                      │      │   into preferred frame    │  │
                      │      └───────────────────────────┘  │
                      └─────────────────────────────────────┘
```

**Key idea**: the access method is unchanged (it stores only page_id), so any
data structure works. But misses are expensive (overflow lookup + occasional
page copy on promotion).

---

## 3. TLB-BP — hint cached per-thread

Per-thread translation cache (16 KB, fits in L1). The hint comes from a
recently-used entry; misses go through an ordered ART tree.

```
   PageKey
      │
      ├─hash─▶ ┌──────────────────────────────┐
      │        │  Thread 1     │   Thread 2   │   ... per core
      │        │ ┌──────────┐  │ ┌──────────┐ │
      │        │ │  TLB     │  │ │  TLB     │ │
      │        │ │ 16 KB    │  │ │ 16 KB    │ │   ← FITS IN L1
      │        │ │ 1024×4   │  │ │ 1024×4   │ │
      │        │ │ way SA   │  │ │ way SA   │ │
      │        │ └────┬─────┘  │ └──────────┘ │
      │        └──────┼────────────────────────┘
      │               │
      │               ▼
      │           frame_id
      │               │
      │               ▼
      │   ┌──────────────────────────────────────────────────────┐
      │   │  Global Frame Array  (validates only)                │
      │   │  ┌────┬────┬────┬────┬────┬────┬────┬────┬────┐     │
      │   │  │meta│meta│meta│meta│meta│meta│meta│meta│meta│     │
      │   │  └────┴────┴────┴────┴────┴────┴────┴────┴────┘     │
      │   └──────────────────────┬───────────────────────────────┘
      │                          │
      │                          ▼
      └──────────────────▶ meta.key == PageKey?
                                  │
                          ┌───────┴───────┐
                         YES             NO  (TLB miss or stale entry)
                          │               │
                     ┌─────────┐    ┌─────────────────────────────────┐
                     │ FAST    │    │  SLOW PATH                      │
                     │ PATH    │    │  ┌──────────────────────────┐   │
                     │         │    │  │  Congee ART (ordered)    │   │
                     │ TLB     │    │  │  ┌──┐  ┌──┐  ┌──┐  ┌──┐  │   │
                     │ probe   │    │  │  │N4│─▶│N4│─▶│N4│─▶│N4│  │   │
                     │ + 1 meta│    │  │  └──┘  └──┘  └──┘  └──┘  │   │
                     │ access  │    │  └─────────┬────────────────┘   │
                     └─────────┘    │            ▼                    │
                                    │     atomic latch + sibling      │
                                    │     prefill (8 entries) into    │
                                    │     thread-local TLB            │
                                    └─────────────────────────────────┘
```

**Key idea**: per-thread TLB eliminates cross-core contention on the
translation cache itself; sequential-scan workloads benefit from sibling
prefill via the ordered ART overflow.

---

## 4. Where the hint lives — the central distinction

```
              hint source         hint cost                hint update
              ──────────────      ──────────────           ─────────────
LIPAH         PageFrameKey        0 (already in            on slow lookup,
              field next to       caller's data)           caller writes
              page_id                                      fresh frame_id
              (kept together)                              into its own
                                                          PageFrameKey

PT-FP-1       hash function       ~5 cycles compute        never (fixed
              (computed)                                   by hash function;
                                                          page can be
                                                          promoted/demoted
                                                          via copy)

TLB-BP        per-thread TLB      ~4 cycles L1 probe       overflow lookup
              (cached)                                     populates TLB;
                                                          sibling prefill
                                                          fills neighbors
```
