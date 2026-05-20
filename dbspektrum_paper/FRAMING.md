# Paper framing: a two-axis study of buffer-pool translation

## Headline claim

In PrediCache-style buffer pools, the translation hot path has two
independent design axes:

- **Placement function** — where each page's preferred frame lives
- **Hot-path bypass** — whether the overflow translator is consulted on
  every access, or only after the preferred-frame metadata check misses

PrediCache picks one choice along each axis. LIPAH demonstrates the
opposite choice on the bypass axis (via in-page hints). We adapt LIPAH's
bypass mechanism to the PrediCache-style setting, pair it with a new
order-preserving placement, and run the 2×2 ablation plus a controlled
displacement-rate sweep. The empirical question — *how much* of LIPAH's
translation-throughput gap does the (prefix, bypass) cell actually
close, and in which workload regimes? — is what the paper answers; we
defer specific quantitative claims to the evaluation section, since the
preliminary single-run numbers we have so far are not yet tight enough
to support them.

## OPM = OptimisticPageMap

The OPM is a chained hash table with versioned per-bucket locks
([src/bp/optimistic_page_map.rs](src/bp/optimistic_page_map.rs)): readers
take no locks (version-check OLC), writers take a per-bucket mutex. It is
PrediCache's overflow translator and the shared substrate of every
variant in our 2×2 — the two axes vary how PrediCache *uses* OPM, not OPM
itself.

## The two axes

| Axis | "no" choice (PrediCache picks this) | "yes" choice |
|---|---|---|
| **Placement** | `uniform`: `hash(c_key, page_id) mod F` (Stafford-mixed) — uniform spread across the frame array | `prefix`: `(hash(c_key) + page_id) mod F` (prefix hash + suffix offset) — sequential page_ids land in sequential frames, HW-prefetcher-friendly |
| **Bypass** | `always-probe`: every access reads the OPM bucket via `get_apply_with_bucket` | `bypass`: speculatively try-latch at `pref` first; only consult OPM if `meta(pref).key()` mismatches — **mechanism borrowed from LIPAH**, adapted from in-page hints to the metadata array |

The `bypass` mechanism is **LIPAH's**, repurposed: LIPAH uses an in-page
`frame_id` hint to bypass translation entirely; we drive the same idea
off the existing metadata array, so no page-byte coupling is needed. The
`prefix` placement is our own.

## The 2×2 (this *is* Figure 1)

| | **uniform** | **prefix** |
|---|---|---|
| **always-probe** | **PrediCache** *(existing)* | **A** *(new design point — placement only)* |
| **bypass** | **B** *(new design point — bypass only)* | **C** *(new design point — combined)* |

One cell is occupied by the existing PrediCache design; the other three
are new design points proposed and evaluated in this paper. Names for A,
B, C are TBD — they get final names in the writeup. The point of the
table is to make the *space* explicit: three previously-unstudied
combinations sit alongside PrediCache, each motivated by an independent
observation about the translation hot path.

All four cells share the same underlying machinery (the OPM hash table as
the overflow translator, speculative latch with payload-overlap); the
two axes are the *only* dimensions that vary. The bypass mechanism in
B and C is borrowed from LIPAH and adapted to read off the metadata
array instead of in-page hints; the prefix placement in A and C is new.

**Figure 1 of the paper is a polished rendering of this 2×2 table** (in
the previous draft Figure 1 was a page→frame execution diagram for
LAPT; that's replaced). Every later result in the paper sits on one of
these four cells.

LIPAH (with in-page hints) is reported as a separate *reference line* —
a different architectural design point (couples BP to page bytes),
included to show the achievable ceiling under a different abstraction,
but excluded from the 2×2 by principle.

## What the experiments answer

The 2×2 directly answers four design questions:

1. **Bypass effect at fixed placement (uniform):** PrediCache → **B**
2. **Placement effect at fixed bypass (always-probe):** PrediCache → **A**
3. **Do the two effects compose?** PrediCache → **C**
4. **Does bypass survive under uniform placement?** **B** vs **C**

We additionally run **one control experiment** to isolate the bypass
mechanism from the natural preferred-hit rate that the placement induces:

- **Displacement sweep:** add a `--scramble-prob F` knob to the bench that
  re-faults a random `F` fraction of pages so the *observed* preferred-hit
  rate sweeps from ∼100 % down to ∼0 %. Plot throughput vs the *measured*
  preferred-hit rate (read from `pt_counts`), one curve per variant. The
  bypass benefit should be monotone in the preferred-hit rate; the
  placement benefit should be roughly invariant.

### TODO — pending experiment: the always-probe-wins regime

We expect that **`always-probe` beats `bypass` once the miss rate AND the
payload are both large**, even though `bypass` saves work on hits. The
mechanism (to verify, not just assert):

- **`always-probe` overlaps the OPM lookup with the speculative meta + page
  cacheline loads.** When the speculative latch ultimately fails
  (revalidate mismatch), the OPM result is already in-flight or ready, so
  the slow path resumes with no extra serial latency. The lookup hides
  inside the failed-fast-path window.
- **`bypass` serializes the two.** It first attempts `meta(pref)` +
  try-latch + revalidate; only after that fails does it issue the OPM
  lookup. So a misprediction costs `(failed fast path) + (OPM lookup)`,
  whereas `always-probe` costs roughly `max(failed fast path, OPM lookup)`.

Under low miss rates, `bypass` still wins overall (most accesses succeed
without ever touching OPM). Under high miss rates with large payloads,
`always-probe`'s overlap dominates the per-access cost, and `bypass`'s
saved-on-hit work doesn't add up. This should show as a **crossover** in
the displacement-sweep plot at large payloads, *not* at small payloads.

Concrete experiments to run:

1. **Displacement sweep × payload sweep** — cross the `--scramble-prob`
   knob with `--payload-bytes ∈ {0, 256, 1024, 4096, 16384}`. Plot
   throughput vs measured preferred-hit rate, one panel per payload, one
   line per variant. Expectation: at small payload, `bypass` dominates
   for all displacement rates; at large payload, `always-probe` overtakes
   `bypass` once displacement is high enough.
2. **`perf stat` (or equivalent) on the high-miss + large-payload cell**
   to confirm the mechanism: `always-probe` should show comparable
   per-access cycles to `bypass`, but more concurrent memory operations
   in-flight (LFB occupancy / MEM_LOAD_RETIRED counters). If the
   throughput crossover lines up with the in-flight-load count crossover,
   that's direct evidence the win is from overlap, not from something
   else.

If this experiment confirms the crossover, the paper's takeaway sharpens:
**bypass and always-probe are Pareto points, not strict orderings;
PrediCache picked the right end for high-displacement large-payload
workloads, and the bypass adaptation we borrow from LIPAH wins at the
other end of the curve.**

## Workloads / figures

- **Crosstab (44 thread)** — sequential vs uniform-random access × saturated vs stale state. Pure translation cost (`--no-page-fold`).
- **Sequential payload sweep** — payloads 0 → 16 KiB. Shows where each cell of the 2×2 sits along the payload axis (bypass favors small payloads, prefix favors moderate, both compound on large).
- **Displacement-rate control** — the sweep described above. Single plot, throughput vs preferred-hit rate, 4 curves.
- **B-tree GET / range scan** — sanity that the micro effects show up in a real-ish workload.

LIPAH appears as a reference line in each plot.

## Preliminary indications from existing data

Based on `run_20260518_220100` (TRIALS=5, N=100k, F=200k, C=500, T=44,
sequential `read_page_with`) and a few ad-hoc single-trial sweeps in our
notes. These are *directional only* — not the numbers we'd put in a
results table. Treat them as hypotheses the planned experiments will
either confirm or refute.

- **Bypass alone** *appears to* help most at small payloads, where
  OPM-lookup latency is a larger fraction of total per-access cost.
  Pending: the missing cell **B** (uniform + bypass) needs to be built
  and benched.
- **Prefix placement alone** *appears to* help at moderate-to-large
  payloads — `PrediCache2` (cell **A**) beat PrediCache at payload 2 KiB
  and 4 KiB in our existing run, but the per-trial variance was high and
  the single-trial 4-way sweep we did doesn't have tight enough error
  bars to lean on.
- **Composition (cell C):** in the single available run, **C** was
  roughly tied with vanilla PrediCache and well short of LIPAH at large
  payloads. Whether **C** closes the LIPAH gap on the *sequential-scan,
  moderate-payload* regime is the central empirical question the paper
  has yet to resolve.
- **Uniform-random access** likely shrinks all of these effects, since
  neither axis exploits structure that isn't there.

## What we explicitly do not claim

- We do not claim a single design point is universally best — each of
  A, B, C wins in some regime, and PrediCache wins in others.
- We do not claim bypass is novel — it's LIPAH's mechanism, adapted to
  read off the metadata array instead of in-page hints.
- We do not claim LIPAH is dominated — it wins at large payloads via
  in-page hints; that's an orthogonal axis (different abstraction) and
  we report it as a reference line, not a baseline to beat.

## Relation to the existing paper draft

This framing is a **replacement** for the previously-proposed LAPT
optimization that we earlier (incorrectly) believed was universally
better than PrediCache. The 2×2 makes it visible that bypass and
prefix-hash are independent knobs and that no single cell strictly
dominates — which is the honest version of the story.

What stays from the existing draft:

- **The survey of existing buffer-pool translation solutions** (LIPAH,
  PrediCache, etc.) — keep as-is. It motivates *why* we are studying
  the PrediCache-style design point and what the design space looks
  like across prior systems.
- All prior-work attribution to PrediCache (placement + always-probe +
  OPM) and LIPAH (in-page hints + bypass) — both are now first-class
  citations in the 2×2.

What gets replaced:

- **The LAPT name disappears.** What was "LAPT" was a particular cell of
  this 2×2 — specifically cell **C** (prefix + bypass) — but it was
  never universally better, so calling it out as a named system was
  misleading. The three new cells are presented as design points along
  named axes; final naming for A/B/C happens in the writeup.
- **Figure 1** in the previous draft was a diagram of how LAPT executes
  the page → frame translation. **Replace it with a polished rendering
  of the 2×2 table** — placement on one axis, bypass on the other, the
  four cells labeled with the prior systems they correspond to where
  applicable (e.g. `(uniform, always-probe)` = PrediCache, `(_, bypass)`
  inherits the bypass mechanism from LIPAH). This figure is the
  paper's intellectual scaffold: every result later in the paper sits
  on a cell of this table.

## Narrative arc for the writeup

1. **Motivation (1 paragraph).** Two observations from prior work
   motivate this study. (a) **PrediCache** introduced the
   speculative-latch + payload-overlap technique to hide overflow-
   translator lookup latency, but did not quantify the cost of
   *always probing* the overflow table on every translation — vs e.g.
   **LIPAH**, which avoids the probe entirely via an in-page
   `frame_id` hint. (b) **Calico** identifies sequential-workload
   adaptivity as an open problem for translation-style buffer pools.
   These two observations map directly onto the two axes of our 2×2:
   *bypass* asks (a) whether always-probing is actually paying for
   itself, and *placement* asks (b) whether a scan-aware placement
   function meaningfully improves sequential-workload throughput.
2. **Two axes (1 paragraph + the 2×2 table).** Placement and bypass are
   independent choices; PrediCache picks one extreme on each.
3. **Method (½ page).** All four variants share OPM + speculative-latch;
   `preferred_frame` and the `read_page_with` entry-check are the only
   things that differ.
4. **Results (1 page, 3 figures).** Crosstab, payload sweep,
   displacement control. LIPAH as a reference line in each.
5. **Discussion (½ page).** Where each axis pays off; why the two
   compound on sequential workloads; honest scoping vs LIPAH's
   hint-based design.
