#!/usr/bin/env python3
"""Theoretical + empirical collision-rate analysis for PT-style translators.

Compares two preferred-frame schemes:
  - "stafford":  preferred(c, p) = mix64(c << 32 | p) mod F        (PrediCache, no ophash)
  - "ophash":    preferred(c, p) = (mix64(c) + p) mod F            (LAPT)

For each scheme we report:
  - total colliding pages (pages whose preferred slot already holds another page)
  - distribution of cluster sizes (for the ophash scheme: a cluster is a maximal run
    of consecutive slots with multiple pages mapped to them)
  - max cluster size (slow-path locality proxy)

Closed-form expectations are also printed where they apply.

Usage:
    python3 simulate_collisions.py
    python3 simulate_collisions.py --N 1000000 --F 2000000 --C 1000 --trials 20
"""
import argparse
import math
import random
from collections import Counter
from dataclasses import dataclass
from typing import List, Tuple


# Stafford-style mix64 (xor-multiply-shift), matches the spirit of hash_u64 in
# the Rust codebase. Doesn't need to match exactly — collision statistics
# depend on the function being a reasonable "random oracle," not on exact bits.
def mix64(x: int) -> int:
    x &= 0xFFFFFFFFFFFFFFFF
    x ^= x >> 30
    x = (x * 0xBF58476D1CE4E5B9) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 27
    x = (x * 0x94D049BB133111EB) & 0xFFFFFFFFFFFFFFFF
    x ^= x >> 31
    return x


def preferred_stafford(c: int, p: int, F: int) -> int:
    """PrediCache (no ophash): full-key Stafford mix."""
    return mix64((c << 32) | p) % F


def preferred_ophash(c: int, p: int, F: int) -> int:
    """LAPT: hash(c) + p (mod F)."""
    return (mix64(c) + p) % F


def assign_pages(C: int, P: int, F: int, scheme: str) -> List[int]:
    """Assign C*P pages to F frames using the given scheme. Returns the list
    of preferred slots (one entry per page)."""
    fn = preferred_ophash if scheme == "ophash" else preferred_stafford
    slots = []
    for c in range(C):
        for p in range(P):
            slots.append(fn(c, p, F))
    return slots


def collision_stats(slots: List[int], F: int):
    """Return dict of stats for a given assignment.

    - colliding_pages: pages whose preferred slot has 2+ pages mapped (counts
      every page beyond the first per slot, summed across slots)
    - unique_slots: distinct slots used
    - clusters: list of (start, length, max_load) for maximal runs of slots
      with load >= 2
    """
    counts = Counter(slots)
    colliding_pages = sum(max(0, k - 1) for k in counts.values())
    unique_slots = len(counts)

    # Find maximal runs of consecutive slots with >= 2 pages
    clusters = []
    i = 0
    sorted_slots = sorted(counts.keys())
    # Walk full slot range to find runs (much simpler than walking sorted keys)
    s = 0
    while s < F:
        if counts.get(s, 0) >= 2:
            start = s
            max_load = 0
            while s < F and counts.get(s, 0) >= 2:
                max_load = max(max_load, counts[s])
                s += 1
            clusters.append((start, s - start, max_load))
        else:
            s += 1

    cluster_sizes = [c[1] for c in clusters]
    return {
        "colliding_pages": colliding_pages,
        "unique_slots": unique_slots,
        "n_clusters": len(clusters),
        "max_cluster_size": max(cluster_sizes) if cluster_sizes else 0,
        "mean_cluster_size": sum(cluster_sizes) / len(cluster_sizes) if cluster_sizes else 0,
        "cluster_sizes": cluster_sizes,
    }


def expected_collisions_stafford(N: int, F: int) -> float:
    """E[colliding pages] for N independent uniform balls in F bins.

    Each bin's load X ~ Binomial(N, 1/F). P(X=k) ≈ Poisson(λ=N/F).
    Total colliding pages = Σ_bin max(0, X_bin - 1) = N - unique_bins.
    E[unique_bins] = F * (1 - (1 - 1/F)^N) ≈ F * (1 - e^(-N/F)).
    """
    return N - F * (1 - (1 - 1 / F) ** N)


def expected_collisions_ophash(C: int, P: int, F: int) -> float:
    """E[colliding pages] for C blocks of P contiguous slots each at random offset.

    For any slot s, number of blocks covering s ~ Binomial(C, P/F)
    (each block independently has P/F probability of including s).
    Total colliding pages = Σ_slot max(0, X_s - 1) = (C*P) - E[unique_slots].
    E[unique_slots] = F * (1 - (1 - P/F)^C) ≈ F * (1 - e^(-CP/F)).
    """
    N = C * P
    return N - F * (1 - (1 - P / F) ** C)


def run_one(C: int, P: int, F: int, trials: int = 5):
    N = C * P
    rho = N / F
    print(f"\n=== C={C} P={P} F={F}  (N={N}, ρ={rho:.3f}) ===")

    print(f"\nClosed-form expected colliding pages:")
    e_staff = expected_collisions_stafford(N, F)
    e_ophash = expected_collisions_ophash(C, P, F)
    print(f"  stafford : {e_staff:>10.0f}  ({e_staff / N * 100:.1f}% of pages)")
    print(f"  ophash   : {e_ophash:>10.0f}  ({e_ophash / N * 100:.1f}% of pages)")
    print(f"  ratio    : {e_ophash / e_staff:.4f}")

    print(f"\nEmpirical (over {trials} trials):")
    for scheme in ("stafford", "ophash"):
        cps = []
        max_clusters = []
        n_clusters_list = []
        mean_clusters = []
        all_cluster_sizes = []
        for _ in range(trials):
            slots = assign_pages(C, P, F, scheme)
            stats = collision_stats(slots, F)
            cps.append(stats["colliding_pages"])
            max_clusters.append(stats["max_cluster_size"])
            n_clusters_list.append(stats["n_clusters"])
            mean_clusters.append(stats["mean_cluster_size"])
            all_cluster_sizes.extend(stats["cluster_sizes"])
        mean_cps = sum(cps) / trials
        mean_max = sum(max_clusters) / trials
        mean_n = sum(n_clusters_list) / trials
        mean_mean = sum(mean_clusters) / trials
        cluster_pct = sum(all_cluster_sizes) / (F * trials) * 100
        print(
            f"  {scheme:8s} : colliding pages = {mean_cps:>8.0f} "
            f"({mean_cps / N * 100:.1f}%)  "
            f"clusters: count={mean_n:.0f} avg_size={mean_mean:.2f} "
            f"max_size={mean_max:.0f}  "
            f"(slot fraction in clusters: {cluster_pct:.2f}%)"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--N", type=int, default=100_000, help="total pages")
    p.add_argument("--F", type=int, default=200_000, help="total frames")
    p.add_argument("--C", type=int, default=500, help="containers")
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--sweep", action="store_true",
                   help="run a small sweep over (C, ρ) instead of one config")
    args = p.parse_args()

    if args.sweep:
        # Sweep: vary C with N and F fixed.
        print("Sweep: vary C with N=100k, F=200k (ρ=0.5)")
        for C in [1, 10, 50, 100, 500, 2000, 10000]:
            P = max(1, args.N // C)
            run_one(C, P, args.F, trials=args.trials)
        # Sweep: vary load factor (N) with C fixed.
        print("\n\nSweep: vary load factor with C=500, F=200k")
        for N in [50_000, 100_000, 150_000, 180_000, 200_000]:
            P = max(1, N // 500)
            run_one(500, P, args.F, trials=args.trials)
    else:
        P = max(1, args.N // args.C)
        run_one(args.C, P, args.F, trials=args.trials)


if __name__ == "__main__":
    main()
