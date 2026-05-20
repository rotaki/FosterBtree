#!/usr/bin/env python3
"""Generate cluster-size distribution plot from a single (C, P, F) config.

Shows that PrediCache's collisions are scattered (mostly singletons),
LAPT's are clustered (long runs)."""
import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from simulate_collisions import assign_pages, collision_stats


# Apply project paper-style if available.
_STYLE = Path(__file__).resolve().parent / "custom_plt_style.mplstyle"
if _STYLE.exists():
    plt.style.use(str(_STYLE))
    plt.rcParams["font.serif"] = ["Times New Roman", "Liberation Serif",
                                   "DejaVu Serif", "serif"]

# Same color/hatch mapping as plot_results.py for cross-figure consistency.
COLOR_PREDICACHE = "#00B945"   # green (PrediCache)
COLOR_LAPT = "#FF9500"         # orange (LAPT)
HATCH_PREDICACHE = "//"
HATCH_LAPT = "xx"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--C", type=int, default=500)
    p.add_argument("--P", type=int, default=200)
    p.add_argument("--F", type=int, default=200_000)
    p.add_argument("--out", default=str(Path(__file__).resolve().parent / "results" / "canonical" / "plots" / "collision_clusters.png"))
    args = p.parse_args()

    # Run both schemes first so we can pick a shared x-axis range based on
    # the larger of the two max cluster sizes (LAPT). Sharing x makes the
    # cluster-size distributions visually comparable.
    schemes = []
    for scheme, label, color, hatch in [
        ("stafford", "PrediCache*", COLOR_PREDICACHE, HATCH_PREDICACHE),
        ("ophash",   "LAPT",        COLOR_LAPT,       HATCH_LAPT),
    ]:
        slots = assign_pages(args.C, args.P, args.F, scheme)
        stats = collision_stats(slots, args.F)
        schemes.append((scheme, label, color, hatch, stats["cluster_sizes"]))
    overall_max = max((max(s[4]) if s[4] else 1) for s in schemes)
    # Bin edges shared across both panels, log-spaced over the union range.
    shared_bins = np.logspace(0, np.log10(overall_max + 1), 24)

    fig, axes = plt.subplots(1, 2, figsize=(3.4, 1.4), sharey=True, sharex=True)
    for ax, (scheme, label, color, hatch, sizes) in zip(axes, schemes):
        if sizes:
            ax.hist(sizes, bins=shared_bins, edgecolor="black", linewidth=0.3,
                    color=color, alpha=0.85, hatch=hatch)
        ax.set_xscale("log")
        ax.set_xlabel("Cluster size")
        nclust = len(sizes)
        mx = max(sizes) if sizes else 0
        ax.set_title(f"{label} ({nclust} clusters, max={mx})", fontsize=6)
        # Explicit major ticks so log-scale labels don't crowd at small max
        ax.set_xticks([1, 10, 100, 1000])
        ax.set_xticklabels(["1", "10", "100", "1000"])
        ax.tick_params(axis="x", which="minor", bottom=True, top=True)
    axes[0].set_ylabel("# Clusters")
    axes[0].set_yscale("log")
    # Manual layout: tighter than tight_layout, no risk of legend/title overlap.
    fig.subplots_adjust(left=0.11, right=0.99, top=0.85, bottom=0.22, wspace=0.08)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(args.out)
    fig.savefig(str(args.out).replace(".png", ".pdf"))
    print(f"wrote {args.out} and .pdf")


if __name__ == "__main__":
    main()
