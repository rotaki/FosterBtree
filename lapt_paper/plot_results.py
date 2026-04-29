#!/usr/bin/env python3
"""Plot generator for run_all_benchmarks.sh outputs.

Reads CSVs from --indir and writes PNGs to --outdir. Requires matplotlib and
pandas:
    pip install matplotlib pandas
"""
import argparse
import os
import sys
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
except ImportError as e:
    print(f"missing dependency: {e}. Install with: pip install matplotlib pandas",
          file=sys.stderr)
    sys.exit(1)


# Apply the project's paper-style sheet if present (small fonts, serif,
# tight legend, minor ticks, custom color cycle). Falls back to default if
# the file is missing.
_STYLE = Path(__file__).resolve().parent / "custom_plt_style.mplstyle"
if _STYLE.exists():
    plt.style.use(str(_STYLE))
    # Add fallbacks so the absence of Times New Roman doesn't kick us back to
    # a sans-serif default. DejaVu Serif is preinstalled with matplotlib.
    plt.rcParams["font.serif"] = ["Times New Roman", "Liberation Serif",
                                   "DejaVu Serif", "serif"]

# Variant order and colors. Pulled from the first three colors of the style's
# axes.prop_cycle so all plots stay color-consistent with the rest of the
# paper. Hatches let bars distinguish in B/W print.
#
# Display names use PrediCache* to flag that our PrediCache implementation
# is a faithful re-build, not the original codebase. CSV column values use
# the un-asterisked name; CSV_NAMES maps display → CSV.
VARIANTS = ["LIPAH", "PrediCache*", "LAPT"]
CSV_NAMES = {"LIPAH": "LIPAH", "PrediCache*": "PrediCache", "LAPT": "LAPT"}
COLORS = {"LIPAH": "#0C5DA5", "PrediCache*": "#00B945", "LAPT": "#FF9500"}
MARKERS = {"LIPAH": "o", "PrediCache*": "s", "LAPT": "^"}
HATCHES = {"LIPAH": "", "PrediCache*": "//", "LAPT": "xx"}


def median(series):
    return series.median()


def plot_seq_payload(indir, outdir):
    """Workload 1: payload sweep at T_LOW. Two subplots side by side
    (saturated, stale), x-axis = payload bytes (log scale), y-axis = Mops/s,
    one line per variant."""
    sat = pd.read_csv(indir / "seq_payload_sat.csv")
    stale = pd.read_csv(indir / "seq_payload_stale.csv")

    # Two-panel; widen vs the style default 1.6 width
    fig, axes = plt.subplots(1, 2, figsize=(3.4, 1.5), sharey=True)
    for ax, df, title in [(axes[0], sat, "Pages at preferred frame"),
                          (axes[1], stale, "Pages displaced from preferred")]:
        for v in VARIANTS:
            sub = df[df.variant == CSV_NAMES[v]].groupby("payload_bytes").mops.agg(["median", "min", "max"]).reset_index()
            x = sub.payload_bytes.replace(0, 0.5)  # 0 doesn't fit log scale; use 0.5
            ax.plot(x, sub["median"], label=v, color=COLORS[v],
                    marker=MARKERS[v], markersize=2.5, linewidth=0.8)
            ax.fill_between(x, sub["min"], sub["max"], color=COLORS[v], alpha=0.15,
                            linewidth=0)
        ax.set_xscale("log")
        ax.set_xlabel("Payload bytes / access")
        ax.set_title(title)
        ax.set_xticks([0.5, 1, 256, 1024, 4096, 16384])
        ax.set_xticklabels(["0", "1", "256", "1K", "4K", "16K"])
        ax.tick_params(axis="x", which="minor", bottom=False, top=False)
    axes[0].set_ylabel("Throughput (Mops/s)")
    axes[0].legend(loc="upper right")
    fig.subplots_adjust(left=0.11, right=0.99, top=0.91, bottom=0.20, wspace=0.06)
    fig.savefig(outdir / "seq_payload.png")
    fig.savefig(outdir / "seq_payload.pdf")
    plt.close(fig)
    print(f"  wrote {outdir / 'seq_payload.png'} and .pdf")


def plot_crosstab_44t(indir, outdir):
    """Workload 2: 4-cell crosstab, grouped bar chart."""
    df = pd.read_csv(indir / "crosstab_44t.csv")
    # Median across trials.
    g = df.groupby(["variant", "access", "state"]).mops.agg(["median", "min", "max"]).reset_index()

    # 4 scenarios in 2 access groups: (Sequential, Uniform random) × (Preferred, Not preferred)
    scenarios = [("sequential", "sat", "Preferred"),
                 ("sequential", "stale", "Not preferred"),
                 ("uniform", "sat", "Preferred"),
                 ("uniform", "stale", "Not preferred")]
    fig, ax = plt.subplots(figsize=(3.4, 1.9))
    bar_w = 0.27
    xs = list(range(len(scenarios)))
    for i, v in enumerate(VARIANTS):
        ys, errs_lo, errs_hi = [], [], []
        for access, state, _ in scenarios:
            row = g[(g.variant == CSV_NAMES[v]) & (g.access == access) & (g.state == state)]
            if len(row) == 0:
                ys.append(0); errs_lo.append(0); errs_hi.append(0)
            else:
                m = row["median"].iloc[0]
                lo = row["min"].iloc[0]
                hi = row["max"].iloc[0]
                ys.append(m)
                errs_lo.append(m - lo)
                errs_hi.append(hi - m)
        offsets = [x + (i - 1) * bar_w for x in xs]
        ax.bar(offsets, ys, bar_w, label=v, color=COLORS[v],
               yerr=[errs_lo, errs_hi], capsize=1.5,
               edgecolor="black", linewidth=0.4,
               hatch=HATCHES[v])
        for off, y in zip(offsets, ys):
            ax.text(off, y + 1.5, f"{y:.0f}", ha="center", va="bottom", fontsize=4.5)
    # Headroom so value labels and legend don't overlap bars
    ymax = max([max([row["max"].iloc[0]
                     for access, state, _ in scenarios
                     for row in [g[(g.variant == CSV_NAMES[v]) & (g.access == access) & (g.state == state)]]
                     if len(row) > 0])
                for v in VARIANTS])
    ax.set_ylim(0, ymax * 1.22)

    # Leaf labels (Fresh / Stale)
    ax.set_xticks(xs)
    ax.set_xticklabels([s[2] for s in scenarios])
    ax.set_ylabel("Throughput (Mops/s)")
    ax.tick_params(axis="x", which="minor", bottom=False, top=False)

    # Group labels (Sequential / Uniform random) on a second tick axis below.
    # Place them at the midpoint of each group (xs 0-1 and 2-3) and add a
    # connecting bracket above them.
    group_centers = [0.5, 2.5]
    group_labels = ["Sequential", "Uniform random"]
    # Manual brackets via axhspan or just text below xticks.
    for cx, lbl in zip(group_centers, group_labels):
        ax.text(cx, -ymax * 0.18, lbl, ha="center", va="top",
                fontsize=6.5, fontweight="bold")
    # Vertical separator between the two access groups
    ax.axvline(x=1.5, color="0.5", linestyle=":", linewidth=0.4, alpha=0.7)

    # Headroom no longer needed for legend (it lives above the axis)
    ax.set_ylim(0, ymax * 1.1)
    ax.legend(loc="lower center", ncol=3, bbox_to_anchor=(0.5, 1.02),
              fontsize=7, handlelength=1.5, handletextpad=0.4,
              borderpad=0.3, columnspacing=0.8)
    fig.subplots_adjust(left=0.11, right=0.99, top=0.86, bottom=0.22)
    fig.savefig(outdir / "crosstab_44t.png")
    fig.savefig(outdir / "crosstab_44t.pdf")
    plt.close(fig)
    print(f"  wrote {outdir / 'crosstab_44t.png'} and .pdf")


def plot_btree_get(indir, outdir):
    """Workload 3: B-tree GET, single grouped bar with mean ± range."""
    df = pd.read_csv(indir / "btree_get.csv")
    g = df.groupby("variant").mops.agg(["mean", "min", "max"]).reindex([CSV_NAMES[v] for v in VARIANTS]).reset_index()

    fig, ax = plt.subplots(figsize=(6, 4))
    xs = list(range(len(VARIANTS)))
    means = g["mean"].values
    err_lo = (g["mean"] - g["min"]).values
    err_hi = (g["max"] - g["mean"]).values
    bars = ax.bar(xs, means, color=[COLORS[v] for v in VARIANTS],
                  yerr=[err_lo, err_hi], capsize=4, edgecolor="black", linewidth=0.5)
    for b, m in zip(bars, means):
        ax.text(b.get_x() + b.get_width() / 2, m + 0.05, f"{m:.2f}",
                ha="center", va="bottom", fontsize=10)
    ax.set_xticks(xs)
    ax.set_xticklabels(VARIANTS)
    ax.set_ylabel("Throughput (Mops/s)")
    ax.set_title("B-tree random GET (T=44, 2M keys, in-memory, no-copy)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outdir / "btree_get.png", dpi=150)
    plt.close(fig)
    print(f"  wrote {outdir / 'btree_get.png'}")


def plot_btree_range_scan(indir, outdir):
    """Workload 4: B-tree range scan, grouped bar with mean ± range."""
    df = pd.read_csv(indir / "btree_range_scan.csv")
    g = df.groupby("variant").mkvs_per_s.agg(["mean", "min", "max"]).reindex([CSV_NAMES[v] for v in VARIANTS]).reset_index()

    fig, ax = plt.subplots(figsize=(6, 4))
    xs = list(range(len(VARIANTS)))
    means = g["mean"].values
    err_lo = (g["mean"] - g["min"]).values
    err_hi = (g["max"] - g["mean"]).values
    bars = ax.bar(xs, means, color=[COLORS[v] for v in VARIANTS],
                  yerr=[err_lo, err_hi], capsize=4, edgecolor="black", linewidth=0.5)
    for b, m in zip(bars, means):
        ax.text(b.get_x() + b.get_width() / 2, m + 5, f"{m:.0f}",
                ha="center", va="bottom", fontsize=10)
    ax.set_xticks(xs)
    ax.set_xticklabels(VARIANTS)
    ax.set_ylabel("Throughput (M kvs/s)")
    ax.set_title("B-tree random-start range scan (T=44, scan_size=1000)")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    fig.savefig(outdir / "btree_range_scan.png", dpi=150)
    plt.close(fig)
    print(f"  wrote {outdir / 'btree_range_scan.png'}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--indir", required=True, help="directory with CSVs")
    p.add_argument("--outdir", required=True, help="directory to write PNGs")
    args = p.parse_args()
    indir = Path(args.indir)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    plots = [
        ("seq_payload_sat.csv",      plot_seq_payload),
        ("crosstab_44t.csv",         plot_crosstab_44t),
        ("btree_get.csv",            plot_btree_get),
        ("btree_range_scan.csv",     plot_btree_range_scan),
    ]
    # plot_seq_payload reads both sat+stale csvs internally; we just check sat exists.
    for csvname, fn in plots:
        if (indir / csvname).exists():
            fn(indir, outdir)
        else:
            print(f"  skip {csvname} (not found)")


if __name__ == "__main__":
    main()
