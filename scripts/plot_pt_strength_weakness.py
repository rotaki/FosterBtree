#!/usr/bin/env python3
"""Plot the PT strength/weakness study figures.

Consumes the artefacts emitted by bench_pt_strength_weakness.sh (default:
bench_pt_strength_weakness_<ts>/). Produces three panels:

    figure_collision.png      coverage vs collision width k (stationary)
    figure_phase_shift.png    windowed coverage vs time for each variant
    figure_scan_throughput.png throughput vs variant for sequential/random/hot

These two coverage panels are the headline figures from the plan; throughput
is a secondary panel.

Usage:
    python scripts/plot_pt_strength_weakness.py <results_dir>

Each log is parsed with simple regexes:
    - `fast_path_coverage: X.YYYY` for the collision sweep.
    - TSV `t_ms<tab>phase<tab>dh<tab>dt<tab>coverage_window` for phase shift.
    - `Throughput: ... Mops/s` + `Avg latency: X ns` for scan.
"""
import os
import re
import sys
from collections import defaultdict

import matplotlib.pyplot as plt

if len(sys.argv) != 2:
    sys.exit(f"Usage: {sys.argv[0]} <bench_pt_strength_weakness_<ts>/>")

ROOT = sys.argv[1]

VARIANTS = [
    ("lipah_v2", "LIPAH-V2"),
    ("tlb_v2", "TLB-V2"),
    ("pt_v2", "PT-V2"),
    ("pt_v2_ophash", "PT-V2-ophash"),
    ("pt_fp_v2", "PT-FP-V2"),
    ("pt_fp_v2_ophash", "PT-FP-V2-ophash"),
]

# Colours chosen so PT variants share a family and LIPAH/TLB contrast against
# them. Keep PT(FP) solid, PT dashed.
STYLES = {
    "lipah_v2": dict(color="#1f77b4", marker="o", linestyle="-"),
    "tlb_v2": dict(color="#2ca02c", marker="s", linestyle="-"),
    "pt_v2": dict(color="#d62728", marker="v", linestyle="--"),
    "pt_v2_ophash": dict(color="#ff7f0e", marker="v", linestyle="--"),
    "pt_fp_v2": dict(color="#d62728", marker="^", linestyle="-"),
    "pt_fp_v2_ophash": dict(color="#ff7f0e", marker="^", linestyle="-"),
}


def grep_coverage(path):
    cov = None
    with open(path) as f:
        for line in f:
            m = re.search(r"fast_path_coverage:\s*([0-9.]+)", line)
            if m:
                cov = float(m.group(1))
    return cov


def grep_throughput(path):
    tp = None
    lat = None
    with open(path) as f:
        for line in f:
            m = re.search(r"Throughput:\s*[\d.]+\s*ops/s\s*\(([0-9.]+)\s*Mops", line)
            if m:
                tp = float(m.group(1))
            m = re.search(r"Avg latency:\s*([0-9.]+)\s*ns", line)
            if m:
                lat = float(m.group(1))
    return tp, lat


# ---------------- Panel A: coverage vs collision width ---------------------
def plot_collision_panel():
    ks = [1, 2, 4, 8]
    series = defaultdict(list)
    for k in ks:
        for tag, _label in VARIANTS:
            p = os.path.join(ROOT, f"partB2_k{k}_{tag}.log")
            if not os.path.exists(p):
                series[tag].append(float("nan"))
            else:
                series[tag].append(grep_coverage(p) or float("nan"))

    fig, ax = plt.subplots(figsize=(6, 4))
    for tag, label in VARIANTS:
        ax.plot(ks, series[tag], label=label, **STYLES.get(tag, {}))
    ax.set_xlabel("Collision width k (pages per preferred slot)")
    ax.set_ylabel("fast_path_coverage")
    ax.set_title("Coverage vs collision width\n(PT(FP) should track 1/k; TLB/LIPAH flat)")
    ax.set_xscale("log", base=2)
    ax.set_xticks(ks)
    ax.set_xticklabels([str(k) for k in ks])
    ax.set_ylim(0, 1.05)
    ax.plot(ks, [1.0 / k for k in ks], color="grey", linestyle=":", label="1/k (analytic)")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8, loc="center right")
    out = os.path.join(ROOT, "figure_collision.png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print("wrote", out)


# ---------------- Panel B: coverage vs time since phase shift --------------
def plot_phase_shift_panel():
    fig, ax = plt.subplots(figsize=(7, 4))
    plotted = False
    for tag, label in VARIANTS:
        p = os.path.join(ROOT, f"partB4_phase_{tag}.tsv")
        if not os.path.exists(p):
            continue
        ts, covs = [], []
        with open(p) as f:
            for line in f:
                if line.startswith("#") or not line.strip():
                    continue
                parts = line.strip().split("\t")
                if len(parts) < 5:
                    continue
                try:
                    ts.append(int(parts[0]) / 1000.0)
                    covs.append(float(parts[4]))
                except ValueError:
                    continue
        if ts:
            ax.plot(ts, covs, label=label, **STYLES.get(tag, {}))
            plotted = True
    if not plotted:
        print("no phase-shift data found")
        return
    ax.set_xlabel("time (s)")
    ax.set_ylabel("windowed coverage")
    ax.set_ylim(0, 1.05)
    ax.set_title("Coverage vs time across phase shifts\n(baselines flat; PT(FP) dips + recovers)")
    ax.grid(True, alpha=0.3)
    ax.legend(fontsize=8, loc="lower right")
    out = os.path.join(ROOT, "figure_phase_shift.png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print("wrote", out)


# ---------------- Panel C (secondary): scan throughput ---------------------
def plot_scan_panel():
    patterns = ["sequential", "random", "hotspots"]
    fig, axs = plt.subplots(1, 3, figsize=(14, 4), sharey=True)
    for ax, pat in zip(axs, patterns):
        tp_per = {}
        for tag, label in VARIANTS:
            p = os.path.join(ROOT, f"partA_{pat}_{tag}.log")
            tp, _lat = grep_throughput(p) if os.path.exists(p) else (None, None)
            tp_per[tag] = tp if tp is not None else float("nan")
        tags = [t for t, _ in VARIANTS]
        labels = [l for _, l in VARIANTS]
        vals = [tp_per[t] for t in tags]
        colors = [STYLES[t]["color"] for t in tags]
        ax.bar(labels, vals, color=colors)
        ax.set_title(pat)
        ax.tick_params(axis="x", labelrotation=40, labelsize=8)
        ax.grid(True, axis="y", alpha=0.3)
    axs[0].set_ylabel("throughput (Mops/s)")
    fig.suptitle("Part A: scan / random / hotspot throughput")
    out = os.path.join(ROOT, "figure_scan_throughput.png")
    fig.tight_layout()
    fig.savefig(out, dpi=150)
    print("wrote", out)


def main():
    plot_collision_panel()
    plot_phase_shift_panel()
    plot_scan_panel()


if __name__ == "__main__":
    main()
