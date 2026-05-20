#!/usr/bin/env python3
"""Plot the bypass-vs-always-probe Pareto sweep.

Reads CSV with columns: variant,payload,prefer_prob,observed_hit_rate,mops
Emits a 2x2 panel grid (one per payload) of throughput vs prefer_prob, with
two lines per panel (always-probe vs bypass). Output: prefer_sweep.{png,pdf}
in --outdir.
"""
import argparse
import sys
from pathlib import Path

try:
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as e:
    print(f"missing dependency: {e}. Install: pip install matplotlib pandas",
          file=sys.stderr)
    sys.exit(1)

# Match the project style if available.
_STYLE = Path(__file__).resolve().parent / "custom_plt_style.mplstyle"
if _STYLE.exists():
    plt.style.use(str(_STYLE))
    plt.rcParams["font.serif"] = ["Times New Roman", "Liberation Serif",
                                   "DejaVu Serif", "serif"]

COLORS = {"always": "#00B945", "bypass": "#0C5DA5"}
MARKERS = {"always": "s",       "bypass": "o"}
LABELS = {
    "always": "always-probe (PrediCache)",
    "bypass": "bypass on meta(pref) hit",
}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", required=True)
    p.add_argument("--outdir", required=True)
    args = p.parse_args()

    df = pd.read_csv(args.csv)
    df = df.dropna(subset=["mops"])
    payloads = sorted(df.payload.unique())
    variants = ["always", "bypass"]

    # Median per (variant, payload, prefer_prob) so TRIALS>1 collapses cleanly.
    g = (df.groupby(["variant", "payload", "prefer_prob"])
            .mops.agg(["median", "min", "max"])
            .reset_index())

    n_payload = len(payloads)
    fig, axes = plt.subplots(2, 2, figsize=(6.8, 4.5), sharex=True)
    axes = axes.flatten()
    if n_payload < 4:
        for ax in axes[n_payload:]:
            ax.axis("off")

    for i, payload in enumerate(payloads):
        ax = axes[i]
        for v in variants:
            sub = g[(g.variant == v) & (g.payload == payload)].sort_values("prefer_prob")
            if sub.empty:
                continue
            ax.plot(sub.prefer_prob, sub["median"],
                    label=LABELS[v], color=COLORS[v], marker=MARKERS[v],
                    markersize=4, linewidth=1.0)
            ax.fill_between(sub.prefer_prob, sub["min"], sub["max"],
                            color=COLORS[v], alpha=0.15, linewidth=0)
        ax.set_title(f"payload = {payload} B", fontsize=9)
        ax.set_xlabel("prefer_prob (fraction of accesses hitting preferred)",
                      fontsize=7)
        ax.set_ylabel("Mops/s", fontsize=7)
        ax.grid(True, alpha=0.3)
        ax.tick_params(labelsize=7)

    # One shared legend at the top.
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels,
               loc="upper center", ncol=2,
               bbox_to_anchor=(0.5, 0.99),
               fontsize=8, frameon=False)
    fig.subplots_adjust(top=0.88, hspace=0.42, wspace=0.30,
                        left=0.10, right=0.98, bottom=0.10)

    outdir = Path(args.outdir)
    fig.savefig(outdir / "prefer_sweep.png", dpi=160)
    fig.savefig(outdir / "prefer_sweep.pdf")
    plt.close(fig)
    print(f"  wrote {outdir / 'prefer_sweep.png'} and .pdf")


if __name__ == "__main__":
    main()
