#!/usr/bin/env python3
"""Plot the bypass-vs-always-probe Pareto sweep.

Reads CSV with columns: variant,payload,prefer_prob,natural_hit_rate,mops
Emits a grid (one panel per payload, auto-sized to N panels) of throughput
vs prefer_prob, with two lines per panel (always-probe vs bypass). Output:
prefer_sweep.{png,pdf} in --outdir.
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

    # Auto-size the grid: aim for ~3 columns, as many rows as needed.
    n_payload = len(payloads)
    ncols = min(3, n_payload)
    nrows = (n_payload + ncols - 1) // ncols
    fig, axes = plt.subplots(
        nrows, ncols,
        figsize=(2.4 * ncols, 1.9 * nrows),
        sharex=True,
    )
    axes = [axes] if n_payload == 1 else (axes.flatten() if hasattr(axes, "flatten") else list(axes))
    # Hide any unused panels.
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
        if payload == 0:
            title = "payload = 0 B (translation-only)"
        elif payload >= 1024:
            title = f"payload = {payload // 1024} KiB"
        else:
            title = f"payload = {payload} B"
        ax.set_title(title, fontsize=8)
        ax.set_xlabel("prefer_prob", fontsize=7)
        ax.set_ylabel("Mops/s", fontsize=7)
        ax.grid(True, alpha=0.3)
        ax.tick_params(labelsize=7)

    # One shared legend at the top.
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels,
               loc="upper center", ncol=2,
               bbox_to_anchor=(0.5, 0.99),
               fontsize=8, frameon=False)
    fig.subplots_adjust(top=1.0 - 0.13 / nrows, hspace=0.50, wspace=0.32,
                        left=0.09, right=0.98, bottom=0.12 / nrows)

    outdir = Path(args.outdir)
    fig.savefig(outdir / "prefer_sweep.png", dpi=160)
    fig.savefig(outdir / "prefer_sweep.pdf")
    plt.close(fig)
    print(f"  wrote {outdir / 'prefer_sweep.png'} and .pdf")


if __name__ == "__main__":
    main()
