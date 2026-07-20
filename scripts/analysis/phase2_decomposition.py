"""Phase 2: optimizer value vs predicate-transfer robustness decomposition.

Per query, using the 20-seed sweep as the plan space:
    best(c)   = min over seeds of case c   (perfect optimizer proxy)
    median(c) = median over seeds          (typical fallible-optimizer plan)

Key ratios (>1 means the first quantity is slower):
    insurance          = median(c1) / median(c4)
        How much full RPT+ saves a fallible optimizer.
    insurance_fwd      = median(c1) / median(c2)
        Share of the insurance provided by the forward pass alone.
    residual_qo        = median(c4) / best(c4)
        How much a perfect optimizer still adds ON TOP of RPT+.
    residual_qo_c1     = median(c1) / best(c1)
        How much a perfect optimizer adds without PT (for contrast).
    pt_replaces_qo     = median(c4) / best(c1)
        "Skip the optimizer, run RPT+ on a typical plan" vs
        "perfect optimizer, vanilla execution". <1 -> PT on a random plan
        BEATS the best vanilla plan.
    backward_extra     = median(c2) / median(c4)
        What the backward pass adds for typical plans.

Failures capped at the sweep timeout (300/60 s): conservative for c1 which
fails most. CDFs and per-benchmark geomeans are produced.
Outputs: results/analysis/phase2/{decomposition.csv, cdf_*.png} + stdout summary.
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, load_runtimes, REPO_ROOT  # noqa: E402

OUT_DIR = os.path.join(REPO_ROOT, "results", "analysis", "phase2")
os.makedirs(OUT_DIR, exist_ok=True)

EPS = 0.005


def cap_for(benchmark):
    return 60.0 if benchmark == "appian" else 300.0


def main():
    rows = []
    for sw in load_sweep_index():
        df = load_runtimes(sw)
        cap = cap_for(sw.benchmark)
        failed = df["timeout"] | df["oom"]
        df["rt"] = df["runtime_seconds"].where(~failed, cap).clip(lower=EPS)
        med = df.pivot_table(index="query", columns="case", values="rt", aggfunc="median")
        mn = df.pivot_table(index="query", columns="case", values="rt", aggfunc="min")
        for q in med.index:
            rows.append({
                "label": sw.label, "benchmark": sw.benchmark, "sf": sw.scale_factor,
                "threads": sw.threads, "query": q,
                "med_c1": med.loc[q, 1], "med_c2": med.loc[q, 2],
                "med_c3": med.loc[q, 3], "med_c4": med.loc[q, 4],
                "best_c1": mn.loc[q, 1], "best_c2": mn.loc[q, 2],
                "best_c3": mn.loc[q, 3], "best_c4": mn.loc[q, 4],
                "insurance": med.loc[q, 1] / med.loc[q, 4],
                "insurance_fwd": med.loc[q, 1] / med.loc[q, 2],
                "residual_qo": med.loc[q, 4] / mn.loc[q, 4],
                "residual_qo_c1": med.loc[q, 1] / mn.loc[q, 1],
                "pt_replaces_qo": med.loc[q, 4] / mn.loc[q, 1],
                "backward_extra": med.loc[q, 2] / med.loc[q, 4],
            })
    out = pd.DataFrame(rows)
    out.to_csv(os.path.join(OUT_DIR, "decomposition.csv"), index=False)

    def geo(s):
        return float(np.exp(np.mean(np.log(s.clip(lower=1e-9)))))

    print("## Per-sweep geomeans of the decomposition ratios")
    print(f"{'sweep':16s} {'insur':>6s} {'insurF':>6s} {'residQO4':>8s} "
          f"{'residQO1':>8s} {'PTvsQO':>7s} {'bwdX':>6s}")
    for label in out.label.unique():
        s = out[out.label == label]
        print(f"{label:16s} {geo(s.insurance):6.2f} {geo(s.insurance_fwd):6.2f} "
              f"{geo(s.residual_qo):8.2f} {geo(s.residual_qo_c1):8.2f} "
              f"{geo(s.pt_replaces_qo):7.2f} {geo(s.backward_extra):6.2f}")

    print("\n## Share of queries where PT-on-typical-plan beats perfect-optimizer-vanilla")
    for label in out.label.unique():
        s = out[out.label == label]
        frac = float((s.pt_replaces_qo < 1.0).mean())
        frac_worse15 = float((s.pt_replaces_qo > 1.15).mean())
        print(f"{label:16s} med(c4)<best(c1): {frac*100:5.1f}%   "
              f">15% worse: {frac_worse15*100:5.1f}%")

    # ---- CDF figures for the 1-thread flagship sweeps ----
    flagship = ["job_t1", "tpch_sf100_t1", "tpcds_sf100_t1"]
    metrics = [("insurance", "median(c1)/median(c4)  [RPT+ insurance]"),
               ("residual_qo", "median(c4)/best(c4)  [QO value under RPT+]"),
               ("pt_replaces_qo", "median(c4)/best(c1)  [typical RPT+ vs best vanilla]")]
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5))
    for ax, (m, title) in zip(axes, metrics):
        for label in flagship:
            s = np.sort(out[out.label == label][m].values)
            ax.plot(s, np.arange(1, len(s) + 1) / len(s), label=label)
        ax.axvline(1.0, color="k", lw=0.8, ls="--")
        ax.set_xscale("log")
        ax.set_xlabel(title)
        ax.set_ylabel("CDF over queries")
        ax.grid(alpha=0.3)
    axes[0].legend()
    fig.suptitle("Optimizer value vs predicate-transfer robustness (1 thread)")
    fig.savefig(os.path.join(OUT_DIR, "cdf_decomposition_t1.png"), dpi=150,
                bbox_inches="tight")
    plt.close(fig)
    print(f"\nwrote {OUT_DIR}/decomposition.csv and cdf_decomposition_t1.png")


if __name__ == "__main__":
    main()
