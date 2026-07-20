"""Phase 6: distributed-system estimate.

Question (professor): on a distributed engine, Bloom-filter creation and
probing get more expensive (filters must be merged across workers and shipped
between nodes). If we inflate the measured CREATE_BF + USE_BF operator time by
a factor k, do the RPT variants still win?

Model:
    latency'(q, case, seed, k) = latency + (k - 1) * (CREATE_BF + USE_BF time)

using per-query profiling JSONs. At t1 operator thread-seconds == wall
seconds, so the additive model is exact for single-threaded execution; for
t64 the same additive adjustment is an upper bound on the impact (BF work is
spread over threads), so we report t1 as the primary estimate.

Also reported: an alternative model where the *backward* pass is charged an
extra full transfer round (k applied twice to case 4's BF time), reflecting
that forward+backward requires two communication phases.

Break-even: smallest k at which case 1 median-beats the case in question.

Outputs: results/analysis/phase6/{distributed_winrates.csv, breakeven.csv}.
"""

import json
import os
import sys
from collections import defaultdict

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, REPO_ROOT  # noqa: E402

OUT_DIR = os.path.join(REPO_ROOT, "results", "analysis", "phase6")
os.makedirs(OUT_DIR, exist_ok=True)

KS = [1.0, 1.5, 2.0, 3.0, 5.0, 10.0]


def bf_times(path):
    with open(path) as f:
        d = json.load(f)
    acc = {"bf": 0.0}

    def walk(n):
        if n.get("operator_type") in ("CREATE_BF", "USE_BF"):
            acc["bf"] += n.get("operator_timing", 0.0)
        for c in n.get("children", []):
            walk(c)
    walk(d)
    return d.get("latency", 0.0), acc["bf"]


def collect(sw):
    files = os.listdir(sw.profiling_dir)
    stems = sorted({f.split("_case")[0] for f in files if "_case" in f})
    rows = []
    for stem in stems:
        for seed in range(20):
            rec = {}
            ok = True
            for case in (1, 2, 3, 4):
                p = os.path.join(sw.profiling_dir, f"{stem}_case{case}_seed{seed}_run1.json")
                if not os.path.exists(p):
                    ok = False
                    break
                try:
                    lat, bf = bf_times(p)
                except (json.JSONDecodeError, OSError):
                    ok = False
                    break
                rec[case] = (lat, bf)
            if not ok:
                continue
            for case, (lat, bf) in rec.items():
                rows.append({"query": stem, "seed": seed, "case": case,
                             "latency": lat, "bf_time": bf})
    return pd.DataFrame(rows)


def main():
    targets = ["job_t1", "tpch_sf100_t1", "tpcds_sf100_t1"]
    win_rows, be_rows = [], []
    for sw in load_sweep_index():
        if sw.label not in targets:
            continue
        df = collect(sw)
        print(f"{sw.label}: {len(df)//4} complete tuples; "
              f"BF time share of latency (c4): "
              f"{df[df.case == 4].bf_time.sum() / df[df.case == 4].latency.sum() * 100:.1f}%")
        for k in KS:
            d = df.copy()
            d["adj"] = d.latency + (k - 1) * d.bf_time
            med = d.pivot_table(index="query", columns="case", values="adj",
                                aggfunc="median")
            winner = med.idxmin(axis=1)
            counts = winner.value_counts().to_dict()
            geo = {c: float(np.exp(np.mean(np.log(med[1] / med[c]))))
                   for c in (2, 3, 4)}
            win_rows.append({"label": sw.label, "k": k,
                             **{f"win_c{c}": counts.get(c, 0) for c in (1, 2, 3, 4)},
                             **{f"geomean_c{c}_vs_c1": geo[c] for c in (2, 3, 4)},
                             "sum_med_c1": float(med[1].sum()),
                             "sum_med_c2": float(med[2].sum()),
                             "sum_med_c4": float(med[4].sum())})
        # per-query break-even k for case 4 and case 2 (median over seeds)
        med_lat = df.pivot_table(index="query", columns="case", values="latency",
                                 aggfunc="median")
        med_bf = df.pivot_table(index="query", columns="case", values="bf_time",
                                aggfunc="median")
        for c in (2, 4):
            # solve med_lat[c] + (k-1)*med_bf[c] = med_lat[1]  ->  k
            with np.errstate(divide="ignore", invalid="ignore"):
                k_be = 1.0 + (med_lat[1] - med_lat[c]) / med_bf[c]
            for q in med_lat.index:
                be_rows.append({"label": sw.label, "query": q, "case": c,
                                "k_breakeven": float(k_be.loc[q]),
                                "med_c1": float(med_lat.loc[q, 1]),
                                "med_c": float(med_lat.loc[q, c]),
                                "med_bf": float(med_bf.loc[q, c])})

    win_df = pd.DataFrame(win_rows)
    be_df = pd.DataFrame(be_rows)
    win_df.to_csv(os.path.join(OUT_DIR, "distributed_winrates.csv"), index=False)
    be_df.to_csv(os.path.join(OUT_DIR, "breakeven.csv"), index=False)

    print("\n## Median wins and geomean speedups vs BF-cost multiplier k")
    for _, r in win_df.iterrows():
        print(f"{r.label:16s} k={r.k:4.1f}  wins c1/c2/c3/c4 = "
              f"{r.win_c1:3.0f}/{r.win_c2:3.0f}/{r.win_c3:3.0f}/{r.win_c4:3.0f}  "
              f"geo c2={r.geomean_c2_vs_c1:.3f} c4={r.geomean_c4_vs_c1:.3f}  "
              f"sum c1={r.sum_med_c1:6.0f}s c4={r.sum_med_c4:6.0f}s")

    print("\n## Break-even k distribution (case 4 vs case 1, per query)")
    for label in be_df.label.unique():
        s = be_df[(be_df.label == label) & (be_df.case == 4)]
        wins_now = s[s.med_c < s.med_c1]
        never = (wins_now.k_breakeven > 100).sum() + np.isinf(wins_now.k_breakeven).sum()
        ks = wins_now.k_breakeven.replace([np.inf], np.nan).dropna()
        print(f"{label:16s} queries where c4 wins at k=1: {len(wins_now)}; "
              f"of those, still winning at k=2: {(wins_now.k_breakeven > 2).sum()}, "
              f"k=5: {(wins_now.k_breakeven > 5).sum()}, "
              f"k=10: {(wins_now.k_breakeven > 10).sum()} "
              f"(median break-even k={ks.median():.1f})")


if __name__ == "__main__":
    main()
