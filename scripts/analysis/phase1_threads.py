"""Phase 1b: why does predicate transfer's advantage shrink at 64 threads?

Parses the per-query DuckDB profiling JSONs of selected sweeps and aggregates
operator time by class:
    SCAN     = TABLE_SCAN
    JOIN     = HASH_JOIN (+ other joins)
    BF       = CREATE_BF + USE_BF
    OTHER    = everything else (group by, projections, sort, ...)

Reported per (sweep, case):
  - total wall latency (sum over queries of JSON `latency`)
  - total operator thread-seconds per class
  - effective parallelism = thread-seconds / wall
The comparison of t1 vs t64 shows where the case differences live and why they
vanish (or don't) with parallelism.

Only tuples with an existing JSON are counted, and only (query,seed) tuples
present for ALL FOUR cases in that sweep are included, so per-case sums are
comparable (timed-out tuples have no JSON).
"""

import json
import os
import sys
from collections import defaultdict

import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, REPO_ROOT  # noqa: E402

OUT_DIR = os.path.join(REPO_ROOT, "results", "analysis", "phase1")
os.makedirs(OUT_DIR, exist_ok=True)

JOIN_OPS = {"HASH_JOIN", "PIECEWISE_MERGE_JOIN", "NESTED_LOOP_JOIN", "BLOCKWISE_NL_JOIN",
            "CROSS_PRODUCT", "LEFT_DELIM_JOIN", "RIGHT_DELIM_JOIN", "ASOF_JOIN"}
BF_OPS = {"CREATE_BF", "USE_BF"}


def classify(op):
    if op == "TABLE_SCAN":
        return "SCAN"
    if op in JOIN_OPS:
        return "JOIN"
    if op == "CREATE_BF":
        return "CREATE_BF"
    if op == "USE_BF":
        return "USE_BF"
    return "OTHER"


def profile_breakdown(path):
    with open(path) as f:
        d = json.load(f)
    acc = defaultdict(float)

    def walk(n):
        op = n.get("operator_type")
        if op:
            acc[classify(op)] += n.get("operator_timing", 0.0)
        for c in n.get("children", []):
            walk(c)
    walk(d)
    return d.get("latency", 0.0), acc


def sweep_prefix(sw):
    return {"tpch": "tpch", "tpcds": "tpcds", "job": "job", "appian": "appian"}[sw.benchmark]


def analyze(sw, seeds=range(20)):
    # discover queries from profiling dir contents; stems like 'tpch_q5' / 'job_q10a'
    files = os.listdir(sw.profiling_dir)
    queries = sorted({f.split("_case")[0] for f in files if "_case" in f})
    rows = []
    for q in queries:
        for seed in seeds:
            paths = {}
            ok = True
            for case in (1, 2, 3, 4):
                stem = f"{q}_case{case}_seed{seed}_run1.json"
                p = os.path.join(sw.profiling_dir, stem)
                if not os.path.exists(p):
                    ok = False
                    break
                paths[case] = p
            if not ok:
                continue
            for case, p in paths.items():
                try:
                    lat, acc = profile_breakdown(p)
                except (json.JSONDecodeError, OSError):
                    ok = False
                    break
                rows.append({"query": q, "seed": seed, "case": case, "latency": lat,
                             **{k: acc.get(k, 0.0) for k in
                                ("SCAN", "JOIN", "CREATE_BF", "USE_BF", "OTHER")}})
    df = pd.DataFrame(rows)
    df["label"] = sw.label
    return df


def main():
    targets = {("tpch", 100, 1), ("tpch", 100, 64), ("tpch", 100, 8),
               ("tpcds", 100, 1), ("tpcds", 100, 64),
               ("job", 0, 1), ("job", 0, 64)}
    all_frames = []
    for sw in load_sweep_index():
        if (sw.benchmark, sw.scale_factor, sw.threads) not in targets:
            continue
        if not os.path.isdir(sw.profiling_dir):
            print(f"skip {sw.label}: no profiling dir")
            continue
        df = analyze(sw)
        all_frames.append(df)
        agg = df.groupby("case")[["latency", "SCAN", "JOIN", "CREATE_BF", "USE_BF", "OTHER"]].sum()
        agg["thread_secs"] = agg[["SCAN", "JOIN", "CREATE_BF", "USE_BF", "OTHER"]].sum(axis=1)
        agg["eff_parallelism"] = agg["thread_secs"] / agg["latency"]
        n = df.groupby("case").size().iloc[0]
        print(f"\n=== {sw.label} (complete 4-case tuples: {n}) ===")
        print(agg.round(1).to_string())

    out = pd.concat(all_frames, ignore_index=True)
    out.to_csv(os.path.join(OUT_DIR, "operator_breakdown.csv"), index=False)
    print(f"\nwrote {os.path.join(OUT_DIR, 'operator_breakdown.csv')}")


if __name__ == "__main__":
    main()
