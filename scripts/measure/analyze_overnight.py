#!/usr/bin/env python3
"""Post-process overnight matrix output.

Reads per-config CSV files written by run_overnight_matrix.sh and computes
per-query c3-c2 deltas with mean, median, and 95% confidence intervals across
passes. The point of multi-pass is to push past single-run noise; this script
makes the residual signal interpretable.

Usage:
    python3 scripts/measure/analyze_overnight.py path/to/overnight_<ts>/

CSV format expected:
    query,case,seed,pass,runtime_seconds
"""

from __future__ import annotations

import csv
import math
import sys
from collections import defaultdict
from pathlib import Path


def read_csv(path: Path) -> dict[tuple[str, int], list[float]]:
    """Group runtime samples by (query, case). Each (query, case) is sampled
    once per seed-pass combination, so the list captures the full distribution
    across seeds and passes for that (query, case)."""
    samples: dict[tuple[str, int], list[float]] = defaultdict(list)
    with path.open() as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            key = (row["query"], int(row["case"]))
            samples[key].append(float(row["runtime_seconds"]))
    return samples


def percentile(xs: list[float], p: float) -> float:
    if not xs:
        return float("nan")
    s = sorted(xs)
    k = (len(s) - 1) * p
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return s[int(k)]
    return s[f] + (s[c] - s[f]) * (k - f)


def mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else float("nan")


def stdev(xs: list[float]) -> float:
    if len(xs) < 2:
        return float("nan")
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))


def ci95(xs: list[float]) -> tuple[float, float]:
    """Approximate 95% CI from sample std (large-N gaussian assumption)."""
    if len(xs) < 2:
        return (float("nan"), float("nan"))
    m = mean(xs)
    se = stdev(xs) / math.sqrt(len(xs))
    return (m - 1.96 * se, m + 1.96 * se)


def summarize_config(csv_path: Path) -> dict:
    samples = read_csv(csv_path)
    c2_keys = {q for (q, c) in samples if c == 2}
    c3_keys = {q for (q, c) in samples if c == 3}
    common = c2_keys & c3_keys

    c2_all: list[float] = []
    c3_all: list[float] = []
    per_query_delta: list[float] = []

    for q in sorted(common):
        c2_runs = samples[(q, 2)]
        c3_runs = samples[(q, 3)]
        c2_all.extend(c2_runs)
        c3_all.extend(c3_runs)
        per_query_delta.append(mean(c3_runs) - mean(c2_runs))

    out = {
        "config": csv_path.stem,
        "n_queries": len(common),
        "n_c2": len(c2_all),
        "n_c3": len(c3_all),
        "c2_total": sum(c2_all),
        "c3_total": sum(c3_all),
        "c2_mean_per_run": mean(c2_all),
        "c3_mean_per_run": mean(c3_all),
        "c3_minus_c2_mean_per_query": mean(per_query_delta),
        "c3_minus_c2_median_per_query": percentile(per_query_delta, 0.5),
        "c3_minus_c2_ci95_lo": ci95(per_query_delta)[0],
        "c3_minus_c2_ci95_hi": ci95(per_query_delta)[1],
        "n_help": sum(1 for d in per_query_delta if d < -0.005),
        "n_neutral": sum(1 for d in per_query_delta if abs(d) <= 0.005),
        "n_hurt": sum(1 for d in per_query_delta if d > 0.005),
    }
    return out


def fmt(x: float, fmtspec: str = "%+.4f") -> str:
    if math.isnan(x):
        return "  nan  "
    return fmtspec % x


def main() -> int:
    if len(sys.argv) != 2:
        print(f"Usage: {sys.argv[0]} <overnight_dir>", file=sys.stderr)
        return 2

    out_dir = Path(sys.argv[1])
    if not out_dir.is_dir():
        print(f"Not a directory: {out_dir}", file=sys.stderr)
        return 2

    csvs = sorted(out_dir.glob("ab_*.csv"))
    if not csvs:
        print(f"No CSVs found in {out_dir}", file=sys.stderr)
        return 2

    summaries = [summarize_config(p) for p in csvs]

    # Per-config table
    print(f"{'config':<28} {'c2_tot':>9} {'c3_tot':>9} {'mean_dt':>10} {'med_dt':>10} "
          f"{'CI95_lo':>10} {'CI95_hi':>10} {'help':>5} {'neut':>5} {'hurt':>5}")
    for s in summaries:
        # Truncate config name to just the variant portion (drop ab_, _<sha>)
        cfg = s["config"]
        if cfg.startswith("ab_"):
            cfg = cfg[3:]
        cfg = cfg.rsplit("_", 1)[0]  # drop trailing _<sha>
        print(f"{cfg:<28} "
              f"{s['c2_total']:>9.2f} "
              f"{s['c3_total']:>9.2f} "
              f"{fmt(s['c3_minus_c2_mean_per_query']):>10} "
              f"{fmt(s['c3_minus_c2_median_per_query']):>10} "
              f"{fmt(s['c3_minus_c2_ci95_lo']):>10} "
              f"{fmt(s['c3_minus_c2_ci95_hi']):>10} "
              f"{s['n_help']:>5} "
              f"{s['n_neutral']:>5} "
              f"{s['n_hurt']:>5}")

    # If a baseline config exists, compute pairwise deltas vs it.
    baseline = next((s for s in summaries if "baseline" in s["config"]), None)
    if baseline:
        print()
        print(f"=== Pairwise c3 mean delta vs baseline ({baseline['config']}) ===")
        print(f"{'config':<28} {'mean_dt':>10} {'CI95_lo':>10} {'CI95_hi':>10}")
        for s in summaries:
            if s is baseline:
                continue
            cfg = s["config"]
            if cfg.startswith("ab_"):
                cfg = cfg[3:]
            cfg = cfg.rsplit("_", 1)[0]
            d_mean = s["c3_minus_c2_mean_per_query"] - baseline["c3_minus_c2_mean_per_query"]
            # CI for difference of means (approximate, independent samples)
            lo_diff = (s["c3_minus_c2_ci95_lo"] or 0) - (baseline["c3_minus_c2_ci95_hi"] or 0)
            hi_diff = (s["c3_minus_c2_ci95_hi"] or 0) - (baseline["c3_minus_c2_ci95_lo"] or 0)
            print(f"{cfg:<28} "
                  f"{fmt(d_mean):>10} "
                  f"{fmt(lo_diff):>10} "
                  f"{fmt(hi_diff):>10}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
