#!/usr/bin/env python3
"""

CAREFUL, THIS SCRIPT IS DUPLICATED ACROSS THE spy AND results-spy REPOS!!!!!


Plot per-query runtime box plots from a CSV produced by run_job.sh
(or any CSV with columns: query, case, seed, runtime_seconds).

For each query, produces one PNG with one box per case showing the
distribution of runtimes across seeds.

Usage:
  scripts/measure/plot_runtime_boxplots.py --csv <path> [--out-dir <dir>]
"""
import argparse
import csv
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

CASE_NAMES = {
    "1": "DuckDB",
    "2": "Forward-Only",
    "3": "THC",
    "4": "RPT",
}


def case_label(case):
    name = CASE_NAMES.get(str(case))
    return name if name else f"Case {case}"


def load_csv(path):
    """Returns {query: {case: [runtime_seconds, ...]}}."""
    data = defaultdict(lambda: defaultdict(list))
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        required = {"query", "case", "seed", "runtime_seconds"}
        if not required.issubset(reader.fieldnames or []):
            sys.exit(
                f"Error: CSV must have columns {sorted(required)}; "
                f"got {reader.fieldnames}"
            )
        for row in reader:
            data[row["query"]][row["case"]].append(float(row["runtime_seconds"]))
    return data


def plot_query(query, by_case, out_path):
    cases = sorted(by_case.keys(), key=lambda c: (len(c), c))
    samples = [by_case[c] for c in cases]
    n_seeds = max(len(s) for s in samples)

    fig, ax = plt.subplots(figsize=(max(4, 1.2 * len(cases) + 2), 5))
    ax.boxplot(
        samples,
        tick_labels=[case_label(c) for c in cases],
        showmeans=True,
        meanline=True,
    )
    # Overlay individual points so single-seed boxes are still visible.
    for i, s in enumerate(samples, start=1):
        ax.scatter([i] * len(s), s, alpha=0.5, s=20, color="tab:blue")
    ax.set_ylabel("Runtime (seconds)")
    ax.set_title(f"JOB query {query}  (n_seeds = {n_seeds})")
    ax.grid(True, axis="y", linestyle=":", alpha=0.5)
    # Anchor the y-axis at 0 so visual bar/box heights are proportional to
    # absolute runtime instead of being truncated to the data range.
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120)
    plt.close(fig)


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--csv", required=True, help="Path to runtimes CSV")
    ap.add_argument(
        "--out-dir",
        default=None,
        help="Output directory for PNGs (default: <csv-dir>/boxplots_<csv-stem>/)",
    )
    args = ap.parse_args()

    if not os.path.isfile(args.csv):
        sys.exit(f"Error: CSV not found: {args.csv}")

    out_dir = args.out_dir
    if out_dir is None:
        csv_dir = os.path.dirname(os.path.abspath(args.csv))
        stem = os.path.splitext(os.path.basename(args.csv))[0]
        out_dir = os.path.join(csv_dir, f"boxplots_{stem}")
    os.makedirs(out_dir, exist_ok=True)

    data = load_csv(args.csv)
    if not data:
        sys.exit(f"Error: no rows found in {args.csv}")

    queries = sorted(data.keys(), key=lambda q: (len(q), q))
    for q in queries:
        out_path = os.path.join(out_dir, f"{q}.png")
        plot_query(q, data[q], out_path)
        print(f"wrote {out_path}")

    print(f"Done. {len(queries)} figure(s) in {out_dir}")


if __name__ == "__main__":
    main()
