#!/usr/bin/env python3
"""
Condense a benchmark runtime CSV (written by scripts/measure/run_tpc.sh,
run_job.sh, or run_appian.sh, possibly already augmented by
thc_csv_postprocess.py) down to one row per (query, case).

For each (query, case) group the rows are sorted by `runtime_seconds` and the
row at index `len(group) // 2` is kept -- the true median for an odd number of
runs, and the upper of the two middle rows for an even number. OOM/temp-spill
and timeout sentinel rows (`runtime_seconds == 8888888` or `9999999`) sort to
the top and are kept as legitimate data points.

The selected rows are written to a new CSV (default: the input path with
`_median` inserted before the `.csv` extension) with the same header and the
full set of original columns -- including any `Join*-*` THC telemetry columns --
preserved verbatim.
"""

from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path


def default_out_path(csv_path: Path) -> Path:
    """Insert `_median` before the suffix: foo.csv -> foo_median.csv."""
    return csv_path.with_name(f"{csv_path.stem}_median{csv_path.suffix}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--csv", required=True, help="Input runtime CSV to condense")
    p.add_argument("--out", default=None,
                   help="Output CSV path (default: <csv> with '_median' before .csv)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    out_path = Path(args.out) if args.out else default_out_path(csv_path)

    with csv_path.open(newline="") as f:
        rows = list(csv.reader(f))

    if not rows:
        print(f"warning: {csv_path} is empty", file=sys.stderr)
        return 0

    header = rows[0]
    body = rows[1:]
    needed = {"query", "case", "runtime_seconds"}
    missing = needed - set(header)
    if missing:
        print(f"error: CSV {csv_path} is missing required column(s): {sorted(missing)}", file=sys.stderr)
        return 1
    idx = {h: i for i, h in enumerate(header)}

    # Group rows by (query, case), preserving the order in which each group key
    # first appears in the input.
    groups: dict[tuple[str, str], list[list[str]]] = {}
    for row in body:
        key = (row[idx["query"]], row[idx["case"]])
        groups.setdefault(key, []).append(row)

    # For each group, sort by runtime and pick the upper-middle row.
    selected = []
    for key, group in groups.items():
        group.sort(key=lambda r: float(r[idx["runtime_seconds"]]))
        selected.append(group[len(group) // 2])

    with out_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(selected)

    print(f"median_runtime_csv: wrote {len(selected)} rows (one per query/case) to {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
