#!/usr/bin/env python3
"""
Augment a benchmark runtime CSV (written by scripts/measure/run_tpc.sh or
run_job.sh) with per-join THC telemetry columns.

For each row in the runtime CSV, the corresponding DuckDB profiling JSON is
located by reconstructing the path the shell script wrote it to. The JSON is
walked depth-first; every HASH_JOIN operator's `extra_info` contributes one
`JoinK` cell to the row. The cell is a single space-separated string of
`key=value` tokens. Empty values stay empty so downstream tooling can
distinguish "0 probes" from "this state was never reached."

The runtime CSV is rewritten in place with `Join1, Join2, ..., JoinN` columns
appended, where N is the maximum HASH_JOIN count observed across the sweep
(jagged rows are padded with empty cells).
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Iterable


# Keys we surface from each HASH_JOIN's extra_info, in the order they appear in
# the per-join cell.
THC_KEYS = [
    "THC Probe Table",
    "THC Build Table",
    "THC Build Rows",
    "THC Instantiated",
    "THC Final State",
    "THC Total Probes",
    "THC Probes At Freeze",
    "THC Freeze Reason",
    "THC Probes At Abandon",
    "THC Abandon Reason",
    "THC Total New Inserts",
    "THC First-Cycle U1",
]

# Map verbose extra_info key -> short token printed in the cell. Keep tokens
# stable; downstream notebooks may parse them.
SHORT = {
    "THC Probe Table": "probe",
    "THC Build Table": "build",
    "THC Build Rows": "build_rows",
    "THC Instantiated": "instantiated",
    "THC Final State": "state",
    "THC Total Probes": "total_probes",
    "THC Probes At Freeze": "probes_at_freeze",
    "THC Freeze Reason": "freeze_reason",
    "THC Probes At Abandon": "probes_at_abandon",
    "THC Abandon Reason": "abandon_reason",
    "THC Total New Inserts": "inserts",
    "THC First-Cycle U1": "u1",
}


def collect_hash_joins(node: dict) -> Iterable[dict]:
    """Depth-first walk of the profiling JSON. Yields each operator node whose
    extra_info carries our THC keys (i.e. a HASH_JOIN that ran the new code)."""
    info = node.get("extra_info") or {}
    if any(k in info for k in THC_KEYS):
        yield node
    for c in node.get("children") or []:
        yield from collect_hash_joins(c)


def format_join_cell(info: dict) -> str:
    """Render one HASH_JOIN's THC keys as a single CSV cell string."""
    tokens = []
    for key in THC_KEYS:
        val = info.get(key, "")
        # extra_info values can be lists when DuckDB sees embedded newlines.
        # Our keys never have newlines, but be defensive.
        if isinstance(val, list):
            val = " ".join(str(x) for x in val)
        tokens.append(f"{SHORT[key]}={val}")
    return " ".join(tokens)


def join_cells_for_query(profiling_json_path: Path) -> list[str]:
    if not profiling_json_path.exists():
        return []
    try:
        with profiling_json_path.open() as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"warning: could not parse {profiling_json_path}: {e}", file=sys.stderr)
        return []
    cells = []
    for hj in collect_hash_joins(data):
        info = hj.get("extra_info") or {}
        cells.append(format_join_cell(info))
    return cells


def query_token(query: str) -> str:
    """Strip the 'Q' prefix and any leading zero so e.g. 'Q05' -> '5'. JOB
    queries like '10a' come through unchanged. The shell scripts encode the
    query the same way when forming the profiling JSON filename."""
    q = query.strip()
    if q.startswith("Q") or q.startswith("q"):
        q = q[1:].lstrip("0") or "0"
    return q


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--csv", required=True, help="Runtime CSV to augment in place")
    p.add_argument("--profiling-dir", required=True, help="Directory of per-query profiling JSON files")
    p.add_argument("--prefix", required=True,
                   help="Filename prefix for profiling JSON files, e.g. 'tpch', 'tpcds', 'job'")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    prof_dir = Path(args.profiling_dir)
    prefix = args.prefix

    with csv_path.open(newline="") as f:
        reader = csv.reader(f)
        rows = list(reader)

    if not rows:
        print(f"warning: {csv_path} is empty", file=sys.stderr)
        return 0

    header = rows[0]
    body = rows[1:]
    needed = {"query", "case", "seed", "run_idx"}
    missing = needed - set(header)
    if missing:
        print(f"error: CSV {csv_path} is missing required column(s): {sorted(missing)}", file=sys.stderr)
        return 1
    idx = {h: i for i, h in enumerate(header)}

    # First pass: collect per-row Join cells and the global max join count.
    per_row_cells: list[list[str]] = []
    max_joins = 0
    for row in body:
        q = query_token(row[idx["query"]])
        c = row[idx["case"]]
        s = row[idx["seed"]] or "default"
        r = row[idx["run_idx"]]
        json_path = prof_dir / f"{prefix}_q{q}_case{c}_seed{s}_run{r}.json"
        cells = join_cells_for_query(json_path)
        per_row_cells.append(cells)
        max_joins = max(max_joins, len(cells))

    # Build new header: original + Join1..JoinN.
    new_header = list(header) + [f"Join{i+1}" for i in range(max_joins)]

    # Build new body: pad each row to max_joins.
    new_body = []
    for orig, cells in zip(body, per_row_cells):
        padded = cells + [""] * (max_joins - len(cells))
        new_body.append(list(orig) + padded)

    # Rewrite in place.
    with csv_path.open("w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(new_header)
        writer.writerows(new_body)

    print(f"thc_csv_postprocess: wrote {len(new_body)} rows with up to {max_joins} Join columns to {csv_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
