"""Pretty-printer for DuckDB profiling JSONs in the corpus.

Usage:
    python plan_tree.py <sweep_label> <query_stem> <case> <seed> [--min-timing SECS]

e.g.
    python plan_tree.py job_t1 job_q10b 1 6
    python plan_tree.py tpch_sf100_t1 tpch_q8 4 3

Prints the operator tree with per-operator timing (thread-seconds),
cardinality, and the table/join info from extra_info, so plans across
cases/seeds can be compared side by side.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index  # noqa: E402


def load_profile(label, qstem, case, seed):
    sw = {s.label: s for s in load_sweep_index()}[label]
    path = os.path.join(sw.profiling_dir, f"{qstem}_case{case}_seed{seed}_run1.json")
    with open(path) as f:
        return json.load(f), path


def describe(node):
    op = node.get("operator_type", "ROOT")
    extra = node.get("extra_info", {}) or {}
    bits = []
    if op == "TABLE_SCAN":
        bits.append(extra.get("Table", "?"))
    if "Join Type" in extra:
        bits.append(extra["Join Type"])
    if "Conditions" in extra:
        cond = extra["Conditions"]
        if isinstance(cond, list):
            cond = " AND ".join(cond)
        bits.append(str(cond)[:80])
    # CREATE_BF / USE_BF carry BF sizing info in extra_info on some builds
    for k in ("Bloom Filters", "BF size", "Filtered Table"):
        if k in extra:
            bits.append(f"{k}={extra[k]}")
    return op, " | ".join(bits)


def print_tree(d, min_timing=0.0):
    lat = d.get("latency")
    print(f"latency={lat}s  cpu={d.get('cpu_time')}  "
          f"peak_buffer={d.get('system_peak_buffer_memory')}  "
          f"peak_temp={d.get('system_peak_temp_dir_size')}")

    def walk(n, depth):
        op, desc = describe(n)
        t = n.get("operator_timing", 0.0)
        card = n.get("operator_cardinality", "")
        mark = "" if t >= min_timing else " ."
        print(f"{'  ' * depth}{op:<16s} t={t:9.3f}  card={card:>12}  {desc}{mark}")
        for c in n.get("children", []):
            walk(c, depth + 1)

    for c in d.get("children", [d]):
        walk(c, 0)


if __name__ == "__main__":
    label, qstem, case, seed = sys.argv[1:5]
    d, path = load_profile(label, qstem, int(case), int(seed))
    print(f"# {path}")
    print_tree(d)
