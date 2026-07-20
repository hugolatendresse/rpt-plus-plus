"""Corpus loader for the RPT+/THC paper analysis.

Loads every benchmark sweep listed in results_summary.csv (repo root) from the
results-spy corpus directory. Each sweep is a runtime CSV with columns
    query, case, seed, run_idx, runtime_seconds [, Join1-* .. JoinN-* THC telemetry]
swept over cases 1-4 and transfer_graph_seed 0-19.

Case semantics (see scripts/measure/run_tpc.sh case_settings_for):
    1: vanilla DuckDB execution      (disable_rpt, disable_tiered_hash_cache)
    2: forward-only RPT+             (rpt_forward_only, disable_tiered_hash_cache)
    3: forward-only RPT+ with THC    (rpt_forward_only)
    4: full forward+backward RPT+    (disable_tiered_hash_cache)

All cases share the same seeded random left-deep join order (join_order_mode =
'seeded_left_deep', use_seeded_transfer_order = true), so for a given
(query, seed) all four cases execute the *same* join order; the seed sweep
models an optimizer that may pick any of 20 plans.

Sentinel runtimes (see run_tpc.sh run_timed_query):
    9999999 -> wall-clock timeout (300s TPC / JOB, 60s appian)
    8888888 -> DuckDB OOM / temp-spill-limit failure
"""

import csv
import os
from dataclasses import dataclass, field

import pandas as pd

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SUMMARY_CSV = os.path.join(REPO_ROOT, "results_summary.csv")
RESULTS_SPY = "/mnt/local_ssd/results-spy/results"

TIMEOUT_SENTINEL = 9999999
OOM_SENTINEL = 8888888

CASE_NAMES = {
    1: "DuckDB",
    2: "Fwd-only",
    3: "Fwd+THC",
    4: "RPT+",
}


@dataclass
class Sweep:
    commit: str
    benchmark: str          # 'job' | 'appian' | 'tpch' | 'tpcds'
    scale_factor: int       # 0 for job/appian
    threads: int
    runtimes_csv: str       # absolute path
    profiling_dir: str      # absolute path (may not exist)
    boxplots_dir: str       # absolute path (may not exist)
    label: str = field(init=False)

    def __post_init__(self):
        sf = f"_sf{self.scale_factor}" if self.scale_factor else ""
        self.label = f"{self.benchmark}{sf}_t{self.threads}"


def _bench_dir(benchmark: str) -> str:
    # TPC-DS results live under results/tpch/ in the corpus (historical quirk).
    return {"job": "job", "appian": "appian", "tpch": "tpch", "tpcds": "tpch"}[benchmark]


def _parse_bench_field(bench_field: str):
    b = bench_field.strip().lower()
    if b.startswith("tpch"):
        return "tpch", int(b.split("sf")[1])
    if b.startswith("tpcds"):
        return "tpcds", int(b.split("sf")[1])
    return b, 0


def load_sweep_index() -> list:
    """Parse results_summary.csv into a list of Sweep objects with resolved paths."""
    sweeps = []
    with open(SUMMARY_CSV) as fh:
        for row in csv.DictReader(fh):
            benchmark, sf = _parse_bench_field(row["Benchmark"])
            d = os.path.join(RESULTS_SPY, _bench_dir(benchmark))
            rt = row["Runtimes"].strip()
            if not rt.endswith(".csv"):
                rt += ".csv"
            sweeps.append(Sweep(
                commit=row["commit"].strip(),
                benchmark=benchmark,
                scale_factor=sf,
                threads=int(row["Thread Count"]),
                runtimes_csv=os.path.join(d, rt),
                profiling_dir=os.path.join(d, row["Profiling"].strip()),
                boxplots_dir=os.path.join(d, row["Boxplots"].strip()),
            ))
    return sweeps


def load_runtimes(sweep: Sweep) -> pd.DataFrame:
    """Load one sweep's runtime CSV. Adds columns:
    - timeout / oom: bool flags for the sentinel values
    - runtime: runtime_seconds with sentinels replaced by NaN (use for stats)
    """
    df = pd.read_csv(sweep.runtimes_csv, dtype={"query": str}, low_memory=False)
    df["case"] = df["case"].astype(int)
    df["seed"] = pd.to_numeric(df["seed"], errors="coerce").astype("Int64")
    timeout = df["runtime_seconds"] == TIMEOUT_SENTINEL
    oom = df["runtime_seconds"] == OOM_SENTINEL
    extra = pd.DataFrame({
        "timeout": timeout,
        "oom": oom,
        "runtime": df["runtime_seconds"].where(~(timeout | oom)),
        "label": sweep.label,
        "benchmark": sweep.benchmark,
        "scale_factor": sweep.scale_factor,
        "threads": sweep.threads,
    }, index=df.index)
    return pd.concat([df, extra], axis=1)


def load_all(benchmarks=None, threads=None) -> pd.DataFrame:
    """Concatenate all sweeps (optionally filtered) into one long DataFrame."""
    frames = []
    for sw in load_sweep_index():
        if benchmarks and sw.benchmark not in benchmarks:
            continue
        if threads and sw.threads not in threads:
            continue
        frames.append(load_runtimes(sw))
    return pd.concat(frames, ignore_index=True)


def per_query_stats(df: pd.DataFrame) -> pd.DataFrame:
    """Per (label, query, case) statistics over the seed sweep.

    A timed-out/OOMed run counts as 'failure'; for median/min purposes timeouts
    are treated as +inf (a timeout IS worse than any measured runtime, so
    dropping them would bias medians optimistically for the failing case).
    """
    d = df.copy()
    penal = d["runtime_seconds"].where(~(d["timeout"] | d["oom"]), float("inf"))
    d["runtime_penalized"] = penal
    g = d.groupby(["label", "query", "case"])
    out = pd.DataFrame({
        "median": g["runtime_penalized"].median(),
        "mean": g["runtime_penalized"].mean(),
        "min": g["runtime_penalized"].min(),
        "max": g["runtime_penalized"].max(),
        "p10": g["runtime_penalized"].quantile(0.10),
        "p90": g["runtime_penalized"].quantile(0.90),
        "n_runs": g["runtime_seconds"].size,
        "n_timeout": g["timeout"].sum(),
        "n_oom": g["oom"].sum(),
    })
    return out.reset_index()
