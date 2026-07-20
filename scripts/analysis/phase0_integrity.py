"""Phase 0: corpus integrity checks.

Verifies for every sweep in results_summary.csv:
  - runtime CSV / profiling dir / boxplots dir exist
  - every query has 4 cases x 20 seeds (flags gaps)
  - sentinel (timeout / OOM) counts per case

Also re-validates the meeting-notes claims that came from OLDER runs
(boxplots_job_runtimes_20260514_041359, boxplots_tpch_runtimes_sf100_20260630_131335):
queries where vanilla DuckDB's best seed beats every other case's best seed.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, load_runtimes, per_query_stats  # noqa: E402

import pandas as pd  # noqa: E402


def main():
    sweeps = load_sweep_index()
    print(f"# sweeps in results_summary.csv: {len(sweeps)}\n")

    all_ok = True
    for sw in sweeps:
        problems = []
        if not os.path.exists(sw.runtimes_csv):
            problems.append(f"MISSING runtimes csv: {sw.runtimes_csv}")
            print(f"[{sw.label}] " + "; ".join(problems))
            all_ok = False
            continue
        if not os.path.isdir(sw.profiling_dir):
            problems.append("missing profiling dir")
        if not os.path.isdir(sw.boxplots_dir):
            problems.append("missing boxplots dir")

        df = load_runtimes(sw)
        n_queries = df["query"].nunique()
        combos = df.groupby("query")[["case", "seed"]].nunique()
        bad_cases = combos[combos["case"] != 4]
        bad_seeds = combos[combos["seed"] != 20]
        expected = n_queries * 4 * 20
        n_to = int(df["timeout"].sum())
        n_oom = int(df["oom"].sum())
        to_by_case = df[df.timeout].groupby("case").size().to_dict()
        oom_by_case = df[df.oom].groupby("case").size().to_dict()

        msg = (f"[{sw.label:16s}] commit={sw.commit[:10]} queries={n_queries:3d} "
               f"rows={len(df):5d}/{expected:5d} timeouts={n_to:3d}{to_by_case if n_to else ''} "
               f"ooms={n_oom}{oom_by_case if n_oom else ''}")
        if len(bad_cases):
            msg += f" | queries w/o 4 cases: {list(bad_cases.index)}"
            all_ok = False
        if len(bad_seeds):
            msg += f" | queries w/o 20 seeds: {list(bad_seeds.index)}"
            all_ok = False
        if problems:
            msg += " | " + "; ".join(problems)
        print(msg)

    print("\nIntegrity:", "OK" if all_ok else "GAPS FOUND (see above)")

    # ---- Re-validate meeting-note claims on latest sweeps --------------------
    # Notes said: DuckDB's lowest-whisker is the global best for JOB 10b, 23b
    # (from the May 14 run) and TPC-H SF100 Q4, Q11, Q12, Q13, Q16 (from the
    # Jun 30 run). Check whether min-over-seeds of case 1 is the global min in
    # the LATEST 1-thread sweeps.
    print("\n== Meeting-note re-validation: is min(case1) the global best? ==")
    for sw in sweeps:
        if sw.threads != 1 or sw.benchmark not in ("job", "tpch"):
            continue
        if sw.benchmark == "tpch" and sw.scale_factor != 100:
            continue
        df = load_runtimes(sw)
        stats = per_query_stats(df)
        piv = stats.pivot_table(index="query", columns="case", values="min")
        best_case = piv.idxmin(axis=1)
        duck_best = sorted(best_case[best_case == 1].index)
        print(f"[{sw.label}] queries where case 1 has the global best runtime "
              f"({len(duck_best)}/{piv.shape[0]}): {duck_best}")

    # Older runs referenced in the notes, for comparison:
    print("\n== Same check on the meeting-notes (older) runs ==")
    old = {
        "job_may14": "/mnt/local_ssd/results-spy/results/job/job_runtimes_20260514_041359.csv",
        "tpch_sf100_jun30": "/mnt/local_ssd/results-spy/results/tpch/tpch_runtimes_sf100_20260630_131335.csv",
    }
    for name, path in old.items():
        if not os.path.exists(path):
            print(f"[{name}] file not found: {path}")
            continue
        df = pd.read_csv(path, dtype={"query": str})
        df["case"] = df["case"].astype(int)
        ok = df[~df.runtime_seconds.isin([9999999, 8888888])]
        piv = ok.groupby(["query", "case"]).runtime_seconds.min().unstack()
        best_case = piv.idxmin(axis=1)
        duck_best = sorted(best_case[best_case == 1].index)
        print(f"[{name}] queries where case 1 has the global best runtime "
              f"({len(duck_best)}/{piv.shape[0]}): {duck_best}")


if __name__ == "__main__":
    main()
