"""Phase 4: THC (TieredHashCache) autopsy.

The sweeps ran THC in its *non-adaptive* configuration (all runtime
freeze/abandon checks disabled; activation threshold 1M build-side rows,
collect phase 100k probes, budget 36MB). Questions:

1. How often does THC even instantiate? (per benchmark, case 3 runs)
2. When it instantiates, what are build sizes / probe counts / insert counts?
3. Per (query,seed): does case3-vs-case2 runtime delta correlate with THC
   instantiation and with probe/build characteristics?
4. Which queries does THC help / hurt the most?

Outputs: results/analysis/phase4/{thc_states.csv, thc_deltas.csv} + stdout.
"""

import os
import sys

import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, load_runtimes, REPO_ROOT  # noqa: E402

OUT_DIR = os.path.join(REPO_ROOT, "results", "analysis", "phase4")
os.makedirs(OUT_DIR, exist_ok=True)

MAX_JOINS = 40


def telemetry_long(df):
    """Explode JoinN-* columns of case-3 rows into long format."""
    rows = []
    case3 = df[df["case"] == 3]
    join_ids = []
    for j in range(1, MAX_JOINS):
        if f"Join{j}-state" in df.columns:
            join_ids.append(j)
    for _, r in case3.iterrows():
        for j in join_ids:
            st = r.get(f"Join{j}-state")
            if not isinstance(st, str) or not st:
                continue
            rows.append({
                "query": r["query"], "seed": r["seed"], "join": j,
                "build": r.get(f"Join{j}-build"),
                "build_rows": r.get(f"Join{j}-build_rows"),
                "state": st,
                "total_probes": r.get(f"Join{j}-total_probes"),
                "inserts": r.get(f"Join{j}-inserts"),
                "freeze_reason": r.get(f"Join{j}-freeze_reason"),
                "abandon_reason": r.get(f"Join{j}-abandon_reason"),
            })
    return pd.DataFrame(rows)


def main():
    sweeps = [s for s in load_sweep_index() if s.threads == 1]
    state_rows, delta_rows = [], []
    for sw in sweeps:
        df = load_runtimes(sw)
        tel = telemetry_long(df)
        if len(tel):
            counts = tel.state.value_counts()
            n_inst = int((tel.state != "never_instantiated").sum())
            state_rows.append({
                "label": sw.label, "n_join_instances": len(tel),
                **{f"state_{k}": int(v) for k, v in counts.items()},
                "pct_instantiated": 100.0 * n_inst / len(tel),
            })
            # aggregate per (query,seed): was any THC instantiated? total probes
            inst = (tel.assign(is_inst=tel.state != "never_instantiated")
                    .groupby(["query", "seed"])
                    .agg(any_inst=("is_inst", "any"),
                         thc_probes=("total_probes", "sum"),
                         thc_inserts=("inserts", "sum"),
                         max_build=("build_rows", "max")))
        else:
            inst = None

        # case3 vs case2 per (query,seed), failures dropped (both must succeed)
        ok = df[~(df.timeout | df.oom)]
        piv = ok.pivot_table(index=["query", "seed"], columns="case",
                             values="runtime_seconds")
        if 2 not in piv.columns or 3 not in piv.columns:
            continue
        piv = piv.dropna(subset=[2, 3])
        d = pd.DataFrame({
            "ratio_c3_c2": piv[3] / piv[2].clip(lower=0.005),
            "c2": piv[2], "c3": piv[3],
        })
        if inst is not None:
            d = d.join(inst, how="left")
        d["label"] = sw.label
        delta_rows.append(d.reset_index())

    states = pd.DataFrame(state_rows)
    deltas = pd.concat(delta_rows, ignore_index=True)
    states.to_csv(os.path.join(OUT_DIR, "thc_states.csv"), index=False)
    deltas.to_csv(os.path.join(OUT_DIR, "thc_deltas.csv"), index=False)

    print("## THC instantiation rates (case 3, t1 sweeps)")
    for _, r in states.iterrows():
        parts = " ".join(f"{c.replace('state_','')}={int(r[c])}"
                         for c in states.columns if c.startswith("state_") and pd.notna(r[c]))
        print(f"{r.label:16s} joins={int(r.n_join_instances):6d} "
              f"inst={r.pct_instantiated:5.1f}%  {parts}")

    print("\n## case3/case2 runtime ratio by THC instantiation (geomean)")
    deltas["any_inst"] = deltas["any_inst"].fillna(False)
    for label in deltas.label.unique():
        s = deltas[deltas.label == label]
        for flag in (False, True):
            ss = s[s.any_inst == flag]
            if len(ss) < 3:
                continue
            g = float(np.exp(np.mean(np.log(ss.ratio_c3_c2.clip(lower=1e-3)))))
            print(f"{label:16s} inst={flag!s:5s} n={len(ss):5d} geomean c3/c2={g:.3f}")

    print("\n## Biggest THC effects per (label,query), median over seeds of c3/c2")
    med = (deltas.groupby(["label", "query"])
           .agg(med_ratio=("ratio_c3_c2", "median"), n=("ratio_c3_c2", "size"),
                med_c2=("c2", "median"), any_inst=("any_inst", "any"))
           .reset_index())
    med = med[med.med_c2 >= 0.5]  # ignore sub-0.5s queries (quantization noise)
    print("--- THC helps most (ratio < 1):")
    print(med.nsmallest(12, "med_ratio").to_string(index=False))
    print("--- THC hurts most (ratio > 1):")
    print(med.nlargest(12, "med_ratio").to_string(index=False))


if __name__ == "__main__":
    main()
