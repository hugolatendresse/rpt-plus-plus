"""Phase 1: full-corpus aggregate analysis.

For every sweep (benchmark x SF x thread count) computes:
  - per-case win rates, by median-over-seeds and by min-over-seeds
  - per-(query,seed) same-plan win rates (all cases share the seed's join order)
  - geomean speedup vs case 1 (timeouts capped at the sweep's timeout value;
    conservative: understates the winner's advantage)
  - workload sums of per-query medians
  - robustness metrics over seeds: p90/p10 ratio, max/min ratio, timeout counts
  - orderings taxonomy: ranking pattern of the four cases by median runtime

Outputs CSV tables + figures under results/analysis/phase1/ and prints a
markdown summary to stdout.

Conventions:
  - Timeout/OOM runs are +inf for winner determination (a failure is worse than
    any measured time). A (query,case) median is inf if >=10/20 seeds failed.
  - For ratio metrics (geomean, robustness) failures are capped at the timeout
    limit (300 s TPC/JOB, 60 s appian) to keep ratios finite; noted in the paper.
  - "Win" = strictly smallest value; exact ties (e.g. all-inf) count for nobody.
"""

import os
import sys

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

sys.path.insert(0, os.path.dirname(__file__))
from corpus import load_sweep_index, load_runtimes, CASE_NAMES, REPO_ROOT  # noqa: E402

OUT_DIR = os.path.join(REPO_ROOT, "results", "analysis", "phase1")
os.makedirs(OUT_DIR, exist_ok=True)

TIMEOUT_CAP = {"appian": 60.0}  # default 300 for job/tpch/tpcds
EPS = 0.005  # CSVs have 2-decimal precision; avoid /0 on 0.00s queries


def cap_for(benchmark: str) -> float:
    return TIMEOUT_CAP.get(benchmark, 300.0)


def strict_winner(row: pd.Series):
    """Index of strictly smallest value, or None on ties/all-NaN."""
    m = row.min()
    if not np.isfinite(m):
        return None
    winners = row[row == m]
    return winners.index[0] if len(winners) == 1 else None


def analyze_sweep(sw):
    df = load_runtimes(sw)
    cap = cap_for(sw.benchmark)
    d = df[["query", "case", "seed", "runtime_seconds", "timeout", "oom"]].copy()
    failed = d["timeout"] | d["oom"]
    d["rt_inf"] = d["runtime_seconds"].where(~failed, np.inf)
    d["rt_cap"] = d["runtime_seconds"].where(~failed, cap).clip(lower=EPS)

    # --- per-(query,case) stats over seeds ---
    g = d.groupby(["query", "case"])
    stats = pd.DataFrame({
        "median": g["rt_inf"].median(),
        "min": g["rt_inf"].min(),
        "median_cap": g["rt_cap"].median(),
        "min_cap": g["rt_cap"].min(),
        "max_cap": g["rt_cap"].max(),
        "p10_cap": g["rt_cap"].quantile(0.10),
        "p90_cap": g["rt_cap"].quantile(0.90),
        "n_fail": g["rt_inf"].apply(lambda s: int(np.isinf(s).sum())),
    }).reset_index()

    med_piv = stats.pivot(index="query", columns="case", values="median")
    min_piv = stats.pivot(index="query", columns="case", values="min")
    medcap_piv = stats.pivot(index="query", columns="case", values="median_cap")
    mincap_piv = stats.pivot(index="query", columns="case", values="min_cap")

    # --- win rates ---
    med_winner = med_piv.apply(strict_winner, axis=1)
    min_winner = min_piv.apply(strict_winner, axis=1)
    win = {
        "label": sw.label, "benchmark": sw.benchmark, "sf": sw.scale_factor,
        "threads": sw.threads, "n_queries": med_piv.shape[0],
    }
    for c in (1, 2, 3, 4):
        win[f"med_win_c{c}"] = int((med_winner == c).sum())
        win[f"min_win_c{c}"] = int((min_winner == c).sum())
    win["med_win_none"] = int(med_winner.isna().sum())
    win["min_win_none"] = int(min_winner.isna().sum())

    # --- same-plan (per query,seed) win rates ---
    seed_piv = d.pivot_table(index=["query", "seed"], columns="case", values="rt_inf")
    seed_winner = seed_piv.apply(strict_winner, axis=1)
    for c in (1, 2, 3, 4):
        win[f"seed_win_c{c}"] = int((seed_winner == c).sum())
    win["seed_win_none"] = int(seed_winner.isna().sum())
    win["n_tuples"] = len(seed_piv)

    # --- geomeans / sums (capped) ---
    gm = {"label": sw.label, "benchmark": sw.benchmark, "sf": sw.scale_factor,
          "threads": sw.threads}
    for c in (2, 3, 4):
        gm[f"geomean_speedup_med_c{c}_vs_c1"] = float(
            np.exp(np.mean(np.log(medcap_piv[1] / medcap_piv[c]))))
        gm[f"geomean_speedup_min_c{c}_vs_c1"] = float(
            np.exp(np.mean(np.log(mincap_piv[1] / mincap_piv[c]))))
    for c in (1, 2, 3, 4):
        gm[f"sum_median_c{c}"] = float(medcap_piv[c].sum())
        gm[f"sum_min_c{c}"] = float(mincap_piv[c].sum())

    # --- robustness over seeds ---
    rob_rows = []
    for c in (1, 2, 3, 4):
        s = stats[stats["case"] == c]
        rob_rows.append({
            "label": sw.label, "benchmark": sw.benchmark, "sf": sw.scale_factor,
            "threads": sw.threads, "case": c,
            "geomean_p90_over_p10": float(np.exp(np.mean(np.log(
                (s["p90_cap"] / s["p10_cap"]).clip(lower=1e-9))))),
            "geomean_max_over_min": float(np.exp(np.mean(np.log(
                (s["max_cap"] / s["min_cap"]).clip(lower=1e-9))))),
            "n_fail_runs": int(s["n_fail"].sum()),
            "n_queries_any_fail": int((s["n_fail"] > 0).sum()),
        })

    # --- orderings taxonomy (by capped median; strict ranking with 5% tie band) ---
    tax_rows = []
    for q, row in medcap_piv.iterrows():
        order = row.sort_values().index.tolist()
        vals = row.sort_values().values
        # Collapse near-ties (within 5%) into '~' groups for a readable pattern
        pattern_parts = [str(order[0])]
        for i in range(1, 4):
            sep = "~" if vals[i] <= vals[i - 1] * 1.05 else "<"
            pattern_parts.append(sep + str(order[i]))
        tax_rows.append({
            "label": sw.label, "query": q, "pattern": "".join(pattern_parts),
            "fastest": order[0], "slowest": order[3],
            "c1_over_best": float(row[1] / vals[0]),
        })

    detail = stats.assign(label=sw.label, benchmark=sw.benchmark,
                          sf=sw.scale_factor, threads=sw.threads)
    return win, gm, rob_rows, tax_rows, detail, seed_winner


def main():
    sweeps = load_sweep_index()
    wins, gms, robs, taxs, details = [], [], [], [], []
    for sw in sweeps:
        win, gm, rob_rows, tax_rows, detail, _ = analyze_sweep(sw)
        wins.append(win); gms.append(gm); robs.extend(rob_rows)
        taxs.extend(tax_rows); details.append(detail)
        print(f"done {sw.label}")

    win_df = pd.DataFrame(wins)
    gm_df = pd.DataFrame(gms)
    rob_df = pd.DataFrame(robs)
    tax_df = pd.DataFrame(taxs)
    detail_df = pd.concat(details, ignore_index=True)

    win_df.to_csv(os.path.join(OUT_DIR, "win_rates.csv"), index=False)
    gm_df.to_csv(os.path.join(OUT_DIR, "geomeans_sums.csv"), index=False)
    rob_df.to_csv(os.path.join(OUT_DIR, "robustness.csv"), index=False)
    tax_df.to_csv(os.path.join(OUT_DIR, "orderings_taxonomy.csv"), index=False)
    detail_df.to_csv(os.path.join(OUT_DIR, "per_query_case_stats.csv"), index=False)

    # ---------------- figures ----------------
    # Win-rate heatmap (median wins, share of queries) per thread count
    for metric, fname in [("med_win", "winrate_median.png"),
                          ("seed_win", "winrate_sameplan.png")]:
        fig, axes = plt.subplots(1, 3, figsize=(16, 5), sharey=True)
        for ax, t in zip(axes, (1, 8, 64)):
            sub = win_df[win_df.threads == t].set_index("label")
            denom = (sub["n_tuples"] if metric == "seed_win" else sub["n_queries"])
            mat = pd.DataFrame({CASE_NAMES[c]: sub[f"{metric}_c{c}"] / denom
                                for c in (1, 2, 3, 4)})
            im = ax.imshow(mat.values, cmap="viridis", vmin=0, vmax=1, aspect="auto")
            ax.set_xticks(range(4), mat.columns, rotation=30)
            ax.set_yticks(range(len(mat)), [i.rsplit('_t', 1)[0] for i in mat.index])
            ax.set_title(f"{t} thread(s)")
            for i in range(mat.shape[0]):
                for j in range(mat.shape[1]):
                    ax.text(j, i, f"{mat.values[i, j]*100:.0f}%", ha="center",
                            va="center", color="w", fontsize=8)
        fig.suptitle(f"Share of {'(query,seed) same-plan tuples' if metric=='seed_win' else 'queries (by median over seeds)'} won per case")
        fig.colorbar(im, ax=axes, shrink=0.7)
        fig.savefig(os.path.join(OUT_DIR, fname), dpi=150, bbox_inches="tight")
        plt.close(fig)

    # Geomean speedup vs case1 by thread count
    fig, axes = plt.subplots(1, 3, figsize=(16, 4.5), sharey=True)
    for ax, t in zip(axes, (1, 8, 64)):
        sub = gm_df[gm_df.threads == t].set_index("label")
        x = np.arange(len(sub))
        for k, c in enumerate((2, 3, 4)):
            ax.bar(x + (k - 1) * 0.25, sub[f"geomean_speedup_med_c{c}_vs_c1"], 0.25,
                   label=CASE_NAMES[c])
        ax.axhline(1.0, color="k", lw=0.8)
        ax.set_xticks(x, [i.rsplit('_t', 1)[0] for i in sub.index], rotation=45, ha="right")
        ax.set_title(f"{t} thread(s)")
        ax.set_ylabel("geomean speedup vs DuckDB (median plans)")
    axes[0].legend()
    fig.suptitle("Geomean speedup over vanilla DuckDB, median-over-seeds, timeouts capped")
    fig.savefig(os.path.join(OUT_DIR, "geomean_speedup.png"), dpi=150, bbox_inches="tight")
    plt.close(fig)

    # ---------------- markdown summary ----------------
    print("\n\n## WIN RATES (median-over-seeds | min-over-seeds | same-plan tuples)")
    for _, r in win_df.iterrows():
        nq, nt = r.n_queries, r.n_tuples
        med = "/".join(f"{r[f'med_win_c{c}']}" for c in (1, 2, 3, 4))
        mn = "/".join(f"{r[f'min_win_c{c}']}" for c in (1, 2, 3, 4))
        sd = "/".join(f"{r[f'seed_win_c{c}']}" for c in (1, 2, 3, 4))
        print(f"{r.label:16s} nq={nq:3d}: med {med} | min {mn} | plan({nt}) {sd}")

    print("\n## GEOMEAN SPEEDUP vs case1 (median | min) and workload sums")
    for _, r in gm_df.iterrows():
        gmed = " ".join(f"c{c}={r[f'geomean_speedup_med_c{c}_vs_c1']:.3f}" for c in (2, 3, 4))
        gmin = " ".join(f"c{c}={r[f'geomean_speedup_min_c{c}_vs_c1']:.3f}" for c in (2, 3, 4))
        sums = " ".join(f"c{c}={r[f'sum_median_c{c}']:.0f}" for c in (1, 2, 3, 4))
        print(f"{r.label:16s} med[{gmed}] min[{gmin}] summed[{sums}]")

    print("\n## ROBUSTNESS (geomean p90/p10 across seeds; failures)")
    for label in rob_df.label.unique():
        sub = rob_df[rob_df.label == label]
        s = " ".join(f"c{int(r['case'])}={r['geomean_p90_over_p10']:.2f}"
                     f"(f{int(r['n_fail_runs'])})" for _, r in sub.iterrows())
        print(f"{label:16s} {s}")

    print("\n## TOP ORDERING PATTERNS per thread count (share of query*sweep)")
    tax_df["threads"] = tax_df.label.str.rsplit("_t", n=1).str[1].astype(int)
    for t in (1, 8, 64):
        sub = tax_df[tax_df.threads == t]
        top = sub.pattern.value_counts().head(12)
        print(f"-- {t} threads (n={len(sub)}):")
        for pat, n in top.items():
            print(f"   {pat:12s} {n:4d} ({n/len(sub)*100:.1f}%)")


if __name__ == "__main__":
    main()
