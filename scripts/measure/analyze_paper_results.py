#!/usr/bin/env python3
"""
Reproduce the quantitative analysis used by paper_findings.md.

The paper benchmark is unusual in one important respect: seeds are deterministic
join/transfer orders, not repeated executions of one plan.  Consequently this
script keeps every comparison paired on (dataset, query, seed), reports
descriptive plan-distribution statistics, and never treats the 20 seeds as
ordinary execution-noise replicates.

Raw benchmark artifacts are read-only.  Compact CSV/JSON summaries and figures
are written below paper_analysis/ (or --output-dir).  Timeout and OOM sentinels
are statuses, never numeric runtimes.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
import re
import statistics
import sys
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence


TIMEOUT_SENTINEL = 9_999_999.0
OOM_SENTINEL = 8_888_888.0
EXPECTED_QUERIES = {"appian": 8, "job": 113, "tpch": 22, "tpcds": 99}
CASE_NAMES = {1: "Baseline", 2: "Forward-only", 3: "THC", 4: "RPT+"}

# Speedup is always base_runtime / treatment_runtime, so values above one favor
# the treatment named by each comparison.
COMPARISONS = (
	("forward_vs_baseline", 1, 2),
	("thc_vs_baseline", 1, 3),
	("full_vs_baseline", 1, 4),
	("thc_vs_forward", 2, 3),
	("backward_vs_forward", 2, 4),
	("full_vs_thc", 3, 4),
)
BF_MULTIPLIERS = (1.0, 1.5, 2.0, 4.0, 8.0)
CASE_STUDIES = (
	("tpcds_sf100_t8", "Q95", "Backward pass eliminates a cardinality explosion"),
	("tpcds_sf100_t8", "Q40", "Backward pass reduces hash-state memory"),
	("tpcds_sf100_t8", "Q83", "Forward pass is sufficient"),
	("job_t8", "6f", "THC adds work without pruning"),
	("tpch_sf100_t8", "Q13", "Baseline-friendly plan with no useful BF"),
)


@dataclass(frozen=True)
class Dataset:
	dataset_id: str
	benchmark: str
	suite: str
	scale_factor: int | None
	threads: int
	commit: str
	runtime_csv: Path
	median_csv: Path
	profiling_dir: Path
	boxplot_dir: Path
	profile_prefix: str
	timeout_seconds: int


@dataclass(frozen=True)
class RuntimeValue:
	runtime: float | None
	status: str


def parse_args() -> argparse.Namespace:
	repo_root = Path(__file__).resolve().parents[2]
	parser = argparse.ArgumentParser(description=__doc__)
	parser.add_argument("--summary", type=Path, default=repo_root / "results_summary.csv")
	parser.add_argument("--results-root", type=Path, default=repo_root.parent / "results-spy" / "results")
	parser.add_argument("--output-dir", type=Path, default=repo_root / "paper_analysis")
	parser.add_argument("--skip-profiles", action="store_true", help="Only analyze runtime CSVs")
	parser.add_argument("--profile-workers", type=int, default=min(8, os.cpu_count() or 1))
	parser.add_argument("--no-figures", action="store_true")
	return parser.parse_args()


def benchmark_parts(name: str) -> tuple[str, int | None]:
	lower = name.lower()
	if lower == "appian":
		return "appian", None
	if lower == "job":
		return "job", None
	match = re.fullmatch(r"(tpch|tpcds)\s+sf(\d+)", lower)
	if not match:
		raise ValueError(f"Unrecognized benchmark label: {name!r}")
	return match.group(1), int(match.group(2))


def dataset_id(suite: str, scale_factor: int | None, threads: int) -> str:
	sf = f"_sf{scale_factor}" if scale_factor is not None else ""
	return f"{suite}{sf}_t{threads}"


def resolve_csv(base: Path, value: str) -> Path:
	path = base / value
	return path if path.suffix == ".csv" else path.with_suffix(".csv")


def load_manifest(summary_path: Path, results_root: Path) -> list[Dataset]:
	datasets: list[Dataset] = []
	with summary_path.open(newline="") as handle:
		for row in csv.DictReader(handle):
			suite, sf = benchmark_parts(row["Benchmark"])
			threads = int(row["Thread Count"])
			subdir = "appian" if suite == "appian" else "job" if suite == "job" else "tpch"
			base = results_root / subdir
			runtime_csv = resolve_csv(base, row["Runtimes"])
			median_csv = runtime_csv.with_name(f"{runtime_csv.stem}_median.csv")
			datasets.append(
				Dataset(
					dataset_id=dataset_id(suite, sf, threads),
					benchmark=row["Benchmark"],
					suite=suite,
					scale_factor=sf,
					threads=threads,
					commit=row["commit"],
					runtime_csv=runtime_csv,
					median_csv=median_csv,
					profiling_dir=base / row["Profiling"],
					boxplot_dir=base / row["Boxplots"],
					profile_prefix=suite,
					timeout_seconds=60 if suite == "appian" else 300,
				)
			)
	if len(datasets) != 30:
		raise ValueError(f"Expected 30 canonical datasets, found {len(datasets)}")
	if len({d.dataset_id for d in datasets}) != len(datasets):
		raise ValueError("Dataset identifiers are not unique; check results_summary.csv")
	return datasets


def analyze_existing_ash(results_root: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
	"""Inventory supplemental ASH-gen CSVs without pretending their settings are known.

	The ASH driver did not embed generator or THC settings in these files.  We
	therefore summarize each timestamp independently and mark provenance as
	unverified; combining timestamps would create a false controlled sweep.
	"""
	ash_dir = results_root / "ash_datagen"
	run_rows: list[dict[str, Any]] = []
	file_rows: list[dict[str, Any]] = []
	if not ash_dir.is_dir():
		return run_rows, file_rows
	for path in sorted(ash_dir.glob("ash_datagen_runtimes_*.csv")):
		if path.stem.endswith("_median"):
			continue
		with path.open(newline="") as handle:
			rows = list(csv.DictReader(handle))
		if not rows:
			continue
		by_case: dict[int, list[float]] = defaultdict(list)
		for index, row in enumerate(rows, 1):
			value = float(row["runtime_seconds"])
			case = int(row["case"])
			by_case[case].append(value)
			run_rows.append(
				{
					"source_file": str(path),
					"timestamp": path.stem.removeprefix("ash_datagen_runtimes_"),
					"query": row["query"],
					"case": case,
					"seed": row.get("seed", ""),
					"row_index": index,
					"runtime_seconds": value,
					"settings_provenance": "not_embedded_unverified",
				}
			)
		medians = {case: quantile(values, 0.5) for case, values in by_case.items()}
		file_rows.append(
			{
				"source_file": str(path),
				"timestamp": path.stem.removeprefix("ash_datagen_runtimes_"),
				"row_count": len(rows),
				"cases_present": ",".join(str(case) for case in sorted(by_case)),
				**{f"case{case}_run_count": len(by_case.get(case, [])) for case in range(1, 5)},
				**{f"case{case}_median_runtime": medians.get(case) for case in range(1, 5)},
				"thc_speedup_vs_forward_from_file_medians": safe_ratio(medians.get(2), medians.get(3)),
				"full_speedup_vs_forward_from_file_medians": safe_ratio(medians.get(2), medians.get(4)),
				"full_speedup_vs_thc_from_file_medians": safe_ratio(medians.get(3), medians.get(4)),
				"settings_provenance": "not_embedded_unverified",
			}
		)
	return run_rows, file_rows


def runtime_value(raw: str) -> RuntimeValue:
	value = float(raw)
	if value == TIMEOUT_SENTINEL:
		return RuntimeValue(None, "timeout")
	if value == OOM_SENTINEL:
		return RuntimeValue(None, "oom")
	if not math.isfinite(value) or value <= 0:
		raise ValueError(f"Invalid successful runtime: {raw!r}")
	return RuntimeValue(value, "success")


def quantile(values: Sequence[float], q: float) -> float | None:
	if not values:
		return None
	ordered = sorted(values)
	if len(ordered) == 1:
		return ordered[0]
	position = (len(ordered) - 1) * q
	lower = math.floor(position)
	upper = math.ceil(position)
	if lower == upper:
		return ordered[lower]
	fraction = position - lower
	return ordered[lower] * (1 - fraction) + ordered[upper] * fraction


def geometric_mean(values: Iterable[float]) -> float | None:
	positive = [value for value in values if value > 0 and math.isfinite(value)]
	if not positive:
		return None
	return math.exp(sum(math.log(value) for value in positive) / len(positive))


def safe_ratio(numerator: float | None, denominator: float | None) -> float | None:
	if numerator is None or denominator is None or denominator <= 0:
		return None
	return numerator / denominator


def cv(values: Sequence[float]) -> float | None:
	if len(values) < 2:
		return None
	mean = statistics.fmean(values)
	return statistics.pstdev(values) / mean if mean else None


def fmt(value: Any) -> str:
	if value is None:
		return ""
	if isinstance(value, float):
		if not math.isfinite(value):
			return ""
		return f"{value:.9g}"
	return str(value)


def write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fields: Sequence[str] | None = None) -> None:
	path.parent.mkdir(parents=True, exist_ok=True)
	if fields is None:
		fields = list(rows[0].keys()) if rows else []
	with path.open("w", newline="") as handle:
		writer = csv.DictWriter(handle, fieldnames=fields)
		writer.writeheader()
		for row in rows:
			writer.writerow({field: fmt(row.get(field)) for field in fields})


def profile_file_count(directory: Path) -> int:
	if not directory.is_dir():
		return 0
	return sum(1 for path in directory.iterdir() if path.suffix == ".json")


RuntimeData = dict[str, dict[str, dict[int, dict[int, RuntimeValue]]]]


def load_runtimes(datasets: Sequence[Dataset]) -> tuple[RuntimeData, list[dict[str, Any]]]:
	data: RuntimeData = {}
	manifest_rows: list[dict[str, Any]] = []
	for ds in datasets:
		if not ds.runtime_csv.is_file():
			raise FileNotFoundError(ds.runtime_csv)
		if not ds.median_csv.is_file():
			raise FileNotFoundError(ds.median_csv)
		if not ds.profiling_dir.is_dir():
			raise FileNotFoundError(ds.profiling_dir)
		if not ds.boxplot_dir.is_dir():
			raise FileNotFoundError(ds.boxplot_dir)

		queries: dict[str, dict[int, dict[int, RuntimeValue]]] = defaultdict(lambda: defaultdict(dict))
		statuses: Counter[str] = Counter()
		seen: set[tuple[str, int, int, int]] = set()
		run_indices: set[int] = set()
		with ds.runtime_csv.open(newline="") as handle:
			reader = csv.reader(handle)
			header = next(reader)
			index = {name: header.index(name) for name in ("query", "case", "seed", "run_idx", "runtime_seconds")}
			for row in reader:
				query = row[index["query"]]
				case = int(row[index["case"]])
				seed = int(row[index["seed"]])
				run_idx = int(row[index["run_idx"]])
				key = (query, case, seed, run_idx)
				if key in seen:
					raise ValueError(f"Duplicate runtime key in {ds.runtime_csv}: {key}")
				seen.add(key)
				run_indices.add(run_idx)
				value = runtime_value(row[index["runtime_seconds"]])
				statuses[value.status] += 1
				# Canonical runs have run_idx=1. Keeping the assertion makes an
				# accidental future mixture of plan seeds and repetitions loud.
				if run_idx != 1:
					raise ValueError(f"Unexpected run_idx={run_idx} in canonical dataset {ds.dataset_id}")
				queries[query][seed][case] = value

		query_count = len(queries)
		expected_rows = EXPECTED_QUERIES[ds.suite] * 4 * 20
		for query, by_seed in queries.items():
			if set(by_seed) != set(range(20)):
				raise ValueError(f"{ds.dataset_id}/{query} does not contain seeds 0..19")
			for seed, by_case in by_seed.items():
				if set(by_case) != {1, 2, 3, 4}:
					raise ValueError(f"{ds.dataset_id}/{query}/seed{seed} lacks four cases")
		if len(seen) != expected_rows:
			raise ValueError(f"{ds.dataset_id}: expected {expected_rows} rows, found {len(seen)}")
		if query_count != EXPECTED_QUERIES[ds.suite]:
			raise ValueError(f"{ds.dataset_id}: unexpected query count {query_count}")

		data[ds.dataset_id] = {query: dict(by_seed) for query, by_seed in queries.items()}
		manifest_rows.append(
			{
				**{key: str(value) if isinstance(value, Path) else value for key, value in asdict(ds).items()},
				"row_count": len(seen),
				"query_count": query_count,
				"seed_count": 20,
				"success_count": statuses["success"],
				"timeout_count": statuses["timeout"],
				"oom_count": statuses["oom"],
				"profile_json_count": profile_file_count(ds.profiling_dir),
			}
		)
	return data, manifest_rows


def capped_runtime(value: RuntimeValue, cap: int) -> float:
	# A timeout is known only to be >= cap; cap is therefore an explicitly
	# optimistic lower bound. OOM is assigned the same cap only for sensitivity
	# summaries and is always counted separately.
	return value.runtime if value.status == "success" and value.runtime is not None else float(cap)


def expected_best_of_k(values: Sequence[float], k: int) -> float:
	"""Expected minimum of k draws without replacement from the finite plan set."""
	ordered = sorted(values)
	n = len(ordered)
	denominator = math.comb(n, k)
	total = 0.0
	for i, value in enumerate(ordered):
		remaining_after = n - i - 1
		if remaining_after >= k - 1:
			total += value * math.comb(remaining_after, k - 1) / denominator
	return total


def analyze_runtimes(
	datasets: Sequence[Dataset], data: RuntimeData
) -> tuple[
	list[dict[str, Any]],
	list[dict[str, Any]],
	list[dict[str, Any]],
	list[dict[str, Any]],
	list[dict[str, Any]],
	list[dict[str, Any]],
	dict[str, Any],
]:
	case_rows: list[dict[str, Any]] = []
	pair_rows: list[dict[str, Any]] = []
	query_pair_rows: list[dict[str, Any]] = []
	corpus_rows: list[dict[str, Any]] = []
	best_seed_rows: list[dict[str, Any]] = []
	winner_rows: list[dict[str, Any]] = []
	best_k_rows: list[dict[str, Any]] = []
	ds_by_id = {ds.dataset_id: ds for ds in datasets}

	for ds in datasets:
		for query in sorted(data[ds.dataset_id], key=query_sort_key):
			by_seed = data[ds.dataset_id][query]
			case_stats: dict[int, dict[str, Any]] = {}
			for case in range(1, 5):
				values = [by_seed[seed][case] for seed in sorted(by_seed)]
				successes = [value.runtime for value in values if value.runtime is not None]
				successes_float = [float(value) for value in successes]
				oracle = min(successes_float) if successes_float else None
				regrets = [value / oracle - 1 for value in successes_float] if oracle else []
				capped = [capped_runtime(value, ds.timeout_seconds) for value in values]
				stats = {
					"dataset_id": ds.dataset_id,
					"benchmark": ds.benchmark,
					"suite": ds.suite,
					"scale_factor": ds.scale_factor,
					"threads": ds.threads,
					"query": query,
					"case": case,
					"case_name": CASE_NAMES[case],
					"success_count": len(successes_float),
					"timeout_count": sum(value.status == "timeout" for value in values),
					"oom_count": sum(value.status == "oom" for value in values),
					"completion_rate": len(successes_float) / len(values),
					"min_runtime": oracle,
					"median_runtime_success": quantile(successes_float, 0.5),
					"p10_runtime_success": quantile(successes_float, 0.1),
					"p90_runtime_success": quantile(successes_float, 0.9),
					"iqr_runtime_success": (
						(quantile(successes_float, 0.75) or 0) - (quantile(successes_float, 0.25) or 0)
						if successes_float
						else None
					),
					"cv_runtime_success": cv(successes_float),
					"p90_p50": safe_ratio(quantile(successes_float, 0.9), quantile(successes_float, 0.5)),
					"median_capped_runtime": quantile(capped, 0.5),
					"median_regret": quantile(regrets, 0.5),
					"p90_regret": quantile(regrets, 0.9),
					"within_10pct_oracle_rate": (
						sum(value <= oracle * 1.10 for value in successes_float) / len(values) if oracle else None
					),
					"bad_plan_rate_over_2x": (
						(sum(value > oracle * 2 for value in successes_float) + len(values) - len(successes_float))
						/ len(values)
						if oracle
						else None
					),
					"best_seed": (
						min(
							(seed for seed in by_seed if by_seed[seed][case].runtime is not None),
							key=lambda seed: (by_seed[seed][case].runtime, seed),
						)
						if successes_float
						else None
					),
				}
				case_stats[case] = stats
				case_rows.append(stats)
				for k in (1, 2, 3, 5, 10, 20):
					best_k_rows.append(
						{
							"dataset_id": ds.dataset_id,
							"query": query,
							"case": case,
							"k": k,
							"expected_best_capped_runtime": expected_best_of_k(capped, k),
							"sampled_oracle_capped_runtime": min(capped),
						}
					)

			eligible_winner = all(case_stats[case]["success_count"] == 20 for case in range(1, 5))
			medians = {case: case_stats[case]["median_runtime_success"] for case in range(1, 5)}
			if eligible_winner:
				best_median = min(float(value) for value in medians.values() if value is not None)
				winners = [case for case, value in medians.items() if value == best_median]
				if len(winners) == 1:
					winner = str(winners[0])
				else:
					winner = "tie:" + ",".join(str(case) for case in winners)
				monotone = bool(medians[1] > medians[2] > medians[3] > medians[4])
				forward_speedup = float(medians[1]) / float(medians[2])
				thc_marginal_speedup = float(medians[2]) / float(medians[3])
				backward_speedup = float(medians[2]) / float(medians[4])
				full_speedup = float(medians[1]) / float(medians[4])
				forward_sufficient = forward_speedup >= 1.10 and 0.95 <= backward_speedup <= 1.05
				backward_essential = backward_speedup >= 1.10
				thc_regression = thc_marginal_speedup <= 1 / 1.10
				forward_regression = forward_speedup <= 1 / 1.10
				full_regression = full_speedup <= 1 / 1.10
				all_treatments_regress = all(float(medians[case]) >= float(medians[1]) * 1.10 for case in (2, 3, 4))
				thc_approximates_full = (
					backward_essential and abs(float(medians[3]) / float(medians[4]) - 1.0) <= 0.05
				)
			else:
				winner = "censored"
				monotone = False
				forward_sufficient = backward_essential = thc_regression = False
				forward_regression = full_regression = all_treatments_regress = False
				thc_approximates_full = False
			winner_rows.append(
				{
					"dataset_id": ds.dataset_id,
					"benchmark": ds.benchmark,
					"threads": ds.threads,
					"query": query,
					"eligible_all_20_seeds_successful": int(eligible_winner),
					"median_winner": winner,
					"strict_monotone_1_gt_2_gt_3_gt_4": int(monotone),
					"forward_sufficient_10pct": int(forward_sufficient),
					"backward_essential_10pct": int(backward_essential),
					"thc_regression_10pct": int(thc_regression),
					"forward_regression_10pct": int(forward_regression),
					"full_regression_10pct": int(full_regression),
					"all_treatments_regress_10pct": int(all_treatments_regress),
					"thc_approximates_full_when_backward_essential": int(thc_approximates_full),
					**{f"case{case}_median": medians[case] for case in range(1, 5)},
				}
			)

			# Anchor treatment comparisons on the best sampled baseline plan.
			baseline_success_seeds = [seed for seed in by_seed if by_seed[seed][1].runtime is not None]
			if baseline_success_seeds:
				anchor = min(baseline_success_seeds, key=lambda seed: (by_seed[seed][1].runtime, seed))
				anchor_values = {case: by_seed[anchor][case] for case in range(1, 5)}
				best_seed_rows.append(
					{
						"dataset_id": ds.dataset_id,
						"benchmark": ds.benchmark,
						"threads": ds.threads,
						"query": query,
						"baseline_best_seed": anchor,
						**{
							f"case{case}_runtime_on_baseline_best_seed": anchor_values[case].runtime
							for case in range(1, 5)
						},
						**{f"case{case}_status_on_baseline_best_seed": anchor_values[case].status for case in range(1, 5)},
						**{
							f"case{case}_speedup_vs_baseline_on_anchor": safe_ratio(
								anchor_values[1].runtime, anchor_values[case].runtime
							)
							for case in (2, 3, 4)
						},
						**{f"case{case}_own_best_runtime": case_stats[case]["min_runtime"] for case in range(1, 5)},
						**{f"case{case}_own_best_seed": case_stats[case]["best_seed"] for case in range(1, 5)},
					}
				)

			for comparison, base_case, treatment_case in COMPARISONS:
				ratios: list[float] = []
				status_counts: Counter[str] = Counter()
				for seed in sorted(by_seed):
					base = by_seed[seed][base_case]
					treatment = by_seed[seed][treatment_case]
					if base.runtime is not None and treatment.runtime is not None:
						ratio = base.runtime / treatment.runtime
						ratios.append(ratio)
						status = "paired_success"
					elif base.runtime is None and treatment.runtime is not None:
						ratio = None
						status = "treatment_completion_win"
					elif base.runtime is not None and treatment.runtime is None:
						ratio = None
						status = "treatment_completion_loss"
					else:
						ratio = None
						status = "both_failed"
					status_counts[status] += 1
					pair_rows.append(
						{
							"dataset_id": ds.dataset_id,
							"query": query,
							"seed": seed,
							"comparison": comparison,
							"base_case": base_case,
							"treatment_case": treatment_case,
							"base_runtime": base.runtime,
							"treatment_runtime": treatment.runtime,
							"speedup": ratio,
							"status": status,
						}
					)
				query_pair_rows.append(
					{
						"dataset_id": ds.dataset_id,
						"benchmark": ds.benchmark,
						"suite": ds.suite,
						"scale_factor": ds.scale_factor,
						"threads": ds.threads,
						"query": query,
						"comparison": comparison,
						"base_case": base_case,
						"treatment_case": treatment_case,
						"paired_success_count": len(ratios),
						"treatment_completion_win_count": status_counts["treatment_completion_win"],
						"treatment_completion_loss_count": status_counts["treatment_completion_loss"],
						"both_failed_count": status_counts["both_failed"],
						"median_speedup": quantile(ratios, 0.5),
						"geomean_speedup": geometric_mean(ratios),
						"p10_speedup": quantile(ratios, 0.1),
						"p90_speedup": quantile(ratios, 0.9),
						"treatment_win_rate_paired": (
							sum(ratio > 1.0 for ratio in ratios) / len(ratios) if ratios else None
						),
						"tie_rate_paired": sum(ratio == 1.0 for ratio in ratios) / len(ratios) if ratios else None,
						"helped_at_least_10pct_rate": (
							sum(ratio >= 1.10 for ratio in ratios) / len(ratios) if ratios else None
						),
						"hurt_at_least_10pct_rate": (
							sum(ratio <= 1 / 1.10 for ratio in ratios) / len(ratios) if ratios else None
						),
						"at_least_2x_speedup_rate": (
							sum(ratio >= 2.0 for ratio in ratios) / len(ratios) if ratios else None
						),
					}
				)

	# Corpus summaries weight queries equally by geometrically averaging each
	# query's median paired speedup. They never weight a long query more merely
	# because its runtime is large.
	by_corpus_comparison: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
	for row in query_pair_rows:
		by_corpus_comparison[(str(row["dataset_id"]), str(row["comparison"]))].append(row)
	for (ds_id, comparison), rows in sorted(by_corpus_comparison.items()):
		ds = ds_by_id[ds_id]
		medians = [float(row["median_speedup"]) for row in rows if row["median_speedup"] is not None]
		corpus_rows.append(
			{
				"dataset_id": ds_id,
				"benchmark": ds.benchmark,
				"suite": ds.suite,
				"scale_factor": ds.scale_factor,
				"threads": ds.threads,
				"comparison": comparison,
				"query_count": len(rows),
				"queries_with_paired_success": len(medians),
				"query_geomean_of_median_speedup": geometric_mean(medians),
				"query_median_of_median_speedup": quantile(medians, 0.5),
				"queries_treatment_faster": sum(value > 1 for value in medians),
				"queries_treatment_slower": sum(value < 1 for value in medians),
				"queries_tied": sum(value == 1 for value in medians),
				"queries_at_least_10pct_faster": sum(value >= 1.1 for value in medians),
				"queries_at_least_2x_faster": sum(value >= 2 for value in medians),
			}
		)

	summary = {
		"canonical_dataset_count": len(datasets),
		"runtime_observation_count": sum(
			len(by_seed) * 4 for ds_data in data.values() for by_seed in ds_data.values()
		),
		"timeout_count": sum(
			value.status == "timeout"
			for ds_data in data.values()
			for by_seed in ds_data.values()
			for by_case in by_seed.values()
			for value in by_case.values()
		),
		"oom_count": sum(
			value.status == "oom"
			for ds_data in data.values()
			for by_seed in ds_data.values()
			for by_case in by_seed.values()
			for value in by_case.values()
		),
	}
	return case_rows, pair_rows, query_pair_rows, corpus_rows, best_seed_rows, winner_rows, best_k_rows, summary


def query_sort_key(query: str) -> tuple[int, str]:
	match = re.match(r"^[Qq]?(\d+)(.*)$", query)
	if not match:
		return sys.maxsize, query
	return int(match.group(1)), match.group(2)


def query_token(query: str) -> str:
	value = query.strip()
	if value.startswith(("Q", "q")):
		value = value[1:]
	if value.isdigit():
		return str(int(value))
	return value


def profile_path(ds: Dataset, query: str, case: int, seed: int) -> Path:
	token = query_token(query)
	return ds.profiling_dir / f"{ds.profile_prefix}_q{token}_case{case}_seed{seed}_run1.json"


def numeric(value: Any) -> float:
	if value in (None, ""):
		return 0.0
	if isinstance(value, (int, float)):
		return float(value)
	match = re.search(r"[-+]?(?:\d+(?:\.\d*)?|\.\d+)", str(value).replace(",", ""))
	return float(match.group(0)) if match else 0.0


PROFILE_FIELDS = (
	"dataset_id",
	"query",
	"case",
	"seed",
	"runtime_seconds",
	"latency",
	"cpu_time",
	"blocked_thread_time",
	"peak_buffer_memory",
	"peak_temp_dir_size",
	"cumulative_cardinality",
	"cumulative_rows_scanned",
	"operator_count",
	"operator_timing_sum",
	"create_bf_count",
	"use_bf_count",
	"hash_join_count",
	"scan_count",
	"create_bf_timing",
	"use_bf_timing",
	"hash_join_timing",
	"scan_timing",
	"bf_timing",
	"bf_cpu_fraction",
	"bf_operator_cardinality_sum",
	"normalized_cumulative_cardinality",
	"join_build_rows_sum",
	"thc_instantiated_count",
	"thc_total_probes",
	"thc_total_inserts",
	"thc_never_instantiated_count",
	"thc_active_count",
	"thc_frozen_count",
	"thc_abandoned_count",
	"thc_other_state_count",
	"thc_full_freeze_count",
	"thc_high_miss_abandon_count",
	"join_signature",
	"profile_path",
	"parse_error",
)


def extract_profile(task: tuple[str, str, int, int, float, str]) -> dict[str, Any]:
	ds_id, query, case, seed, runtime, path_string = task
	path = Path(path_string)
	row: dict[str, Any] = {field: None for field in PROFILE_FIELDS}
	row.update(
		{
			"dataset_id": ds_id,
			"query": query,
			"case": case,
			"seed": seed,
			"runtime_seconds": runtime,
			"profile_path": path_string,
			"parse_error": "",
		}
	)
	try:
		with path.open() as handle:
			root = json.load(handle)
	except Exception as exc:  # A corrupt profile must be recorded, not hide a run.
		row["parse_error"] = f"{type(exc).__name__}: {exc}"
		return row

	for field, output in (
		("latency", "latency"),
		("cpu_time", "cpu_time"),
		("blocked_thread_time", "blocked_thread_time"),
		("system_peak_buffer_memory", "peak_buffer_memory"),
		("system_peak_temp_dir_size", "peak_temp_dir_size"),
		("cumulative_cardinality", "cumulative_cardinality"),
		("cumulative_rows_scanned", "cumulative_rows_scanned"),
	):
		row[output] = numeric(root.get(field))

	timing = Counter()
	counts = Counter()
	thc_states = Counter()
	thc_reasons = Counter()
	signature: list[tuple[str, str, str, str, str]] = []
	build_rows = probes = inserts = instantiated = 0.0
	bf_operator_cardinality = 0.0
	operator_timing_sum = 0.0
	stack = list(reversed(root.get("children") or []))
	while stack:
		node = stack.pop()
		children = node.get("children") or []
		stack.extend(reversed(children))
		operator_type = str(node.get("operator_type") or node.get("operator_name") or "").strip().upper()
		operator_name = str(node.get("operator_name") or "").strip().upper()
		operator_time = numeric(node.get("operator_timing"))
		operator_timing_sum += operator_time
		counts["operator"] += 1
		if operator_type == "CREATE_BF":
			counts["create_bf"] += 1
			timing["create_bf"] += operator_time
			bf_operator_cardinality += numeric(node.get("operator_cardinality"))
		elif operator_type == "USE_BF":
			counts["use_bf"] += 1
			timing["use_bf"] += operator_time
			bf_operator_cardinality += numeric(node.get("operator_cardinality"))
		elif operator_type == "HASH_JOIN":
			counts["hash_join"] += 1
			timing["hash_join"] += operator_time
			info = node.get("extra_info") or {}
			join_type = str(info.get("Join Type", "")).strip()
			conditions = info.get("Conditions", "")
			if isinstance(conditions, list):
				conditions = "\n".join(str(item) for item in conditions)
			signature.append(
				(
					join_type,
					" ".join(str(conditions).split()),
					str(info.get("Table", "")).strip(),
					str(info.get("CTE Index", "")).strip(),
					str(info.get("Delim Index", "")).strip(),
				)
			)
			build_rows += numeric(info.get("THC Build Rows"))
			probes += numeric(info.get("THC Total Probes"))
			inserts += numeric(info.get("THC Total New Inserts"))
			instantiated += numeric(info.get("THC Instantiated"))
			state = str(info.get("THC Final State", "")).strip().lower()
			thc_states[state or "other"] += 1
			freeze_reason = str(info.get("THC Freeze Reason", "")).strip()
			abandon_reason = str(info.get("THC Abandon Reason", "")).strip()
			if freeze_reason:
				thc_reasons[f"freeze:{freeze_reason}"] += 1
			if abandon_reason:
				thc_reasons[f"abandon:{abandon_reason}"] += 1
		elif "SCAN" in operator_type or "SCAN" in operator_name:
			counts["scan"] += 1
			timing["scan"] += operator_time

	signature_json = json.dumps(signature, separators=(",", ":"), ensure_ascii=True)
	cpu_time = float(row["cpu_time"] or 0)
	bf_timing = timing["create_bf"] + timing["use_bf"]
	normalized_cumulative_cardinality = max(
		0.0, float(row["cumulative_cardinality"] or 0) - bf_operator_cardinality
	)
	row.update(
		{
			"operator_count": counts["operator"],
			"operator_timing_sum": operator_timing_sum,
			"create_bf_count": counts["create_bf"],
			"use_bf_count": counts["use_bf"],
			"hash_join_count": counts["hash_join"],
			"scan_count": counts["scan"],
			"create_bf_timing": timing["create_bf"],
			"use_bf_timing": timing["use_bf"],
			"hash_join_timing": timing["hash_join"],
			"scan_timing": timing["scan"],
			"bf_timing": bf_timing,
			"bf_cpu_fraction": bf_timing / cpu_time if cpu_time > 0 else 0.0,
			"bf_operator_cardinality_sum": bf_operator_cardinality,
			"normalized_cumulative_cardinality": normalized_cumulative_cardinality,
			"join_build_rows_sum": build_rows,
			"thc_instantiated_count": instantiated,
			"thc_total_probes": probes,
			"thc_total_inserts": inserts,
			"thc_never_instantiated_count": thc_states["never_instantiated"],
			"thc_active_count": thc_states["active"],
			"thc_frozen_count": thc_states["frozen"],
			"thc_abandoned_count": thc_states["abandoned"],
			"thc_other_state_count": sum(
				value
				for state, value in thc_states.items()
				if state not in {"never_instantiated", "active", "frozen", "abandoned"}
			),
			"thc_full_freeze_count": thc_reasons["freeze:THC-Full"],
			"thc_high_miss_abandon_count": thc_reasons["abandon:High-Miss-Rate"],
			"join_signature": hashlib.sha256(signature_json.encode()).hexdigest(),
		}
	)
	return row


def profile_tasks(datasets: Sequence[Dataset], data: RuntimeData) -> tuple[list[tuple[str, str, int, int, float, str]], list[dict[str, Any]]]:
	tasks: list[tuple[str, str, int, int, float, str]] = []
	missing: list[dict[str, Any]] = []
	for ds in datasets:
		for query in sorted(data[ds.dataset_id], key=query_sort_key):
			for seed in sorted(data[ds.dataset_id][query]):
				for case in range(1, 5):
					value = data[ds.dataset_id][query][seed][case]
					if value.runtime is None:
						continue
					path = profile_path(ds, query, case, seed)
					if not path.is_file():
						missing.append(
							{
								"dataset_id": ds.dataset_id,
								"query": query,
								"case": case,
								"seed": seed,
								"status": value.status,
								"expected_profile": str(path),
							}
						)
						continue
					tasks.append((ds.dataset_id, query, case, seed, float(value.runtime), str(path)))
	return tasks, missing


def parse_profiles(tasks: Sequence[tuple[str, str, int, int, float, str]], workers: int) -> list[dict[str, Any]]:
	print(f"Parsing {len(tasks):,} profiling JSON files with {workers} worker(s)...", flush=True)
	rows: list[dict[str, Any]] = []
	if workers <= 1:
		iterator = map(extract_profile, tasks)
		for index, row in enumerate(iterator, 1):
			rows.append(row)
			if index % 5000 == 0:
				print(f"  parsed {index:,}/{len(tasks):,}", flush=True)
		return rows
	with ProcessPoolExecutor(max_workers=workers) as executor:
		for index, row in enumerate(executor.map(extract_profile, tasks, chunksize=32), 1):
			rows.append(row)
			if index % 5000 == 0:
				print(f"  parsed {index:,}/{len(tasks):,}", flush=True)
	return rows


def pearson(xs: Sequence[float], ys: Sequence[float]) -> float | None:
	if len(xs) < 3 or len(xs) != len(ys):
		return None
	x_mean = statistics.fmean(xs)
	y_mean = statistics.fmean(ys)
	numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
	x_den = math.sqrt(sum((x - x_mean) ** 2 for x in xs))
	y_den = math.sqrt(sum((y - y_mean) ** 2 for y in ys))
	return numerator / (x_den * y_den) if x_den and y_den else None


def ranks(values: Sequence[float]) -> list[float]:
	result = [0.0] * len(values)
	ordered = sorted(range(len(values)), key=lambda index: values[index])
	start = 0
	while start < len(ordered):
		end = start + 1
		while end < len(ordered) and values[ordered[end]] == values[ordered[start]]:
			end += 1
		average_rank = (start + end - 1) / 2 + 1
		for position in range(start, end):
			result[ordered[position]] = average_rank
		start = end
	return result


def spearman(xs: Sequence[float], ys: Sequence[float]) -> float | None:
	if len(xs) < 3 or len(xs) != len(ys):
		return None
	return pearson(ranks(xs), ranks(ys))


def analyze_profiles(
	datasets: Sequence[Dataset],
	profiles: Sequence[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
	ds_by_id = {ds.dataset_id: ds for ds in datasets}
	by_key: dict[tuple[str, str, int], dict[int, dict[str, Any]]] = defaultdict(dict)
	for row in profiles:
		if row["parse_error"]:
			continue
		by_key[(str(row["dataset_id"]), str(row["query"]), int(row["seed"]))][int(row["case"])] = row

	paired_rows: list[dict[str, Any]] = []
	plan_rows: list[dict[str, Any]] = []
	distributed_query_rows: list[dict[str, Any]] = []
	distributed_corpus_rows: list[dict[str, Any]] = []
	plan_mismatches = 0
	correlation_inputs: dict[
		tuple[str, str], tuple[list[float], list[float], list[float], list[float], list[float], list[float]]
	] = defaultdict(
		lambda: ([], [], [], [], [], [])
	)

	for (ds_id, query, seed), cases in sorted(by_key.items(), key=lambda item: (item[0][0], query_sort_key(item[0][1]), item[0][2])):
		if len(cases) == 4:
			signatures = {case: row["join_signature"] for case, row in cases.items()}
			match = len(set(signatures.values())) == 1
			if not match:
				plan_mismatches += 1
			plan_rows.append(
				{
					"dataset_id": ds_id,
					"query": query,
					"seed": seed,
					"all_four_profiles_present": 1,
					"normalized_join_signature_match": int(match),
					**{f"case{case}_signature": signatures[case] for case in range(1, 5)},
				}
			)
		for comparison, base_case, treatment_case in (
			("thc_vs_forward", 2, 3),
			("backward_vs_forward", 2, 4),
			("full_vs_baseline", 1, 4),
		):
			if base_case not in cases or treatment_case not in cases:
				continue
			base = cases[base_case]
			treatment = cases[treatment_case]
			runtime_speedup = safe_ratio(float(base["runtime_seconds"]), float(treatment["runtime_seconds"]))
			profile_latency_speedup = safe_ratio(float(base["latency"]), float(treatment["latency"]))
			cpu_time_reduction = safe_ratio(float(base["cpu_time"]), float(treatment["cpu_time"]))
			cardinality_reduction = safe_ratio(
				float(base["cumulative_cardinality"]), float(treatment["cumulative_cardinality"])
			)
			normalized_cardinality_reduction = safe_ratio(
				float(base["normalized_cumulative_cardinality"]),
				float(treatment["normalized_cumulative_cardinality"]),
			)
			memory_reduction = safe_ratio(float(base["peak_buffer_memory"]), float(treatment["peak_buffer_memory"]))
			hash_timing_reduction = safe_ratio(float(base["hash_join_timing"]), float(treatment["hash_join_timing"]))
			rows_scanned_reduction = safe_ratio(
				float(base["cumulative_rows_scanned"]), float(treatment["cumulative_rows_scanned"])
			)
			paired_rows.append(
				{
					"dataset_id": ds_id,
					"query": query,
					"seed": seed,
					"comparison": comparison,
					"runtime_speedup": runtime_speedup,
					"profile_latency_speedup": profile_latency_speedup,
					"cpu_time_reduction": cpu_time_reduction,
					"cumulative_cardinality_reduction": cardinality_reduction,
					"normalized_cumulative_cardinality_reduction": normalized_cardinality_reduction,
					"peak_memory_reduction": memory_reduction,
					"hash_join_timing_reduction": hash_timing_reduction,
					"rows_scanned_reduction": rows_scanned_reduction,
					"treatment_create_bf_count": treatment["create_bf_count"],
					"treatment_use_bf_count": treatment["use_bf_count"],
					"treatment_bf_timing": treatment["bf_timing"],
					"treatment_bf_cpu_fraction": treatment["bf_cpu_fraction"],
					"treatment_thc_instantiated_count": treatment["thc_instantiated_count"],
					"treatment_thc_frozen_count": treatment["thc_frozen_count"],
					"treatment_thc_abandoned_count": treatment["thc_abandoned_count"],
					"join_signature_match": int(base["join_signature"] == treatment["join_signature"]),
				}
			)
			if (
				profile_latency_speedup
				and normalized_cardinality_reduction
				and memory_reduction
				and cpu_time_reduction
				and hash_timing_reduction
				and rows_scanned_reduction
			):
				xs, cards, memory, cpu, hash_timing, rows_scanned = correlation_inputs[(ds_id, comparison)]
				xs.append(math.log(profile_latency_speedup))
				cards.append(math.log(normalized_cardinality_reduction))
				memory.append(math.log(memory_reduction))
				cpu.append(math.log(cpu_time_reduction))
				hash_timing.append(math.log(hash_timing_reduction))
				rows_scanned.append(math.log(rows_scanned_reduction))

	# Cost sensitivity is evaluated per query from seed-level adjusted runtimes,
	# then aggregated with equal query weight.
	distributed_seed: dict[tuple[str, str, int, float, str], list[float]] = defaultdict(list)
	for (ds_id, query, seed), cases in by_key.items():
		if 1 not in cases:
			continue
		baseline_runtime = float(cases[1]["runtime_seconds"])
		for treatment_case in (2, 4):
			if treatment_case not in cases:
				continue
			treatment = cases[treatment_case]
			runtime = float(treatment["runtime_seconds"])
			fraction = float(treatment["bf_cpu_fraction"])
			bf_timing = float(treatment["bf_timing"])
			for multiplier in BF_MULTIPLIERS:
				cpu_share_adjusted = runtime * (1 + (multiplier - 1) * fraction)
				direct_add_adjusted = runtime + (multiplier - 1) * bf_timing
				distributed_seed[(ds_id, query, treatment_case, multiplier, "cpu_share")].append(
					baseline_runtime / cpu_share_adjusted
				)
				if ds_by_id[ds_id].threads == 1:
					distributed_seed[(ds_id, query, treatment_case, multiplier, "direct_add")].append(
						baseline_runtime / direct_add_adjusted
					)
	for (ds_id, query, treatment_case, multiplier, model), values in sorted(distributed_seed.items()):
		distributed_query_rows.append(
			{
				"dataset_id": ds_id,
				"query": query,
				"treatment_case": treatment_case,
				"bf_cost_multiplier": multiplier,
				"model": model,
				"seed_count": len(values),
				"median_adjusted_speedup_vs_baseline": quantile(values, 0.5),
				"geomean_adjusted_speedup_vs_baseline": geometric_mean(values),
				"seed_win_rate": sum(value > 1 for value in values) / len(values),
			}
		)
	grouped_distributed: dict[tuple[str, int, float, str], list[float]] = defaultdict(list)
	for row in distributed_query_rows:
		value = row["median_adjusted_speedup_vs_baseline"]
		if value is not None:
			grouped_distributed[
				(
					str(row["dataset_id"]),
					int(row["treatment_case"]),
					float(row["bf_cost_multiplier"]),
					str(row["model"]),
				)
			].append(float(value))
	for (ds_id, treatment_case, multiplier, model), values in sorted(grouped_distributed.items()):
		distributed_corpus_rows.append(
			{
				"dataset_id": ds_id,
				"treatment_case": treatment_case,
				"bf_cost_multiplier": multiplier,
				"model": model,
				"query_count": len(values),
				"query_geomean_adjusted_speedup_vs_baseline": geometric_mean(values),
				"queries_still_faster": sum(value > 1 for value in values),
				"queries_now_slower": sum(value < 1 for value in values),
			}
		)

	correlation_rows = []
	for (ds_id, comparison), (
		speedups,
		cardinalities,
		memory,
		cpu,
		hash_timing,
		rows_scanned,
	) in sorted(correlation_inputs.items()):
		correlation_rows.append(
			{
				"dataset_id": ds_id,
				"comparison": comparison,
				"pair_count": len(speedups),
				"aggregation": "seed_pairs",
				"normalized_cardinality_query_count": None,
				"memory_query_count": None,
				"cpu_query_count": None,
				"hash_join_query_count": None,
				"rows_scanned_query_count": None,
				"pearson_log_latency_vs_log_normalized_cardinality_reduction": pearson(speedups, cardinalities),
				"spearman_log_latency_vs_log_normalized_cardinality_reduction": spearman(speedups, cardinalities),
				"pearson_log_latency_vs_log_memory_reduction": pearson(speedups, memory),
				"spearman_log_latency_vs_log_memory_reduction": spearman(speedups, memory),
				"pearson_log_latency_vs_log_cpu_reduction": pearson(speedups, cpu),
				"spearman_log_latency_vs_log_cpu_reduction": spearman(speedups, cpu),
				"pearson_log_latency_vs_log_hash_join_reduction": pearson(speedups, hash_timing),
				"spearman_log_latency_vs_log_hash_join_reduction": spearman(speedups, hash_timing),
				"pearson_log_latency_vs_log_rows_scanned_reduction": pearson(speedups, rows_scanned),
				"spearman_log_latency_vs_log_rows_scanned_reduction": spearman(speedups, rows_scanned),
			}
		)
	query_groups: dict[tuple[str, str], dict[str, list[float]]] = defaultdict(lambda: defaultdict(list))
	for row in paired_rows:
		if row["comparison"] != "backward_vs_forward" or not str(row["dataset_id"]).endswith("_t8"):
			continue
		group = query_groups[(str(row["dataset_id"]), str(row["query"]))]
		for field in (
			"profile_latency_speedup",
			"normalized_cumulative_cardinality_reduction",
			"peak_memory_reduction",
			"cpu_time_reduction",
			"hash_join_timing_reduction",
			"rows_scanned_reduction",
		):
			value = row[field]
			if value is not None and float(value) > 0:
				group[field].append(float(value))
	query_metric_pairs: dict[str, tuple[list[float], list[float]]] = defaultdict(lambda: ([], []))
	for group in query_groups.values():
		if not group["profile_latency_speedup"]:
			continue
		log_speedup = math.log(float(quantile(group["profile_latency_speedup"], 0.5)))
		for field in (
			"normalized_cumulative_cardinality_reduction",
			"peak_memory_reduction",
			"cpu_time_reduction",
			"hash_join_timing_reduction",
			"rows_scanned_reduction",
		):
			if not group[field]:
				continue
			# A ratio base/treatment becomes fractional work reduction
			# 1-treatment/base. Pearson then captures whether larger removed
			# work tracks log speedup without the leverage of log-ratio tails.
			reductions = [1 - 1 / value for value in group[field]]
			xs, ys = query_metric_pairs[field]
			xs.append(log_speedup)
			ys.append(float(quantile(reductions, 0.5)))
	card_x, card_y = query_metric_pairs["normalized_cumulative_cardinality_reduction"]
	memory_x, memory_y = query_metric_pairs["peak_memory_reduction"]
	cpu_x, cpu_y = query_metric_pairs["cpu_time_reduction"]
	hash_x, hash_y = query_metric_pairs["hash_join_timing_reduction"]
	scan_x, scan_y = query_metric_pairs["rows_scanned_reduction"]
	correlation_rows.append(
		{
			"dataset_id": "all_t8",
			"comparison": "backward_vs_forward",
			"pair_count": len(query_groups),
			"aggregation": "per_query_median_fractional_reduction",
			"normalized_cardinality_query_count": len(card_x),
			"memory_query_count": len(memory_x),
			"cpu_query_count": len(cpu_x),
			"hash_join_query_count": len(hash_x),
			"rows_scanned_query_count": len(scan_x),
			"pearson_log_latency_vs_log_normalized_cardinality_reduction": pearson(card_x, card_y),
			"spearman_log_latency_vs_log_normalized_cardinality_reduction": spearman(card_x, card_y),
			"pearson_log_latency_vs_log_memory_reduction": pearson(memory_x, memory_y),
			"spearman_log_latency_vs_log_memory_reduction": spearman(memory_x, memory_y),
			"pearson_log_latency_vs_log_cpu_reduction": pearson(cpu_x, cpu_y),
			"spearman_log_latency_vs_log_cpu_reduction": spearman(cpu_x, cpu_y),
			"pearson_log_latency_vs_log_hash_join_reduction": pearson(hash_x, hash_y),
			"spearman_log_latency_vs_log_hash_join_reduction": spearman(hash_x, hash_y),
			"pearson_log_latency_vs_log_rows_scanned_reduction": pearson(scan_x, scan_y),
			"spearman_log_latency_vs_log_rows_scanned_reduction": spearman(scan_x, scan_y),
		}
	)
	case3_profiles = [row for row in profiles if not row["parse_error"] and int(row["case"]) == 3]
	case3_profiles_t8 = [
		row for row in case3_profiles if str(row["dataset_id"]).endswith("_t8")
	]
	summary = {
		"profiles_parsed": len(profiles),
		"profile_parse_errors": sum(bool(row["parse_error"]) for row in profiles),
		"same_seed_all_case_plan_checks": len(plan_rows),
		"normalized_plan_mismatches": plan_mismatches,
		"case3_thc_final_states": {
			"never_instantiated": sum(int(float(row["thc_never_instantiated_count"])) for row in case3_profiles),
			"active": sum(int(float(row["thc_active_count"])) for row in case3_profiles),
			"frozen": sum(int(float(row["thc_frozen_count"])) for row in case3_profiles),
			"abandoned": sum(int(float(row["thc_abandoned_count"])) for row in case3_profiles),
		},
		"case3_thc_reasons": {
			"THC-Full": sum(int(float(row["thc_full_freeze_count"])) for row in case3_profiles),
			"High-Miss-Rate": sum(int(float(row["thc_high_miss_abandon_count"])) for row in case3_profiles),
		},
		"case3_thc_final_states_t8": {
			"never_instantiated": sum(
				int(float(row["thc_never_instantiated_count"])) for row in case3_profiles_t8
			),
			"active": sum(int(float(row["thc_active_count"])) for row in case3_profiles_t8),
			"frozen": sum(int(float(row["thc_frozen_count"])) for row in case3_profiles_t8),
			"abandoned": sum(int(float(row["thc_abandoned_count"])) for row in case3_profiles_t8),
		},
		"case3_thc_reasons_t8": {
			"THC-Full": sum(int(float(row["thc_full_freeze_count"])) for row in case3_profiles_t8),
			"High-Miss-Rate": sum(
				int(float(row["thc_high_miss_abandon_count"])) for row in case3_profiles_t8
			),
		},
	}
	return paired_rows, plan_rows, correlation_rows, distributed_query_rows, distributed_corpus_rows, summary


def case_study_metrics(data: RuntimeData, profiles: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
	profile_lookup = {
		(str(row["dataset_id"]), str(row["query"]), int(row["seed"]), int(row["case"])): row
		for row in profiles
		if not row["parse_error"]
	}
	rows: list[dict[str, Any]] = []
	for ds_id, query, story in CASE_STUDIES:
		by_seed = data[ds_id][query]
		baseline_seeds = [seed for seed in by_seed if by_seed[seed][1].runtime is not None]
		if not baseline_seeds:
			continue
		anchor = min(baseline_seeds, key=lambda seed: (by_seed[seed][1].runtime, seed))
		for case in range(1, 5):
			runtime = by_seed[anchor][case]
			profile = profile_lookup.get((ds_id, query, anchor, case), {})
			rows.append(
				{
					"dataset_id": ds_id,
					"query": query,
					"story": story,
					"baseline_best_seed": anchor,
					"case": case,
					"case_name": CASE_NAMES[case],
					"runtime_seconds": runtime.runtime,
					"runtime_status": runtime.status,
					"speedup_vs_baseline": safe_ratio(by_seed[anchor][1].runtime, runtime.runtime),
					"latency": profile.get("latency"),
					"cpu_time": profile.get("cpu_time"),
					"peak_buffer_memory": profile.get("peak_buffer_memory"),
					"cumulative_cardinality": profile.get("cumulative_cardinality"),
					"normalized_cumulative_cardinality": profile.get("normalized_cumulative_cardinality"),
					"hash_join_timing": profile.get("hash_join_timing"),
					"create_bf_count": profile.get("create_bf_count"),
					"use_bf_count": profile.get("use_bf_count"),
					"bf_timing": profile.get("bf_timing"),
					"thc_instantiated_count": profile.get("thc_instantiated_count"),
					"thc_frozen_count": profile.get("thc_frozen_count"),
					"join_signature": profile.get("join_signature"),
				}
			)
	return rows


def render_case_study_figures(output_dir: Path, rows: Sequence[dict[str, Any]]) -> None:
	try:
		import matplotlib

		matplotlib.use("Agg")
		import matplotlib.pyplot as plt
	except ImportError:
		return
	figures = output_dir / "figures"
	figures.mkdir(parents=True, exist_ok=True)
	grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
	for row in rows:
		grouped[(str(row["dataset_id"]), str(row["query"]))].append(row)
	for (ds_id, query), case_rows in grouped.items():
		case_rows.sort(key=lambda row: int(row["case"]))
		if any(row["runtime_seconds"] is None for row in case_rows):
			continue
		labels = [str(row["case_name"]) for row in case_rows]
		runtimes = [float(row["runtime_seconds"]) for row in case_rows]
		baseline = case_rows[0]
		fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
		axes[0].bar(range(4), runtimes)
		axes[0].set_xticks(range(4), labels, rotation=20, ha="right")
		axes[0].set_ylabel("External wall runtime (seconds)")
		axes[0].set_title(f"Runtime on baseline-best seed {baseline['baseline_best_seed']}")
		axes[0].grid(axis="y", linestyle=":", alpha=0.35)
		for index, row in enumerate(case_rows):
			axes[0].text(
				index,
				runtimes[index],
				f"BF {row['create_bf_count']}/{row['use_bf_count']}\nTHC {row['thc_instantiated_count']}",
				ha="center",
				va="bottom",
				fontsize=7,
			)

		metrics = (
			("cumulative_cardinality", "Intermediate cardinality"),
			("peak_buffer_memory", "Peak buffer memory"),
			("hash_join_timing", "Hash-join time"),
		)
		width = 0.24
		for metric_index, (metric, label) in enumerate(metrics):
			base_value = float(baseline[metric] or 0)
			values = [
				(float(row[metric]) / base_value if base_value > 0 and float(row[metric] or 0) > 0 else math.nan)
				for row in case_rows
			]
			axes[1].bar(
				[index + (metric_index - 1) * width for index in range(4)],
				values,
				width=width,
				label=label,
			)
		axes[1].axhline(1.0, color="black", linewidth=0.8)
		axes[1].set_yscale("log")
		axes[1].set_xticks(range(4), labels, rotation=20, ha="right")
		axes[1].set_ylabel("Normalized to baseline (log scale)")
		axes[1].set_title("Executed-work and memory proxies")
		axes[1].legend(fontsize=8)
		axes[1].grid(axis="y", linestyle=":", alpha=0.35)
		fig.suptitle(f"{ds_id} {query}: {baseline['story']}")
		fig.text(
			0.5,
			0.01,
			"Source: canonical profiling JSON and runtime CSV; BF labels are CREATE/USE counts.",
			ha="center",
			fontsize=8,
		)
		fig.tight_layout(rect=(0, 0.04, 1, 0.95))
		fig.savefig(figures / f"case_study_{ds_id}_{query.lower()}.png", dpi=180)
		plt.close(fig)


def render_figures(
	output_dir: Path,
	datasets: Sequence[Dataset],
	corpus_rows: Sequence[dict[str, Any]],
	case_rows: Sequence[dict[str, Any]],
	winner_rows: Sequence[dict[str, Any]],
	best_k_rows: Sequence[dict[str, Any]],
	distributed_corpus_rows: Sequence[dict[str, Any]],
) -> None:
	try:
		import matplotlib

		matplotlib.use("Agg")
		import matplotlib.pyplot as plt
	except ImportError:
		print("warning: matplotlib unavailable; skipping figures", file=sys.stderr)
		return

	figures = output_dir / "figures"
	figures.mkdir(parents=True, exist_ok=True)
	eight_thread_ids = [ds.dataset_id for ds in datasets if ds.threads == 8]
	labels = [ds.benchmark for ds in datasets if ds.threads == 8]
	xs = list(range(len(eight_thread_ids)))
	width = 0.24
	comparison_style = (
		("forward_vs_baseline", "Forward-only", -width),
		("thc_vs_baseline", "THC", 0.0),
		("full_vs_baseline", "RPT+", width),
	)
	lookup = {(row["dataset_id"], row["comparison"]): row for row in corpus_rows}
	fig, ax = plt.subplots(figsize=(13, 5.5))
	for comparison, label, offset in comparison_style:
		values = [
			float(lookup[(ds_id, comparison)]["query_geomean_of_median_speedup"]) for ds_id in eight_thread_ids
		]
		ax.bar([x + offset for x in xs], values, width=width, label=label)
	ax.axhline(1.0, color="black", linewidth=0.8)
	ax.set_xticks(xs, labels, rotation=35, ha="right")
	ax.set_ylabel("Geometric mean speedup vs baseline (×)")
	ax.set_title("Treatment speedup across queries, 8 threads")
	ax.legend()
	ax.grid(axis="y", linestyle=":", alpha=0.4)
	fig.tight_layout()
	fig.savefig(figures / "geomean_speedup_8threads.png", dpi=180)
	plt.close(fig)

	winner_counts: dict[str, Counter[str]] = defaultdict(Counter)
	for row in winner_rows:
		if int(row["threads"]) == 8 and row["median_winner"] in {"1", "2", "3", "4"}:
			winner_counts[str(row["dataset_id"])][str(row["median_winner"])] += 1
	fig, ax = plt.subplots(figsize=(13, 5.5))
	bottom = [0] * len(eight_thread_ids)
	for case in range(1, 5):
		values = [winner_counts[ds_id][str(case)] for ds_id in eight_thread_ids]
		ax.bar(xs, values, bottom=bottom, label=CASE_NAMES[case])
		bottom = [a + b for a, b in zip(bottom, values)]
	ax.set_xticks(xs, labels, rotation=35, ha="right")
	ax.set_ylabel("Queries with lowest median runtime (count)")
	ax.set_title("Per-query median winner, 8 threads (fully completed queries)")
	ax.legend(ncols=4)
	fig.tight_layout()
	fig.savefig(figures / "median_winner_counts_8threads.png", dpi=180)
	plt.close(fig)

	tail_lookup: dict[tuple[str, int], list[float]] = defaultdict(list)
	for row in case_rows:
		if int(row["threads"]) == 8 and row["p90_p50"] is not None:
			tail_lookup[(str(row["dataset_id"]), int(row["case"]))].append(float(row["p90_p50"]))
	fig, ax = plt.subplots(figsize=(13, 5.5))
	for case, offset in ((1, -1.5 * width), (2, -0.5 * width), (3, 0.5 * width), (4, 1.5 * width)):
		values = [quantile(tail_lookup[(ds_id, case)], 0.5) or 0 for ds_id in eight_thread_ids]
		ax.bar([x + offset for x in xs], values, width=width, label=CASE_NAMES[case])
	ax.axhline(1.0, color="black", linewidth=0.8)
	ax.set_xticks(xs, labels, rotation=35, ha="right")
	ax.set_ylabel("Median per-query p90 / p50 runtime")
	ax.set_title("Plan-sensitivity tail ratio, 8 threads")
	ax.legend(ncols=4)
	fig.tight_layout()
	fig.savefig(figures / "plan_tail_ratio_8threads.png", dpi=180)
	plt.close(fig)

	# Parallelism is a first-order result. For TPC suites, each point gives
	# equal weight to the four scale-factor dataset summaries.
	fig, axes = plt.subplots(1, 4, figsize=(15, 4.2), sharey=True)
	for axis, suite in zip(axes, ("appian", "job", "tpch", "tpcds")):
		for comparison, label in (("forward_vs_baseline", "Forward-only"), ("full_vs_baseline", "RPT+")):
			values = []
			for threads in (1, 8, 64):
				matches = [
					float(row["query_geomean_of_median_speedup"])
					for row in corpus_rows
					if row["suite"] == suite and int(row["threads"]) == threads and row["comparison"] == comparison
				]
				values.append(geometric_mean(matches) or math.nan)
			axis.plot((1, 8, 64), values, marker="o", label=label)
		axis.axhline(1.0, color="black", linewidth=0.7)
		axis.set_xscale("log", base=2)
		axis.set_xticks((1, 8, 64), ("1", "8", "64"))
		axis.set_title(suite.upper())
		axis.set_xlabel("Configured threads")
		axis.grid(linestyle=":", alpha=0.35)
	axes[0].set_ylabel("Geomean speedup vs baseline (×)")
	axes[0].legend()
	fig.suptitle("Predicate-transfer benefit attenuates with parallelism")
	fig.tight_layout()
	fig.savefig(figures / "speedup_by_threads.png", dpi=180)
	plt.close(fig)

	# Exact expected best-of-k over the finite JOB/t8 plan set. Normalize every
	# case to Case 1's sampled oracle so optimizer search and treatment value
	# remain visible in one plot.
	job_best = [row for row in best_k_rows if row["dataset_id"] == "job_t8"]
	baseline_oracle = {
		str(row["query"]): float(row["sampled_oracle_capped_runtime"])
		for row in job_best
		if int(row["case"]) == 1 and int(row["k"]) == 20
	}
	fig, ax = plt.subplots(figsize=(7.2, 4.8))
	for case in range(1, 5):
		ys = []
		for k in (1, 2, 3, 5, 10, 20):
			ratios = [
				float(row["expected_best_capped_runtime"]) / baseline_oracle[str(row["query"])]
				for row in job_best
				if int(row["case"]) == case and int(row["k"]) == k and str(row["query"]) in baseline_oracle
			]
			ys.append(geometric_mean(ratios) or math.nan)
		ax.plot((1, 2, 3, 5, 10, 20), ys, marker="o", label=CASE_NAMES[case])
	ax.axhline(1.0, color="black", linewidth=0.8, label="Baseline sampled oracle")
	ax.set_xscale("log", base=2)
	ax.set_xticks((1, 2, 3, 5, 10, 20), ("1", "2", "3", "5", "10", "20"))
	ax.set_xlabel("Plans examined (k)")
	ax.set_ylabel("Expected best runtime / baseline oracle (lower is better)")
	ax.set_title("JOB: optimizer search and predicate transfer are complementary")
	ax.legend()
	ax.grid(linestyle=":", alpha=0.35)
	fig.tight_layout()
	fig.savefig(figures / "job_expected_best_of_k.png", dpi=180)
	plt.close(fig)

	if distributed_corpus_rows:
		dist_lookup = {
			(
				str(row["dataset_id"]),
				int(row["treatment_case"]),
				float(row["bf_cost_multiplier"]),
				str(row["model"]),
			): float(row["query_geomean_adjusted_speedup_vs_baseline"])
			for row in distributed_corpus_rows
		}
		fig, axes = plt.subplots(2, 5, figsize=(15, 7), sharey=True)
		for axis, ds_id, label in zip(axes.flat, eight_thread_ids, labels):
			for case, case_label in ((2, "Forward-only"), (4, "RPT+")):
				values = [dist_lookup.get((ds_id, case, multiplier, "cpu_share"), math.nan) for multiplier in BF_MULTIPLIERS]
				axis.plot(BF_MULTIPLIERS, values, marker="o", label=case_label)
			axis.axhline(1.0, color="black", linewidth=0.7)
			axis.set_title(label)
			axis.set_xlabel("BF cost multiplier (×)")
			axis.grid(linestyle=":", alpha=0.3)
		axes[0, 0].set_ylabel("Geomean adjusted speedup")
		axes[1, 0].set_ylabel("Geomean adjusted speedup")
		axes[0, 0].legend()
		fig.suptitle("Bloom-filter cost sensitivity, 8 threads")
		fig.tight_layout()
		fig.savefig(figures / "bf_cost_sensitivity_8threads.png", dpi=180)
		plt.close(fig)


def json_ready(value: Any) -> Any:
	if isinstance(value, Path):
		return str(value)
	if isinstance(value, dict):
		return {str(key): json_ready(item) for key, item in value.items()}
	if isinstance(value, (list, tuple)):
		return [json_ready(item) for item in value]
	if isinstance(value, float) and not math.isfinite(value):
		return None
	return value


def main() -> int:
	args = parse_args()
	args.output_dir.mkdir(parents=True, exist_ok=True)
	derived = args.output_dir / "derived"
	derived.mkdir(parents=True, exist_ok=True)

	datasets = load_manifest(args.summary.resolve(), args.results_root.resolve())
	data, manifest_rows = load_runtimes(datasets)
	ash_run_rows, ash_file_rows = analyze_existing_ash(args.results_root.resolve())
	(
		case_rows,
		pair_rows,
		query_pair_rows,
		corpus_rows,
		best_seed_rows,
		winner_rows,
		best_k_rows,
		runtime_summary,
	) = analyze_runtimes(datasets, data)

	write_csv(derived / "manifest.csv", manifest_rows)
	write_csv(derived / "query_case_metrics.csv", case_rows)
	write_csv(derived / "paired_seed_metrics.csv", pair_rows)
	write_csv(derived / "query_pair_metrics.csv", query_pair_rows)
	write_csv(derived / "corpus_pair_metrics.csv", corpus_rows)
	write_csv(derived / "baseline_best_seed.csv", best_seed_rows)
	write_csv(derived / "query_median_winners.csv", winner_rows)
	write_csv(derived / "optimizer_best_of_k.csv", best_k_rows)
	write_csv(derived / "ash_existing_runs.csv", ash_run_rows)
	write_csv(derived / "ash_existing_file_summaries.csv", ash_file_rows)

	profile_summary: dict[str, Any] = {}
	distributed_corpus_rows: list[dict[str, Any]] = []
	profiles: list[dict[str, Any]] = []
	study_rows: list[dict[str, Any]] = []
	if not args.skip_profiles:
		tasks, missing_profiles = profile_tasks(datasets, data)
		write_csv(
			derived / "missing_success_profiles.csv",
			missing_profiles,
			fields=("dataset_id", "query", "case", "seed", "status", "expected_profile"),
		)
		profiles = parse_profiles(tasks, args.profile_workers)
		write_csv(derived / "profile_metrics.csv", profiles, fields=PROFILE_FIELDS)
		(
			profile_pair_rows,
			plan_rows,
			correlation_rows,
			distributed_query_rows,
			distributed_corpus_rows,
			profile_summary,
		) = analyze_profiles(datasets, profiles)
		profile_summary["missing_success_profiles"] = len(missing_profiles)
		write_csv(derived / "profile_pair_metrics.csv", profile_pair_rows)
		write_csv(derived / "plan_equivalence.csv", plan_rows)
		write_csv(derived / "profile_correlations.csv", correlation_rows)
		write_csv(derived / "distributed_query_sensitivity.csv", distributed_query_rows)
		write_csv(derived / "distributed_corpus_sensitivity.csv", distributed_corpus_rows)
		study_rows = case_study_metrics(data, profiles)
		write_csv(derived / "case_study_metrics.csv", study_rows)

	if not args.no_figures:
		render_figures(
			args.output_dir,
			datasets,
			corpus_rows,
			case_rows,
			winner_rows,
			best_k_rows,
			distributed_corpus_rows,
		)
		if study_rows:
			render_case_study_figures(args.output_dir, study_rows)

	summary = {
		"generated_by": str(Path(__file__).resolve()),
		"summary_source": str(args.summary.resolve()),
		"results_root": str(args.results_root.resolve()),
		"runtime": runtime_summary,
		"profiles": profile_summary,
		"ash": {
			"source_file_count": len(ash_file_rows),
			"runtime_observation_count": len(ash_run_rows),
			"settings_provenance": "not_embedded_unverified",
		},
		"datasets": manifest_rows,
	}
	with (derived / "analysis_summary.json").open("w") as handle:
		json.dump(json_ready(summary), handle, indent=2, sort_keys=True)
		handle.write("\n")

	print(
		f"Analyzed {runtime_summary['canonical_dataset_count']} datasets and "
		f"{runtime_summary['runtime_observation_count']:,} runtime observations; "
		f"wrote outputs to {args.output_dir}",
		flush=True,
	)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
