# Synthetic Three-Join Benchmark Generator

**TL;DR:** Edit `settings-common.sql` and the run-specific settings file, then run `scripts/measure/run_ash_datagen_release.sh --case 3 rs`

## Overview

This project provides a **controllable synthetic data generator** for benchmarking a three-table join with selection predicates. It is designed so you can independently control:

- the fraction of rows selected from each base table
- the fraction of filtered `S` rows that participate in `R ⋈ S`
- the fraction of filtered `S` rows that participate in `S ⋈ T`
- the fraction of filtered `S` rows that survive the full three-way join
- the number of selected rows that pass filters but do not match in joins
- the probe multiplicities under a fixed left-deep hash-join plan

The generator produces three tables:

```sql
R(sel_key_R, join_key_RS)
S(sel_key_S, join_key_RS, join_key_ST)
T(sel_key_T, join_key_ST)
````

An example query (`query_rst.sql`) is:

```sql
SELECT COUNT(*) AS full_result_count
FROM R
JOIN S ON R.join_key_RS = S.join_key_RS
JOIN T ON S.join_key_ST = T.join_key_ST
WHERE R.sel_key_R <= cutoff_R
  AND S.sel_key_S <= cutoff_S
  AND T.sel_key_T <= cutoff_T;
```

The `sel_key_*` columns support simple range predicates. The `join_key_*` columns control the join behavior.

## Fixed Plan Assumption

The multiplicity parameters are defined with respect to a **fixed left-deep hash-join plan**:

```sql
(R JOIN S) JOIN T
```

with the **right side as the build side** in both joins:

* first join: `R ⋈ S`, with **`S` as the build side**
* second join: `(R ⋈ S) ⋈ T`, with **`T` as the build side**

Under that assumption:

* `probe_multiplicity_in_R` is the number of probes made by productive tuples from `R` into the hash table built on `S` during the first join
* `probe_multiplicity_in_S` is the number of probes made by productive tuples from the output of the first join into the hash table built on `T` during the second join

These parameters are execution-oriented, but they also have a data-layout consequence: the generator constructs the key distributions so that the selected productive tuples realize those probe counts under this plan.

## Core Design

Among the **filtered rows of `S`**, the generator conceptually creates four groups:

| Group   | Meaning                     |
| ------- | --------------------------- |
| Bridge  | joins with both `R` and `T` |
| RS-only | joins with `R` but not `T`  |
| ST-only | joins with `T` but not `R`  |
| Neither | joins with neither side     |

These groups are still the easiest way to understand the SQL generator, because they control the three conceptual join-support counts:

* filtered `S` rows that can join with `R`
* filtered `S` rows that can join with `T`
* filtered `S` rows that can join with both

Those conceptual counts are controlled independently of the multiplicity parameters.

## Parameters

### Scale

| Parameter      | Meaning                         |
| -------------- | ------------------------------- |
| `scale_factor` | Multiplies the base table sizes |

Base sizes at a few scale factors: 

| Scale factor |  `R` |  `S` |  `T` |
| ------------ | ---: | ---: | ---: |
| 1            |   1M | 300k | 100k |
| 10           |  10M |   3M |   1M |
| 100          | 100M |  30M |  10M |

### Filter selectivities

These determine how many rows pass the range predicates.

| Parameter             | Meaning                       |
| --------------------- | ----------------------------- |
| `selected_fraction_R` | Fraction of `R` rows selected |
| `selected_fraction_S` | Fraction of `S` rows selected |
| `selected_fraction_T` | Fraction of `T` rows selected |

The selected rows are those satisfying:

```sql
R.sel_key_R <= filtered_R
S.sel_key_S <= filtered_S
T.sel_key_T <= filtered_T
```

### Conceptual join-support fractions

These are defined over the **filtered rows of `S`**.

| Parameter          | Meaning                                                       |
| ------------------ | ------------------------------------------------------------- |
| `join_fraction_RS` | Fraction of filtered `S` rows that join with `R`              |
| `join_fraction_ST` | Fraction of filtered `S` rows that join with `T`              |
| `bridge_fraction`  | Fraction of filtered `S` rows that join with both `R` and `T` |

These determine the four `S` groups:

* `bridge_rows = filtered_S × bridge_fraction`
* `matched_rows_RS_in_S = filtered_S × join_fraction_RS`
* `matched_rows_ST_in_S = filtered_S × join_fraction_ST`
* `rs_only_rows_in_S = matched_rows_RS_in_S - bridge_rows`
* `st_only_rows_in_S = matched_rows_ST_in_S - bridge_rows`
* `neither_rows_in_S = filtered_S - matched_rows_RS_in_S - matched_rows_ST_in_S + bridge_rows`

### Probe multiplicities

These are defined under the fixed left-deep plan `(R JOIN S) JOIN T`, with the right side as the build side.

| Parameter                 | Meaning                                                                                                                |
| ------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `probe_multiplicity_in_R` | Number of probes from productive tuples in `R` into the hash table built on `S` during `R ⋈ S`                         |
| `probe_multiplicity_in_S` | Number of probes from productive tuples in the output of `R ⋈ S` into the hash table built on `T` during `(R ⋈ S) ⋈ T` |

The conceptual join-support fractions above stay the same regardless of these multiplicities. The multiplicities affect how many probe-side tuples are generated for the productive parts of the plan.

### Unproductive selected rows

These control selected rows that pass filters but are guaranteed not to match.

| Parameter              | Meaning                                                    |
| ---------------------- | ---------------------------------------------------------- |
| `unproductive_rate_RS` | Selected non-matching mass introduced for the `R`/`S` join |
| `unproductive_rate_ST` | Selected non-matching mass introduced for the `S`/`T` join |

## Reading the Counts

It helps to distinguish two kinds of quantities:

### 1. Conceptual join-support counts

These are defined from the filtered rows of `S` and do **not** depend on multiplicity:

* filtered `S` rows that can participate in `R ⋈ S`
* filtered `S` rows that can participate in `S ⋈ T`
* filtered `S` rows that can participate in `R ⋈ S ⋈ T`

In the SQL file these correspond to quantities such as:

* `matched_rows_RS_in_S`
* `matched_rows_ST_in_S`
* `bridge_rows`

### 2. Probe-side multiplicities under the fixed plan

These determine how many probe-side tuples are generated for the productive parts of the two joins:

* `probe_multiplicity_in_R` applies to probes from `R` into hash table `S`
* `probe_multiplicity_in_S` applies to probes from `(R ⋈ S)` output into hash table `T`

So the bridge / RS-only / ST-only / neither split tells you **which filtered `S` rows are productive**, while the multiplicity parameters tell you **how many probe-side tuples are driven through those productive regions** under the assumed execution plan.

## Usage Examples

To run the R-S query in DuckDB Release mode:
```text
`scripts/measure/run_ash_datagen_release.sh --case 3 rs`
```

To run the R-S-T query in DuckDB Debug mode:
```text
`scripts/measure/run_ash_datagen_debug.sh --case 3 rst`
```

## Driver File Workflow

Edit `settings-common.sql` and (optionally) a run-specific settings file under `scripts/measure/`, then set your scenario values with `SET VARIABLE`.


Set scale factor and other parameters:

```sql
SET VARIABLE scale_factor = 100;
SET VARIABLE selected_fraction_R = 0.08;
SET VARIABLE selected_fraction_S = 0.04;
SET VARIABLE selected_fraction_T = 0.18;
-- ... etc
```

## Notes

* The generator is deterministic.
* Join keys use disjoint numeric ranges so productive and unproductive regions do not overlap accidentally.
* The script computes feasibility checks before creating the tables.
* If the chosen parameter combination is infeasible, `generator_status` will report that and table creation is gated on `is_feasible`.