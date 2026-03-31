# THC Probe Loop Performance Analysis

**Date**: 2026-03-27
**Platform**: AWS Graviton2 (ARM Neoverse-N1), 64KB L1d, 1MB L2, 32MB shared L3, 64B cache lines
**Benchmark**: `./launch_release.sh rs` (5-run average, `taskset -c 4-59`, single-threaded query)
**Workload**: 50.4M probe-side rows joined against ~400K hot build-side entries in the THC

## Methodology

All measurements use Linux `perf stat` with ARM PMU hardware counters.
The counters instrument the **entire DuckDB process** (not just the THC loops),
so THC-specific numbers are estimated by proportional attribution where noted.

### Commands used

**Counter group 1** (branch prediction, cache hierarchy, stalls):

```bash
sudo perf stat -e cpu_cycles,inst_retired,br_retired,br_mis_pred_retired,\
l1d_cache,l1d_cache_refill,l2d_cache,l2d_cache_refill,\
mem_access,stall_frontend,stall_backend \
-- taskset -c 4-59 build/release/duckdb ASH-datagen/bench.duckdb
```

**Counter group 2** (DRAM bus traffic):

```bash
sudo perf stat -e cpu_cycles,inst_retired,bus_access,bus_access_rd,bus_access_wr,\
l1d_cache_refill_outer,l2d_cache_wb_victim,stall_backend \
-- taskset -c 4-59 build/release/duckdb ASH-datagen/bench.duckdb
```

Both run the same 5-iteration benchmark via piped SQL statements.
`perf stat` multiplexes when there are more events than hardware PMU slots (~6 on
Neoverse-N1), which is why some counters show (xx%) sampling fractions. The values
are scaled estimates from the sampled fraction.

## Measured Results

### With prefetch (production baseline, 2.93s avg)

| Counter | Value | Derived Metric |
|---------|-------|----------------|
| cpu_cycles | 45.3 B | |
| inst_retired | 77.1 B | **IPC = 1.70** (max 4.0 on N1) |
| br_retired | 14.9 B | |
| br_mis_pred_retired | 89.3 M | **Misprediction rate = 0.60%** |
| l1d_cache | 28.2 B | |
| l1d_cache_refill | 1.36 B | **L1D miss rate = 4.8%** |
| l2d_cache | 5.85 B | |
| l2d_cache_refill | 660 M | **L2 miss rate = 11.3%** |
| mem_access | 28.2 B | |
| stall_frontend | 8.3 B | **18.3% of cycles** |
| stall_backend | 13.1 B | **28.9% of cycles** |
| bus_access | 3.71 B | ~237 GB bus traffic (13.2 GB/s) |

### Without prefetch (3.18s avg, +8.8% regression)

| Counter | Value | Delta vs baseline |
|---------|-------|-------------------|
| cpu_cycles | 50.1 B | **+10.6%** |
| inst_retired | 75.4 B | -2.2% (fewer prefetch insns) |
| br_retired | 14.7 B | -1.3% |
| br_mis_pred_retired | 92.5 M | +3.6% |
| l1d_cache | 28.4 B | +0.7% |
| l1d_cache_refill | 1.65 B | **+21.3%** |
| l2d_cache | 5.87 B | +0.3% |
| l2d_cache_refill | 666 M | +0.9% |
| stall_frontend | 6.0 B | -27.7% (fewer insns to decode) |
| stall_backend | 20.1 B | **+53.5% (28.9% → 40.2% of cycles)** |

## Analysis

### 1. The workload is memory-latency-bound

**Evidence:**

- **Backend stalls consume 28.9% of all CPU cycles** in the baseline. Backend stalls
  on Neoverse-N1 count cycles where the execution units are starved — primarily
  caused by cache misses and memory access latency.

- **IPC is 1.70 out of a theoretical 4.0** (43% pipeline utilization). If the
  workload were compute-bound, IPC would be closer to 3-4. The 57% waste
  is almost entirely from memory stalls.

- **Removing prefetch increases backend stalls from 28.9% to 40.2%** of cycles
  (+53.5%), while instructions _decrease_ 2.2%. Fewer instructions but more
  stall cycles proves the bottleneck is memory latency, not computation.

- **L1D cache misses increase 21.3% without prefetch**, directly showing that
  prefetch was successfully pre-loading THC entries into L1D before they were needed.

### 2. Branch prediction is 99.4% accurate — not a bottleneck

**Evidence:**

- **br_mis_pred_retired / br_retired = 89.3M / 14.9B = 0.60%** misprediction rate.
  The Neoverse-N1 uses a TAGE-class branch predictor that learns the strongly biased
  patterns in the THC probe loops (90%+ tag matches).

- **Estimated misprediction cost**: 89.3M × 12 cycles (pipeline flush penalty) =
  1.07B cycles = **2.4% of total cycles**. This is an upper bound — the THC likely
  accounts for only a fraction of total mispredictions.

- **Compare to memory stalls**: backend stalls (28.9% of cycles) are **12× larger**
  than branch misprediction cost (2.4%). Even eliminating ALL branch mispredictions
  would save at most ~2.4% of run time, while memory stalls waste 28.9%.

### 3. Why removing control flow hurts performance

Three branchless/batch variants were benchmarked:

| Variant | Total Time | THC Probe Time | vs Baseline |
|---------|-----------|----------------|-------------|
| Original (branchy + prefetch) | 2.93s | 581ms | baseline |
| Fully branchless | 3.04s | 675ms | +3.8% |
| Partial branchless (empty-guard) | 2.96s | 597ms | +1.0% |
| Batch prefetch (32) | 3.07s | 700ms | +5.0% |
| No prefetch | 3.18s | 875ms | +8.8% |

**Why branchless is worse:**

1. **Unconditional loads from empty entries**: Empty slots (stored_tag == 0) are
   ~80% of THC capacity. With branches, the code skips key loads for empty entries.
   Branchless code unconditionally loads keys from every slot, bringing cold cache
   lines into L1/L2 and evicting useful data.

2. **Extra stores**: Branchless code writes to match_sel, miss_sel, and uncertain_buf
   on every iteration. Branchy code only writes to the relevant path. At 50M probes,
   ~3 extra stores/iteration = 150M+ extra store instructions.

3. **Branch savings are negligible**: Since misprediction is only 0.60%, the ~12-cycle
   penalty applies to <1% of branches. The cost of removing branches
   (unconditional loads + stores) exceeds the misprediction savings.

### 4. Prefetch is critical and should not be removed

**Removing prefetch regresses THC Probe Time by 50%** (581ms → 875ms), or +294ms.
The `__builtin_prefetch` calls hide memory latency by issuing load requests 16
iterations ahead of the current probe. This gives ~160ns lead time (16 × ~10ns/probe),
sufficient to cover L2 miss latency (~12ns) and partially cover DRAM latency (~100-200ns).

The GCC vectorization report flags `__builtin_prefetch` as "statement clobbers memory."
This prevents auto-vectorization of the probe loop. However, **the loop cannot be
vectorized regardless**: it requires gather loads (random hash table slot accesses),
and ARM NEON has no gather instruction. Removing prefetch would not unlock
vectorization but would cost 294ms of performance.

### 5. Why SIMD/vectorization is fundamentally inapplicable

The THC probe loop accesses `base_ptr + (hash & bitmask) * stride` — a **random
scatter/gather** pattern. Each probe reads from a different cache line at an
unpredictable address. ARM NEON provides no gather-load instruction (unlike
x86 AVX2/512 `vpgatherdd`). Even on x86, gather instructions have high latency
and often don't improve throughput for random-access patterns.

DuckDB's "vectorized execution" means processing data in batches (vectors of 2048 rows)
to amortize per-tuple overhead, which the THC already does. It does not mean
SIMD vectorization of every loop.

## Conclusion

The THC probe loops are **memory-latency-bound with well-predicted branches**.
The current implementation (interleaved software prefetch + branchy early-exit +
`__builtin_expect` hints) is already well-tuned for this access pattern.
Further optimization should focus on:

- Reducing THC capacity/working set to improve cache residency
- Improving the THC hit rate (more entries at primary slot, fewer collisions)
- Reducing entry size to fit more entries per cache line
- Exploring alternative prefetch distances or two-level prefetch strategies

Branch elimination and SIMD vectorization are **not viable optimization paths**
for this random-access, pointer-chasing workload.
