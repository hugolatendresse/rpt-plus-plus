# How `CreateBF`, `UseBF`, and `HashJoin` execute — a runtime tour

This document walks through what actually happens at runtime when DuckDB
executes the plan

```
Scan -> Filter -> UseBF -> CreateBF -> BuildHashTable
```

on the build side of a hash join. It is written for someone who has read
`optimization-pipeline.md` and `top_down_tour.md` and understands roughly what
RPT+ is, but who has not necessarily traced a pipeline through the executor.

A short cheat-sheet of DuckDB terms used below:

- **`PhysicalOperator`** — the runtime operator base class. It can implement
  three interfaces: `Source` (produces chunks), `Operator` / `Execute`
  (streaming transform), and `Sink` (consumes and materializes chunks).
- **Pipeline** — a chain of operators that streams data in one push: from one
  *source* through any number of *streaming operators* into exactly one *sink*.
  A pipeline runs to completion (sink fully drains) before any pipeline that
  depends on its output can start.
- **`MetaPipeline`** — a tree of pipelines. When a sink itself has a `Source`
  side (like `CreateBF` or `PhysicalHashJoin`), the executor builds a *child*
  meta-pipeline to fill its sink first, then a sibling pipeline to read from
  its source.
- **`DataChunk`** — a vectorized column-oriented batch (≤ 2048 rows). Sometimes
  the columns are real arrays (flat vectors), sometimes the chunk just
  *references* another chunk's vectors via `Reference`/`Slice`.
- **`ColumnDataCollection`** — a chunked, column-oriented store. Append a
  chunk, scan it back later. Used by `CreateBF` to buffer the build side.
- **`TupleDataCollection`** — a chunked, **row-oriented** store. Each row is
  a packed `[key1 .. keyN, payload1 .. payloadM, (found?), hash]` byte blob.
  Used by `JoinHashTable` for the actual join state.
- **`RadixPartitionedTupleData`** — a `TupleDataCollection` split into 2^k
  partitions by the high bits of the row's hash. Lets multiple threads build
  and probe independent slices of the table.

All line numbers refer to the current tree on branch `hl/ablation0512`.

---

## Q1 — Is `Scan → Filter → UseBF → CreateBF → BuildHashTable` one pipeline?

**No. It is two pipelines, plus a third for the probe side later.**

Two operators in that chain are sinks: `CreateBF` and the `BuildHashTable`
half of `PhysicalHashJoin`. Each sink forces a pipeline break, because a
pipeline must end at exactly one sink and a sink has to be fully populated
before its source side can be read.

### How the executor builds the pipeline tree

When physical-plan-generator walks bottom-up to assemble pipelines, it calls
`BuildPipelines` on each operator. The relevant ones:

- `PhysicalCreateBF::BuildPipelines`
  (`src/execution/operator/persistent/physical_create_bf.cpp:556-589`).
  This method (a) calls `state.SetPipelineSource(current, *this)` — so the
  *current* pipeline (the one ending at the hash join build sink) will read
  *from* `CreateBF` as a source — and (b) creates a `CreateChildMetaPipeline`
  whose base pipeline ends at `CreateBF` as a *sink*. That child pipeline is
  what builds the BF; the current pipeline is a *dependent* of it.
- `PhysicalUseBF` does **not** override pipeline construction in a way that
  breaks the pipeline. Its `BuildPipelines` (`physical_use_bf.cpp:67-74`)
  just adds itself as a streaming operator and recurses. Note that
  `PhysicalUseBF` inherits from `CachingPhysicalOperator`, not from anything
  sink-like: `IsSink()` is the inherited default `false`.
- `PhysicalHashJoin::IsSink()` returns true
  (`src/include/duckdb/execution/operator/join/physical_hash_join.hpp:101-106`),
  and the join is also a `Source` (probe-side output). It does its own
  child-meta-pipeline trick so the build side is fully materialized before
  the probe-side pipeline runs.

### The three pipelines for `Scan → Filter → UseBF → CreateBF → BuildHashTable → … (probe)`

Drawing them in execution order:

```
Pipeline B1 (sink: CreateBF)
    Scan  ->  Filter  ->  UseBF  ->  CreateBF[sink]

Pipeline B2 (sink: HashJoin build side)
    CreateBF[source]  ->  HashJoin[build-side sink]
        (depends on B1; the HashJoin sink only starts after CreateBF finishes)

Pipeline P (sink: whatever consumes the join output)
    (probe Scan)  ->  (probe-side UseBFs etc.)  ->  HashJoin[probe/source]  ->  ...
        (depends on B2; cannot probe until the hash directory is finalized)
```

### What is materialized where

| Sink | What gets materialized | Storage type |
|------|------------------------|--------------|
| `CreateBF` | All surviving build-side rows (post-filter, post-UseBF) | One `ColumnDataCollection` per thread → merged into one global `ColumnDataCollection` (`src/execution/operator/persistent/physical_create_bf.cpp:287`, `:303-317`). Held in buffer-managed blocks. |
| `HashJoin` build | The same rows re-serialized as row-oriented tuples `[keys, payload, (found?), hash]` | `RadixPartitionedTupleData` (sink_collection) during the sink phase, then merged into a `TupleDataCollection` (data_collection) for the final pointer table (`src/execution/join_hashtable.cpp:1268-1313`). |

So the build-side data is in fact materialized **twice** on this path — once
as columnar storage inside `CreateBF`, then again as row-oriented tuple data
inside `JoinHashTable`. The first copy is what's used to populate the
Bloom filter; the second copy is the actual hash-join state. `CreateBF` does
*not* hand its in-memory chunks directly to `JoinHashTable`; it re-emits them
through its `Source` interface (`GetData`,
`physical_create_bf.cpp:508-514`) and `JoinHashTable::Build` re-ingests them.

---

## Q2 — How does `CreateBF` work?

`PhysicalCreateBF` is both a `Sink` and a `Source`
(`physical_create_bf.hpp:72-98`). Pipeline B1 above pushes chunks into it;
pipeline B2 reads them back out.

### The sink half — three phases

#### Phase 1 — `Sink` (per chunk, per thread)

`physical_create_bf.cpp:279-298`. For every chunk coming up from below:

1. Check `GiveUpBFCreation(chunk, input)` (see below). If it returns true,
   set `is_successful = false` and return `SinkResultType::FINISHED`, which
   tells the executor to stop feeding this sink.
2. Append the chunk verbatim to a thread-local `ColumnDataCollection`
   (`state.local_data->Append(chunk)`). This is the materialization step:
   from here on the rows are owned by the buffer manager via the
   `ColumnDataCollection`'s blocks.
3. For each column that has a min/max filter attached (the dynamic filter
   path that lets RPT+ push range predicates as well as BFs), feed the
   relevant column into the thread-local `RowOperationsState`-backed
   aggregate state.

Note that the data is not yet inserted into any Bloom filter. The Bloom
filter is empty at this point. The sink only buffers rows and accumulates
min/max stats.

#### Phase 2 — `Combine` (per thread, on finish)

`physical_create_bf.cpp:300-319`. When a thread finishes, it (a) merges its
local min/max aggregate into the global aggregate, (b) moves its local
`ColumnDataCollection` into a vector on the global sink state
(`gstate.local_data_collections.push_back(...)`), and (c) updates the
temporary-memory reservation so the buffer manager knows the BF stage needs
that much memory.

#### Phase 3 — `Finalize` (single-threaded, then async tasks)

`physical_create_bf.cpp:420-447`. This is where the Bloom filter is actually
built:

1. If `is_successful` is already false, return — nothing else to do.
2. `sink.FinalizeMinMax()` turns the merged min/max state into
   `ConstantFilter` / range entries on the `DynamicTableFilterSet`s. These
   are pushed to the corresponding probe-side scans and are independent of
   the Bloom filter.
3. Concatenate all the thread-local `ColumnDataCollection`s into one global
   `data_collection` by calling `Combine` on it (`:433-436`).
4. For every distinct `BloomFilter` in `unique_bloom_filters` (one per
   distinct key column set; multiple BFs can come out of one `CreateBF`),
   call `bf->Initialize(context, num_rows)` to allocate the bit array sized
   from the total row count (`:439-443`).
5. Schedule a `CreateBFFinalizeEvent` which spawns `CreateBFFinalizeTask`s
   in parallel. Each task scans a chunk-range of `data_collection` and calls
   `bf->Insert(chunk, cols_build)` for every BF
   (`physical_create_bf.cpp:344-412`). After the event finishes, every BF
   has `finalized_ = true`, which is what `BloomFilter::IsValid()` checks.

So the full path is: rows → columnar buffer → one parallel scan over the
buffer → BF populated. One pass over the data to materialize, one pass to
hash and insert into the BF.

### Where are the tuples after Finalize?

Still in the `ColumnDataCollection` (`gstate.data_collection`). They are not
freed: pipeline B2 still needs to read them as the source side of `CreateBF`.

The `ColumnDataCollection` is backed by buffer-managed blocks. If memory
pressure spikes, those blocks can be evicted to disk — they live in the
ordinary block-manager world like any other DuckDB intermediate. The
operator itself does not pin them in RAM beyond what the buffer manager
chooses.

After pipeline B2 drains, the global sink state goes out of scope and the
collection is freed.

### `GiveUpBFCreation` and `drop_bf_at_runtime`

`physical_create_bf.cpp:200-277`. Gated by `ClientConfig.drop_bf_at_runtime`
(`true` by default). When enabled, the operator may abort early if any of:

- **OOM**: adding this chunk would exceed the temporary-memory reservation
  (`:220-224`).
- **Bad selectivity**: after at least 32 chunks (~64 K rows), the operator
  estimates `actual_rows / estimated_rows_from_optimizer`. If this ratio is
  above 0.20 (or 0.35 in SQLStorm mode), the upstream filter is too weak —
  a BF on most of the table is not worth the build cost (`:227-262`).
- **Estimated final memory**: extrapolate the rate at which rows are
  arriving against the optimizer's cardinality estimate, and if the
  extrapolation says we'll blow the budget, give up (`:266-272`).

On giving up: `is_successful = false`, `Sink` returns `FINISHED`, `Finalize`
does nothing, and the BFs stay un-finalized (so `IsValid()` returns false
for downstream `UseBF`s).

### How many BFs per `CreateBF`?

One per distinct set of build-side key columns referenced by an incoming
`FilterPlan` (`physical_create_bf.cpp:30-39`). The map is
`unique_bloom_filters: vector<idx_t> -> shared_ptr<BloomFilter>`
(`physical_create_bf.hpp:55`). A single `CreateBF` can build several BFs in
parallel because they share the same materialized data; the
`CreateBFFinalizeTask` loop inserts into all of them in one scan
(`:374-389`).

### Source half — what `CreateBF` emits downstream

Once `Finalize` returns, pipeline B2 starts and `CreateBF` acts as a
parallel `Source` (`physical_create_bf.cpp:498-514`):

- `GetGlobalSourceState` initializes a `ColumnDataParallelScanState` over
  the same `data_collection` that was used for BF building.
- `GetData` is a thin wrapper over `ColumnDataCollection::Scan`. It emits
  the same chunks — minus any per-row transformation; `CreateBF` does *not*
  filter, hash, or sort here. It is a faithful re-emit.

So from the next operator's point of view, `CreateBF` looks exactly like
any other table source.

---

## Q3 — How does `UseBF` work?

`PhysicalUseBF` is a *streaming* operator (no sink, no source — just
`Execute`). Specifically it inherits from `CachingPhysicalOperator`
(`physical_use_bf.hpp:14`), which is a streaming operator that buffers
small chunks until it has ~256 rows before passing them to
`ExecuteInternal` — this matters because BF lookups have per-chunk overhead
and benefit from full vectors.

### The filtering path

`physical_use_bf.cpp:76-109`, in `ExecuteInternal`:

1. If the per-state `use_bf` flag is false (BF was abandoned by the
   producer, or selectivity check disabled it later), just
   `chunk.Reference(input)` and return — all input rows pass through with
   their vectors aliased, no copy.
2. Call `bf_to_use->Lookup(input, state.lookup_results)`. This writes a
   `uint32_t` per input row into a pre-sized vector: 1 = "BF says maybe
   present", 0 = "BF says definitely absent".
3. Walk the result vector, accumulating a `SelectionVector` of the indices
   where `lookup_results[i] == 1`. The trick at line 92-95 is branchless:
   it always `set_index(result_count, i)` and then increments `result_count`
   by the 0-or-1 lookup result.
4. If everything passed, `chunk.Reference(input)`. Otherwise,
   `chunk.Slice(input, sel, result_count)`. **Slicing is logical**: it
   wraps every input vector with a `DictionaryVector` that follows the
   selection vector. No payload data is copied; only the selection vector
   is materialized. Subsequent operators dereference through the selection
   vector lazily.

So `UseBF` is essentially "compute a selection vector from a hashed view of
the key columns, then slice the whole chunk by it". Payload columns are
never touched (no hashing, no copying); only the key columns are read for
the BF lookup, and only the selection vector is produced as work.

A couple of details worth being precise about, because they explain why the
downstream operators never pay payload cost for filtered rows:

- `lookup_results` is a `vector<uint32_t>` of 0/1 per input row
  (`physical_use_bf.cpp:54-59`), not a bitvector. The selection vector
  built from it (`sel_vector`, declared on the same state) is a
  `SelectionVector` — an array of `sel_t` indices, which is what every
  DuckDB downstream operator expects. The `result_count += lookup_results[i]`
  loop (`:154-159`) packs the survivor indices in one branchless pass.
- The BF reads only key columns: `BloomFilter::Lookup`
  (`src/optimizer/predicate_transfer/bloom_filter/bloom_filter.cpp:57-62`)
  calls `HashColumns(chunk, bound_cols_applied)`, where
  `bound_cols_applied` is the set of join-key column indices. `HashColumns`
  (`:25-38`) iterates only over those columns — payload vectors are not
  read.
- The downstream `ColumnDataCollection::Append`
  (`src/common/types/column/column_data_collection.cpp:809`) calls
  `ToUnifiedFormat` on each incoming vector. For the `DictionaryVector`s
  produced by `chunk.Slice`, `ToUnifiedFormat` captures the dictionary's
  selection vector into `format.sel`, and the copy functions then copy
  **only the selected rows** into the CDC's blocks. Filtered-out rows'
  payload bytes are therefore never read, copied, or written downstream of
  the scan.

### Adaptive disable

`UseBFState::CheckBFSelectivity` (`:37-50`): after 32 chunks, if more than
90% of rows are surviving the BF (selectivity > 0.9 and < 1), set
`use_bf = false`. From then on this thread's `ExecuteInternal` is a
zero-cost pass-through. This is per-thread state, not global.

### How `UseBF` finds its BF

The `bf_to_use` is a `shared_ptr<BloomFilterUsage>` set at construction
(`physical_use_bf.hpp:26`). The mapping from `LogicalUseBF` to its
`LogicalCreateBF` is wired by the optimizer-time pass
`TransferBFLinker` (`src/execution/transfer_bf_linker.cpp:12-30`), which
runs five sub-passes (`COLLECT_BF_CREATORS`, `LINK_BF_USERS`,
`CLEAN_USELESS_OPERATORS`, `UPDATE_MIN_MAX_BINDING`, `SMOOTH_MARK_JOIN`,
`USE_BF_BELOW_HASH_JOIN`).

The pointer relationship at runtime is *not* `weak_ptr` for the BF itself —
the BF is held by `shared_ptr` because there is only one `PhysicalCreateBF`
operator object per query and it lives for the duration of the executor.
The `weak_ptr` you might have seen in commit `d41318c0f0` and
`physical_create_bf.hpp:41` is `this_pipeline`, which caches the producer
*Pipeline* so multiple `UseBF`s reading from the same `CreateBF` can share
its build pipeline as a dependency. That is a per-execute cache, not the BF
storage.

### What if `CreateBF` gave up?

`UseBF::GetOperatorState` (`physical_use_bf.cpp:57-59`) constructs the
operator state with `bf_to_use->IsValid()` — which is true only if the
producer finalized the BF. If the producer set `is_successful = false`,
`finalized_` stays false, `IsValid()` is false, and `use_bf` is false from
the start. The operator therefore degrades to pure pass-through with no
extra cost beyond a `chunk.Reference`.

---

## Q4 — How does `PhysicalHashJoin` build the hash table?

The build is conceptually a **single streaming pass** to materialize
key+payload+hash into row-oriented partitioned storage, followed by a
**parallel finalize** that fills the actual hash directory. There is no
second pass over the raw input.

### Layout decided once, up front

Inside `JoinHashTable`'s constructor
(`src/execution/join_hashtable.cpp:48-152`), a `TupleDataLayout` is
constructed with column types in this exact order:

```
[ key_1, key_2, ..., key_K,
  payload_1, payload_2, ..., payload_P,
  (optional) found_bool,                  // only for RIGHT/FULL OUTER
  hash_t hash ]
```

That layout drives every later offset computation. `pointer_offset` is the
byte offset within a row at which the chain-next pointer is stored (DuckDB
overwrites the `hash` slot with the next-pointer once the directory is
built — same 8 bytes serve both purposes at different lifetime stages).

Two collections are built around this layout:

- `sink_collection` — a `RadixPartitionedTupleData`, the *intermediate*
  store. Rows are radix-partitioned by the high bits of their hash so
  multiple threads can sink concurrently without contention.
- `data_collection` — a `TupleDataCollection` built later from
  `sink_collection`, which is what the directory ends up pointing into.

### Phase 1 — `PhysicalHashJoin::Sink` (one call per chunk per thread)

`src/execution/operator/join/physical_hash_join.cpp:427-452`. Per chunk:

1. `lstate.join_key_executor.Execute(chunk, lstate.join_keys)` evaluates
   the join-key expressions over this chunk into a fresh `DataChunk` of
   keys. This is the *only* place key expressions are evaluated; the
   payload columns just need a `ReferenceColumns` pass below.
2. If `filter_pushdown` is enabled, feed the keys to the build-side
   filter-pushdown machinery (this is how `PhysicalHashJoin` builds its own
   dynamic min/max filters that get pushed to the probe side — separate
   from the RPT+ BFs).
3. `lstate.payload_chunk.ReferenceColumns(chunk, payload_columns.col_idxs)`
   builds a zero-copy "payload view" of the input chunk by aliasing the
   payload columns.
4. Call `lstate.hash_table->Build(lstate.append_state, lstate.join_keys,
   lstate.payload_chunk)`.

`JoinHashTable::Build` (`join_hashtable.cpp:1240-1313`) is the workhorse:

1. Build a `source_chunk` that *references* (no copy) keys, then payload,
   then a constant-false `vfound` if right/full outer, then a placeholder
   for the hash column. The cardinality is set to keys' size.
2. `TupleDataCollection::ToUnifiedFormat(append_state.chunk_state,
   source_chunk)` walks each column once to compute a "unified format"
   description (validity mask, selection vector, data pointer). This is
   what the row-serializer consumes.
3. `PrepareKeys` filters out rows with NULL keys (for inner/left/semi
   joins; not for right/full outer). It builds a selection vector
   `current_sel` of surviving rows. If everything is NULL, return early.
4. `Hash(keys, *current_sel, added_count, hash_values)` (`:1211-1225`):
   `VectorOperations::Hash` on the first key column, then
   `VectorOperations::CombineHash` on each additional one. Only the
   equality key columns are hashed. Result is a vector of `hash_t`.
5. Re-reference the hash column in `source_chunk` and re-run unified
   format on just that vector (`:1308-1309`).
6. `sink_collection->AppendUnified(append_state, source_chunk,
   *current_sel, added_count)`. This is where rows actually get
   *serialized* (copied) into row-oriented buffer-managed storage,
   partitioned by hash. The append state caches a per-partition write
   buffer so high-throughput threads aren't lock-contending on every chunk.

So a single Sink call does: evaluate keys, reference payload, hash, append.
One pass over the input. The data is materialized at step 6, into
`sink_collection`'s tuple-data blocks. The output is row-oriented (each row
is contiguous in memory: `keyN | payloadM | found? | hash`).

### Phase 2 — `Combine` (one call per thread on finish)

`physical_hash_join.cpp:458-483`. The thread flushes its
`PartitionedTupleDataAppendState` (any in-flight partition buffers) into
its local `sink_collection`, then moves its whole local `JoinHashTable`
onto the global state's `local_hash_tables` vector. Combine also accumulates
the filter-pushdown statistics so the right/full outer found vector etc.
are merged.

After every thread has Combined, the global state owns N thread-local
JoinHashTables, each holding its own `sink_collection`.

### Phase 3 — `PrepareFinalize`

`physical_hash_join.cpp:563-578`. The global state asks every local for its
size, sums them up, and reserves the memory needed for (a) the merged
tuple data and (b) the pointer table (which is `~2 * num_rows * 8` bytes,
rounded to a power of two). If the total exceeds the reservation budget,
the join transitions to *external mode* (`external = true`), which causes
the probe side to also be radix-repartitioned and the join to be run one
partition at a time.

### Phase 4 — `Finalize` (parallel)

`physical_hash_join.cpp:607-641` schedules two events:

1. **`HashJoinTableInitEvent`** — N parallel tasks `memset` slices of the
   pointer table to zero. The pointer table is an array of
   `2^bits` entries, each 8 bytes (a tagged pointer for hash + next).
2. **`HashJoinFinalizeEvent`** — N parallel tasks call
   `JoinHashTable::Finalize(chunk_idx_from, chunk_idx_to, parallel)`
   (`join_hashtable.cpp:1636-1657`). Each task:
   - Iterates its assigned chunks in `data_collection` via
     `TupleDataChunkIterator`. The iterator yields the row pointers
     (`data_ptr_t`) for the rows in each chunk.
   - Loads the hash value out of the in-row layout (still at
     `pointer_offset` from the original Build step).
   - Calls `InsertHashes`, which computes `ht_offset = hash & bitmask`,
     CAS-installs the row into the directory slot, and if the slot was
     occupied, links the displaced entry into the chain by writing it back
     at `row_ptr + pointer_offset`. After this step, the hash slot in the
     row is overwritten with the next-pointer — which is fine because no
     one reads the hash from the row again; the directory carries the
     tag bits needed for probing.

Across the two finalize events, the data is read exactly once more (to
extract hashes) and the directory is filled in parallel.

### Counting passes over the build-side data

Stripping it down:

| Pass | What it does | When |
|------|--------------|------|
| (1) | Read chunk from `Scan`, push through `Filter` and `UseBF`; surviving rows get copied into `CreateBF`'s `ColumnDataCollection` | Pipeline B1 |
| (2) | Scan `ColumnDataCollection` to insert into the Bloom filter | `CreateBFFinalizeEvent` (still pipeline B1's tail) |
| (3) | Scan `ColumnDataCollection` again, this time emitted as chunks to `JoinHashTable::Build`; rows get re-serialized as row-oriented tuples into `sink_collection` | Pipeline B2 |
| (4) | Iterate `data_collection` to extract hashes and fill the directory | `HashJoinFinalizeEvent` |

So in this configuration (RPT+ BF in front of a hash-join build) the
build-side rows are read four times overall: once as columnar data flowing
into the BF buffer, once to populate the BF, once to copy into the
JoinHashTable's row store, and once to wire up the directory. Without
RPT+ (no `CreateBF`), passes (1) and (2) collapse and the data is copied
straight from the scan into the JoinHashTable.

### A note on `TupleDataCollection` vs the buffer manager

`TupleDataCollection` is not a separate caching layer — its blocks come
from the same `BufferManager` as everything else. Rows live in pinned
buffer-managed blocks while the build is in progress and unpinned blocks
that the buffer manager may evict to disk if the configured
`memory_limit` is exceeded. The `temporary_memory_state` machinery you'll
see referenced in `Sink` / `Combine` / `PrepareFinalize` is how the
operator negotiates with the buffer manager for its share.

---

## Q5 — Why is `CreateBF + UseBF + BuildHT` faster than `BuildHT` alone, despite doing more passes?

The pass count in the table above is honest — RPT+ does add two passes over
the build-side data (one columnar buffer fill, one BF-insert scan). The
speedup must come from elsewhere. It comes from **row-count reduction at the
expensive operators**, where each row pruned by `UseBF` saves work that is
strictly larger than the BF lookup cost.

### What every pruned row would have cost

Consider a build-side row whose key is not in any other table that this
join feeds. Without `UseBF`, that row still goes through the full
hash-join build pipeline:

1. **Key expression evaluation** in `PhysicalHashJoin::Sink`
   (`physical_hash_join.cpp:427`) — every join key expression is evaluated
   per input row.
2. **Hashing** in `JoinHashTable::Build`
   (`src/execution/join_hashtable.cpp:1240`) — `VectorOperations::Hash`
   on the first key column plus a `CombineHash` for every additional key.
3. **NULL filtering** (`PrepareKeys`) — builds a survivor selection vector.
4. **Row-oriented serialization** into `sink_collection` via
   `sink_collection->AppendUnified(...)`. This is the expensive write on
   the build side: a real copy of `[key1..keyN, payload1..payloadM,
   (found?), hash]` into buffer-managed row blocks, partitioned by hash.
   **Payload bytes get copied here, even though they were never touched
   inside `UseBF`.**
5. **Directory CAS** in `JoinHashTable::Finalize`
   (`src/execution/join_hashtable.cpp:1636`) — one atomic compare-and-swap
   per row to install it in the pointer table, plus a chain-link write if
   the slot was occupied.
6. **Probe-side work** — every probe-side row whose key happens to hash to
   the same directory slot pays for hashing this row's slot, the
   pointer-array lookup, and (on a hash-only false positive) a key
   comparison.

Pruning the row upstream skips **all six** of those.

### What the BF lookup actually costs per row

- One vectorized hash over the key columns
  (`BloomFilter::Lookup` → `HashColumns` → `BloomFilterLookup` in
  `bloom_filter.cpp:25-62`). Bandwidth-bound on the key columns only.
- A bit-test per row, written into `lookup_results[i]` as 0/1.
- One increment + one `sel.set_index` in the survivor-packing loop
  (`physical_use_bf.cpp:154-159`).
- For survivors, a logical slice (`chunk.Slice`, `:164`) that allocates no
  payload memory — just a `DictionaryVector` wrapping each input vector.

That's it. There is no payload read, no row write, no atomic operation.
The cost scales with `input.size() * (sizeof(key) + sizeof(bit_test))`,
not with payload width or row count downstream.

### Why "more passes" still wins

The two extra passes RPT+ adds:

- **Pass (1) — fill `CreateBF`'s `ColumnDataCollection`.** This appends
  only *post-UseBF survivors*: the same rows that would have been the
  hash-join's input anyway. The difference is that the columnar copy
  happens *here* instead of *inside `JoinHashTable::Build`*. So this pass
  is mostly amortized: it replaces one bulk copy with another.
- **Pass (2) — BF-insert scan in `CreateBFFinalizeTask`.** This reads
  only key columns from the `ColumnDataCollection` (
  `bf->Insert(chunk, cols_build)`) and does hash-and-set-bit work. Cheap
  per row; cheap in total because it scales with the post-filter row
  count, not the pre-filter row count.

Meanwhile the savings — across the build *and* probe sides — scale with
the *pruned* row count and include row serialization, hash-table
materialization, directory inserts, and every probe that would have
touched those rows. The same BF created here is consumed by a probe-side
`UseBF` too, and the probe side usually has more rows than the build
side, so the probe-side savings typically dominate.

### Cascading across joins

RPT+ builds a transfer graph across the whole query, not just one join.
Filtering one table early cascades: a smaller build side here means a
smaller hash table, which means cheaper probes, which means fewer rows
flowing into the next join's build side, where another `CreateBF` may be
running, and so on. The wins compound.

### Bounded worst case via `GiveUpBFCreation`

If upstream filtering is *not* selective — say `UseBF` lets 95% of rows
through — the BF cost-benefit math flips and the extra passes would just
be overhead. `PhysicalCreateBF::GiveUpBFCreation`
(`physical_create_bf.cpp:248`) measures `actual_rows /
estimated_rows_from_optimizer` after the first 32 chunks and aborts the
BF if the ratio is above 0.2 (or 0.35 in SQLStorm mode). When that
happens, `is_successful` flips to false, `Finalize` is a no-op, the BF
stays un-finalized, and the downstream `UseBF` degrades to a
`chunk.Reference` pass-through. So the worst case is bounded: at most ~64
K rows of wasted BF work plus the one columnar buffer copy.

### The one-line takeaway

**BF lookup is O(keys). Each row it prunes saves an O(keys + payload)
row materialization, a hash-directory insert, and all of the probe-side
work that row would have caused.** With a moderately selective filter
upstream and a non-trivial payload, that trade is heavily favorable —
which is why RPT+ wins despite "more passes" on paper.

---

## Summary

- The path `Scan → Filter → UseBF → CreateBF → BuildHashTable` is two
  build-side pipelines: one ends at `CreateBF`'s sink, the second starts
  at `CreateBF`'s source and ends at `JoinHashTable`'s sink. A third
  probe-side pipeline follows after the hash directory is finalized.
- `CreateBF` is a strict sink: rows are buffered in a per-thread
  `ColumnDataCollection`, then `Finalize` scans the merged buffer in
  parallel tasks and inserts into one or more `BlockedBloomFilter`s. The
  buffered rows are *re-emitted* via the operator's `Source` interface to
  feed the next pipeline; they are not abandoned after BF construction.
- `UseBF` is a streaming, no-copy filter: it asks the BF for a 0/1 per row
  on the key columns, builds a `SelectionVector` of survivors, and
  `Slice`s the input. Payload columns are never touched. A
  selectivity-tracking guard disables the BF lookup after 32 chunks if the
  filter isn't paying off.
- The hash-table build is one streaming pass per chunk: keys are
  expression-evaluated, payload is referenced, the chunk is hashed, and the
  result is serialized in row-oriented form into
  `RadixPartitionedTupleData`. After all threads `Combine`, a parallel
  `Finalize` walks the merged tuple data once more to populate the
  pointer-array directory and the chain links. The "pass count" cost of
  enabling RPT+ on top of a hash join is one extra columnar copy plus one
  extra pass to insert into the Bloom filter.
