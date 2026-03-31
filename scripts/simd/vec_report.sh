#!/usr/bin/env bash
# Generate GCC auto-vectorization report for the TieredHashCache.
#
# Usage:
#   scripts/simd/vec_report.sh              # show missed + optimized for tiered_hash_cache.hpp
#   scripts/simd/vec_report.sh all          # show ALL vectorization notes (verbose)
#   scripts/simd/vec_report.sh missed       # only show missed (why loops were NOT vectorized)
#   scripts/simd/vec_report.sh optimized    # only show successfully vectorized loops
#
# The script re-compiles the single translation unit that contains the
# inlined THC code (ub_duckdb_execution.cpp) with the same flags used by
# the release build, plus GCC's -fopt-info-vec-* diagnostics.
# Output is filtered to lines mentioning tiered_hash_cache.hpp.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
cd "$REPO_ROOT"

MODE="${1:-missed}"
case "$MODE" in
    all)       OPT_FLAG="-fopt-info-vec-all" ;;
    missed)    OPT_FLAG="-fopt-info-vec-missed" ;;
    optimized) OPT_FLAG="-fopt-info-vec-optimized" ;;
    *)         echo "Usage: $0 [all|missed|optimized]"; exit 1 ;;
esac

SRC="build/release/src/execution/ub_duckdb_execution.cpp"
if [ ! -f "$SRC" ]; then
    echo "ERROR: $SRC not found. Run a release build first." >&2
    exit 1
fi

echo "=== Compiling with $OPT_FLAG (this takes ~15s) ==="

# Exact flags extracted from: ninja -C build/release -t commands <target>
/usr/bin/c++ \
  -DDUCKDB -DDUCKDB_BUILD_LIBRARY \
  -DDUCKDB_EXTENSION_CORE_FUNCTIONS_LINKED=1 \
  -DDUCKDB_EXTENSION_HTTPFS_LINKED=1 \
  -DDUCKDB_EXTENSION_JEMALLOC_LINKED=1 \
  -DDUCKDB_EXTENSION_PARQUET_LINKED=1 \
  -DDUCKDB_EXTENSION_TPCDS_LINKED=1 \
  -DDUCKDB_EXTENSION_TPCH_LINKED=1 \
  -DDUCKDB_MAIN_LIBRARY \
  -I src/include \
  -I third_party/fsst \
  -I third_party/fmt/include \
  -I third_party/hyperloglog \
  -I third_party/fastpforlib \
  -I third_party/skiplist \
  -I third_party/fast_float \
  -I third_party/re2 \
  -I third_party/miniz \
  -I third_party/utf8proc/include \
  -I third_party/concurrentqueue \
  -I third_party/pcg \
  -I third_party/tdigest \
  -I third_party/mbedtls/include \
  -I third_party/jaro_winkler \
  -I third_party/yyjson/include \
  -I third_party/zstd/include \
  -I extension \
  -I extension/tpch/include \
  -I extension/tpcds/include \
  -I build/release/_deps/httpfs_extension_fc-src/extension/httpfs/include \
  -I extension/core_functions/include \
  -I extension/parquet/include \
  -I extension/jemalloc/include \
  -O3 -DNDEBUG -std=c++11 -fPIC \
  "$OPT_FLAG" \
  -c "$SRC" -o /dev/null \
  2>&1 | grep 'tiered_hash_cache'

echo ""
echo "=== Done ==="
