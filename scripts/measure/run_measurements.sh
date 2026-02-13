build/release/duckdb -f scripts/measure/0_cold.sql
build/release/duckdb -f scripts/measure/1_cold_interleaved.sql
build/release/duckdb -f scripts/measure/1_cold_segmented.sql
build/release/duckdb -f scripts/measure/10_cold_interleaved.sql
build/release/duckdb -f scripts/measure/10_cold_segmented.sql
build/release/duckdb -f scripts/measure/100_cold_interleaved.sql
build/release/duckdb -f scripts/measure/100_cold_segmented.sql