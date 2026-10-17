-- A sorted columnar archive of an activity file, made with DuckDB outside the
-- repository's build and tests. It is the reference "a write-once sorted file": the
-- records of the input in one total order, in one Parquet file with zstd, which takes
-- no append and no late event without a rewrite. Nothing here is linked, and nothing
-- runs in CI.
--
-- Usage (the variables are src, dst and tmp: DuckDB reserves in and out):
--
--   duckdb -c "SET VARIABLE src = '/path/to/activity.parquet'" \
--          -c "SET VARIABLE dst = '/path/to/archive.parquet'" \
--          -c "SET VARIABLE tmp = '/path/to/scratch-directory'" \
--          -c ".read archive.sql"
--
-- It prints the number of rows of the archive, which must be the input's.

SET threads = 1;
SET memory_limit = '4GB';
SET temp_directory = getvariable('tmp');
SET preserve_insertion_order = true;
COPY (SELECT * FROM read_parquet(getvariable('src'))
      ORDER BY layer, subject_kind, source, target, relation, producer, event_time_ns, seq)
  TO (getvariable('dst')) (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 1048576);
SELECT count(*) AS rows FROM read_parquet(getvariable('dst'));
