-- An independent check of the reducer, in SQL, outside the repository's build and tests.
--
-- It re-derives, from the CSV files of the archives and by its own queries, the
-- numbers the reducer wrote, and prints one line per comparison with ok = true
-- where they are equal. It shares no code with the reducer. DuckDB is an external
-- tool: nothing here is linked, and nothing runs in CI.
--
-- Usage (the archives' CSV files extracted to a scratch directory, MSMetricsUpdate_0.csv
-- and so on; the reducer's result directory is the -out directory of its run):
--
--   duckdb -c "SET VARIABLE dir='/tmp/alibaba-reduce-csv'" \
--          -c "SET VARIABLE res='/path/to/result-directory'" \
--          -c ".read check.sql"
--
-- Optional variables, set the same way before the .read:
--   per_file       true if the archives' timestamps restart at zero (default false)
--   placement_span minutes of samples in one placement archive (default 30)
--   call_span_ms   milliseconds of calls in one call archive (default 180000)
--   epoch_ns       the reducer's epoch in Unix nanoseconds (default 2022-01-01T00:00:00Z)
--   hour_minutes   the placement hour in minutes (default 60)
--   call_hour_minutes the calls' hour in minutes (default 60)
--
-- It reads the lines of each file as text and splits them at commas, so it assumes
-- no field is quoted; the first check counts the lines that hold a quote and must be 0.
-- A distinct count of traces is exact here, and the reducer's is by 64-bit hash: a
-- difference in that column alone is a hash collision, and is reported as such.

SET memory_limit = '4GB';
SET threads = 4;
SET preserve_insertion_order = false;

SET VARIABLE per_file = coalesce(getvariable('per_file'), false);
SET VARIABLE placement_span = coalesce(getvariable('placement_span'), 30);
SET VARIABLE call_span_ms = coalesce(getvariable('call_span_ms'), 180000);
SET VARIABLE epoch_ns = coalesce(getvariable('epoch_ns'), 1640995200000000000);
SET VARIABLE hour_minutes = coalesce(getvariable('hour_minutes'), 60);
SET VARIABLE call_hour_minutes = coalesce(getvariable('call_hour_minutes'), 60);

-- The rules of the reducer, stated again.
CREATE OR REPLACE TEMP MACRO bad_id(x) AS (
  x = '' OR trim(x) = '' OR length(x) > 4096 OR regexp_matches(x, '[\x00-\x1f\x7f]')
);
CREATE OR REPLACE TEMP MACRO unknown_service(x) AS (x IN ('UNKNOWN', 'UNAVAILABLE'));
CREATE OR REPLACE TEMP MACRO unknown_instance(x) AS (
  x = '' OR x IN ('UNKNOWN', 'UNAVAILABLE') OR starts_with(x, 'UNKNOWN_POD_')
);

-- Placement: one text field per line, split at commas.
CREATE OR REPLACE TEMP TABLE pl_lines AS
SELECT regexp_extract(filename, 'MSMetricsUpdate_([0-9]+)\.csv$', 1)::INTEGER AS file,
       string_split(l, ',') AS f
FROM read_csv(getvariable('dir') || '/MSMetricsUpdate_*.csv', header = false, skip = 1,
              columns = {'l': 'VARCHAR'}, delim = chr(1), quote = '', escape = '', filename = true,
              strict_mode = false);

CREATE OR REPLACE TEMP TABLE pl AS
SELECT file, f,
  CASE WHEN len(f) <> 6 THEN 'columns'
       WHEN NOT regexp_full_match(f[1], '[0-9]{1,15}') THEN 'timestamp'
       WHEN bad_id(f[2]) OR bad_id(f[3]) OR bad_id(f[4]) THEN 'empty_id'
       ELSE 'used' END AS fate
FROM pl_lines;

CREATE OR REPLACE TEMP TABLE pl_used AS
SELECT file,
       f[3] AS inst, f[4] AS node, f[2] AS name,
       (f[1]::BIGINT) // 60000 + CASE WHEN getvariable('per_file') THEN file * getvariable('placement_span') ELSE 0 END AS m
FROM pl WHERE fate = 'used';

-- Calls.
CREATE OR REPLACE TEMP TABLE cg_lines AS
SELECT regexp_extract(filename, 'CallGraph_([0-9]+)\.csv$', 1)::INTEGER AS file,
       string_split(l, ',') AS f
FROM read_csv(getvariable('dir') || '/CallGraph_*.csv', header = false, skip = 1,
              columns = {'l': 'VARCHAR'}, delim = chr(1), quote = '', escape = '', filename = true,
              strict_mode = false);

CREATE OR REPLACE TEMP TABLE cg AS
SELECT file, f,
  CASE WHEN len(f) <> 11 THEN 'columns'
       WHEN f[5] NOT IN ('rpc', 'http', 'mc', 'db', 'mq', 'UNKNOWN') THEN 'shifted'
       WHEN NOT regexp_full_match(f[1], '[0-9]{1,15}') THEN 'timestamp'
       WHEN bad_id(f[6]) OR bad_id(f[9]) THEN 'empty_id'
       WHEN unknown_service(f[6]) OR unknown_service(f[9]) OR unknown_instance(f[7]) OR unknown_instance(f[10]) THEN 'unknown_endpoint'
       ELSE 'used' END AS fate
FROM cg_lines;

CREATE OR REPLACE TEMP TABLE cg_used AS
SELECT file,
       f[2] AS trace, f[5] AS rpc, f[6] AS um, f[9] AS dm,
       (f[1]::BIGINT) // 60000 + CASE WHEN getvariable('per_file') THEN file * (getvariable('call_span_ms') // 60000) ELSE 0 END AS m,
       CASE WHEN regexp_full_match(f[11], '[0-9]{1,12}(\.[0-9]{1,3})?') THEN round(f[11]::DECIMAL(18, 3) * 1000)::BIGINT END AS rt_milli
FROM cg WHERE fate = 'used';

-- The reducer's own files.
CREATE OR REPLACE TEMP TABLE r_skips AS
SELECT * FROM read_csv(getvariable('res') || '/out/skips.tsv', delim = '\t', header = true, all_varchar = true);
CREATE OR REPLACE TEMP TABLE r_manifest AS SELECT * FROM read_json(getvariable('res') || '/manifest.json');
CREATE OR REPLACE TEMP TABLE r_params AS SELECT * FROM read_json(getvariable('res') || '/params.json');

CREATE OR REPLACE TEMP TABLE checks (name VARCHAR, sql_value VARCHAR, reducer_value VARCHAR);

-- 0. The assumption: no quoted field.
INSERT INTO checks SELECT 'lines holding a quote', (SELECT count(*) FROM pl_lines WHERE list_aggregate(f, 'string_agg', ',') LIKE '%"%')
  + (SELECT count(*) FROM cg_lines WHERE list_aggregate(f, 'string_agg', ',') LIKE '%"%'), '0';

-- 1. Rows per file, and the skips per reason.
INSERT INTO checks
SELECT 'rows ' || t || '_' || file, count(*)::VARCHAR,
       (SELECT value FROM r_skips s WHERE s.file = t || '/' || t || '_' || file || '.tar.gz' AND s.key = 'rows')
FROM (SELECT 'MSMetricsUpdate' AS t, file FROM pl UNION ALL SELECT 'CallGraph', file FROM cg)
GROUP BY t, file;

INSERT INTO checks
SELECT 'skips ' || a.t || '_' || a.file || ' ' || a.reason, a.n::VARCHAR, s.value
FROM (
  SELECT t, file, reason, count(*) FILTER (WHERE fate = reason) AS n
  FROM (SELECT 'MSMetricsUpdate' AS t, file, fate FROM pl UNION ALL SELECT 'CallGraph', file, fate FROM cg),
       (VALUES ('used'), ('columns'), ('shifted'), ('timestamp'), ('empty_id'), ('unknown_endpoint')) AS r(reason)
  GROUP BY t, file, reason) a
LEFT JOIN r_skips s ON s.file = a.t || '/' || a.t || '_' || a.file || '.tar.gz' AND s.key = a.reason;

-- 2. Distinct instances, nodes and services of the used placement rows.
INSERT INTO checks SELECT 'distinct instances', count(DISTINCT inst)::VARCHAR, (SELECT instances::VARCHAR FROM r_params) FROM pl_used;
INSERT INTO checks SELECT 'distinct nodes', count(DISTINCT node)::VARCHAR, (SELECT nodes::VARCHAR FROM r_params) FROM pl_used;
INSERT INTO checks SELECT 'distinct placement services', count(DISTINCT name)::VARCHAR, (SELECT placement_services::VARCHAR FROM r_params) FROM pl_used;

-- 3. Maximal intervals at C60 and C300: gaps and islands with lag(), a gap of more
--    than one TTL (three sampling intervals) splitting.

CREATE OR REPLACE TEMP TABLE edge_samples AS SELECT DISTINCT inst, node, m FROM pl_used;
CREATE OR REPLACE TEMP TABLE pod_samples AS SELECT DISTINCT inst, m FROM pl_used;
CREATE OR REPLACE TEMP TABLE node_samples AS SELECT DISTINCT node, m FROM pl_used;

CREATE OR REPLACE TEMP TABLE sql_edge_iv AS
SELECT cad, step, inst, node, isl, min(m) AS s, max(m) AS l, count(*) AS samples FROM (
  SELECT *, sum(CASE WHEN gap IS NULL OR gap > 3 * step THEN 1 ELSE 0 END) OVER (PARTITION BY cad, inst, node ORDER BY m) AS isl FROM (
    SELECT c.cad, c.step, e.inst, e.node, e.m, e.m - lag(e.m) OVER (PARTITION BY c.cad, e.inst, e.node ORDER BY e.m) AS gap
    FROM edge_samples e, (VALUES ('c60', 1), ('c300', 5)) AS c(cad, step) WHERE e.m % c.step = 0)) GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE sql_pod_iv AS
SELECT cad, inst, isl, min(m) AS s, max(m) AS l FROM (
  SELECT *, sum(CASE WHEN gap IS NULL OR gap > 3 * step THEN 1 ELSE 0 END) OVER (PARTITION BY cad, inst ORDER BY m) AS isl FROM (
    SELECT c.cad, c.step, e.inst, e.m, e.m - lag(e.m) OVER (PARTITION BY c.cad, e.inst ORDER BY e.m) AS gap
    FROM pod_samples e, (VALUES ('c60', 1), ('c300', 5)) AS c(cad, step) WHERE e.m % c.step = 0)) GROUP BY ALL;

CREATE OR REPLACE TEMP TABLE sql_node_iv AS
SELECT cad, node, isl, min(m) AS s, max(m) AS l FROM (
  SELECT *, sum(CASE WHEN gap IS NULL OR gap > 3 * step THEN 1 ELSE 0 END) OVER (PARTITION BY cad, node ORDER BY m) AS isl FROM (
    SELECT c.cad, c.step, e.node, e.m, e.m - lag(e.m) OVER (PARTITION BY c.cad, e.node ORDER BY e.m) AS gap
    FROM node_samples e, (VALUES ('c60', 1), ('c300', 5)) AS c(cad, step) WHERE e.m % c.step = 0)) GROUP BY ALL;

INSERT INTO checks SELECT 'maximal pod intervals c60', count(*) FILTER (WHERE cad = 'c60')::VARCHAR, (SELECT ratios.maximal_intervals_c60.pods::VARCHAR FROM r_manifest) FROM sql_pod_iv;
INSERT INTO checks SELECT 'maximal node intervals c60', count(*) FILTER (WHERE cad = 'c60')::VARCHAR, (SELECT ratios.maximal_intervals_c60.nodes::VARCHAR FROM r_manifest) FROM sql_node_iv;
INSERT INTO checks SELECT 'maximal pod-on-node intervals c60', count(*) FILTER (WHERE cad = 'c60')::VARCHAR, (SELECT ratios.maximal_intervals_c60.pod_on_node::VARCHAR FROM r_manifest) FROM sql_edge_iv;
INSERT INTO checks SELECT 'pod intervals c300', count(*) FILTER (WHERE cad = 'c300')::VARCHAR, (SELECT ratios.intervals_c300.pods::VARCHAR FROM r_manifest) FROM sql_pod_iv;
INSERT INTO checks SELECT 'node intervals c300', count(*) FILTER (WHERE cad = 'c300')::VARCHAR, (SELECT ratios.intervals_c300.nodes::VARCHAR FROM r_manifest) FROM sql_node_iv;
INSERT INTO checks SELECT 'pod-on-node intervals c300', count(*) FILTER (WHERE cad = 'c300')::VARCHAR, (SELECT ratios.intervals_c300.pod_on_node::VARCHAR FROM r_manifest) FROM sql_edge_iv;

-- The intervals table, row for row: pod, node, start, end, samples, censoring.
CREATE OR REPLACE TEMP TABLE r_iv AS
SELECT cadence AS cad, pod AS inst, node,
       ((start_ns::HUGEINT - getvariable('epoch_ns')) // 60000000000)::BIGINT AS s,
       ((end_ns::HUGEINT - getvariable('epoch_ns')) // 60000000000)::BIGINT AS e,
       samples::BIGINT AS samples, left_censored::INT AS lc, right_censored::INT AS rc
FROM read_csv(getvariable('res') || '/out/placement_intervals.tsv.gz', delim = '\t', header = true, all_varchar = true);

INSERT INTO checks
SELECT 'placement_intervals rows only in SQL', count(*)::VARCHAR, '0' FROM (
  SELECT cad, inst, node, s, l + 3 * step AS e, samples,
         (s = 0)::INT AS lc, (l + 3 * step > getvariable('hour_minutes'))::INT AS rc FROM sql_edge_iv
  EXCEPT SELECT cad, inst, node, s, e, samples, lc, rc FROM r_iv);
INSERT INTO checks
SELECT 'placement_intervals rows only in the reducer', count(*)::VARCHAR, '0' FROM (
  SELECT cad, inst, node, s, e, samples, lc, rc FROM r_iv
  EXCEPT SELECT cad, inst, node, s, l + 3 * step AS e, samples,
         (s = 0)::INT, (l + 3 * step > getvariable('hour_minutes'))::INT FROM sql_edge_iv);

-- Censored counts of the pod intervals at C60.
INSERT INTO checks SELECT 'left-censored pod intervals c60', count(*) FILTER (WHERE s = 0)::VARCHAR,
  (SELECT instance_lifetime_censoring.left_censored + instance_lifetime_censoring.both_censored FROM r_params) :: VARCHAR FROM sql_pod_iv WHERE cad = 'c60';
INSERT INTO checks SELECT 'right-censored pod intervals c60', count(*) FILTER (WHERE l + 3 > getvariable('hour_minutes'))::VARCHAR,
  (SELECT instance_lifetime_censoring.right_censored + instance_lifetime_censoring.both_censored FROM r_params) :: VARCHAR FROM sql_pod_iv WHERE cad = 'c60';

-- 4. Call rollups: one row per (minute, um, dm, rpctype).
CREATE OR REPLACE TEMP TABLE sql_rollup AS
SELECT m AS minute, um, dm, rpc AS rpctype, count(*) AS calls, count(rt_milli) AS rt_count,
       coalesce(sum(rt_milli), 0) AS rt_sum_milli, count(DISTINCT trace) AS traces
FROM cg_used GROUP BY ALL;
CREATE OR REPLACE TEMP TABLE r_rollup AS
SELECT minute::BIGINT AS minute, um, dm, rpctype, calls::BIGINT AS calls, rt_count::BIGINT AS rt_count,
       rt_sum_milli::BIGINT AS rt_sum_milli, traces::BIGINT AS traces
FROM read_csv(getvariable('res') || '/out/call_rollup_minutes.tsv.gz', delim = '\t', header = true, all_varchar = true);

INSERT INTO checks SELECT 'call rollup rows', (SELECT count(*) FROM sql_rollup)::VARCHAR, (SELECT count(*) FROM r_rollup)::VARCHAR;
INSERT INTO checks SELECT 'call rollup cells differing (calls, rt, sums)', count(*)::VARCHAR, '0' FROM (
  SELECT minute, um, dm, rpctype, calls, rt_count, rt_sum_milli FROM sql_rollup
  EXCEPT SELECT minute, um, dm, rpctype, calls, rt_count, rt_sum_milli FROM r_rollup);
INSERT INTO checks SELECT 'call rollup cells differing in distinct traces (hash collisions)', count(*)::VARCHAR, '0' FROM (
  SELECT minute, um, dm, rpctype, traces FROM sql_rollup EXCEPT SELECT minute, um, dm, rpctype, traces FROM r_rollup);
INSERT INTO checks SELECT 'distinct call services', (SELECT count(DISTINCT s) FROM (SELECT um AS s FROM cg_used UNION SELECT dm FROM cg_used))::VARCHAR,
  (SELECT call_services::VARCHAR FROM r_params);
INSERT INTO checks SELECT 'self-call rows', count(*)::VARCHAR, (SELECT ratios.self_call_rows::VARCHAR FROM r_manifest) FROM cg_used WHERE um = dm;

-- 5. Edge intervals: a minute with a call is an edge; a gap of more than 3 minutes splits.
CREATE OR REPLACE TEMP TABLE sql_edge_minutes AS SELECT DISTINCT um, dm, m FROM cg_used WHERE um <> dm;
INSERT INTO checks SELECT 'edge minutes', count(*)::VARCHAR, (SELECT ratios.call_edge_minutes::VARCHAR FROM r_manifest) FROM sql_edge_minutes;
CREATE OR REPLACE TEMP TABLE sql_call_iv AS
SELECT um, dm, isl, min(m) AS s, max(m) AS l, count(*) AS samples FROM (
  SELECT *, sum(CASE WHEN gap IS NULL OR gap > 3 THEN 1 ELSE 0 END) OVER (PARTITION BY um, dm ORDER BY m) AS isl FROM (
    SELECT *, m - lag(m) OVER (PARTITION BY um, dm ORDER BY m) AS gap FROM sql_edge_minutes)) GROUP BY ALL;
INSERT INTO checks SELECT 'maximal edge intervals', count(*)::VARCHAR, (SELECT ratios.call_edge_maximal_intervals::VARCHAR FROM r_manifest) FROM sql_call_iv;
CREATE OR REPLACE TEMP TABLE r_call_iv AS
SELECT um, dm, ((start_ns::HUGEINT - getvariable('epoch_ns')) // 60000000000)::BIGINT AS s,
       ((end_ns::HUGEINT - getvariable('epoch_ns')) // 60000000000)::BIGINT AS e, minutes::BIGINT AS samples,
       left_censored::INT AS lc, right_censored::INT AS rc
FROM read_csv(getvariable('res') || '/out/call_edges_intervals.tsv.gz', delim = '\t', header = true, all_varchar = true);
INSERT INTO checks SELECT 'edge interval rows differing', count(*)::VARCHAR, '0' FROM (
  (SELECT um, dm, s, l + 3 AS e, samples, (s = 0)::INT AS lc, (l + 3 > getvariable('call_hour_minutes'))::INT AS rc FROM sql_call_iv
   EXCEPT SELECT um, dm, s, e, samples, lc, rc FROM r_call_iv)
  UNION ALL
  (SELECT um, dm, s, e, samples, lc, rc FROM r_call_iv
   EXCEPT SELECT um, dm, s, l + 3, samples, (s = 0)::INT, (l + 3 > getvariable('call_hour_minutes'))::INT FROM sql_call_iv));

-- The verdict.
SELECT name, sql_value, reducer_value, (sql_value = reducer_value) AS ok FROM checks ORDER BY ok, name;
SELECT count(*) FILTER (WHERE sql_value <> reducer_value) AS differences, count(*) AS comparisons FROM checks;
