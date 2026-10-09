-- Round trip of the golden activity file through DuckDB.
--
-- Run it from the repository root:
--
--   duckdb -c ".read internal/replay/activity/testdata/duckdb_check.sql"
--
-- It shows what DuckDB makes of the file and checks that DuckDB can write the
-- same rows to a new Parquet file. The expected results are in the comments.

-- Columns and types. Expected, in this order:
--   seq            UBIGINT   (an unsigned 64-bit integer)
--   event_time_ns  BIGINT
--   layer          VARCHAR
--   subject_kind   VARCHAR
--   source         VARCHAR
--   target         VARCHAR   (null for an entity)
--   relation       VARCHAR   (null for an entity)
--   producer       VARCHAR
--   kind           VARCHAR
--   ttl_ns         BIGINT
--   through_ns     BIGINT    (null when the record is not a run)
--   payload        BLOB
--   boot           VARCHAR   (null unless the record is an observation of a host with a boot id)
--   event_time_basis INTEGER (an event time basis, 0 to 4: unknown, object_field, observed, receipt, producer_event)
DESCRIBE SELECT * FROM 'internal/replay/activity/testdata/activity_v1.parquet';

-- The key-value metadata of the footer. Expected keys: toposhift.activity.version
-- (1), .records (14), .min_seq (1), .max_seq (18446744073709551615),
-- .group_digests (two digests of 64 hex digits, separated by a comma) and .digest
-- (sha256: and 64 hex digits; the pinned values are goldenGroupDigests and
-- goldenDigest in golden_test.go).
SELECT key, value FROM parquet_kv_metadata('internal/replay/activity/testdata/activity_v1.parquet');

-- Row count and sequence range. Expected: 14, 1, 18446744073709551615.
SELECT count(*) AS records, min(seq) AS min_seq, max(seq) AS max_seq
FROM 'internal/replay/activity/testdata/activity_v1.parquet';

-- The rows by seq, with the event time shown as a timestamp. The column
-- event_time_ns stays an integer in the file so that no reader loses
-- nanoseconds; a reader converts it with its own function.
-- Where this DuckDB version has no make_timestamp_ns, replace it with
-- to_timestamp(event_time_ns / 1e9), which keeps microseconds only.
SELECT seq, make_timestamp_ns(event_time_ns) AS event_time, layer, subject_kind, source, target, relation,
       producer, kind, ttl_ns, through_ns, length(payload) AS payload_bytes, boot, event_time_basis
FROM 'internal/replay/activity/testdata/activity_v1.parquet'
ORDER BY seq;

-- The event time bases. Expected: 0 for 3 rows, 1 for 3, 2 for 3, 3 for 3, 4 for 2.
SELECT event_time_basis, count(*) AS records FROM 'internal/replay/activity/testdata/activity_v1.parquet'
GROUP BY event_time_basis ORDER BY event_time_basis;

-- The boot ids. Expected: two rows, seq 2 with boot 6f1c2d3e-0a4b-4c5d-8e9f-0123456789ab
-- and seq 9223372036854775813 with boot ブート-2; every other row has a null boot.
SELECT seq, boot FROM 'internal/replay/activity/testdata/activity_v1.parquet'
WHERE boot IS NOT NULL ORDER BY seq;

-- The golden file has a record at the latest representable event time,
-- event_time_ns = 9223372036854775807 (INT64 max). DuckDB may display it as
-- infinity in its timestamp conversion above; that is expected and not a fault
-- of the file.

-- DuckDB writes the same rows to a new file, and the two files hold the same
-- rows. Expected: 0 and 0.
COPY (SELECT * FROM 'internal/replay/activity/testdata/activity_v1.parquet')
TO '/tmp/toposhift_activity_copy.parquet' (FORMAT parquet);

SELECT count(*) AS only_in_the_original FROM (
    SELECT * FROM 'internal/replay/activity/testdata/activity_v1.parquet'
    EXCEPT
    SELECT * FROM '/tmp/toposhift_activity_copy.parquet'
);

SELECT count(*) AS only_in_the_copy FROM (
    SELECT * FROM '/tmp/toposhift_activity_copy.parquet'
    EXCEPT
    SELECT * FROM 'internal/replay/activity/testdata/activity_v1.parquet'
);
