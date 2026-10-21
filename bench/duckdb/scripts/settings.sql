-- The settings of the DuckDB baseline: the one source of every setting a read or a
-- build runs under.
--
-- duckengine applies each statement when it opens a connection and then asks
-- duckdb_settings() for the value of every name below, refusing the connection if any
-- differs. A line is one SET statement; the value DuckDB prints for the setting is
-- the text after the dashes when it is not the literal itself (DuckDB prints a memory
-- limit of 4GB as 3.7 GiB). Nothing else belongs here: not a statement that makes a
-- table, not a setting two statements make, not a comment inside a statement.
--
-- This is the regime in which counters are taken: one thread, so that every scan is
-- one reader's and the counters do not depend on how a scan was split. A regime for
-- timings is a second file, written when timings are.
--
-- Not here, because it depends on where the run is and not on what it is asked to do:
-- temp_directory, which a run puts under its own directory. A build also sets
-- preserve_insertion_order to true, as the statement that made the archive did; see
-- duckengine.ModeBuild.

-- Memory: one limit for the whole process, and the one the archive was made under.
SET memory_limit = '4GB'; -- 3.7 GiB
SET threads = 1;

-- Reads need no insertion order; with one thread it changes no answer, and the
-- answers are ordered by the adapter in any case.
SET preserve_insertion_order = false;

-- Parquet footers and file contents are cached once a file has been read. The counters
-- that depend on a cache (bytes read by the profiler) are not counters; see
-- duckengine.Profile.
SET enable_object_cache = true;
SET parquet_metadata_cache = true;
SET enable_external_file_cache = true;

-- No network: nothing is installed or loaded from outside.
SET autoinstall_known_extensions = false;
SET autoload_known_extensions = false;
SET allow_community_extensions = false;
