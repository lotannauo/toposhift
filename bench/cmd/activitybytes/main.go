// Command activitybytes weighs an activity file in the store that ships. It writes
// the file's records to a new internal/store/pebblestore store opened with its
// default options (the benchmark tuning, the boot key of the default lifecycle policy,
// the default checkpoint policy, no retention), brings the store to rest, and reads
// how many bytes its tables hold.
//
//	activitybytes -in placement_c60.parquet -out DIR -label C60/C -expect-records N -samples M
//	activitybytes -empty -out DIR -label E
//
// The records are written in the file's order, in batches, without syncing the log
// (which changes no table byte). The store is then compacted (CompactAll) and
// rewritten into tables that are a function of the data alone (Canonicalize), so the
// bytes it reports do not depend on how fast the compactions ran. Only bytes and
// counts are measured: no read and no time. With -empty nothing is written, and the
// store is weighed the same way: the fixed overhead of a store with no data.
//
// Under the default boot key the checkpoint policy places no checkpoint on the prefix
// of a pod or a node, only on those of edges. -checkpoints off writes none.
//
// One line of tab-separated values goes to standard output (with -header, after a
// header line), and DIR receives LABEL.json (with / in the label written as _), which
// holds the same numbers and what they were taken from. Neither is written unless the
// whole measurement succeeded. A failed measurement exits non-zero with no row: a
// record count that differs from the file's footer or from -expect-records, a store
// that holds blob files (its live table bytes would then leave values out), or any
// error of the store. Bytes written and the other counters of the engine are printed
// to standard error as local diagnostics: they depend on how the compactions ran, and
// are not part of the result.
//
// DIR is never inside a git work tree. The store is made in DIR and removed afterwards
// unless -keep. Arrow is reached only through internal/replay/activity, which checks
// the file against its footer as it reads it.
//
// The sorted columnar archive the results are compared with is made by
// testdata/archive.sql in DuckDB, outside this command and outside the build.
package main

import "os"

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }
