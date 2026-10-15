package rockskv

// #include <stdlib.h>
// #include "rocksdb/c.h"
//
// static const char* toposhift_cc_version(void) {
// #ifdef __VERSION__
// 	return __VERSION__;
// #else
// 	return "unknown";
// #endif
// }
import "C"

// headerPerfIDs are the ids of the perf metrics RL reports, as the header this
// package is compiled against defines them.
func headerPerfIDs() map[string]int {
	return map[string]int{
		"block_cache_hit_count": int(C.rocksdb_block_cache_hit_count),
		"block_read_count":      int(C.rocksdb_block_read_count),
		"block_read_byte":       int(C.rocksdb_block_read_byte),
		"internal_key_skipped":  int(C.rocksdb_internal_key_skipped_count),
	}
}

// ccVersion is the version of the C compiler cgo compiled this package with.
func ccVersion() string { return C.GoString(C.toposhift_cc_version()) }
