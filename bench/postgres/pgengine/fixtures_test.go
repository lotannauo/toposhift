package pgengine_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
)

// The plans the server printed for the statements of every kind of read, recorded from a
// live server on a tiny synthetic plan (TestRecordExplainFixtures) and frozen: what the
// parser must read out of them. Each row was checked against the JSON by hand: the
// buffers are the top node's, and the entries are the rows an index scan handed to the
// executor, over the loops it ran.
var fixtureCounts = map[string]struct {
	hit, read, rows, entries, heap, filter, recheck int64
	shape                                           string
}{
	"pg-bulk_neighbors-forward_0":   {0, 7, 1, 1, 0, 0, 0, "Sort > Index Only Scan on edges using edges_src_g"},
	"pg-bulk_neighbors-reverse_0":   {0, 5, 11, 11, 0, 0, 0, "Sort > Index Only Scan on edges using edges_dst_g"},
	"pg-bulk_batch_0":               {20, 5, 35, 36, 0, 0, 0, "Incremental Sort > Nested Loop > Function Scan > Index Only Scan on edges using edges_dst_g"},
	"pg-bulk_alive_0":               {0, 5, 1, 1, 1, 0, 0, "Result > Index Only Scan on entities using entities_g"},
	"pg-bulk_window_0":              {59, 198, 926, 926, 0, 0, 0, "Index Scan on log using log_g_dst"},
	"pg-bulk_fold-neighbors_0":      {0, 1, 1, 0, 0, 0, 0, "Seq Scan on meta"},
	"pg-bulk_fold-neighbors_1":      {0, 192, 873, 836, 0, 451, 0, "Sort > Append > Bitmap Heap Scan on log > Bitmap Index Scan using log_g_dst > Seq Scan on log_base"},
	"pg-bulk_fold-alive_0":          {0, 1, 1, 0, 0, 0, 0, "Seq Scan on meta"},
	"pg-bulk_fold-alive_1":          {0, 61, 56, 57, 0, 1, 0, "Append > Bitmap Heap Scan on log > Bitmap Index Scan using log_e > Index Scan on log_base using log_base_subject"},
	"pg-bulk-b_neighbors-forward_0": {0, 3, 1, 1, 0, 0, 0, "Sort > Index Only Scan on edges using edges_src_b"},
	"pg-bulk-b_neighbors-reverse_0": {0, 3, 11, 11, 0, 0, 0, "Sort > Index Only Scan on edges using edges_dst_b"},
	"pg-bulk-b_batch_0":             {5, 8, 35, 36, 0, 0, 0, "Incremental Sort > Nested Loop > Function Scan > Index Only Scan on edges using edges_dst_b"},
	"pg-bulk-b_alive_0":             {0, 4, 1, 1, 1, 0, 0, "Result > Index Only Scan on entities using entities_b"},
	"pg-bulk-b_window_0":            {59, 198, 926, 926, 0, 0, 0, "Index Scan on log using log_g_dst"},
	"pg-bulk-b_fold-neighbors_0":    {0, 1, 1, 0, 0, 0, 0, "Seq Scan on meta"},
	"pg-bulk-b_fold-neighbors_1":    {0, 192, 873, 836, 0, 451, 0, "Sort > Append > Bitmap Heap Scan on log > Bitmap Index Scan using log_g_dst > Seq Scan on log_base"},
	"pg-bulk-b_fold-alive_0":        {0, 1, 1, 0, 0, 0, 0, "Seq Scan on meta"},
	"pg-bulk-b_fold-alive_1":        {0, 61, 56, 57, 0, 1, 0, "Append > Bitmap Heap Scan on log > Bitmap Index Scan using log_e > Index Scan on log_base using log_base_subject"},
}

// walk reads a plan as untyped JSON, which is what the parser must agree with.
type walked struct {
	nodes   int
	entries float64
	filter  float64
	heap    float64
}

func walk(n map[string]any, w *walked) {
	w.nodes++
	loops, _ := n["Actual Loops"].(float64)
	rows, _ := n["Actual Rows"].(float64)
	f, _ := n["Rows Removed by Filter"].(float64)
	r, _ := n["Rows Removed by Index Recheck"].(float64)
	w.filter += f * loops
	switch n["Node Type"] {
	case "Index Scan", "Index Only Scan", "Bitmap Heap Scan":
		w.entries += rows*loops + f*loops + r*loops
	}
	if n["Node Type"] == "Index Only Scan" {
		h, _ := n["Heap Fetches"].(float64)
		w.heap += h
	}
	if kids, ok := n["Plans"].([]any); ok {
		for _, k := range kids {
			walk(k.(map[string]any), w)
		}
	}
}

func TestExplainFixturesAreReadAsTheServerPrintedThem(t *testing.T) {
	files, err := filepath.Glob("../testdata/explain/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != len(fixtureCounts) {
		t.Errorf("%d fixtures, %d expectations", len(files), len(fixtureCounts))
	}
	for _, f := range files {
		name := filepath.Base(f)
		name = name[:len(name)-len(".json")]
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		got, err := pgengine.ParseExplain(raw)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		want, ok := fixtureCounts[name]
		if !ok {
			t.Errorf("%s: no expectation", name)
			continue
		}
		if got.SharedHit != want.hit || got.SharedRead != want.read || got.RowsReturned != want.rows || got.IndexEntries != want.entries ||
			got.HeapFetches != want.heap || got.RowsRemovedByFilter != want.filter || got.RowsRemovedByRecheck != want.recheck || got.Shape() != want.shape {
			t.Errorf("%s: %+v (%s)", name, got, got.Shape())
		}

		// And as an independent reading of the JSON says: the buffers are the root's,
		// the rows are the root's over its loops, each node counted once.
		var doc []struct{ Plan map[string]any }
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		root := doc[0].Plan
		var w walked
		walk(root, &w)
		hit, read := root["Shared Hit Blocks"].(float64), root["Shared Read Blocks"].(float64)
		rows, loops := root["Actual Rows"].(float64), root["Actual Loops"].(float64)
		if float64(got.SharedHit) != hit || float64(got.SharedRead) != read || float64(got.RowsReturned) != rows*loops {
			t.Errorf("%s: parsed %d hits, %d reads, %d rows; the root has %v, %v, %v x %v", name, got.SharedHit, got.SharedRead, got.RowsReturned, hit, read, rows, loops)
		}
		if len(got.Plan) != w.nodes || float64(got.IndexEntries) != w.entries || float64(got.RowsRemovedByFilter) != w.filter || float64(got.HeapFetches) != w.heap {
			t.Errorf("%s: parsed %d nodes, %d entries; a walk of the JSON finds %d, %v", name, len(got.Plan), got.IndexEntries, w.nodes, w.entries)
		}
		// A buffer count of a child is part of its parent's: the root's is at least any node's.
		var maxChild func(n map[string]any) float64
		maxChild = func(n map[string]any) float64 {
			m, _ := n["Shared Read Blocks"].(float64)
			if kids, ok := n["Plans"].([]any); ok {
				for _, k := range kids {
					m = max(m, maxChild(k.(map[string]any)))
				}
			}
			return m
		}
		if maxChild(root) > read {
			t.Errorf("%s: a node reads more blocks than the root, so the root is not the statement's", name)
		}
	}
	// The batch plans have a scan that ran once for every entity asked about, the case in
	// which a loop counted twice or not at all gives the wrong entries.
	for _, name := range []string{"pg-bulk_batch_0", "pg-bulk-b_batch_0"} {
		raw, _ := os.ReadFile("../testdata/explain/" + name + ".json")
		var doc []struct{ Plan map[string]any }
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatal(err)
		}
		var scan func(n map[string]any) bool
		scan = func(n map[string]any) bool {
			if l, _ := n["Actual Loops"].(float64); n["Node Type"] == "Index Only Scan" && l > 1 {
				return true
			}
			if kids, ok := n["Plans"].([]any); ok {
				for _, k := range kids {
					if scan(k.(map[string]any)) {
						return true
					}
				}
			}
			return false
		}
		if !scan(doc[0].Plan) {
			t.Errorf("%s: no scan ran more than once: the fixture no longer tests the loops", name)
		}
	}
}
