package pgengine_test

import (
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
)

// The server's JSON for a statement that looks up a batch of entities: a nested loop
// whose inner side is an index scan that ran once per entity. A node's buffer counts
// include its children's, which is why the root's are the statement's. The figures
// are made up to tell the readings apart.
const nestedLoop = `[
  {
    "Plan": {
      "Node Type": "Sort",
      "Parallel Aware": false,
      "Actual Rows": 12,
      "Actual Loops": 1,
      "Shared Hit Blocks": 100,
      "Shared Read Blocks": 7,
      "Shared Dirtied Blocks": 0,
      "Plans": [
        {
          "Node Type": "Nested Loop",
          "Parent Relationship": "Outer",
          "Join Type": "Left",
          "Actual Rows": 12,
          "Actual Loops": 1,
          "Shared Hit Blocks": 100,
          "Shared Read Blocks": 7,
          "Plans": [
            {
              "Node Type": "Function Scan",
              "Parent Relationship": "Outer",
              "Function Name": "unnest",
              "Actual Rows": 4,
              "Actual Loops": 1,
              "Shared Hit Blocks": 0,
              "Shared Read Blocks": 0
            },
            {
              "Node Type": "Index Scan",
              "Parent Relationship": "Inner",
              "Relation Name": "edges",
              "Index Name": "edges_src_g",
              "Actual Rows": 2,
              "Actual Loops": 4,
              "Rows Removed by Filter": 1,
              "Rows Removed by Index Recheck": 0,
              "Shared Hit Blocks": 60,
              "Shared Read Blocks": 7
            }
          ]
        }
      ]
    }
  }
]`

func TestExplainCountersComeFromTheTopNodeAndTheRowsFromEveryNode(t *testing.T) {
	c, err := pgengine.ParseExplain([]byte(nestedLoop))
	if err != nil {
		t.Fatal(err)
	}
	// Buffers: the root's 100 and 7, not the sum over the three nodes below it.
	if c.SharedHit != 100 || c.SharedRead != 7 {
		t.Errorf("shared hit %d, read %d; the root's are 100 and 7", c.SharedHit, c.SharedRead)
	}
	if c.RowsReturned != 12 {
		t.Errorf("rows returned %d, want the root's 12", c.RowsReturned)
	}
	// The inner scan ran 4 times and, per loop, returned 2 rows and removed 1: 8 and 4.
	// Its entries are what it handed to the executor, the rows it returned and the rows
	// its filter removed, over its loops: 12, not 3 (one loop), nor 48 (loops twice).
	if c.IndexEntries != 12 {
		t.Errorf("index entries %d, want (2 + 1) x 4 = 12", c.IndexEntries)
	}
	if c.RowsRemovedByFilter != 4 || c.RowsRemovedByRecheck != 0 || c.HeapFetches != 0 {
		t.Errorf("removed by filter %d, by recheck %d, heap fetches %d", c.RowsRemovedByFilter, c.RowsRemovedByRecheck, c.HeapFetches)
	}
	want := "Sort > Nested Loop > Function Scan > Index Scan on edges using edges_src_g"
	if c.Shape() != want {
		t.Errorf("shape %q, want %q", c.Shape(), want)
	}
}

func TestExplainCountsIndexOnlyAndBitmapPaths(t *testing.T) {
	const indexOnly = `[{"Plan": {"Node Type": "Index Only Scan", "Relation Name": "edges", "Index Name": "edges_src_b",
		"Actual Rows": 5, "Actual Loops": 1, "Heap Fetches": 3, "Rows Removed by Filter": 2,
		"Shared Hit Blocks": 4, "Shared Read Blocks": 1}}]`
	c, err := pgengine.ParseExplain([]byte(indexOnly))
	if err != nil {
		t.Fatal(err)
	}
	if c.IndexEntries != 7 || c.HeapFetches != 3 || c.SharedHit != 4 || c.SharedRead != 1 || c.RowsReturned != 5 {
		t.Errorf("index only scan: %+v", c)
	}

	// The server counts no entries for a Bitmap Index Scan, so the heap scan above it
	// is what is read; its recheck removals are counted.
	const bitmap = `[{"Plan": {"Node Type": "Bitmap Heap Scan", "Relation Name": "edges", "Actual Rows": 3, "Actual Loops": 1,
		"Rows Removed by Index Recheck": 4, "Shared Hit Blocks": 9, "Shared Read Blocks": 2,
		"Plans": [{"Node Type": "Bitmap Index Scan", "Index Name": "edges_src_g", "Actual Rows": 0, "Actual Loops": 1,
		"Shared Hit Blocks": 5, "Shared Read Blocks": 2}]}}]`
	c, err = pgengine.ParseExplain([]byte(bitmap))
	if err != nil {
		t.Fatal(err)
	}
	if c.IndexEntries != 7 || c.RowsRemovedByRecheck != 4 || c.SharedHit != 9 || c.SharedRead != 2 {
		t.Errorf("bitmap path: %+v", c)
	}
	if want := "Bitmap Heap Scan on edges > Bitmap Index Scan using edges_src_g"; c.Shape() != want {
		t.Errorf("shape %q, want %q", c.Shape(), want)
	}
}

func TestAMalformedExplainResultIsRefused(t *testing.T) {
	for name, raw := range map[string]string{
		"empty":               ``,
		"not json":            `Seq Scan on edges`,
		"an object":           `{"Plan": {"Node Type": "Result"}}`,
		"an empty array":      `[]`,
		"two statements":      `[{"Plan": {"Node Type": "Result"}}, {"Plan": {"Node Type": "Result"}}]`,
		"no plan":             `[{"Planning": {}}]`,
		"a plan with no type": `[{"Plan": {"Actual Rows": 1}}]`,
		"a text result":       `["Result  (cost=0.00..0.01 rows=1 width=4)"]`,
	} {
		if _, err := pgengine.ParseExplain([]byte(raw)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
