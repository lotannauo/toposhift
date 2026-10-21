package pgengine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ExplainPrefix is put before a statement to have the server run it and report the
// work it did, in a form that does not depend on the machine: buffer counts and row
// counts, with no timings (TIMING OFF, SUMMARY OFF).
const ExplainPrefix = "EXPLAIN (ANALYZE, BUFFERS, TIMING OFF, SUMMARY OFF, FORMAT JSON) "

// Querier is what Explain runs a statement on: a connection, a pool or a
// transaction.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// PlanNode is one node of a plan: its type and, for a scan, the table and the index.
type PlanNode struct {
	Type     string
	Relation string `json:",omitempty"`
	Index    string `json:",omitempty"`
}

// Counters are the work an explained statement did.
//
// Buffer counts are the top node's, never a sum over the nodes: a node's counts
// include its children's, so the root already has the whole statement's. The row
// counts are sums over the nodes, because a statement has no row count of its own
// beyond the rows it returns. The server reports a node's rows and removed rows as an
// average over its loops, rounded to a whole number, so a figure for a node that ran
// more than once is exact only to within half a row per loop.
type Counters struct {
	// SharedHit and SharedRead are the shared buffers the statement found in the
	// cache and the ones it had to read.
	SharedHit, SharedRead int64
	// RowsReturned is what the statement returned: the top node's rows over its loops.
	RowsReturned int64
	// IndexEntries is the tuples the index scans handed to the executor: for an
	// Index Scan or Index Only Scan, the rows it returned and the rows its filter and
	// recheck removed; for a bitmap path, the same for its Bitmap Heap Scan, since the
	// server counts no entries for the Bitmap Index Scan itself.
	IndexEntries int64
	// HeapFetches is the heap visits of Index Only Scans that the visibility map did
	// not spare.
	HeapFetches int64
	// RowsRemovedByFilter and RowsRemovedByRecheck are the rows scans threw away.
	RowsRemovedByFilter, RowsRemovedByRecheck int64
	// Plan is the nodes in the order of a depth-first walk.
	Plan []PlanNode
}

// Shape is the plan as one line: the nodes, with the relation and the index of each
// scan. Two runs of one statement have one shape unless the planner chose differently.
func (c Counters) Shape() string {
	parts := make([]string, len(c.Plan))
	for i, n := range c.Plan {
		parts[i] = n.Type
		if n.Relation != "" {
			parts[i] += " on " + n.Relation
		}
		if n.Index != "" {
			parts[i] += " using " + n.Index
		}
	}
	return strings.Join(parts, " > ")
}

// explainNode is the part of a plan node of the server's JSON that is read.
type explainNode struct {
	NodeType       string        `json:"Node Type"`
	Relation       string        `json:"Relation Name"`
	Index          string        `json:"Index Name"`
	ActualRows     float64       `json:"Actual Rows"`
	ActualLoops    float64       `json:"Actual Loops"`
	SharedHit      int64         `json:"Shared Hit Blocks"`
	SharedRead     int64         `json:"Shared Read Blocks"`
	HeapFetches    int64         `json:"Heap Fetches"`
	RemovedFilter  float64       `json:"Rows Removed by Filter"`
	RemovedRecheck float64       `json:"Rows Removed by Index Recheck"`
	Plans          []explainNode `json:"Plans"`
}

// ParseExplain reads the JSON that [ExplainPrefix] makes the server print.
func ParseExplain(raw []byte) (Counters, error) {
	var doc []struct {
		Plan *explainNode `json:"Plan"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return Counters{}, fmt.Errorf("pgengine: reading an EXPLAIN result: %w", err)
	}
	if len(doc) != 1 || doc[0].Plan == nil {
		return Counters{}, errors.New("pgengine: an EXPLAIN result must be an array of one object with a Plan")
	}
	root := doc[0].Plan
	if root.NodeType == "" {
		return Counters{}, errors.New("pgengine: the top node of an EXPLAIN result has no Node Type")
	}
	c := Counters{
		SharedHit: root.SharedHit, SharedRead: root.SharedRead,
		RowsReturned: total(root.ActualRows, root.ActualLoops),
	}
	var walk func(n *explainNode)
	walk = func(n *explainNode) {
		c.Plan = append(c.Plan, PlanNode{Type: n.NodeType, Relation: n.Relation, Index: n.Index})
		removedFilter := total(n.RemovedFilter, n.ActualLoops)
		removedRecheck := total(n.RemovedRecheck, n.ActualLoops)
		c.RowsRemovedByFilter += removedFilter
		c.RowsRemovedByRecheck += removedRecheck
		switch n.NodeType {
		case "Index Scan", "Index Only Scan", "Bitmap Heap Scan":
			c.IndexEntries += total(n.ActualRows, n.ActualLoops) + removedFilter + removedRecheck
		}
		if n.NodeType == "Index Only Scan" {
			c.HeapFetches += n.HeapFetches
		}
		for i := range n.Plans {
			walk(&n.Plans[i])
		}
	}
	walk(root)
	return c, nil
}

// total is a node's per-loop figure over all its loops.
func total(perLoop, loops float64) int64 { return int64(math.Round(perLoop * loops)) }

// Explain runs the statement under [ExplainPrefix] and returns the work it did. The
// statement runs: an EXPLAIN ANALYZE of a write writes.
func Explain(ctx context.Context, q Querier, sql string, args ...any) (Counters, error) {
	var raw []byte
	if err := q.QueryRow(ctx, ExplainPrefix+sql, args...).Scan(&raw); err != nil {
		return Counters{}, fmt.Errorf("pgengine: explaining: %w", err)
	}
	return ParseExplain(raw)
}
