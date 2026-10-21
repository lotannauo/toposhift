package xtengine

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// TestMain sets how many random workloads rapid runs, as the spike's own candidates
// do: a few by default, TOPOSHIFT_RAPID_CHECKS for more, and a -rapid.checks on the
// command line wins.
func TestMain(m *testing.M) {
	if err := flag.Set("rapid.checks", conformance.RapidChecks("4")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

// openFake opens an engine over a fake node.
func openFake(t testing.TB, node *fakeNode, mut mutation) *Engine {
	t.Helper()
	names, err := namesFor("")
	if err != nil {
		t.Fatal(err)
	}
	e, err := openWith(context.Background(), Config{}, names, node.conn("w"), node.conn("r"))
	if err != nil {
		t.Fatal(err)
	}
	e.mut = mut
	// What the engine asked at the start, to find what an earlier engine left, is not
	// what the tests look at.
	node.statements = make(map[string][]loggedStmt)
	return e
}

// fakeFactory opens the node a directory names, so that an engine closed and opened
// again over the same directory finds what it left.
func fakeFactory(mut mutation) conformance.Factory {
	var mu sync.Mutex
	nodes := make(map[string]*fakeNode)
	return func(dir string) (engine.Engine, error) {
		mu.Lock()
		node := nodes[dir]
		if node == nil {
			node = newFakeNode()
			nodes[dir] = node
		}
		mu.Unlock()
		names, err := namesFor("")
		if err != nil {
			return nil, err
		}
		e, err := openWith(context.Background(), Config{}, names, node.conn("w"), node.conn("r"))
		if err != nil {
			return nil, err
		}
		e.mut = mut
		return e, nil
	}
}

// runChecks is the spike's conformance test for an engine whose temporal columns hold
// microseconds: [conformance.Run], less the one check that writes records which are not
// on a whole microsecond (CheckExtremes: its last record is at engine.MaxEventTime, which
// is 2262-04-11T23:47:16.854775807Z). TestRefusesWhatXTDBCannotHold covers that refusal.
// full adds the reopen check and the concurrent-read check, and every workload and
// retention schedule; without it a workload and the scripted checks run, as in a
// trimmed run.
func runChecks(t *testing.T, newEngine conformance.Factory, full bool) {
	t.Helper()
	open := func(t *testing.T) engine.Engine {
		e, err := newEngine(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = e.Close() })
		return e
	}
	if full {
		t.Run("reopen", func(t *testing.T) {
			if err := conformance.CheckReopen(newEngine); err != nil {
				t.Fatal(err)
			}
		})
		t.Run("concurrent reads", func(t *testing.T) {
			if err := conformance.CheckConcurrentReads(open(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
	cfgs := conformance.Configs()
	retains := [][]float64{{0.3, 0.7}}
	which := []int{len(cfgs) - 1}
	if full {
		retains = [][]float64{nil, {0.5}, {0.3, 0.7}}
		which = which[:0]
		for i := range cfgs {
			which = append(which, i)
		}
	}
	for _, i := range which {
		for _, retain := range retains {
			t.Run(fmt.Sprintf("config %d retain %v", i, retain), func(t *testing.T) {
				if err := conformance.Check(open(t), cfgs[i], conformance.Options{RetainAt: retain}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
	for _, c := range []struct {
		name  string
		check func(engine.Engine) error
	}{
		{"write contract", conformance.CheckWriteContract},
		{"read contract", conformance.CheckReadContract},
		{"instant", conformance.CheckInstant},
		{"producers", conformance.CheckProducers},
		{"relations", conformance.CheckRelations},
	} {
		t.Run(c.name, func(t *testing.T) {
			if err := c.check(open(t)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestConformanceOnAFakeNode is the engine contract, on the fake node: the engine is
// checked against the reference engine on every fixed workload with and without
// mid-stream retentions, on the scripted checks, on reopens and on concurrent reads,
// with no server. What it shows is that the engine sends the statements it should and
// reads them back as it should, under the semantics the fake has (those the live
// server is tested for).
func TestConformanceOnAFakeNode(t *testing.T) {
	runChecks(t, fakeFactory(0), !testing.Short())
}
