package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// variant is a way of opening pebblestore: the checkpoint options and the
// retention mode.
type variant struct {
	name string
	ck   pebblestore.CheckpointOptions
	mode pebblestore.RetentionMode
}

// variants keep the checkpoint gaps small, so the seed walks stop at
// checkpoints, and retain both ways: a Background pass may not have reached a
// prefix when it is read.
func variants() []variant {
	return []variant{
		{"a checkpoint per record, synchronous", pebblestore.CheckpointOptions{On: true, KMin: 1}, pebblestore.Synchronous},
		{"k8 with a lag, background", pebblestore.CheckpointOptions{On: true, KMin: 8, Alpha: 1, Lag: time.Minute}, pebblestore.Background},
		{"the default, synchronous", pebblestore.DefaultCheckpoints(), pebblestore.Synchronous},
	}
}

// openPeb opens an empty pebblestore in memory, with the tiny tuning, under
// policy p.
func openPeb(tb testing.TB, p lifecycle.Policy, ck pebblestore.CheckpointOptions, mode pebblestore.RetentionMode) *pebblestore.Store {
	tb.Helper()
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync, o.SettleRetention = false, false
	o.FS = vfs.NewMem()
	o.Policy, o.Retention = p, mode
	o.Checkpoints = &ck
	st, err := pebblestore.Open("db", o)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	return st
}

// servePolicy is the policy of a store the fixture is written to.
func servePolicy(rank map[lifecycle.Producer]int) lifecycle.Policy {
	return lifecycle.Policy{Rank: rank, BootKey: lifecycle.BootID}
}

// settle waits for the background retention pass of a pebblestore, if st is one.
func settle(tb testing.TB, st store.Store) {
	tb.Helper()
	if p, ok := st.(*pebblestore.Store); ok {
		if err := p.Instrument().WaitRetained(ctx); err != nil {
			tb.Fatal(err)
		}
	}
}

// outcome classifies the failure of a tool: the refusals the twin test pins.
func outcome(err error) string {
	switch {
	case err == nil:
		return "answered"
	case errors.Is(err, interval.ErrRetained):
		return "retained"
	case errors.Is(err, store.ErrBeforeHorizon):
		return "before the horizon"
	case errors.Is(err, ErrNoRangeReads):
		return "no range reads"
	}
	return "other"
}

// twinCoverage counts what the twin test asked, to guard against a vacuous run.
type twinCoverage struct {
	compared           int
	answered           map[string]int // by tool, on the unretained runs
	retained           int            // refusals whose cause is an *interval.RetainedError
	beforeHorizon      int
	pebbleNamesWebZero bool
}

// Every tool, asked the same questions of memstore and of pebblestore holding
// the same records, gives the same JSON or the same refusal: unretained, and
// retained at three horizons in each way pebblestore retains, before and after
// the background pass.
func TestToolsAnswerAsTheirTwin(t *testing.T) {
	cov := &twinCoverage{answered: map[string]int{}}
	t.Cleanup(func() {
		t.Logf("twin: %d questions compared; answers by tool on unretained stores %v; %d retention refusals with a seed, %d before the horizon",
			cov.compared, cov.answered, cov.retained, cov.beforeHorizon)
		for _, tool := range []string{"blast_radius", "what_changed", "co_residents", "service_context", "neighbors_at", "entity_history", "find_entities", "resolve_entity", "describe_schema"} {
			if cov.answered[tool] == 0 {
				t.Errorf("no %s answer on an unretained store", tool)
			}
		}
		if cov.retained == 0 {
			t.Error("no refusal carried an *interval.RetainedError")
		}
		if cov.beforeHorizon == 0 {
			t.Error("no refusal was a read before the horizon")
		}
		if !cov.pebbleNamesWebZero {
			t.Error(`no unretained pebblestore answer holds "name":"web-0"`)
		}
	})
	for _, v := range variants() {
		for _, h := range []time.Time{{}, hm(9, 0), hm(9, 30), hm(10, 0)} {
			name := v.name + ", no retention"
			if !h.IsZero() {
				name = v.name + ", retained at " + h.Format("15:04")
			}
			t.Run(name, func(t *testing.T) {
				wm := newWorld(t, rankObjs)
				wp := newWorldOn(t, openPeb(t, servePolicy(rankObjs), v.ck, v.mode))
				for _, w := range []*world{wm, wp} {
					if !h.IsZero() {
						if err := w.st.Retain(ctx, h); err != nil {
							t.Fatal(err)
						}
					}
				}
				if wm.st.LastSeq() != wp.st.LastSeq() {
					t.Fatalf("LastSeq %d and %d", wm.st.LastSeq(), wp.st.LastSeq())
				}
				for l := catalog.L0; l <= catalog.L3; l++ {
					if lm, lp := wm.st.LayerHorizon(l), wp.st.LayerHorizon(l); lm != lp {
						t.Fatalf("horizon of %s: %v and %v", l, lm, lp)
					}
				}
				if !RangeReadsFor(wm.st) || !RangeReadsFor(wp.st) {
					t.Fatal("RangeReadsFor is false for a store of the module")
				}
				sm, sp := wm.source(t, rankObjs, RangeReadsFor(wm.st)), wp.source(t, rankObjs, RangeReadsFor(wp.st))
				qs := append(questions(wm), twinExtras(wm)...)
				compare := func() {
					t.Helper()
					for _, q := range qs {
						vm, em := q.ask(sm)
						vp, ep := q.ask(sp)
						cov.compared++
						if outcome(em) != outcome(ep) {
							t.Errorf("%s: memstore %s (%v), pebblestore %s (%v)", q.name, outcome(em), em, outcome(ep), ep)
							continue
						}
						if em != nil || ep != nil {
							var tm, tp *Error
							if !errors.As(em, &tm) || !errors.As(ep, &tp) || tm.Message != tp.Message {
								t.Errorf("%s: memstore refused %q, pebblestore %q", q.name, em, ep)
							}
							var re *interval.RetainedError
							if errors.As(ep, &re) {
								cov.retained++
							}
							if errors.Is(ep, store.ErrBeforeHorizon) {
								cov.beforeHorizon++
							}
							continue
						}
						bm, err := json.Marshal(vm)
						if err != nil {
							t.Fatal(err)
						}
						bp, err := json.Marshal(vp)
						if err != nil {
							t.Fatal(err)
						}
						if !bytes.Equal(bm, bp) {
							t.Errorf("%s differs:\nmemstore    %s\npebblestore %s", q.name, bm, bp)
						}
						if h.IsZero() {
							cov.answered[q.tool]++
							if strings.Contains(string(bp), `"name":"web-0"`) {
								cov.pebbleNamesWebZero = true
							}
						}
					}
				}
				compare()
				if v.mode == pebblestore.Background {
					settle(t, wp.st)
					compare()
				}
			})
		}
	}
}

// twinExtras are the questions of the window [10:00, 10:30), which starts at
// the horizon of the last retention: three histories, a diff of a pod that
// appears in the window, and a point read at the horizon.
func twinExtras(w *world) []question {
	var out []question
	for _, e := range []struct {
		name string
		fp   interface{ String() string }
	}{{"pod", w.p1}, {"node", w.n1}, {"host", w.h1}} {
		out = append(out, question{name: "history in the window of a " + e.name, tool: "entity_history", ask: func(s *Source) (any, error) {
			return s.EntityHistory(ctx, EntityHistoryInput{Entity: e.fp.String(), From: w10, To: w1030})
		}})
	}
	out = append(out,
		question{name: "diff of a pod that appears", tool: "what_changed", ask: func(s *Source) (any, error) {
			return s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p3.String()}, From: w10, To: w1030})
		}},
		question{name: "neighbors of a node at the horizon", tool: "neighbors_at", ask: func(s *Source) (any, error) {
			return s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: w10})
		}},
	)
	return out
}

// The golden answers hold on pebblestore as they do on memstore: byte for byte.
func TestGoldenAnswersOnPebblestore(t *testing.T) {
	wp := newWorldOn(t, openPeb(t, servePolicy(rankObjs), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous))
	sp := wp.source(t, rankObjs, RangeReadsFor(wp.st))
	for name, v := range goldenAnswers(t, wp, sp) {
		t.Run(name, func(t *testing.T) {
			got, err := json.MarshalIndent(v, "", "  ")
			if err != nil {
				t.Fatal(err)
			}
			got = append(got, '\n')
			path := filepath.Join("testdata", name+".golden.json")
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Errorf("%s on pebblestore differs from %s.\ngot:\n%s", name, path, got)
			}
		})
	}
}

// service_context gives the same JSON or the same refusal on memstore and on
// pebblestore holding the service world: unretained, and retained at 09:30 and
// 10:00 in each way pebblestore retains, before and after the background pass.
func TestServiceContextTwin(t *testing.T) {
	answered, refused := 0, 0
	for _, v := range variants() {
		for _, h := range []time.Time{{}, hm(9, 30), hm(10, 0)} {
			name := v.name + ", no retention"
			if !h.IsZero() {
				name = v.name + ", retained at " + h.Format("15:04")
			}
			t.Run(name, func(t *testing.T) {
				wm := newSvcWorld(t, newSvcMem(t))
				wp := newSvcWorld(t, openPeb(t, servePolicy(rankObjs), v.ck, v.mode))
				if !h.IsZero() {
					for _, st := range []store.Store{wm.st, wp.st} {
						if err := st.Retain(ctx, h); err != nil {
							t.Fatal(err)
						}
					}
				}
				if wm.st.LastSeq() != wp.st.LastSeq() {
					t.Fatalf("LastSeq %d and %d", wm.st.LastSeq(), wp.st.LastSeq())
				}
				sm, sp := wm.source(t), wp.source(t)
				type q struct {
					name string
					in   func(w *svcWorld) ServiceContextInput
				}
				qs := []q{
					{"checkout", func(w *svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030}
					}},
					{"checkout, node only", func(w *svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030, NodeOnly: true}
					}},
					{"cart", func(w *svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: w.id("cart"), From: w10, To: w1030}
					}},
					{"checkout from 09:00", func(w *svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: w.id("checkout"), From: "2026-01-01T09:00:00Z", To: w1030}
					}},
					{"checkout, a window that ends before legacy-pay's edge expires", func(w *svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: w.id("checkout"), From: w10, To: "2026-01-01T10:14:00Z"}
					}},
					{"an unknown service", func(*svcWorld) ServiceContextInput {
						return ServiceContextInput{Service: unknownService().String(), From: w10, To: w1030}
					}},
				}
				compare := func() {
					t.Helper()
					for _, c := range qs {
						vm, em := sm.ServiceContext(ctx, c.in(wm))
						vp, ep := sp.ServiceContext(ctx, c.in(wp))
						if outcome(em) != outcome(ep) {
							t.Errorf("%s: memstore %s (%v), pebblestore %s (%v)", c.name, outcome(em), em, outcome(ep), ep)
							continue
						}
						if em != nil || ep != nil {
							var tm, tp *Error
							if !errors.As(em, &tm) || !errors.As(ep, &tp) || tm.Message != tp.Message {
								t.Errorf("%s: memstore refused %q, pebblestore %q", c.name, em, ep)
							}
							refused++
							continue
						}
						if bm, bp := asJSON(t, vm), asJSON(t, vp); !bytes.Equal(bm, bp) {
							t.Errorf("%s differs:\nmemstore    %s\npebblestore %s", c.name, bm, bp)
						}
						answered++
					}
				}
				compare()
				if v.mode == pebblestore.Background {
					settle(t, wp.st)
					compare()
				}
			})
		}
	}
	t.Logf("service_context twin: %d answers and %d refusals compared", answered, refused)
	if answered == 0 || refused == 0 {
		t.Errorf("%d answers and %d refusals: the test compares nothing of one kind", answered, refused)
	}
}
