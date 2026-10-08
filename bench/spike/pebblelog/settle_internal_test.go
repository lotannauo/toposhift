package pebblelog

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// settleVariants are the layouts of this package: no checkpoints, and checkpoints
// from a policy that writes some in the streams below.
func settleVariants() map[string]CheckpointOptions {
	return map[string]CheckpointOptions{
		"off":       {},
		"k64a2l1ns": {On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond},
		"stress":    stress(time.Nanosecond),
	}
}

func settling(c CheckpointOptions, rec engine.Recorder) Options {
	return Options{Config: pebblekv.Config{SettleRetention: true}, Checkpoints: c, Recorder: rec}
}

// settleStream writes eighty records to one entity's forward prefix (so the policy
// above writes a checkpoint) and spreads them over five peers' reverse prefixes,
// retains inside them, writes forty more, and retains again. afterRetain is called
// after each of the two retentions with its number.
func settleStream(t *testing.T, e *Engine, afterRetain func(n int)) {
	t.Helper()
	ps := peers("a", "b", "c", "d", "e")
	names := []string{"a", "b", "c", "d", "e"}
	var seq uint64
	write := func(from, to int) {
		for i := from; i < to; i++ {
			seq++
			if err := e.Write([]engine.Record{edgeTo(seq, ps[names[i%len(names)]], "p", sec(i), lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
		}
	}
	write(1, 81)
	if err := e.Retain(sec(40)); err != nil {
		t.Fatal(err)
	}
	afterRetain(1)
	write(81, 121)
	if err := e.Retain(sec(100)); err != nil {
		t.Fatal(err)
	}
	afterRetain(2)
}

func TestARetentionSettlesBeforeItReturns(t *testing.T) {
	t.Parallel()
	for name, ck := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &countRecorder{}
			e := openMem(t, settling(ck, rec))
			settleStream(t, e, func(n int) {
				work, flush, settle, hit := e.LastRetain()
				if work <= 0 || flush <= 0 || settle <= 0 || hit {
					t.Errorf("retention %d: LastRetain = %s, %s, %s, %v; want the work, the flush and the wait all above zero and the deadline not reached", n, work, flush, settle, hit)
				}
				c := rec.counts()
				if c["retain.settle_ns"] <= 0 || c["retain.flush_ns"] <= 0 {
					t.Errorf("retention %d: the counters say %d ns of waiting and %d ns of flushing", n, c["retain.settle_ns"], c["retain.flush_ns"])
				}
				if hits, ok := c["retain.settle_deadline_hits"]; !ok || hits != 0 {
					t.Errorf("retention %d: retain.settle_deadline_hits = %d (counted: %v), want 0, counted", n, hits, ok)
				}
			})
		})
	}
}

func TestARetentionThatDoesNotSettleRecordsNoWait(t *testing.T) {
	t.Parallel()
	for name, ck := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &countRecorder{}
			e := openMem(t, Options{Checkpoints: ck, Recorder: rec})
			settleStream(t, e, func(n int) {
				work, flush, settle, hit := e.LastRetain()
				if work <= 0 || flush != 0 || settle != 0 || hit {
					t.Errorf("retention %d: LastRetain = %s, %s, %s, %v; want work only", n, work, flush, settle, hit)
				}
				if c := rec.counts(); c["retain.settle_ns"] != 0 || c["retain.flush_ns"] != 0 || c["retain.settle_deadline_hits"] != 0 {
					t.Errorf("retention %d counted a wait: %v", n, c)
				}
			})
		})
	}
}

// A retention that reaches the deadline returns without an error, says so, and has
// still flushed and stored the same.
func TestARetentionThatReachesTheDeadlineSaysSo(t *testing.T) {
	t.Parallel()
	for name, ck := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &countRecorder{}
			opts := settling(ck, rec)
			opts.SettleDeadline = time.Nanosecond
			e := openMem(t, opts)
			settleStream(t, e, func(n int) {
				_, flush, settle, hit := e.LastRetain()
				if !hit || flush <= 0 || settle >= time.Second {
					t.Errorf("retention %d: flush %s, wait %s, reached %v; want a flush, a short wait and the deadline reached", n, flush, settle, hit)
				}
				if hits := rec.counts()["retain.settle_deadline_hits"]; hits != int64(n) {
					t.Errorf("retention %d: %d deadline hits counted, want %d", n, hits, n)
				}
			})
		})
	}
}

// What LastRetain says is about the last call: a retention that changes nothing
// leaves no flush or wait behind from the one before it, whether it returns for a
// horizon that is not after the last or for one before every instant.
func TestARetentionThatChangesNothingForgetsTheLastOne(t *testing.T) {
	t.Parallel()
	for name, ck := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			e := openMem(t, settling(ck, nil))
			// A horizon before every instant a record can have: the horizon is stored
			// and nothing else happens.
			if err := e.Retain(time.Date(1000, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
				t.Fatal(err)
			}
			if _, flush, settle, hit := e.LastRetain(); flush != 0 || settle != 0 || hit {
				t.Errorf("a retention before every instant: flush %s, wait %s, reached %v", flush, settle, hit)
			}
			settleStream(t, e, func(n int) {
				if _, flush, settle, _ := e.LastRetain(); flush <= 0 || settle <= 0 {
					t.Fatalf("retention %d did not settle (flush %s, wait %s): the test has nothing to forget", n, flush, settle)
				}
			})
			// The horizon is not after the last one.
			if err := e.Retain(sec(100)); err != nil {
				t.Fatal(err)
			}
			if _, flush, settle, hit := e.LastRetain(); flush != 0 || settle != 0 || hit {
				t.Errorf("a retention that is not later than the last: flush %s, wait %s, reached %v", flush, settle, hit)
			}
		})
	}
}

// Settling changes no stored key or value, no counter but its own, and no
// remembered state: two engines given the same stream, one settling and one not,
// hold the same database once everything is compacted.
func TestSettlingStoresTheSameAsNotSettling(t *testing.T) {
	t.Parallel()
	own := []string{"retain.flush_ns", "retain.settle_ns", "retain.settle_deadline_hits"}
	for name, ck := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			recOn, recOff := &countRecorder{}, &countRecorder{}
			on, off := openMem(t, settling(ck, recOn)), openMem(t, Options{Checkpoints: ck, Recorder: recOff})
			for _, e := range []*Engine{on, off} {
				settleStream(t, e, func(int) {})
				if err := e.kv.CompactAll(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			a, b := rawDump(t, on), rawDump(t, off)
			if !slices.EqualFunc(a, b, func(x, y [2][]byte) bool { return bytes.Equal(x[0], y[0]) && bytes.Equal(x[1], y[1]) }) {
				t.Fatalf("the databases differ: %d keys with settling, %d without", len(a), len(b))
			}
			if len(a) == 0 {
				t.Fatal("the databases are empty")
			}
			ca, cb := recOn.counts(), recOff.counts()
			if ca["retain.range_deletes"] == 0 {
				t.Error("the stream's retentions deleted nothing")
			}
			for _, k := range own {
				delete(ca, k)
				delete(cb, k)
			}
			if !maps.Equal(ca, cb) {
				t.Errorf("the counters differ:\nsettling %v\nnot      %v", ca, cb)
			}
			if ck.On {
				if !on.anyCkpt {
					t.Error("the stream wrote no checkpoint, so it does not test the layout with them")
				}
			}
		})
	}
}

// The writer's map of each prefix is the one the retention worked out, whether the
// wait ends at rest or at its deadline: it is true of the committed data, and the
// wait changes none.
func TestTheWriterStateMapSurvivesSettling(t *testing.T) {
	t.Parallel()
	for _, deadline := range []time.Duration{0, time.Nanosecond} {
		t.Run(deadline.String(), func(t *testing.T) {
			t.Parallel()
			for name, ck := range settleVariants() {
				if !ck.On {
					continue
				}
				t.Run(name, func(t *testing.T) {
					t.Parallel()
					opts := settling(ck, nil)
					opts.SettleDeadline = deadline
					on, off := openMem(t, opts), openMem(t, Options{Checkpoints: ck})
					results := map[*Engine][]int{}
					for _, e := range []*Engine{on, off} {
						settleStream(t, e, func(n int) {
							if !e.complete {
								t.Fatalf("retention %d left the map incomplete: the test does not exercise the map a retention works out", n)
							}
							requireWholeReads(t, e)
							results[e] = append(results[e], len(e.states))
						})
					}
					if !slices.Equal(results[on], results[off]) {
						t.Errorf("the map holds %v prefixes with settling and %v without", results[on], results[off])
					}
				})
			}
		})
	}
}

// An engine whose retentions settle passes the conformance checks that retain, on
// two of the fixed workloads, with checkpoints and without. (The whole conformance
// test settles every retention it makes, which is minutes of waiting.)
func TestConformsWithSettledRetentions(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // each retention waits a second for rest; the full tier runs it
	cfgs := conformance.Configs()
	for name, ck := range map[string]CheckpointOptions{
		"off": {},
		"k8":  {On: true, KMin: 8, Alpha: 1},
	} {
		for _, i := range []int{0, 5} {
			t.Run(fmt.Sprintf("%s config %d", name, i), func(t *testing.T) {
				t.Parallel()
				e, err := Open("db", settledMem(ck))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = e.Close() })
				if err := conformance.Check(e, cfgs[i], conformance.Options{RetainAt: []float64{0.3, 0.7}}); err != nil {
					t.Fatal(err)
				}
			})
		}
	}
}

func settledMem(ck CheckpointOptions) Options {
	o := settling(ck, nil)
	o.Tuning, o.FS = pebblekv.TinyTuning(), vfs.NewMem()
	return o
}
