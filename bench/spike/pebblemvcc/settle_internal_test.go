package pebblemvcc

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// countRecorder keeps the counts an engine reports, including the names counted
// with zero.
type countRecorder struct {
	mu sync.Mutex
	m  map[string]int64
}

func (r *countRecorder) Count(name string, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.m == nil {
		r.m = map[string]int64{}
	}
	r.m[name] += n
}

func (r *countRecorder) Sample(string, int64) {}

func (r *countRecorder) counts() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return maps.Clone(r.m)
}

// rawDump is every key and value in the database, meta keys included.
func rawDump(t *testing.T, e *Engine) [][2][]byte {
	t.Helper()
	it, err := e.kv.NewIter(&pebble.IterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out [][2][]byte
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, [2][]byte{slices.Clone(it.Key()), slices.Clone(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

func sec(n int) time.Time { return t0.Add(time.Duration(n) * time.Second) }

func settling(rec engine.Recorder) Options {
	return Options{Config: pebblekv.Config{SettleRetention: true}, Recorder: rec}
}

// settleStream writes eighty versions of five keys (one producer each), retains
// inside them, writes forty more, and retains again. afterRetain is called after
// each of the two retentions with its number.
func settleStream(t *testing.T, e *Engine, afterRetain func(n int)) {
	t.Helper()
	var seq uint64
	write := func(from, to int) {
		for i := from; i < to; i++ {
			seq++
			producer := lifecycle.Producer(fmt.Sprintf("p%d", i%5))
			if err := e.Write([]engine.Record{edgeRecord(seq, producer, sec(i), lifecycle.Observe, 0)}); err != nil {
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

// variants are the layouts of this package a retention is settled in: both key
// schemas, with and without the time filter.
func settleVariants() map[string]Options {
	out := map[string]Options{}
	for name, schema := range map[string]pebblekv.Schema{"crdb1": pebblekv.SchemaCRDB, "default": pebblekv.SchemaDefault} {
		out[name] = Options{Config: pebblekv.Config{Schema: schema}}
		out[name+"+filter"] = Options{Config: pebblekv.Config{Schema: schema, TimeFilter: true}}
	}
	return out
}

func withSettling(o Options, rec engine.Recorder, deadline time.Duration) Options {
	o.Recorder, o.SettleRetention, o.SettleDeadline = rec, true, deadline
	return o
}

func TestARetentionSettlesBeforeItReturns(t *testing.T) {
	t.Parallel()
	for name, base := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &countRecorder{}
			e := openMem(t, withSettling(base, rec, 0))
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
	rec := &countRecorder{}
	e := openMem(t, Options{Recorder: rec})
	settleStream(t, e, func(n int) {
		work, flush, settle, hit := e.LastRetain()
		if work <= 0 || flush != 0 || settle != 0 || hit {
			t.Errorf("retention %d: LastRetain = %s, %s, %s, %v; want work only", n, work, flush, settle, hit)
		}
		if c := rec.counts(); c["retain.settle_ns"] != 0 || c["retain.flush_ns"] != 0 || c["retain.settle_deadline_hits"] != 0 {
			t.Errorf("retention %d counted a wait: %v", n, c)
		}
	})
}

// A retention that reaches the deadline returns without an error, says so, and has
// still flushed.
func TestARetentionThatReachesTheDeadlineSaysSo(t *testing.T) {
	t.Parallel()
	rec := &countRecorder{}
	e := openMem(t, withSettling(Options{}, rec, time.Nanosecond))
	settleStream(t, e, func(n int) {
		_, flush, settle, hit := e.LastRetain()
		if !hit || flush <= 0 || settle >= time.Second {
			t.Errorf("retention %d: flush %s, wait %s, reached %v; want a flush, a short wait and the deadline reached", n, flush, settle, hit)
		}
		if hits := rec.counts()["retain.settle_deadline_hits"]; hits != int64(n) {
			t.Errorf("retention %d: %d deadline hits counted, want %d", n, hits, n)
		}
	})
}

// What LastRetain says is about the last call: a retention that changes nothing
// leaves no flush or wait behind from the one before it, whether it returns for a
// horizon that is not after the last or for one before every instant.
func TestARetentionThatChangesNothingForgetsTheLastOne(t *testing.T) {
	t.Parallel()
	e := openMem(t, settling(nil))
	// A horizon before every instant a record can have: the horizon is stored and
	// nothing else happens.
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
}

// Settling changes no stored key or value and no counter but its own: two engines
// given the same stream, one settling and one not, hold the same database once
// everything is compacted.
func TestSettlingStoresTheSameAsNotSettling(t *testing.T) {
	t.Parallel()
	own := []string{"retain.flush_ns", "retain.settle_ns", "retain.settle_deadline_hits"}
	for name, base := range settleVariants() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			recOn, recOff := &countRecorder{}, &countRecorder{}
			off := base
			off.Recorder = recOff
			on, plain := openMem(t, withSettling(base, recOn, 0)), openMem(t, off)
			for _, e := range []*Engine{on, plain} {
				settleStream(t, e, func(int) {})
				if err := e.kv.CompactAll(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			a, b := rawDump(t, on), rawDump(t, plain)
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
		})
	}
}

// An engine whose retentions settle passes the conformance checks that retain, on
// two of the fixed workloads. (The whole conformance test settles every retention
// it makes, which is minutes of waiting.)
func TestConformsWithSettledRetentions(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t) // each retention waits a second for rest; the full tier runs it
	cfgs := conformance.Configs()
	for _, i := range []int{0, 5} {
		t.Run(fmt.Sprintf("config %d", i), func(t *testing.T) {
			t.Parallel()
			o := settling(nil)
			o.Schema, o.Tuning, o.FS = pebblekv.SchemaCRDB, pebblekv.TinyTuning(), vfs.NewMem()
			e, err := Open("db", o)
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
