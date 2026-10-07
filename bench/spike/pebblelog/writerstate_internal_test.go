package pebblelog

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// countRecorder keeps the counts an engine reports.
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

// checkpointsOnDisk lists the instants of every checkpoint in each prefix,
// ascending.
func checkpointsOnDisk(t *testing.T, e *Engine) map[string][]int64 {
	t.Helper()
	out := map[string][]int64{}
	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		prefix, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		if kind == kindCheckpoint {
			out[string(prefix)] = append(out[string(prefix)], ns)
		}
	}
	for _, cs := range out {
		slices.Sort(cs)
	}
	return out
}

// failures are the commits the next operation makes fail: the same for both
// engines of a comparison, each consuming its own copy.
type failures struct{ record, ckptBefore, ckptAfter bool }

func (f *failures) hooks(o *Options) {
	take := func(b *bool) func() error {
		return func() error {
			if *b {
				*b = false
				return errInjected
			}
			return nil
		}
	}
	o.beforeRecordApply = take(&f.record)
	o.beforeCheckpointApply = take(&f.ckptBefore)
	o.afterCheckpointApply = take(&f.ckptAfter)
}

// TestTheWriterStateEqualsAWholeRead runs random histories through two engines:
// one that reads the whole of a prefix to learn its state, as the writer once
// did, and one that reads only its tail and looks older checkpoints up when it
// needs them. The histories have late records on a nanosecond grid, several
// records per prefix in a batch, checkpoints from the policy (with a lag and a
// size condition) and the hook, retentions in random pieces that sometimes stop
// halfway, reopenings that switch checkpoints on and off, and commits that fail
// before or after landing. After every step the two databases must hold the same
// keys and values, the engines must have returned the same errors and counted the
// same of everything but what they read to learn the state, they must remember
// the same of every prefix (the tail-reading one the checkpoints at or after its
// bound), and the whole-read engine's list must be exactly what is on disk. The
// answers of both are checked against the oracle, after the counts are compared,
// so the reads' counts are compared too.
func TestTheWriterStateEqualsAWholeRead(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	t.Parallel()
	// What the histories exercised, summed over the seeds: checkpoints the tail
	// read left unknown, lookups and invalidations.
	var mu sync.Mutex
	var hidden, lookups, invalidated int64
	t.Run("seeds", func(t *testing.T) {
		for seed := int64(0); seed < 48; seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewSource(seed))
				lags := []time.Duration{0, 1, 3, 7}
				alphas := []float64{0, 0, 0.5, 2}
				ck := CheckpointOptions{On: rng.Intn(5) != 0, KMin: 1 + rng.Intn(3), Alpha: alphas[rng.Intn(len(alphas))], Lag: lags[rng.Intn(len(lags))]}
				type side struct {
					e   *Engine
					cfg pebblekv.Config
					rec *countRecorder
					f   failures
					id  bool // reads the whole prefix
				}
				sides := []*side{{id: true}, {}}
				for _, s := range sides {
					s.cfg = pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
					s.rec = &countRecorder{}
				}
				open := func() {
					o := ck
					if rng.Intn(3) == 0 {
						o = CheckpointOptions{}
					}
					pieceBytes, stop := 1+rng.Intn(2000), rng.Intn(3)
					for _, s := range sides {
						opts := Options{Config: s.cfg, Recorder: s.rec, Checkpoints: o, retainBatchBytes: pieceBytes, retainStopAfter: stop, fullStateRead: s.id}
						s.f.hooks(&opts)
						e, err := Open("db", opts)
						if err != nil {
							t.Fatal(err)
						}
						s.e = e
					}
				}
				open()
				defer func() {
					for _, s := range sides {
						_ = s.e.Close()
					}
				}()
				ora := oracle.New()
				ps := peers("a", "b", "c")
				names := []string{"a", "b", "c"}
				at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
				var seq, last uint64
				horizon, retained := 0, false
				var log []string
				both := func(f func(e *Engine) error) {
					var errs []error
					for _, s := range sides {
						errs = append(errs, f(s.e))
					}
					if fmt.Sprint(errs[0]) != fmt.Sprint(errs[1]) {
						t.Fatalf("the engines returned %v and %v\n%v", errs[0], errs[1], log)
					}
					if errs[0] != nil && !errors.Is(errs[0], errInjected) && !errors.Is(errs[0], engine.ErrInvalid) {
						t.Fatalf("%v\n%v", errs[0], log)
					}
				}
				setFailures := func() {
					f := failures{rng.Intn(12) == 0, rng.Intn(12) == 0, rng.Intn(12) == 0}
					for _, s := range sides {
						s.f = f
					}
				}
				for step := range 80 {
					switch op := rng.Intn(12); {
					case op < 7:
						var batch []engine.Record
						for range 1 + rng.Intn(4) {
							seq++
							ns := horizon + rng.Intn(60)
							kind, ttl := lifecycle.Observe, time.Duration(0)
							switch {
							case rng.Intn(3) == 0:
								kind = lifecycle.Delete
							case rng.Intn(3) == 0:
								ttl = time.Duration(1 + rng.Intn(10))
							}
							prod := lifecycle.Producer([]string{"p", "q"}[rng.Intn(2)])
							var r engine.Record
							if rng.Intn(4) == 0 {
								r = engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(podFP), Producer: prod, EventTime: at(ns), Seq: seq, Kind: kind, TTL: ttl}
								if kind == lifecycle.Observe {
									r.Payload = bytes.Repeat([]byte("x"), 1+rng.Intn(20))
								}
							} else {
								r = edgeTo(seq, ps[names[rng.Intn(3)]], prod, at(ns), kind, ttl)
							}
							batch = append(batch, r)
						}
						setFailures()
						injected := false
						both(func(e *Engine) error {
							err := e.Write(cloneAll(batch))
							injected = errors.Is(err, errInjected)
							return err
						})
						if injected {
							seq -= uint64(len(batch))
							log = append(log, fmt.Sprintf("write of %d failed", len(batch)))
						} else {
							if err := ora.Write(cloneAll(batch)); err != nil {
								t.Fatal(err)
							}
							for _, r := range batch {
								log = append(log, fmt.Sprintf("write seq %d %s@%d %v ttl %d", r.Seq, r.Producer, r.EventTime.Sub(t0), r.Kind, r.TTL))
							}
						}
					case op < 9:
						c := horizon - 2 + rng.Intn(65)
						which, peer := rng.Intn(3), names[rng.Intn(3)]
						setFailures()
						both(func(e *Engine) error {
							switch which {
							case 0:
								return e.CheckpointEdges(catalog.L2, podFP, engine.Forward, at(c))
							case 1:
								return e.CheckpointEdges(catalog.L2, ps[peer], engine.Reverse, at(c))
							}
							return e.CheckpointEntity(catalog.L2, podFP, at(c))
						})
						log = append(log, fmt.Sprintf("checkpoint at %d", c))
					case op < 11:
						if nh := horizon + rng.Intn(20); rng.Intn(2) == 0 && (!retained || nh > horizon) {
							horizon, retained, last = nh, true, seq
							both(func(e *Engine) error { return e.Retain(at(horizon)) })
							if err := ora.Retain(at(horizon)); err != nil {
								t.Fatal(err)
							}
							log = append(log, fmt.Sprintf("retain %d (last seq %d)", horizon, last))
						}
					default:
						for _, s := range sides {
							if err := s.e.Close(); err != nil {
								t.Fatal(err)
							}
						}
						open()
						log = append(log, "reopen")
					}

					full, tail := sides[0].e, sides[1].e
					if a, b := rawDump(t, full), rawDump(t, tail); !slices.EqualFunc(a, b, func(x, y [2][]byte) bool {
						return bytes.Equal(x[0], y[0]) && bytes.Equal(x[1], y[1])
					}) {
						t.Fatalf("step %d: the databases differ\n%v", step, log)
					}
					ca, cb := sides[0].rec.counts(), sides[1].rec.counts()
					for _, m := range []map[string]int64{ca, cb} {
						delete(m, "checkpoint.load_keys")
						delete(m, "checkpoint.lookups")
					}
					if !maps.Equal(ca, cb) {
						t.Fatalf("step %d: the counts differ: %v and %v\n%v", step, ca, cb, log)
					}
					disk := checkpointsOnDisk(t, full)
					if !slices.Equal(slices.Sorted(maps.Keys(full.states)), slices.Sorted(maps.Keys(tail.states))) {
						t.Fatalf("step %d: the engines remember different prefixes\n%v", step, log)
					}
					for k, a := range full.states {
						b := tail.states[k]
						if a.below != 0 {
							t.Fatalf("step %d: the whole read left a bound %d", step, a.below)
						}
						if !slices.Equal(a.ckpts, disk[k]) {
							t.Fatalf("step %d: the whole read remembers checkpoints %v, the database has %v\n%v", step, a.ckpts, disk[k], log)
						}
						var known []int64
						for _, c := range a.ckpts {
							if c >= b.below {
								known = append(known, c)
							}
						}
						if len(known) < len(a.ckpts) {
							mu.Lock()
							hidden++
							mu.Unlock()
						}
						if a.latest != b.latest || a.since != b.since || a.sinceBytes != b.sinceBytes || a.lastBytes != b.lastBytes || !slices.Equal(known, b.ckpts) {
							t.Fatalf("step %d: the whole read remembers %+v, the tail read %+v\n%v", step, *a, *b, log)
						}
					}
					for _, fp := range []identity.Fingerprint{podFP, ps["a"], ps["b"], ps["c"]} {
						for range 4 {
							tm, tok := at(horizon+rng.Intn(70)), []uint64{engine.Latest, last, seq}[rng.Intn(3)]
							sc := engine.Scope{Layer: catalog.L2, AsOf: tok}
							for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
								want, _ := ora.Neighbors(fp, dir, tm, sc)
								for _, e := range []*Engine{full, tail} {
									got, err := e.Neighbors(fp, dir, tm, sc)
									if err != nil || !slices.Equal(got, want) {
										t.Fatalf("step %d: Neighbors(%s, %s, %d, asOf %d) = %v, %v; oracle says %v\n%v", step, fp, dir, tm.Sub(t0), tok, got, err, want, log)
									}
								}
							}
						}
					}
				}
				c := sides[1].rec.counts()
				mu.Lock()
				lookups += c["checkpoint.lookups"]
				invalidated += c["checkpoint.invalidated"]
				mu.Unlock()
			})
		}
	})
	if hidden == 0 || lookups == 0 || invalidated == 0 {
		t.Fatalf("the histories left %d states with checkpoints unknown, made %d lookups and invalidated %d checkpoints: they do not exercise the tail read", hidden, lookups, invalidated)
	}
	t.Logf("%d states with checkpoints unknown, %d lookups, %d checkpoints invalidated", hidden, lookups, invalidated)
}

// The state read after a retention is as long as the prefix's tail, not its
// history, where reading the whole prefix grows with it.
func TestTheStateReadDoesNotGrowWithHistory(t *testing.T) {
	t.Parallel()
	keysRead := func(n int, whole bool) int64 {
		rec := &countRecorder{}
		e := openMem(t, Options{Recorder: rec, Checkpoints: CheckpointOptions{On: true, KMin: 8}, fullStateRead: whole})
		var seq uint64
		for i := range n {
			seq++
			if err := e.Write([]engine.Record{edgeRecord(seq, lifecycle.Producer(fmt.Sprintf("p%d", i%5)), sec(i+1), lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Retain(sec(2)); err != nil {
			t.Fatal(err)
		}
		before := rec.counts()["checkpoint.load_keys"]
		seq++
		if err := e.Write([]engine.Record{edgeRecord(seq, "p0", sec(n+1), lifecycle.Observe, 0)}); err != nil {
			t.Fatal(err)
		}
		return rec.counts()["checkpoint.load_keys"] - before
	}
	// An edge is in two prefixes, each with a tail of fewer than KMin records, a
	// checkpoint and the newest record before it.
	for _, n := range []int{100, 1000, 4000} {
		if got := keysRead(n, false); got > 2*(8+2) {
			t.Fatalf("learning the state after a retention read %d keys of a %d-record history; want at most a tail's", got, n)
		}
	}
	if w := keysRead(1000, true); w < 1000 {
		t.Fatalf("the whole read read %d keys of a 1000-record history", w)
	}
}
