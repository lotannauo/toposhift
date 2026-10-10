package pebblestore

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

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// countRecorder keeps the counts a store reports.
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
func rawDump(t *testing.T, e *Store) [][2][]byte {
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
func checkpointsOnDisk(t *testing.T, e *Store) map[string][]int64 {
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

// prefixesHoldingKeys are the prefixes that hold a record or a checkpoint, the
// keys that learning the state of a prefix counts.
func prefixesHoldingKeys(e *Store) (map[string]struct{}, error) {
	out := map[string]struct{}{}
	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return nil, err
	}
	defer func() { _ = it.Close() }()
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			return nil, err
		}
		if kind != kindBaseline {
			out[string(prefix)] = struct{}{}
		}
	}
	return out, it.Error()
}

func prefixesWithKeys(t *testing.T, e *Store) map[string]struct{} {
	t.Helper()
	out, err := prefixesHoldingKeys(e)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// failures are the commits the next operation makes fail: the same for both
// stores of a comparison, each consuming its own copy.
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

// TestTheWriterStateEqualsAWholeRead runs random histories through two stores:
// one that reads the whole of a prefix to learn its state, as the writer once
// did, and one that reads only its tail and looks older checkpoints up when it
// needs them. The histories have late records on a nanosecond grid, several
// records per prefix in a batch, checkpoints from the policy (with a lag and a
// size condition) and the hook, retentions in random pieces that sometimes stop
// halfway, reopenings that switch checkpoints on and off, and commits that fail
// before or after landing. The tail-reading store also works the state of every
// prefix out during a retention instead of reading it afterwards, and takes a
// prefix its map lacks for empty once the map is complete. After every step the
// two databases must hold the same keys and values, the stores must have
// returned the same errors and counted the same of everything but what they read
// to learn the state, and what the tail-reading store remembers of each prefix
// (the checkpoints at or after its bound) must be what the whole-read store
// remembers of it or, where that one has not met the prefix since a retention,
// what a whole read of it finds now. A prefix only the whole-read store
// remembers must be an empty one in a complete map, and a complete map must have
// every prefix that holds a record or a checkpoint. The whole-read store's list
// must be exactly what is on disk. The answers of both are checked against the
// reference, after the counts are compared, so the reads' counts are compared too.
func TestTheWriterStateEqualsAWholeRead(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the random histories run in the full tier")
	}
	t.Parallel()
	const steps = 160
	seeds := int64(120)
	if raceEnabled {
		seeds = 6 // one goroutine: the race detector finds nothing here and costs minutes; plain runs take all seeds
	}
	// What the histories exercised, summed over the seeds: checkpoints the tail
	// read left unknown, lookups and invalidations.
	var mu sync.Mutex
	var hidden, lookups, invalidated, saved, completes, forced int64
	t.Run("seeds", func(t *testing.T) {
		for seed := int64(0); seed < seeds; seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewSource(seed))
				lags := []time.Duration{0, 1, 3, 7}
				alphas := []float64{0, 0, 0.5, 2}
				ck := CheckpointOptions{On: rng.Intn(5) != 0, KMin: 1 + rng.Intn(3), Alpha: alphas[rng.Intn(len(alphas))], Lag: lags[rng.Intn(len(lags))]}
				type side struct {
					e   *Store
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
						opts := Options{Config: s.cfg, Recorder: s.rec, Checkpoints: policy(o), retainBatchBytes: pieceBytes, retainStopAfter: stop, fullStateRead: s.id}
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
				ora := newRef(t)
				// Several sources and peers, so that there are prefixes a write
				// has never met, and prefixes a retention leaves with only a
				// baseline, while others have history.
				srcs := []identity.Fingerprint{podFP, fingerprintOf(catalog.K8sPod, 1), fingerprintOf(catalog.K8sPod, 2), fingerprintOf(catalog.K8sPod, 3)}
				names := []string{"a", "b", "c", "d", "e", "f"}
				ps := peers(names...)
				src := func() identity.Fingerprint { return srcs[rng.Intn(len(srcs))] }
				var queried []identity.Fingerprint
				queried = append(queried, srcs...)
				for _, n := range names {
					queried = append(queried, ps[n])
				}
				at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
				var seq, last uint64
				horizon, retained := 0, false
				var log []string
				both := func(f func(e *Store) error) {
					var errs []error
					for _, s := range sides {
						errs = append(errs, f(s.e))
					}
					if fmt.Sprint(errs[0]) != fmt.Sprint(errs[1]) {
						t.Fatalf("the stores returned %v and %v\n%v", errs[0], errs[1], log)
					}
					if errs[0] != nil && !errors.Is(errs[0], errInjected) && !errors.Is(errs[0], store.ErrInvalid) {
						t.Fatalf("%v\n%v", errs[0], log)
					}
				}
				setFailures := func() {
					f := failures{rng.Intn(12) == 0, rng.Intn(12) == 0, rng.Intn(12) == 0}
					for _, s := range sides {
						s.f = f
					}
				}
				for step := range steps {
					switch op := rng.Intn(12); {
					case op < 7:
						var batch []store.Record
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
							var r store.Record
							if rng.Intn(4) == 0 {
								r = store.Record{Layer: catalog.L2, Subject: store.EntitySubject(src()), Producer: prod, EventTime: at(ns), Seq: seq, Kind: kind, TTL: ttl}
								if kind == lifecycle.Observe {
									r.Payload = bytes.Repeat([]byte("x"), 1+rng.Intn(20))
								}
							} else {
								r = edgeTo(seq, ps[names[rng.Intn(len(names))]], prod, at(ns), kind, ttl)
								r.Subject.A = src()
							}
							batch = append(batch, r)
						}
						setFailures()
						injected := false
						both(func(e *Store) error {
							err := e.Write(bg, cloneAll(batch))
							injected = errors.Is(err, errInjected)
							return err
						})
						if injected {
							seq -= uint64(len(batch))
							log = append(log, fmt.Sprintf("write of %d failed", len(batch)))
							// A store whose record commit failed stops until it is
							// reopened, which reads everything afresh.
							for _, s := range sides {
								if err := s.e.Close(); err != nil {
									t.Fatal(err)
								}
							}
							open()
						} else {
							if err := ora.Write(bg, cloneAll(batch)); err != nil {
								t.Fatal(err)
							}
							for _, r := range batch {
								log = append(log, fmt.Sprintf("write seq %d %s@%d %v ttl %d", r.Seq, r.Producer, r.EventTime.Sub(t0), r.Kind, r.TTL))
							}
						}
					case op < 9:
						c := horizon - 2 + rng.Intn(65)
						which, peer, from := rng.Intn(3), names[rng.Intn(len(names))], src()
						setFailures()
						both(func(e *Store) error {
							switch which {
							case 0:
								return e.Instrument().CheckpointEdges(catalog.L2, from, store.Forward, at(c))
							case 1:
								return e.Instrument().CheckpointEdges(catalog.L2, ps[peer], store.Reverse, at(c))
							}
							return e.Instrument().CheckpointEntity(catalog.L2, from, at(c))
						})
						log = append(log, fmt.Sprintf("checkpoint at %d", c))
					case op < 11:
						if nh := horizon + rng.Intn(20); rng.Intn(2) == 0 && (!retained || nh > horizon) {
							horizon, retained, last = nh, true, seq
							both(func(e *Store) error { return e.Retain(bg, at(horizon)) })
							if err := ora.Retain(bg, at(horizon)); err != nil {
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
					// The tail store reads less to learn what it knows, and the
					// lookups it needs differ with what each read left unknown.
					if cb["checkpoint.loads"] > ca["checkpoint.loads"] {
						t.Fatalf("step %d: the store that works the state out loaded %d times, the whole read %d\n%v", step, cb["checkpoint.loads"], ca["checkpoint.loads"], log)
					}
					mu.Lock()
					saved += ca["checkpoint.loads"] - cb["checkpoint.loads"]
					mu.Unlock()
					for _, m := range []map[string]int64{ca, cb} {
						delete(m, "retain.state_keys")
						delete(m, "checkpoint.loads")
						delete(m, "checkpoint.load_keys")
						delete(m, "checkpoint.lookups")
						delete(m, "write.iterators")     // what they open to read the state
						delete(m, "checkpoint.build_ns") // a time
					}
					if !maps.Equal(ca, cb) {
						t.Fatalf("step %d: the counts differ: %v and %v\n%v", step, ca, cb, log)
					}
					disk := checkpointsOnDisk(t, full)
					if full.complete {
						t.Fatalf("step %d: the whole read store believes its map is complete\n%v", step, log)
					}
					// A prefix the whole read store remembers and the other does not
					// is one the other knows to be empty.
					for k, a := range full.states {
						if _, ok := tail.states[k]; !ok && (!tail.complete || !a.empty()) {
							t.Fatalf("step %d: the whole read remembers %+v of a prefix the other does not (complete %v)\n%v", step, *a, tail.complete, log)
						}
					}
					if tail.complete {
						mu.Lock()
						completes++
						mu.Unlock()
						for k := range prefixesWithKeys(t, tail) {
							if _, ok := tail.states[k]; !ok {
								t.Fatalf("step %d: the map is complete and lacks a prefix that holds keys\n%v", step, log)
							}
						}
					}
					// Everything the tail store remembers is what a whole read finds
					// (the same as the whole read store's own, which has read and
					// changed the same since, or, for a prefix that one has not
					// touched since a retention, a whole read made now).
					probe := wholeReader(full)
					for k, b := range tail.states {
						a, ok := full.states[k]
						if !ok {
							var err error
							if a, err = probe.state([]byte(k)); err != nil {
								t.Fatal(err)
							}
							mu.Lock()
							forced++
							mu.Unlock()
						}
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
					for _, fp := range queried {
						for range 2 {
							tm, tok := at(horizon+rng.Intn(70)), []uint64{store.Latest, last, seq}[rng.Intn(3)]
							sc := store.Scope{Layer: catalog.L2, AsOf: tok}
							for _, dir := range []store.Direction{store.Forward, store.Reverse} {
								want, _ := ora.Neighbors(bg, fp, dir, tm, sc)
								for _, e := range []*Store{full, tail} {
									got, err := e.Neighbors(bg, fp, dir, tm, sc)
									if err != nil || !slices.Equal(got, want) {
										t.Fatalf("step %d: Neighbors(%s, %s, %d, asOf %d) = %v, %v; the reference says %v\n%v", step, fp, dir, tm.Sub(t0), tok, got, err, want, log)
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
	if saved == 0 || completes == 0 || forced == 0 {
		t.Fatalf("the histories saved %d loads, had a complete map in %d steps and compared %d remembered states with a forced whole read: they do not exercise the worked-out state", saved, completes, forced)
	}
	t.Logf("%d states with checkpoints unknown, %d lookups, %d checkpoints invalidated; %d loads saved, %d steps with a complete map, %d states compared with a forced whole read", hidden, lookups, invalidated, saved, completes, forced)
}

// The state read after an opening is as long as the prefix's tail, not its
// history, where reading the whole prefix grows with it. (A retention does not
// leave a state to read: see TestARetentionWorksOutWhatAWholeReadFinds.)
func TestTheStateReadDoesNotGrowWithHistory(t *testing.T) {
	t.Parallel()
	keysRead := func(n int, whole bool) int64 {
		rec := &countRecorder{}
		cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
		opts := Options{Config: cfg, Recorder: rec, Checkpoints: policy(CheckpointOptions{On: true, KMin: 8}), fullStateRead: whole}
		e, err := Open("db", opts)
		if err != nil {
			t.Fatal(err)
		}
		var seq uint64
		for i := range n {
			seq++
			if err := e.Write(bg, []store.Record{edgeRecord(seq, lifecycle.Producer(fmt.Sprintf("p%d", i%5)), sec(i+1), lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
		}
		if err := e.Retain(bg, sec(2)); err != nil {
			t.Fatal(err)
		}
		if err := e.Close(); err != nil {
			t.Fatal(err)
		}
		if e, err = Open("db", opts); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = e.Close() }()
		before := rec.counts()["checkpoint.load_keys"]
		seq++
		if err := e.Write(bg, []store.Record{edgeRecord(seq, "p0", sec(n+1), lifecycle.Observe, 0)}); err != nil {
			t.Fatal(err)
		}
		return rec.counts()["checkpoint.load_keys"] - before
	}
	// An edge is in two prefixes, each with a tail of fewer than KMin records, a
	// checkpoint and the newest record before it.
	for _, n := range []int{100, 1000, 4000} {
		if got := keysRead(n, false); got > 2*(8+2) {
			t.Fatalf("learning the state after a reopening read %d keys of a %d-record history; want at most a tail's", got, n)
		}
	}
	if w := keysRead(1000, true); w < 1000 {
		t.Fatalf("the whole read read %d keys of a 1000-record history", w)
	}
}
