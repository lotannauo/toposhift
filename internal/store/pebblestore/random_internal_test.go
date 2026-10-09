package pebblestore

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// TestRandomCheckpointHistories compares the store with the reference on random
// histories whose event times are nanoseconds on a 40 ns grid, so checkpoints,
// records, reads and horizons land on one another and a nanosecond either side,
// which the whole-second workloads never do. Each history mixes entity and edge
// records, several records per prefix in one batch, checkpoints written by the
// hook at random instants, a random lag and spacing, reopenings that switch
// checkpoints on and off, and retentions committed in random pieces that
// sometimes stop halfway, and is read at sampled instants and tokens (the
// retention's last Seq and up) after every step.
func TestRandomCheckpointHistories(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the random histories run in the full tier")
	}
	t.Parallel()
	seeds := int64(10)
	if raceEnabled {
		seeds = 3 // one goroutine: the race detector finds nothing here and costs minutes; plain runs take all seeds
	}
	for seed := int64(0); seed < seeds; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			lags := []time.Duration{0, 1, 3, 7}
			ck := CheckpointOptions{On: rng.Intn(4) != 0, KMin: 1 + rng.Intn(3), Lag: lags[rng.Intn(len(lags))]}
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
			open := func() *Store {
				o := ck
				if rng.Intn(3) == 0 {
					o = CheckpointOptions{}
				}
				e, err := Open("db", Options{Config: cfg, Checkpoints: policy(o), retainBatchBytes: 1 + rng.Intn(2000), retainStopAfter: rng.Intn(3)})
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			e := open()
			defer func() { _ = e.Close() }()
			ora := newRef(t)
			ps := peers("a", "b", "c")
			names := []string{"a", "b", "c"}
			at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
			var seq, last uint64
			horizon, retained := 0, false
			var log []string
			for step := range 40 {
				switch op := rng.Intn(10); {
				case op < 6:
					var batch []store.Record
					for range 1 + rng.Intn(3) {
						seq++
						ns := horizon + rng.Intn(40)
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
							r = store.Record{Layer: catalog.L2, Subject: store.EntitySubject(podFP), Producer: prod, EventTime: at(ns), Seq: seq, Kind: kind, TTL: ttl}
							if kind == lifecycle.Observe {
								r.Payload = []byte("x")
							}
						} else {
							r = edgeTo(seq, ps[names[rng.Intn(3)]], prod, at(ns), kind, ttl)
						}
						batch = append(batch, r)
						log = append(log, fmt.Sprintf("write seq %d %s@%d %v ttl %d", seq, prod, ns, kind, ttl))
					}
					for _, x := range []store.Store{e, ora} {
						if err := x.Write(bg, cloneAll(batch)); err != nil {
							t.Fatalf("%v\n%v", err, log)
						}
					}
				case op < 8:
					c := horizon - 2 + rng.Intn(45)
					var err error
					switch rng.Intn(3) {
					case 0:
						err = e.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, at(c))
					case 1:
						err = e.Instrument().CheckpointEdges(catalog.L2, ps[names[rng.Intn(3)]], store.Reverse, at(c))
					default:
						err = e.Instrument().CheckpointEntity(catalog.L2, podFP, at(c))
					}
					if err != nil && !errors.Is(err, store.ErrInvalid) {
						t.Fatal(err)
					}
					log = append(log, fmt.Sprintf("checkpoint at %d, refused %v", c, err != nil))
				case op < 9:
					if nh := horizon + rng.Intn(15); rng.Intn(2) == 0 && (!retained || nh > horizon) {
						horizon, retained, last = nh, true, seq
						for _, x := range []store.Store{e, ora} {
							if err := x.Retain(bg, at(horizon)); err != nil && !errors.Is(err, errInjected) {
								t.Fatal(err)
							}
						}
						log = append(log, fmt.Sprintf("retain %d (last seq %d)", horizon, last))
					}
				default:
					if err := e.Close(); err != nil {
						t.Fatal(err)
					}
					e = open()
					log = append(log, "reopen")
				}
				tokens := []uint64{store.Latest, last, seq}
				for range 3 {
					tokens = append(tokens, last+uint64(rng.Intn(int(seq-last)+1)))
				}
				times := []time.Time{at(horizon), at(horizon + 1), at(horizon + 49)}
				for range 9 {
					times = append(times, at(horizon+rng.Intn(50)))
				}
				for _, fp := range []identity.Fingerprint{podFP, ps["a"], ps["b"], ps["c"]} {
					for _, tm := range times {
						for _, tok := range tokens {
							sc := store.Scope{Layer: catalog.L2, AsOf: tok}
							for _, dir := range []store.Direction{store.Forward, store.Reverse} {
								want, _ := ora.Neighbors(bg, fp, dir, tm, sc)
								got, err := e.Neighbors(bg, fp, dir, tm, sc)
								if err != nil || !slices.Equal(got, want) {
									t.Fatalf("step %d: Neighbors(%s, %s, %d, asOf %d) = %v, %v; the reference says %v\nhistory:\n%v", step, fp, dir, tm.Sub(t0), tok, got, err, want, log)
								}
							}
							wantAlive, _ := ora.Alive(bg, fp, tm, sc)
							if gotAlive, err := e.Alive(bg, fp, tm, sc); err != nil || gotAlive != wantAlive {
								t.Fatalf("step %d: Alive(%s, %d, asOf %d) = %v, %v; the reference says %v\nhistory:\n%v", step, fp, tm.Sub(t0), tok, gotAlive, err, wantAlive, log)
							}
						}
					}
				}
			}
		})
	}
}

// quarantined describes how an Alive ended, for comparing two stores: the answer, or
// the collision that quarantined the entity.
func quarantined(ok bool, err error) string {
	var q *store.QuarantineError
	switch {
	case errors.As(err, &q) && q.Collision != nil:
		return fmt.Sprintf("quarantined %+v", *q.Collision)
	case err != nil:
		return "error " + err.Error()
	}
	return fmt.Sprint(ok)
}

// TestRandomBootHistories is TestRandomCheckpointHistories for a store with a boot
// key, which keeps the prefix of a host whole when a retention would discard a
// boot, and so is the one place a retention works the writer's state out of a prefix
// it does not rewrite. Hosts reboot (and sometimes a clone reports an old boot),
// checkpoints are written for their own prefixes, by the policy and the hook, the
// horizon moves in random pieces, the store is reopened, and after every step the
// store answers as the reference does, quarantines included, and what it remembers
// of each prefix is what a read of the whole prefix finds.
func TestRandomBootHistories(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the random histories run in the full tier")
	}
	t.Parallel()
	// What the histories exercised, summed over the seeds.
	var mu sync.Mutex
	var kept, workedOut, quarantines, keptAndComplete int64
	t.Run("seeds", func(t *testing.T) {
		seeds := int64(16)
		if raceEnabled {
			seeds = 4 // one goroutine: the race detector finds nothing here and costs minutes; plain runs take all seeds
		}
		for seed := int64(0); seed < seeds; seed++ {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewSource(seed))
				lags := []time.Duration{0, time.Second, 3 * time.Second}
				ck := CheckpointOptions{On: rng.Intn(4) != 0, KMin: 1 + rng.Intn(3), Lag: lags[rng.Intn(len(lags))]}
				cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
				rec := newMemRecorder()
				open := func() *Store {
					o := ck
					if rng.Intn(3) == 0 {
						o = CheckpointOptions{}
					}
					s, err := Open("db", Options{Config: cfg, Policy: storetest.QuarantinePolicy(), Checkpoints: policy(o), Recorder: rec, retainBatchBytes: 1 + rng.Intn(2000), retainStopAfter: rng.Intn(3)})
					if err != nil {
						t.Fatal(err)
					}
					return s
				}
				s := open()
				defer func() { _ = s.Close() }()
				ref, err := memstoreWithPolicy(storetest.QuarantinePolicy())
				if err != nil {
					t.Fatal(err)
				}
				hosts := []identity.Fingerprint{fingerprintOf(catalog.Host, 0x60), fingerprintOf(catalog.Host, 0x61), fingerprintOf(catalog.Host, 0x62)}
				boot := make([]int, len(hosts))
				ps := peers("a", "b")
				at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }
				var seq uint64
				horizon := 0
				var log []string
				var seen, completeAndKept, keptBefore int64
				for step := range 50 {
					switch op := rng.Intn(10); {
					case op < 5:
						var batch []store.Record
						for range 1 + rng.Intn(3) {
							seq++
							sec := horizon + rng.Intn(30)
							if rng.Intn(3) == 0 {
								r := edgeTo(seq, ps[[]string{"a", "b"}[rng.Intn(2)]], "p", at(sec), lifecycle.Observe, 0)
								batch = append(batch, r)
								continue
							}
							h := rng.Intn(len(hosts))
							switch rng.Intn(6) {
							case 0: // reboots
								boot[h]++
							case 1: // a clone of an earlier machine reports an old boot
								boot[h] = max(0, boot[h]-1)
							}
							r := store.Record{
								Layer: catalog.L1, Subject: store.EntitySubject(hosts[h]), Producer: "agent", EventTime: at(sec), Seq: seq,
								Kind: lifecycle.Observe, Boot: fmt.Sprintf("boot-%d", boot[h]), Payload: []byte("host"),
							}
							if rng.Intn(5) == 0 {
								r.Kind, r.Boot, r.Payload = lifecycle.Delete, "", nil
							}
							batch = append(batch, r)
						}
						for _, x := range []store.Store{s, ref} {
							if err := x.Write(bg, cloneAll(batch)); err != nil {
								t.Fatalf("%v\n%v", err, log)
							}
						}
						log = append(log, fmt.Sprintf("write %d records up to seq %d", len(batch), seq))
					case op < 7:
						c := horizon + rng.Intn(40)
						var err error
						if rng.Intn(2) == 0 {
							err = s.Instrument().CheckpointEntity(catalog.L1, hosts[rng.Intn(len(hosts))], at(c))
						} else {
							err = s.Instrument().CheckpointEdges(catalog.L2, podFP, store.Forward, at(c))
						}
						if err != nil && !errors.Is(err, store.ErrInvalid) {
							t.Fatal(err)
						}
						log = append(log, fmt.Sprintf("checkpoint at %d, refused %v", c, err != nil))
					case op < 9:
						if nh := horizon + rng.Intn(15); nh > horizon {
							horizon = nh
							for _, x := range []store.Store{s, ref} {
								if err := x.Retain(bg, at(horizon)); err != nil && !errors.Is(err, errInjected) {
									t.Fatal(err)
								}
							}
							log = append(log, fmt.Sprintf("retain %d (last seq %d)", horizon, seq))
							if s.complete {
								if err := rememberedStateMismatch(s); err != nil {
									t.Fatalf("%v\n%v", err, log)
								}
								if n := rec.counters["retain.prefixes_kept_for_boots"]; n > keptBefore {
									completeAndKept++
								}
							}
							keptBefore = rec.counters["retain.prefixes_kept_for_boots"]
						}
					default:
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						s = open()
						log = append(log, "reopen")
					}
					for _, fp := range hosts {
						for _, d := range []int{0, 1, 10, 30, 80} {
							tm := at(horizon + d)
							for _, tok := range []uint64{store.Latest, seq} {
								sc := store.Scope{Layer: catalog.L1, AsOf: tok}
								got, gerr := s.Alive(bg, fp, tm, sc)
								want, werr := ref.Alive(bg, fp, tm, sc)
								a, b := quarantined(got, gerr), quarantined(want, werr)
								if a != b {
									t.Fatalf("step %d: Alive(%s, %d, asOf %d) = %s; the reference says %s\n%v", step, fp, tm.Sub(t0), tok, a, b, log)
								}
								if strings.HasPrefix(a, "quarantined") {
									seen++
								}
							}
						}
					}
					for _, fp := range []identity.Fingerprint{podFP, ps["a"], ps["b"]} {
						for _, d := range []int{0, 5, 20, 60} {
							tm := at(horizon + d)
							for _, dir := range []store.Direction{store.Forward, store.Reverse} {
								got, err := s.Neighbors(bg, fp, dir, tm, store.Current(catalog.L2))
								want, _ := ref.Neighbors(bg, fp, dir, tm, store.Current(catalog.L2))
								if err != nil || !slices.Equal(got, want) {
									t.Fatalf("step %d: Neighbors(%s, %s, %d) = %v, %v; the reference says %v\n%v", step, fp, dir, tm.Sub(t0), got, err, want, log)
								}
							}
						}
					}
				}
				mu.Lock()
				defer mu.Unlock()
				kept += rec.counters["retain.prefixes_kept_for_boots"]
				workedOut += rec.counters["retain.state_keys"]
				quarantines += seen
				keptAndComplete += completeAndKept
			})
		}
	})
	if kept == 0 || workedOut == 0 || quarantines == 0 || keptAndComplete == 0 {
		t.Fatalf("the histories kept %d prefixes whole for boots, read %d keys to work states out, saw %d quarantines and worked a state out in a retention that kept a prefix whole %d times: they do not exercise the rule", kept, workedOut, quarantines, keptAndComplete)
	}
	t.Logf("%d prefixes kept whole, %d keys read to work states out, %d quarantines seen, %d retentions that kept a prefix whole and worked states out", kept, workedOut, quarantines, keptAndComplete)
}

// memstoreWithPolicy is the reference store with a lifecycle policy.
func memstoreWithPolicy(p lifecycle.Policy) (*memstore.Store, error) {
	return memstore.Open(memstore.Options{Policy: p})
}
