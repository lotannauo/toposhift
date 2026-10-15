package pebblestore

import (
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// These tests are about the exception to retain-on-touch: a state the publication of
// a retention leaves valid, because it is exactly what a read of its prefix finds and
// the retention cannot change that, and a write that leaves such a prefix's step to the
// pass. The claim is that no stored byte and no answer differs from a build that runs
// the step first: the oracle (runAsync) holds each run to the synchronous store, byte
// for byte, and to the reference, and the differential tests here hold the build with
// the exception to the build without it (asyncConfig.noKeep).

// newKeepWorld is newAsyncWorld with more prefixes that the exception reaches: every
// pod has records at or after the horizon before the retention, so that, with a
// checkpoint after it, its state is one that is kept, and the records written after
// the retention fall both above and below the newest checkpoint of the prefixes they
// reach.
func newKeepWorld(seed int64) *asyncWorld {
	w := newAsyncWorld(seed)
	rng := rand.New(rand.NewSource(seed*31 + 7))
	hNs := int(w.h.Sub(w.base))
	edge := func(i, at int, kind lifecycle.Kind) store.Record {
		layer := layerOfPod(i)
		r := store.Record{
			Layer: layer, Producer: "p", EventTime: w.at(at), Kind: kind,
			Subject: store.EdgeSubject(w.pods[i], w.nodes[rng.Intn(len(w.nodes))], catalog.ScheduledOn),
		}
		if kind == lifecycle.Observe {
			r.Payload = fmt.Appendf(nil, "k-%d-%d", i, at)
		}
		return r
	}
	entity := func(i, at int) store.Record {
		return store.Record{
			Layer: catalog.L2, Producer: "p", EventTime: w.at(at), Kind: lifecycle.Observe,
			Subject: store.EntitySubject(w.pods[i]), Payload: fmt.Appendf(nil, "e-%d-%d", i, at),
		}
	}
	var pre []store.Record
	for i := range w.pods {
		for range 2 + rng.Intn(3) {
			pre = append(pre, edge(i, hNs+5+rng.Intn(60), lifecycle.Observe))
		}
		if i%3 == 0 {
			pre = append(pre, entity(i, hNs+10+rng.Intn(50)))
		}
	}
	rng.Shuffle(len(pre), func(i, j int) { pre[i], pre[j] = pre[j], pre[i] })
	for len(pre) > 0 {
		n := min(len(pre), 1+rng.Intn(3))
		w.pre = append(w.pre, pre[:n])
		pre = pre[n:]
	}
	var later []store.Record
	for range 30 {
		i := rng.Intn(len(w.pods))
		kind := lifecycle.Observe
		if rng.Intn(5) == 0 {
			kind = lifecycle.Delete
		}
		later = append(later, edge(i, hNs+rng.Intn(90), kind))
		if rng.Intn(6) == 0 {
			later = append(later, entity(i, hNs+rng.Intn(90)))
		}
	}
	for len(later) > 0 {
		n := min(len(later), 1+rng.Intn(3))
		w.later = append(w.later, later[:n])
		later = later[n:]
	}
	w.renumber()
	return w
}

// warmStates makes the writer read, and so remember exactly, every prefix the database
// holds, as a store that has run for a while remembers the prefixes it touched.
func warmStates(t *testing.T, s *Store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.forgetAll()
	held, err := prefixesHoldingKeys(s)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range slices.Sorted(maps.Keys(held)) {
		if _, err := s.state([]byte(k)); err != nil {
			t.Fatal(err)
		}
	}
}

// keepPolicies are the checkpoint policies the differential tests run: a checkpoint
// at every record, one every second record three nanoseconds behind the newest, the
// same with a lag that leaves the newest checkpoint far behind, and one that also
// waits for a number of records worth the last checkpoint's size.
func keepPolicies() []*CheckpointOptions {
	return []*CheckpointOptions{
		policy(CheckpointOptions{On: true, KMin: 1}),
		policy(CheckpointOptions{On: true, KMin: 2, Lag: 3}),
		policy(CheckpointOptions{On: true, KMin: 1, Lag: 25}),
		policy(CheckpointOptions{On: true, KMin: 1, Alpha: 1.5, Lag: 3}),
	}
}

// keepTally adds up what a set of runs did.
type keepTally struct {
	mu                                 sync.Mutex
	kept, deferred, fallback, rewrites int64
	runs                               int
}

func (k *keepTally) add(res *asyncResult) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.kept += res.rec.counters["retain.states_kept"]
	k.deferred += res.rec.counters["retain.touch_deferred"]
	k.fallback += res.rec.counters["retain.touch_fallback"]
	k.rewrites += res.rec.counters["retain.touch_rewrites"]
	k.runs++
}

// The build with the exception and the build without it leave the same bytes, and
// both are the synchronous store's, on random histories with the writes falling
// between the chunks of the pass, under three checkpoint policies, with the writer's
// cache of states at its default and cut to a few states (which evicts the kept ones
// between two writes), and with chunks of one prefix and of several. The oracle
// (runAsync) compares each run with the synchronous store's bytes, with the
// reference's answers (during the pass too) and with a whole read of every state the
// writer holds, and compares the writer's state of every prefix a write put records
// in before the pass reached it with the synchronous store's.
func TestAKeptStateLeavesTheBytesAndTheAnswersOfARewriteFirst(t *testing.T) {
	t.Parallel()
	seeds := make([]int, 36)
	for i := range seeds {
		seeds[i] = i
	}
	if raceEnabled {
		// A few seeds that keep states, leave prefixes to the pass and fall back, as the
		// race detector finds no more in more of them and costs minutes.
		seeds = []int{2, 6, 17}
	}
	caps := []int{0, 8, 64}
	var with, plain, without keepTally
	t.Run("seeds", func(t *testing.T) {
		for _, seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				w := newKeepWorld(int64(seed))
				ck := keepPolicies()[seed%4]
				n := caps[(seed/3)%len(caps)]
				warm := seed%4 != 0
				cfg := asyncConfig{
					ck: ck, bytes: []int{1, 1, 300}[seed%3], exact: seed%2 == 0, noMidReads: raceEnabled,
					prep: func(t *testing.T, s *Store) {
						if warm {
							warmStates(t, s)
						}
						if n > 0 {
							s.Instrument().CapCachedStates(n)
						}
					},
				}
				// With writes to the prefix the pass is to take next (exact), the writes the two
				// builds get depend on how far each pass has come, so the data of the two is
				// compared without them; each is held to the synchronous store's in any case.
				with.add(runAsync(t, w, cfg))
				cfg.exact = false
				a := runAsync(t, newKeepWorld(int64(seed)), cfg)
				cfg.noKeep = true
				b := runAsync(t, newKeepWorld(int64(seed)), cfg)
				if a.digest != b.digest {
					t.Errorf("the build that keeps states leaves other data than the one that does not (policy %+v, cap %d)", *ck, n)
				}
				if n := b.rec.counters["retain.touch_deferred"] + b.rec.counters["retain.touch_fallback"]; n != 0 {
					t.Errorf("the build without the exception deferred or fell back %d times", n)
				}
				plain.add(a)
				without.add(b)
			})
		}
	})
	// This holds under the race detector too, whose fixed seeds were chosen to keep,
	// defer and fall back: it must not come to cover none of those paths unseen.
	if with.kept == 0 || with.deferred == 0 || with.fallback == 0 || with.deferred >= with.kept {
		t.Fatalf("%d runs kept %d states, left %d prefixes to the pass and rewrote %d of the kept ones first: the histories do not exercise the exception",
			with.runs, with.kept, with.deferred, with.fallback)
	}
	if plain.rewrites >= without.rewrites {
		t.Errorf("the build with the exception rewrote %d prefixes in writes, the one without %d: it saved none", plain.rewrites, without.rewrites)
	}
	t.Logf("%d runs: %d states kept, %d prefixes left to the pass, %d fallbacks; without writes to the next prefix, writes rewrote %d prefixes first, against %d without the exception",
		with.runs, with.kept, with.deferred, with.fallback, plain.rewrites, without.rewrites)
}

// The layers of a retention have horizons of their own (the horizon less the layer's
// offset), and a state is kept against its own layer's. The build with the exception
// and the one without leave the same bytes, and both the synchronous store's.
func TestKeptStatesAreKeptAgainstTheHorizonOfTheirOwnLayer(t *testing.T) {
	t.Parallel()
	offsets := [4]time.Duration{0, 15, 30, 45}
	seeds := 12
	if raceEnabled {
		seeds = 2
	}
	var kept, deferred int64
	t.Run("seeds", func(t *testing.T) {
		for seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				cfg := func(w *asyncWorld) asyncConfig {
					w.refused(w.h, offsets, [4]bool{})
					return asyncConfig{
						ck: keepPolicies()[seed%2], bytes: 1, noMidReads: raceEnabled,
						tweak: func(o *Options) { o.Offsets = offsets }, ref: memstore.Options{Offsets: offsets},
						prep: func(t *testing.T, s *Store) { warmStates(t, s) },
					}
				}
				w1, w2 := newKeepWorld(int64(seed)), newKeepWorld(int64(seed))
				a := runAsync(t, w1, cfg(w1))
				c2 := cfg(w2)
				c2.noKeep = true
				b := runAsync(t, w2, c2)
				if a.digest != b.digest {
					t.Error("the build that keeps states leaves other data than the one that does not")
				}
				atomic.AddInt64(&kept, a.rec.counters["retain.states_kept"])
				atomic.AddInt64(&deferred, a.rec.counters["retain.touch_deferred"])
			})
		}
	})
	if !raceEnabled && (kept == 0 || deferred == 0) {
		t.Fatalf("%d states were kept and %d prefixes left to the pass: the histories do not exercise the exception", kept, deferred)
	}
}

// Under a boot key the entity prefixes of hosts are never kept, and the edges' are.
func TestKeptStatesWithABootKeyLeaveTheBytesOfARewriteFirst(t *testing.T) {
	t.Parallel()
	for _, noKeep := range []bool{false, true} {
		t.Run(fmt.Sprint("without the exception: ", noKeep), func(t *testing.T) {
			t.Parallel()
			w := newBootWorld()
			// Records of the edges' pod and of the hosts at the horizon and after it, before the
			// retention, so that their prefixes have a state that stands above it.
			hNs := int(w.h.Sub(w.base))
			var recs []store.Record
			for i, at := range []int{hNs + 2, hNs + 3} {
				recs = append(recs, store.Record{
					Layer: catalog.L2, Subject: store.EdgeSubject(w.pods[3], w.nodes[0], catalog.ScheduledOn), Producer: "p",
					EventTime: w.at(at), Kind: lifecycle.Observe, Payload: []byte("e"),
				}, store.Record{
					Layer: catalog.L1, Subject: store.EntitySubject(w.pods[i]), Producer: "agent", EventTime: w.at(at),
					Kind: lifecycle.Observe, Boot: "boot-2", Payload: []byte("host"),
				})
			}
			w.pre = append(w.pre, recs)
			w.renumber()
			res := runAsync(t, w, asyncConfig{
				ck: policy(CheckpointOptions{On: true, KMin: 1}), slots: zeros(len(w.later)), noMidReads: raceEnabled, noKeep: noKeep,
				tweak: func(o *Options) { o.Policy = storetest.QuarantinePolicy() }, ref: memstore.Options{Policy: storetest.QuarantinePolicy()},
				prep: func(t *testing.T, s *Store) { warmStates(t, s) },
			})
			if !noKeep && res.rec.counters["retain.states_kept"] == 0 {
				t.Error("no state was kept")
			}
		})
	}
}
