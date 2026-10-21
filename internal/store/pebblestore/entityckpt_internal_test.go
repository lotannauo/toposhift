package pebblestore

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// # Entity checkpoints under a boot key
//
// Under a boot key Alive folds the whole prefix of a host, because the boots of a
// host are folded from every record it has. Only a host record carries a boot id
// (store.Record.Validate), so the prefix of any other entity has no sighting and no
// collision, and what the fold finds is what the walk back from the instant finds
// (the walk the store without a boot key takes, which stops at a checkpoint). These
// tests hold the store to that: the walk, the fold and the reference agree for the
// entities that are not hosts, whatever checkpoints stand in their prefixes, and a
// host is still folded whole.

// aliveByWalk is Alive as a store with no boot key answers it: the walk back from t,
// which stops at the newest checkpoint or baseline that the token can trust.
func aliveByWalk(s *Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	r, err := s.begin(bg, "Alive", t, sc, nil)
	if err != nil {
		return false, err
	}
	defer r.close()
	prefix, ok := s.prefixOf(sc.Layer, fp, dirEntity)
	if !ok {
		return false, nil
	}
	tNs, empty := instant(t)
	if empty {
		return false, nil
	}
	alive := false
	err = r.decide(prefix, seekKey(prefix, tNs), sc.AsOf, func(_ []byte, ns int64, v pebblekv.Value) (bool, error) {
		if v.Holds(ns, tNs) {
			alive = true
			return false, nil
		}
		return true, nil
	})
	return alive, err
}

// aliveByFold is Alive as the store folds the whole prefix under a boot key.
func aliveByFold(s *Store, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	r, err := s.begin(bg, "Alive", t, sc, nil)
	if err != nil {
		return false, err
	}
	defer r.close()
	prefix, ok := s.prefixOf(sc.Layer, fp, dirEntity)
	if !ok {
		return false, nil
	}
	return r.aliveFolded(prefix, fp, t, sc)
}

// entityCheckpointsOf counts the checkpoints on disk under the entity prefixes of
// the hosts and of the other entities.
func entityCheckpointsOf(t *testing.T, s *Store) (hosts, others int) {
	t.Helper()
	hostID, _ := pebblekv.Default.EntityID(catalog.Host)
	for prefix, cs := range checkpointsOnDisk(t, s) {
		if prefix[prefixLen-1] != dirEntity {
			continue
		}
		if uint16(prefix[1])<<8|uint16(prefix[2]) == uint16(hostID) {
			hosts += len(cs)
		} else {
			others += len(cs)
		}
	}
	return hosts, others
}

// TestEntityCheckpointsUnderABootKeyAnswerAsTheFoldedPrefix is the hinge of
// checkpointing the prefixes of entities that are not hosts under a boot key. After
// every step of a random history, for every entity that is not a host, Alive, the
// walk that stops at a checkpoint, the fold of the whole prefix and the reference
// give one answer, and WindowSince seeds the window's entities as the reference does.
// A host is answered as the reference answers it, quarantines included. The history
// writes late records (before the checkpoints there are), runs, deletes and several
// producers; places checkpoints by the policy and by the hook, on hosts as well;
// moves the horizon in pieces that sometimes stop halfway; and reopens the store
// with the policy off and on. Half of the seeds place no checkpoint by hand, so that
// what stands in a prefix is what the policy put there.
func TestEntityCheckpointsUnderABootKeyAnswerAsTheFoldedPrefix(t *testing.T) {
	if storetest.Trimmed() {
		t.Skip("trimmed run: the random histories run in the full tier")
	}
	t.Parallel()
	var mu sync.Mutex
	var placedByPolicy, hostPlacedByPolicy, hits, reads, quarantines, seedCalls, late, invalidated, midPass int64
	t.Run("seeds", func(t *testing.T) {
		seeds := int64(24)
		if raceEnabled {
			seeds = 4
		}
		for seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				rng := rand.New(rand.NewSource(seed))
				byHook := seed%2 == 0
				mode := Synchronous
				if seed%3 == 2 {
					mode = Background
				}
				lags := []time.Duration{0, time.Second, 3 * time.Second}
				ck := CheckpointOptions{On: true, KMin: 1 + rng.Intn(3), Lag: lags[rng.Intn(len(lags))]}
				cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
				rec := newLockedSampleRecorder()
				open := func() *Store {
					o := ck
					if rng.Intn(4) == 0 {
						o = CheckpointOptions{}
					}
					opts := Options{Config: cfg, Policy: storetest.QuarantinePolicy(), Checkpoints: policy(o), Recorder: rec, Retention: mode, retainBatchBytes: 1 + rng.Intn(2000)}
					if mode == Synchronous {
						opts.retainStopAfter = rng.Intn(3)
					} else {
						opts.retainBatchBytes, opts.retainChunkTime = 1, time.Hour // a chunk of one prefix, so that a pass is pending across steps
					}
					s, err := Open("db", opts)
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
				hosts := []identity.Fingerprint{fingerprintOf(catalog.Host, 0x60), fingerprintOf(catalog.Host, 0x61)}
				boot := make([]int, len(hosts))
				var others []identity.Fingerprint
				for i := range 3 {
					others = append(others,
						fingerprintOf(catalog.K8sPod, byte(0x10+i)),
						fingerprintOf(catalog.K8sNode, byte(0x30+i)),
						fingerprintOf(catalog.Container, byte(0x50+i)))
				}
				at := func(sec int) time.Time { return t0.Add(time.Duration(sec) * time.Second) }
				var seq, retainedSeq uint64
				horizon, newest := 0, 0
				var log []string
				var myMidPass, myPlaced, myHostPlaced, myReads, myQuar, mySeeds, myLate int64
				for step := range 60 {
					switch op := rng.Intn(10); {
					case op < 6:
						var batch []store.Record
						for range 1 + rng.Intn(4) {
							seq++
							var sec int
							if rng.Intn(3) == 0 { // late: before the newest record, so before the checkpoints there
								sec = max(horizon, newest-rng.Intn(25))
								myLate++
							} else {
								sec = max(horizon, newest) + rng.Intn(8)
							}
							newest = max(newest, sec)
							if rng.Intn(5) == 0 {
								h := rng.Intn(len(hosts))
								switch rng.Intn(6) {
								case 0:
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
								continue
							}
							fp := others[rng.Intn(len(others))]
							r := store.Record{
								Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: lifecycle.Producer([]string{"p", "q", "r"}[rng.Intn(3)]),
								EventTime: at(sec), Seq: seq, Kind: lifecycle.Observe, Payload: fmt.Appendf(nil, "v%d", seq),
							}
							switch {
							case rng.Intn(4) == 0:
								r.Kind, r.Payload = lifecycle.Delete, nil
							case rng.Intn(3) == 0:
								r.TTL = time.Duration(1+rng.Intn(8)) * time.Second
								if rng.Intn(2) == 0 { // a run: identical refreshes up to Through
									r.Through = at(sec + rng.Intn(6))
								}
							}
							batch = append(batch, r)
						}
						for _, x := range []store.Store{s, ref} {
							if err := x.Write(bg, cloneAll(batch)); err != nil {
								t.Fatalf("%v\n%v", err, log)
							}
						}
						log = append(log, fmt.Sprintf("write %d records up to seq %d, newest %d", len(batch), seq, newest))
					case op < 7 && byHook:
						c := horizon + rng.Intn(40)
						fp, l := others[rng.Intn(len(others))], catalog.L2
						if rng.Intn(4) == 0 {
							fp, l = hosts[rng.Intn(len(hosts))], catalog.L1
						}
						err := s.Instrument().CheckpointEntity(l, fp, at(c))
						if err != nil && !errors.Is(err, store.ErrInvalid) {
							t.Fatal(err)
						}
						log = append(log, fmt.Sprintf("checkpoint %s at %d, refused %v", fp, c, err != nil))
					case op < 9:
						if nh := horizon + rng.Intn(15); nh > horizon {
							horizon, retainedSeq = nh, seq
							for _, x := range []store.Store{s, ref} {
								if err := x.Retain(bg, at(horizon)); err != nil && !errors.Is(err, errInjected) {
									t.Fatal(err)
								}
							}
							log = append(log, fmt.Sprintf("retain %d (last seq %d)", horizon, seq))
							// A Background pass is left pending half the time, and read mid-pass.
							if mode == Background && rng.Intn(2) == 0 {
								break
							}
							if mode == Background {
								waitRetained(t, s)
							}
							waitScanned(t, s)
							s.mu.Lock()
							complete := s.complete
							err := rememberedStateMismatch(s)
							s.mu.Unlock()
							if complete && err != nil {
								t.Fatalf("%v\n%v", err, log)
							}
						}
					default:
						if err := s.Close(); err != nil {
							t.Fatal(err)
						}
						s = open()
						log = append(log, "reopen")
					}
					s.mu.Lock()
					_, _, err := exactStatesMismatch(s)
					s.mu.Unlock()
					if err != nil {
						t.Fatalf("step %d: %v\n%v", step, err, log)
					}
					s.mu.Lock()
					if s.pass != nil {
						myMidPass++
					}
					s.mu.Unlock()
					tokens := []uint64{store.Latest, seq, retainedSeq + (seq-retainedSeq)/2}
					for _, fp := range hosts {
						for _, d := range []int{0, 1, 10, 30, 80} {
							tm := at(horizon + d)
							for _, tok := range tokens {
								sc := store.Scope{Layer: catalog.L1, AsOf: tok}
								got, gerr := s.Alive(bg, fp, tm, sc)
								want, werr := ref.Alive(bg, fp, tm, sc)
								a, b := quarantined(got, gerr), quarantined(want, werr)
								if a != b {
									t.Fatalf("step %d: Alive(host %s, %d, asOf %d) = %s; the reference says %s\n%v", step, fp, tm.Sub(t0), tok, a, b, log)
								}
								if a != "true" && a != "false" {
									myQuar++
								}
							}
						}
					}
					for _, fp := range others {
						for _, d := range []int{0, 1, 3, 7, 12, 20, 30, 45, 80} {
							tm := at(horizon + d)
							for _, tok := range tokens {
								sc := store.Scope{Layer: catalog.L2, AsOf: tok}
								want, werr := ref.Alive(bg, fp, tm, sc)
								if werr != nil {
									t.Fatalf("the reference refused Alive(%s, %d, asOf %d): %v", fp, tm.Sub(t0), tok, werr)
								}
								got, gerr := s.Alive(bg, fp, tm, sc)
								walk, werr2 := aliveByWalk(s, fp, tm, sc)
								fold, ferr := aliveByFold(s, fp, tm, sc)
								if gerr != nil || werr2 != nil || ferr != nil || got != want || walk != want || fold != want {
									t.Fatalf("step %d: Alive(%s, %d, asOf %d) = %v, %v; by the walk %v, %v; by the fold %v, %v; the reference says %v\n%v",
										step, fp, tm.Sub(t0), tok, got, gerr, walk, werr2, fold, ferr, want, log)
								}
								myReads++
							}
						}
					}
					for i, d := range []int{0, 2, 9, 25} {
						from := at(horizon + d)
						sc := store.Scope{Layer: catalog.L2, AsOf: tokens[i%len(tokens)]}
						o := store.SinceOptions{Payloads: i%2 == 0}
						want, werr := ref.WindowSince(bg, others, 0, from, from.Add(30*time.Second), sc, o)
						got, gerr := s.WindowSince(bg, others, 0, from, from.Add(30*time.Second), sc, o)
						if (werr == nil) != (gerr == nil) || (werr != nil && !errors.Is(gerr, store.ErrBeforeHorizon)) {
							t.Fatalf("step %d: WindowSince(entities, from %d, asOf %d) = %v; the reference says %v\n%v", step, from.Sub(t0), sc.AsOf, gerr, werr, log)
						}
						if werr == nil {
							if !reflect.DeepEqual(got, want) {
								t.Fatalf("step %d: WindowSince(entities, from %d, asOf %d) =\n%+v\nthe reference says\n%+v\n%v", step, from.Sub(t0), sc.AsOf, got, want, log)
							}
							mySeeds++
						}
					}
					if !byHook {
						h, o := entityCheckpointsOf(t, s)
						myHostPlaced = max(myHostPlaced, int64(h))
						myPlaced = max(myPlaced, int64(o))
					}
				}
				mu.Lock()
				defer mu.Unlock()
				placedByPolicy += myPlaced
				hostPlacedByPolicy += myHostPlaced
				hits += rec.Counter("read.checkpoint_hits")
				invalidated += rec.Counter("checkpoint.invalidated")
				reads += myReads
				quarantines += myQuar
				seedCalls += mySeeds
				late += myLate
				midPass += myMidPass
			})
		}
	})
	if placedByPolicy == 0 || hits == 0 || quarantines == 0 || seedCalls == 0 || late == 0 || invalidated == 0 || midPass == 0 {
		t.Fatalf("the histories ended with %d checkpoints placed by the policy on entity prefixes that are not hosts, %d reads that used a checkpoint, %d quarantines, %d seeded windows, %d late records, %d checkpoints invalidated and %d steps read while a retention pass was pending: they do not exercise the rule",
			placedByPolicy, hits, quarantines, seedCalls, late, invalidated, midPass)
	}
	if hostPlacedByPolicy != 0 {
		t.Fatalf("the policy placed %d checkpoints on the prefix of a host under a boot key", hostPlacedByPolicy)
	}
	t.Logf("%d entity checkpoints placed by the policy outside hosts (none on a host), %d reads used a checkpoint, %d Alive reads of entities, %d quarantines, %d seeded windows, %d late records, %d checkpoints invalidated, %d steps read mid-pass",
		placedByPolicy, hits, reads, quarantines, seedCalls, late, invalidated, midPass)
}

// Only a host observation carries a boot id, whichever way it reaches the store: the
// equivalence above rests on it, because a boot id on another entity's record is a
// sighting the fold of the whole prefix would take and the walk would not.
func TestNoRecordButAHostObservationCarriesABootIDInTheStore(t *testing.T) {
	t.Parallel()
	pod, node := fingerprintOf(catalog.K8sPod, 0x10), fingerprintOf(catalog.K8sNode, 0x30)
	obs := func(seq uint64, fp identity.Fingerprint) store.Record {
		return store.Record{Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: "p", EventTime: sec(int(seq)), Seq: seq, Kind: lifecycle.Observe, Payload: []byte("x"), Boot: "boot-a"}
	}
	edge := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p", EventTime: sec(1), Seq: 1,
		Kind: lifecycle.Observe, Payload: []byte("x"), Boot: "boot-a",
	}
	for name, r := range map[string]store.Record{
		"a pod": obs(1, pod), "a node": obs(1, node), "a container": obs(1, fingerprintOf(catalog.Container, 0x50)), "an edge": edge,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, x := range []store.Store{openMem(t, Options{Policy: storetest.QuarantinePolicy()}), openMem(t, Options{})} {
				if err := x.Write(bg, []store.Record{r}); !errors.Is(err, store.ErrInvalid) {
					t.Errorf("Write of a record of %s with a boot id = %v; want ErrInvalid", name, err)
				}
				if x.LastSeq() != 0 {
					t.Errorf("the refused record was stored: LastSeq %d", x.LastSeq())
				}
			}
		})
	}
	host := obs(1, fingerprintOf(catalog.Host, 0x60))
	host.Layer = catalog.L1
	if err := openMem(t, Options{Policy: storetest.QuarantinePolicy()}).Write(bg, []store.Record{host}); err != nil {
		t.Errorf("Write of a host observation with a boot id: %v", err)
	}
}
