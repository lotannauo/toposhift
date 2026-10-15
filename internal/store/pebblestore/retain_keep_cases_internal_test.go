package pebblestore

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// Directed cases of the exception to retain-on-touch (see retain_keep_internal_test.go
// for the random ones). Each is a scene: a Background store whose pass is held before
// its first chunk, the synchronous store that has finished the same retention, and the
// reference. A write goes to the three, and finish lets the pass run on and requires
// the background store to hold the synchronous store's bytes, true checkpoints, its
// remembered states as a whole read finds them and the reference's answers.

// keepScene: each pod has a node of its own, so that the prefix of the pod's forward
// edges and the prefix of the node's reverse edges hold the same records and nothing
// else. The pod i is in the layer layerOfPod(i). The horizon is instant 120.
type keepScene struct {
	t           *testing.T
	ck          *CheckpointOptions
	h           time.Time
	pods, nodes []identity.Fingerprint
	bg, syn     *Store
	ref         *memstore.Store
	bgFS, synFS vfs.FS
	pk          *parker
	rec         *memRecorder
	seq         uint64
	// deferred is what the pass had been left, when the pass was let go.
	deferred map[string]struct{}
	// lagged is how many states of the background store were exact with a count of their
	// own just before the retention.
	lagged int
}

// sceneConfig is how a scene is set up: the checkpoint policy, how many pods, a change
// to the options of both stores and to the reference's, a hook that runs on both
// stores before the background store's states are warmed and one that runs after what
// afterWarm returns is written, a cap on the states the background store keeps, and
// reopen, which closes both stores after the records of pre are written and opens
// them again with another policy.
type sceneConfig struct {
	ck        *CheckpointOptions
	pods      int
	tweak     func(*Options)
	ref       memstore.Options
	hook      func(t *testing.T, s *Store, sc *keepScene)
	afterWarm func(sc *keepScene) []store.Record
	afterHook func(t *testing.T, s *Store, sc *keepScene)
	cap       int
	reopen    *CheckpointOptions
}

func (sc *keepScene) rec1(pod, at int) store.Record {
	sc.seq++
	return edgeAt(layerOfPod(pod), sc.pods[pod], sc.nodes[pod], asyncAt(at), sc.seq)
}

// edges are records of the pod's edges at the instants.
func (sc *keepScene) edges(pod int, ats ...int) []store.Record {
	var out []store.Record
	for _, at := range ats {
		out = append(out, sc.rec1(pod, at))
	}
	return out
}

func (sc *keepScene) forward(pod int) []byte {
	p, ok := sc.bg.prefixOf(layerOfPod(pod), sc.pods[pod], byte(store.Forward))
	if !ok {
		sc.t.Fatal("the pod's type is not in the catalog")
	}
	return p
}

func (sc *keepScene) reverse(pod int) []byte {
	p, ok := sc.bg.prefixOf(layerOfPod(pod), sc.nodes[pod], byte(store.Reverse))
	if !ok {
		sc.t.Fatal("the node's type is not in the catalog")
	}
	return p
}

// newKeepScene writes the records pre returns to the three stores, warms what the
// background store remembers, writes what cfg says after it, retains at the horizon
// and holds the background pass before its first chunk.
func newKeepScene(t *testing.T, cfg sceneConfig, pre func(sc *keepScene) []store.Record) *keepScene {
	t.Helper()
	sc := &keepScene{t: t, ck: cfg.ck, h: asyncAt(120), pk: newParker(t, 1), rec: newMemRecorder(), bgFS: vfs.NewMem(), synFS: vfs.NewMem()}
	for i := range max(cfg.pods, 6) {
		sc.pods = append(sc.pods, fingerprintOf(catalog.K8sPod, byte(0x10+i*3)))
		sc.nodes = append(sc.nodes, fingerprintOf(catalog.K8sNode, byte(0x80+i*3)))
	}
	open := func(ck *CheckpointOptions) {
		so := syncOptions(sc.synFS, ck)
		bo := bgOptions(sc.bgFS, ck)
		bo.beforeRetainChunk, bo.Recorder = sc.pk.hook, sc.rec
		if cfg.tweak != nil {
			cfg.tweak(&so)
			cfg.tweak(&bo)
		}
		sc.syn, sc.bg = mustOpen(t, so), mustOpen(t, bo)
		t.Cleanup(sc.pk.release) // released before the store is closed (cleanups run last in, first out)
	}
	open(cfg.ck)
	ref, err := memstore.Open(cfg.ref)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ref.Close() })
	sc.ref = ref
	both := func(recs []store.Record) {
		for _, r := range recs {
			for _, x := range []store.Store{sc.bg, sc.syn, sc.ref} {
				if err := x.Write(bg, []store.Record{r}); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	both(pre(sc))
	if cfg.reopen != nil {
		if err := sc.bg.Close(); err != nil {
			t.Fatal(err)
		}
		if err := sc.syn.Close(); err != nil {
			t.Fatal(err)
		}
		sc.ck = cfg.reopen
		open(cfg.reopen)
	}
	if cfg.hook != nil {
		for _, s := range []*Store{sc.bg, sc.syn} {
			cfg.hook(t, s, sc)
		}
	}
	// Both stores remember what a read finds of every prefix, as a store that has run for
	// a while does; what the writer counts since a checkpoint is then the same in both.
	for _, s := range []*Store{sc.bg, sc.syn} {
		warmStates(t, s)
		if cfg.cap > 0 {
			s.Instrument().CapCachedStates(cfg.cap)
		}
	}
	if cfg.afterWarm != nil {
		both(cfg.afterWarm(sc))
	}
	if cfg.afterHook != nil {
		for _, s := range []*Store{sc.bg, sc.syn} {
			cfg.afterHook(t, s, sc)
		}
	}
	sc.bg.mu.Lock()
	for _, st := range sc.bg.states.all() {
		if st.exact && st.since != st.readSince {
			sc.lagged++
		}
	}
	sc.bg.mu.Unlock()
	for _, x := range []store.Store{sc.syn, sc.ref, sc.bg} {
		if err := x.Retain(bg, sc.h); err != nil {
			t.Fatal(err)
		}
	}
	sc.pk.wait(t)
	return sc
}

// write puts the records in the background store, one batch, and then in the
// synchronous store, which has finished its retention, and the reference.
func (sc *keepScene) write(recs ...store.Record) {
	sc.t.Helper()
	for _, x := range []store.Store{sc.bg, sc.syn, sc.ref} {
		if err := x.Write(bg, cloneAll(recs)); err != nil {
			sc.t.Fatal(err)
		}
	}
}

// both runs f on the background store and, once it has returned, on the synchronous one.
func (sc *keepScene) both(f func(s *Store) error) {
	sc.t.Helper()
	for _, s := range []*Store{sc.bg, sc.syn} {
		if err := f(s); err != nil {
			sc.t.Fatal(err)
		}
	}
}

// inPass runs f with the background store's lock held, on its pass.
func (sc *keepScene) inPass(f func(p *retainPass)) {
	sc.bg.mu.Lock()
	defer sc.bg.mu.Unlock()
	f(sc.bg.pass)
}

// leftToThePass is the prefixes a write has left to the pass, kept, and rewritten by a
// write.
func (sc *keepScene) sets() (deferred, keep, touched map[string]struct{}) {
	sc.t.Helper()
	sc.inPass(func(p *retainPass) {
		deferred, keep, touched = cloneSet(p.deferred), cloneSet(p.keep), cloneSet(p.touched)
	})
	return deferred, keep, touched
}

func cloneSet(m map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(m))
	for k := range m {
		out[k] = struct{}{}
	}
	return out
}

func has(m map[string]struct{}, prefix []byte) bool {
	_, ok := m[string(prefix)]
	return ok
}

// below is how many keys of the prefix are before the horizon, not counting a baseline.
func (sc *keepScene) below(prefix []byte) int {
	sc.t.Helper()
	hNs := sc.h.UnixNano()
	var n int
	for _, kv := range dataBytes(sc.t, sc.bg) {
		if kv[0][:prefixLen] != string(prefix) {
			continue
		}
		_, ns, _, kind, err := parseKey([]byte(kv[0]))
		if err != nil {
			sc.t.Fatal(err)
		}
		if ns < hNs && kind != kindBaseline {
			n++
		}
	}
	return n
}

// baselines is how many baselines the prefix holds.
func (sc *keepScene) baselines(prefix []byte) int {
	sc.t.Helper()
	var n int
	for _, kv := range dataBytes(sc.t, sc.bg) {
		if kv[0][:prefixLen] != string(prefix) {
			continue
		}
		if _, _, _, kind, err := parseKey([]byte(kv[0])); err != nil {
			sc.t.Fatal(err)
		} else if kind == kindBaseline {
			n++
		}
	}
	return n
}

func (sc *keepScene) counter(name string) int64 { return sc.rec.Counter(name) }

// requireMemory fails unless every state the background store remembers is what a read
// of the whole prefix finds now (with the writer's own counts, if the policy has no lag).
func (sc *keepScene) requireMemory() {
	sc.t.Helper()
	sc.bg.mu.Lock()
	err := asyncStateMismatch(sc.bg, sc.ck.Lag == 0)
	sc.bg.mu.Unlock()
	if err != nil {
		sc.t.Fatal(err)
	}
}

// answers fails unless the background store answers as the reference does.
func (sc *keepScene) answers(tokens ...uint64) {
	sc.t.Helper()
	var fps []identity.Fingerprint
	fps = append(append(fps, sc.pods...), sc.nodes...)
	if err := asyncAnswers(sc.bg, sc.ref, fps, asyncTimes(sc.h), append(tokens, store.Latest, sc.bg.LastSeq())); err != nil {
		sc.t.Fatal(err)
	}
}

// finish lets the pass run to its end and requires what the scene's comment says.
func (sc *keepScene) finish() { sc.t.Helper(); sc.finishAs(true) }

// finishAs is finish, with the bytes held to the synchronous store's only if same is
// set. They cannot be where a checkpoint was built on a prefix the pass had not
// reached, which walks the keys the step removes in a Background store, whoever left it
// there.
func (sc *keepScene) finishAs(same bool) {
	sc.t.Helper()
	sc.requireMemory()
	sc.answers()
	sc.deferred, _, _ = sc.sets()
	sc.pk.release()
	waitRetained(sc.t, sc.bg)
	if m := readMarker(sc.t, sc.bg); m != nil {
		sc.t.Errorf("the marker %+v is left", m)
	}
	kinds := []byte{kindRecord, kindCheckpoint, kindBaseline}
	if !same {
		kinds = kinds[:1:1]
		kinds = append(kinds, kindBaseline)
	}
	if d := diffData(sc.t, dataBytes(sc.t, sc.bg), dataBytes(sc.t, sc.syn), kinds...); d != "" {
		sc.t.Fatalf("the data differs from the synchronous store's: %s", d)
	}
	if err := asyncCheckpointsTrue(sc.bg); err != nil {
		sc.t.Fatalf("a stored checkpoint is not true: %v", err)
	}
	sc.requireMemory()
	sc.answers()
	// The state the writer holds of a prefix a write left to the pass is the one it
	// evolved, and the synchronous store's: the pass puts none over it.
	sc.bg.mu.Lock()
	defer sc.bg.mu.Unlock()
	sc.syn.mu.Lock()
	defer sc.syn.mu.Unlock()
	for k := range sc.deferred {
		a, b := sc.bg.states.peek(k), sc.syn.states.peek(k)
		if a != nil && b != nil && !sameState(a, b) {
			sc.t.Errorf("the prefix %x, left to the pass, is remembered as %+v; the synchronous store remembers %+v", k, *a, *b)
		}
	}
}

// hookAt places checkpoints, by the instrument, on the forward prefix of each pod and
// the reverse prefix of its node, at the instants the pod has in at.
func hookAt(at map[int][]int) func(t *testing.T, s *Store, sc *keepScene) {
	return func(t *testing.T, s *Store, sc *keepScene) {
		t.Helper()
		for pod, instants := range at {
			for _, c := range instants {
				if err := s.Instrument().CheckpointEdges(layerOfPod(pod), sc.pods[pod], store.Forward, asyncAt(c)); err != nil {
					t.Fatal(err)
				}
				if err := s.Instrument().CheckpointEdges(layerOfPod(pod), sc.nodes[pod], store.Reverse, asyncAt(c)); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}

// ckptAt places one by the instrument on the forward prefix of the pod.
func (sc *keepScene) ckptAt(pod, at int) {
	sc.t.Helper()
	sc.both(func(s *Store) error {
		return s.Instrument().CheckpointEdges(layerOfPod(pod), sc.pods[pod], store.Forward, asyncAt(at))
	})
}

// threeAtATime is a policy that places a checkpoint after each third record, at the
// instant of the newest (no lag), so that a prefix of a few records has one or two.
func threeAtATime() *CheckpointOptions {
	return policy(CheckpointOptions{On: true, KMin: 3})
}

// preOfPod is records of the pod at 10, 20 and 30 (a checkpoint at 31 under
// threeAtATime) and at 130, 135 and 140 (one at 141, and none between it and the
// horizon at 120).
func preOfPod(sc *keepScene, pod int) []store.Record {
	return sc.edges(pod, 10, 20, 30, 130, 135, 140)
}

// What the publication keeps: a state that is exact, whose newest checkpoint is after
// the horizon (not at it) and whose newest record is at or after it, and no other. A
// kept state stands on its newest checkpoint alone and takes the counts a read would
// find since it, which is what the read that follows the step finds.
func TestThePublicationKeepsTheStatesTheStepCannotChange(t *testing.T) {
	t.Parallel()
	// No checkpoint is due by the policy: the instrument places all of them.
	ck := policy(CheckpointOptions{On: true, KMin: 1000})
	sc := newKeepScene(t, sceneConfig{
		ck: ck, pods: 7,
		hook: hookAt(map[int][]int{0: {130}, 1: {120}, 2: {130}, 3: {30}, 5: {130}}),
		// A checkpoint placed by hand after the state is read makes it not exact.
		afterHook: hookAt(map[int][]int{5: {135}}),
	}, func(sc *keepScene) []store.Record {
		var recs []store.Record
		recs = append(recs, sc.edges(0, 10, 20, 125)...) // 0: newest checkpoint 130, newest record 125: kept
		recs = append(recs, sc.edges(1, 10, 20, 125)...) // 1: newest checkpoint exactly at the horizon
		recs = append(recs, sc.edges(2, 10, 20)...)      // 2: checkpoint after the horizon, newest record before it
		recs = append(recs, sc.edges(3, 10, 20)...)      // 3: checkpoint before the horizon
		recs = append(recs, sc.edges(4, 125, 126)...)    // 4: no checkpoint
		recs = append(recs, sc.edges(5, 10, 20, 125)...) // 5: as 0, but its state is not exact when the retention comes
		recs = append(recs, sc.edges(6, 125)...)         // 6: no checkpoint
		return recs
	})
	defer sc.finish()
	sc.bg.mu.Lock()
	defer sc.bg.mu.Unlock()
	kept := sc.bg.pass.keep
	for pod := range 7 {
		for _, prefix := range [][]byte{sc.forward(pod), sc.reverse(pod)} {
			isKept := has(kept, prefix)
			st := sc.bg.states.peek(string(prefix))
			if want := pod == 0; isKept != want {
				t.Errorf("pod %d, prefix %x: kept %v, want %v", pod, prefix, isKept, want)
			}
			if isKept != (st != nil) {
				t.Errorf("pod %d, prefix %x: a state is held (%v) where kept is %v: every other state of a moved layer is stale", pod, prefix, st != nil, isKept)
			}
			if isKept && st != nil {
				c := asyncAt(130).UnixNano()
				if !slices.Equal(st.ckpts, []int64{c}) || st.below != c || !st.exact || st.since != st.readSince || st.sinceBytes != st.readSinceBytes {
					t.Errorf("pod %d, prefix %x: the kept state is %+v, want one standing on the checkpoint at %d alone, exact, with the read's counts", pod, prefix, *st, c)
				}
			}
		}
	}
	if got := sc.rec.Counter("retain.states_kept"); got != int64(len(kept)) || got != 2 {
		t.Errorf("retain.states_kept = %d with %d prefixes kept, want 2 (both copies of the edges of one pod)", got, len(kept))
	}
}

// With a lag, the writer's own count of the records since its last checkpoint is
// behind what a read finds. A kept state takes the read's, as the read the step is
// followed by does, so that the checkpoint after it falls due where it does there.
func TestAKeptStateTakesTheCountsOfAReadAndNotTheWritersOwn(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{
		ck:        policy(CheckpointOptions{On: true, KMin: 2, Lag: 3}),
		afterWarm: func(sc *keepScene) []store.Record { return sc.edges(0, 127) },
	}, func(sc *keepScene) []store.Record { return sc.edges(0, 10, 20, 125, 126) })
	if sc.lagged == 0 {
		t.Fatal("no state has a count of its own that differs from a read's: the scene does not show the difference")
	}
	deferred, keep, _ := sc.sets()
	if !has(keep, sc.forward(0)) {
		t.Fatalf("the state of the prefix is not kept (%d kept)", len(keep))
	}
	if st := stateOf(sc.bg, sc.forward(0)); st == nil || st.since != st.readSince || st.since == 0 {
		t.Errorf("the kept state is %+v: want the count of the read, not the writer's", st)
	}
	if len(deferred) != 0 {
		t.Fatal("a prefix was deferred before the first write")
	}
	// A record at the newest checkpoint's instant (125) leaves the prefix due with the
	// checkpoint it already has: nothing is built, and the step is left to the pass.
	sc.write(sc.rec1(0, 126))
	if n, f := sc.counter("retain.touch_deferred"), sc.counter("retain.touch_fallback"); n != 2 || f != 0 {
		t.Errorf("%d prefixes were left to the pass and %d fell back, want the two copies of the pod's edges and none", n, f)
	}
	// The count makes the next record due a checkpoint, placed on a prefix whose step is
	// still to come, and the bytes are the synchronous store's.
	sc.write(sc.rec1(0, 128), sc.rec1(0, 129))
	if n, f := sc.counter("retain.touch_deferred"), sc.counter("retain.touch_fallback"); n != 2 || f != 0 {
		t.Errorf("%d prefixes were left to the pass and %d fell back after the second write", n, f)
	}
	sc.finish()
}

// A write to a kept prefix does not run the pass's step first: it reads nothing, its
// records come beside the keys the step has yet to remove, and the pass runs the step
// when it reaches the prefix, to the bytes the synchronous store has.
func TestAWriteToAKeptPrefixLeavesItsStepToThePass(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: policy(CheckpointOptions{On: true, KMin: 1})}, func(sc *keepScene) []store.Record {
		return append(sc.edges(0, 10, 20, 130, 140), sc.edges(1, 10, 20, 130, 140)...)
	})
	fwd, rev, other := sc.forward(0), sc.reverse(0), sc.forward(1)
	loads := sc.counter("checkpoint.loads")
	belowBefore := sc.below(fwd)
	if belowBefore == 0 {
		t.Fatal("the prefix holds nothing before the horizon")
	}
	sc.write(sc.rec1(0, 150))
	deferred, keep, touched := sc.sets()
	if !has(deferred, fwd) || !has(deferred, rev) || len(deferred) != 2 || len(touched) != 0 {
		t.Errorf("%d prefixes deferred and %d rewritten by the write; want the pod's two, and none rewritten", len(deferred), len(touched))
	}
	if !has(keep, other) || has(deferred, other) {
		t.Error("the other pod's prefix is not kept and untouched")
	}
	if got := sc.counter("retain.touch_deferred"); got != 2 {
		t.Errorf("retain.touch_deferred = %d, want 2", got)
	}
	if sc.counter("retain.touch_rewrites") != 0 || sc.counter("retain.touch_fallback") != 0 {
		t.Errorf("a write that left the step to the pass rewrote %d prefixes and fell back %d times", sc.counter("retain.touch_rewrites"), sc.counter("retain.touch_fallback"))
	}
	if got := sc.counter("checkpoint.loads"); got != loads {
		t.Errorf("the write read %d prefixes, want none: the state was kept", got-loads)
	}
	if n, b := sc.below(fwd), sc.baselines(fwd); n != belowBefore || b != 0 {
		t.Errorf("the write rewrote the prefix: %d keys before the horizon (were %d) and %d baselines", n, belowBefore, b)
	}
	if st := stateOf(sc.bg, fwd); st == nil || !st.exact {
		t.Errorf("the state of the deferred prefix is %+v, want it kept and exact", st)
	}
	// A second write to it is left to the pass too, and the counts stay.
	sc.write(sc.rec1(0, 160))
	if got := sc.counter("retain.touch_deferred"); got != 2 {
		t.Errorf("retain.touch_deferred = %d after a second write to the same prefixes, want 2", got)
	}
	sc.finish()
	if sc.below(fwd) != 0 || sc.baselines(fwd) != 1 {
		t.Error("the pass left the deferred prefix unrewritten")
	}
	if len(sc.deferred) != 2 {
		t.Errorf("%d prefixes were deferred, want 2", len(sc.deferred))
	}
}

// A record before the newest checkpoint of the state deletes it, and the state no
// longer stands above the horizon: the step runs first, in the write's batch, as it
// does for any prefix the pass owes.
func TestARecordBeforeTheNewestCheckpointRunsTheStepFirst(t *testing.T) {
	t.Parallel()
	for name, deferFirst := range map[string]bool{"at the first write": false, "after the prefix was deferred": true} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			sc := newKeepScene(t, sceneConfig{ck: policy(CheckpointOptions{On: true, KMin: 1})}, func(sc *keepScene) []store.Record {
				return sc.edges(0, 10, 20, 130, 140)
			})
			fwd := sc.forward(0)
			if deferFirst {
				sc.write(sc.rec1(0, 150))
				if sc.below(fwd) == 0 {
					t.Fatal("the first write rewrote the prefix")
				}
			}
			// 135 is at or after the horizon and before the newest checkpoint, at 151 or 141.
			sc.write(sc.rec1(0, 135))
			_, _, touched := sc.sets()
			if !has(touched, fwd) || sc.below(fwd) != 0 || sc.baselines(fwd) != 1 {
				t.Errorf("the prefix was not rewritten by the write: touched %v, %d keys before the horizon, %d baselines", has(touched, fwd), sc.below(fwd), sc.baselines(fwd))
			}
			if got := sc.counter("retain.touch_fallback"); got != 2 {
				t.Errorf("retain.touch_fallback = %d, want 2 (both copies of the pod's edges)", got)
			}
			deferred := int64(0)
			if deferFirst {
				deferred = 2
			}
			if sc.counter("retain.touch_deferred") != deferred || sc.counter("retain.touch_rewrites") != 2-deferred {
				t.Errorf("%d deferred and %d rewritten, want %d and %d: each prefix is counted once", sc.counter("retain.touch_deferred"), sc.counter("retain.touch_rewrites"), deferred, 2-deferred)
			}
			sc.finish()
		})
	}
}

// Records that make a prefix due for a checkpoint placed with a larger lag than the
// one that placed its newest, so that it falls before it, and no checkpoint known between
// the horizon and it, run the step first: the walk that builds it would go on into the
// keys the step removes, and the checkpoint would hold what it finds there, where the
// step's baseline stands in the synchronous store. This is a store opened again with
// another lag.
func TestARecordThatMakesADueCheckpointBeforeTheNewestRunsTheStepFirst(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{
		ck:     threeAtATime(),
		reopen: policy(CheckpointOptions{On: true, KMin: 3, Lag: 3}),
	}, func(sc *keepScene) []store.Record { return preOfPod(sc, 0) })
	fwd := sc.forward(0)
	if _, keep, _ := sc.sets(); !has(keep, fwd) {
		t.Fatal("the state of the prefix is not kept")
	}
	// Three records at the instant of the newest checkpoint (at 141): none is before it,
	// the prefix is due, and the checkpoint would be at 139, with nothing known between
	// 120 and it.
	sc.write(sc.rec1(0, 141), sc.rec1(0, 141), sc.rec1(0, 141))
	_, _, touched := sc.sets()
	if !has(touched, fwd) || sc.below(fwd) != 0 {
		t.Errorf("the step was not run first: touched %v, %d keys before the horizon", has(touched, fwd), sc.below(fwd))
	}
	if got := sc.counter("retain.touch_fallback"); got != 2 && got != 1 {
		t.Errorf("retain.touch_fallback = %d, want the prefix that is due to fall back", got)
	}
	sc.finish()
}

// The same records, with a checkpoint known above the horizon and below the one placed,
// leave the step to the pass: the build stops there.
func TestARecordThatMakesACheckpointAboveAKnownOneLeavesTheStepToThePass(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: threeAtATime()}, func(sc *keepScene) []store.Record { return preOfPod(sc, 0) })
	fwd := sc.forward(0)
	sc.write(sc.rec1(0, 150), sc.rec1(0, 151), sc.rec1(0, 152)) // due, at 153, above the one at 141
	deferred, _, touched := sc.sets()
	if !has(deferred, fwd) || has(touched, fwd) || sc.below(fwd) == 0 {
		t.Errorf("deferred %v, rewritten %v, %d keys before the horizon: the step should be left to the pass", has(deferred, fwd), has(touched, fwd), sc.below(fwd))
	}
	if n := sc.counter("retain.touch_fallback"); n != 0 {
		t.Errorf("%d fallbacks", n)
	}
	sc.finish()
}

// A checkpoint the instrument places on a prefix whose step was left to the pass runs
// the step first, in a commit of its own: the checkpoint is built from the keys the
// step leaves.
func TestACheckpointPlacedByHandOnADeferredPrefixRunsTheStepFirst(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: threeAtATime()}, func(sc *keepScene) []store.Record { return preOfPod(sc, 0) })
	fwd := sc.forward(0)
	sc.write(sc.rec1(0, 150))
	deferred, _, _ := sc.sets()
	if !has(deferred, fwd) || sc.below(fwd) == 0 {
		t.Fatal("the prefix was not left to the pass")
	}
	// At 138: above the horizon, below the newest checkpoint (141), with none between.
	sc.ckptAt(0, 138)
	if _, _, touched := sc.sets(); !has(touched, fwd) || sc.below(fwd) != 0 {
		t.Errorf("the step was not run before the checkpoint: touched %v, %d keys before the horizon", has(touched, fwd), sc.below(fwd))
	}
	sc.finish()
}

// A kept prefix that is not deferred is as any prefix the pass owes when a checkpoint
// is placed on it by hand.
func TestACheckpointPlacedByHandOnAKeptPrefixLeavesItsStateNotExactAndItsStepToTheWrite(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: threeAtATime()}, func(sc *keepScene) []store.Record { return preOfPod(sc, 0) })
	fwd := sc.forward(0)
	sc.ckptAt(0, 138)
	if st := stateOf(sc.bg, fwd); st == nil || st.exact {
		t.Fatalf("the state is %+v after a checkpoint by hand, want one that is not exact", st)
	}
	// The state is no longer one the pass can leave alone: the write rewrites first and
	// reads the prefix.
	sc.write(sc.rec1(0, 150))
	deferred, _, touched := sc.sets()
	if has(deferred, fwd) || !has(touched, fwd) || sc.below(fwd) != 0 {
		t.Errorf("deferred %v, rewritten %v, %d keys before the horizon: the write should have rewritten the prefix", has(deferred, fwd), has(touched, fwd), sc.below(fwd))
	}
	// The checkpoint by hand was built on a prefix the pass had not reached, in a store
	// with or without the exception.
	sc.finishAs(false)
}

// A state that the cache evicts between two writes is read when it is next touched,
// and a prefix whose state is gone before its first write is rewritten first.
func TestEvictingAKeptStateBetweenTwoWritesChangesNoByte(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: threeAtATime(), pods: 8}, func(sc *keepScene) []store.Record {
		var recs []store.Record
		for pod := range 4 {
			recs = append(recs, preOfPod(sc, pod)...)
		}
		return recs
	})
	f0, f1, f2 := sc.forward(0), sc.forward(1), sc.forward(2)
	sc.write(sc.rec1(0, 150))
	if deferred, _, _ := sc.sets(); !has(deferred, f0) {
		t.Fatal("the prefix was not left to the pass")
	}
	// Only the deferred prefix's own state and one more are kept; the rest are evicted.
	sc.bg.Instrument().CapCachedStates(1)
	if st := stateOf(sc.bg, f0); st != nil && stateOf(sc.bg, f1) != nil && stateOf(sc.bg, f2) != nil {
		t.Fatal("the cache evicted nothing")
	}
	// The first write to a prefix whose state is gone rewrites it first, as any prefix.
	for pod := 1; pod < 3; pod++ {
		if stateOf(sc.bg, sc.forward(pod)) == nil {
			before := sc.counter("retain.touch_rewrites")
			sc.write(sc.rec1(pod, 150))
			if sc.counter("retain.touch_rewrites") == before {
				t.Errorf("a write to the prefix of pod %d, whose state was evicted, did not rewrite it", pod)
			}
		}
	}
	// A second write to the deferred prefix reads its state afresh, and is still left to
	// the pass.
	sc.write(sc.rec1(0, 151), sc.rec1(0, 152))
	if _, _, touched := sc.sets(); has(touched, f0) {
		t.Error("a write to the deferred prefix with its state evicted rewrote it")
	}
	sc.finish()
}

// A write that fails before its commit leaves nothing deferred: the prefixes are the
// pass's, as any, and what the writer remembers is dropped.
func TestAWriteThatFailsAfterDecidingToDeferLeavesNothingDeferred(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: policy(CheckpointOptions{On: true, KMin: 1})}, func(sc *keepScene) []store.Record {
		return sc.edges(0, 10, 20, 130, 140)
	})
	r := sc.rec1(0, 150)
	ctx := &countdownCtx{Context: bg}
	ctx.left.Store(1) // the first look at it finds nothing, the one before the commit a cancellation
	if err := sc.bg.Write(ctx, []store.Record{r}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Write = %v, want the cancellation", err)
	}
	deferred, _, touched := sc.sets()
	if len(deferred) != 0 || len(touched) != 0 {
		t.Errorf("a write that stored nothing left %d prefixes deferred and %d touched", len(deferred), len(touched))
	}
	for _, name := range []string{"retain.touch_deferred", "retain.touch_fallback", "retain.touch_rewrites"} {
		if n := sc.counter(name); n != 0 {
			t.Errorf("%s = %d after a write that stored nothing", name, n)
		}
	}
	// The write that follows, which finds the writer's memory dropped, rewrites first, and
	// everything ends as it does in the synchronous store.
	sc.write(r)
	sc.finish()
}

// A store closed with prefixes left to the pass, and one that crashes with them, are
// finished by the next opening, in either mode, to the bytes of an uninterrupted
// retention: nothing was lost with the memory of what was deferred.
func TestAStoreClosedOrCrashedWithPrefixesLeftToThePassIsFinishedByTheNextOpening(t *testing.T) {
	t.Parallel()
	for _, mode := range []RetentionMode{Background, Synchronous} {
		for _, how := range []string{"closed", "crashed"} {
			t.Run(retentionName(mode)+"/"+how, func(t *testing.T) {
				t.Parallel()
				fs := vfs.NewCrashableMem()
				ck := threeAtATime()
				sc := newKeepScene(t, sceneConfig{ck: ck, pods: 8, tweak: func(o *Options) {
					if o.Retention == Background {
						o.FS = fs
						o.Sync = true
					}
				}}, func(sc *keepScene) []store.Record {
					var recs []store.Record
					for pod := range 4 {
						recs = append(recs, preOfPod(sc, pod)...)
					}
					return recs
				})
				sc.write(sc.rec1(0, 150), sc.rec1(1, 150), sc.rec1(2, 155))
				if deferred, _, _ := sc.sets(); len(deferred) == 0 {
					t.Fatal("nothing was left to the pass")
				}
				var disk vfs.FS = fs
				switch how {
				case "crashed":
					if err := sc.bg.kv.LogData(nil, pebble.Sync); err != nil {
						t.Fatal(err)
					}
					disk = fs.CrashClone(vfs.CrashCloneCfg{})
				case "closed":
					done := make(chan error, 1)
					go func() { done <- sc.bg.Close() }()
					sc.pk.release()
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				}
				o := bgOptions(disk, ck)
				o.Retention = mode
				r := mustOpen(t, o)
				waitRetained(t, r)
				if m := readMarker(t, r); m != nil {
					t.Errorf("the marker %+v is left", m)
				}
				if d := diffData(t, dataBytes(t, r), dataBytes(t, sc.syn), kindRecord, kindBaseline); d != "" {
					t.Errorf("the data differs from the uninterrupted retention's: %s", d)
				}
				if err := asyncCheckpointsTrue(r); err != nil {
					t.Error(err)
				}
				var fps []identity.Fingerprint
				fps = append(append(fps, sc.pods...), sc.nodes...)
				if err := asyncAnswers(r, sc.ref, fps, asyncTimes(sc.h), []uint64{store.Latest, r.LastSeq()}); err != nil {
					t.Error(err)
				}
			})
		}
	}
}

func retentionName(m RetentionMode) string {
	if m == Background {
		return "background"
	}
	return "synchronous"
}

// A second Retain while a pass is pending starts it over at the new horizon: what was
// left to the older pass is the new one's to rewrite, the states are kept or not
// against the new horizon, and the bytes are those of two retentions.
func TestASecondRetainWhilePrefixesAreLeftToThePassStartsOver(t *testing.T) {
	t.Parallel()
	sc := newKeepScene(t, sceneConfig{ck: threeAtATime(), pods: 8}, func(sc *keepScene) []store.Record {
		var recs []store.Record
		for pod := range 3 {
			recs = append(recs, preOfPod(sc, pod)...)
		}
		return recs
	})
	f0, f1, f2 := sc.forward(0), sc.forward(1), sc.forward(2)
	sc.write(sc.rec1(0, 150), sc.rec1(1, 150), sc.rec1(1, 151), sc.rec1(1, 152))
	if deferred, _, _ := sc.sets(); !has(deferred, f0) || !has(deferred, f1) {
		t.Fatal("the prefixes were not left to the pass")
	}
	// The second horizon, 145, is after the newest checkpoint of pods 0 and 2 (141) and
	// before that of pod 1 (153).
	h2 := asyncAt(145)
	if err := sc.syn.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	if err := sc.ref.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	if err := sc.bg.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	deferred, keep, touched := sc.sets()
	if len(deferred) != 0 || len(touched) != 0 {
		t.Errorf("the new pass starts with %d prefixes deferred and %d touched", len(deferred), len(touched))
	}
	if has(keep, f0) || !has(keep, f1) || has(keep, f2) {
		t.Errorf("kept against the new horizon: pod 0 %v, pod 1 %v, pod 2 %v; want pod 1 only (the newest checkpoints of the others are before it)", has(keep, f0), has(keep, f1), has(keep, f2))
	}
	sc.h = h2
	before := sc.counter("retain.touch_deferred")
	sc.write(sc.rec1(1, 160), sc.rec1(0, 160))
	if got := sc.counter("retain.touch_deferred") - before; got != 2 {
		t.Errorf("%d prefixes were left to the new pass, want pod 1's two (pod 0's are rewritten first)", got)
	}
	sc.finish()
}

// A layer the new Retain does not move is carried over at its own horizon, and its
// states are kept against that one: here a store opened with the last layer kept
// resumes a pass that holds it.
func TestACarriedOverLayerKeepsItsStatesAgainstItsOwnHorizon(t *testing.T) {
	t.Parallel()
	w := newKeepWorld(3)
	ck := policy(CheckpointOptions{On: true, KMin: 1})
	h1, h2 := w.h, w.h.Add(300) // after every record the world has and the one written below
	fs := vfs.NewMem()
	stopped := syncOptions(fs, ck)
	stopped.retainStopAfter = 1
	s, err := Open("db", stopped)
	if err != nil {
		t.Fatal(err)
	}
	for _, batch := range w.pre {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Retain(bg, h1); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v, want the injected failure", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	pk := newParker(t, 1)
	rec := newMemRecorder()
	bo := bgOptions(fs, ck)
	bo.Keep[3] = true
	bo.beforeRetainChunk, bo.Recorder = pk.hook, rec
	bgs := mustOpen(t, bo)
	t.Cleanup(pk.release)
	pk.wait(t)
	// The pass that Open resumed holds every layer. A write to a prefix of the last
	// layer rewrites it at the first horizon and leaves a state that stands above it.
	hNs := int(h1.Sub(w.base))
	r := edgeAt(catalog.L3, w.pods[3], w.nodes[0], w.at(hNs+150), bgs.LastSeq()+1)
	sides, err := bgs.sidesOf(r)
	if err != nil {
		t.Fatal(err)
	}
	target := sides[0].prefix
	if err := bgs.Write(bg, []store.Record{r}); err != nil {
		t.Fatal(err)
	}
	if st := stateOf(bgs, target); st == nil || !st.exact || len(st.ckpts) == 0 {
		t.Fatalf("the state of the prefix is %+v, want an exact one with a checkpoint", st)
	}
	if err := bgs.Retain(bg, h2); err != nil {
		t.Fatal(err)
	}
	m := marked(t, bgs)
	var carried bool
	for _, l := range m.layers {
		if l.layer == catalog.L3 && l.horizon.Equal(h1) {
			carried = true
		}
	}
	if !carried {
		t.Fatalf("the layer is not carried over at its own horizon: %+v", m.layers)
	}
	bgs.mu.Lock()
	keep := bgs.pass.keep
	var inLayers [layers]int
	for k := range keep {
		inLayers[int(layerOfPrefix([]byte(k)))-int(catalog.L0)]++
	}
	bgs.mu.Unlock()
	// Every other layer moved to a horizon after all its records: none stands above it.
	if inLayers[3] == 0 || inLayers[0]+inLayers[1]+inLayers[2] != 0 {
		t.Errorf("states kept by layer %v: want some in the carried-over layer, against its own horizon, and none in the layers moved after every record", inLayers)
	}
	pk.release()
	waitRetained(t, bgs)
}

// Under a boot key an entity's prefix is never kept, even with a checkpoint by hand
// and a state that is exact.
func TestAHostsPrefixUnderABootKeyIsNeverKept(t *testing.T) {
	t.Parallel()
	policyOf := storetest.QuarantinePolicy()
	host := fingerprintOf(catalog.Host, 0x60)
	sc := newKeepScene(t, sceneConfig{
		ck:    policy(CheckpointOptions{On: true, KMin: 1000}),
		tweak: func(o *Options) { o.Policy = policyOf },
		ref:   memstore.Options{Policy: policyOf},
		hook: func(t *testing.T, s *Store, sc *keepScene) {
			if err := s.Instrument().CheckpointEntity(catalog.L1, host, asyncAt(130)); err != nil {
				t.Fatal(err)
			}
			hookAt(map[int][]int{0: {130}})(t, s, sc)
		},
	}, func(sc *keepScene) []store.Record {
		var recs []store.Record
		for i, at := range []int{10, 125} {
			sc.seq++
			recs = append(recs, store.Record{
				Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "agent", EventTime: asyncAt(at), Seq: sc.seq,
				Kind: lifecycle.Observe, Boot: []string{"boot-1", "boot-2"}[i], Payload: []byte("host"),
			})
		}
		return append(recs, sc.edges(0, 10, 20, 125)...)
	})
	entity, ok := sc.bg.prefixOf(catalog.L1, host, dirEntity)
	if !ok {
		t.Fatal("the entity's type is not in the catalog")
	}
	_, keep, _ := sc.sets()
	if !has(keep, sc.forward(0)) {
		t.Error("the edges' prefix, which has the same shape, is not kept")
	}
	if has(keep, entity) || stateOf(sc.bg, entity) != nil {
		t.Errorf("the entity's prefix under a boot key is kept (%v) or its state is valid (%v)", has(keep, entity), stateOf(sc.bg, entity) != nil)
	}
	sc.finish()
}

// The kept states, and what the writer does with them, rest on there being one fold
// of a checkpoint: a build stops at a checkpoint of its own version only and walks on
// past any other, into the keys a step removes. A change of the version makes the
// states stand on nothing until it is proved again.
func TestTheFoldVersionTheKeptStatesRestOnIsPinned(t *testing.T) {
	t.Parallel()
	if foldVersion != 1 {
		t.Fatalf("foldVersion is %d: the exception for kept states (retain.go, write.go) is proved for version 1 only; prove it again for the new fold, then change this test", foldVersion)
	}
}
