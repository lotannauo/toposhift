package pebblestore

import (
	"bytes"
	"errors"
	"fmt"
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
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// This file is the differential oracle of the background pass. A history is
// written to a store that retains in the background, to one that retains
// synchronously and to the reference; the background store gets some of its writes
// between the chunks of the pass, at points chosen by the test, and, once the pass
// is over, must hold:
//
//	(a) the same records and baselines as the synchronous store, byte for byte;
//	(b) the same checkpoints too, so the whole of the data keyspace is equal;
//	(c) only true checkpoints, a writer's memory that is what a whole read finds,
//	    and the reference's answers.

// asyncWorld is a random history on a grid of nanoseconds: edges from a dozen pods,
// spread over the four layers, to a few nodes, and entity records of the pods, with
// deletes and TTLs. pre is written before the retention at h; later is written
// after it, every record at or after h.
type asyncWorld struct {
	pre, later [][]store.Record
	base, h    time.Time
	pods       []identity.Fingerprint
	nodes      []identity.Fingerprint
}

func (w *asyncWorld) at(n int) time.Time { return w.base.Add(time.Duration(n)) }

func asyncAt(n int) time.Time { return t0.Add(time.Duration(n)) }

func newAsyncWorld(seed int64) *asyncWorld { return newAsyncWorldAt(seed, t0) }

// layerOf is the layer the edges of the pod are stored in.
func layerOfPod(i int) catalog.Layer { return catalog.L0 + catalog.Layer(i%4) }

func newAsyncWorldAt(seed int64, base time.Time) *asyncWorld {
	rng := rand.New(rand.NewSource(seed))
	w := &asyncWorld{base: base}
	w.h = w.at(120 + rng.Intn(40))
	for i := range 12 {
		w.pods = append(w.pods, fingerprintOf(catalog.K8sPod, byte(0x10+i*3)))
	}
	for i := range 4 {
		w.nodes = append(w.nodes, fingerprintOf(catalog.K8sNode, byte(0x80+i*0x10)))
	}
	var seq uint64
	record := func(lo, hi int) store.Record {
		seq++
		i := rng.Intn(len(w.pods))
		layer := layerOfPod(i)
		kind, ttl := lifecycle.Observe, time.Duration(0)
		switch {
		case rng.Intn(3) == 0:
			kind = lifecycle.Delete
		case rng.Intn(3) == 0:
			ttl = time.Duration(1 + rng.Intn(10))
		}
		r := store.Record{
			Layer: layer, Producer: lifecycle.Producer([]string{"p", "q"}[rng.Intn(2)]),
			EventTime: w.at(lo + rng.Intn(hi-lo)), Seq: seq, Kind: kind, TTL: ttl,
		}
		if rng.Intn(4) == 0 {
			r.Subject, r.Layer = store.EntitySubject(w.pods[i]), catalog.L2 // an entity lives in its type's layer
		} else {
			r.Subject = store.EdgeSubject(w.pods[i], w.nodes[rng.Intn(len(w.nodes))], catalog.ScheduledOn)
		}
		if kind == lifecycle.Observe {
			r.Payload = fmt.Appendf(nil, "payload-%d", seq)
		}
		return r
	}
	hNs := int(w.h.Sub(w.base))
	for range 30 {
		var batch []store.Record
		for range 1 + rng.Intn(4) {
			batch = append(batch, record(0, hNs+80))
		}
		w.pre = append(w.pre, batch)
	}
	for range 10 {
		var batch []store.Record
		for range 1 + rng.Intn(4) {
			batch = append(batch, record(hNs, hNs+70))
		}
		w.later = append(w.later, batch)
	}
	return w
}

func (w *asyncWorld) entities() []identity.Fingerprint {
	return append(slices.Clone(w.pods), w.nodes...)
}

// asyncConfig says how a run of the oracle is set up.
type asyncConfig struct {
	ck *CheckpointOptions
	// tweak changes the options of both stores (a policy, offsets), and ref those of
	// the reference.
	tweak func(*Options)
	ref   memstore.Options
	// prep runs on both stores after the history before the retention is written.
	prep func(t *testing.T, s *Store)
	// bytes is the size at which a chunk ends.
	bytes int
	// slots[i] is how many chunks of the pass are committed before w.later[i] is
	// written to the background store; the rest are written once the pass is over.
	// A nil slots writes the batches in random places.
	slots []int
	// h is the retention horizon, if not the world's.
	h time.Time
	// noMidReads makes no check of the answers during the pass.
	noMidReads bool
	// exact makes the test also write, at every chunk boundary, a record to the prefix
	// that is exactly the first one the pass has not reached, if the world has one.
	exact bool
}

// asyncResult is what a run leaves for the tests that look further.
type asyncResult struct {
	bg, syn *Store
	ref     *memstore.Store
	rec     *memRecorder
	// touched is every prefix a write rewrote, as seen at the points the writes were
	// made.
	touched map[string]struct{}
	// chunks is how many chunks the synchronous pass took.
	chunks int
	// before and after count the puts to a prefix the pass had not reached (so the
	// write rewrote it) and to one it had passed.
	before, after int
	// exact is how many writes went to the prefix the pass was to take next.
	exact int
}

// asyncErrs collects what the checks that run on the goroutine of the pass find,
// which may not fail the test themselves.
type asyncErrs struct {
	mu   sync.Mutex
	errs []error
}

func (e *asyncErrs) add(err error) {
	if err == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	e.errs = append(e.errs, err)
}

func (e *asyncErrs) check(t *testing.T) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.errs) > 0 {
		t.Fatal(errors.Join(e.errs[:min(len(e.errs), 3)]...))
	}
}

// runAsync runs the oracle on a world and returns the stores for further checks.
func runAsync(t *testing.T, w *asyncWorld, c asyncConfig) *asyncResult {
	t.Helper()
	h := w.h
	if !c.h.IsZero() {
		h = c.h
	}
	chunkBytes := c.bytes
	if chunkBytes == 0 {
		chunkBytes = 1
	}
	refOpts := c.ref
	ref, err := memstore.Open(refOpts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ref.Close() })

	open := func(mode RetentionMode, rec *memRecorder, hooks func(*Options)) *Store {
		o := chunkOptions(vfs.NewMem(), c.ck)
		o.Retention, o.retainBatchBytes, o.retainChunkTime = mode, chunkBytes, time.Hour
		o.Recorder = rec
		if c.tweak != nil {
			c.tweak(&o)
		}
		if hooks != nil {
			hooks(&o)
		}
		return mustOpen(t, o)
	}
	recSyn, recBg := newMemRecorder(), newMemRecorder()
	syn := open(Synchronous, recSyn, nil)

	res := &asyncResult{syn: syn, ref: ref, rec: recBg, touched: map[string]struct{}{}}
	errs := &asyncErrs{}
	var bgs *Store
	next, hooks := 0, 0
	var slots []int
	var planned bool
	plan := func(chunks int) {
		if planned {
			return
		}
		planned = true
		slots = c.slots
		if slots == nil {
			rng := rand.New(rand.NewSource(int64(chunks)*7919 + int64(len(w.later))))
			for range w.later {
				slots = append(slots, rng.Intn(chunks+1))
			}
			slices.Sort(slots)
		}
	}
	// Every batch written to the background store is kept, with the sequence numbers it
	// was given, so that the synchronous store is written the same, in the same order.
	var written [][]store.Record
	var seq uint64
	var layerPrefix func(catalog.Layer, identity.Fingerprint, byte) []byte
	writeBatch := func(batch []store.Record) {
		out := cloneAll(batch)
		for i := range out {
			seq++
			out[i].Seq = seq
		}
		written = append(written, out)
		// Which side of the pass's progress each put falls on, as the write sees it.
		bgs.mu.Lock()
		if p := bgs.pass; p != nil {
			for _, r := range out {
				sides, serr := bgs.sidesOf(r)
				errs.add(serr)
				for _, sd := range sides {
					if p.owes(sd.prefix) {
						res.before++
					} else if bytes.Compare(sd.prefix, p.marker.resume) < 0 {
						res.after++
					}
				}
			}
		}
		bgs.mu.Unlock()
		errs.add(bgs.Write(bg, cloneAll(out)))
		errs.add(ref.Write(bg, cloneAll(out)))
		bgs.mu.Lock()
		defer bgs.mu.Unlock()
		if p := bgs.pass; p != nil {
			for k := range p.touched {
				res.touched[k] = struct{}{}
			}
		}
	}
	// writeExact writes a record to the prefix that begins exactly where the pass
	// resumes, if there is a prefix of the world there.
	writeExact := func() {
		bgs.mu.Lock()
		if bgs.pass == nil {
			bgs.mu.Unlock()
			return
		}
		resume := slices.Clone(bgs.pass.marker.resume)
		bgs.mu.Unlock()
		at := func(i int) time.Time { return w.at(int(w.h.Sub(w.base)) + i) }
		edge := func(layer catalog.Layer, pod, node identity.Fingerprint) store.Record {
			return store.Record{Layer: layer, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p", EventTime: at(5), Kind: lifecycle.Observe, Payload: []byte("exact")}
		}
		for i, pod := range w.pods {
			if prefix := layerPrefix(layerOfPod(i), pod, byte(store.Forward)); bytes.Equal(prefix, resume) {
				res.exact++
				writeBatch([]store.Record{edge(layerOfPod(i), pod, w.nodes[0])})
				return
			}
			if prefix := layerPrefix(catalog.L2, pod, dirEntity); bytes.Equal(prefix, resume) {
				res.exact++
				r := edge(catalog.L2, pod, w.nodes[0])
				r.Subject, r.EventTime = store.EntitySubject(pod), at(6)
				writeBatch([]store.Record{r})
				return
			}
		}
		for j, node := range w.nodes {
			for l := catalog.L0; l <= catalog.L3; l++ {
				if prefix := layerPrefix(l, node, byte(store.Reverse)); bytes.Equal(prefix, resume) {
					res.exact++
					writeBatch([]store.Record{edge(l, w.pods[int(l-catalog.L0)], w.nodes[j])})
					return
				}
			}
		}
	}
	// A policy with a lag writes a checkpoint older than the newest record, so what the
	// writer counts as written since the last checkpoint is not what a read finds
	// (that is so in the synchronous mode too); the other fields are exact always.
	exact := c.ck == nil || c.ck.Lag == 0
	checks := func() {
		bgs.mu.Lock()
		errs.add(asyncStateMismatch(bgs, exact))
		bgs.mu.Unlock()
		if !c.noMidReads {
			errs.add(asyncAnswers(bgs, ref, w.entities(), asyncTimes(w.h), []uint64{store.Latest}))
		}
	}
	before := func() {
		k := hooks
		hooks++
		for next < len(w.later) && slots[next] <= k {
			writeBatch(w.later[next])
			next++
			checks()
		}
		if c.exact {
			writeExact()
			checks()
		}
	}
	bgs = open(Background, recBg, func(o *Options) { o.beforeRetainChunk = before })
	layerPrefix = func(l catalog.Layer, fp identity.Fingerprint, dir byte) []byte {
		p, _ := bgs.prefixOf(l, fp, dir)
		return p
	}

	for _, batch := range w.pre {
		for _, x := range []store.Store{bgs, syn, ref} {
			if err := x.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
	if c.prep != nil {
		c.prep(t, bgs)
		c.prep(t, syn)
	}
	// The synchronous store says how many chunks there are, and the reference is
	// retained before anything is written to it after.
	if err := syn.Retain(bg, h); err != nil {
		t.Fatal(err)
	}
	if err := ref.Retain(bg, h); err != nil {
		t.Fatal(err)
	}
	if got := recSyn.samples["retain.chunks"]; len(got) == 1 {
		res.chunks = int(got[0])
	}
	plan(res.chunks)
	last := bgs.LastSeq()
	seq = last
	if err := bgs.Retain(bg, h); err != nil {
		t.Fatal(err)
	}
	waitRetained(t, bgs)
	for ; next < len(w.later); next++ {
		writeBatch(w.later[next])
	}
	errs.check(t)
	for _, batch := range written {
		if err := syn.Write(bg, cloneAll(batch)); err != nil {
			t.Fatal(err)
		}
	}
	res.bg = bgs

	if m := readMarker(t, bgs); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	// Each prefix is rewritten by a write once.
	if n := recBg.counters["retain.touch_rewrites"]; n != int64(len(res.touched)) {
		t.Errorf("the writes rewrote %d prefixes, and %d distinct ones were touched", n, len(res.touched))
	}
	// (a) and (b)
	got, want := dataBytes(t, bgs), dataBytes(t, syn)
	if d := diffData(t, got, want, kindRecord, kindBaseline); d != "" {
		t.Fatalf("the records and baselines differ from the synchronous store's: %s", d)
	}
	if d := diffData(t, got, want, kindRecord, kindCheckpoint, kindBaseline); d != "" {
		t.Fatalf("the checkpoints differ from the synchronous store's: %s", d)
	}
	// (c)
	if err := asyncCheckpointsTrue(bgs); err != nil {
		t.Fatalf("a stored checkpoint is not true: %v", err)
	}
	bgs.mu.Lock()
	err = asyncStateMismatch(bgs, exact)
	bgs.mu.Unlock()
	if err != nil {
		t.Fatalf("after the pass: %v", err)
	}
	toks := []uint64{store.Latest, last, last + 1, bgs.LastSeq()}
	if err := asyncAnswers(bgs, ref, w.entities(), asyncTimes(w.h), toks); err != nil {
		t.Fatalf("after the pass: %v", err)
	}
	// What a write left of the state of a prefix it rewrote is what the synchronous
	// store has of it.
	bgs.mu.Lock()
	defer bgs.mu.Unlock()
	syn.mu.Lock()
	defer syn.mu.Unlock()
	for k := range res.touched {
		a, aok := bgs.states[k]
		b, bok := syn.states[k]
		if aok != bok {
			continue // one of the two maps does not hold it: the checks above compared it with a whole read
		}
		if aok && !sameState(a, b) {
			t.Errorf("the prefix %x, rewritten by a write, is remembered as %+v; the synchronous store remembers %+v", k, *a, *b)
		}
	}
	return res
}

// asyncStateMismatch is rememberedStateMismatch for a moment after writes: with
// exact unset it leaves out the count of the records written since the last
// checkpoint. The caller holds the store's lock.
func asyncStateMismatch(e *Store, exact bool) error {
	if exact && e.anyCkpt {
		return rememberedStateMismatch(e)
	}
	if !e.anyCkpt {
		// A database without a checkpoint is not read: every prefix is taken to be empty
		// until a write says otherwise, so there is nothing for the memory to equal.
		return nil
	}
	probe := wholeReader(e)
	for k, b := range e.states {
		a, err := probe.state([]byte(k))
		if err != nil {
			return err
		}
		if a.latest != b.latest || !slices.Equal(checkpointsFrom(a.ckpts, b.below), b.ckpts) {
			return fmt.Errorf("the store remembers %+v of the prefix %x, where a whole read finds %+v", *b, k, *a)
		}
	}
	if e.complete {
		held, err := prefixesHoldingKeys(e)
		if err != nil {
			return err
		}
		for k := range held {
			if _, ok := e.states[k]; !ok {
				return fmt.Errorf("the map is complete and lacks the prefix %x, which holds keys", k)
			}
		}
	}
	return nil
}

// sameState says two remembered states describe the same prefix: what a read finds
// is the same, down to the lower of the bounds each was read to.
func sameState(a, b *prefixState) bool {
	lo := max(a.below, b.below)
	return a.latest == b.latest && a.since == b.since && a.sinceBytes == b.sinceBytes && a.lastBytes == b.lastBytes &&
		slices.Equal(checkpointsFrom(a.ckpts, lo), checkpointsFrom(b.ckpts, lo))
}

// asyncTimes are the instants the answers are compared at: around the horizon and
// through the span of the writes after it.
func asyncTimes(h time.Time) []time.Time {
	return []time.Time{h, h.Add(1), h.Add(7), h.Add(23), h.Add(41), h.Add(69), h.Add(120)}
}

// asyncAnswers compares what two stores answer for the entities, at the instants
// and tokens, in every layer. It returns the first difference.
func asyncAnswers(s, ref store.Store, fps []identity.Fingerprint, times []time.Time, tokens []uint64) error {
	for _, fp := range fps {
		for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
			for _, tm := range times {
				for _, tok := range tokens {
					sc := store.Scope{Layer: layer, AsOf: tok}
					for _, dir := range []store.Direction{store.Forward, store.Reverse} {
						want, werr := ref.Neighbors(bg, fp, dir, tm, sc)
						got, gerr := s.Neighbors(bg, fp, dir, tm, sc)
						if !sameOutcome(gerr, werr) || !slices.Equal(got, want) {
							return fmt.Errorf("Neighbors(%s, %s, %d, %s, asOf %d) = %v, %v; the reference says %v, %v", fp, dir, tm.UnixNano(), layer, tok, got, errText(gerr), want, errText(werr))
						}
					}
					wantAlive, werr := ref.Alive(bg, fp, tm, sc)
					gotAlive, gerr := s.Alive(bg, fp, tm, sc)
					if errors.Is(gerr, store.ErrBeforeHorizon) || errors.Is(werr, store.ErrBeforeHorizon) {
						if !sameOutcome(gerr, werr) {
							return fmt.Errorf("Alive(%s, %d, %s, asOf %d) = %v, %v; the reference says %v, %v", fp, tm.UnixNano(), layer, tok, gotAlive, errText(gerr), wantAlive, errText(werr))
						}
						continue
					}
					if a, b := quarantined(gotAlive, gerr), quarantined(wantAlive, werr); a != b {
						return fmt.Errorf("Alive(%s, %d, %s, asOf %d) = %s; the reference says %s", fp, tm.UnixNano(), layer, tok, a, b)
					}
				}
			}
		}
	}
	return nil
}

func errText(err error) string { return fmt.Sprint(err) }

// sameOutcome says two stores gave the same error, or none: a refusal because the
// instant is before the horizon is the same refusal in both, whatever its text.
func sameOutcome(a, b error) bool {
	if errors.Is(a, store.ErrBeforeHorizon) || errors.Is(b, store.ErrBeforeHorizon) {
		return errors.Is(a, store.ErrBeforeHorizon) && errors.Is(b, store.ErrBeforeHorizon)
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}

// diffData describes the first difference between two dumps of the data keys,
// looking at the keys of the given kinds only; it is empty when there is none.
func diffData(t *testing.T, a, b [][2]string, kinds ...byte) string {
	t.Helper()
	pick := func(rows [][2]string) [][2]string {
		var out [][2]string
		for _, kv := range rows {
			_, _, _, kind, err := parseKey([]byte(kv[0]))
			if err != nil {
				t.Fatal(err)
			}
			if slices.Contains(kinds, kind) {
				out = append(out, kv)
			}
		}
		return out
	}
	x, y := pick(a), pick(b)
	for i := 0; i < len(x) || i < len(y); i++ {
		switch {
		case i >= len(x):
			return fmt.Sprintf("the synchronous store has %d keys and the background one %d; the first extra is %x", len(y), len(x), y[i][0])
		case i >= len(y):
			return fmt.Sprintf("the background store has %d keys and the synchronous one %d; the first extra is %x", len(x), len(y), x[i][0])
		case x[i] != y[i]:
			if x[i][0] != y[i][0] {
				return fmt.Sprintf("key %d is %x in the background store and %x in the synchronous one", i, x[i][0], y[i][0])
			}
			return fmt.Sprintf("the value of key %x (%d of %d) differs", x[i][0], i, len(x))
		}
	}
	return ""
}

// asyncCheckpointsTrue walks every checkpoint in the database, builds it again with
// the store's own builder from the records (and the older checkpoints below it, each
// of which is checked in its turn, so the lowest stale one is built from records
// alone), and requires the same entries; and requires the W of each to be at least
// the highest Seq among the records before it. B and W are left out of the
// comparison: B is the last sequence number at the time of the build, and W is
// conservative.
func asyncCheckpointsTrue(s *Store) error {
	type ckpt struct {
		prefix []byte
		ns     int64
		val    []byte
	}
	var found []ckpt
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return err
	}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			_ = it.Close()
			return err
		}
		if kind == kindCheckpoint {
			found = append(found, ckpt{slices.Clone(prefix), ns, slices.Clone(it.Value())})
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range found {
		stored, err := decodeStamp(c.val)
		if err != nil {
			return fmt.Errorf("the checkpoint at %d of prefix %x does not decode: %w", c.ns, c.prefix, err)
		}
		if stored.foldVersion != foldVersion {
			continue
		}
		raw, err := s.build(c.prefix, c.ns)
		if err != nil {
			return err
		}
		built, err := decodeStamp(raw)
		if err != nil {
			return err
		}
		floor, err := asyncHighestSeqBefore(s, c.prefix, c.ns)
		if err != nil {
			return err
		}
		if stored.w < floor {
			return fmt.Errorf("the checkpoint at %d of prefix %x has W %d, below %d, the highest Seq among the records before it", c.ns, c.prefix, stored.w, floor)
		}
		stored.through, built.through, stored.w, built.w = 0, 0, 0, 0
		x, xerr := appendStamp(nil, stored)
		y, yerr := appendStamp(nil, built)
		if err := errors.Join(xerr, yerr); err != nil {
			return err
		}
		if !bytes.Equal(x, y) {
			return fmt.Errorf("the checkpoint at %d (%d entries) of prefix %x is not what building it from the records gives (%d entries): it is stale", c.ns, len(stored.entries), c.prefix, len(built.entries))
		}
	}
	return nil
}

// asyncHighestSeqBefore is the highest Seq among the records of the prefix with an
// event time before ns.
func asyncHighestSeqBefore(s *Store, prefix []byte, ns int64) (uint64, error) {
	lo, hi := prefixBounds(prefix)
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return 0, err
	}
	var top uint64
	for ok := it.First(); ok; ok = it.Next() {
		_, at, seq, kind, err := parseKey(it.Key())
		if err != nil {
			_ = it.Close()
			return 0, err
		}
		if kind == kindRecord && at < ns {
			top = max(top, seq)
		}
	}
	return top, errors.Join(it.Error(), it.Close())
}

// The oracle, on random histories, with the writes after the horizon falling at
// random chunk boundaries, with the checkpoint policy on and off, and with chunks of
// one prefix and of several.
func TestABackgroundPassIsTheSynchronousOneWhateverTheInterleaving(t *testing.T) {
	t.Parallel()
	seeds := 24
	if raceEnabled {
		// One goroutine of the test and the retainer's, in turn: the race detector finds
		// no more in more seeds, and costs minutes. Plain builds run them all.
		seeds = 2
	}
	policies := []*CheckpointOptions{policy(CheckpointOptions{On: true, KMin: 1}), policy(CheckpointOptions{On: true, KMin: 2, Lag: 3}), off()}
	var mu sync.Mutex
	var touches, passed, chunks, wrote, exactly int64
	t.Run("seeds", func(t *testing.T) {
		for seed := range seeds {
			t.Run(fmt.Sprint(seed), func(t *testing.T) {
				t.Parallel()
				w := newAsyncWorld(int64(seed))
				res := runAsync(t, w, asyncConfig{ck: policies[seed%len(policies)], bytes: []int{1, 1, 300}[seed%3], exact: seed%2 == 0, noMidReads: raceEnabled})
				mu.Lock()
				defer mu.Unlock()
				touches += int64(res.before)
				passed += int64(res.after)
				chunks += int64(res.chunks)
				wrote += res.rec.counters["retain.touch_rewrites"]
				exactly += int64(res.exact)
			})
		}
	})
	if touches == 0 || passed == 0 || chunks == 0 || wrote == 0 || exactly == 0 {
		t.Fatalf("the histories wrote to %d prefixes the pass had not reached and %d it had passed, over %d chunks, with %d rewrites by a write and %d writes to the prefix the pass was to take next: they do not exercise the pass", touches, passed, chunks, wrote, exactly)
	}
	t.Logf("%d puts to prefixes the pass had not reached, %d to prefixes it had passed, %d chunks, %d rewrites by a write, %d writes to the prefix the pass was to take next", touches, passed, chunks, wrote, exactly)
}

// refused drops from the writes after the retention those a layer's horizon would
// refuse: with these offsets and kept layers, a retention at h puts the horizon of
// a layer at h less its offset.
func (w *asyncWorld) refused(h time.Time, offsets [4]time.Duration, keep [4]bool) {
	var later [][]store.Record
	for _, batch := range w.later {
		var kept []store.Record
		for _, r := range batch {
			i := int(r.Layer) - int(catalog.L0)
			if !keep[i] && r.EventTime.Before(h.Add(-offsets[i])) {
				continue
			}
			kept = append(kept, r)
		}
		if len(kept) > 0 {
			later = append(later, kept)
		}
	}
	w.later = later
}

func zeros(n int) []int { return make([]int, n) }

// The state a write leaves in the writer's memory of a prefix it has rewritten
// first is the state the synchronous store has after the same writes: in a database
// without a checkpoint, with a horizon after the range, with a prefix kept whole for
// its boots, with a prefix that holds nothing before the horizon, and with a
// checkpoint exactly at the horizon. runAsync compares the maps of the two stores
// for every prefix a write rewrote, checks the whole of the data against the
// synchronous store's, and checks the memory against a whole read.
func TestAPrefixARewritesFirstIsRememberedAsTheSynchronousStoreRemembersIt(t *testing.T) {
	t.Parallel()
	every := policy(CheckpointOptions{On: true, KMin: 1})

	// Each case is the same code with other data, on the goroutines of one test and the
	// retainer: under the race detector two of them run, plain builds run all five.
	onlyPlain := func(t *testing.T) {
		if raceEnabled {
			t.Skip("single-goroutine data case; plain builds run it, the race detector keeps two of the five")
		}
	}
	noMidReads := raceEnabled

	t.Run("no checkpoint in the database", func(t *testing.T) {
		t.Parallel()
		onlyPlain(t)
		w := newAsyncWorld(1)
		res := runAsync(t, w, asyncConfig{ck: policy(CheckpointOptions{On: true, KMin: 100000}), slots: zeros(len(w.later)), noMidReads: noMidReads})
		if res.bg.anyCkpt || res.before == 0 {
			t.Errorf("the history wrote a checkpoint (%v) or touched no prefix (%d)", res.bg.anyCkpt, res.before)
		}
	})

	t.Run("a horizon after the range", func(t *testing.T) {
		t.Parallel()
		onlyPlain(t)
		year := 365 * 24 * time.Hour
		base := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC)
		w := newAsyncWorldAt(2, base)
		// L0's horizon is after the range, L1's and L3's are inside it, and L2's is
		// before every record. No pass derives anything then, and the layers that are
		// inside it are rewritten with the writer reading the prefixes.
		offsets := [4]time.Duration{0, 100 * year, 150 * year, 100 * year}
		h := w.h.Add(100 * year)
		w.refused(h, offsets, [4]bool{})
		res := runAsync(t, w, asyncConfig{
			ck: every, h: h, bytes: 1, slots: zeros(len(w.later)), noMidReads: noMidReads,
			tweak: func(o *Options) { o.Offsets = offsets }, ref: memstore.Options{Offsets: offsets},
		})
		if res.before == 0 {
			t.Error("no write touched a prefix")
		}
		if res.bg.complete || res.syn.complete {
			t.Error("the pass called the map complete, but no pass derives anything when a horizon is after the range")
		}
	})

	t.Run("a prefix kept whole for its boots", func(t *testing.T) {
		t.Parallel()
		w := newBootWorld()
		res := runAsync(t, w, asyncConfig{
			ck: every, slots: zeros(len(w.later)), noMidReads: noMidReads,
			tweak: func(o *Options) { o.Policy = storetest.QuarantinePolicy() }, ref: memstore.Options{Policy: storetest.QuarantinePolicy()},
		})
		if got := res.rec.counters["retain.prefixes_kept_for_boots"]; got == 0 {
			t.Error("no prefix was kept whole for its boots")
		}
		if res.before == 0 {
			t.Error("no write touched a prefix")
		}
	})

	t.Run("a prefix with only keys at or after the horizon", func(t *testing.T) {
		t.Parallel()
		onlyPlain(t)
		w := newAsyncWorld(4)
		// Records at the horizon and after it only, in pods that have no earlier
		// record, written before the retention and after.
		hNs := int(w.h.Sub(w.base))
		seq := uint64(10000)
		fresh := func(i int, at int) store.Record {
			seq++
			return store.Record{
				Layer: layerOfPod(i), Subject: store.EdgeSubject(w.fresh(i), w.nodes[0], catalog.ScheduledOn), Producer: "p",
				EventTime: w.at(at), Seq: seq, Kind: lifecycle.Observe, Payload: []byte("x"),
			}
		}
		w.pre = append(w.pre, []store.Record{fresh(0, hNs), fresh(1, hNs+3), fresh(2, hNs+9)})
		w.later = append(w.later, []store.Record{fresh(0, hNs+20), fresh(1, hNs+1), fresh(2, hNs+30)}, []store.Record{fresh(3, hNs+4), fresh(4, hNs+40)})
		w.renumber()
		res := runAsync(t, w, asyncConfig{ck: every, slots: zeros(len(w.later)), noMidReads: noMidReads})
		if res.before == 0 {
			t.Error("no write touched a prefix")
		}
	})

	t.Run("a checkpoint exactly at the horizon", func(t *testing.T) {
		t.Parallel()
		w := newAsyncWorld(5)
		var wrote int
		res := runAsync(t, w, asyncConfig{
			ck: every, slots: zeros(len(w.later)), noMidReads: noMidReads,
			prep: func(t *testing.T, s *Store) {
				for i, pod := range w.pods {
					if err := s.Instrument().CheckpointEdges(layerOfPod(i), pod, store.Forward, w.h); err != nil {
						t.Fatal(err)
					}
					wrote++
				}
			},
		})
		if wrote == 0 || res.before == 0 {
			t.Errorf("%d checkpoints at the horizon, %d prefixes touched", wrote, res.before)
		}
	})
}

// renumber gives the records of the world the sequence numbers 1, 2, ... in the
// order they are written.
func (w *asyncWorld) renumber() {
	var seq uint64
	for _, side := range [][][]store.Record{w.pre, w.later} {
		for _, batch := range side {
			for i := range batch {
				seq++
				batch[i].Seq = seq
			}
		}
	}
}

// fresh is a pod that no record of the world names.
func (w *asyncWorld) fresh(i int) identity.Fingerprint {
	return fingerprintOf(catalog.K8sPod, byte(0xC0+i))
}

// newBootWorld is hosts that reboot, whose records carry boot ids, and a few edges
// so that the database holds checkpoints. The hosts are written before the
// retention, which keeps their prefixes whole, and after it.
func newBootWorld() *asyncWorld {
	w := &asyncWorld{base: t0}
	w.h = w.at(100)
	for i := range 3 {
		w.pods = append(w.pods, fingerprintOf(catalog.Host, byte(0x60+i)))
	}
	w.pods = append(w.pods, fingerprintOf(catalog.K8sPod, 0x10))
	w.nodes = []identity.Fingerprint{fingerprintOf(catalog.K8sNode, 0x80)}
	var seq uint64
	host := func(i, at, boot int) store.Record {
		seq++
		return store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(w.pods[i]), Producer: "agent", EventTime: w.at(at), Seq: seq,
			Kind: lifecycle.Observe, Boot: fmt.Sprintf("boot-%d", boot), Payload: []byte("host"),
		}
	}
	edge := func(at int) store.Record {
		seq++
		return store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(w.pods[3], w.nodes[0], catalog.ScheduledOn), Producer: "p",
			EventTime: w.at(at), Seq: seq, Kind: lifecycle.Observe, Payload: []byte("e"),
		}
	}
	for i := range 3 {
		w.pre = append(w.pre, []store.Record{host(i, 10+i, 1), edge(11 + i)}, []store.Record{host(i, 50+i, 2), edge(51 + i)})
	}
	w.later = [][]store.Record{
		{host(0, 105, 2), host(1, 106, 3)}, {edge(110), host(2, 107, 2)}, {host(0, 120, 4)},
	}
	return w
}
