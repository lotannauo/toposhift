package pebblestore

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// This file and faults_test.go check what the store promises about its disk. The
// product acknowledges the stream only after a store commit, so the promise is: a
// batch whose Write returned nil, and a horizon whose Retain returned nil, are
// there after a crash. What a crash may lose is derived data (checkpoints), a
// batch that was in flight and so never acknowledged, and the unfinished rewriting
// of a retention, which a later retention finishes.
//
// A crash is a copy of an in-memory file system that keeps what was synced and,
// at random, some of what was not (see [vfs.MemFS.CrashClone]). The store is
// reopened on the copy and compared with the reference store, fed the same
// history up to the point the reopened store says it reached.

// histOp is one operation a store was asked to do: a Write of a batch, or a
// Retain. A history is the operations in order; the reference is fed a prefix of
// it, and which prefix a reopened store holds is found out from its last sequence
// number and its horizon.
type histOp struct {
	batch    []store.Record
	retain   time.Time
	isRetain bool
}

type opHistory []histOp

// lastSeq is the Seq of the last record the first n operations wrote.
func (h opHistory) lastSeq(n int) uint64 {
	var seq uint64
	for _, o := range h[:n] {
		if !o.isRetain {
			seq = o.batch[len(o.batch)-1].Seq
		}
	}
	return seq
}

// horizon is the latest horizon the first n operations retained at, or the zero
// time.
func (h opHistory) horizon(n int) time.Time {
	var at time.Time
	for _, o := range h[:n] {
		if o.isRetain {
			at = o.retain
		}
	}
	return at
}

// apply feeds one operation to a store.
func (o histOp) apply(s store.Store) error {
	if o.isRetain {
		return s.Retain(bg, o.retain)
	}
	return s.Write(bg, cloneAll(o.batch))
}

func (o histOp) String() string {
	if o.isRetain {
		return fmt.Sprintf("retain at %s", o.retain.Format(time.RFC3339Nano))
	}
	return fmt.Sprintf("write seq %d..%d (%d records)", o.batch[0].Seq, o.batch[len(o.batch)-1].Seq, len(o.batch))
}

// fatalLog is the logger a store is opened with in these tests. Pebble's own ends
// the process when it meets a condition it cannot continue from, which is what the
// fault tests inject; this one counts them instead, so that a test can treat the
// moment as the process dying.
type fatalLog struct {
	n    atomic.Int64
	mu   sync.Mutex
	msg  string
	once sync.Once
	dead chan struct{}
	// onFatal, if set before the store is opened, runs inside the first Fatalf,
	// before dead is closed: the moment the process dies, which is when a test takes
	// the crash of the file system, so that nothing a dying process would not have
	// done can land in it.
	onFatal func()
}

func newFatalLog() *fatalLog { return &fatalLog{dead: make(chan struct{})} }

var _ pebble.Logger = (*fatalLog)(nil)

func (l *fatalLog) Infof(string, ...any)  {}
func (l *fatalLog) Errorf(string, ...any) {}

func (l *fatalLog) Fatalf(format string, args ...any) {
	l.mu.Lock()
	if l.msg == "" {
		l.msg = fmt.Sprintf(format, args...)
	}
	l.mu.Unlock()
	l.n.Add(1)
	l.once.Do(func() {
		if l.onFatal != nil {
			l.onFatal()
		}
		close(l.dead)
	})
}

// fatal is how many fatal conditions Pebble has reported.
func (l *fatalLog) fatal() int64 { return l.n.Load() }

func (l *fatalLog) message() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.msg
}

// crashVariant is one workload and store configuration. The seeds cycle through them,
// so a fixed list of seeds covers every one.
type crashVariant struct {
	name        string
	gen         storetest.Config
	policy      lifecycle.Policy
	ckpt        *CheckpointOptions
	retainBytes int
}

func crashVariantOf(seed int) crashVariant {
	c := storetest.Tiny()
	c.Seed = uint64(1000 + seed)
	v := crashVariant{gen: c, ckpt: policy(CheckpointOptions{On: true, KMin: 4, Alpha: 1})}
	switch seed % 4 {
	case 0:
		v.name = "tiny"
	case 1:
		v.name = "late and confirmed"
		v.gen.LateProbability, v.gen.LateMax, v.gen.ConfirmProbability = 0.4, 5*time.Minute, 0.6
		v.ckpt = policy(CheckpointOptions{On: true, KMin: 2, Alpha: 1})
	case 2:
		v.name = "runs and lateness"
		v.gen.Runs, v.gen.LateProbability = true, 0.3
		v.ckpt = policy(DefaultCheckpoints())
		if seed%8 == 6 {
			v.ckpt = off()
		}
	default:
		v.name = "reboots and clones"
		v.gen.Runs, v.gen.RebootProbability, v.gen.CloneProbability = true, 0.2, 0.5
		v.policy = storetest.QuarantinePolicy()
		v.ckpt = policy(CheckpointOptions{On: true, KMin: 3, Alpha: 1})
	}
	// Retentions that commit in one piece, in a piece for each prefix, and in a few.
	v.retainBytes = []int{0, 1, 400}[(seed/4)%3]
	return v
}

// options opens a store of this variant on fs. The Pebble tuning is the tiny one,
// so a few hundred records already fill memtables and reach tables, and commits
// are synced, which is the promise under test.
func (v crashVariant) options(fs vfs.FS, lg pebble.Logger, autoCompactions bool) Options {
	return Options{
		Config:      pebblekv.Config{Tuning: pebblekv.TinyTuning(), Sync: true, FS: fs, Logger: lg, DisableAutoCompactions: !autoCompactions},
		Policy:      v.policy,
		Checkpoints: v.ckpt,
		// A retention commits in pieces of this many bytes, so that a crash can fall
		// between them.
		retainBatchBytes: v.retainBytes,
	}
}

// stream is a variant's whole stream, generated once: a run takes records off its
// front, and a reopened store is offered the ones after, so no generator has to be
// told of a horizon.
func (v crashVariant) stream() ([]store.Record, error) {
	g, err := storetest.NewGenerator(v.gen)
	if err != nil {
		return nil, err
	}
	return g.All(), nil
}

// notBefore are the records not before the horizon, which are all a batch may hold:
// Write refuses a batch with one that is.
func notBefore(batch []store.Record, h time.Time) []store.Record {
	if h.IsZero() {
		return slices.Clone(batch)
	}
	return slices.DeleteFunc(slices.Clone(batch), func(r store.Record) bool { return r.EventTime.Before(h) })
}

// maxEvent is the latest event time among records, or at.
func maxEvent(at time.Time, records []store.Record) time.Time {
	for _, r := range records {
		if r.EventTime.After(at) {
			at = r.EventTime
		}
	}
	return at
}

// crashSeeds is the fixed list of seeds a run takes: 20 in a short run, more in a full
// one, and fewer under the race detector, which finds nothing in these
// single-goroutine runs and costs several times as much.
func crashSeeds() []int {
	n := 40
	switch {
	case raceEnabled && testing.Short():
		n = 3
	case raceEnabled:
		n = 6
	case testing.Short():
		n = 20
	}
	out := make([]int, n)
	for i := range out {
		out[i] = i
	}
	return out
}

// reproduce is how to run one seed again, for a failure message.
func reproduce(test string, seed int) string {
	return fmt.Sprintf("reproduce with: go test ./internal/store/pebblestore -run '^%s$/^seeds$/^seed=%d$' -v (everything but the timing of Pebble's background work is fixed by the seed, and no outcome may depend on that)", test, seed)
}

// --- comparing a reopened store with the reference ---

func sameHorizonOf(a, b store.Horizon) bool { return a.Time.Equal(b.Time) && a.Seq == b.Seq }

// readClass names how a read ended, for comparing two stores: it was answered, was
// refused for the horizon or as invalid, found a quarantined entity, or failed.
func readClass(err error) string {
	var q *store.QuarantineError
	switch {
	case err == nil:
		return "answered"
	case errors.As(err, &q):
		return quarantined(false, err)
	case errors.Is(err, store.ErrBeforeHorizon):
		return "refused for the horizon"
	case errors.Is(err, store.ErrInvalid):
		return "refused as invalid"
	}
	return "failed: " + err.Error()
}

func renderRecords(rs []store.Record) string {
	var b strings.Builder
	for _, r := range rs {
		fmt.Fprintf(&b, "\n  %s %v seq %d %s@%d kind %d ttl %d through %s boot %q basis %d payload %x",
			r.Layer, r.Subject, r.Seq, r.Producer, r.EventTime.UnixNano(), r.Kind, r.TTL, r.Through.UTC().Format(time.RFC3339Nano), r.Boot, r.EventTimeBasis, r.Payload)
	}
	return b.String()
}

// probeSet is what a comparison asks: the entities, instants and snapshot tokens
// sampled from the stream, and the layers its records are in.
type probeSet struct {
	fps    []identity.Fingerprint
	layers []catalog.Layer
	times  []time.Time
	tokens []uint64
}

// How much a comparison asks: entities, instants and extra tokens at most. The
// race detector makes every read many times dearer and finds nothing in these
// single-goroutine comparisons, so it is given a fifth of the questions.
var maxFPs, maxTimes, maxTokens = func() (int, int, int) {
	if raceEnabled {
		return 3, 3, 1
	}
	return 6, 6, 2
}()

// sampleProbes chooses what to ask about a store whose records are recs and whose
// horizon and last sequence number are h and last. Entities and instants come from
// the records, the newest two always among them, and the instants include one
// nanosecond either side of a record's, the horizon, and a moment after everything.
// Nothing is asked below the horizon, which the store refuses.
func sampleProbes(rng *rand.Rand, recs []store.Record, h store.Horizon, last uint64) probeSet {
	var p probeSet
	var picked []store.Record
	for i := 0; i < 8 && len(recs) > 0; i++ {
		picked = append(picked, recs[rng.IntN(len(recs))])
	}
	if n := len(recs); n > 0 {
		picked = append(picked, recs[n-1], recs[max(0, n-2)])
	}
	seenFP := map[identity.Fingerprint]bool{}
	seenLayer := map[catalog.Layer]bool{}
	var times []time.Time
	add := func(t time.Time) {
		if !t.IsZero() && !t.Before(h.Time) && !slices.ContainsFunc(times, t.Equal) {
			times = append(times, t)
		}
	}
	for _, r := range picked {
		for _, fp := range []identity.Fingerprint{r.Subject.A, r.Subject.B} {
			if !fp.IsZero() && !seenFP[fp] && len(p.fps) < maxFPs {
				seenFP[fp] = true
				p.fps = append(p.fps, fp)
			}
		}
		if !seenLayer[r.Layer] {
			seenLayer[r.Layer] = true
			p.layers = append(p.layers, r.Layer)
		}
		add(r.EventTime)
		add(r.EventTime.Add(time.Nanosecond))
		add(r.EventTime.Add(-time.Nanosecond))
		add(r.Through)
	}
	rng.Shuffle(len(times), func(i, j int) { times[i], times[j] = times[j], times[i] })
	p.times = times[:min(len(times), maxTimes)]
	if !h.Time.IsZero() {
		p.times = append(p.times, h.Time)
	}
	if n := len(recs); n > 0 {
		p.times = append(p.times, maxEvent(time.Time{}, recs).Add(time.Hour))
	}
	slices.SortFunc(p.times, time.Time.Compare)
	p.tokens = []uint64{store.Latest, last}
	if h.Seq > 0 {
		p.tokens = append(p.tokens, h.Seq)
	}
	for i := 0; i < maxTokens && len(picked) > 0; i++ {
		if sq := picked[rng.IntN(len(picked))].Seq; sq >= h.Seq && sq <= last {
			p.tokens = append(p.tokens, sq)
		}
	}
	slices.Sort(p.tokens)
	p.tokens = slices.Compact(p.tokens)
	return p
}

// answerDiff asks both stores the questions of p and returns the first
// difference, or "". The question is in the message, with both answers.
func answerDiff(got, want store.Store, p probeSet) string {
	return answerDiffTolerating(got, want, p, false)
}

// answerDiffTolerating is answerDiff, but with ioErrors a read of got that fails
// with an error the file system injected is not a difference: a store may refuse
// to answer when it cannot read, and must not answer wrongly.
func answerDiffTolerating(got, want store.Store, p probeSet, ioErrors bool) string {
	check := func(what string, g string, gerr error, w string, werr error) string {
		if ioErrors && gerr != nil && werr == nil && strings.Contains(gerr.Error(), "injected error") {
			return ""
		}
		if gc, wc := readClass(gerr), readClass(werr); gc != wc || (gerr == nil && g != w) {
			return fmt.Sprintf("%s:\n got  %s %s\n want %s %s", what, gc, g, wc, w)
		}
		return ""
	}
	for _, l := range p.layers {
		for _, tok := range p.tokens {
			sc := store.Scope{Layer: l, AsOf: tok}
			for _, fp := range p.fps {
				for i, tm := range p.times {
					for _, dir := range []store.Direction{store.Forward, store.Reverse} {
						g, gerr := got.Neighbors(bg, fp, dir, tm, sc)
						w, werr := want.Neighbors(bg, fp, dir, tm, sc)
						if d := check(fmt.Sprintf("Neighbors(%s, %s, %s, layer %s, token %d)", fp, dir, tm.Format(time.RFC3339Nano), l, tok), fmt.Sprint(g), gerr, fmt.Sprint(w), werr); d != "" {
							return d
						}
					}
					ga, gerr := got.Alive(bg, fp, tm, sc)
					wa, werr := want.Alive(bg, fp, tm, sc)
					if d := check(fmt.Sprintf("Alive(%s, %s, layer %s, token %d)", fp, tm.Format(time.RFC3339Nano), l, tok), quarantined(ga, gerr), gerr, quarantined(wa, werr), werr); d != "" {
						return d
					}
					// One window from this instant to the next one asked about.
					if i+1 < len(p.times) {
						to := p.times[i+1]
						for _, dir := range []store.Direction{store.Forward, store.Reverse} {
							g, gerr := got.Window(bg, fp, dir, tm, to, sc)
							w, werr := want.Window(bg, fp, dir, tm, to, sc)
							if d := check(fmt.Sprintf("Window(%s, %s, [%s, %s), layer %s, token %d)", fp, dir, tm.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), l, tok), renderRecords(g), gerr, renderRecords(w), werr); d != "" {
								return d
							}
						}
						g, gerr := got.EntityWindow(bg, fp, tm, to, sc)
						w, werr := want.EntityWindow(bg, fp, tm, to, sc)
						if d := check(fmt.Sprintf("EntityWindow(%s, [%s, %s), layer %s, token %d)", fp, tm.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), l, tok), renderRecords(g), gerr, renderRecords(w), werr); d != "" {
							return d
						}
					}
				}
			}
		}
	}
	return ""
}

// compareStores is answerDiff as an error, and compares the token and the horizons
// too.
func compareStores(got, want store.Store, recs []store.Record, rng *rand.Rand) error {
	if g, w := got.LastSeq(), want.LastSeq(); g != w {
		return fmt.Errorf("LastSeq = %d, the reference says %d", g, w)
	}
	if g, w := got.Horizon(), want.Horizon(); !sameHorizonOf(g, w) {
		return fmt.Errorf("Horizon() = %v, the reference says %v", g, w)
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		if g, w := got.LayerHorizon(l), want.LayerHorizon(l); !sameHorizonOf(g, w) {
			return fmt.Errorf("LayerHorizon(%s) = %v, the reference says %v", l, g, w)
		}
	}
	if d := answerDiff(got, want, sampleProbes(rng, recs, want.Horizon(), want.LastSeq())); d != "" {
		return errors.New(d)
	}
	return nil
}

// referenceAfter is the reference store fed the first n operations of h.
func referenceAfter(p lifecycle.Policy, h opHistory, n int) (*memstore.Store, error) {
	ref, err := memstoreWithPolicy(p)
	if err != nil {
		return nil, err
	}
	for i, o := range h[:n] {
		if err := o.apply(ref); err != nil {
			_ = ref.Close()
			return nil, fmt.Errorf("the reference refused operation %d (%s): %w", i, o, err)
		}
	}
	return ref, nil
}

// requireReopened checks a store opened on what a crash or a fault left. h is the
// history, of which the first acked operations had returned to the caller and the
// first begun had been started (the operations between were in flight). It
// requires the properties of the promise:
//
//   - the last sequence number is at least that of the last acknowledged batch;
//   - the horizon is the last acknowledged one or a newer one;
//   - the store holds a whole number of the operations: some prefix of the
//     history from acked to begun, and so no part of a batch;
//   - every answer equals the reference fed that prefix.
//
// It returns the length of the prefix and the reference fed it, which the caller
// closes.
func requireReopened(p lifecycle.Policy, s store.Store, h opHistory, acked, begun int, recs []store.Record, rng *rand.Rand) (int, *memstore.Store, error) {
	if want, got := h.lastSeq(acked), s.LastSeq(); got < want {
		return 0, nil, fmt.Errorf("LastSeq = %d after reopening, below %d, the last batch whose Write returned nil", got, want)
	}
	if want, got := h.horizon(acked), s.Horizon().Time; got.Before(want) {
		return 0, nil, fmt.Errorf("the horizon is %s after reopening, older than %s, the last a Retain returned nil for", got.Format(time.RFC3339Nano), want.Format(time.RFC3339Nano))
	}
	ref, err := referenceAfter(p, h, acked)
	if err != nil {
		return 0, nil, err
	}
	for n := acked; ; n++ {
		if ref.LastSeq() == s.LastSeq() && sameHorizonOf(ref.Horizon(), s.Horizon()) {
			if err := compareStores(s, ref, recs, rng); err != nil {
				_ = ref.Close()
				return 0, nil, fmt.Errorf("with the first %d of %d operations (acknowledged %d): %w", n, begun, acked, err)
			}
			return n, ref, nil
		}
		if n == begun {
			break
		}
		if err := h[n].apply(ref); err != nil {
			_ = ref.Close()
			return 0, nil, fmt.Errorf("the reference refused operation %d (%s): %w", n, h[n], err)
		}
	}
	got := fmt.Sprintf("LastSeq %d and horizon %v", s.LastSeq(), s.Horizon())
	_ = ref.Close()
	return 0, nil, fmt.Errorf("the reopened store holds %s, which is none of the states after the operations from %d (the acknowledged ones) to %d (the ones begun); the last acknowledged batch ends at seq %d, the history is:\n%v",
		got, acked, begun, h.lastSeq(acked), h[:begun])
}

// requireCarriesOn is the last property: after reopening, the store takes the next
// batches, and a retention, and goes on answering as the reference does. The
// batches are the ones of the stream after pos, and ref is the reference fed the
// operations the store holds, which is fed alongside.
func requireCarriesOn(s *Store, ref *memstore.Store, all []store.Record, pos int, rng *rand.Rand) error {
	seen := all[:pos]
	for round := 0; round < 2; round++ {
		end := min(len(all), pos+1+rng.IntN(30))
		batch := notBefore(all[pos:end], ref.Horizon().Time)
		pos = end
		if len(batch) == 0 {
			continue
		}
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			return fmt.Errorf("the reopened store refused the next batch (seq %d..%d): %w", batch[0].Seq, batch[len(batch)-1].Seq, err)
		}
		if err := ref.Write(bg, cloneAll(batch)); err != nil {
			return fmt.Errorf("the reference refused the next batch: %w", err)
		}
		seen = all[:pos]
		if err := compareStores(s, ref, seen, rng); err != nil {
			return fmt.Errorf("after writing seq %d..%d to the reopened store: %w", batch[0].Seq, batch[len(batch)-1].Seq, err)
		}
	}
	retained := false
	if h := maxEvent(time.Time{}, seen).Add(-time.Duration(1+rng.IntN(240)) * time.Second); h.After(ref.Horizon().Time) {
		retained = true
		if err := s.Retain(bg, h); err != nil {
			return fmt.Errorf("the reopened store failed to retain at %s: %w", h.Format(time.RFC3339Nano), err)
		}
		if err := ref.Retain(bg, h); err != nil {
			return err
		}
		if err := compareStores(s, ref, seen, rng); err != nil {
			return fmt.Errorf("after retaining at %s on the reopened store: %w", h.Format(time.RFC3339Nano), err)
		}
	}
	// Straight after a retention, what the writer remembers of each prefix is what a
	// read of the whole prefix finds (where the database holds a checkpoint, which
	// is where the whole read looks).
	if retained && s.anyCkpt {
		if err := rememberedStateMismatch(s); err != nil {
			return fmt.Errorf("what the reopened store remembers of the prefixes after a retention: %w", err)
		}
	}
	if err := requireCheckpointsTrue(s); err != nil {
		return fmt.Errorf("after carrying on: %w", err)
	}
	return nil
}

// requireCheckpointsTrue walks every checkpoint in the database, builds it again
// with the store's own builder from the records (and the older checkpoints below
// it, each of which is checked in its turn, so the lowest stale one is built from
// records alone), and requires the same entries. The sampled probes of a
// comparison can miss a stale checkpoint; this cannot. B (through) and W are left out: B is the last sequence number at the time of the
// build, and W is conservative, so each differs legitimately.
// A checkpoint at or below its layer's horizon is rebuilt too: it survives only in a
// prefix the retention did not rewrite, whose history is intact, and a read at or
// after the horizon can reach it.
//
// W is not compared with the rebuilt one, which is conservative and so differs
// legitimately; it is bounded instead. W decides which tokens may use the
// checkpoint, so one too low lets a token below the Seq of a record the checkpoint
// summarises read that record: it must be at least the highest Seq among the
// records of the prefix before the checkpoint's instant.
func requireCheckpointsTrue(s *Store) error {
	type ckpt struct {
		prefix []byte
		ns     int64
		val    []byte
	}
	var all []ckpt
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
			all = append(all, ckpt{slices.Clone(prefix), ns, slices.Clone(it.Value())})
		}
	}
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range all {
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
		// B differs legitimately, and so does W, which is conservative: it counts every
		// record walked, and a rebuild walks other records than the build did.
		floor, err := highestSeqBefore(s, c.prefix, c.ns)
		if err != nil {
			return err
		}
		if stored.w < floor {
			return fmt.Errorf("the checkpoint at %d of prefix %x has W %d, below %d, the highest Seq among the records before it: a token between them would read a record it summarises",
				c.ns, c.prefix, stored.w, floor)
		}
		stored.through, built.through, stored.w, built.w = 0, 0, 0, 0
		a, aerr := appendStamp(nil, stored)
		b, berr := appendStamp(nil, built)
		if err := errors.Join(aerr, berr); err != nil {
			return err
		}
		if !bytes.Equal(a, b) {
			return fmt.Errorf("the checkpoint at %d (%d entries) of prefix %x is not what building it from the records gives (%d entries): it is stale",
				c.ns, len(stored.entries), c.prefix, len(built.entries))
		}
	}
	return nil
}

// highestSeqBefore is the highest Seq among the records of the prefix with an event
// time before ns, read as the builder reads them.
func highestSeqBefore(s *Store, prefix []byte, ns int64) (uint64, error) {
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

// --- the crash property ---

// How a crash is placed. crashBetween falls after an operation has returned; the
// others fall inside one, at a point where the store has a hook: before or after
// the commit of the records, of the checkpoints or of the horizon, or after a
// commit of a retention's rewriting.
type crashAt int

const (
	crashBetween crashAt = iota
	crashBeforeRecords
	crashAfterRecords
	crashBeforeCheckpoints
	crashAfterCheckpoints
	crashBeforeHorizon
	crashAfterHorizon
	crashInRetention
	crashKinds
)

func (c crashAt) String() string {
	return [crashKinds]string{
		"between operations", "before the record commit", "after the record commit", "before the checkpoint commit",
		"after the checkpoint commit", "before the horizon commit", "after the horizon commit", "after a commit of the retention's rewriting",
	}[c]
}

// writeKinds and retainKinds are the places a crash can fall in each operation.
var (
	writeKinds  = []crashAt{crashBetween, crashBeforeRecords, crashAfterRecords, crashBeforeCheckpoints, crashAfterCheckpoints}
	retainKinds = []crashAt{crashBetween, crashBeforeHorizon, crashAfterHorizon, crashInRetention}
)

// crashSnapshot is a crash: the file system as it would be, and how far the run had
// got.
type crashSnapshot struct {
	fs      *vfs.MemFS
	at      crashAt
	percent int
	// begun is how many operations had been started (the last may be in flight);
	// acked is how many had returned.
	begun, acked int
	// pos is how much of the stream the run had taken.
	pos int
}

// crashRun is one seed's run.
type crashRun struct {
	seed  int
	v     crashVariant
	all   []store.Record
	fs    *vfs.MemFS
	s     *Store
	hist  opHistory
	acked int
	pos   int
	maxEv time.Time
	hz    time.Time // the horizon of the operations that have returned
	snaps []crashSnapshot
	taken uint64
}

// clone takes a crash of the file system now.
func (r *crashRun) clone(at crashAt, percent int) crashSnapshot {
	r.taken++
	cfg := vfs.CrashCloneCfg{UnsyncedDataPercent: percent, RNG: rand.New(rand.NewPCG(uint64(r.seed), r.taken))}
	return crashSnapshot{fs: r.fs.CrashClone(cfg), at: at, percent: percent, begun: len(r.hist), acked: r.acked, pos: r.pos}
}

// arm sets the hook of kind at to take a crash on its skip-th firing.
func (r *crashRun) arm(at crashAt, skip, percent int) {
	n := 0
	take := func() {
		if n == skip {
			r.snaps = append(r.snaps, r.clone(at, percent))
		}
		n++
	}
	hook := func() error { take(); return nil }
	switch at {
	case crashBeforeRecords:
		r.s.beforeRecordApply = hook
	case crashAfterRecords:
		r.s.afterRecordApply = hook
	case crashBeforeCheckpoints:
		r.s.beforeCheckpointApply = hook
	case crashAfterCheckpoints:
		r.s.afterCheckpointApply = hook
	case crashBeforeHorizon:
		r.s.beforeHorizonApply = hook
	case crashAfterHorizon:
		r.s.afterHorizonApply = hook
	case crashInRetention:
		r.s.afterRetainCommit = func() { take() }
	}
}

// disarm removes every hook.
func (r *crashRun) disarm() {
	r.s.beforeRecordApply, r.s.afterRecordApply = nil, nil
	r.s.beforeCheckpointApply, r.s.afterCheckpointApply = nil, nil
	r.s.beforeHorizonApply, r.s.afterHorizonApply = nil, nil
	r.s.afterRetainCommit = nil
}

// crashTally counts the crashes a run of seeds took, by where they fell, to show the
// runs reach every place.
type crashTally struct {
	mu sync.Mutex
	by [crashKinds]int
}

func (c *crashTally) add(at crashAt) {
	c.mu.Lock()
	c.by[at]++
	c.mu.Unlock()
}

// runCrash runs one seed and returns the first failure. It writes and retains on a
// store over a crashable file system, and takes crashes of the file system at
// random operations and places; each crash is reopened and checked at once.
func runCrash(seed int, count *crashTally) error {
	v := crashVariantOf(seed)
	all, err := v.stream()
	if err != nil {
		return err
	}
	r := &crashRun{seed: seed, v: v, all: all, fs: vfs.NewCrashableMem()}
	lg := newFatalLog()
	// Compactions run. Their timing is the one thing a seed does not fix, and no
	// outcome may depend on it. (A database that may not compact stalls its writes
	// once enough tables pile up in level 0.)
	r.s, err = Open("db", v.options(r.fs, lg, true))
	if err != nil {
		return err
	}
	defer func() { _ = r.s.Close() }()
	rng := rand.New(rand.NewPCG(uint64(seed), 0xc4a5))
	placed := map[bool]int{}

	steps := 45
	if raceEnabled {
		steps = 25
	}
	for step := 0; step < steps && r.pos < len(r.all); step++ {
		var op histOp
		retain := r.pos > 0 && rng.IntN(10) < 2
		kinds := writeKinds
		if retain {
			kinds = retainKinds
			h := r.maxEv.Add(-time.Duration(1+rng.IntN(240)) * time.Second)
			if !h.After(r.hz) {
				continue
			}
			op = histOp{retain: h, isRetain: true}
		} else {
			end := min(len(r.all), r.pos+1+rng.IntN(40))
			batch := notBefore(r.all[r.pos:end], r.hz)
			r.pos = end
			if len(batch) == 0 {
				continue
			}
			r.maxEv = maxEvent(r.maxEv, batch)
			op = histOp{batch: batch}
		}
		// Two operations in three have a crash taken in or after them.
		crash := rng.IntN(3) != 0
		// Where it falls cycles through the places, from a different start for each
		// seed, so that a list of seeds reaches every one however the random draws go.
		at, percent, skip := kinds[(seed+placed[retain])%len(kinds)], []int{0, 0, 25, 60, 100}[rng.IntN(5)], rng.IntN(3)
		placed[retain]++
		r.hist = append(r.hist, op)
		first := len(r.snaps)
		if crash && at != crashBetween {
			r.arm(at, skip, percent)
		}
		err := op.apply(r.s)
		r.disarm()
		if err != nil {
			return fmt.Errorf("step %d: %s: %w", step, op, err)
		}
		if n := lg.fatal(); n != 0 {
			return fmt.Errorf("step %d: Pebble reported a fatal condition: %s", step, lg.message())
		}
		r.acked = len(r.hist)
		if op.isRetain {
			r.hz = op.retain
		}
		if crash && at == crashBetween {
			r.snaps = append(r.snaps, r.clone(at, percent))
		}
		for _, sn := range r.snaps[first:] {
			count.add(sn.at)
			if err := r.verify(sn, rng.Uint64()); err != nil {
				return fmt.Errorf("step %d (%s), crash %s, %d%% of the unsynced data kept, %d operations begun and %d acknowledged: %w",
					step, op, sn.at, sn.percent, sn.begun, sn.acked, err)
			}
		}
	}
	// The run itself, never crashed, is compared with the reference too, so that a
	// disagreement above is the crash's and not the harness's.
	ref, err := referenceAfter(v.policy, r.hist, len(r.hist))
	if err != nil {
		return err
	}
	defer func() { _ = ref.Close() }()
	if err := compareStores(r.s, ref, r.all[:r.pos], rand.New(rand.NewPCG(uint64(seed), 1))); err != nil {
		return fmt.Errorf("the store the crashes were taken from, at the end: %w", err)
	}
	return nil
}

// verify reopens a crash and checks it.
func (r *crashRun) verify(sn crashSnapshot, seed uint64) error {
	rng := rand.New(rand.NewPCG(seed, 7))
	lg := newFatalLog()
	s, err := Open("db", r.v.options(sn.fs, lg, true))
	if err != nil {
		return fmt.Errorf("reopening failed: %w", err)
	}
	defer func() { _ = s.Close() }()
	n, ref, err := requireReopened(r.v.policy, s, r.hist, sn.acked, sn.begun, r.all[:sn.pos], rng)
	if err != nil {
		return err
	}
	defer func() { _ = ref.Close() }()
	_ = n
	if err := requireCheckpointsTrue(s); err != nil {
		return fmt.Errorf("on reopening: %w", err)
	}
	if err := requireCarriesOn(s, ref, r.all, sn.pos, rng); err != nil {
		return err
	}
	if lg.fatal() != 0 {
		return fmt.Errorf("Pebble reported a fatal condition on the reopened store: %s", lg.message())
	}
	return nil
}

// A crash at any point of the run, with any part of what was not synced kept or
// lost, leaves a store that opens and holds every acknowledged batch and horizon,
// a whole number of the operations after them (never part of a batch), and answers
// as the reference does when fed those. The store then takes the next batches.
func TestCrashKeepsEveryAcknowledgedBatch(t *testing.T) {
	t.Parallel()
	count := &crashTally{}
	t.Run("seeds", func(t *testing.T) {
		for _, seed := range crashSeeds() {
			t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
				t.Parallel()
				if err := runCrash(seed, count); err != nil {
					t.Fatalf("seed %d (%s): %v\n%s", seed, crashVariantOf(seed).name, err, reproduce("TestCrashKeepsEveryAcknowledgedBatch", seed))
				}
			})
		}
	})
	// The subtests above have finished by now. The crashes are taken by hooks that
	// run in the goroutine of the run, so where the seeds' crashes fall is fixed by
	// the seeds.
	total := 0
	for at, n := range count.by {
		total += n
		// The trimmed lists of the race detector reach what they reach; the plain
		// builds, which run the whole list, check every place is reached.
		if n == 0 && !raceEnabled {
			t.Errorf("no crash fell %s: the seeds do not reach it", crashAt(at))
		}
	}
	t.Logf("%d crashes reopened: %v", total, count.by)
}
