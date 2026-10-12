package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The two boot policies the tests run under: the hosts' boot id, which a record
// carries in its Boot, and one that reads the boot from the payload, which a
// baseline's entries do not keep.
var (
	bootIDPolicy  = lifecycle.Policy{BootKey: lifecycle.BootID}
	payloadPolicy = lifecycle.Policy{BootKey: "payload"}
)

// fatalf is what the harness needs of a *testing.T or a *rapid.T.
type fatalf interface {
	Helper()
	Fatalf(format string, args ...any)
}

// bootHarness writes host histories to the store under test, to the reference, and
// to a list it folds itself, and asks all three the same questions. With the
// Background mode it also keeps a Synchronous twin, given the same calls, whose
// data the store's must equal once its pass is over. Records written from the
// pass's hook (writes ahead of the pass and behind it) are kept like any other.
type bootHarness struct {
	t     fatalf
	pol   lifecycle.Policy
	mode  RetentionMode
	hosts []identity.Fingerprint
	fs    vfs.FS
	twFS  vfs.FS
	stop  int // retainStopAfter of a Synchronous store under test
	hook  func()
	rec   *memRecorder

	s, twin *Store
	ref     *memstore.Store
	history []store.Record
	errs    asyncErrs

	seq, last uint64
	horizon   time.Time
	reopens   int
}

// newBootHarness opens the stores. A Synchronous store under test stops its
// retentions after stop chunk commits (zero for never), leaving the rest to the next
// Open or retention.
func newBootHarness(t fatalf, pol lifecycle.Policy, mode RetentionMode, hosts, stop int) *bootHarness {
	t.Helper()
	h := &bootHarness{t: t, pol: pol, mode: mode, fs: vfs.NewMem(), rec: newMemRecorder(), horizon: t0, stop: stop}
	for i := range hosts {
		h.hosts = append(h.hosts, fingerprintOf(catalog.Host, byte(0x60+i)))
	}
	ref, err := memstore.Open(memstore.Options{Policy: pol})
	if err != nil {
		t.Fatalf("%v", err)
	}
	h.ref = ref
	h.s = h.open(h.fs, mode, h.rec)
	if mode == Background {
		h.twFS = vfs.NewMem()
		h.twin = h.open(h.twFS, Synchronous, nil)
	}
	return h
}

func (h *bootHarness) open(fs vfs.FS, mode RetentionMode, rec *memRecorder) *Store {
	h.t.Helper()
	o := Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}, Policy: h.pol, Retention: mode, retainBatchBytes: 1, retainChunkTime: time.Hour}
	if rec != nil {
		o.Recorder = rec
	}
	if mode == Background {
		o.beforeRetainChunk = func() {
			if h.hook != nil {
				h.hook()
			}
		}
	} else if fs == h.fs {
		o.retainStopAfter = h.stop
	}
	s, err := Open("db", o)
	if err != nil {
		h.t.Fatalf("Open: %v", err)
	}
	return s
}

func (h *bootHarness) close() {
	for _, s := range []*Store{h.s, h.twin} {
		if s != nil {
			_ = s.Close()
		}
	}
	_ = h.ref.Close()
}

// reopen closes the store under test and its twin and opens them again. A pass
// that is pending is waited for first.
func (h *bootHarness) reopen() {
	h.t.Helper()
	h.wait()
	if err := h.s.Close(); err != nil {
		h.t.Fatalf("Close: %v", err)
	}
	h.s = h.open(h.fs, h.mode, h.rec)
	if h.twin != nil {
		if err := h.twin.Close(); err != nil {
			h.t.Fatalf("Close: %v", err)
		}
		h.twin = h.open(h.twFS, Synchronous, nil)
	}
	h.reopens++
}

// host is an observation of host i by producer at instant at, in boot boot (none if
// empty), as a run to through if that is later, with a TTL.
func (h *bootHarness) host(i int, producer lifecycle.Producer, at time.Time, boot string, through time.Time, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L1, Subject: store.EntitySubject(h.hosts[i]), Producer: producer, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("host"),
	}
	if through.After(at) {
		r.Through = through
	}
	switch {
	case h.pol.BootKey == lifecycle.BootID:
		r.Boot = boot
	case boot != "":
		r.Payload = []byte(boot)
	}
	return r
}

// gone is a delete of host i by producer at instant at.
func (h *bootHarness) gone(i int, producer lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: catalog.L1, Subject: store.EntitySubject(h.hosts[i]), Producer: producer, EventTime: at, Kind: lifecycle.Delete}
}

// write gives the batch the next sequence numbers and writes it everywhere. It may
// run on the pass's goroutine, from the hook, so it reports errors to errs.
func (h *bootHarness) write(batch ...store.Record) {
	out := cloneAll(batch)
	for i := range out {
		h.seq++
		out[i].Seq = h.seq
	}
	h.history = append(h.history, cloneAll(out)...)
	h.errs.add(h.s.Write(bg, cloneAll(out)))
	if h.twin != nil {
		h.errs.add(h.twin.Write(bg, cloneAll(out)))
	}
	h.errs.add(h.ref.Write(bg, cloneAll(out)))
}

// retain moves the horizon to at everywhere, the store under test last, so that
// the writes its pass's hook makes come after the retention everywhere. A
// Synchronous store that stops on purpose (stop) leaves the rest to the next Open
// or retention.
func (h *bootHarness) retain(at time.Time) {
	h.t.Helper()
	h.horizon, h.last = at, h.seq
	if h.twin != nil {
		if err := h.twin.Retain(bg, at); err != nil {
			h.t.Fatalf("Retain of the twin: %v", err)
		}
	}
	if err := h.ref.Retain(bg, at); err != nil {
		h.t.Fatalf("Retain of the reference: %v", err)
	}
	if err := h.s.Retain(bg, at); err != nil && !errors.Is(err, errInjected) {
		h.t.Fatalf("Retain: %v", err)
	}
}

// wait returns when the pass of a Background store is over, and checks that its
// data is then the Synchronous twin's, byte for byte.
func (h *bootHarness) wait() {
	h.t.Helper()
	if h.mode != Background {
		return
	}
	ctx, cancel := context.WithTimeout(bg, 5*time.Minute)
	defer cancel()
	if err := h.s.Instrument().WaitRetained(ctx); err != nil {
		h.t.Fatalf("WaitRetained: %v", err)
	}
	h.checkErrs()
	got, want := dataOf(h.t, h.s), dataOf(h.t, h.twin)
	if !slices.Equal(got, want) {
		h.t.Fatalf("the data of the Background store is not the Synchronous store's:\n%s\nthe Synchronous store's:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func (h *bootHarness) checkErrs() {
	h.t.Helper()
	h.errs.mu.Lock()
	defer h.errs.mu.Unlock()
	if len(h.errs.errs) > 0 {
		h.t.Fatalf("%v", errors.Join(h.errs.errs...))
	}
}

// folded is what lifecycle.Fold says of host fp at instant at over every record of
// it the token sees: the whole history, nothing retained.
func (h *bootHarness) folded(fp identity.Fingerprint, at time.Time, tok uint64) string {
	var as []lifecycle.Assertion
	for _, r := range h.history {
		if r.Subject.A == fp && r.Seq <= tok {
			as = append(as, r.Assertion())
		}
	}
	tl, err := lifecycle.Fold(as, h.pol)
	var cc *lifecycle.CloneCollisionError
	switch {
	case errors.As(err, &cc):
		return exactCollision(cc)
	case err != nil:
		return "error " + err.Error()
	}
	return fmt.Sprint(tl.AliveAt(at))
}

// exactCollision describes every field of a collision, the instants to the
// nanosecond.
func exactCollision(c *lifecycle.CloneCollisionError) string {
	return fmt.Sprintf("quarantined: %q at %d after %q first at %d", c.StaleBoot, c.ObservedAt.UnixNano(), c.NewerBoot, c.NewerFirstSeen.UnixNano())
}

// answered describes an Alive of a store the way folded does.
func answered(fp identity.Fingerprint, ok bool, err error) string {
	var q *store.QuarantineError
	switch {
	case errors.As(err, &q) && q.Collision != nil:
		if q.Entity != fp || q.Layer != catalog.L1 {
			return fmt.Sprintf("quarantined %s in %s", q.Entity, q.Layer)
		}
		return exactCollision(q.Collision)
	case err != nil:
		return "error " + err.Error()
	}
	return fmt.Sprint(ok)
}

// check asks the store, the reference and the fold whether each host is alive at
// each instant, at every token from the last retention's to the last Seq and at
// Latest, and fails on the first difference. It returns how many of the answers
// were quarantines.
func (h *bootHarness) check(times []time.Time, what string) (quarantines int) {
	h.t.Helper()
	h.checkErrs()
	toks := []uint64{store.Latest}
	for tok := h.last; tok <= h.seq; tok++ {
		toks = append(toks, tok)
	}
	for _, fp := range h.hosts {
		for _, at := range times {
			for _, tok := range toks {
				sc := store.Scope{Layer: catalog.L1, AsOf: tok}
				want := h.folded(fp, at, min(tok, h.seq))
				got, err := h.s.Alive(bg, fp, at, sc)
				refOK, refErr := h.ref.Alive(bg, fp, at, sc)
				a, b := answered(fp, got, err), answered(fp, refOK, refErr)
				if a != want || b != want {
					h.t.Fatalf("%s: Alive(%s, %d, asOf %d): the store says %s, the reference %s, the fold of the whole history %s\nhistory:\n%s",
						what, fp, at.Sub(t0), tok, a, b, want, h.describe())
				}
				if strings.HasPrefix(a, "quarantined") {
					quarantines++
				}
			}
		}
	}
	return quarantines
}

func (h *bootHarness) describe() string {
	var b strings.Builder
	for _, r := range h.history {
		boot := r.Boot
		if h.pol.BootKey != lifecycle.BootID {
			boot = string(r.Payload)
		}
		fmt.Fprintf(&b, "  seq %d host %s %s %v @%d", r.Seq, r.Subject.A, r.Producer, r.Kind, r.EventTime.Sub(t0))
		if !r.Through.IsZero() {
			fmt.Fprintf(&b, " through %d", r.Through.Sub(t0))
		}
		fmt.Fprintf(&b, " ttl %d boot %q\n", r.TTL, boot)
	}
	return b.String()
}

// dataOf is every data key and value in key order, printed.
func dataOf(t fatalf, s *Store) []string {
	t.Helper()
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer func() { _ = it.Close() }()
	var out []string
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, fmt.Sprintf("%x=%x", it.Key(), it.Value()))
	}
	if err := it.Error(); err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

// baselines decodes every baseline of the store's entity prefixes, by prefix,
// with its leading format byte.
func baselines(t fatalf, s *Store) map[string]formatted {
	t.Helper()
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatalf("%v", err)
	}
	defer func() { _ = it.Close() }()
	out := map[string]formatted{}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, _, _, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatalf("%v", err)
		}
		if kind != kindBaseline {
			continue
		}
		raw := slices.Clone(it.Value())
		st, err := decodeStamp(raw)
		if err != nil {
			t.Fatalf("a baseline does not decode: %v", err)
		}
		out[string(prefix)] = formatted{format: raw[0], st: st, raw: raw}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

type formatted struct {
	format byte
	st     stamp
	raw    []byte
}

// baselineOf is the baseline of host i, if it has one.
func (h *bootHarness) baselineOf(i int) (formatted, bool) {
	h.t.Helper()
	prefix, ok := h.s.prefixOf(catalog.L1, h.hosts[i], dirEntity)
	if !ok {
		h.t.Fatalf("no prefix")
	}
	b, ok := baselines(h.t, h.s)[string(prefix)]
	return b, ok
}

// The central property: on random host histories (reboots, clones that report an
// old boot, late records, records that overwrite one another at an instant, runs
// with a last instant, deletes and TTLs, from two producers) with retentions in
// between, Alive's verdict and every field of a collision, at every token from the
// last retention's on and at instants from the horizon on, are what lifecycle.Fold
// says of the whole history, and what the reference says. Both boot policies, both
// retention modes: in the Background mode the writes after a retention are made from
// the pass's hook, ahead of the pass or behind it, so that a write rewrites a host
// prefix before the pass does, and once the pass is over the data must be the
// Synchronous store's byte for byte. A Synchronous store sometimes stops its pass
// partway, leaving the rest to the next Open, and the stores are reopened between
// steps.
//
// The draws are rapid's: -rapid.seed (or RAPID_SEED) and -rapid.checks fix and
// size a run. What the histories exercised is logged.
func TestPropertyAliveIsTheFoldOfTheWholeHistoryThroughRetention(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var cases, background, stopping, payload, quarantines, formatTwo, carried, storedCollisions, touches, reopens int
	rapid.Check(t, func(rt *rapid.T) {
		pol := rapid.SampledFrom([]lifecycle.Policy{bootIDPolicy, payloadPolicy}).Draw(rt, "policy")
		pol.Skew = time.Duration(rapid.SampledFrom([]int{0, 0, 1, 3}).Draw(rt, "skew"))
		mode := rapid.SampledFrom([]RetentionMode{Synchronous, Background}).Draw(rt, "mode")
		hosts := rapid.IntRange(1, 3).Draw(rt, "hosts")
		stop := 0
		if mode == Synchronous {
			stop = rapid.SampledFrom([]int{0, 0, 1, 2}).Draw(rt, "stop after")
		}
		h := newBootHarness(rt, pol, mode, hosts, stop)
		defer h.close()
		at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
		hzn := 0
		instants := map[int][]int{} // by host, the instants written, for overwrites
		boots := []string{"boot-a", "boot-b", "boot-c"}
		drawRecord := func(label string) store.Record {
			i := rapid.IntRange(0, hosts-1).Draw(rt, label+" host")
			producer := rapid.SampledFrom([]lifecycle.Producer{"agent", "node"}).Draw(rt, label+" producer")
			n := hzn + rapid.IntRange(0, 40).Draw(rt, label+" at")
			if prev := instants[i]; len(prev) > 0 && rapid.IntRange(0, 3).Draw(rt, label+" same instant") == 0 {
				if p := rapid.SampledFrom(prev).Draw(rt, label+" instant"); p >= hzn {
					n = p
				}
			}
			instants[i] = append(instants[i], n)
			if rapid.IntRange(0, 5).Draw(rt, label+" kind") == 0 {
				return h.gone(i, producer, at(n))
			}
			boot := rapid.SampledFrom(boots).Draw(rt, label+" boot")
			if pol.BootKey == lifecycle.BootID && rapid.IntRange(0, 5).Draw(rt, label+" no boot") == 0 {
				boot = ""
			}
			through := time.Time{}
			if rapid.IntRange(0, 3).Draw(rt, label+" run") == 0 {
				through = at(n + rapid.IntRange(0, 30).Draw(rt, label+" run length"))
			}
			ttl := time.Duration(rapid.SampledFrom([]int{0, 0, 2, 7}).Draw(rt, label+" ttl"))
			return h.host(i, producer, at(n), boot, through, ttl)
		}
		drawBatch := func(label string) []store.Record {
			var batch []store.Record
			for k := range rapid.IntRange(1, 3).Draw(rt, label+" size") {
				batch = append(batch, drawRecord(fmt.Sprintf("%s[%d]", label, k)))
			}
			return batch
		}
		times := func() []time.Time {
			return []time.Time{at(hzn), at(hzn + 1), at(hzn + 7), at(hzn + 25), at(hzn + 80), store.MaxEventTime}
		}
		seen := 0
		rounds := rapid.IntRange(1, 4).Draw(rt, "rounds")
		for round := range rounds {
			for b := range rapid.IntRange(0, 3).Draw(rt, "writes") {
				h.write(drawBatch(fmt.Sprintf("round %d write %d", round, b))...)
				if rapid.IntRange(0, 4).Draw(rt, "reopen after a write") == 0 {
					h.reopen()
				}
			}
			seen += h.check(times(), fmt.Sprintf("round %d, before the retention", round))
			hzn += rapid.IntRange(1, 30).Draw(rt, "horizon step")
			// The writes after the retention: in the Background mode each is made
			// before the chunk of its slot, from the pass's hook (one prefix per chunk),
			// and those whose slot is never reached after the pass.
			var later [][]store.Record
			var slots []int
			for b := range rapid.IntRange(0, 3).Draw(rt, "later writes") {
				later = append(later, drawBatch(fmt.Sprintf("round %d later %d", round, b)))
				slots = append(slots, rapid.IntRange(0, hosts).Draw(rt, "slot"))
			}
			slices.Sort(slots)
			next, chunk := 0, 0
			if mode == Background {
				h.hook = func() {
					for next < len(later) && slots[next] <= chunk {
						h.write(later[next]...)
						next++
					}
					chunk++
				}
			}
			h.retain(at(hzn))
			if mode == Background {
				h.wait()
				h.hook = nil
			}
			for ; next < len(later); next++ {
				h.write(later[next]...)
			}
			seen += h.check(times(), fmt.Sprintf("round %d, after the retention", round))
			if rapid.Bool().Draw(rt, "reopen after the round") {
				h.reopen()
				seen += h.check(times(), fmt.Sprintf("round %d, reopened", round))
			}
		}
		if mode == Synchronous && stop > 0 {
			h.reopen() // finishes any pass that stopped, so that the baselines are counted
			seen += h.check(times(), "reopened at the end")
		}
		mu.Lock()
		defer mu.Unlock()
		cases++
		if mode == Background {
			background++
		}
		if stop > 0 {
			stopping++
		}
		if pol.BootKey != lifecycle.BootID {
			payload++
		}
		quarantines += seen
		for _, b := range baselines(rt, h.s) {
			if b.format == stampFormatBoots {
				formatTwo++
				if len(b.st.boots.carried) > 0 {
					carried++
				}
				if b.st.boots.collision != nil {
					storedCollisions++
				}
			}
		}
		touches += int(h.rec.counters["retain.touch_rewrites"])
		reopens += h.reopens
	})
	t.Logf("%d histories (%d in the Background mode, %d whose Synchronous passes stop partway, %d with the boot in the payload): %d quarantined answers, %d baselines of format 2, %d with carried sightings, %d holding a collision, %d host prefixes rewritten by a write ahead of the pass, %d reopenings",
		cases, background, stopping, payload, quarantines, formatTwo, carried, storedCollisions, touches, reopens)
}

// eachBootCase runs a scripted case under both policies and both modes.
func eachBootCase(t *testing.T, hosts int, skew time.Duration, run func(t *testing.T, h *bootHarness)) {
	for _, pol := range []lifecycle.Policy{bootIDPolicy, payloadPolicy} {
		pol.Skew = skew
		for _, mode := range []RetentionMode{Synchronous, Background} {
			t.Run(fmt.Sprintf("boot key %s, mode %d", pol.BootKey, mode), func(t *testing.T) {
				t.Parallel()
				h := newBootHarness(t, pol, mode, hosts, 0)
				defer h.close()
				run(t, h)
			})
		}
	}
}

// step runs one step of a scripted case, reopens the stores, and checks them at
// instants from the horizon on.
func (h *bootHarness) step(what string, do func()) int {
	h.t.Helper()
	do()
	h.wait()
	n := h.check(h.instants(), what)
	h.reopen()
	return n + h.check(h.instants(), what+", reopened")
}

func (h *bootHarness) instants() []time.Time {
	return []time.Time{h.horizon, h.horizon.Add(1), h.horizon.Add(time.Minute), h.horizon.Add(time.Hour), store.MaxEventTime}
}

// Two hosts boot as boot-a and then boot-b; a retention discards all of it; a
// clone of boot-a on host 1 after the horizon collides with boot-b, which the
// baseline remembers, and host 2, which only goes on in boot-b, does not.
func TestACloneAfterTheHorizonIsQuarantinedByTheBootsTheBaselineKeeps(t *testing.T) {
	t.Parallel()
	eachBootCase(t, 2, 0, func(t *testing.T, h *bootHarness) {
		at := func(d time.Duration) time.Time { return t0.Add(d) }
		h.step("boot-a then boot-b", func() {
			h.write(h.host(0, "agent", at(0), "boot-a", time.Time{}, 0), h.host(1, "agent", at(0), "boot-a", time.Time{}, 0))
			h.write(h.host(0, "agent", at(time.Minute), "boot-b", time.Time{}, 0), h.host(1, "agent", at(time.Minute), "boot-b", time.Time{}, 0))
		})
		h.step("a retention past both", func() { h.retain(at(10 * time.Minute)) })
		for i := range 2 {
			b, ok := h.baselineOf(i)
			if !ok || b.format != stampFormatBoots || len(b.st.boots.boots) != 2 || b.st.boots.collision != nil || len(b.st.boots.carried) != 0 {
				t.Fatalf("host %d: the baseline is %+v (found %v); want one of format 2 with two boots", i, b, ok)
			}
			want := []lifecycle.Boot{{ID: "boot-a", FirstSeen: at(0), LastSeen: at(0)}, {ID: "boot-b", FirstSeen: at(time.Minute), LastSeen: at(time.Minute)}}
			if got := b.st.boots.boots; !slices.EqualFunc(got, want, func(x, y lifecycle.Boot) bool {
				return x.ID == y.ID && x.FirstSeen.Equal(y.FirstSeen) && x.LastSeen.Equal(y.LastSeen)
			}) {
				t.Errorf("host %d: boots %+v, want %+v", i, got, want)
			}
		}
		n := h.step("a clone of boot-a on host 1, boot-b on host 2", func() {
			h.write(h.host(0, "clone", at(20*time.Minute), "boot-a", time.Time{}, 0), h.host(1, "agent", at(20*time.Minute), "boot-b", time.Time{}, 0))
		})
		ok, err := h.s.Alive(bg, h.hosts[0], at(30*time.Minute), store.Current(catalog.L1))
		want := exactCollision(&lifecycle.CloneCollisionError{StaleBoot: "boot-a", ObservedAt: at(20 * time.Minute), NewerBoot: "boot-b", NewerFirstSeen: at(time.Minute)})
		if got := answered(h.hosts[0], ok, err); got != want {
			t.Errorf("host 1 after the clone: %s, want %s", got, want)
		}
		if ok, err := h.s.Alive(bg, h.hosts[1], at(30*time.Minute), store.Current(catalog.L1)); err != nil || !ok {
			t.Errorf("host 2 after it went on in boot-b: %v, %v", ok, err)
		}
		if n == 0 {
			t.Error("no answer was a quarantine")
		}
		// A second retention past the clone keeps the collision in the baseline.
		h.step("a retention past the clone", func() { h.retain(at(time.Hour)) })
		if b, _ := h.baselineOf(0); b.st.boots == nil || b.st.boots.collision == nil || exactCollision(b.st.boots.collision) != want {
			t.Errorf("host 1's baseline after the clone is retained: %+v", b.st.boots)
		}
		if b, _ := h.baselineOf(1); b.st.boots == nil || b.st.boots.collision != nil {
			t.Errorf("host 2's baseline: %+v", b.st.boots)
		}
	})
}

// A run that began before the horizon and lasts past it is seen at its last
// instant, after the horizon: the baseline carries that sighting, and it collides
// with a boot that first appeared before its end, before the horizon or after it.
// The run is not the newest record of its reference, so a baseline that took only
// the newest record of each reference would lose it.
func TestARunAcrossTheHorizonIsCarried(t *testing.T) {
	t.Parallel()
	for name, newerAt := range map[string]time.Duration{"a newer boot before the horizon": 8 * time.Minute, "a newer boot after it": 12 * time.Minute} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			eachBootCase(t, 1, 0, func(t *testing.T, h *bootHarness) {
				at := func(d time.Duration) time.Time { return t0.Add(d) }
				hz := at(10 * time.Minute)
				h.step("a run of boot-a to after the horizon, and a later record of the reference", func() {
					h.write(h.host(0, "agent", at(0), "boot-a", at(15*time.Minute), 0))
					h.write(h.host(0, "agent", at(time.Minute), "boot-a", time.Time{}, 0)) // newer than the run, by the same producer
				})
				if newerAt < 10*time.Minute {
					h.step("boot-b before the horizon", func() { h.write(h.host(0, "node", at(newerAt), "boot-b", time.Time{}, 0)) })
				}
				h.step("the retention", func() { h.retain(hz) })
				b, ok := h.baselineOf(0)
				if !ok || b.st.boots == nil || len(b.st.boots.carried) != 1 || b.st.boots.carried[0].ID != "boot-a" || !b.st.boots.carried[0].At.Equal(at(15*time.Minute)) {
					t.Fatalf("the baseline carries %+v (found %v); want boot-a at 15m", b.st.boots, ok)
				}
				if newerAt > 10*time.Minute {
					h.step("boot-b after the horizon", func() { h.write(h.host(0, "node", at(newerAt), "boot-b", time.Time{}, 0)) })
				}
				ok2, err := h.s.Alive(bg, h.hosts[0], hz, store.Current(catalog.L1))
				want := exactCollision(&lifecycle.CloneCollisionError{StaleBoot: "boot-a", ObservedAt: at(15 * time.Minute), NewerBoot: "boot-b", NewerFirstSeen: at(newerAt)})
				if got := answered(h.hosts[0], ok2, err); got != want {
					t.Errorf("Alive = %s, want %s", got, want)
				}
				// A retention past the run's end folds the carried sighting into the
				// state, and the collision stays.
				h.step("a retention past the run's end", func() { h.retain(at(20 * time.Minute)) })
				if b, _ := h.baselineOf(0); b.st.boots == nil || b.st.boots.collision == nil || exactCollision(b.st.boots.collision) != want || len(b.st.boots.carried) != 0 {
					t.Errorf("after the run's end is retained: %+v", b.st.boots)
				}
			})
		})
	}
}

// The skew is judged across the horizon against the first sighting of the newer
// boot the baseline keeps: an old boot seen exactly the skew after it is not a
// collision, and a nanosecond more is.
func TestTheSkewIsJudgedAcrossTheHorizon(t *testing.T) {
	t.Parallel()
	skew := time.Minute
	for name, past := range map[string]time.Duration{"exactly at the limit": 0, "a nanosecond past it": 1} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			eachBootCase(t, 1, skew, func(t *testing.T, h *bootHarness) {
				at := func(d time.Duration) time.Time { return t0.Add(d) }
				h.step("boot-a, then boot-b", func() {
					h.write(h.host(0, "agent", at(0), "boot-a", time.Time{}, 0), h.host(0, "node", at(10*time.Second), "boot-b", time.Time{}, 0))
				})
				h.step("the retention", func() { h.retain(at(30 * time.Second)) })
				h.step("boot-a again", func() {
					h.write(h.host(0, "agent", at(10*time.Second+skew+past), "boot-a", time.Time{}, 0))
				})
				ok, err := h.s.Alive(bg, h.hosts[0], at(2*time.Minute), store.Current(catalog.L1))
				got := answered(h.hosts[0], ok, err)
				want := "true"
				if past > 0 {
					want = exactCollision(&lifecycle.CloneCollisionError{StaleBoot: "boot-a", ObservedAt: at(10*time.Second + skew + past), NewerBoot: "boot-b", NewerFirstSeen: at(10 * time.Second)})
				}
				if got != want {
					t.Errorf("Alive = %s, want %s", got, want)
				}
			})
		})
	}
}

// A host prefix with no boot under a boot key, and every prefix under no boot key,
// gets the baseline it always got, of format 1; only a boot under a boot key makes
// one of format 2. Every prefix is rewritten.
func TestOnlyABootUnderABootKeyMakesABaselineOfFormatTwo(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		policy lifecycle.Policy
		boot   string
		format byte
	}{
		"a boot under a boot key":  {bootIDPolicy, "boot-a", stampFormatBoots},
		"no boot under a boot key": {bootIDPolicy, "", stampFormat},
		"a boot with no boot key":  {lifecycle.Policy{}, "boot-a", stampFormat},
		"no boot and no boot key":  {lifecycle.Policy{}, "", stampFormat},
		"a payload under its key":  {payloadPolicy, "", stampFormatBoots},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newMemRecorder()
			p := newPair(t, Options{Policy: tc.policy, Recorder: rec})
			p.write(hostObservation(1, t0, tc.boot))
			p.retain(t0.Add(time.Hour))
			if got := rec.counters["retain.range_deletes"]; got != 1 {
				t.Errorf("range deletes = %d, want 1", got)
			}
			bs := baselines(t, p.s)
			if len(bs) != 1 {
				t.Fatalf("%d baselines, want 1", len(bs))
			}
			for _, b := range bs {
				if b.format != tc.format {
					t.Errorf("a baseline of format %d, want %d", b.format, tc.format)
				}
				if b.format == stampFormat {
					// Byte for byte what the store wrote before format 2.
					want, err := appendStamp(nil, stamp{kind: kindBaseline, foldVersion: foldVersion, through: 1, w: 1, horizon: t0.Add(time.Hour), entries: b.st.entries})
					if err != nil {
						t.Fatal(err)
					}
					if string(b.raw) != string(want) {
						t.Errorf("the baseline is %x; want %x", b.raw, want)
					}
				}
			}
			host := hostObservation(0, t0, "").Subject.A
			got, err := p.s.Alive(bg, host, t0.Add(2*time.Hour), store.Current(catalog.L1))
			want, _ := p.ref.Alive(bg, host, t0.Add(2*time.Hour), store.Current(catalog.L1))
			if err != nil || got != want {
				t.Errorf("Alive = %v, %v; the reference says %v", got, err, want)
			}
		})
	}
}

// A host whose boots all went, and that nothing keeps alive at the horizon, still
// has a baseline: its boot state, with no entry.
func TestABaselineKeepsTheBootsOfAHostThatIsGone(t *testing.T) {
	t.Parallel()
	eachBootCase(t, 1, 0, func(t *testing.T, h *bootHarness) {
		at := func(d time.Duration) time.Time { return t0.Add(d) }
		h.step("boot-a, boot-b, then a delete", func() {
			h.write(h.host(0, "agent", at(0), "boot-a", time.Time{}, 0), h.host(0, "agent", at(time.Minute), "boot-b", time.Time{}, 0), h.gone(0, "agent", at(2*time.Minute)))
		})
		h.step("the retention", func() { h.retain(at(10 * time.Minute)) })
		b, ok := h.baselineOf(0)
		if !ok || len(b.st.entries) != 0 || b.st.boots == nil || len(b.st.boots.boots) != 2 {
			t.Fatalf("the baseline is %+v (found %v); want two boots and no entry", b, ok)
		}
		h.step("boot-a again", func() { h.write(h.host(0, "agent", at(20*time.Minute), "boot-a", time.Time{}, 0)) })
	})
}

// A database whose tag names stamp format 1 is one written before format 2, in
// which a retention under a boot key kept a host's prefix whole: its records stay
// below the horizon, beside an older baseline of format 1. It is built here as that
// version left it. Opened read-only, it is read as it is and its tag left; opened
// to write, its tag is rewritten at once, its whole prefix still decides a clone,
// and the next retention gives it a baseline of format 2. A tag of another stamp
// format is refused.
func TestADatabaseOfStampFormatOneIsReadAndItsTagRewritten(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	opts := func(readOnly bool) Options {
		o := Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}, Policy: bootIDPolicy}
		o.ReadOnly = readOnly
		return o
	}
	ref, err := memstore.Open(memstore.Options{Policy: bootIDPolicy})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ref.Close() }()
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	host := hostObservation(0, t0, "").Subject.A
	s := mustOpenFS(t, opts(false))
	both := func(recs ...store.Record) {
		t.Helper()
		for _, x := range []store.Store{s, ref} {
			if err := x.Write(bg, cloneAll(recs)); err != nil {
				t.Fatal(err)
			}
		}
	}
	// A baseline of format 1 from a retention before any boot, then boots.
	both(hostObservation(1, at(0), ""))
	for _, x := range []store.Store{s, ref} {
		if err := x.Retain(bg, at(30*time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	both(hostObservation(2, at(time.Minute), "boot-a"), hostObservation(3, at(2*time.Minute), "boot-b"))
	// The retention at h1 of the earlier version: the horizons published, the
	// prefix kept whole, and the tag of format 1.
	h1 := at(10 * time.Minute)
	raw, err := pebblekv.EncodeLayerHorizon(h1, 3)
	if err != nil {
		t.Fatal(err)
	}
	for l := catalog.L0; l <= catalog.L3; l++ {
		if err := s.kv.Set(metaKey(pebblekv.HorizonMetaName(l)), raw, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	for k, v := range map[string][]byte{metaHorizonLast: raw, pebblekv.MetaFormat: []byte(formatTagStamp1)} {
		if err := s.kv.Set(metaKey(k), v, pebble.Sync); err != nil {
			t.Fatal(err)
		}
	}
	if err := ref.Retain(bg, h1); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	tag := func(s *Store) string {
		t.Helper()
		v, err := s.kv.GetMeta(metaKey(pebblekv.MetaFormat))
		if err != nil {
			t.Fatal(err)
		}
		return string(v)
	}
	from, last := h1, uint64(3) // the horizon and its Seq
	same := func(s *Store, what string) {
		t.Helper()
		for _, x := range []time.Time{from, at(time.Hour), store.MaxEventTime} {
			for _, tok := range []uint64{last, store.Latest} {
				sc := store.Scope{Layer: catalog.L1, AsOf: tok}
				got, gerr := s.Alive(bg, host, x, sc)
				want, werr := ref.Alive(bg, host, x, sc)
				if a, b := answered(host, got, gerr), answered(host, want, werr); a != b {
					t.Fatalf("%s: Alive at %v, asOf %d: %s; the reference says %s", what, x.Sub(t0), tok, a, b)
				}
			}
		}
	}

	ro := mustOpenFS(t, opts(true))
	if got := tag(ro); got != formatTagStamp1 {
		t.Errorf("a read-only Open changed the tag to %q", got)
	}
	same(ro, "read-only")
	if err := ro.Close(); err != nil {
		t.Fatal(err)
	}

	s = mustOpenFS(t, opts(false))
	defer func() { _ = s.Close() }()
	if got := tag(s); got != formatTag {
		t.Errorf("the tag after an Open that can write is %q, want %q", got, formatTag)
	}
	same(s, "opened to write")
	// A clone of boot-a after the horizon is judged against the records kept whole.
	both(hostObservation(4, at(20*time.Minute), "boot-a"))
	same(s, "after the clone")
	if _, err := s.Alive(bg, host, at(time.Hour), store.Current(catalog.L1)); !errors.As(err, new(*store.QuarantineError)) {
		t.Fatalf("the clone is not quarantined: %v", err)
	}
	for _, x := range []store.Store{s, ref} {
		if err := x.Retain(bg, at(30*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	from, last = at(30*time.Minute), 4
	same(s, "after a retention past the clone")
	bs := baselines(t, s)
	if len(bs) != 1 {
		t.Fatalf("%d baselines, want 1", len(bs))
	}
	for _, b := range bs {
		if b.format != stampFormatBoots || len(b.st.boots.boots) != 2 || b.st.boots.collision == nil {
			t.Errorf("the baseline after the retention: format %d, %+v", b.format, b.st.boots)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Another stamp format is not this layout's.
	s = mustOpenFS(t, opts(false))
	if err := s.kv.Set(metaKey(pebblekv.MetaFormat), []byte("toposhift/store/log;key=1;record=1;stamp=3;meta=1"), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err := Open("db", opts(false)); !errors.Is(err, store.ErrInvalid) {
		if s != nil {
			_ = s.Close()
		}
		t.Errorf("Open of a database of stamp format 3: %v, want ErrInvalid", err)
	}
}

// A database of stamp format 1 left with a retention unfinished (its marker in
// place, one prefix rewritten) has its tag rewritten by the Open that finishes the
// retention before that retention commits anything: the database after each
// commit of the pass (each Open stopped after one) holds the tag of format 2, and
// the first baseline of format 2 comes with the first of them.
// A failure of the commit of the tag leaves the database as it was, and openable.
func TestTheTagIsRewrittenBeforeAnUnfinishedRetentionWritesFormatTwo(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	opts := func(f vfs.FS, readOnly bool) Options {
		o := Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: f}, Policy: bootIDPolicy, retainBatchBytes: 1}
		o.ReadOnly = readOnly
		return o
	}
	hosts := []identity.Fingerprint{fingerprintOf(catalog.Host, 0x60), fingerprintOf(catalog.Host, 0x61), fingerprintOf(catalog.Host, 0x62)}
	rec := func(seq uint64, i int, at time.Duration, boot string) store.Record {
		return store.Record{Layer: catalog.L1, Subject: store.EntitySubject(hosts[i]), Producer: "agent", EventTime: t0.Add(at), Seq: seq, Kind: lifecycle.Observe, Boot: boot, Payload: []byte("host")}
	}
	// The first host in key order has no boot, so the chunk that is committed before
	// the stop writes a baseline of format 1, as the earlier version would have.
	o := opts(fs, false)
	o.retainStopAfter = 1
	s := mustOpenFS(t, o)
	if err := s.Write(bg, []store.Record{
		rec(1, 0, 0, ""), rec(2, 1, 0, "boot-a"), rec(3, 2, 0, "boot-a"),
		rec(4, 1, time.Minute, "boot-b"), rec(5, 2, time.Minute, "boot-b"),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Retain(bg, t0.Add(10*time.Minute)); !errors.Is(err, errInjected) {
		t.Fatalf("Retain = %v, want it stopped after one commit", err)
	}
	if err := s.kv.Set(metaKey(pebblekv.MetaFormat), []byte(formatTagStamp1), pebble.Sync); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// What the database holds, read without changing it.
	look := func(f vfs.FS) (tag string, two int, marker bool, data []string, meta map[string]string) {
		t.Helper()
		r := mustOpenFS(t, opts(f, true))
		defer func() { _ = r.Close() }()
		meta = metaOf(t, r)
		for _, b := range baselines(t, r) {
			if b.format == stampFormatBoots {
				two++
			}
		}
		return meta[pebblekv.MetaFormat], two, readMarker(t, r) != nil, dataOf(t, r), meta
	}
	tag, two, marker, data, meta := look(fs)
	if tag != formatTagStamp1 || two != 0 || !marker {
		t.Fatalf("the database built: tag %q, %d baselines of format 2, marker %v; want format 1, none, and a marker", tag, two, marker)
	}

	// The commit of the tag fails: Open fails, and the database is as it was.
	o = opts(fs, false)
	o.beforeTagApply = func() error { return errors.New("the tag's commit failed") }
	if s, err := Open("db", o); err == nil || !strings.Contains(err.Error(), "the tag's commit failed") {
		if s != nil {
			_ = s.Close()
		}
		t.Fatalf("Open with the tag's commit failing = %v", err)
	}
	if tag2, two2, marker2, data2, meta2 := look(fs); tag2 != tag || two2 != two || marker2 != marker || !slices.Equal(data2, data) || fmt.Sprint(meta2) != fmt.Sprint(meta) {
		t.Fatalf("a failed tag commit changed the database: tag %q, %d of format 2, marker %v", tag2, two2, marker2)
	}

	// The Opens that finish the retention, each stopped after one commit of it (and
	// failing for that), until one finishes: after every commit, the commit counted
	// by the hook, the database holds the tag of format 2.
	var commits int
	var seenTwo []int
	for range 10 {
		o = opts(fs, false)
		o.resumeStopAfter = 1
		o.afterRetainCommit = func() { commits++ }
		s, err := Open("db", o)
		if err == nil {
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			break
		}
		if !errors.Is(err, errInjected) {
			t.Fatal(err)
		}
		tag, two, _, _, _ := look(fs)
		if tag != formatTag {
			t.Fatalf("after commit %d of the finished retention the tag is %q, with %d baselines of format 2", commits, tag, two)
		}
		seenTwo = append(seenTwo, two)
	}
	if !slices.Equal(seenTwo, []int{1, 2}) {
		t.Errorf("baselines of format 2 after each commit: %v, want 1 then 2", seenTwo)
	}
	s = mustOpenFS(t, opts(fs, false))
	defer func() { _ = s.Close() }()
	if readMarker(t, s) != nil {
		t.Error("the marker is left")
	}
}
