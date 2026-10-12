package l1growth_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The L1 growth check: does the read work on the host and node prefixes stay within
// the read budget of G1 when the layer keeps 90 days instead of 30?
//
// It simulates 181.5 days (twice the window, and a day and a half) of host and node
// refreshes through the product's coalescer into the product's store, with the
// product's lifecycle policy, checkpoint policy and per-layer retention offsets
// (L0 kept, L1 90 days, L2 and L3 30), and retains once a day from day 31 on. On
// days 30, 91.5 and 181.5 it stops the stream, and reads every host<- and node->L1
// prefix at each of the 24 whole hours ending there and at up to ten ages, counting
// what each read did. Nothing is timed.
//
// The pass rule is fixed before any run and is never retuned (see l1gVerdict):
//
//  1. every ratio of work to budget is at most 1, on every measured day;
//  2. the 90th percentile of day 181.5 is at most 1.10 times that of day 30.
//
// The result gates only the default of the L1 window; it is not a gate on a release.
//
// Each cadence drives one stream into two stores. The check store is written the
// stream as it arrives, late records included; the control store is written the
// same records after an hour's wait, in event-time order, so it holds no late record.
// Where the largest ratio of a day sits is reported with the control's work for the
// same read, as a proxy for whether it sits in a hole that late records left in the
// checkpoints. The report is information; only the two rules and the guards decide.

const (
	// l1gWindow is the number of days L1 keeps; l1gKeep the days L2 and L3 keep.
	l1gWindow = 90 * 24 * time.Hour
	l1gKeep   = 30 * 24 * time.Hour
	// l1gDuration is 2W + 1.5 days with W = 90.
	l1gDuration = 2*l1gWindow + 36*time.Hour
	// l1gCounter is the counter of G1's budget the check reads.
	l1gCounter = "internal_steps+checkpoint_entries_decoded+baseline_entries_decoded"
	// l1gHoleFactor is how many times the control's work the check's must be, at least,
	// for a read to count as in a hole.
	l1gHoleFactor = 1.6
	// l1gTimeCap is the longest the check may take.
	l1gTimeCap = 20 * time.Minute
	// l1gGrowthFactor is rule 2.
	l1gGrowthFactor = 1.10
	// l1gHostsPerRack keeps rmae's 400 hosts in 20 racks as the hosts are fewer.
	l1gHostsPerRack = 20

	// The environment variables a run can set. A run that sets l1gEnvDays is partial: it
	// stops early, and its report is information only.
	l1gEnvHosts = "TOPOSHIFT_L1_GROWTH_HOSTS"
	l1gEnvDays  = "TOPOSHIFT_L1_GROWTH_DAYS"
	l1gEnvSeed  = "TOPOSHIFT_L1_GROWTH_SEED"
)

// l1gHosts is the number of hosts of a run. The rest of the cluster is as small as
// the workload allows, so that the stream is mostly L1.
const l1gHosts = 20

// l1gCadence is how often a node's collector refreshes its node and its binding to
// its host, and how many intervals a refresh lives.
type l1gCadence struct {
	name     string
	interval time.Duration
	factor   int
}

var l1gCadences = []l1gCadence{
	// rmae's: the spec behind the 0.50 of the 30-day measurement.
	{"heartbeat60s", time.Minute, 4},
	// The product's: a refresh every five minutes with a TTL of fifteen.
	{"refresh5m", 5 * time.Minute, 3},
}

func (c l1gCadence) ttl() time.Duration { return time.Duration(c.factor) * c.interval }

// l1gAge is how far before an hour a read is made. A baseline age is the one that
// puts the read an hour above the L1 horizon, whatever the hour.
type l1gAge struct {
	label    string
	back     time.Duration
	baseline bool
}

var l1gAges = []l1gAge{
	{"now", 0, false},
	{"3h", 3 * time.Hour, false},
	{"9h", 9 * time.Hour, false},
	{"1d", 24 * time.Hour, false},
	{"7d", 7 * 24 * time.Hour, false},
	{"29d", 29 * 24 * time.Hour, false},
	{"31d", 31 * 24 * time.Hour, false},
	{"60d", 60 * 24 * time.Hour, false},
	{"89d", 89 * 24 * time.Hour, false},
	{"L1+1h", 0, true},
}

// The two shapes of prefix G1 reads in L1.
const (
	l1gHostIn  = iota // host<-: the in-edges of each host
	l1gNodeOut        // node->L1: each node's L1 out-edges
	l1gKinds
)

var l1gKindNames = [l1gKinds]string{"host<-", "node->L1"}

// l1gHours is the number of whole hours ending at a pause that are read.
const l1gHours = 24

// l1gPause is an instant the stream is stopped at to read the store.
type l1gPause struct {
	offset time.Duration
	label  string
}

func l1gDayLabel(d time.Duration) string {
	return strconv.FormatFloat(d.Hours()/24, 'f', -1, 64)
}

// l1gPauses are the measured days that fall before the end of a stream of the given
// length, in order, and then the end itself.
func l1gPauses(duration time.Duration) []l1gPause {
	var out []l1gPause
	for _, d := range []time.Duration{30 * 24 * time.Hour, l1gWindow + 36*time.Hour} {
		if d < duration {
			out = append(out, l1gPause{d, l1gDayLabel(d)})
		}
	}
	return append(out, l1gPause{duration, l1gDayLabel(duration)})
}

// l1gRetentions retains once a day from the first day after L2 and L3's window.
func l1gRetentions(duration time.Duration) []runner.Retention {
	var out []runner.Retention
	for n := 31; time.Duration(n)*24*time.Hour < duration && n <= 181; n++ {
		out = append(out, runner.Retention{At: time.Duration(n) * 24 * time.Hour, Keep: l1gKeep})
	}
	return out
}

// l1gWorkload is the rmae workload with the cluster made as small as the workload
// allows apart from its hosts: a pod, a service and no churn, so that the stream is
// the hosts' refreshes. Coalescing is the product coalescer's, in the sink.
func l1gWorkload(hosts int, c l1gCadence, duration time.Duration, seed uint64) workload.Config {
	return workload.Config{
		Seed: seed, Start: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Duration: duration, FirstSeq: 1,
		Racks: max(1, (hosts+l1gHostsPerRack-1)/l1gHostsPerRack), Hosts: hosts, Pods: 1, Services: 1,
		ContainersPerPod: 1, DependsPerService: 0,
		EventsPerSecond: 0, PodSkew: 1.15, NodeSkew: 1.1,
		HeartbeatInterval: c.interval, HeartbeatTTLFactor: c.factor,
		OutageProbability: 0.0005, OutageLength: 10 * time.Minute,
		BacklogEvery: 6 * time.Hour, BacklogMeanDelay: 2 * time.Minute, BacklogSpan: 15 * time.Minute,
		PayloadMin: 16, PayloadMax: 96,
		CoalesceRuns: false,
	}
}

// l1gRecorder keeps what the store reports: every count over the whole run, and
// while a read is being measured the sum of every count and sample by name. A count
// and a sample are summed alike, because the iterator statistics (internal_steps
// among them) are samples and the layout's own counters are counts.
type l1gRecorder struct {
	mu      sync.Mutex
	total   map[string]int64
	reading bool
	read    map[string]int64
}

func newL1gRecorder() *l1gRecorder {
	return &l1gRecorder{total: map[string]int64{}, read: map[string]int64{}}
}

func (r *l1gRecorder) Count(name string, n int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.total[name] += n
	if r.reading {
		r.read[name] += n
	}
}

func (r *l1gRecorder) Sample(name string, v int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.reading {
		r.read[name] += v
	}
}

// begin forgets what the last read reported and starts summing for the next.
func (r *l1gRecorder) begin() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.read)
	r.reading = true
}

// end stops summing and returns what the read reported.
func (r *l1gRecorder) end() map[string]int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.reading = false
	return l1gCloneCounts(r.read)
}

func (r *l1gRecorder) count(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.total[name]
}

func l1gCloneCounts(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// l1gWork is the value of a counter expression for one read, term by term, in the
// order the runner looks a term up in: "read.<op>.<term>", then "read.<term>", then
// "<term>". The runner's own function is not exported.
func l1gWork(rec map[string]int64, op, expr string) int64 {
	var n int64
	for term := range strings.SplitSeq(expr, "+") {
		switch {
		case l1gHas(rec, "read."+op+"."+term):
			n += rec["read."+op+"."+term]
		case l1gHas(rec, "read."+term):
			n += rec["read."+term]
		default:
			n += rec[term]
		}
	}
	return n
}

func l1gHas(m map[string]int64, k string) bool { _, ok := m[k]; return ok }

// l1gPercentile is the nearest-rank percentile of sorted values: the smallest value
// with at least p of the values at or below it.
func l1gPercentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return math.NaN()
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}

// l1gRead is one read of one prefix, with what it cost.
type l1gRead struct {
	age, kind, prefix, hour int
	// work is the counter of G1's budget; steps the iterator's internal steps alone and
	// baseline the baseline entries decoded, kept for the checks of the check itself.
	work, steps, baseline int64
	size                  int
	budget                float64
}

func (r l1gRead) ratio() float64 { return float64(r.work) / r.budget }

type l1gKey struct{ age, kind, prefix, hour int }

func (r l1gRead) key() l1gKey { return l1gKey{r.age, r.kind, r.prefix, r.hour} }

// l1gDay is what one store showed on one measured day.
type l1gDay struct {
	pause       l1gPause
	reads       []l1gRead
	notRead     map[string]int // by age: the reads that fall before the horizon or the start
	l1Bytes     int64
	invalidated int64
	keptBoots   int64
}

// l1gPrefixes are the prefixes the stream is read at, found in the stream itself:
// the hosts that a node runs on, and those nodes.
type l1gPrefixes struct {
	hosts, nodes map[identity.Fingerprint]struct{}
	fixed        [l1gKinds][]identity.Fingerprint
	frozen       bool
}

func newL1gPrefixes() *l1gPrefixes {
	return &l1gPrefixes{hosts: map[identity.Fingerprint]struct{}{}, nodes: map[identity.Fingerprint]struct{}{}}
}

func (p *l1gPrefixes) see(recs []engine.Record) {
	for _, r := range recs {
		if r.Subject.Kind == engine.SubjectEdge && r.Layer == catalog.L1 && r.Subject.Relation == catalog.RunsOn &&
			r.Subject.A.Type() == catalog.K8sNode && r.Subject.B.Type() == catalog.Host {
			p.nodes[r.Subject.A] = struct{}{}
			p.hosts[r.Subject.B] = struct{}{}
		}
	}
}

// list is the prefixes of a kind, in a fixed order. The first call fixes them: the
// workload's hosts and nodes all exist from its first instant.
func (p *l1gPrefixes) list(kind int) []identity.Fingerprint {
	if !p.frozen {
		for k, set := range [l1gKinds]map[identity.Fingerprint]struct{}{p.hosts, p.nodes} {
			for fp := range set {
				p.fixed[k] = append(p.fixed[k], fp)
			}
			slices.SortFunc(p.fixed[k], engine.CompareFingerprints)
		}
		p.frozen = true
	}
	return p.fixed[kind]
}

func (p *l1gPrefixes) name(kind, i int) string {
	fp := p.list(kind)[i]
	h := fp.Hash()
	return l1gKindNames[kind] + "/" + hex.EncodeToString(h[:3])
}

// l1gRun is what the two stores of a cadence share.
type l1gRun struct {
	start     time.Time
	duration  time.Duration
	cadence   l1gCadence
	pauses    []l1gPause // the pauses inside the stream, then its end
	retained  []runner.Retention
	prefixes  *l1gPrefixes
	blockSize int64
	budget    runner.Budget
	full      bool
}

// midPauses are the pauses the stream is stopped at; the last is its end.
func (r *l1gRun) midPauses() []l1gPause { return r.pauses[:len(r.pauses)-1] }

func (r *l1gRun) instant(p l1gPause) time.Time { return r.start.Add(p.offset) }

// retentionAt is the last retention that has happened by a pause.
func (r *l1gRun) retentionAt(p l1gPause) (runner.Retention, bool) {
	var last runner.Retention
	found := false
	for _, x := range r.retained {
		if x.At <= p.offset {
			last, found = x, true
		}
	}
	return last, found
}

// l1gStore is a store with the product coalescer in front of it, and what a pause
// reads of it.
type l1gStore struct {
	name string
	t    *testing.T
	ctx  context.Context
	run  *l1gRun
	st   *pebblestore.Store
	ins  *pebblestore.Instrument
	co   *coalesce.Coalescer
	rec  *l1gRecorder

	frontier, lastFlush time.Time
	out                 []store.Record
	written             uint64
	fresh               map[identity.Fingerprint]int // runs started per node's edge to its host, as written
	busy                time.Duration
	spent               struct{ write, retain, settle, read time.Duration }
	days                []*l1gDay
}

func (s *l1gStore) open(dir string) error {
	rec := newL1gRecorder()
	opts := pebblestore.DefaultOptions() // the product's policy, checkpoints and tuning
	opts.Recorder = rec
	opts.Retention = pebblestore.Synchronous
	// The product settles the database at the end of each daily retention, and waits at
	// least a second for it to be at rest; over 151 retentions of four stores that is
	// ten minutes of waiting. The check settles the database once at each pause instead
	// (see settle), which is what makes the counters of a pause repeatable.
	opts.SettleRetention = false
	opts.Keep[0] = true
	opts.Offsets[catalog.L1-catalog.L0] = l1gWindow - l1gKeep
	opts.Offsets[catalog.L2-catalog.L0] = 0
	opts.Offsets[catalog.L3-catalog.L0] = 0
	st, err := pebblestore.Open(dir, opts)
	if err != nil {
		return err
	}
	co, err := coalesce.New(coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true})
	if err != nil {
		_ = st.Close()
		return err
	}
	s.st, s.ins, s.co, s.rec = st, st.Instrument(), co, rec
	s.fresh = map[identity.Fingerprint]int{}
	return nil
}

func toL1gRecord(r engine.Record) store.Record {
	return store.Record{
		Layer:     r.Layer,
		Subject:   store.Subject{Kind: store.SubjectKind(r.Subject.Kind), A: r.Subject.A, B: r.Subject.B, Relation: r.Subject.Relation},
		Producer:  r.Producer,
		EventTime: r.EventTime,
		Seq:       r.Seq,
		Kind:      r.Kind,
		TTL:       r.TTL,
		Through:   r.Through,
		Payload:   r.Payload,
		// Boot is empty and the basis of the event time unknown: the workload's records
		// carry neither.
	}
}

// ingest passes records through the coalescer in arrival order, flushes it once for
// every FlushPeriod of stream time, and writes what comes out as one batch with the
// next sequence numbers.
func (s *l1gStore) ingest(recs []engine.Record) error {
	start := time.Now()
	defer func() { s.busy += time.Since(start) }()
	out := s.out[:0]
	for _, r := range recs {
		out = s.co.Add(toL1gRecord(r), out)
		if r.EventTime.After(s.frontier) {
			s.frontier = r.EventTime
		}
		if s.frontier.Sub(s.lastFlush) >= coalesce.FlushPeriod {
			out = s.co.Flush(s.frontier, out)
			s.lastFlush = s.frontier
		}
	}
	return s.write(out)
}

// flushAll is the graceful stop: everything the coalescer has absorbed is written.
func (s *l1gStore) flushAll() error {
	start := time.Now()
	defer func() { s.busy += time.Since(start) }()
	return s.write(s.co.FlushAll(s.out[:0]))
}

func (s *l1gStore) write(out []store.Record) error {
	s.out = out[:0]
	if len(out) == 0 {
		return nil
	}
	base := s.st.LastSeq()
	for i := range out {
		out[i].Seq = base + 1 + uint64(i)
		if r := out[i]; r.Layer == catalog.L1 && r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.RunsOn &&
			r.Kind == lifecycle.Observe && r.Through.IsZero() {
			s.fresh[r.Subject.A]++
		}
	}
	began := time.Now()
	err := s.st.Write(s.ctx, out)
	s.spent.write += time.Since(began)
	if err != nil {
		return fmt.Errorf("%s store: writing %d records above seq %d: %w", s.name, len(out), base, err)
	}
	s.written += uint64(len(out))
	return nil
}

// retain is what the product does when the horizon moves: the store retains, and then
// the coalescer is told each layer's horizon. L0 is kept and is not told.
func (s *l1gStore) retain(h time.Time) error {
	start := time.Now()
	defer func() { s.busy += time.Since(start) }()
	began := time.Now()
	err := s.st.Retain(s.ctx, h)
	s.spent.retain += time.Since(began)
	if err != nil {
		return fmt.Errorf("%s store: retaining before %s: %w", s.name, h.Format(time.RFC3339), err)
	}
	s.co.SetHorizon(catalog.L1, h.Add(-(l1gWindow - l1gKeep)))
	s.co.SetHorizon(catalog.L2, h)
	s.co.SetHorizon(catalog.L3, h)
	return nil
}

// settle brings the database to rest, so that the shape of its tables is the same
// whenever the reads of a pause are made.
func (s *l1gStore) settle() error {
	if err := s.ins.WaitRetained(s.ctx); err != nil {
		return err
	}
	if err := s.ins.WaitScanned(s.ctx); err != nil {
		return err
	}
	_, _, _, err := s.ins.KV().SettleAfterRetention(0)
	return err
}

// read asks for the neighbors of a prefix at the instant at, which must be h - back,
// as of the newest snapshot, and returns what it cost.
func (s *l1gStore) read(kind int, fp identity.Fingerprint, h time.Time, back time.Duration, at time.Time) (l1gRead, error) {
	if !at.Equal(h.Add(-back)) {
		return l1gRead{}, fmt.Errorf("a read of %s at %s was asked for the hour %s and the age %s, which is %s", fp, at.Format(time.RFC3339), h.Format(time.RFC3339), back, h.Add(-back).Format(time.RFC3339))
	}
	dir := store.Reverse
	if kind == l1gNodeOut {
		dir = store.Forward
	}
	s.rec.begin()
	ns, err := s.st.Neighbors(s.ctx, fp, dir, at, store.Scope{Layer: catalog.L1, AsOf: store.Latest})
	rec := s.rec.end()
	if err != nil {
		return l1gRead{}, fmt.Errorf("%s store: neighbors of %s at %s: %w", s.name, fp, at.Format(time.RFC3339), err)
	}
	r := l1gRead{
		work:     l1gWork(rec, "neighbors", l1gCounter),
		steps:    l1gWork(rec, "neighbors", "internal_steps"),
		baseline: l1gWork(rec, "neighbors", "baseline_entries_decoded"),
		size:     len(ns),
	}
	r.budget = s.run.budget.For(1, len(ns), s.run.blockSize)
	return r, nil
}

// measure stops at a pause and reads: it settles the store, checks the horizons,
// and reads every prefix at every hour and age.
func (s *l1gStore) measure(p l1gPause) error {
	start := time.Now()
	defer func() { s.busy += time.Since(start) }()
	t, run := s.t, s.run
	began := time.Now()
	err := s.settle()
	s.spent.settle += time.Since(began)
	if err != nil {
		return fmt.Errorf("%s store: settling at day %s: %w", s.name, p.label, err)
	}
	s.checkHorizons(p)

	day := &l1gDay{pause: p, notRead: map[string]int{}}
	l1h := s.st.LayerHorizon(catalog.L1).Time
	at := run.instant(p).Truncate(time.Hour)
	for ai, age := range l1gAges {
		for kind := range l1gKinds {
			for pi, fp := range run.prefixes.list(kind) {
				for hi := range l1gHours {
					h := at.Add(-time.Duration(l1gHours-1-hi) * time.Hour)
					back := age.back
					if age.baseline {
						if l1h.IsZero() {
							day.notRead[age.label]++
							continue
						}
						back = h.Sub(l1h.Add(time.Hour))
					}
					instant := h.Add(-back)
					if back < 0 || instant.Before(run.start) || instant.Before(l1h) {
						day.notRead[age.label]++
						continue
					}
					began := time.Now()
					r, err := s.read(kind, fp, h, back, instant)
					s.spent.read += time.Since(began)
					if err != nil {
						return err
					}
					r.age, r.kind, r.prefix, r.hour = ai, kind, pi, hi
					day.reads = append(day.reads, r)
				}
			}
		}
	}
	sizes, err := s.ins.SizeByLayer()
	if err != nil {
		return err
	}
	day.l1Bytes = sizes[catalog.L1]
	day.invalidated = s.rec.count("checkpoint.invalidated")
	day.keptBoots = s.rec.count("retain.prefixes_kept_for_boots")
	s.days = append(s.days, day)

	// Guards on the reads themselves: the check must see the iterator's steps on every
	// read, or it counts nothing.
	for _, r := range day.reads {
		if r.steps <= 0 {
			t.Errorf("%s store, day %s: the read of %s at age %s, hour %d, reported no internal steps: the work is not being counted", s.name, p.label, run.prefixes.name(r.kind, r.prefix), l1gAges[r.age].label, r.hour)
			break
		}
	}
	return nil
}

// checkHorizons asserts that the layers' horizons are where the retentions put them:
// L0 never moves, L1 trails the retention by its window, L2 and L3 by their own.
func (s *l1gStore) checkHorizons(p l1gPause) {
	t, run := s.t, s.run
	t.Helper()
	if h := s.st.LayerHorizon(catalog.L0); !h.IsZero() {
		t.Errorf("%s store, day %s: L0's horizon moved to %s, and L0 is kept", s.name, p.label, h.Time.Format(time.RFC3339))
	}
	last, retained := run.retentionAt(p)
	want := map[catalog.Layer]time.Time{}
	if retained {
		h := last.Horizon(run.start)
		want[catalog.L1] = h.Add(-(l1gWindow - l1gKeep))
		want[catalog.L2], want[catalog.L3] = h, h
	}
	for _, l := range []catalog.Layer{catalog.L1, catalog.L2, catalog.L3} {
		got := s.st.LayerHorizon(l).Time
		switch w, ok := want[l]; {
		case !ok && !got.IsZero():
			t.Errorf("%s store, day %s: %s's horizon is %s before any retention", s.name, p.label, l, got.Format(time.RFC3339))
		case ok && !got.Equal(w):
			t.Errorf("%s store, day %s: %s's horizon is %s, want %s (the retention at day %s)", s.name, p.label, l, got.Format(time.RFC3339), w.Format(time.RFC3339), l1gDayLabel(last.At))
		}
	}
}

func (s *l1gStore) close() {
	if s.st != nil {
		_ = s.st.Close()
	}
}

// l1gSplit is the index of the first record whose event time is after at: the batch
// is written up to it, the store is read, and the rest is written after.
func l1gSplit(batch []engine.Record, at time.Time) int {
	for i, r := range batch {
		if r.EventTime.After(at) {
			return i
		}
	}
	return len(batch)
}

// l1gSink is a runner.Sink over a store. The check sink writes what it is given; the
// control sink holds records back for an hour of event time and releases them in
// event-time order.
type l1gSink struct {
	s       *l1gStore
	control bool
	next    int // the next pause inside the stream
	held    []engine.Record
	newest  time.Time
}

func (k *l1gSink) Write(batch []engine.Record) error {
	run := k.s.run
	if !k.control {
		run.prefixes.see(batch)
	}
	for len(batch) > 0 {
		mid := run.midPauses()
		if k.next < len(mid) {
			p := mid[k.next]
			if i := l1gSplit(batch, run.instant(p)); i < len(batch) {
				if err := k.accept(batch[:i]); err != nil {
					return err
				}
				if err := k.pause(p); err != nil {
					return err
				}
				k.next++
				batch = batch[i:]
				continue
			}
		}
		return k.accept(batch)
	}
	return nil
}

// accept writes the records, or for the control holds them and writes those an hour
// behind the newest.
func (k *l1gSink) accept(recs []engine.Record) error {
	if !k.control {
		return k.s.ingest(recs)
	}
	k.held = append(k.held, recs...)
	for _, r := range recs {
		if r.EventTime.After(k.newest) {
			k.newest = r.EventTime
		}
	}
	return k.release(k.newest.Add(-time.Hour))
}

// release writes the held records with an event time at or before limit, in event-time
// order (a stable sort, so equal times keep their arrival order).
func (k *l1gSink) release(limit time.Time) error {
	var out []engine.Record
	k.held, out = l1gRelease(k.held, limit)
	return k.s.ingest(out)
}

// l1gRelease splits held into the records with an event time at or before limit, in
// event-time order, and the rest.
func l1gRelease(held []engine.Record, limit time.Time) (rest, out []engine.Record) {
	if len(held) == 0 {
		return held, nil
	}
	sort.SliceStable(held, func(i, j int) bool { return held[i].EventTime.Before(held[j].EventTime) })
	n := sort.Search(len(held), func(i int) bool { return held[i].EventTime.After(limit) })
	out = slices.Clone(held[:n])
	rest = append(held[:0], held[n:]...)
	return rest, out
}

func (k *l1gSink) Retain(h time.Time) error { return k.s.retain(h) }

// pause is a read of the store as written up to it. The control first writes
// everything it holds, or it would lack the last hour of the check's stream.
func (k *l1gSink) pause(p l1gPause) error {
	if k.control {
		if err := k.release(k.newest); err != nil {
			return err
		}
	}
	return k.s.measure(p)
}

// finish reads at the end of the stream, and then stops gracefully, writing what the
// coalescer has absorbed.
func (k *l1gSink) finish() error {
	run := k.s.run
	if err := k.pause(run.pauses[len(run.pauses)-1]); err != nil {
		return err
	}
	return k.s.flushAll()
}

// l1gResults are the two stores' days of one cadence.
type l1gResults struct {
	cadence l1gCadence
	run     *l1gRun
	check   *l1gStore
	control *l1gStore
	elapsed time.Duration
	partial bool
	seed    uint64
}

func (r *l1gResults) ctrlWork(day int) map[l1gKey]int64 {
	out := map[l1gKey]int64{}
	for _, x := range r.control.days[day].reads {
		out[x.key()] = x.work
	}
	return out
}

// l1gRow is a group of reads summarized.
type l1gRow struct {
	n             int
	p50, p90, max float64
	maxRead       l1gRead
}

func l1gSummarize(reads []l1gRead) l1gRow {
	var row l1gRow
	row.n = len(reads)
	if row.n == 0 {
		return row
	}
	ratios := make([]float64, len(reads))
	for i, r := range reads {
		ratios[i] = r.ratio()
	}
	sorted := slices.Clone(ratios)
	slices.Sort(sorted)
	row.p50, row.p90, row.max = l1gPercentile(sorted, 0.5), l1gPercentile(sorted, 0.9), sorted[len(sorted)-1]
	// The largest read; among equal ratios the first in the fixed order of the reads.
	best := 0
	for i := range reads {
		if ratios[i] > ratios[best] {
			best = i
		}
	}
	row.maxRead = reads[best]
	return row
}

// inHole is the proxy for a read sitting in a hole that late records left in the
// checkpoints: the check store's work is at least l1gHoleFactor times the control's,
// and late records have invalidated checkpoints in the check store.
func l1gInHole(work, ctrl, invalidated int64) bool {
	return float64(work) >= l1gHoleFactor*float64(ctrl) && invalidated > 0
}

func l1gYesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// verdict applies the pass rule, which is fixed: every ratio of every measured day
// is at most 1, and the 90th percentile of the last day is at most l1gGrowthFactor
// times that of day 30. It returns the failures, each naming the rule, the cadence, the
// day and the five largest reads.
func (r *l1gResults) verdict() (rule1, rule2 bool, failures []string) {
	rule1, rule2 = true, true
	for di, day := range r.check.days {
		for _, x := range day.reads {
			if x.ratio() > 1 {
				rule1 = false
				failures = append(failures, r.fail(fmt.Sprintf("rule 1 (every ratio is at most 1) failed on %s, day %s", r.cadence.name, day.pause.label), di))
				break
			}
		}
	}
	first, last := r.check.days[0], r.check.days[len(r.check.days)-1]
	if len(r.check.days) > 1 && first.pause.offset == 30*24*time.Hour {
		p90 := func(d *l1gDay) float64 { return l1gSummarize(d.reads).p90 }
		if p90(last) > l1gGrowthFactor*p90(first) {
			rule2 = false
			failures = append(failures, r.fail(fmt.Sprintf("rule 2 (p90 of day %s at most %.2f x that of day %s) failed on %s: %.4f against %.4f", last.pause.label, l1gGrowthFactor, first.pause.label, r.cadence.name, p90(last), l1gGrowthFactor*p90(first)), len(r.check.days)-1))
		}
	}
	return rule1, rule2, failures
}

// fail is a failure message with the five largest reads of a day.
func (r *l1gResults) fail(head string, day int) string {
	var b strings.Builder
	b.WriteString(head)
	b.WriteString("\nthe five largest reads (prefix, age, hour, work, budget, ratio, in a hole):\n")
	reads := slices.Clone(r.check.days[day].reads)
	sort.SliceStable(reads, func(i, j int) bool { return reads[i].ratio() > reads[j].ratio() })
	ctrl := r.ctrlWork(day)
	for _, x := range reads[:min(5, len(reads))] {
		fmt.Fprintf(&b, "  %s, %s, hour %d, work %d, budget %.0f, ratio %.3f, in a hole: %s\n", r.run.prefixes.name(x.kind, x.prefix), l1gAges[x.age].label, x.hour, x.work, x.budget, x.ratio(), l1gYesNo(l1gInHole(x.work, ctrl[x.key()], r.check.days[day].invalidated)))
	}
	return b.String()
}

// table is the report of a cadence: one row per measured day and age, then the rules.
func (r *l1gResults) table() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%-13s %-6s %-8s %6s %6s %6s %6s  %-26s %-8s %-8s %9s %6s\n", "cadence", "day", "age", "reads", "p50", "p90", "max", "max-prefix", "max-hour", "in-hole", "ctrl-work", "work")
	for di, day := range r.check.days {
		ctrl := r.ctrlWork(di)
		emit := func(label string, reads []l1gRead) {
			if len(reads) == 0 {
				return
			}
			row := l1gSummarize(reads)
			m := row.maxRead
			at := r.run.instant(day.pause).Truncate(time.Hour).Add(-time.Duration(l1gHours-1-m.hour) * time.Hour)
			fmt.Fprintf(&b, "%-13s %-6s %-8s %6d %6.2f %6.2f %6.2f  %-26s %-8d %-8s %9d %6d\n", r.cadence.name, day.pause.label, label, row.n, row.p50, row.p90, row.max, r.run.prefixes.name(m.kind, m.prefix), at.Hour(), l1gYesNo(l1gInHole(m.work, ctrl[m.key()], day.invalidated)), ctrl[m.key()], m.work)
		}
		for ai, age := range l1gAges {
			var reads []l1gRead
			for _, x := range day.reads {
				if x.age == ai {
					reads = append(reads, x)
				}
			}
			emit(age.label, reads)
		}
		emit("all", day.reads)
		var skipped []string
		for _, age := range l1gAges {
			if n := day.notRead[age.label]; n > 0 {
				skipped = append(skipped, fmt.Sprintf("%s x%d", age.label, n))
			}
		}
		if len(skipped) > 0 {
			fmt.Fprintf(&b, "  day %s: not read, before the horizon or the start of the stream: %s\n", day.pause.label, strings.Join(skipped, ", "))
		}
	}
	rule1, rule2, _ := r.verdict()
	first, last := r.check.days[0], r.check.days[len(r.check.days)-1]
	p90First, p90Last := l1gSummarize(first.reads).p90, l1gSummarize(last.reads).p90
	var sameAges []l1gRead
	inFirst := map[int]bool{}
	for _, x := range first.reads {
		inFirst[x.age] = true
	}
	for _, x := range last.reads {
		if inFirst[x.age] {
			sameAges = append(sameAges, x)
		}
	}
	fmt.Fprintf(&b, "rule 1: %s   rule 2: p90 %s = %.2f <= %.2f x %.2f = %.2f: %s   (day %s on day %s's ages: p90 %.2f)\n",
		l1gYesNo(rule1), last.pause.label, p90Last, l1gGrowthFactor, p90First, l1gGrowthFactor*p90First, l1gYesNo(rule2), last.pause.label, first.pause.label, l1gSummarize(sameAges).p90)
	var l1b, inv []string
	for i := range r.check.days {
		l1b = append(l1b, fmt.Sprintf("%s: %.1f MB (control %.1f)", r.check.days[i].pause.label, float64(r.check.days[i].l1Bytes)/1e6, float64(r.control.days[i].l1Bytes)/1e6))
		inv = append(inv, fmt.Sprintf("%s: %d/%d", r.check.days[i].pause.label, r.check.days[i].invalidated, r.control.days[i].invalidated))
	}
	fmt.Fprintf(&b, "kept_for_boots=%d (no boots in the workload, so the boot-baseline known issue is not exercised)  invalidated check/control by day: %s\n", last.keptBoots, strings.Join(inv, ", "))
	fmt.Fprintf(&b, "L1 bytes by day: %s\n", strings.Join(l1b, ", "))
	maxIn := 0
	for _, day := range r.check.days {
		for _, x := range day.reads {
			if x.kind == l1gHostIn {
				maxIn = max(maxIn, x.size)
			}
		}
	}
	fmt.Fprintf(&b, "largest host<- degree D = %d  seed %d  records written check/control %d/%d  run time check %s, control %s, together %s\n", maxIn, r.seed, r.check.written, r.control.written, r.check.busy.Round(time.Second), r.control.busy.Round(time.Second), r.elapsed.Round(time.Second))
	for _, s := range []*l1gStore{r.check, r.control} {
		fmt.Fprintf(&b, "%s store: write %s, retain %s, settle %s, read %s\n", s.name, s.spent.write.Round(time.Millisecond), s.spent.retain.Round(time.Millisecond), s.spent.settle.Round(time.Millisecond), s.spent.read.Round(time.Millisecond))
	}
	return b.String()
}

// digest covers every read of both stores in a fixed order: the values the check
// computes, so that two runs can be compared.
func (r *l1gResults) digest() string {
	h := sha256.New()
	for di, day := range r.check.days {
		ctrl := r.ctrlWork(di)
		for _, x := range day.reads {
			fmt.Fprintf(h, "%s|%s|%d|%d|%d|%d|%d|%d|%d|%.6f|%d\n", r.cadence.name, day.pause.label, x.age, x.kind, x.prefix, x.hour, x.work, x.steps, x.size, x.ratio(), ctrl[x.key()])
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}

// guards are the checks of the check itself that need the whole run.
func (r *l1gResults) guards(t *testing.T) {
	t.Helper()
	last := r.check.days[len(r.check.days)-1]
	if r.run.full {
		// The work must depend on the age: at the newest and the oldest age it cannot be
		// the same in every read, or the reads are not made where they are said to be.
		now, old := map[l1gKey]int64{}, map[l1gKey]int64{}
		ni, oi := l1gAgeIndex("now"), l1gAgeIndex("89d")
		for _, x := range last.reads {
			k := l1gKey{0, x.kind, x.prefix, x.hour}
			switch x.age {
			case ni:
				now[k] = x.work
			case oi:
				old[k] = x.work
			}
		}
		differ := false
		for k, w := range now {
			if o, ok := old[k]; ok && o != w {
				differ = true
			}
		}
		if !differ {
			t.Errorf("%s: on day %s the work at age now and at age 89d is the same in every read: the age does not reach the read", r.cadence.name, last.pause.label)
		}
		// The work must be the sum of the terms: somewhere it is more than the steps alone.
		more := false
		for _, x := range last.reads {
			if x.work > x.steps {
				more = true
			}
		}
		if !more {
			t.Errorf("%s: on day %s no read decoded a checkpoint or a baseline entry: the work is the steps alone", r.cadence.name, last.pause.label)
		}
		// A read just above the L1 baseline must find the baseline.
		found, read := false, false
		bi := l1gAgeIndex("L1+1h")
		for _, x := range last.reads {
			if x.age == bi {
				read = true
				if x.baseline > 0 {
					found = true
				}
			}
		}
		switch {
		case !read:
			t.Errorf("%s: on day %s no read was made just above the L1 baseline", r.cadence.name, last.pause.label)
		case !found:
			t.Errorf("%s: on day %s no read just above the L1 baseline decoded a baseline entry: L1 has no baseline there", r.cadence.name, last.pause.label)
		}
	}
	// A host at a constant description has a run started at least once for every
	// extension interval and refresh: the coalescer is the product's, with the run's age
	// bounded by its extension interval.
	ttl := r.cadence.ttl()
	floor := int(r.run.duration / (ttl/2 + r.cadence.interval))
	for fp, n := range r.check.fresh {
		if n < floor {
			t.Errorf("%s: the edge of node %s to its host started %d runs in %s, below the %d the product's coalescing gives", r.cadence.name, fp, n, r.run.duration, floor)
			break
		}
	}
	if len(r.check.fresh) != len(r.run.prefixes.list(l1gNodeOut)) {
		t.Errorf("%s: %d nodes had a run of their edge written, and the stream has %d", r.cadence.name, len(r.check.fresh), len(r.run.prefixes.list(l1gNodeOut)))
	}
}

func l1gAgeIndex(label string) int {
	for i, a := range l1gAges {
		if a.label == label {
			return i
		}
	}
	return -1
}

// l1gSettings are the knobs of a run: the environment can make it smaller, never
// longer, and never changes the rules.
type l1gSettings struct {
	hosts    int
	duration time.Duration
	seed     uint64
}

func l1gSettingsFromEnv(t *testing.T) l1gSettings {
	t.Helper()
	s := l1gSettings{hosts: l1gHosts, duration: l1gDuration, seed: 1}
	if v := os.Getenv(l1gEnvHosts); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			t.Fatalf("%s=%q is not a number of hosts", l1gEnvHosts, v)
		}
		s.hosts = n
	}
	if v := os.Getenv(l1gEnvDays); v != "" {
		d, err := strconv.ParseFloat(v, 64)
		if err != nil || d < 1 || d > l1gDuration.Hours()/24 {
			t.Fatalf("%s=%q is not a number of days from 1 to 181.5", l1gEnvDays, v)
		}
		s.duration = time.Duration(d * 24 * float64(time.Hour)).Round(time.Second)
	}
	if v := os.Getenv(l1gEnvSeed); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil || n == 0 {
			t.Fatalf("%s=%q is not a seed", l1gEnvSeed, v)
		}
		s.seed = n
	}
	return s
}

// A run that is shorter than the stream, or has another seed, is information only:
// the verdict is the full stream's with seed 1.
func (s l1gSettings) decides() bool { return s.duration == l1gDuration && s.seed == 1 }

func TestL1GrowthNinetyDays(t *testing.T) {
	if testing.Short() {
		t.Skip("the L1 growth check simulates 181.5 days, and runs in the full tier")
	}
	if raceTag {
		t.Skip("the L1 growth check counts reads and does not start goroutines of its own: it runs without the race detector")
	}
	set := l1gSettingsFromEnv(t)
	began := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), l1gTimeCap)
	defer cancel()
	var digests []string
	for _, c := range l1gCadences {
		t.Run(c.name, func(t *testing.T) {
			res := l1gRunCadence(ctx, t, c, set)
			if res == nil {
				return
			}
			digests = append(digests, c.name+" "+res.digest())
		})
	}
	if time.Since(began) > l1gTimeCap {
		t.Errorf("over the L1 growth check's time cap of %s: it took %s", l1gTimeCap, time.Since(began).Round(time.Second))
	}
	if len(digests) > 0 {
		t.Logf("digest of the table of reads:\n%s", strings.Join(digests, "\n"))
	}
}

// l1gRunCadence drives one stream into the check and control stores and judges it.
func l1gRunCadence(ctx context.Context, t *testing.T, c l1gCadence, set l1gSettings) *l1gResults {
	t.Helper()
	began := time.Now()
	wl := l1gWorkload(set.hosts, c, set.duration, set.seed)
	spec := runner.DefaultSpec(wl)
	spec.Retentions = l1gRetentions(set.duration)
	// About fifty records of each host to a batch, so that a batch spans an hour or
	// two of stream however many hosts there are, and a retention falls between two.
	spec.BatchSize = min(spec.BatchSize, 50*set.hosts)
	run := &l1gRun{
		start: wl.Start, duration: set.duration, cadence: c,
		pauses: l1gPauses(set.duration), retained: l1gRetentions(set.duration), prefixes: newL1gPrefixes(),
		blockSize: int64(pebblestore.DefaultOptions().Tuning.BlockSize),
		budget:    runner.DefaultRules().G1Budget[l1gCounter],
		full:      set.duration == l1gDuration,
	}
	if run.budget.Base == 0 {
		t.Fatalf("the rules have no budget for %s", l1gCounter)
	}
	open := func(name string) *l1gStore {
		s := &l1gStore{name: name, t: t, ctx: ctx, run: run}
		if err := s.open(t.TempDir()); err != nil {
			t.Fatalf("opening the %s store: %v", name, err)
		}
		t.Cleanup(s.close)
		return s
	}
	check, control := open("check"), open("control")
	chk, ctl := &l1gSink{s: check}, &l1gSink{s: control, control: true}

	info, err := runner.Drive(ctx, spec, chk, ctl)
	if err == nil {
		err = chk.finish()
	}
	if err == nil {
		err = ctl.finish()
	}
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("over the L1 growth check's time cap of %s: %v", l1gTimeCap, err)
		}
		t.Fatal(err)
	}
	if len(info.Retentions) != len(run.retained) {
		t.Fatalf("the stream retained %d times, the schedule has %d", len(info.Retentions), len(run.retained))
	}
	res := &l1gResults{cadence: c, run: run, check: check, control: control, elapsed: time.Since(began), seed: set.seed, partial: !set.decides()}
	if len(check.days) != len(run.pauses) || len(control.days) != len(run.pauses) {
		t.Fatalf("%d and %d measured days, want %d", len(check.days), len(control.days), len(run.pauses))
	}
	res.guards(t)
	t.Logf("%s: %d records generated, %d written to the check store and %d to the control\n%s", c.name, info.Records, check.written, control.written, res.table())
	if _, _, failures := res.verdict(); len(failures) > 0 {
		if res.partial {
			t.Logf("this run is not the full stream with seed 1, so the rules below are information only:\n%s", strings.Join(failures, "\n"))
		} else {
			t.Error(strings.Join(failures, "\n"))
		}
	}
	return res
}

// ---- the helpers of the check, tested on their own ----

func TestL1GrowthRecorderSumsCountsAndSamples(t *testing.T) {
	t.Parallel()
	r := newL1gRecorder()
	r.Count("read.checkpoint_entries_decoded", 7) // outside a read: counted in total only
	r.begin()
	r.Count("read.checkpoint_entries_decoded", 3)
	r.Sample("read.neighbors.internal_steps", 10)
	r.Sample("read.neighbors.internal_steps", 5)
	got := r.end()
	if got["read.checkpoint_entries_decoded"] != 3 || got["read.neighbors.internal_steps"] != 15 {
		t.Errorf("a read reported %v", got)
	}
	r.Sample("read.neighbors.internal_steps", 99) // after the read: forgotten
	r.begin()
	if got := r.end(); len(got) != 0 {
		t.Errorf("a read reported %v after the previous one was reset", got)
	}
	if r.count("read.checkpoint_entries_decoded") != 10 || r.count("read.neighbors.internal_steps") != 0 {
		t.Errorf("the totals are %v", r.total)
	}
}

func TestL1GrowthWorkLooksTermsUpAsTheRunnerDoes(t *testing.T) {
	t.Parallel()
	rec := map[string]int64{
		"read.neighbors.internal_steps":   100,
		"read.internal_steps":             1000, // shadowed by the op's own
		"read.checkpoint_entries_decoded": 20,
		"baseline_entries_decoded":        3, // found only without a prefix
	}
	if got := l1gWork(rec, "neighbors", l1gCounter); got != 123 {
		t.Errorf("work = %d, want 123", got)
	}
	if got := l1gWork(rec, "neighbors", "internal_steps"); got != 100 {
		t.Errorf("steps = %d, want 100", got)
	}
	if got := l1gWork(map[string]int64{}, "neighbors", l1gCounter); got != 0 {
		t.Errorf("work of nothing = %d", got)
	}
}

func TestL1GrowthPercentileIsNearestRank(t *testing.T) {
	t.Parallel()
	xs := []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	for _, c := range []struct{ p, want float64 }{{0.5, 5}, {0.9, 9}, {1, 10}, {0.01, 1}} {
		if got := l1gPercentile(xs, c.p); got != c.want {
			t.Errorf("p%g = %g, want %g", c.p*100, got, c.want)
		}
	}
	if !math.IsNaN(l1gPercentile(nil, 0.9)) {
		t.Error("the percentile of nothing is not NaN")
	}
}

func TestL1GrowthControlReleasesInEventOrder(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mk := func(sec int, seq uint64) engine.Record {
		return engine.Record{Seq: seq, EventTime: at.Add(time.Duration(sec) * time.Second)}
	}
	held := []engine.Record{mk(30, 1), mk(10, 2), mk(10, 3), mk(50, 4), mk(20, 5)}
	rest, out := l1gRelease(held, at.Add(30*time.Second))
	var seqs []uint64
	for _, r := range out {
		seqs = append(seqs, r.Seq)
	}
	if !slices.Equal(seqs, []uint64{2, 3, 5, 1}) { // stable: seq 2 before 3 at the same time
		t.Errorf("released %v, want [2 3 5 1]", seqs)
	}
	if len(rest) != 1 || rest[0].Seq != 4 {
		t.Errorf("held back %v, want seq 4", rest)
	}
	if _, out := l1gRelease(rest, at); len(out) != 0 {
		t.Errorf("released %d records before their time", len(out))
	}
}

func TestL1GrowthSplitsABatchAtThePause(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 1, 31, 0, 0, 0, 0, time.UTC)
	batch := []engine.Record{{EventTime: at.Add(-time.Second)}, {EventTime: at}, {EventTime: at.Add(time.Second)}, {EventTime: at.Add(-time.Hour)}}
	if i := l1gSplit(batch, at); i != 2 {
		t.Errorf("split at %d, want 2: the record at the pause is written before it, the first after it is not", i)
	}
	if i := l1gSplit(batch[:2], at); i != 2 {
		t.Errorf("a batch that does not cross the pause is split at %d, want its length", i)
	}
}

func TestL1GrowthPausesAndRetentions(t *testing.T) {
	t.Parallel()
	var labels []string
	for _, p := range l1gPauses(l1gDuration) {
		labels = append(labels, p.label)
	}
	if !slices.Equal(labels, []string{"30", "91.5", "181.5"}) {
		t.Errorf("pauses %v", labels)
	}
	if got := l1gPauses(10 * 24 * time.Hour); len(got) != 1 || got[0].label != "10" {
		t.Errorf("pauses of a stream of 10 days: %v", got)
	}
	rs := l1gRetentions(l1gDuration)
	if len(rs) != 151 || rs[0].At != 31*24*time.Hour || rs[len(rs)-1].At != 181*24*time.Hour {
		t.Errorf("%d retentions from %s to %s, want 151 from day 31 to 181", len(rs), rs[0].At, rs[len(rs)-1].At)
	}
	if got := l1gRetentions(30 * 24 * time.Hour); len(got) != 0 {
		t.Errorf("a stream of 30 days retains %d times", len(got))
	}
}

func TestL1GrowthInHoleNeedsBothConditions(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		work, ctrl, inv int64
		want            bool
	}{{160, 100, 1, true}, {159, 100, 1, false}, {160, 100, 0, false}, {10, 0, 3, true}} {
		if got := l1gInHole(c.work, c.ctrl, c.inv); got != c.want {
			t.Errorf("l1gInHole(%d, %d, %d) = %v, want %v", c.work, c.ctrl, c.inv, got, c.want)
		}
	}
}

// The workload with the cluster at its smallest is accepted, and its stream is the hosts'
// refreshes: the shape of prefixes the check reads is found in it.
func TestL1GrowthWorkloadIsAcceptedAndHasTheShapes(t *testing.T) {
	t.Parallel()
	for _, c := range l1gCadences {
		wl := l1gWorkload(3, c, 2*time.Hour, 1)
		g, err := workload.New(wl)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		p := newL1gPrefixes()
		var bootless, l1 int
		for {
			batch := g.Batch(500)
			if len(batch) == 0 {
				break
			}
			p.see(batch)
			for _, r := range batch {
				if r.Layer == catalog.L1 {
					l1++
				}
				bootless++
			}
		}
		if len(p.list(l1gHostIn)) != 3 || len(p.list(l1gNodeOut)) != 3 {
			t.Errorf("%s: %d hosts and %d nodes found, want 3 and 3", c.name, len(p.list(l1gHostIn)), len(p.list(l1gNodeOut)))
		}
		if l1*3 < bootless {
			t.Errorf("%s: only %d of %d records are in L1", c.name, l1, bootless)
		}
	}
}
