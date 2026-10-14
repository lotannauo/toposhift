package nodeagent_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// The pipeline of these tests is the product's, less the sequencer: a translator,
// the coalescer in the sequencer's setting, and a store, with the flush tick every
// 30 seconds of the ingest clock, which is the time a record is received: a second
// after the time it carries.

// bootPolicy tells the boots of a host apart. The skew is a minute, as clocks of
// two node collectors differ.
var bootPolicy = lifecycle.Policy{BootKey: lifecycle.BootID, Skew: time.Minute}

var coalescerConfig = coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}

func newMem(t testing.TB) *memstore.Store {
	t.Helper()
	s, err := memstore.Open(memstore.Options{Policy: bootPolicy})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

type pipe struct {
	t    testing.TB
	opts []func(*nodeagent.Config)
	tr   *nodeagent.Translator
	co   *coalesce.Coalescer
	st   store.Store

	next    time.Time // the next flush tick; zero before the first record
	written []store.Record
	results []nodeagent.Result
}

func newPipe(t testing.TB, st store.Store, opts ...func(*nodeagent.Config)) *pipe {
	t.Helper()
	p := &pipe{t: t, st: st, opts: opts}
	p.reset()
	return p
}

// reset makes the translator and the coalescer anew, as a restart does.
func (p *pipe) reset() {
	p.t.Helper()
	p.tr = newTranslator(p.t, p.opts...)
	co, err := coalesce.New(coalescerConfig)
	if err != nil {
		p.t.Fatal(err)
	}
	p.co = co
}

func (p *pipe) write(recs []store.Record) {
	p.t.Helper()
	if len(recs) == 0 {
		return
	}
	seq := p.st.LastSeq()
	for i := range recs {
		recs[i].Seq = seq + 1 + uint64(i)
	}
	if err := p.st.Write(context.Background(), recs); err != nil {
		p.t.Fatalf("write: %v", err)
	}
	p.written = append(p.written, recs...)
}

// advanceTo runs the flush ticks up to now.
func (p *pipe) advanceTo(now time.Time) {
	p.t.Helper()
	if p.next.IsZero() {
		p.next = now.Add(coalesce.FlushPeriod)
		return
	}
	for !p.next.After(now) {
		p.write(p.co.Flush(p.next, nil))
		p.next = p.next.Add(coalesce.FlushPeriod)
	}
}

// feed translates the event, received a second after the time it carries, and
// writes what the coalescer lets through.
func (p *pipe) feed(x input) nodeagent.Result {
	p.t.Helper()
	received := instantOf(x.Rec).Add(time.Second)
	p.advanceTo(received)
	r, err := p.tr.Translate(x.Res, x.Rec, received)
	if err != nil {
		p.t.Fatalf("%s: %v", x.Name, err)
	}
	p.results = append(p.results, r)
	var out []store.Record
	for _, rec := range r.Records {
		out = p.co.Add(rec, out)
	}
	p.write(out)
	return r
}

func (p *pipe) feedAll(in []input) {
	p.t.Helper()
	for _, x := range in {
		p.feed(x)
	}
}

// shutdown writes every refresh the coalescer absorbed, as a graceful stop does.
func (p *pipe) shutdown() { p.write(p.co.FlushAll(nil)) }

// ---- streams ----

// steady is n reports of each template, every 300 seconds, the templates a second
// apart, in time order.
func steady(templates []input, n int, start time.Time) []input {
	var out []input
	for k := range n {
		for i, tpl := range templates {
			x := tpl.at(start.Add(time.Duration(k)*interval300 + time.Duration(i)*time.Second))
			out = append(out, x.named(tpl.Name))
		}
	}
	return out
}

// distinctAgents are the three nodes of the capture with three machine ids.
func distinctAgents(t testing.TB) []input { return loadFixture(t, fixtureDistinct) }

// ---- reading the store ----

var (
	l1 = store.Current(catalog.L1)
	l2 = store.Current(catalog.L2)
)

func alive(t testing.TB, st store.Store, f identity.Fingerprint, at time.Time) bool {
	t.Helper()
	ok, err := st.Alive(context.Background(), f, at, l1)
	if err != nil {
		t.Fatalf("Alive(%v, %s): %v", f, at, err)
	}
	return ok
}

// runsOn is the hosts a node runs on at the time, from the store.
func runsOn(t testing.TB, st store.Store, node identity.Fingerprint, at time.Time) []identity.Fingerprint {
	t.Helper()
	ns, err := st.Neighbors(context.Background(), node, store.Forward, at, l1)
	if err != nil {
		t.Fatalf("Neighbors(node %v, %s): %v", node, at, err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		if n.Relation == catalog.RunsOn {
			out = append(out, n.Peer)
		}
	}
	sortFPs(out)
	return out
}

// nodesOn is the nodes that run on the host at the time.
func nodesOn(t testing.TB, st store.Store, host identity.Fingerprint, at time.Time) []identity.Fingerprint {
	t.Helper()
	ns, err := st.Neighbors(context.Background(), host, store.Reverse, at, l1)
	if err != nil {
		t.Fatalf("Neighbors(host %v, %s): %v", host, at, err)
	}
	var out []identity.Fingerprint
	for _, n := range ns {
		if n.Relation == catalog.RunsOn {
			out = append(out, n.Peer)
		}
	}
	sortFPs(out)
	return out
}

func sortFPs(fps []identity.Fingerprint) {
	slices.SortFunc(fps, func(a, b identity.Fingerprint) int { return strings.Compare(a.String(), b.String()) })
}

func sorted(fps ...identity.Fingerprint) []identity.Fingerprint {
	sortFPs(fps)
	return fps
}

// podHost is the host of a pod at a time, derived the way the read side derives it
// and not written anywhere: the pod's scheduled_on node, and the host that node
// runs on over the same instant. It is the host only when there is exactly one.
type placement struct {
	host identity.Fingerprint
	why  string // "", "no node", "no runs_on", "two hosts"
}

func podHost(t testing.TB, st store.Store, pod identity.Fingerprint, at time.Time) placement {
	t.Helper()
	ns, err := st.Neighbors(context.Background(), pod, store.Forward, at, l2)
	if err != nil {
		t.Fatalf("Neighbors(pod, %s): %v", at, err)
	}
	hosts := map[identity.Fingerprint]bool{}
	nodes := 0
	for _, n := range ns {
		if n.Relation != catalog.ScheduledOn {
			continue
		}
		nodes++
		for _, h := range runsOn(t, st, n.Peer, at) {
			hosts[h] = true
		}
	}
	switch {
	case nodes == 0:
		return placement{why: "no node"}
	case len(hosts) == 0:
		return placement{why: "no runs_on"}
	case len(hosts) > 1:
		return placement{why: "two hosts"}
	}
	for h := range hosts {
		return placement{host: h}
	}
	panic("unreachable")
}

// schedule writes a pod's scheduled_on edge as the cluster translators do: an
// observation of another producer, in L2.
func schedule(t testing.TB, st store.Store, pod, node identity.Fingerprint, from time.Time, ttl time.Duration) {
	t.Helper()
	rec := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8scluster/test",
		EventTime: from, Seq: st.LastSeq() + 1, Kind: lifecycle.Observe, TTL: ttl, EventTimeBasis: store.BasisProducerEvent,
	}
	if err := st.Write(context.Background(), []store.Record{rec}); err != nil {
		t.Fatal(err)
	}
}

// ---- pebble ----

// countRecorder keeps the counters a pebblestore reports.
type countRecorder struct {
	mu     sync.Mutex
	counts map[string]int64
}

func newCountRecorder() *countRecorder { return &countRecorder{counts: map[string]int64{}} }

func (r *countRecorder) Count(name string, n int64) {
	r.mu.Lock()
	r.counts[name] += n
	r.mu.Unlock()
}

func (r *countRecorder) Sample(string, int64) {}

func (r *countRecorder) get(name string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[name]
}

// invalidations counts the checkpoints each write deletes, by write, once armed.
type invalidations struct {
	mu    sync.Mutex
	armed bool
	total int
	by    map[string]int
}

func (v *invalidations) arm() {
	v.mu.Lock()
	v.armed = true
	v.mu.Unlock()
}

func (v *invalidations) hook(prefix []byte, n int) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.armed {
		v.total += n
		if v.by == nil {
			v.by = map[string]int{}
		}
		v.by[string(prefix)] += n
	}
}

// openPebble opens a store in dir that writes a checkpoint every few records, with
// the given lag, and tells inv which checkpoints a write deletes.
func openPebble(t testing.TB, dir string, lag time.Duration, rec *countRecorder, inv *invalidations) *pebblestore.Store {
	t.Helper()
	opts := pebblestore.DefaultOptions()
	opts.Sync = false
	ck := pebblestore.DefaultCheckpoints()
	ck.KMin, ck.Lag = 4, lag
	opts.Checkpoints = &ck
	opts.Policy = bootPolicy
	opts.Recorder = rec
	st, err := pebblestore.Open(dir, opts)
	if err != nil {
		t.Fatal(err)
	}
	st.Instrument().OnInvalidate(inv.hook)
	return st
}
