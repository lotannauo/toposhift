package blastradius_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var ctx = context.Background()

// mkfp is an entity of type typ whose hash starts with n, so tests fix the
// order of members: store.CompareFingerprints orders by type name, then hash.
func mkfp(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func rack(n byte) identity.Fingerprint      { return mkfp(catalog.Rack, n) }
func zone(n byte) identity.Fingerprint      { return mkfp(catalog.Zone, n) }
func host(n byte) identity.Fingerprint      { return mkfp(catalog.Host, n) }
func node(n byte) identity.Fingerprint      { return mkfp(catalog.K8sNode, n) }
func pod(n byte) identity.Fingerprint       { return mkfp(catalog.K8sPod, n) }
func container(n byte) identity.Fingerprint { return mkfp(catalog.Container, n) }
func service(n byte) identity.Fingerprint   { return mkfp(catalog.Service, n) }
func instance(n byte) identity.Fingerprint  { return mkfp(catalog.ServiceInstance, n) }

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func edge(a, b identity.Fingerprint, rel catalog.RelationType) store.Subject {
	return store.EdgeSubject(a, b, rel)
}

func runsOn(n, h identity.Fingerprint) store.Subject    { return edge(n, h, catalog.RunsOn) }
func locatedIn(h, r identity.Fingerprint) store.Subject { return edge(h, r, catalog.LocatedIn) }
func sched(p, n identity.Fingerprint) store.Subject     { return edge(p, n, catalog.ScheduledOn) }
func partOf(c, p identity.Fingerprint) store.Subject    { return edge(c, p, catalog.PartOf) }
func dependsOn(a, b identity.Fingerprint) store.Subject { return edge(a, b, catalog.DependsOn) }
func instanceOf(i, s identity.Fingerprint) store.Subject {
	return edge(i, s, catalog.InstanceOf)
}

const (
	k8s    lifecycle.Producer = "k8sobjects/default"
	agent  lifecycle.Producer = "nodeagent/default"
	traces lifecycle.Producer = "traces"
)

// producerOf is the scenarios' producer of a relation's edges.
func producerOf(rel catalog.RelationType) lifecycle.Producer {
	switch rel {
	case catalog.RunsOn, catalog.LocatedIn:
		return agent
	case catalog.DependsOn:
		return traces
	}
	return k8s
}

func observe(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("x"),
	}
}

func run(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration, through time.Time) store.Record {
	r := observe(sub, p, at, ttl)
	r.Through = through
	return r
}

func deleted(sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Delete}
}

// feed writes batches to a store, numbering records after its LastSeq.
type feed struct {
	t   testing.TB
	st  store.Store
	seq uint64
}

func newFeed(t testing.TB, st store.Store) *feed { return &feed{t: t, st: st, seq: st.LastSeq()} }

func (f *feed) write(rs ...store.Record) []store.Record {
	f.t.Helper()
	rs = slices.Clone(rs)
	for i := range rs {
		f.seq++
		rs[i].Seq = f.seq
	}
	if err := f.st.Write(context.Background(), rs); err != nil {
		f.t.Fatal(err)
	}
	return rs
}

func openStore(t testing.TB, opts memstore.Options) *memstore.Store {
	t.Helper()
	st, err := memstore.Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

// spy wraps a store and counts every call, LastSeq included; it records each
// read's token and can run a hook before a Window.
type spy struct {
	store.Store
	mu       sync.Mutex
	calls    int
	writes   int
	windows  int
	tokens   []uint64
	onWindow func(n int)
}

func (s *spy) note(tok uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.tokens = append(s.tokens, tok)
}

func (s *spy) LastSeq() uint64 {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Store.LastSeq()
}

func (s *spy) LayerHorizon(l catalog.Layer) store.Horizon {
	s.mu.Lock()
	s.calls++
	s.mu.Unlock()
	return s.Store.LayerHorizon(l)
}

func (s *spy) Write(ctx context.Context, b []store.Record) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Write(ctx, b)
}

func (s *spy) Retain(ctx context.Context, h time.Time) error {
	s.mu.Lock()
	s.writes++
	s.mu.Unlock()
	return s.Store.Retain(ctx, h)
}

func (s *spy) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	s.note(sc.AsOf)
	return s.Store.Neighbors(ctx, fp, d, t, sc)
}

func (s *spy) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, t time.Time, sc store.Scope) ([][]store.Neighbor, error) {
	s.note(sc.AsOf)
	return s.Store.NeighborsBatch(ctx, fps, d, t, sc)
}

func (s *spy) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	s.note(sc.AsOf)
	return s.Store.Alive(ctx, fp, t, sc)
}

func (s *spy) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc.AsOf)
	s.mu.Lock()
	s.windows++
	n, hook := s.windows, s.onWindow
	s.mu.Unlock()
	if hook != nil {
		hook(n)
	}
	return s.Store.Window(ctx, fp, d, from, to, sc)
}

func (s *spy) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	s.note(sc.AsOf)
	return s.Store.EntityWindow(ctx, fp, from, to, sc)
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func spansString(ss []interval.Span) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = fmt.Sprintf("[%s, %s)", ts(s.Start), ts(s.End))
	}
	return strings.Join(parts, " ")
}

func sameSpans(a, b []interval.Span) bool {
	return slices.EqualFunc(a, b, func(x, y interval.Span) bool { return x.Start.Equal(y.Start) && x.End.Equal(y.End) })
}

// describeLeg is a leg as one comparable line: its subject, and per piece the
// span, Before, the end source and the producers.
func describeLeg(el interval.Element) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s>%s %s", el.Subject.A, el.Subject.B, el.Subject.Relation)
	for _, p := range el.Pieces {
		fmt.Fprintf(&b, " {[%s, %s) before=%t %s %v}", ts(p.Start), ts(p.End), p.Before, p.EndSource, p.Producers)
	}
	return b.String()
}

// describe is a member as one comparable line.
func describe(m blastradius.Member) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s depth=%d reach=%s %s %s transitions=%d", m.Entity, m.Depth, spansString(m.Reach), m.Duration, m.Class, m.Transitions)
	for _, a := range m.Arrivals {
		fmt.Fprintf(&b, " <%s %s %s %s leg=%s>", a.Parent, a.Relation, a.Propagation, spansString(a.Spans), describeLeg(a.Leg))
	}
	return b.String()
}

func describeAll(ms []blastradius.Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		out[i] = describe(m)
	}
	return out
}

func describeUnattributed(us []blastradius.Unattributed) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = fmt.Sprintf("%s %s %s %s %v", u.Subject, u.Relation, u.Target, spansString(u.Spans), u.Candidates)
	}
	return out
}

func quarantinedSubjects(els []interval.Element) []string {
	out := make([]string, len(els))
	for i, el := range els {
		out[i] = fmt.Sprintf("%s>%s %s", el.Subject.A, el.Subject.B, el.Subject.Relation)
	}
	return out
}

func entities(ms []blastradius.Member) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, m := range ms {
		out = append(out, m.Entity)
	}
	return out
}

func diffLines(what string, got, want []string) string {
	if slices.Equal(got, want) {
		return ""
	}
	return fmt.Sprintf("%s:\ngot:\n  %s\nwant:\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
}

// sameResult returns "" when two results agree on everything but Reads, and
// otherwise the first difference.
func sameResult(got, want blastradius.Result) string {
	if got.Token != want.Token || !got.From.Equal(want.From) || !got.To.Equal(want.To) || got.Seed != want.Seed ||
		got.PolicyVersion != want.PolicyVersion || got.IntervalPolicy != want.IntervalPolicy {
		return fmt.Sprintf("header %d %s %s %s %d %d, want %d %s %s %s %d %d",
			got.Token, ts(got.From), ts(got.To), got.Seed, got.PolicyVersion, got.IntervalPolicy,
			want.Token, ts(want.From), ts(want.To), want.Seed, want.PolicyVersion, want.IntervalPolicy)
	}
	for _, d := range []string{
		diffLines("members", describeAll(got.Members), describeAll(want.Members)),
		diffLines("unattributed", describeUnattributed(got.Unattributed), describeUnattributed(want.Unattributed)),
		diffLines("quarantined", quarantinedSubjects(got.Quarantined), quarantinedSubjects(want.Quarantined)),
	} {
		if d != "" {
			return d
		}
	}
	if fmt.Sprint(got.Truncated) != fmt.Sprint(want.Truncated) {
		return fmt.Sprintf("truncated %+v, want %+v", got.Truncated, want.Truncated)
	}
	return ""
}
