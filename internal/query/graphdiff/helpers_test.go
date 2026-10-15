package graphdiff_test

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
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var ctx = context.Background()

// mkfp is an entity of type typ whose hash starts with n, so tests fix the
// order of subjects: store.CompareFingerprints orders by type name, then hash.
func mkfp(typ catalog.EntityType, n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

func pod(n byte) identity.Fingerprint  { return mkfp(catalog.K8sPod, n) }
func node(n byte) identity.Fingerprint { return mkfp(catalog.K8sNode, n) }
func host(n byte) identity.Fingerprint { return mkfp(catalog.Host, n) }

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func sched(p, n identity.Fingerprint) store.Subject {
	return store.EdgeSubject(p, n, catalog.ScheduledOn)
}

func runsOn(n, h identity.Fingerprint) store.Subject { return store.EdgeSubject(n, h, catalog.RunsOn) }

const (
	k8s     lifecycle.Producer = "k8sobjects/default"
	cluster lifecycle.Producer = "k8scluster/default"
	agent   lifecycle.Producer = "nodeagent/default"
)

// observeP is an Observe carrying payload.
func observeP(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration, payload string) store.Record {
	r := store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl,
	}
	if payload != "" {
		r.Payload = []byte(payload)
	}
	return r
}

// observe is an Observe with the payload {}.
func observe(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return observeP(sub, p, at, ttl, "{}")
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
// read's token, counts writes and retentions, and can run a hook before a
// Window.
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

// subjectString names a subject: an entity by its fingerprint, an edge as
// A>B relation.
func subjectString(s store.Subject) string {
	if s.Kind == store.SubjectEntity {
		return s.A.String()
	}
	return fmt.Sprintf("%s>%s %s", s.A, s.B, s.Relation)
}

func descriptionString(d *interval.Description) string {
	if d == nil {
		return "-"
	}
	if d.Payload == nil {
		return fmt.Sprintf("%s:nil", d.Producer)
	}
	return fmt.Sprintf("%s:%q", d.Producer, d.Payload)
}

// elementString is an element as one comparable line: its subject and layer,
// every piece, its class, its quarantine marks and its descriptions.
func elementString(el interval.Element) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s", subjectString(el.Subject), el.Layer)
	for _, p := range el.Pieces {
		fmt.Fprintf(&b, " [%s, %s) before=%t %s %v", ts(p.Start), ts(p.End), p.Before, p.EndSource, p.Producers)
	}
	fmt.Fprintf(&b, " %s transitions=%d quarantine=%t uncascaded=%t from=%s last=%s",
		el.Class, el.Transitions, el.Quarantine != nil, el.Uncascaded != nil,
		descriptionString(el.AtFrom), descriptionString(el.AtLast))
	return b.String()
}

func changeString(c *graphdiff.DescriptionChange) string {
	if c == nil {
		return "-"
	}
	return fmt.Sprintf("%s state=%v attrs=%v %s->%s", c.Kind, c.StateKeys, c.AttributeKeys, c.FromProducer, c.ToProducer)
}

func entryString(e graphdiff.Entry) string {
	s := elementString(e.Element)
	if e.DeleteSource != lifecycle.EndUnknown || !e.DeletedAt.IsZero() {
		s += fmt.Sprintf(" deleted=%s@%s", e.DeleteSource, ts(e.DeletedAt))
	}
	if e.Change != nil {
		s += " change=" + changeString(e.Change)
	}
	return s
}

func entryStrings(es []graphdiff.Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = entryString(e)
	}
	return out
}

func elementStrings(els []interval.Element) []string {
	out := make([]string, len(els))
	for i, el := range els {
		out[i] = elementString(el)
	}
	return out
}

func eventString(e graphdiff.Event) string {
	if e.Appear {
		return fmt.Sprintf("%s appear %s", ts(e.At), subjectString(e.Subject))
	}
	return fmt.Sprintf("%s disappear %s %s", ts(e.At), subjectString(e.Subject), e.Source)
}

func eventStrings(es []graphdiff.Event) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = eventString(e)
	}
	return out
}

func diffLines(what string, got, want []string) string {
	if slices.Equal(got, want) {
		return ""
	}
	return fmt.Sprintf("%s:\ngot:\n  %s\nwant:\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
}

// sameResult returns "" when two results agree on everything but Token and
// Reads, and otherwise the first difference. The change log is compared too.
func sameResult(got, want graphdiff.Result) string {
	if !got.T1.Equal(want.T1) || !got.T2.Equal(want.T2) || fmt.Sprint(got.Scope) != fmt.Sprint(want.Scope) {
		return fmt.Sprintf("header %s %s %v, want %s %s %v", ts(got.T1), ts(got.T2), got.Scope, ts(want.T1), ts(want.T2), want.Scope)
	}
	for _, d := range []string{
		diffLines("created", entryStrings(got.Created), entryStrings(want.Created)),
		diffLines("deleted", entryStrings(got.Deleted), entryStrings(want.Deleted)),
		diffLines("changed", entryStrings(got.Changed), entryStrings(want.Changed)),
		diffLines("transient", entryStrings(got.Transient), entryStrings(want.Transient)),
		diffLines("quarantined", elementStrings(got.Quarantined), elementStrings(want.Quarantined)),
		diffLines("events", eventStrings(graphdiff.Events(got)), eventStrings(graphdiff.Events(want))),
	} {
		if d != "" {
			return d
		}
	}
	if got.Counts != want.Counts {
		return fmt.Sprintf("counts %+v, want %+v", got.Counts, want.Counts)
	}
	if got.Truncated != want.Truncated {
		return fmt.Sprintf("truncated %+v, want %+v", got.Truncated, want.Truncated)
	}
	if got.PolicyVersion != want.PolicyVersion || got.IntervalPolicy != want.IntervalPolicy {
		return fmt.Sprintf("versions %d %d, want %d %d", got.PolicyVersion, got.IntervalPolicy, want.PolicyVersion, want.IntervalPolicy)
	}
	return ""
}

func diffOf(t tb, st store.Store, scope []identity.Fingerprint, t1, t2 time.Time, asOf uint64, o graphdiff.Options) graphdiff.Result {
	t.Helper()
	res, err := graphdiff.NewDiffer(st).Diff(ctx, scope, t1, t2, asOf, o)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// wide is a policy that refuses no generated scope and truncates nothing.
func wide() graphdiff.Policy { return graphdiff.Policy{MaxScope: 1 << 10, MaxListed: 1 << 20} }

// tb is what the checks need of a *testing.T or a *rapid.T.
type tb interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}
