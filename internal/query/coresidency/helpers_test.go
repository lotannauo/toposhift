package coresidency_test

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
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

var ctx = context.Background()

// mkfp is an entity of type typ whose hash starts with n, so tests fix the
// order of peers: store.CompareFingerprints orders by type name, then hash.
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
	k8s   lifecycle.Producer = "k8sobjects/default"
	agent lifecycle.Producer = "nodeagent/default"
)

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

// describe is an entry as one comparable line: its path, its legs' subjects,
// and per overlap the span, Before, the producers of each leg's piece, and the
// ends.
func describe(c coresidency.CoResident) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s peer=%s nodes=%v via=%s legs=[", c.Mechanism, c.Peer, c.Nodes, c.Via)
	for i, l := range c.Legs {
		if i > 0 {
			b.WriteString(" ")
		}
		fmt.Fprintf(&b, "%s>%s", l.Subject.A, l.Subject.B)
	}
	fmt.Fprintf(&b, "] %s %s transitions=%d reason=%s", c.Duration, c.Class, c.Transitions, c.Reason)
	for _, o := range c.Overlaps {
		fmt.Fprintf(&b, " {[%s, %s) before=%t", ts(o.Start), ts(o.End), o.Before)
		for i, k := range o.Pieces {
			p := c.Legs[i].Pieces[k]
			if !p.Start.After(o.Start) && !p.End.Before(o.End) {
				fmt.Fprintf(&b, " leg%d%v", i, p.Producers)
			} else {
				fmt.Fprintf(&b, " leg%d:piece-does-not-contain", i)
			}
		}
		for _, e := range o.Ends {
			if e.Unattributed {
				fmt.Fprintf(&b, " end(%d,unattributed)", e.Leg)
			} else {
				fmt.Fprintf(&b, " end(%d,%s)", e.Leg, e.Source)
			}
		}
		b.WriteString("}")
	}
	return b.String()
}

func describeAll(cs []coresidency.CoResident) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = describe(c)
	}
	return out
}

func describeUnattributed(us []coresidency.Unattributed) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = fmt.Sprintf("%s %s %s %v", u.Subject, u.Reason, spansString(u.Spans), u.Candidates)
	}
	return out
}

func describeSeedHosts(hs []coresidency.HostSpans) []string {
	out := make([]string, len(hs))
	for i, h := range hs {
		out[i] = fmt.Sprintf("%s %s %s", h.Node, h.Host, spansString(h.Spans))
	}
	return out
}

func diffLines(what string, got, want []string) string {
	if slices.Equal(got, want) {
		return ""
	}
	return fmt.Sprintf("%s:\ngot:\n  %s\nwant:\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
}

func quarantinedSubjects(els []interval.Element) []string {
	out := make([]string, len(els))
	for i, el := range els {
		out[i] = fmt.Sprintf("%s>%s %s", el.Subject.A, el.Subject.B, el.Subject.Relation)
	}
	return out
}

// sameResult returns "" when two results agree on everything but Reads, and
// otherwise the first difference.
func sameResult(got, want coresidency.Result) string {
	if got.Token != want.Token || !got.From.Equal(want.From) || !got.To.Equal(want.To) || got.Seed != want.Seed {
		return fmt.Sprintf("header %d %s %s %s, want %d %s %s %s", got.Token, ts(got.From), ts(got.To), got.Seed, want.Token, ts(want.From), ts(want.To), want.Seed)
	}
	for _, d := range []string{
		diffLines("co-residents", describeAll(got.CoResidents), describeAll(want.CoResidents)),
		diffLines("suppressed", describeAll(got.Suppressed), describeAll(want.Suppressed)),
		diffLines("unattributed", describeUnattributed(got.Unattributed), describeUnattributed(want.Unattributed)),
		diffLines("seed hosts", describeSeedHosts(got.SeedHosts), describeSeedHosts(want.SeedHosts)),
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
