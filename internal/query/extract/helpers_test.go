package extract_test

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
	"github.com/lotannauo/toposhift/internal/query/extract"
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

func svc(n byte) identity.Fingerprint  { return mkfp(catalog.Service, n) }
func inst(n byte) identity.Fingerprint { return mkfp(catalog.ServiceInstance, n) }
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

func partOf(i, p identity.Fingerprint) store.Subject { return store.EdgeSubject(i, p, catalog.PartOf) }

func instOf(i, s identity.Fingerprint) store.Subject {
	return store.EdgeSubject(i, s, catalog.InstanceOf)
}

func dependsOn(a, b identity.Fingerprint) store.Subject {
	return store.EdgeSubject(a, b, catalog.DependsOn)
}

const (
	k8s    lifecycle.Producer = "k8sobjects/default"
	traces lifecycle.Producer = "traces/default"
	agent  lifecycle.Producer = "nodeagent/default"
)

// producerOf is the scenarios' producer of a subject: traces for services,
// instances and their edges, the node agent for runs_on, and k8sobjects for
// the rest.
func producerOf(sub store.Subject) lifecycle.Producer {
	switch {
	case sub.Kind == store.SubjectEntity && (sub.A.Type() == catalog.Service || sub.A.Type() == catalog.ServiceInstance),
		sub.Relation == catalog.InstanceOf, sub.Relation == catalog.DependsOn,
		sub.Relation == catalog.PartOf && sub.A.Type() == catalog.ServiceInstance:
		return traces
	case sub.Relation == catalog.RunsOn:
		return agent
	}
	return k8s
}

func observe(sub store.Subject, p lifecycle.Producer, at time.Time, ttl time.Duration) store.Record {
	return store.Record{
		Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at,
		Kind: lifecycle.Observe, TTL: ttl, Payload: []byte("x"),
	}
}

func deleted(sub store.Subject, p lifecycle.Producer, at time.Time) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Delete}
}

// obs and del are observe and deleted with the scenarios' producer.
func obs(sub store.Subject, at time.Time) store.Record { return observe(sub, producerOf(sub), at, 0) }
func del(sub store.Subject, at time.Time) store.Record { return deleted(sub, producerOf(sub), at) }

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

// WindowSince stands for one Window (or, for direction 0, one EntityWindow) per
// fingerprint: it notes each one's token, and runs the hook before the store read
// for each fingerprint of an edge read.
func (s *spy) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	var ns []int
	var hook func(int)
	for range fps {
		s.note(sc.AsOf)
		if d == 0 {
			continue
		}
		s.mu.Lock()
		s.windows++
		ns, hook = append(ns, s.windows), s.onWindow
		s.mu.Unlock()
	}
	for _, n := range ns {
		if hook != nil {
			hook(n)
		}
	}
	return s.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func spansString(ss []interval.Span) string {
	parts := make([]string, len(ss))
	for i, s := range ss {
		parts[i] = fmt.Sprintf("[%s, %s)", ts(s.Start), ts(s.End))
	}
	return strings.Join(parts, " ")
}

func subjectString(s store.Subject) string { return fmt.Sprintf("%s>%s", s.A, s.B) }

func diffLines(what string, got, want []string) string {
	if slices.Equal(got, want) {
		return ""
	}
	return fmt.Sprintf("%s:\ngot:\n  %s\nwant:\n  %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
}

// contribKey names a contribution independently of its index.
func contribKey(c extract.Contribution) string {
	return fmt.Sprintf("%s/%s %s/%s via %s", c.MemberPod, c.MemberInst, c.PeerPod, c.PeerInst, c.Via)
}

// endString writes an end; with byKey, its contribution by key rather than
// index.
func endString(p extract.Pair, e extract.End, byKey bool) string {
	who := fmt.Sprintf("c%d", e.Contribution)
	if byKey {
		who = contribKey(p.Contributions[e.Contribution])
	}
	switch {
	case e.Pod != nil && e.Pod.Unattributed:
		return fmt.Sprintf("%s:leg%d:pod(%d,unattributed)", who, e.Leg, e.Pod.Leg)
	case e.Pod != nil:
		return fmt.Sprintf("%s:leg%d:pod(%d,%s)", who, e.Leg, e.Pod.Leg, e.Pod.Source)
	case e.Unattributed:
		return fmt.Sprintf("%s:leg%d:unattributed", who, e.Leg)
	}
	return fmt.Sprintf("%s:leg%d:%s", who, e.Leg, e.Source)
}

// describePair is a pair as one comparable line. With byKey, contributions are
// sorted and named by key, so two answers that order them differently agree.
func describePair(p extract.Pair, byKey bool) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s member=%s peer=%s %s %s transitions=%d reason=%s coverage=%v", p.Mechanism, p.Member, p.Peer, p.Duration, p.Class, p.Transitions, p.Reason, p.Coverage)
	for _, s := range p.Spans {
		var ends []string
		for _, e := range s.Ends {
			ends = append(ends, endString(p, e, byKey))
		}
		if byKey {
			slices.Sort(ends)
		}
		fmt.Fprintf(&b, " {[%s, %s) before=%t ends=%v}", ts(s.Start), ts(s.End), s.Before, ends)
	}
	var vias []string
	for v, ss := range p.Vias {
		vias = append(vias, fmt.Sprintf("%s:%s", v, spansString(ss)))
	}
	slices.Sort(vias)
	fmt.Fprintf(&b, " vias=%v", vias)
	if byKey {
		var cs []string
		for _, c := range p.Contributions {
			var legs []string
			for _, l := range c.Legs {
				legs = append(legs, subjectString(l.Subject))
			}
			entry := "pod"
			if c.Entry >= 0 {
				entry = "entry"
			}
			cs = append(cs, fmt.Sprintf("<%s %s legs=%v %s>", contribKey(c), entry, legs, spansString(c.Spans)))
		}
		slices.Sort(cs)
		fmt.Fprintf(&b, " contributions=%v", cs)
	} else {
		fmt.Fprintf(&b, " contributions=%d", len(p.Contributions))
	}
	return b.String()
}

func describePairs(ps []extract.Pair, byKey bool) []string {
	out := make([]string, len(ps))
	for i, p := range ps {
		out[i] = describePair(p, byKey)
	}
	return out
}

func describeMembers(ms []extract.Member) []string {
	out := make([]string, len(ms))
	for i, m := range ms {
		var deps, insts []string
		for _, el := range m.Dependency {
			deps = append(deps, fmt.Sprintf("%s %s %s", subjectString(el.Subject), el.Class, piecesString(el.Pieces)))
		}
		for _, el := range m.Instances {
			insts = append(insts, subjectString(el.Subject))
		}
		out[i] = fmt.Sprintf("%s %s deps=%v instances=%v", m.Service, m.Role, deps, insts)
	}
	return out
}

func piecesString(ps []interval.Piece) string {
	parts := make([]string, len(ps))
	for i, p := range ps {
		parts[i] = fmt.Sprintf("[%s, %s) before=%t end=%s", ts(p.Start), ts(p.End), p.Before, p.EndSource)
	}
	return strings.Join(parts, " ")
}

func describeUnattributed(us []extract.Unattributed) []string {
	out := make([]string, len(us))
	for i, u := range us {
		why := u.Mapping.String()
		if u.Mapping == 0 {
			why = u.Placement.String()
		}
		seed := "-"
		if !u.SeedPod.IsZero() {
			seed = u.SeedPod.String()
		}
		out[i] = fmt.Sprintf("%s %s %s %s %v", seed, u.Subject, why, spansString(u.Spans), u.Candidates)
	}
	return out
}

func describeUnmapped(us []extract.Unmapped) []string {
	out := make([]string, len(us))
	for i, u := range us {
		out[i] = fmt.Sprintf("%s %s", u.Pod, spansString(u.Spans))
	}
	return out
}

func seedsOf(res extract.Result) []identity.Fingerprint {
	var out []identity.Fingerprint
	for _, fr := range res.PodResults {
		out = append(out, fr.Seed)
	}
	return out
}

// describeEntries is a Find's co-residents as lines: path, spans, before and
// ends.
func describeEntries(cs []coresidency.CoResident) []string {
	out := make([]string, len(cs))
	for i, c := range cs {
		var b strings.Builder
		fmt.Fprintf(&b, "%s peer=%s nodes=%v via=%s legs=[", c.Mechanism, c.Peer, c.Nodes, c.Via)
		for j, l := range c.Legs {
			if j > 0 {
				b.WriteString(" ")
			}
			b.WriteString(subjectString(l.Subject))
		}
		fmt.Fprintf(&b, "] %s %s transitions=%d reason=%s", c.Duration, c.Class, c.Transitions, c.Reason)
		for _, o := range c.Overlaps {
			fmt.Fprintf(&b, " {[%s, %s) before=%t ends=%v}", ts(o.Start), ts(o.End), o.Before, o.Ends)
		}
		out[i] = b.String()
	}
	return out
}

func sameSpans(a, b []interval.Span) bool {
	return slices.EqualFunc(a, b, func(x, y interval.Span) bool { return x.Start.Equal(y.Start) && x.End.Equal(y.End) })
}

// readRec is one read a recorder saw: the method, the arguments that identify
// the read, and its scope.
type readRec struct {
	op       string
	fp       identity.Fingerprint
	dir      store.Direction
	from, to time.Time
	layer    catalog.Layer
	token    uint64
}

// recorder wraps a store and records every Window, EntityWindow and Alive
// request that reaches it, and counts what must never reach it.
type recorder struct {
	store.Store
	mu     sync.Mutex
	reads  []readRec
	writes int
	closes int
}

func (r *recorder) note(x readRec) {
	r.mu.Lock()
	r.reads = append(r.reads, x)
	r.mu.Unlock()
}

func (r *recorder) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	r.note(readRec{op: "Window", fp: fp, dir: d, from: from, to: to, layer: sc.Layer, token: sc.AsOf})
	return r.Store.Window(ctx, fp, d, from, to, sc)
}

func (r *recorder) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	r.note(readRec{op: "EntityWindow", fp: fp, from: from, to: to, layer: sc.Layer, token: sc.AsOf})
	return r.Store.EntityWindow(ctx, fp, from, to, sc)
}

// WindowSince records one Window (or, for direction 0, one EntityWindow) per
// fingerprint: the reads it stands for.
func (r *recorder) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	for _, fp := range fps {
		if d == 0 {
			r.note(readRec{op: "EntityWindow", fp: fp, from: from, to: to, layer: sc.Layer, token: sc.AsOf})
		} else {
			r.note(readRec{op: "Window", fp: fp, dir: d, from: from, to: to, layer: sc.Layer, token: sc.AsOf})
		}
	}
	return r.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func (r *recorder) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	r.note(readRec{op: "Alive", fp: fp, from: t, layer: sc.Layer, token: sc.AsOf})
	return r.Store.Alive(ctx, fp, t, sc)
}

func (r *recorder) Write(ctx context.Context, b []store.Record) error {
	r.mu.Lock()
	r.writes++
	r.mu.Unlock()
	return r.Store.Write(ctx, b)
}

func (r *recorder) Retain(ctx context.Context, h time.Time) error {
	r.mu.Lock()
	r.writes++
	r.mu.Unlock()
	return r.Store.Retain(ctx, h)
}

func (r *recorder) Close() error {
	r.mu.Lock()
	r.closes++
	r.mu.Unlock()
	return r.Store.Close()
}

// counts is the recording as the Reads of an extraction with Calls 0.
func (r *recorder) counts() coresidency.Reads {
	var out coresidency.Reads
	for _, x := range r.reads {
		switch x.op {
		case "Window":
			out.Windows++
		case "EntityWindow":
			out.EntityWindows++
		case "Alive":
			out.Alive++
		}
	}
	return out
}

// formulaF is the number of reads that must reach the store when every answer
// is kept: the distinct (fingerprint, direction, layer) among the Windows and
// the distinct fingerprints among the Alives and the EntityWindows. It
// assumes the instants and the token of each method are constant, which
// constancy checks.
func formulaF(reads []readRec) coresidency.Reads {
	type wk struct {
		fp    identity.Fingerprint
		dir   store.Direction
		layer catalog.Layer
	}
	windows, alive, ew := map[wk]bool{}, map[identity.Fingerprint]bool{}, map[identity.Fingerprint]bool{}
	for _, x := range reads {
		switch x.op {
		case "Window":
			windows[wk{x.fp, x.dir, x.layer}] = true
		case "Alive":
			alive[x.fp] = true
		case "EntityWindow":
			ew[x.fp] = true
		}
	}
	return coresidency.Reads{Windows: len(windows), Alive: len(alive), EntityWindows: len(ew)}
}

// constant returns "" when every read of each method in a recording has the
// same instants and token, and otherwise the first difference.
func constant(reads []readRec) string {
	first := map[string]readRec{}
	for _, x := range reads {
		f, ok := first[x.op]
		if !ok {
			first[x.op] = x
			continue
		}
		if !f.from.Equal(x.from) || !f.to.Equal(x.to) || f.token != x.token {
			return fmt.Sprintf("%s read at [%s, %s) token %d, and at [%s, %s) token %d", x.op, ts(f.from), ts(f.to), f.token, ts(x.from), ts(x.to), x.token)
		}
	}
	tokens := map[uint64]bool{}
	for _, x := range reads {
		tokens[x.token] = true
	}
	if len(tokens) > 1 {
		return fmt.Sprintf("reads at %d different tokens", len(tokens))
	}
	return ""
}
