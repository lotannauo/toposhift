package memstore_test

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

var base = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

// l2 is the scope of the layer the test records are in.
var l2 = store.Current(catalog.L2)

func fp(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

type topology struct {
	pod, node, node2 identity.Fingerprint
}

func newTopology(t testing.TB) topology {
	return topology{
		pod:   fp(t, catalog.K8sPod, catalog.K8sPodUID, "p1"),
		node:  fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n1"),
		node2: fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n2"),
	}
}

func open(t testing.TB, opts ...memstore.Options) *memstore.Store {
	t.Helper()
	var o memstore.Options
	if len(opts) > 0 {
		o = opts[0]
	}
	s, err := memstore.Open(o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func write(t testing.TB, s *memstore.Store, recs ...store.Record) {
	t.Helper()
	if err := s.Write(context.Background(), recs); err != nil {
		t.Fatal(err)
	}
}

// placed is a record of the edge "pod scheduled_on node" in layer L2.
func placed(w topology, node identity.Fingerprint, seq uint64, p lifecycle.Producer, t time.Duration, kind lifecycle.Kind, ttl time.Duration) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(w.pod, node, catalog.ScheduledOn),
		Producer: p, EventTime: at(t), Seq: seq, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte{byte(seq)}
	}
	return r
}

// podRecord is a record of the pod's own existence in layer L2.
func podRecord(w topology, seq uint64, p lifecycle.Producer, t time.Duration, kind lifecycle.Kind, payload string) store.Record {
	r := store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(w.pod), Producer: p, EventTime: at(t), Seq: seq, Kind: kind,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte(payload)
	}
	return r
}

func neighbors(t testing.TB, s *memstore.Store, fp identity.Fingerprint, dir store.Direction, at time.Time) []store.Neighbor {
	t.Helper()
	ns, err := s.Neighbors(context.Background(), fp, dir, at, l2)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func seqs(recs []store.Record) []uint64 {
	var out []uint64
	for _, r := range recs {
		out = append(out, r.Seq)
	}
	return out
}

func TestNeighborsAreSymmetricAndFollowTheLifecycle(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s,
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 10*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 10*time.Minute, lifecycle.Observe, 0),
	)

	want := func(peer identity.Fingerprint) []store.Neighbor {
		return []store.Neighbor{{Peer: peer, Relation: catalog.ScheduledOn}}
	}
	for _, tt := range []struct {
		name string
		when time.Duration
		node identity.Fingerprint // where the pod is, or zero
	}{
		{"before it exists", -time.Second, identity.Fingerprint{}},
		{"on the first node", 5 * time.Minute, w.node},
		{"moved at once", 10 * time.Minute, w.node2},
		{"still on the second", time.Hour, w.node2},
	} {
		got := neighbors(t, s, w.pod, store.Forward, at(tt.when))
		if tt.node.IsZero() {
			if len(got) != 0 {
				t.Errorf("%s: pod has neighbors %v", tt.name, got)
			}
			continue
		}
		if !slices.Equal(got, want(tt.node)) {
			t.Errorf("%s: pod's neighbors = %v, want %v", tt.name, got, want(tt.node))
		}
		// The same edge read from the node.
		if rev := neighbors(t, s, tt.node, store.Reverse, at(tt.when)); !slices.Equal(rev, want(w.pod)) {
			t.Errorf("%s: node's reverse neighbors = %v, want the pod", tt.name, rev)
		}
	}
	// The node the pod left has nothing pointing at it any more.
	if rev := neighbors(t, s, w.node, store.Reverse, at(time.Hour)); len(rev) != 0 {
		t.Errorf("the old node still has reverse neighbors %v", rev)
	}
}

func TestExpiryAndLateWrites(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s, placed(w, w.node, 1, "node-collector", 0, lifecycle.Observe, 5*time.Minute))
	if got := neighbors(t, s, w.pod, store.Forward, at(6*time.Minute)); len(got) != 0 {
		t.Fatalf("the edge outlived its TTL: %v", got)
	}

	// A refresh that arrives late, with an event time before the deadline,
	// revives it for the time it covers. The cached fold must not be reused.
	write(t, s, placed(w, w.node, 2, "node-collector", 4*time.Minute, lifecycle.Observe, 5*time.Minute))
	if got := neighbors(t, s, w.pod, store.Forward, at(6*time.Minute)); len(got) != 1 {
		t.Errorf("a late refresh did not fill the gap: %v", got)
	}
	if got := neighbors(t, s, w.pod, store.Forward, at(10*time.Minute)); len(got) != 0 {
		t.Errorf("the refresh outlived its own TTL: %v", got)
	}
}

func TestEntityExistence(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s, podRecord(w, 1, "k8s", 0, lifecycle.Observe, "x"), podRecord(w, 2, "k8s", time.Minute, lifecycle.Delete, ""))
	for _, tt := range []struct {
		when time.Duration
		want bool
	}{{-1, false}, {0, true}, {time.Minute - 1, true}, {time.Minute, false}} {
		if got, err := s.Alive(context.Background(), w.pod, at(tt.when), l2); err != nil || got != tt.want {
			t.Errorf("Alive at %s = %v, %v; want %v", tt.when, got, err, tt.want)
		}
	}
}

func TestWindowIsHalfOpenSortedAndPerDirection(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s,
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 5*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 5*time.Minute, lifecycle.Observe, 0),
		placed(w, w.node2, 4, "k8s", 2*time.Minute, lifecycle.Observe, time.Hour), // arrived late
	)
	ctx := context.Background()

	got, err := s.Window(ctx, w.pod, store.Forward, at(0), at(5*time.Minute), l2)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(seqs(got), []uint64{1, 4}) { // by event time, and the end is exclusive
		t.Errorf("window seqs = %v, want [1 4]", seqs(got))
	}
	if rev, _ := s.Window(ctx, w.node, store.Reverse, at(0), at(time.Hour), l2); len(rev) != 2 {
		t.Errorf("the old node's reverse window has %d records, want 2", len(rev))
	}
	if fwd, _ := s.Window(ctx, w.node, store.Forward, at(0), at(time.Hour), l2); len(fwd) != 0 {
		t.Errorf("a node has %d forward records for an edge it is the target of", len(fwd))
	}
	// The start is inclusive and the end exclusive, to the nanosecond.
	for _, tt := range []struct {
		from, to time.Time
		want     []uint64
	}{
		{at(0), at(1), []uint64{1}},
		{at(0).Add(1), at(2 * time.Minute), nil},
		{at(2 * time.Minute), at(2*time.Minute + 1), []uint64{4}},
		{at(2*time.Minute + 1), at(5 * time.Minute), nil},
		{at(5 * time.Minute), at(5*time.Minute + 1), []uint64{2, 3}},
	} {
		got, err := s.Window(ctx, w.pod, store.Forward, tt.from, tt.to, l2)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(seqs(got), tt.want) {
			t.Errorf("window [%s, %s) has seqs %v, want %v", tt.from.Sub(base), tt.to.Sub(base), seqs(got), tt.want)
		}
	}

	// A returned record is a copy.
	got[0].Payload[0] = 0xff
	again, _ := s.Window(ctx, w.pod, store.Forward, at(0), at(5*time.Minute), l2)
	if again[0].Payload[0] == 0xff {
		t.Error("mutating a window result changed what the store holds")
	}
}

func TestWindowReturnsEveryFieldOfTheRecord(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	run := placed(w, w.node, 1, "node-collector", time.Minute, lifecycle.Observe, 5*time.Minute)
	run.Through = at(4 * time.Minute)
	plain := placed(w, w.node, 2, "node-collector", 10*time.Minute, lifecycle.Observe, 5*time.Minute)
	del := placed(w, w.node, 3, "k8s", 11*time.Minute, lifecycle.Delete, 0)
	write(t, s, run, plain, del)

	got, err := s.Window(context.Background(), w.pod, store.Forward, at(0), at(time.Hour), l2)
	if err != nil {
		t.Fatal(err)
	}
	want := []store.Record{run, plain, del}
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		g, r := got[i], want[i]
		if g.Layer != r.Layer || g.Subject != r.Subject || g.Producer != r.Producer || !g.EventTime.Equal(r.EventTime) ||
			g.Seq != r.Seq || g.Kind != r.Kind || g.TTL != r.TTL || !g.Through.Equal(r.Through) || !bytes.Equal(g.Payload, r.Payload) {
			t.Errorf("record %d = %+v, want %+v", i, g, r)
		}
		if g.EventTime.Location() != time.UTC || (!g.Through.IsZero() && g.Through.Location() != time.UTC) {
			t.Errorf("record %d has times that are not in UTC: %s, %s", i, g.EventTime, g.Through)
		}
	}
	if !got[1].Through.IsZero() || !got[2].Through.IsZero() {
		t.Errorf("records that are not runs have Through %s and %s, want the zero time", got[1].Through, got[2].Through)
	}
}

// TestEntityWindowReturnsEveryFieldOfAHostRecord reads back the records of a host
// that carry a boot id, one of them a run. A boot belongs to the observation of a
// host, so Window, which returns edge records only, never has one.
func TestEntityWindowReturnsEveryFieldOfAHostRecord(t *testing.T) {
	t.Parallel()
	host := fp(t, catalog.Host, catalog.HostID, "h1")
	rack := fp(t, catalog.Rack, catalog.RackID, "r1")
	s := open(t)
	hostRecord := func(seq uint64, d time.Duration, kind lifecycle.Kind, boot string) store.Record {
		r := store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "node-collector", EventTime: at(d),
			Seq: seq, Kind: kind,
		}
		if kind == lifecycle.Observe {
			r.TTL, r.Payload, r.Boot = 5*time.Minute, []byte{byte(seq)}, boot
		}
		return r
	}
	run := hostRecord(1, time.Minute, lifecycle.Observe, "boot-a")
	run.Through = at(4 * time.Minute)
	noBoot := hostRecord(2, 5*time.Minute, lifecycle.Observe, "")
	rebooted := hostRecord(3, 10*time.Minute, lifecycle.Observe, "boot-b")
	del := hostRecord(4, 11*time.Minute, lifecycle.Delete, "")
	placement := store.Record{
		Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "fabric",
		EventTime: at(time.Minute), Seq: 5, Kind: lifecycle.Observe, Payload: []byte("placement"),
	}
	want := []store.Record{run, noBoot, rebooted, del}
	write(t, s, run, noBoot, rebooted, del, placement)

	got, err := s.EntityWindow(context.Background(), host, at(0), at(time.Hour), l1)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range want {
		g, r := got[i], want[i]
		if g.Layer != r.Layer || g.Subject != r.Subject || g.Producer != r.Producer || !g.EventTime.Equal(r.EventTime) ||
			g.Seq != r.Seq || g.Kind != r.Kind || g.TTL != r.TTL || !g.Through.Equal(r.Through) ||
			!bytes.Equal(g.Payload, r.Payload) || g.Boot != r.Boot {
			t.Errorf("record %d = %+v, want %+v", i, g, r)
		}
	}
	edges, err := s.Window(context.Background(), host, store.Forward, at(0), at(time.Hour), l1)
	if err != nil || len(edges) != 1 || edges[0].Boot != "" || !bytes.Equal(edges[0].Payload, placement.Payload) {
		t.Errorf("Window = %+v, %v; want the placement without a boot", edges, err)
	}
}

func TestEntityWindowIsHalfOpenSortedAndKeepsEveryRecord(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s,
		podRecord(w, 1, "k8s", 0, lifecycle.Observe, "a"),
		podRecord(w, 2, "k8s", 5*time.Minute, lifecycle.Delete, ""),
		podRecord(w, 3, "k8s", 5*time.Minute, lifecycle.Observe, "b"), // overwrites the delete at the same instant
		podRecord(w, 4, "k8s", 2*time.Minute, lifecycle.Observe, "c"), // arrived late
		podRecord(w, 5, "kubelet", 5*time.Minute, lifecycle.Observe, "d"),
		// An edge record of the same pod is never an entity record.
		placed(w, w.node, 6, "k8s", time.Minute, lifecycle.Observe, 0),
	)
	ctx := context.Background()

	for _, tt := range []struct {
		name     string
		from, to time.Time
		want     []uint64
	}{
		{"everything, by event time then seq", at(0), at(time.Hour), []uint64{1, 4, 2, 3, 5}},
		{"the end is exclusive", at(0), at(5 * time.Minute), []uint64{1, 4}},
		{"the start is inclusive", at(5 * time.Minute), at(time.Hour), []uint64{2, 3, 5}},
		{"one instant", at(5 * time.Minute), at(5*time.Minute + 1), []uint64{2, 3, 5}},
		{"a nanosecond before the first", at(-time.Hour), at(0), nil},
		{"after the last", at(5*time.Minute + 1), at(time.Hour), nil},
		{"from equal to to", at(5 * time.Minute), at(5 * time.Minute), nil},
		{"from after to", at(time.Hour), at(0), nil},
	} {
		got, err := s.EntityWindow(ctx, w.pod, tt.from, tt.to, l2)
		if err != nil {
			t.Fatalf("%s: %v", tt.name, err)
		}
		if !slices.Equal(seqs(got), tt.want) {
			t.Errorf("%s: seqs %v, want %v", tt.name, seqs(got), tt.want)
		}
	}

	// The records keep what they were written with, payloads included.
	got, _ := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), l2)
	if string(got[0].Payload) != "a" || got[0].Subject != store.EntitySubject(w.pod) || got[0].Layer != catalog.L2 {
		t.Errorf("first record = %+v", got[0])
	}
	// A returned record is a copy.
	got[0].Payload[0] = 'z'
	again, _ := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), l2)
	if string(again[0].Payload) != "a" {
		t.Error("mutating an entity window result changed what the store holds")
	}

	// Only the entity's own records: not the edge, in either window.
	edges, err := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), l2)
	if err != nil || !slices.Equal(seqs(edges), []uint64{6}) {
		t.Errorf("Window = %v, %v; want only the edge record 6", seqs(edges), err)
	}
	if nodeRecs, err := s.EntityWindow(ctx, w.node, at(0), at(time.Hour), l2); err != nil || len(nodeRecs) != 0 {
		t.Errorf("EntityWindow of the node = %v, %v; want none, since the node only has an edge", seqs(nodeRecs), err)
	}

	// Another layer sees nothing, and so does a token before the records.
	for _, layer := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L3} {
		if got, err := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), store.Current(layer)); err != nil || len(got) != 0 {
			t.Errorf("EntityWindow in %s = %v, %v; want none", layer, seqs(got), err)
		}
	}
	for _, tt := range []struct {
		asOf uint64
		want []uint64
	}{{0, nil}, {1, []uint64{1}}, {2, []uint64{1, 2}}, {3, []uint64{1, 2, 3}}, {4, []uint64{1, 4, 2, 3}}, {store.Latest, []uint64{1, 4, 2, 3, 5}}} {
		got, err := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), store.Scope{Layer: catalog.L2, AsOf: tt.asOf})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(seqs(got), tt.want) {
			t.Errorf("as of %d: seqs %v, want %v", tt.asOf, seqs(got), tt.want)
		}
	}
}

func TestWriteRules(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	good := placed(w, w.node, 5, "k8s", 0, lifecycle.Observe, 0)
	ctx := context.Background()

	tests := map[string]func(s *memstore.Store) error{
		"repeated seq": func(s *memstore.Store) error { return s.Write(ctx, []store.Record{good, good}) },
		"seq going backwards": func(s *memstore.Store) error {
			_ = s.Write(ctx, []store.Record{good})
			return s.Write(ctx, []store.Record{placed(w, w.node, 4, "k8s", 0, lifecycle.Observe, 0)})
		},
		"before 1970": func(s *memstore.Store) error {
			r := good
			r.EventTime = time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
			return s.Write(ctx, []store.Record{r})
		},
		"after 2262": func(s *memstore.Store) error {
			r := good
			r.EventTime = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
			return s.Write(ctx, []store.Record{r})
		},
		"delete with a TTL": func(s *memstore.Store) error {
			r := good
			r.Kind, r.Payload, r.TTL = lifecycle.Delete, nil, time.Minute
			return s.Write(ctx, []store.Record{r})
		},
		"delete with a payload": func(s *memstore.Store) error {
			r := good
			r.Kind = lifecycle.Delete
			return s.Write(ctx, []store.Record{r})
		},
		"unset kind": func(s *memstore.Store) error {
			r := good
			r.Kind = 0
			return s.Write(ctx, []store.Record{r})
		},
		"unset layer": func(s *memstore.Store) error {
			r := good
			r.Layer = 0
			return s.Write(ctx, []store.Record{r})
		},
		"through before the event time": func(s *memstore.Store) error {
			r := good
			r.Through = r.EventTime.Add(-time.Second)
			return s.Write(ctx, []store.Record{r})
		},
		"empty producer": func(s *memstore.Store) error {
			r := good
			r.Producer = ""
			return s.Write(ctx, []store.Record{r})
		},
		"edge without a relation": func(s *memstore.Store) error {
			r := good
			r.Subject.Relation = ""
			return s.Write(ctx, []store.Record{r})
		},
		"entity with a target": func(s *memstore.Store) error {
			r := good
			r.Subject = store.EntitySubject(w.pod)
			r.Subject.B = w.node
			return s.Write(ctx, []store.Record{r})
		},
		"entity in the wrong layer": func(s *memstore.Store) error {
			r := podRecord(w, 5, "k8s", 0, lifecycle.Observe, "x")
			r.Layer = catalog.L1
			return s.Write(ctx, []store.Record{r})
		},
		"entity of an unknown type": func(s *memstore.Store) error {
			r := podRecord(w, 5, "k8s", 0, lifecycle.Observe, "x")
			r.Subject.A = identity.Fingerprint{}
			return s.Write(ctx, []store.Record{r})
		},
	}
	for name, run := range tests {
		s := open(t)
		if err := run(s); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	t.Run("before the retention horizon", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		if err := s.Retain(ctx, at(time.Hour)); err != nil {
			t.Fatal(err)
		}
		err := s.Write(ctx, []store.Record{good})
		if !errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("err = %v, want ErrBeforeHorizon wrapping ErrInvalid", err)
		}
	})

	t.Run("a batch with one bad record writes nothing", func(t *testing.T) {
		t.Parallel()
		s := open(t)
		bad := good
		bad.Producer = ""
		if err := s.Write(ctx, []store.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0), bad}); err == nil {
			t.Fatal("a bad batch was accepted")
		}
		if got := neighbors(t, s, w.pod, store.Forward, at(time.Minute)); len(got) != 0 {
			t.Errorf("a rejected batch left %v behind", got)
		}
	})
}

// probeAlive is the definition of "alive at t" for an edge, applied straight to
// the records with no use of the lifecycle package: each producer's latest
// record at or before t decides, and the edge lives while any producer's does.
func probeAlive(recs []store.Record, s store.Subject, t time.Time, token uint64) bool {
	latest := map[lifecycle.Producer]store.Record{}
	for _, r := range recs {
		if r.Subject != s || r.EventTime.After(t) || r.Seq > token {
			continue
		}
		if p, ok := latest[r.Producer]; !ok || r.EventTime.After(p.EventTime) || (r.EventTime.Equal(p.EventTime) && r.Seq > p.Seq) {
			latest[r.Producer] = r
		}
	}
	for _, r := range latest {
		end := r.EventTime
		if r.Through.After(end) {
			end = r.Through
		}
		if r.Kind == lifecycle.Observe && (r.TTL == 0 || t.Before(end.Add(r.TTL))) {
			return true
		}
	}
	return false
}

// TestNeighborsMatchAnIndependentProbe checks the store against a second, much
// simpler implementation of the same definition, over a generated stream with
// lateness and flaps.
func TestNeighborsMatchAnIndependentProbe(t *testing.T) {
	t.Parallel()

	cfg := storetest.Tiny()
	cfg.LateProbability = 0.3
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	s := open(t)
	write(t, s, slices.Clone(recs)...)

	subjects := map[store.Subject]bool{}
	layerOf := map[store.Subject]catalog.Layer{}
	for _, r := range recs {
		if r.Subject.Kind == store.SubjectEdge {
			subjects[r.Subject] = true
		}
		layerOf[r.Subject] = r.Layer
	}
	probes := []time.Time{g.Start(), g.Start().Add(7 * time.Minute), g.Start().Add(13 * time.Minute), g.End(), g.End().Add(time.Hour)}
	// Tokens: nothing, a few points in the stream, and everything.
	tokens := []uint64{0, recs[len(recs)/4].Seq, recs[len(recs)/2].Seq, recs[len(recs)-1].Seq - 1, store.Latest}
	read := func(fp identity.Fingerprint, dir store.Direction, at time.Time, sc store.Scope) []store.Neighbor {
		ns, err := s.Neighbors(context.Background(), fp, dir, at, sc)
		if err != nil {
			t.Fatal(err)
		}
		return ns
	}
	for sub := range subjects {
		sc := store.Scope{Layer: layerOf[sub]}
		for _, p := range probes {
			for _, tok := range tokens {
				sc.AsOf = tok
				want := probeAlive(recs, sub, p, tok)
				got := slices.Contains(read(sub.A, store.Forward, p, sc), store.Neighbor{Peer: sub.B, Relation: sub.Relation})
				gotRev := slices.Contains(read(sub.B, store.Reverse, p, sc), store.Neighbor{Peer: sub.A, Relation: sub.Relation})
				if got != want || gotRev != want {
					t.Fatalf("edge %v at %s as of %d: store forward %v reverse %v, probe %v", sub, p.Sub(g.Start()), tok, got, gotRev, want)
				}
			}
		}
	}
}

func TestHorizonOnlyMovesForward(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	ctx := context.Background()
	_ = s.Retain(ctx, at(2*time.Hour))
	_ = s.Retain(ctx, at(time.Hour)) // an earlier horizon does not bring the history back
	err := s.Write(ctx, []store.Record{placed(w, w.node, 1, "k8s", 90*time.Minute, lifecycle.Observe, 0)})
	if !errors.Is(err, store.ErrBeforeHorizon) || !errors.Is(err, store.ErrInvalid) {
		t.Errorf("err = %v, want ErrBeforeHorizon wrapping ErrInvalid", err)
	}
	if err := s.Write(ctx, []store.Record{placed(w, w.node, 2, "k8s", 2*time.Hour, lifecycle.Observe, 0)}); err != nil {
		t.Errorf("a record exactly at the horizon was refused: %v", err)
	}
}

func TestATokenSeesOnlyTheRecordsAtOrBelowIt(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s,
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 10*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 10*time.Minute, lifecycle.Observe, 0),
		// Written last, but about an earlier time: only a token of 4 or more sees it.
		placed(w, w.node2, 4, "k8s", 2*time.Minute, lifecycle.Observe, 0),
	)
	ctx := context.Background()
	peers := func(token uint64, when time.Duration) []identity.Fingerprint {
		ns, err := s.Neighbors(ctx, w.pod, store.Forward, at(when), store.Scope{Layer: catalog.L2, AsOf: token})
		if err != nil {
			t.Fatal(err)
		}
		out := []identity.Fingerprint{}
		for _, n := range ns {
			out = append(out, n.Peer)
		}
		return out
	}
	for _, tt := range []struct {
		token uint64
		when  time.Duration
		want  []identity.Fingerprint
	}{
		{0, 5 * time.Minute, nil},                             // nothing is visible
		{1, 5 * time.Minute, []identity.Fingerprint{w.node}},  // the first record only
		{1, 11 * time.Minute, []identity.Fingerprint{w.node}}, // the delete is not yet visible
		{2, 11 * time.Minute, nil},
		{3, 11 * time.Minute, []identity.Fingerprint{w.node2}},
		{3, 5 * time.Minute, []identity.Fingerprint{w.node}}, // the late record is not visible at 3
		{4, 5 * time.Minute, []identity.Fingerprint{w.node, w.node2}},
		{store.Latest, 5 * time.Minute, []identity.Fingerprint{w.node, w.node2}},
	} {
		got, want := peers(tt.token, tt.when), slices.Clone(tt.want)
		slices.SortFunc(want, store.CompareFingerprints) // the store orders by fingerprint
		if want == nil {
			want = []identity.Fingerprint{}
		}
		if !slices.Equal(got, want) {
			t.Errorf("as of %d at %s: neighbors %v, want %v", tt.token, tt.when, got, want)
		}
	}

	// Window honors the token too, and keeps the overwritten record visible to a
	// token that predates its overwriter.
	same := open(t)
	write(t, same,
		placed(w, w.node, 5, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 6, "k8s", 0, lifecycle.Delete, 0), // same instant, same producer
	)
	for _, tt := range []struct {
		token uint64
		want  []uint64
	}{{4, nil}, {5, []uint64{5}}, {6, []uint64{5, 6}}, {store.Latest, []uint64{5, 6}}} {
		got, err := same.Window(ctx, w.pod, store.Forward, at(0), at(time.Second), store.Scope{Layer: catalog.L2, AsOf: tt.token})
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(seqs(got), tt.want) {
			t.Errorf("window as of %d has seqs %v, want %v", tt.token, seqs(got), tt.want)
		}
	}
	// And the overwriter decides existence only for a token that sees it.
	if alive, _ := same.Alive(ctx, w.pod, at(time.Second), store.Scope{Layer: catalog.L2, AsOf: 6}); alive {
		t.Error("an entity with no records is alive")
	}
	if ns, _ := same.Neighbors(ctx, w.pod, store.Forward, at(time.Second), store.Scope{Layer: catalog.L2, AsOf: 5}); len(ns) != 1 {
		t.Errorf("a token before the same-instant delete sees %v, want the edge", ns)
	}
	if ns, _ := same.Neighbors(ctx, w.pod, store.Forward, at(time.Second), store.Scope{Layer: catalog.L2, AsOf: 6}); len(ns) != 0 {
		t.Errorf("a token at the same-instant delete sees %v, want nothing", ns)
	}
}

func TestReadsAreScopedToALayer(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	ctx := context.Background()
	edge := placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0)
	edge.Layer = catalog.L1
	write(t, s, edge)
	for _, tt := range []struct {
		layer catalog.Layer
		want  int
	}{{catalog.L0, 0}, {catalog.L1, 1}, {catalog.L2, 0}, {catalog.L3, 0}} {
		sc := store.Current(tt.layer)
		if ns, err := s.Neighbors(ctx, w.pod, store.Forward, at(time.Minute), sc); err != nil || len(ns) != tt.want {
			t.Errorf("Neighbors in %s = %v, %v; want %d", tt.layer, ns, err, tt.want)
		}
		if rev, err := s.Neighbors(ctx, w.node, store.Reverse, at(time.Minute), sc); err != nil || len(rev) != tt.want {
			t.Errorf("reverse Neighbors in %s = %v, %v; want %d", tt.layer, rev, err, tt.want)
		}
		if ws, err := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), sc); err != nil || len(ws) != tt.want {
			t.Errorf("Window in %s = %d records, %v; want %d", tt.layer, len(ws), err, tt.want)
		}
	}
	if got := s.Layers(w.pod); !slices.Equal(got, []catalog.Layer{catalog.L1}) {
		t.Errorf("Layers(pod) = %v, want [L1]", got)
	}
	// An entity that is only the target of an edge is in that edge's layer too.
	if got := s.Layers(w.node); !slices.Equal(got, []catalog.Layer{catalog.L1}) {
		t.Errorf("Layers(node) = %v, want [L1]", got)
	}
	if got := s.Layers(w.node2); len(got) != 0 {
		t.Errorf("Layers of an unknown entity = %v, want none", got)
	}
	// An entity in two layers lists both, ascending.
	write(t, s, podRecord(w, 2, "k8s", 0, lifecycle.Observe, "x"))
	if got := s.Layers(w.pod); !slices.Equal(got, []catalog.Layer{catalog.L1, catalog.L2}) {
		t.Errorf("Layers(pod) = %v, want [L1 L2]", got)
	}

	for _, sc := range []store.Scope{{}, {Layer: catalog.L3 + 1, AsOf: store.Latest}} {
		if _, err := s.Neighbors(ctx, w.pod, store.Forward, at(0), sc); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Neighbors with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := s.NeighborsBatch(ctx, nil, store.Forward, at(0), sc); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("NeighborsBatch with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := s.Alive(ctx, w.pod, at(0), sc); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Alive with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := s.Window(ctx, w.pod, store.Forward, at(0), at(time.Hour), sc); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("Window with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := s.EntityWindow(ctx, w.pod, at(0), at(time.Hour), sc); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("EntityWindow with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
	}
}

func TestASubjectKeepsItsLayer(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	moved := placed(w, w.node, 2, "k8s", time.Minute, lifecycle.Observe, 0)
	moved.Layer = catalog.L1
	ctx := context.Background()

	s := open(t)
	write(t, s, placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0))
	if err := s.Write(ctx, []store.Record{moved}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a record changing its subject's layer in a later batch: err = %v, want ErrInvalid", err)
	}
	// The same rule inside one batch, and the refused batch stores nothing.
	s = open(t)
	if err := s.Write(ctx, []store.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0), moved}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a record changing its subject's layer within a batch: err = %v, want ErrInvalid", err)
	}
	if s.LastSeq() != 0 {
		t.Errorf("a refused batch moved LastSeq to %d", s.LastSeq())
	}
}

func TestLastSeqFollowsCommittedBatches(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	if s.LastSeq() != 0 {
		t.Fatalf("a new store has LastSeq %d", s.LastSeq())
	}
	write(t, s, placed(w, w.node, 3, "k8s", 0, lifecycle.Observe, 0), placed(w, w.node, 7, "k8s", time.Minute, lifecycle.Observe, 0))
	if s.LastSeq() != 7 {
		t.Errorf("LastSeq = %d, want 7", s.LastSeq())
	}
	bad := placed(w, w.node, 9, "k8s", 0, lifecycle.Observe, 0)
	bad.Producer = ""
	if err := s.Write(context.Background(), []store.Record{placed(w, w.node, 8, "k8s", 0, lifecycle.Observe, 0), bad}); err == nil {
		t.Fatal("a bad batch was accepted")
	}
	if s.LastSeq() != 7 {
		t.Errorf("a refused batch moved LastSeq to %d", s.LastSeq())
	}
}

func TestNeighborsBatchAnswersEachFingerprintInOrder(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	write(t, s,
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node2, 2, "k8s", 0, lifecycle.Observe, 0),
	)
	stranger := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "never-written")
	fps := []identity.Fingerprint{w.pod, stranger, w.pod, w.node}
	got, err := s.NeighborsBatch(context.Background(), fps, store.Forward, at(time.Minute), l2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(fps) {
		t.Fatalf("%d answers for %d fingerprints", len(got), len(fps))
	}
	for i, f := range fps {
		want := neighbors(t, s, f, store.Forward, at(time.Minute))
		if !slices.Equal(got[i], want) {
			t.Errorf("answer %d for %s = %v, want %v", i, f, got[i], want)
		}
	}
	if len(got[0]) != 2 || len(got[1]) != 0 || len(got[3]) != 0 || !slices.Equal(got[0], got[2]) {
		t.Errorf("answers = %v", got)
	}
	if empty, err := s.NeighborsBatch(context.Background(), nil, store.Forward, at(0), l2); err != nil || len(empty) != 0 {
		t.Errorf("an empty batch answered %v, %v", empty, err)
	}
}

func TestFoldCacheStaysCorrect(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	s := open(t)
	var recs []store.Record
	for i := range 3 * 32 { // more distinct tokens than the cache keeps
		kind := lifecycle.Observe
		if i%2 == 1 {
			kind = lifecycle.Delete
		}
		recs = append(recs, placed(w, w.node, uint64(i+1), "k8s", time.Duration(i)*time.Second, kind, 0))
	}
	// Read as of every token while the stream is still being written, then again
	// afterwards: a fold cached at n records must stay right once more arrive.
	check := func(upTo int) {
		for tok := 0; tok <= upTo; tok++ {
			want := tok%2 == 1 // after an odd number of records the last one is an Observe
			ns, err := s.Neighbors(context.Background(), w.pod, store.Forward, at(time.Hour), store.Scope{Layer: catalog.L2, AsOf: uint64(tok)})
			if err != nil {
				t.Fatal(err)
			}
			if (len(ns) == 1) != want {
				t.Fatalf("as of %d: %d neighbors, want alive=%v", tok, len(ns), want)
			}
		}
	}
	for i := 0; i < len(recs); i += 8 {
		write(t, s, recs[i:i+8]...)
		check(i + 8)
	}
	check(len(recs))
}
