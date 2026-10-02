package oracle_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var base = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

func at(d time.Duration) time.Time { return base.Add(d) }

// l2 is the scope of the layer the test records are in.
var l2 = engine.Current(catalog.L2)

func fp(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
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

func newTopology(t *testing.T) topology {
	return topology{
		pod:   fp(t, catalog.K8sPod, catalog.K8sPodUID, "p1"),
		node:  fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n1"),
		node2: fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n2"),
	}
}

func placed(w topology, node identity.Fingerprint, seq uint64, p lifecycle.Producer, t time.Duration, kind lifecycle.Kind, ttl time.Duration) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(w.pod, node, catalog.ScheduledOn),
		Producer: p, EventTime: at(t), Seq: seq, Kind: kind, TTL: ttl,
	}
	if kind == lifecycle.Observe {
		r.Payload = []byte{byte(seq)}
	}
	return r
}

func neighbors(t *testing.T, o *oracle.Oracle, fp identity.Fingerprint, dir engine.Direction, at time.Time) []engine.Neighbor {
	t.Helper()
	ns, err := o.Neighbors(fp, dir, at, l2)
	if err != nil {
		t.Fatal(err)
	}
	return ns
}

func TestNeighborsAreSymmetricAndFollowTheLifecycle(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	if err := o.Write([]engine.Record{
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 10*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 10*time.Minute, lifecycle.Observe, 0),
	}); err != nil {
		t.Fatal(err)
	}

	want := func(peer identity.Fingerprint) []engine.Neighbor {
		return []engine.Neighbor{{Peer: peer, Relation: catalog.ScheduledOn}}
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
		got := neighbors(t, o, w.pod, engine.Forward, at(tt.when))
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
		if rev := neighbors(t, o, tt.node, engine.Reverse, at(tt.when)); !slices.Equal(rev, want(w.pod)) {
			t.Errorf("%s: node's reverse neighbors = %v, want the pod", tt.name, rev)
		}
	}
	// The node the pod left has nothing pointing at it any more.
	if rev := neighbors(t, o, w.node, engine.Reverse, at(time.Hour)); len(rev) != 0 {
		t.Errorf("the old node still has reverse neighbors %v", rev)
	}
}

func TestExpiryAndLateWrites(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	if err := o.Write([]engine.Record{placed(w, w.node, 1, "node-collector", 0, lifecycle.Observe, 5*time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if got := neighbors(t, o, w.pod, engine.Forward, at(6*time.Minute)); len(got) != 0 {
		t.Fatalf("the edge outlived its TTL: %v", got)
	}

	// A refresh that arrives late, with an event time before the deadline,
	// revives it for the time it covers. The cached fold must not be reused.
	if err := o.Write([]engine.Record{placed(w, w.node, 2, "node-collector", 4*time.Minute, lifecycle.Observe, 5*time.Minute)}); err != nil {
		t.Fatal(err)
	}
	if got := neighbors(t, o, w.pod, engine.Forward, at(6*time.Minute)); len(got) != 1 {
		t.Errorf("a late refresh did not fill the gap: %v", got)
	}
	if got := neighbors(t, o, w.pod, engine.Forward, at(10*time.Minute)); len(got) != 0 {
		t.Errorf("the refresh outlived its own TTL: %v", got)
	}
}

func TestEntityExistence(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	rec := func(seq uint64, kind lifecycle.Kind, t time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(w.pod), Producer: "k8s", EventTime: at(t), Seq: seq, Kind: kind}
	}
	if err := o.Write([]engine.Record{rec(1, lifecycle.Observe, 0), rec(2, lifecycle.Delete, time.Minute)}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		when time.Duration
		want bool
	}{{-1, false}, {0, true}, {time.Minute - 1, true}, {time.Minute, false}} {
		if got, err := o.Alive(w.pod, at(tt.when), l2); err != nil || got != tt.want {
			t.Errorf("Alive at %s = %v, %v; want %v", tt.when, got, err, tt.want)
		}
	}
}

func TestWindowIsHalfOpenSortedAndPerDirection(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	recs := []engine.Record{
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 5*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 5*time.Minute, lifecycle.Observe, 0),
		placed(w, w.node2, 4, "k8s", 2*time.Minute, lifecycle.Observe, time.Hour), // arrived late
	}
	if err := o.Write(recs); err != nil {
		t.Fatal(err)
	}

	got, err := o.Window(w.pod, engine.Forward, at(0), at(5*time.Minute), l2)
	if err != nil {
		t.Fatal(err)
	}
	var seqs []uint64
	for _, r := range got {
		seqs = append(seqs, r.Seq)
	}
	if !slices.Equal(seqs, []uint64{1, 4}) { // by event time, and the end is exclusive
		t.Errorf("window seqs = %v, want [1 4]", seqs)
	}
	if rev, _ := o.Window(w.node, engine.Reverse, at(0), at(time.Hour), l2); len(rev) != 2 {
		t.Errorf("the old node's reverse window has %d records, want 2", len(rev))
	}
	if fwd, _ := o.Window(w.node, engine.Forward, at(0), at(time.Hour), l2); len(fwd) != 0 {
		t.Errorf("a node has %d forward records for an edge it is the target of", len(fwd))
	}

	// A returned record is a copy.
	got[0].Payload[0] = 0xff
	again, _ := o.Window(w.pod, engine.Forward, at(0), at(5*time.Minute), l2)
	if again[0].Payload[0] == 0xff {
		t.Error("mutating a window result changed what the oracle holds")
	}
}

func TestWriteRules(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	good := placed(w, w.node, 5, "k8s", 0, lifecycle.Observe, 0)

	tests := map[string]func(o *oracle.Oracle) error{
		"repeated seq": func(o *oracle.Oracle) error { return o.Write([]engine.Record{good, good}) },
		"seq going backwards": func(o *oracle.Oracle) error {
			_ = o.Write([]engine.Record{good})
			return o.Write([]engine.Record{placed(w, w.node, 4, "k8s", 0, lifecycle.Observe, 0)})
		},
		"before 1970": func(o *oracle.Oracle) error {
			r := good
			r.EventTime = time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC)
			return o.Write([]engine.Record{r})
		},
		"after 2262": func(o *oracle.Oracle) error {
			r := good
			r.EventTime = time.Date(2300, 1, 1, 0, 0, 0, 0, time.UTC)
			return o.Write([]engine.Record{r})
		},
		"delete with a TTL": func(o *oracle.Oracle) error {
			r := good
			r.Kind, r.Payload, r.TTL = lifecycle.Delete, nil, time.Minute
			return o.Write([]engine.Record{r})
		},
		"delete with a payload": func(o *oracle.Oracle) error {
			r := good
			r.Kind = lifecycle.Delete
			return o.Write([]engine.Record{r})
		},
		"unset kind": func(o *oracle.Oracle) error {
			r := good
			r.Kind = 0
			return o.Write([]engine.Record{r})
		},
		"unset layer": func(o *oracle.Oracle) error {
			r := good
			r.Layer = 0
			return o.Write([]engine.Record{r})
		},
		"through before the event time": func(o *oracle.Oracle) error {
			r := good
			r.Through = r.EventTime.Add(-time.Second)
			return o.Write([]engine.Record{r})
		},
		"before the retention horizon": func(o *oracle.Oracle) error {
			_ = o.Retain(at(time.Hour))
			err := o.Write([]engine.Record{good})
			if !errors.Is(err, engine.ErrBeforeHorizon) {
				return errors.New("expected ErrBeforeHorizon, got: " + err.Error())
			}
			return err
		},
		"empty producer": func(o *oracle.Oracle) error {
			r := good
			r.Producer = ""
			return o.Write([]engine.Record{r})
		},
		"edge without a relation": func(o *oracle.Oracle) error {
			r := good
			r.Subject.Relation = ""
			return o.Write([]engine.Record{r})
		},
		"entity with a target": func(o *oracle.Oracle) error {
			r := good
			r.Subject = engine.EntitySubject(w.pod)
			r.Subject.B = w.node
			return o.Write([]engine.Record{r})
		},
	}
	for name, run := range tests {
		if err := run(oracle.New()); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	// A batch with one bad record writes nothing.
	o := oracle.New()
	bad := good
	bad.Producer = ""
	if err := o.Write([]engine.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0), bad}); err == nil {
		t.Fatal("a bad batch was accepted")
	}
	if got := neighbors(t, o, w.pod, engine.Forward, at(time.Minute)); len(got) != 0 {
		t.Errorf("a rejected batch left %v behind", got)
	}
}

// probe is the definition of "alive at t" for an edge, applied straight to the
// records with no use of the lifecycle package: each producer's latest record
// at or before t decides, and the edge lives while any producer's does.
func probeAlive(recs []engine.Record, s engine.Subject, t time.Time, token uint64) bool {
	latest := map[lifecycle.Producer]engine.Record{}
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

// TestNeighborsMatchAnIndependentProbe checks the oracle against a second, much
// simpler implementation of the same definition, over a generated workload
// with lateness, flaps and outages.
func TestNeighborsMatchAnIndependentProbe(t *testing.T) {
	t.Parallel()

	cfg := workload.Tiny()
	cfg.LateProbability, cfg.OutageProbability = 0.3, 0.1
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	o := oracle.New()
	if err := o.Write(slices.Clone(recs)); err != nil {
		t.Fatal(err)
	}

	subjects := map[engine.Subject]bool{}
	for _, r := range recs {
		if r.Subject.Kind == engine.SubjectEdge {
			subjects[r.Subject] = true
		}
	}
	probes := []time.Time{g.Start(), g.Start().Add(7 * time.Minute), g.Start().Add(13 * time.Minute), g.End(), g.End().Add(time.Hour)}
	// Tokens: nothing, a few points in the stream, and everything.
	tokens := []uint64{0, recs[len(recs)/4].Seq, recs[len(recs)/2].Seq, recs[len(recs)-1].Seq - 1, engine.Latest}
	layerOf := map[engine.Subject]catalog.Layer{}
	for _, r := range recs {
		layerOf[r.Subject] = r.Layer
	}
	read := func(fp identity.Fingerprint, dir engine.Direction, at time.Time, sc engine.Scope) []engine.Neighbor {
		ns, err := o.Neighbors(fp, dir, at, sc)
		if err != nil {
			t.Fatal(err)
		}
		return ns
	}
	for s := range subjects {
		sc := engine.Scope{Layer: layerOf[s]}
		for _, p := range probes {
			for _, tok := range tokens {
				sc.AsOf = tok
				want := probeAlive(recs, s, p, tok)
				got := slices.Contains(read(s.A, engine.Forward, p, sc), engine.Neighbor{Peer: s.B, Relation: s.Relation})
				gotRev := slices.Contains(read(s.B, engine.Reverse, p, sc), engine.Neighbor{Peer: s.A, Relation: s.Relation})
				if got != want || gotRev != want {
					t.Fatalf("edge %v at %s as of %d: oracle forward %v reverse %v, probe %v", s, p.Sub(g.Start()), tok, got, gotRev, want)
				}
			}
		}
	}
}

func TestSizeGrows(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	before, _ := o.Size()
	_ = o.Write([]engine.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0)})
	after, _ := o.Size()
	if after <= before {
		t.Errorf("Size() = %d then %d after a write", before, after)
	}
	if err := o.Retain(at(time.Hour)); err != nil {
		t.Error(err)
	}
	if err := o.Close(); err != nil {
		t.Error(err)
	}
}

func TestHorizonOnlyMovesForward(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	_ = o.Retain(at(2 * time.Hour))
	_ = o.Retain(at(time.Hour)) // an earlier horizon does not bring the history back
	err := o.Write([]engine.Record{placed(w, w.node, 1, "k8s", 90*time.Minute, lifecycle.Observe, 0)})
	if !errors.Is(err, engine.ErrBeforeHorizon) || !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("err = %v, want ErrBeforeHorizon wrapping ErrInvalid", err)
	}
	if err := o.Write([]engine.Record{placed(w, w.node, 2, "k8s", 2*time.Hour, lifecycle.Observe, 0)}); err != nil {
		t.Errorf("a record exactly at the horizon was refused: %v", err)
	}
}

func TestATokenSeesOnlyTheRecordsAtOrBelowIt(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	if err := o.Write([]engine.Record{
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 2, "k8s", 10*time.Minute, lifecycle.Delete, 0),
		placed(w, w.node2, 3, "k8s", 10*time.Minute, lifecycle.Observe, 0),
		// Written last, but about an earlier time: only a token of 4 or more sees it.
		placed(w, w.node2, 4, "k8s", 2*time.Minute, lifecycle.Observe, 0),
	}); err != nil {
		t.Fatal(err)
	}
	peers := func(token uint64, when time.Duration) []identity.Fingerprint {
		ns, err := o.Neighbors(w.pod, engine.Forward, at(when), engine.Scope{Layer: catalog.L2, AsOf: token})
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
		{engine.Latest, 5 * time.Minute, []identity.Fingerprint{w.node, w.node2}},
	} {
		got, want := peers(tt.token, tt.when), slices.Clone(tt.want)
		slices.SortFunc(want, engine.CompareFingerprints) // the oracle orders by hash
		if !slices.Equal(got, want) {
			t.Errorf("as of %d at %s: neighbors %v, want %v", tt.token, tt.when, got, want)
		}
	}

	// Window honors the token too, and keeps the overwritten record visible to a
	// token that predates its overwriter.
	same := oracle.New()
	if err := same.Write([]engine.Record{
		placed(w, w.node, 5, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node, 6, "k8s", 0, lifecycle.Delete, 0), // same instant, same producer
	}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		token uint64
		want  []uint64
	}{{4, nil}, {5, []uint64{5}}, {6, []uint64{5, 6}}, {engine.Latest, []uint64{5, 6}}} {
		got, err := same.Window(w.pod, engine.Forward, at(0), at(time.Second), engine.Scope{Layer: catalog.L2, AsOf: tt.token})
		if err != nil {
			t.Fatal(err)
		}
		var seqs []uint64
		for _, r := range got {
			seqs = append(seqs, r.Seq)
		}
		if !slices.Equal(seqs, tt.want) {
			t.Errorf("window as of %d has seqs %v, want %v", tt.token, seqs, tt.want)
		}
	}
	// And the overwriter decides existence only for a token that sees it.
	if alive, _ := same.Alive(w.pod, at(time.Second), engine.Scope{Layer: catalog.L2, AsOf: 6}); alive {
		t.Error("an entity with no records is alive")
	}
	if ns, _ := same.Neighbors(w.pod, engine.Forward, at(time.Second), engine.Scope{Layer: catalog.L2, AsOf: 5}); len(ns) != 1 {
		t.Errorf("a token before the same-instant delete sees %v, want the edge", ns)
	}
	if ns, _ := same.Neighbors(w.pod, engine.Forward, at(time.Second), engine.Scope{Layer: catalog.L2, AsOf: 6}); len(ns) != 0 {
		t.Errorf("a token at the same-instant delete sees %v, want nothing", ns)
	}
}

func TestReadsAreScopedToALayer(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	edge := placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0)
	edge.Layer = catalog.L1
	if err := o.Write([]engine.Record{edge}); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		layer catalog.Layer
		want  int
	}{{catalog.L0, 0}, {catalog.L1, 1}, {catalog.L2, 0}, {catalog.L3, 0}} {
		sc := engine.Current(tt.layer)
		if ns, err := o.Neighbors(w.pod, engine.Forward, at(time.Minute), sc); err != nil || len(ns) != tt.want {
			t.Errorf("Neighbors in %s = %v, %v; want %d", tt.layer, ns, err, tt.want)
		}
		if rev, err := o.Neighbors(w.node, engine.Reverse, at(time.Minute), sc); err != nil || len(rev) != tt.want {
			t.Errorf("reverse Neighbors in %s = %v, %v; want %d", tt.layer, rev, err, tt.want)
		}
		if ws, err := o.Window(w.pod, engine.Forward, at(0), at(time.Hour), sc); err != nil || len(ws) != tt.want {
			t.Errorf("Window in %s = %d records, %v; want %d", tt.layer, len(ws), err, tt.want)
		}
	}
	if got := o.Layers(w.pod); !slices.Equal(got, []catalog.Layer{catalog.L1}) {
		t.Errorf("Layers(pod) = %v, want [L1]", got)
	}
	// An entity that is only the target of an edge is in that edge's layer too.
	if got := o.Layers(w.node); !slices.Equal(got, []catalog.Layer{catalog.L1}) {
		t.Errorf("Layers(node) = %v, want [L1]", got)
	}
	if got := o.Layers(w.node2); len(got) != 0 {
		t.Errorf("Layers of an unknown entity = %v, want none", got)
	}

	for _, sc := range []engine.Scope{{}, {Layer: catalog.L3 + 1, AsOf: engine.Latest}} {
		if _, err := o.Neighbors(w.pod, engine.Forward, at(0), sc); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("Neighbors with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := o.NeighborsBatch(nil, engine.Forward, at(0), sc); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("NeighborsBatch with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := o.Alive(w.pod, at(0), sc); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("Alive with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
		if _, err := o.Window(w.pod, engine.Forward, at(0), at(time.Hour), sc); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("Window with scope %+v: err = %v, want ErrInvalid", sc, err)
		}
	}
}

func TestASubjectKeepsItsLayer(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	moved := placed(w, w.node, 2, "k8s", time.Minute, lifecycle.Observe, 0)
	moved.Layer = catalog.L1

	o := oracle.New()
	if err := o.Write([]engine.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := o.Write([]engine.Record{moved}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a record changing its subject's layer in a later batch: err = %v, want ErrInvalid", err)
	}
	// The same rule inside one batch, and the refused batch stores nothing.
	o = oracle.New()
	if err := o.Write([]engine.Record{placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0), moved}); !errors.Is(err, engine.ErrInvalid) {
		t.Errorf("a record changing its subject's layer within a batch: err = %v, want ErrInvalid", err)
	}
	if o.LastSeq() != 0 {
		t.Errorf("a refused batch moved LastSeq to %d", o.LastSeq())
	}
}

func TestLastSeqFollowsCommittedBatches(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	if o.LastSeq() != 0 {
		t.Fatalf("a new oracle has LastSeq %d", o.LastSeq())
	}
	if err := o.Write([]engine.Record{placed(w, w.node, 3, "k8s", 0, lifecycle.Observe, 0), placed(w, w.node, 7, "k8s", time.Minute, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if o.LastSeq() != 7 {
		t.Errorf("LastSeq = %d, want 7", o.LastSeq())
	}
	bad := placed(w, w.node, 9, "k8s", 0, lifecycle.Observe, 0)
	bad.Producer = ""
	if err := o.Write([]engine.Record{placed(w, w.node, 8, "k8s", 0, lifecycle.Observe, 0), bad}); err == nil {
		t.Fatal("a bad batch was accepted")
	}
	if o.LastSeq() != 7 {
		t.Errorf("a refused batch moved LastSeq to %d", o.LastSeq())
	}
}

func TestNeighborsBatchAnswersEachFingerprintInOrder(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	if err := o.Write([]engine.Record{
		placed(w, w.node, 1, "k8s", 0, lifecycle.Observe, 0),
		placed(w, w.node2, 2, "k8s", 0, lifecycle.Observe, 0),
	}); err != nil {
		t.Fatal(err)
	}
	stranger := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "never-written")
	fps := []identity.Fingerprint{w.pod, stranger, w.pod, w.node}
	got, err := o.NeighborsBatch(fps, engine.Forward, at(time.Minute), l2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != len(fps) {
		t.Fatalf("%d answers for %d fingerprints", len(got), len(fps))
	}
	for i, f := range fps {
		want := neighbors(t, o, f, engine.Forward, at(time.Minute))
		if !slices.Equal(got[i], want) {
			t.Errorf("answer %d for %s = %v, want %v", i, f, got[i], want)
		}
	}
	if len(got[0]) != 2 || len(got[1]) != 0 || len(got[3]) != 0 {
		t.Errorf("answers = %v", got)
	}
	if empty, err := o.NeighborsBatch(nil, engine.Forward, at(0), l2); err != nil || len(empty) != 0 {
		t.Errorf("an empty batch answered %v, %v", empty, err)
	}
}

func TestFoldCacheStaysCorrectAndBounded(t *testing.T) {
	t.Parallel()
	w := newTopology(t)
	o := oracle.New()
	var recs []engine.Record
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
			ns, err := o.Neighbors(w.pod, engine.Forward, at(time.Hour), engine.Scope{Layer: catalog.L2, AsOf: uint64(tok)})
			if err != nil {
				t.Fatal(err)
			}
			if (len(ns) == 1) != want {
				t.Fatalf("as of %d: %d neighbors, want alive=%v", tok, len(ns), want)
			}
		}
	}
	for i := 0; i < len(recs); i += 8 {
		if err := o.Write(recs[i : i+8]); err != nil {
			t.Fatal(err)
		}
		check(i + 8)
	}
	check(len(recs))
}
