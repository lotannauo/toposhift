package memstore_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// model is a second implementation of the store, as simple as it can be: it keeps
// the accepted records and answers every read by brute force. It shares nothing
// with the store under test except lifecycle.Fold and the helpers of package
// store.
type model struct {
	policy     lifecycle.Policy
	bySubject  map[store.Subject][]store.Record // accepted records, ascending Seq
	layers     map[store.Subject]catalog.Layer
	lastSeq    uint64
	hasHorizon bool
	hzTime     time.Time
	hzSeq      uint64
}

func newModel(p lifecycle.Policy) *model {
	return &model{policy: p, bySubject: map[store.Subject][]store.Record{}, layers: map[store.Subject]catalog.Layer{}}
}

// entityLayers are the layers of the entity types of the universe.
var entityLayers = map[catalog.EntityType]catalog.Layer{
	catalog.K8sPod: catalog.L2, catalog.K8sNode: catalog.L2, catalog.Host: catalog.L1,
	catalog.Rack: catalog.L0, catalog.Container: catalog.L2, catalog.Service: catalog.L3,
}

// judge says what the contract makes of a batch: nil if it is accepted, else
// store.ErrBeforeHorizon or store.ErrInvalid, whichever the first offending
// record, checked in the order of the contract, gives.
func (m *model) judge(batch []store.Record) error {
	prev := m.lastSeq
	inBatch := map[store.Subject]catalog.Layer{}
	for _, r := range batch {
		if !m.wellFormed(r) {
			return store.ErrInvalid
		}
		if r.Seq <= prev || r.Seq == store.Latest {
			return store.ErrInvalid
		}
		prev = r.Seq
		if m.hasHorizon && r.EventTime.Before(m.hzTime) {
			return store.ErrBeforeHorizon
		}
		known, ok := m.layers[r.Subject]
		if !ok {
			known, ok = inBatch[r.Subject]
		}
		if ok && known != r.Layer {
			return store.ErrInvalid
		}
		inBatch[r.Subject] = r.Layer
	}
	return nil
}

// wellFormed is the rules of a single record, as the contract states them.
func (m *model) wellFormed(r store.Record) bool {
	if r.Producer == "" || r.Layer < catalog.L0 || r.Layer > catalog.L3 {
		return false
	}
	if r.EventTime.Before(store.MinEventTime) || r.EventTime.After(store.MaxEventTime) {
		return false
	}
	if r.Subject.Kind == store.SubjectEntity && entityLayers[r.Subject.A.Type()] != r.Layer {
		return false
	}
	if r.Through.After(store.MaxEventTime) {
		return false
	}
	// A boot id belongs to the observation of a host, and is not blank.
	isHost := r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Host
	if r.Boot != "" && (r.Kind != lifecycle.Observe || !isHost || strings.TrimSpace(r.Boot) == "") {
		return false
	}
	switch r.Kind {
	case lifecycle.Observe:
		last := r.EventTime
		if r.Through.After(last) {
			last = r.Through
		}
		// The deadline, the last observation plus the TTL, must be representable too.
		if r.TTL > 0 && last.Add(r.TTL).After(store.MaxEventTime) {
			return false
		}
		return r.TTL >= 0 && (r.Through.IsZero() || !r.Through.Before(r.EventTime))
	case lifecycle.Delete:
		return r.TTL == 0 && r.Through.IsZero() && len(r.Payload) == 0
	}
	return false
}

func (m *model) accept(batch []store.Record) {
	for _, r := range batch {
		r.Payload = slices.Clone(r.Payload)
		m.bySubject[r.Subject] = append(m.bySubject[r.Subject], r)
		m.layers[r.Subject] = r.Layer
		m.lastSeq = r.Seq
	}
}

func (m *model) retain(h time.Time) {
	if h.After(m.hzTime) { // with no horizon yet, hzTime is the zero time
		m.hasHorizon, m.hzTime, m.hzSeq = true, h, m.lastSeq
	}
}

// refusal is the error class a read must give for the arguments, before
// anything is read, or nil. t is the instant, or a window's from.
func (m *model) refusal(sc store.Scope, dirOK bool, fps []identity.Fingerprint, t time.Time) error {
	if sc.Layer < catalog.L0 || sc.Layer > catalog.L3 || !dirOK {
		return store.ErrInvalid
	}
	for _, fp := range fps {
		if fp == (identity.Fingerprint{}) {
			return store.ErrInvalid
		}
	}
	if m.hasHorizon && (t.Before(m.hzTime) || sc.AsOf < m.hzSeq) {
		return store.ErrBeforeHorizon
	}
	return nil
}

// visible are a subject's accepted records that the token sees.
func (m *model) visible(sub store.Subject, asOf uint64) []store.Record {
	var out []store.Record
	for _, r := range m.bySubject[sub] {
		if r.Seq <= asOf {
			out = append(out, r)
		}
	}
	return out
}

func fold(recs []store.Record, p lifecycle.Policy) (lifecycle.Timeline, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	return lifecycle.Fold(as, p)
}

// alive is Alive by brute force: whether the entity exists, and the collision if
// its existence cannot be folded.
func (m *model) alive(fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, *lifecycle.CloneCollisionError, error) {
	sub := store.EntitySubject(fp)
	if m.layers[sub] != sc.Layer {
		return false, nil, nil
	}
	tl, err := fold(m.visible(sub, sc.AsOf), m.policy)
	var cc *lifecycle.CloneCollisionError
	if errors.As(err, &cc) {
		return false, cc, nil
	}
	if err != nil {
		return false, nil, err
	}
	return tl.AliveAt(t), nil, nil
}

// incident are the edge subjects in the scope's layer that touch fp in the direction.
func (m *model) incident(fp identity.Fingerprint, dir store.Direction, sc store.Scope) []store.Subject {
	var out []store.Subject
	for sub, layer := range m.layers {
		if sub.Kind != store.SubjectEdge || layer != sc.Layer {
			continue
		}
		if (dir == store.Forward && sub.A == fp) || (dir == store.Reverse && sub.B == fp) {
			out = append(out, sub)
		}
	}
	return out
}

func (m *model) neighbors(fp identity.Fingerprint, dir store.Direction, t time.Time, sc store.Scope) ([]store.Neighbor, error) {
	var out []store.Neighbor
	for _, sub := range m.incident(fp, dir, sc) {
		tl, err := fold(m.visible(sub, sc.AsOf), lifecycle.Policy{})
		if err != nil {
			return nil, err
		}
		if tl.AliveAt(t) {
			peer := sub.B
			if dir == store.Reverse {
				peer = sub.A
			}
			out = append(out, store.Neighbor{Peer: peer, Relation: sub.Relation})
		}
	}
	store.SortNeighbors(out)
	return out, nil
}

func inWindow(recs []store.Record, from, to time.Time) []store.Record {
	var out []store.Record
	for _, r := range recs {
		if !r.EventTime.Before(from) && r.EventTime.Before(to) {
			out = append(out, r)
		}
	}
	return out
}

func (m *model) window(fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) []store.Record {
	var out []store.Record
	for _, sub := range m.incident(fp, dir, sc) {
		out = append(out, inWindow(m.visible(sub, sc.AsOf), from, to)...)
	}
	store.SortRecords(out)
	return out
}

func (m *model) entityWindow(fp identity.Fingerprint, from, to time.Time, sc store.Scope) []store.Record {
	sub := store.EntitySubject(fp)
	if m.layers[sub] != sc.Layer {
		return nil
	}
	out := inWindow(m.visible(sub, sc.AsOf), from, to)
	store.SortRecords(out)
	return out
}

// layersOf is Layers by brute force.
func (m *model) layersOf(fp identity.Fingerprint) []catalog.Layer {
	seen := map[catalog.Layer]bool{}
	for sub, layer := range m.layers {
		if (sub.Kind == store.SubjectEntity && sub.A == fp) || (sub.Kind == store.SubjectEdge && (sub.A == fp || sub.B == fp)) {
			seen[layer] = true
		}
	}
	out := []catalog.Layer{}
	for l := range seen {
		out = append(out, l)
	}
	slices.Sort(out)
	return out
}

func sameNeighbors(a, b []store.Neighbor) bool { return len(a) == len(b) && slices.Equal(a, b) }

func sameRecord(a, b store.Record) bool {
	return a.Layer == b.Layer && a.Subject == b.Subject && a.Producer == b.Producer &&
		a.EventTime.Equal(b.EventTime) && a.Seq == b.Seq && a.Kind == b.Kind && a.TTL == b.TTL &&
		a.Through.Equal(b.Through) && bytes.Equal(a.Payload, b.Payload) && a.Boot == b.Boot &&
		a.EventTimeBasis == b.EventTimeBasis
}

func sameRecords(a, b []store.Record) bool { return slices.EqualFunc(a, b, sameRecord) }

// sameClass reports whether two sentinel errors are the same one: ErrBeforeHorizon
// wraps ErrInvalid, so a one-way errors.Is would not tell them apart.
func sameClass(a, b error) bool { return errors.Is(a, b) && errors.Is(b, a) }

// checkRefusal compares the error a read gave with the class the model expects.
func checkRefusal(t interface{ Fatalf(string, ...any) }, what string, want, got error) bool {
	switch {
	case want == nil && got != nil:
		t.Fatalf("%s: unexpected error %v", what, got)
	case sameClass(want, store.ErrBeforeHorizon) && !errors.Is(got, store.ErrBeforeHorizon):
		t.Fatalf("%s: err = %v, want ErrBeforeHorizon", what, got)
	case sameClass(want, store.ErrInvalid) && (!errors.Is(got, store.ErrInvalid) || errors.Is(got, store.ErrBeforeHorizon)):
		t.Fatalf("%s: err = %v, want ErrInvalid and not ErrBeforeHorizon", what, got)
	}
	return want == nil
}

// compareAlive checks Alive against the model.
func compareAlive(t interface{ Fatalf(string, ...any) }, s *memstore.Store, m *model, fp identity.Fingerprint, when time.Time, sc store.Scope) {
	what := fmt.Sprintf("Alive(%s, %s, %+v)", fp, when.Format(time.RFC3339Nano), sc)
	got, err := s.Alive(context.Background(), fp, when, sc)
	if refusal := m.refusal(sc, true, []identity.Fingerprint{fp}, when); refusal != nil {
		checkRefusal(t, what, refusal, err)
		if got {
			t.Fatalf("%s: true with an error", what)
		}
		return
	}
	want, cc, ferr := m.alive(fp, when, sc)
	if ferr != nil {
		t.Fatalf("%s: the model could not fold: %v", what, ferr)
	}
	if cc != nil {
		var qe *store.QuarantineError
		if got || !errors.As(err, &qe) || qe.Entity != fp || qe.Layer != sc.Layer || qe.Collision == nil ||
			qe.Collision.StaleBoot != cc.StaleBoot || qe.Collision.NewerBoot != cc.NewerBoot ||
			!qe.Collision.ObservedAt.Equal(cc.ObservedAt) || !qe.Collision.NewerFirstSeen.Equal(cc.NewerFirstSeen) {
			t.Fatalf("%s = %v, %v; want false and a quarantine for %v", what, got, err, cc)
		}
		return
	}
	if err != nil || got != want {
		t.Fatalf("%s = %v, %v; want %v, nil", what, got, err, want)
	}
}

// compareNeighbors checks Neighbors and NeighborsBatch against the model.
func compareNeighbors(t interface{ Fatalf(string, ...any) }, s *memstore.Store, m *model, fps []identity.Fingerprint, dir store.Direction, when time.Time, sc store.Scope) {
	ctx := context.Background()
	dirOK := dir == store.Forward || dir == store.Reverse
	for _, fp := range fps {
		what := fmt.Sprintf("Neighbors(%s, %s, %s, %+v)", fp, dir, when.Format(time.RFC3339Nano), sc)
		got, err := s.Neighbors(ctx, fp, dir, when, sc)
		if !checkRefusal(t, what, m.refusal(sc, dirOK, []identity.Fingerprint{fp}, when), err) {
			if got != nil {
				t.Fatalf("%s: a result with an error", what)
			}
			continue
		}
		want, merr := m.neighbors(fp, dir, when, sc)
		if merr != nil {
			t.Fatalf("%s: the model could not fold: %v", what, merr)
		}
		if !sameNeighbors(got, want) {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	}
	what := fmt.Sprintf("NeighborsBatch(%d fingerprints, %s, %s, %+v)", len(fps), dir, when.Format(time.RFC3339Nano), sc)
	got, err := s.NeighborsBatch(ctx, fps, dir, when, sc)
	if !checkRefusal(t, what, m.refusal(sc, dirOK, fps, when), err) {
		if got != nil {
			t.Fatalf("%s: a result with an error", what)
		}
		return
	}
	if len(got) != len(fps) {
		t.Fatalf("%s: %d answers", what, len(got))
	}
	for i, fp := range fps {
		want, _ := m.neighbors(fp, dir, when, sc)
		if !sameNeighbors(got[i], want) {
			t.Fatalf("%s: answer %d for %s = %v, want %v", what, i, fp, got[i], want)
		}
	}
}

// compareWindows checks Window and EntityWindow against the model.
func compareWindows(t interface{ Fatalf(string, ...any) }, s *memstore.Store, m *model, fp identity.Fingerprint, dir store.Direction, from, to time.Time, sc store.Scope) {
	ctx := context.Background()
	dirOK := dir == store.Forward || dir == store.Reverse
	what := fmt.Sprintf("Window(%s, %s, [%s, %s), %+v)", fp, dir, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), sc)
	got, err := s.Window(ctx, fp, dir, from, to, sc)
	if checkRefusal(t, what, m.refusal(sc, dirOK, []identity.Fingerprint{fp}, from), err) {
		var want []store.Record
		if from.Before(to) {
			want = m.window(fp, dir, from, to, sc)
		}
		if !sameRecords(got, want) {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	} else if got != nil {
		t.Fatalf("%s: a result with an error", what)
	}

	what = fmt.Sprintf("EntityWindow(%s, [%s, %s), %+v)", fp, from.Format(time.RFC3339Nano), to.Format(time.RFC3339Nano), sc)
	got, err = s.EntityWindow(ctx, fp, from, to, sc)
	if checkRefusal(t, what, m.refusal(sc, true, []identity.Fingerprint{fp}, from), err) {
		var want []store.Record
		if from.Before(to) {
			want = m.entityWindow(fp, from, to, sc)
		}
		if !sameRecords(got, want) {
			t.Fatalf("%s = %v, want %v", what, got, want)
		}
	} else if got != nil {
		t.Fatalf("%s: a result with an error", what)
	}
}

// compareState checks what a store says about itself against the model.
func compareState(t interface{ Fatalf(string, ...any) }, s *memstore.Store, m *model, fps []identity.Fingerprint) {
	if got := s.LastSeq(); got != m.lastSeq {
		t.Fatalf("LastSeq = %d, want %d", got, m.lastSeq)
	}
	h := s.Horizon()
	if m.hasHorizon {
		if h.IsZero() || !h.Time.Equal(m.hzTime) || h.Seq != m.hzSeq || h.Time.Location() != time.UTC {
			t.Fatalf("Horizon = %+v, want {%s, %d}", h, m.hzTime, m.hzSeq)
		}
	} else if !h.IsZero() {
		t.Fatalf("Horizon = %+v, want the zero horizon", h)
	}
	for _, fp := range fps {
		if got, want := s.Layers(fp), m.layersOf(fp); !slices.Equal(got, want) {
			t.Fatalf("Layers(%s) = %v, want %v", fp, got, want)
		}
	}
}

// universe is a small world: two pods, two nodes and a host, and the edges
// between them.
type universe struct {
	fps      []identity.Fingerprint
	subjects []store.Subject
	layer    map[store.Subject]catalog.Layer
}

func newUniverse(t testing.TB) universe {
	pods := []identity.Fingerprint{fp(t, catalog.K8sPod, catalog.K8sPodUID, "p1"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "p2")}
	nodes := []identity.Fingerprint{fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n1"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n2")}
	host := fp(t, catalog.Host, catalog.HostID, "h1")
	u := universe{layer: map[store.Subject]catalog.Layer{}}
	u.fps = append(append(append(u.fps, pods...), nodes...), host)
	add := func(s store.Subject, l catalog.Layer) {
		u.subjects = append(u.subjects, s)
		u.layer[s] = l
	}
	for _, f := range u.fps {
		add(store.EntitySubject(f), entityLayers[f.Type()])
	}
	for _, p := range pods {
		for _, n := range nodes {
			add(store.EdgeSubject(p, n, catalog.ScheduledOn), catalog.L2)
		}
	}
	for _, n := range nodes {
		add(store.EdgeSubject(n, host, catalog.RunsOn), catalog.L1)
	}
	return u
}

func (u universe) genRecord(t *rapid.T) store.Record {
	sub := rapid.SampledFrom(u.subjects).Draw(t, "subject")
	r := store.Record{
		Layer: u.layer[sub], Subject: sub,
		Producer:  rapid.SampledFrom([]lifecycle.Producer{"p", "q"}).Draw(t, "producer"),
		EventTime: base.Add(time.Duration(rapid.IntRange(-2, 4).Draw(t, "seconds")) * time.Second),
		Kind:      rapid.SampledFrom([]lifecycle.Kind{lifecycle.Observe, lifecycle.Observe, lifecycle.Observe, lifecycle.Delete}).Draw(t, "kind"),
		// Any kind of record may carry any valid basis.
		EventTimeBasis: store.EventTimeBasis(rapid.IntRange(0, int(store.BasisProducerEvent)).Draw(t, "basis")),
	}
	if r.Kind == lifecycle.Observe {
		r.TTL = time.Duration(rapid.IntRange(0, 3).Draw(t, "ttl")) * time.Second
		r.Payload = []byte(rapid.SampledFrom([]string{"a", "b", "c"}).Draw(t, "payload"))
		if sub.Kind == store.SubjectEntity && sub.A.Type() == catalog.Host {
			r.Boot = rapid.SampledFrom([]string{"", "a", "b", "c"}).Draw(t, "boot")
		}
		if rapid.IntRange(0, 2).Draw(t, "run") == 0 {
			r.Through = r.EventTime.Add(time.Duration(rapid.IntRange(0, 3).Draw(t, "through")) * time.Second)
		}
	}
	return r
}

// corruptions break one rule of a record each; the model decides whether the
// batch is refused, and why.
var corruptions = []func(r *store.Record, lastSeq uint64){
	func(r *store.Record, _ uint64) { r.Producer = "" },
	func(r *store.Record, _ uint64) { r.Kind = 0 },
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Through, r.Payload = lifecycle.Delete, 0, time.Time{}, []byte("a")
	},
	func(r *store.Record, _ uint64) { r.Kind, r.TTL, r.Payload = lifecycle.Delete, time.Second, nil },
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Payload, r.Through = lifecycle.Observe, 0, []byte("a"), r.EventTime.Add(-time.Second)
	},
	func(r *store.Record, _ uint64) { r.Layer = r.Layer%catalog.L3 + 1 },
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Through, r.Payload = lifecycle.Observe, 0, time.Time{}, nil
	},
	func(r *store.Record, _ uint64) { r.Seq = 0 },
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Through, r.Payload, r.Boot = lifecycle.Delete, 0, time.Time{}, nil, "a"
	},
	func(r *store.Record, _ uint64) {
		// A boot on an edge; on any other subject the record is left a plain one.
		if r.Subject.Kind == store.SubjectEdge {
			r.Kind, r.TTL, r.Payload, r.Boot = lifecycle.Observe, 0, []byte("a"), "a"
		}
	},
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Payload, r.Boot = lifecycle.Observe, 0, []byte("a"), " "
	},
	func(r *store.Record, _ uint64) { r.Seq = store.Latest },
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Payload, r.Through = lifecycle.Observe, 0, []byte("a"), store.MaxEventTime.Add(time.Second)
	},
	func(r *store.Record, _ uint64) {
		// A deadline past the last representable instant, from an event time and a TTL
		// that are each fine.
		r.Kind, r.TTL, r.Payload, r.Through = lifecycle.Observe, time.Minute, []byte("a"), time.Time{}
		r.EventTime = store.MaxEventTime.Add(-time.Second)
	},
	func(r *store.Record, _ uint64) {
		r.Kind, r.TTL, r.Payload = lifecycle.Observe, time.Minute, []byte("a")
		r.EventTime, r.Through = base, store.MaxEventTime.Add(-time.Second)
	},
	func(r *store.Record, last uint64) { r.Seq = last },
	func(r *store.Record, _ uint64) { r.EventTime = store.MaxEventTime.Add(time.Second) },
	func(r *store.Record, _ uint64) { r.EventTime = store.MinEventTime.Add(-time.Second) },
}

func TestStoreMatchesABruteForceModel(t *testing.T) {
	t.Parallel()
	u := newUniverse(t)
	zero := identity.Fingerprint{}
	ctx := context.Background()

	rapid.Check(t, func(t *rapid.T) {
		policy := rapid.SampledFrom([]lifecycle.Policy{{}, bootPolicy}).Draw(t, "policy")
		s, err := memstore.Open(memstore.Options{Policy: policy})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		m := newModel(policy)

		instants := func(label string) time.Time {
			return base.Add(time.Duration(rapid.IntRange(-4, 8).Draw(t, label)) * time.Second)
		}
		fpOrZero := rapid.SampledFrom(append(slices.Clone(u.fps), zero, u.fps[0], u.fps[3]))
		layers := rapid.SampledFrom([]catalog.Layer{0, catalog.L0, catalog.L1, catalog.L1, catalog.L2, catalog.L2, catalog.L2, catalog.L3, catalog.L3 + 1})
		dirs := rapid.SampledFrom([]store.Direction{0, store.Forward, store.Forward, store.Reverse, store.Reverse, 3})

		steps := rapid.IntRange(5, 40).Draw(t, "steps")
		for step := range steps {
			switch rapid.IntRange(0, 9).Draw(t, "op") {
			case 0, 1, 2, 3: // write
				n := rapid.IntRange(1, 4).Draw(t, "batch size")
				batch := make([]store.Record, n)
				seq := m.lastSeq
				for i := range batch {
					batch[i] = u.genRecord(t)
					seq += 1 + uint64(rapid.IntRange(0, 2).Draw(t, "seq gap"))
					batch[i].Seq = seq
				}
				if rapid.IntRange(0, 4).Draw(t, "corrupt") == 0 {
					i := rapid.IntRange(0, n-1).Draw(t, "which record")
					rapid.SampledFrom(corruptions).Draw(t, "corruption")(&batch[i], m.lastSeq)
				}
				want := m.judge(batch)
				got := s.Write(ctx, slices.Clone(batch))
				switch {
				case want == nil && got != nil:
					t.Fatalf("step %d: Write of %+v refused: %v", step, batch, got)
				case sameClass(want, store.ErrBeforeHorizon) && !errors.Is(got, store.ErrBeforeHorizon):
					t.Fatalf("step %d: Write of %+v = %v, want ErrBeforeHorizon", step, batch, got)
				case sameClass(want, store.ErrInvalid) && (!errors.Is(got, store.ErrInvalid) || errors.Is(got, store.ErrBeforeHorizon)):
					t.Fatalf("step %d: Write of %+v = %v, want ErrInvalid", step, batch, got)
				}
				if want == nil {
					m.accept(batch)
				}
			case 4: // retain
				h := instants("horizon")
				if rapid.Bool().Draw(t, "other zone") {
					h = h.In(time.FixedZone("z", 3600*rapid.IntRange(-12, 12).Draw(t, "offset")))
				}
				if err := s.Retain(ctx, h); err != nil {
					t.Fatalf("step %d: Retain: %v", step, err)
				}
				m.retain(h)
			default: // read
				hzSeq := m.hzSeq
				tokens := []uint64{0, 1, m.lastSeq, m.lastSeq + 3, store.Latest, hzSeq, max(hzSeq, 1) - 1, hzSeq + 1}
				sc := store.Scope{Layer: layers.Draw(t, "layer"), AsOf: rapid.SampledFrom(tokens).Draw(t, "token")}
				dir := dirs.Draw(t, "direction")
				when := instants("instant")
				switch rapid.IntRange(0, 4).Draw(t, "read") {
				case 0:
					compareNeighbors(t, s, m, []identity.Fingerprint{fpOrZero.Draw(t, "fp")}, dir, when, sc)
				case 1:
					compareNeighbors(t, s, m, rapid.SliceOfN(fpOrZero, 0, 3).Draw(t, "fps"), dir, when, sc)
				case 2:
					compareAlive(t, s, m, fpOrZero.Draw(t, "fp"), when, sc)
				default:
					to := when.Add(time.Duration(rapid.IntRange(-1, 7).Draw(t, "window")) * time.Second)
					compareWindows(t, s, m, fpOrZero.Draw(t, "fp"), dir, when, to, sc)
				}
			}
			compareState(t, s, m, u.fps)
		}

		// Whatever happened, a final sweep of the whole universe at Latest.
		sc := store.Scope{Layer: catalog.L2, AsOf: store.Latest}
		for _, layer := range []catalog.Layer{catalog.L1, catalog.L2} {
			sc.Layer = layer
			from := base.Add(time.Duration(rapid.IntRange(0, 2).Draw(t, "final from")) * time.Second)
			for _, f := range u.fps {
				compareAlive(t, s, m, f, from, sc)
				compareNeighbors(t, s, m, []identity.Fingerprint{f}, store.Forward, from, sc)
				compareNeighbors(t, s, m, []identity.Fingerprint{f}, store.Reverse, from, sc)
				compareWindows(t, s, m, f, store.Forward, from, from.Add(time.Minute), sc)
				compareWindows(t, s, m, f, store.Reverse, from, from.Add(time.Minute), sc)
			}
		}
	})
}

func TestStoreMatchesABruteForceModelOnAGeneratedStream(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := storetest.Tiny()
		c.Seed = rapid.Uint64().Draw(t, "seed")
		c.Duration = 10 * time.Minute
		c.Runs = rapid.Bool().Draw(t, "runs")
		c.LateProbability = rapid.SampledFrom([]float64{0, 0.3}).Draw(t, "late")
		if rapid.Bool().Draw(t, "reboots") {
			c.RebootProbability, c.CloneProbability = 0.15, 0.5
		}
		policy := rapid.SampledFrom([]lifecycle.Policy{{}, bootPolicy}).Draw(t, "policy")

		g, err := storetest.NewGenerator(c)
		if err != nil {
			t.Fatal(err)
		}
		recs := g.All()
		s, err := memstore.Open(memstore.Options{Policy: policy})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		m := newModel(policy)
		for rest := recs; len(rest) > 0; {
			n := min(len(rest), rapid.IntRange(1, 300).Draw(t, "batch size"))
			if err := s.Write(context.Background(), slices.Clone(rest[:n])); err != nil {
				t.Fatalf("Write: %v", err)
			}
			m.accept(rest[:n])
			rest = rest[n:]
		}
		entities := g.Entities()
		compareState(t, s, m, entities)

		for range 12 {
			f := rapid.SampledFrom(entities).Draw(t, "entity")
			when := g.Start().Add(time.Duration(rapid.IntRange(-60, 15*60).Draw(t, "seconds")) * time.Second)
			token := rapid.SampledFrom([]uint64{0, recs[len(recs)/3].Seq, recs[2*len(recs)/3].Seq, recs[len(recs)-1].Seq, store.Latest}).Draw(t, "token")
			layer := rapid.SampledFrom([]catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}).Draw(t, "layer")
			sc := store.Scope{Layer: layer, AsOf: token}
			compareAlive(t, s, m, f, when, sc)
			compareNeighbors(t, s, m, []identity.Fingerprint{f}, store.Forward, when, sc)
			compareNeighbors(t, s, m, []identity.Fingerprint{f}, store.Reverse, when, sc)
			compareWindows(t, s, m, f, store.Forward, when, when.Add(time.Minute), sc)
		}
	})
}

// TestAliveQuarantinesExactlyTheClonedHosts writes a generated stream with clones
// and sweeps Alive over every entity at Latest: the quarantined ones are the
// hosts a clone reported an old boot for, which are also the ones folding their
// records directly with the policy fails for.
func TestAliveQuarantinesExactlyTheClonedHosts(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := storetest.Tiny()
		c.Seed = rapid.Uint64().Draw(t, "seed")
		c.Duration = 10 * time.Minute
		c.Hosts = rapid.IntRange(1, 12).Draw(t, "hosts")
		c.Runs = rapid.Bool().Draw(t, "runs")
		c.RebootProbability = rapid.SampledFrom([]float64{0.2, 1}).Draw(t, "reboot")
		c.CloneProbability = rapid.SampledFrom([]float64{0, 0.3, 1}).Draw(t, "clone")
		g, err := storetest.NewGenerator(c)
		if err != nil {
			t.Fatal(err)
		}
		recs := g.All()
		s, err := memstore.Open(memstore.Options{Policy: bootPolicy})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if err := s.Write(context.Background(), slices.Clone(recs)); err != nil {
			t.Fatal(err)
		}

		cloned := map[identity.Fingerprint]bool{}
		folded := map[identity.Fingerprint]bool{} // the hosts whose own records fail to fold
		bySub := map[store.Subject][]store.Record{}
		for _, r := range recs {
			bySub[r.Subject] = append(bySub[r.Subject], r)
			if r.Producer == storetest.ProducerClone {
				cloned[r.Subject.A] = true
			}
		}
		for sub, rs := range bySub {
			if sub.Kind != store.SubjectEntity {
				continue
			}
			if _, err := fold(rs, bootPolicy); err != nil {
				folded[sub.A] = true
			}
		}

		quarantined := map[identity.Fingerprint]bool{}
		layers := map[identity.Fingerprint]catalog.Layer{}
		for _, r := range recs {
			if r.Subject.Kind == store.SubjectEntity {
				layers[r.Subject.A] = r.Layer
			}
		}
		for _, fp := range g.Entities() {
			layer, ok := layers[fp]
			if !ok {
				continue
			}
			_, err := s.Alive(context.Background(), fp, g.End(), store.Current(layer))
			var qe *store.QuarantineError
			switch {
			case err == nil:
			case errors.As(err, &qe):
				quarantined[fp] = true
			default:
				t.Fatalf("Alive(%s): %v", fp, err)
			}
		}
		if !maps.Equal(quarantined, cloned) || !maps.Equal(quarantined, folded) {
			t.Fatalf("quarantined %d hosts, cloned %d, failing to fold %d: the sets differ", len(quarantined), len(cloned), len(folded))
		}
		if c.CloneProbability == 1 && c.RebootProbability == 1 && len(cloned) == 0 {
			t.Fatal("no clones with CloneProbability 1: the test reaches nothing")
		}
	})
}
