package bytesgate

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// from is the window's start in every fixture and window is its length. The
// store holds everything that exists at From from a time well before it, as a
// store of some weeks does.
var from = time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)

const (
	window  = time.Hour
	history = 29 * 24 * time.Hour
)

const (
	pK8s   lifecycle.Producer = "k8sobjects/default"
	pTrace lifecycle.Producer = "traces/default"
	pAgent lifecycle.Producer = "nodeagent/default"
)

// rng is a splitmix64 generator. Its output is fixed by this code, so a fixture
// has the same bytes under any Go version.
type rng struct{ x uint64 }

func newRNG(seed uint64) *rng { return &rng{x: seed} }

func (r *rng) next() uint64 {
	r.x += 0x9e3779b97f4a7c15
	z := r.x
	z = (z ^ z>>30) * 0xbf58476d1ce4e5b9
	z = (z ^ z>>27) * 0x94d049bb133111eb
	return z ^ z>>31
}

// intn is a number in [0, n), n at least 1. The bias of the modulus is
// negligible at the sizes used here and is part of the fixture.
func (r *rng) intn(n int) int { return int(r.next() % uint64(n)) }

// ids names entities. The hash of each is the first 16 bytes of the SHA-256 of
// the seed, the type and a count, so fingerprints look like real ones.
type ids struct {
	seed uint64
	n    map[catalog.EntityType]int
}

func newIDs(seed uint64) *ids { return &ids{seed: seed, n: map[catalog.EntityType]int{}} }

func (g *ids) next(typ catalog.EntityType) identity.Fingerprint {
	g.n[typ]++
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], g.seed)
	binary.BigEndian.PutUint64(buf[8:], uint64(g.n[typ]))
	sum := sha256.Sum256(append([]byte("toposhift/bytesgate/"+string(typ)+"/"), buf[:]...))
	var h [identity.FingerprintBytes]byte
	copy(h[:], sum[:])
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err) // the types used here are in the catalog
	}
	return fp
}

func entity(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func scheduledOn(p, n identity.Fingerprint) store.Subject {
	return store.EdgeSubject(p, n, catalog.ScheduledOn)
}

func runsOn(n, h identity.Fingerprint) store.Subject { return store.EdgeSubject(n, h, catalog.RunsOn) }

func partOf(i, p identity.Fingerprint) store.Subject { return store.EdgeSubject(i, p, catalog.PartOf) }

func instanceOf(i, s identity.Fingerprint) store.Subject {
	return store.EdgeSubject(i, s, catalog.InstanceOf)
}

func dependsOn(a, b identity.Fingerprint) store.Subject {
	return store.EdgeSubject(a, b, catalog.DependsOn)
}

// producerOf is the producer of a subject: traces for services, instances and
// their edges, the node agent for runs_on, and the Kubernetes objects for the
// rest.
func producerOf(s store.Subject) lifecycle.Producer {
	switch {
	case s.Kind == store.SubjectEntity && (s.A.Type() == catalog.Service || s.A.Type() == catalog.ServiceInstance),
		s.Relation == catalog.InstanceOf, s.Relation == catalog.DependsOn,
		s.Relation == catalog.PartOf && s.A.Type() == catalog.ServiceInstance:
		return pTrace
	case s.Relation == catalog.RunsOn:
		return pAgent
	}
	return pK8s
}

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

// event is one record before it is numbered.
type event struct {
	at  time.Time
	rec store.Record
}

// world is a memstore and the events that will fill it. Events are collected,
// then written in event-time order, so a fixture reads as a history and is
// written as one.
type world struct {
	st     *memstore.Store
	events []event
}

func newWorld() (*world, error) {
	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		return nil, err
	}
	return &world{st: st}, nil
}

func (w *world) close() { _ = w.st.Close() }

// base is an instant before the window, at which what exists at From began.
func base() time.Time { return from.Add(-history) }

func (w *world) obs(s store.Subject, at time.Time) {
	w.events = append(w.events, event{at: at, rec: store.Record{
		Layer: layerOf(s), Subject: s, Producer: producerOf(s), EventTime: at,
		Kind: lifecycle.Observe, Payload: []byte("x"),
	}})
}

func (w *world) del(s store.Subject, at time.Time) {
	w.events = append(w.events, event{at: at, rec: store.Record{
		Layer: layerOf(s), Subject: s, Producer: producerOf(s), EventTime: at, Kind: lifecycle.Delete,
	}})
}

// commit writes the events in event-time order, in batches. The sort is stable:
// the events of one instant keep the order they were added in.
func (w *world) commit(ctx context.Context) error {
	slices.SortStableFunc(w.events, func(a, b event) int { return a.at.Compare(b.at) })
	const batch = 1024
	seq := w.st.LastSeq()
	for len(w.events) > 0 {
		n := min(batch, len(w.events))
		rs := make([]store.Record, n)
		for i := range rs {
			seq++
			rs[i] = w.events[i].rec
			rs[i].Seq = seq
		}
		if err := w.st.Write(ctx, rs); err != nil {
			return fmt.Errorf("bytesgate: writing the world: %w", err)
		}
		w.events = w.events[n:]
	}
	return nil
}

// podRec is a pod and, where the fixture has them, its instance and the service
// the instance is mapped to, if any.
type podRec struct{ pod, inst, svc identity.Fingerprint }

// nodeRec is a node and the pods that are on it now: the pods the history has
// reached, as it is built.
type nodeRec struct {
	node identity.Fingerprint
	pods []podRec
}

// placePod makes a pod on a node at instant at, with an instance of its own if
// instances is set, and maps the instance to svc when svc is not zero.
func (g *ids) placePod(w *world, n *nodeRec, at time.Time, instances bool, svc identity.Fingerprint) podRec {
	p := podRec{pod: g.next(catalog.K8sPod), svc: svc}
	w.obs(entity(p.pod), at)
	w.obs(scheduledOn(p.pod, n.node), at)
	if instances {
		p.inst = g.next(catalog.ServiceInstance)
		w.obs(entity(p.inst), at)
		w.obs(partOf(p.inst, p.pod), at)
		if !svc.IsZero() {
			w.obs(instanceOf(p.inst, svc), at)
		}
	}
	n.pods = append(n.pods, p)
	return p
}

// retire deletes a pod, its edge to its node, and its instance.
func retire(w *world, p podRec, n *nodeRec, at time.Time) {
	w.del(scheduledOn(p.pod, n.node), at)
	w.del(entity(p.pod), at)
	if !p.inst.IsZero() {
		w.del(partOf(p.inst, p.pod), at)
		w.del(entity(p.inst), at)
	}
}

// replace is the churn of a fixture: every period, one pod that is not among the
// first keep of its node is deleted, on a node the generator picks, and a new
// one is put in its place a few seconds later, with an instance of the same
// service if the old one had one. The seed's own pods are among the ones kept.
func (g *ids) replace(w *world, r *rng, nodes []*nodeRec, period time.Duration, keep int, instances bool) {
	for t := period; t < window; t += period {
		n := nodes[r.intn(len(nodes))]
		if len(n.pods) <= keep {
			continue
		}
		i := keep + r.intn(len(n.pods)-keep)
		old := n.pods[i]
		n.pods = slices.Delete(n.pods, i, i+1)
		at := from.Add(t)
		retire(w, old, n, at)
		g.placePod(w, n, at.Add(time.Duration(1+r.intn(5))*time.Second), instances, old.svc)
	}
}
