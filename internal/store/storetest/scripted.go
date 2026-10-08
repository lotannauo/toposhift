package storetest

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// scenario is a fixed sequence of writes applied to the store under test and a
// reference, with the same questions asked of both between them.
type scenario struct {
	cand store.Store
	ref  *memstore.Store
	rd   reads
	hz   store.Horizon // the horizon after the last Retain; zero before
}

func newScenario(cand store.Store, policy lifecycle.Policy, label func(time.Time) string) (*scenario, error) {
	ref, err := memstore.Open(memstore.Options{Policy: policy})
	if err != nil {
		return nil, err
	}
	return &scenario{cand: cand, ref: ref, rd: reads{cand: cand, ref: ref, label: label}}, nil
}

func (s *scenario) close() { _ = s.ref.Close() }

// write applies one batch to both, and checks that the token follows it.
func (s *scenario) write(recs ...store.Record) error {
	given := cloneRecords(recs)
	if err := s.cand.Write(bg, given); err != nil {
		return fmt.Errorf("the Write of seq %d: %w", recs[0].Seq, err)
	}
	scribble(given)
	if err := s.ref.Write(bg, cloneRecords(recs)); err != nil {
		return fmt.Errorf("reference Write of seq %d: %w", recs[0].Seq, err)
	}
	if got, want := s.cand.LastSeq(), s.ref.LastSeq(); got != want {
		return fmt.Errorf("LastSeq = %d after seq %d; want %d", got, recs[len(recs)-1].Seq, want)
	}
	return nil
}

// retain moves the horizon of both, and checks that the store reports what the
// reference does.
func (s *scenario) retain(h time.Time) error {
	if err := s.cand.Retain(bg, h); err != nil {
		return fmt.Errorf("Retain(%s): %w", s.rd.label(h), err)
	}
	if err := s.ref.Retain(bg, h); err != nil {
		return err
	}
	if got, want := s.cand.Horizon(), s.ref.Horizon(); !sameHorizon(got, want) {
		return fmt.Errorf("Horizon() = %v after Retain(%s); reference says %v", got, s.rd.label(h), want)
	}
	if got, want := s.cand.LastSeq(), s.ref.LastSeq(); got != want {
		return fmt.Errorf("the Retain moved LastSeq to %d; want %d", got, want)
	}
	s.hz = s.ref.Horizon()
	return nil
}

// check asks every question about the given entities at each instant, layer and
// token: Alive and Neighbors, the windows between the instants and the windows
// that put an edge on each instant, the entities' own records the same way, and
// the batched read for all of them at once. Windows whose lower edge falls before
// floor are skipped. After a retention it also asks what must be refused.
func (s *scenario) check(fps []identity.Fingerprint, layers []catalog.Layer, times []time.Time, tokens []uint64, floor time.Time) error {
	var edges []interval
	for _, t := range times {
		edges = append(edges, around(t, floor)...)
	}
	edges = append(edges, between(times)...)
	for _, fp := range fps {
		if err := s.rd.entity(fp, layers, times, tokens); err != nil {
			return err
		}
		if err := s.rd.windows(fp, layers, bothDirections, edges, tokens); err != nil {
			return err
		}
		if err := s.rd.entityWindows(fp, layers, edges, tokens); err != nil {
			return err
		}
	}
	if err := s.rd.batch(fps, layers, times, tokens); err != nil {
		return err
	}
	if !s.hz.IsZero() {
		for _, layer := range layers {
			if err := s.rd.refusals(fps[0], layer, s.hz); err != nil {
				return err
			}
		}
	}
	return nil
}

func entityFP(typ catalog.EntityType, key catalog.AttributeKey, v string) (identity.Fingerprint, error) {
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		return identity.Fingerprint{}, err
	}
	return id.Fingerprint(), nil
}

// seqBase makes the scripted checks' sequence numbers straddle 2^32, so a layout
// that stores Seq in 32 bits cannot pass them: the first records are just below
// it and the last just above.
const seqBase = 1<<32 - 3

// tokensAround returns the tokens a scripted check asks under for a run of
// sequence numbers first..last: nothing, each one in turn, one past the last,
// and tokens far above it, which a store must treat as the latest.
func tokensAround(first, last uint64) []uint64 {
	toks := []uint64{0, first - 1}
	for seq := first; seq <= last; seq++ {
		toks = append(toks, seq)
	}
	return append(toks, last+1, 1<<63, store.Latest-1, store.Latest)
}

// CheckInstant checks what happens when several records of one producer share
// one event time: a delete, a re-observation, and two run extensions that
// re-assert the run at its start with a later Seq and a later Through. A query
// pinned to each token in turn must see exactly the records up to it, so every
// version must have been kept, and the overwritten ones must still be returned
// by Window. It then retains at that very instant and writes at it again: records
// at the horizon stay, and the new one lands among them. The sequence numbers
// cross 2^32. Use a fresh store opened with the zero policy.
func CheckInstant(st store.Store) error {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "instant-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "instant-node")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	rec := func(seq uint64, at time.Time, kind lifecycle.Kind, ttl time.Duration, through time.Time, payload string) store.Record {
		r := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "instant",
			EventTime: at, Seq: seq, Kind: kind, TTL: ttl, Through: through,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte(payload)
		}
		return r
	}
	s, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.Sub(x).String() })
	if err != nil {
		return err
	}
	defer s.close()

	for _, r := range []store.Record{
		rec(seqBase+1, x.Add(-10*time.Minute), lifecycle.Observe, 0, time.Time{}, "before"),
		rec(seqBase+2, x, lifecycle.Delete, 0, time.Time{}, ""),
		rec(seqBase+3, x, lifecycle.Observe, 3*time.Minute, time.Time{}, "run"),
		rec(seqBase+4, x, lifecycle.Observe, 3*time.Minute, x.Add(time.Minute), "run"),
		rec(seqBase+5, x, lifecycle.Observe, 3*time.Minute, x.Add(2*time.Minute), "run"),
	} {
		if err := s.write(r); err != nil {
			return err
		}
	}
	fps := []identity.Fingerprint{pod, node}
	layers := []catalog.Layer{catalog.L2, catalog.L1}
	// The deadlines are x+3m, x+4m and x+5m for the three versions of the run.
	times := []time.Time{
		x.Add(-11 * time.Minute), x.Add(-10 * time.Minute), x.Add(-time.Nanosecond), x, x.Add(time.Nanosecond), x.Add(time.Minute),
		x.Add(3*time.Minute - time.Nanosecond), x.Add(3 * time.Minute), x.Add(4*time.Minute - time.Nanosecond), x.Add(4 * time.Minute),
		x.Add(5*time.Minute - time.Nanosecond), x.Add(5 * time.Minute), x.Add(time.Hour),
	}
	if err := s.check(fps, layers, times, tokensAround(seqBase+1, seqBase+5), store.MinEventTime); err != nil {
		return fmt.Errorf("before the retention: %w", err)
	}

	// Retain at the instant the four records share. Everything at or after it
	// stays, including all four, and a token at or above the sequence reached
	// sees the same.
	if err := s.retain(x); err != nil {
		return err
	}
	if err := s.write(rec(seqBase+6, x, lifecycle.Observe, 3*time.Minute, time.Time{}, "again")); err != nil {
		return fmt.Errorf("a record exactly at the horizon was refused: %w", err)
	}
	after := times[3:] // from x on
	toks := []uint64{seqBase + 5, seqBase + 6, seqBase + 7, 1 << 63, store.Latest - 1, store.Latest}
	if err := s.check(fps, layers, after, toks, x); err != nil {
		return fmt.Errorf("after retaining at the instant: %w", err)
	}
	return nil
}

// CheckEntityWindow is CheckInstant's shape applied to an entity's own records:
// several records of one producer at one instant (an observe, a delete, a
// re-observe, two run extensions), records of another producer, and an edge of the
// same fingerprint, which EntityWindow must not return. Every token is asked, and
// windows that start, end and sit on each instant, in the entity's layer and
// another. It then retains at the shared instant, writes at it again, and asks the
// same from there. Use a fresh store opened with the zero policy.
func CheckEntityWindow(st store.Store) error {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "entity-window-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "entity-window-node")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	own := func(seq uint64, p lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration, through time.Time, payload string) store.Record {
		r := store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: p, EventTime: at, Seq: seq, Kind: kind, TTL: ttl, Through: through,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte(payload)
		}
		return r
	}
	edge := func(seq uint64, at time.Time, payload string) store.Record {
		return store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "alpha",
			EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte(payload),
		}
	}
	s, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.Sub(x).String() })
	if err != nil {
		return err
	}
	defer s.close()

	for _, r := range []store.Record{
		own(seqBase+1, "alpha", x.Add(-10*time.Minute), lifecycle.Observe, 0, time.Time{}, "before"),
		own(seqBase+2, "alpha", x, lifecycle.Delete, 0, time.Time{}, ""),
		own(seqBase+3, "alpha", x, lifecycle.Observe, 3*time.Minute, time.Time{}, "run"),
		own(seqBase+4, "alpha", x, lifecycle.Observe, 3*time.Minute, x.Add(time.Minute), "run"),
		own(seqBase+5, "alpha", x, lifecycle.Observe, 3*time.Minute, x.Add(2*time.Minute), "run"),
		own(seqBase+6, "beta", x, lifecycle.Observe, 0, time.Time{}, "other"),
		edge(seqBase+7, x, "edge"),
		own(seqBase+8, "beta", x.Add(30*time.Second), lifecycle.Delete, 0, time.Time{}, ""),
		edge(seqBase+9, x.Add(time.Minute), "edge again"),
	} {
		if err := s.write(r); err != nil {
			return err
		}
	}
	fps := []identity.Fingerprint{pod, node}
	layers := []catalog.Layer{catalog.L2, catalog.L1}
	times := []time.Time{
		x.Add(-11 * time.Minute), x.Add(-10 * time.Minute), x.Add(-time.Nanosecond), x, x.Add(time.Nanosecond), x.Add(30 * time.Second),
		x.Add(time.Minute), x.Add(3*time.Minute - time.Nanosecond), x.Add(3 * time.Minute), x.Add(5 * time.Minute), x.Add(time.Hour),
	}
	if err := s.check(fps, layers, times, tokensAround(seqBase+1, seqBase+9), store.MinEventTime); err != nil {
		return fmt.Errorf("before the retention: %w", err)
	}
	if err := s.retain(x); err != nil {
		return err
	}
	if err := s.write(own(seqBase+10, "alpha", x, lifecycle.Observe, 3*time.Minute, time.Time{}, "again")); err != nil {
		return fmt.Errorf("a record exactly at the horizon was refused: %w", err)
	}
	toks := []uint64{seqBase + 9, seqBase + 10, seqBase + 11, 1 << 63, store.Latest - 1, store.Latest}
	if err := s.check(fps, layers, times[3:], toks, x); err != nil {
		return fmt.Errorf("after retaining at the instant: %w", err)
	}
	return nil
}

// CheckProducers checks that a reference belongs to the producer that made it. A
// delete ends only its own producer's reference, so an edge or an entity stays
// alive while any other producer still holds it, including when one producer
// deletes at the very instant another observes, and when the producer that
// deletes had observed later than the one that holds on. A layout that lets the
// newest record for a subject decide, without asking whose it is, answers these
// wrongly. It uses an edge and an entity, and tokens between every record. Use a
// fresh store opened with the zero policy.
func CheckProducers(st store.Store) error {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "producers-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "producers-node")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	edge, entity := store.EdgeSubject(pod, node, catalog.ScheduledOn), store.EntitySubject(pod)
	var seq uint64 = seqBase
	var recs []store.Record
	// each statement is made about the edge and about the entity, so the same
	// producer interplay is checked for both kinds of subject.
	say := func(p lifecycle.Producer, at time.Duration, kind lifecycle.Kind, ttl time.Duration) {
		for _, sub := range []store.Subject{edge, entity} {
			seq++
			r := store.Record{Layer: catalog.L2, Subject: sub, Producer: p, EventTime: x.Add(at), Seq: seq, Kind: kind, TTL: ttl}
			if kind == lifecycle.Observe {
				r.Payload = []byte(p)
			}
			recs = append(recs, r)
		}
	}
	say("p", 0, lifecycle.Observe, 0)                        // p holds it open-ended from x
	say("q", time.Minute, lifecycle.Observe, 4*time.Minute)  // q also holds it, until x+5m
	say("q", 2*time.Minute, lifecycle.Delete, 0)             // q lets go: p still holds it
	say("p", 20*time.Minute, lifecycle.Observe, 0)           // at one instant p observes ...
	say("q", 20*time.Minute, lifecycle.Delete, 0)            // ... and q deletes: it lives
	say("q", 30*time.Minute, lifecycle.Observe, time.Minute) // q holds it to x+31m ...
	say("p", 30*time.Minute, lifecycle.Delete, 0)            // ... while p lets go at the same instant, later in Seq
	say("q", 31*time.Minute, lifecycle.Delete, 0)            // now nobody holds it

	s, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.Sub(x).String() })
	if err != nil {
		return err
	}
	defer s.close()
	for _, r := range recs {
		if err := s.write(r); err != nil {
			return err
		}
	}
	times := []time.Time{x.Add(-time.Nanosecond)}
	for _, at := range []time.Duration{
		0, 30 * time.Second, time.Minute, 2*time.Minute - 1, 2 * time.Minute, 3 * time.Minute, 5 * time.Minute,
		20*time.Minute - 1, 20 * time.Minute, 20*time.Minute + 1, 30*time.Minute - 1, 30 * time.Minute, 31*time.Minute - 1, 31 * time.Minute, time.Hour,
	} {
		times = append(times, x.Add(at))
	}
	return s.check([]identity.Fingerprint{pod, node}, []catalog.Layer{catalog.L2, catalog.L1}, times, tokensAround(seqBase+1, seq), store.MinEventTime)
}

// CheckRelations checks that an edge is identified by its relation as well as
// its two ends: two relations between one pair of entities are two edges, one
// ending does not end the other, and an edge from an entity to itself is read from
// both of its sides. The workload fixes the relation by the types at its ends, so
// no random workload puts two relations on one pair, and a layout that grouped by
// peer alone would pass them all. Use a fresh store opened with the zero policy.
func CheckRelations(st store.Store) error {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "relations-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "relations-node")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	var seq uint64 = seqBase
	var recs []store.Record
	say := func(sub store.Subject, p lifecycle.Producer, at time.Duration, kind lifecycle.Kind, ttl time.Duration) {
		seq++
		r := store.Record{Layer: catalog.L2, Subject: sub, Producer: p, EventTime: x.Add(at), Seq: seq, Kind: kind, TTL: ttl}
		if kind == lifecycle.Observe {
			r.Payload = []byte(sub.Relation)
		}
		recs = append(recs, r)
	}
	sched, runs := store.EdgeSubject(pod, node, catalog.ScheduledOn), store.EdgeSubject(pod, node, catalog.RunsOn)
	loop := store.EdgeSubject(pod, pod, catalog.PartOf)
	say(sched, "p", 0, lifecycle.Observe, 0)
	say(runs, "p", time.Minute, lifecycle.Observe, 0)
	say(loop, "p", 90*time.Second, lifecycle.Observe, 2*time.Minute)
	say(sched, "p", 2*time.Minute, lifecycle.Delete, 0) // only the first relation ends
	say(runs, "q", 3*time.Minute, lifecycle.Observe, time.Minute)
	say(runs, "p", 5*time.Minute, lifecycle.Delete, 0)

	s, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.Sub(x).String() })
	if err != nil {
		return err
	}
	defer s.close()
	for _, r := range recs {
		if err := s.write(r); err != nil {
			return err
		}
	}
	times := []time.Time{x.Add(-time.Nanosecond)}
	for _, at := range []time.Duration{
		0, 30 * time.Second, time.Minute, 90 * time.Second, 2*time.Minute - 1, 2 * time.Minute, 3 * time.Minute,
		3*time.Minute + 30*time.Second, 3*time.Minute + 30*time.Second + 1, 4 * time.Minute, 5 * time.Minute, time.Hour,
	} {
		times = append(times, x.Add(at))
	}
	return s.check([]identity.Fingerprint{pod, node}, []catalog.Layer{catalog.L2, catalog.L1}, times, tokensAround(seqBase+1, seq), store.MinEventTime)
}

// CheckExtremes checks the ends of the representable range, where a layout that
// encodes time in a narrow or unsigned field can go wrong: a record at the very
// first instant (an encoding that treats time zero as "no version" sorts it
// wrong), at the very last, a deadline exactly at the last that comes from a
// run's Through, a delete at the last instant, and reads one nanosecond either
// side of both ends and far outside them. Use a fresh store opened with the zero
// policy.
func CheckExtremes(st store.Store) error {
	var pods [4]identity.Fingerprint
	for i := range pods {
		p, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("extremes-pod-%d", i))
		if err != nil {
			return err
		}
		pods[i] = p
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "extremes-node")
	if err != nil {
		return err
	}
	lo, hi := store.MinEventTime, store.MaxEventTime
	edge := func(pod identity.Fingerprint, seq uint64, at time.Time, kind lifecycle.Kind, ttl time.Duration, through time.Time) store.Record {
		r := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "extremes",
			EventTime: at, Seq: seq, Kind: kind, TTL: ttl, Through: through,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte{byte(seq)}
		}
		return r
	}
	podRecord := func(pod identity.Fingerprint, seq uint64, at time.Time) store.Record {
		return store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "extremes", EventTime: at, Seq: seq,
			Kind: lifecycle.Observe, Payload: []byte{byte(seq)},
		}
	}
	s, err := newScenario(st, lifecycle.Policy{}, func(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) })
	if err != nil {
		return err
	}
	defer s.close()

	for _, r := range []store.Record{
		edge(pods[0], 1, lo, lifecycle.Observe, 0, time.Time{}),                                    // exists from the first instant, open
		podRecord(pods[0], 2, lo),                                                                  // so does the entity
		edge(pods[1], 3, hi.Add(-time.Minute), lifecycle.Observe, time.Minute, time.Time{}),        // deadline exactly at the last instant
		edge(pods[2], 4, hi.Add(-time.Hour), lifecycle.Observe, time.Minute, hi.Add(-time.Minute)), // a run whose deadline, from its Through, is the last instant
		edge(pods[3], 5, hi, lifecycle.Observe, 0, time.Time{}),                                    // starts at the last instant
		edge(pods[0], 6, hi, lifecycle.Delete, 0, time.Time{}),                                     // ends the open edge at the last instant
		podRecord(pods[3], 7, hi),
	} {
		if err := s.write(r); err != nil {
			return err
		}
	}
	fps := append(pods[:], node)
	layers := []catalog.Layer{catalog.L2, catalog.L1}
	times := []time.Time{
		{},
		time.Unix(-1<<40, 0), lo.Add(-time.Nanosecond), lo, lo.Add(time.Nanosecond), lo.Add(time.Second),
		hi.Add(-time.Hour), hi.Add(-time.Minute - time.Nanosecond), hi.Add(-time.Minute), hi.Add(-time.Nanosecond), hi,
		hi.Add(time.Nanosecond), time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	// Windows may start anywhere: nothing has been retained.
	return s.check(fps, layers, times, tokensAround(1, 7), time.Time{})
}
