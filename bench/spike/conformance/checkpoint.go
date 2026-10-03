package conformance

import (
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrNotCheckpointer is returned by CheckCheckpoint for an engine that does not
// implement [engine.Checkpointer].
var ErrNotCheckpointer = errors.New("the candidate does not implement engine.Checkpointer")

// CheckCheckpoint checks an engine that keeps checkpoints at the instants a
// policy would never choose in a workload whose event times fall on whole
// seconds. It writes checkpoints itself ([engine.Checkpointer]) and then probes
// around them at every token:
//
//   - the sequence number a checkpoint depends on: a Delete an old token must not
//     see is part of what it summarizes, even though nothing of it is kept;
//   - the boundaries: a record a nanosecond before a checkpoint's instant makes it
//     untrue, one exactly at the instant or after it does not, and reads exactly
//     at the instant and either side of it;
//   - a checkpoint built on an older one, which must depend on everything the
//     older one did;
//   - a retention exactly at a checkpoint's instant, after which none may be
//     written at or below the horizon, and a record written at the horizon must
//     not be hidden by one.
//
// An engine may refuse a checkpoint it cannot place ([engine.ErrInvalid]); every
// answer must be the oracle's whether it wrote one or not. Use a fresh engine.
func CheckCheckpoint(cand engine.Engine) error {
	cp, ok := cand.(engine.Checkpointer)
	if !ok {
		return ErrNotCheckpointer
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	s := newScenario(cand, func(t time.Time) string { return t.Sub(x).String() })
	var seq uint64 = seqBase
	layers := []catalog.Layer{catalog.L2, catalog.L1}

	// write applies records one at a time, so every Seq is a token to probe.
	write := func(recs ...engine.Record) error {
		for _, r := range recs {
			seq++
			r.Seq = seq
			if err := s.write(r); err != nil {
				return err
			}
		}
		return nil
	}
	rec := func(sub engine.Subject, p lifecycle.Producer, at time.Time, kind lifecycle.Kind, ttl time.Duration) engine.Record {
		r := engine.Record{Layer: catalog.L2, Subject: sub, Producer: p, EventTime: at, Kind: kind, TTL: ttl}
		if kind == lifecycle.Observe {
			r.Payload = []byte(p)
		}
		return r
	}
	// checkpoint writes one at c for the pod's three prefixes: its forward edges,
	// its existence, and the node's reverse edges. A refusal is fine.
	checkpoint := func(pod, node identity.Fingerprint, c time.Time) error {
		for _, err := range []error{
			cp.CheckpointEdges(catalog.L2, pod, engine.Forward, c),
			cp.CheckpointEntity(catalog.L2, pod, c),
			cp.CheckpointEdges(catalog.L2, node, engine.Reverse, c),
		} {
			if err != nil && !errors.Is(err, engine.ErrInvalid) {
				return fmt.Errorf("writing a checkpoint at %s: %w", c.Sub(x), err)
			}
		}
		return nil
	}
	probe := func(what string, fps []identity.Fingerprint, first uint64, times []time.Time, floor time.Time) error {
		if err := s.check(fps, layers, times, tokensAround(first, seq), floor); err != nil {
			return fmt.Errorf("%s: %w", what, err)
		}
		return nil
	}
	around := func(cs ...time.Duration) []time.Time {
		var out []time.Time
		for _, c := range cs {
			out = append(out, x.Add(c-time.Nanosecond), x.Add(c), x.Add(c+time.Nanosecond))
		}
		return out
	}
	pair := func(name string) (identity.Fingerprint, identity.Fingerprint, error) {
		pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, name+"-pod")
		if err != nil {
			return pod, pod, err
		}
		node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, name+"-node")
		return pod, node, err
	}

	// 1. What a checkpoint depends on. P observes at 10s, Q at 12s for five
	// seconds, P deletes at 20s; a checkpoint at 30s holds nothing alive and
	// depends on the Delete (sequence 3). A token that has seen only the first
	// two records must see P alive, so it must not use the checkpoint.
	{
		pod, node, err := pair("checkpoint-w")
		if err != nil {
			return err
		}
		edge, entity := engine.EdgeSubject(pod, node, catalog.ScheduledOn), engine.EntitySubject(pod)
		first := seq + 1
		for _, sub := range []engine.Subject{edge, entity} {
			if err := write(
				rec(sub, "p", x.Add(10*time.Second), lifecycle.Observe, 0),
				rec(sub, "q", x.Add(12*time.Second), lifecycle.Observe, 5*time.Second),
				rec(sub, "p", x.Add(20*time.Second), lifecycle.Delete, 0),
			); err != nil {
				return err
			}
		}
		if err := checkpoint(pod, node, x.Add(30*time.Second)); err != nil {
			return err
		}
		times := append(around(10*time.Second, 17*time.Second, 20*time.Second, 30*time.Second), x.Add(time.Minute), x.Add(time.Hour))
		if err := probe("a checkpoint over a Delete an older token must not see", []identity.Fingerprint{pod, node}, first, times, engine.MinEventTime); err != nil {
			return err
		}
	}

	// 2. The boundaries of a checkpoint at 100s. A alone holds the edge from 50s.
	{
		pod, node, err := pair("checkpoint-edge")
		if err != nil {
			return err
		}
		edge, entity := engine.EdgeSubject(pod, node, catalog.ScheduledOn), engine.EntitySubject(pod)
		first := seq + 1
		both := func(p lifecycle.Producer, at time.Duration, kind lifecycle.Kind, ttl time.Duration) error {
			return write(rec(edge, p, x.Add(at), kind, ttl), rec(entity, p, x.Add(at), kind, ttl))
		}
		fps := []identity.Fingerprint{pod, node}
		if err := both("a", 50*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := checkpoint(pod, node, x.Add(100*time.Second)); err != nil {
			return err
		}
		// Late by a nanosecond: the checkpoint at 100s no longer holds.
		if err := both("a", 100*time.Second-time.Nanosecond, lifecycle.Delete, 0); err != nil {
			return err
		}
		if err := probe("a record a nanosecond before the checkpoint's instant", fps, first, around(50*time.Second, 100*time.Second, 101*time.Second), engine.MinEventTime); err != nil {
			return err
		}
		// A second checkpoint at 200s; a record exactly at it and one after it do
		// not make it untrue.
		if err := both("b", 150*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := checkpoint(pod, node, x.Add(200*time.Second)); err != nil {
			return err
		}
		if err := both("a", 200*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := both("b", 200*time.Second+time.Nanosecond, lifecycle.Delete, 0); err != nil {
			return err
		}
		if err := probe("records exactly at and just after the checkpoint's instant", fps, first, around(150*time.Second, 200*time.Second, 201*time.Second), engine.MinEventTime); err != nil {
			return err
		}
		// A third, built on the second and the records after it; then a record
		// before it that is not before the second.
		if err := checkpoint(pod, node, x.Add(300*time.Second)); err != nil {
			return err
		}
		if err := both("c", 300*time.Second-time.Nanosecond, lifecycle.Observe, 10*time.Second); err != nil {
			return err
		}
		if err := both("b", 400*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := probe("a checkpoint built on another, then a late record", fps, first, around(200*time.Second, 300*time.Second, 310*time.Second, 400*time.Second), engine.MinEventTime); err != nil {
			return err
		}
	}

	// 3. A checkpoint built on an older one depends on all it did. Four peers, so
	// the answer says which references are alive. X observes at 15s, P at 0s, Q at
	// 3s and P deletes at 5s; a checkpoint at 5s+1ns holds {Q}. After Y at 30s, a
	// checkpoint at 20s+1ns is built on it and on X: it holds {X, Q} and depends on
	// sequence 4, not only on the records it walked. A token that has seen only
	// the first two records must see {X, P}.
	{
		pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "checkpoint-inc-pod")
		if err != nil {
			return err
		}
		node := func(n string) identity.Fingerprint {
			fp, _ := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "checkpoint-inc-"+n)
			return fp
		}
		peers := map[string]identity.Fingerprint{"x": node("x"), "p": node("p"), "q": node("q"), "y": node("y")}
		edge := func(n string) engine.Subject { return engine.EdgeSubject(pod, peers[n], catalog.ScheduledOn) }
		first := seq + 1
		if err := write(
			rec(edge("x"), "h", x.Add(15*time.Second), lifecycle.Observe, 0),
			rec(edge("p"), "h", x.Add(0), lifecycle.Observe, 0),
			rec(edge("q"), "h", x.Add(3*time.Second), lifecycle.Observe, 0),
			rec(edge("p"), "h", x.Add(5*time.Second), lifecycle.Delete, 0),
		); err != nil {
			return err
		}
		if err := cp.CheckpointEdges(catalog.L2, pod, engine.Forward, x.Add(5*time.Second+time.Nanosecond)); err != nil && !errors.Is(err, engine.ErrInvalid) {
			return err
		}
		if err := write(rec(edge("y"), "h", x.Add(30*time.Second), lifecycle.Observe, 0)); err != nil {
			return err
		}
		if err := cp.CheckpointEdges(catalog.L2, pod, engine.Forward, x.Add(20*time.Second+time.Nanosecond)); err != nil && !errors.Is(err, engine.ErrInvalid) {
			return err
		}
		fps := []identity.Fingerprint{pod, peers["x"], peers["p"], peers["q"], peers["y"]}
		times := append(around(5*time.Second, 15*time.Second, 20*time.Second, 25*time.Second, 30*time.Second), x.Add(time.Hour))
		if err := probe("a checkpoint built on an older one", fps, first, times, engine.MinEventTime); err != nil {
			return err
		}
	}

	// 4. A retention exactly at a checkpoint's instant. The horizon is far from the
	// scenarios above, which are done with.
	{
		pod, node, err := pair("checkpoint-retain")
		if err != nil {
			return err
		}
		edge, entity := engine.EdgeSubject(pod, node, catalog.ScheduledOn), engine.EntitySubject(pod)
		base := 10000 * time.Second
		h := x.Add(base + 200*time.Second)
		fps := []identity.Fingerprint{pod, node}
		both := func(p lifecycle.Producer, at time.Duration, kind lifecycle.Kind, ttl time.Duration) error {
			return write(rec(edge, p, x.Add(base+at), kind, ttl), rec(entity, p, x.Add(base+at), kind, ttl))
		}
		if err := both("q", 100*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := checkpoint(pod, node, h); err != nil {
			return err
		}
		retained := seq // L: only tokens at or above it are asked about from here on
		if err := s.retain(h); err != nil {
			return err
		}
		// At or below the horizon none may be written; whether it is refused or
		// quietly written, a record at the horizon must still be seen with the
		// baseline's state under it.
		if err := checkpoint(pod, node, h); err != nil {
			return err
		}
		if err := both("p", 200*time.Second, lifecycle.Observe, 0); err != nil {
			return err
		}
		if err := checkpoint(pod, node, h.Add(time.Second)); err != nil {
			return err
		}
		if err := both("q", 250*time.Second, lifecycle.Delete, 0); err != nil {
			return err
		}
		times := []time.Time{h, h.Add(time.Nanosecond), h.Add(50*time.Second - time.Nanosecond), h.Add(50 * time.Second), h.Add(50*time.Second + time.Nanosecond), h.Add(time.Hour)}
		toks := []uint64{retained}
		for t := retained + 1; t <= seq; t++ {
			toks = append(toks, t)
		}
		toks = append(toks, seq+1, 1<<63, engine.Latest-1, engine.Latest)
		if err := s.check(fps, layers, times, toks, h); err != nil {
			return fmt.Errorf("a checkpoint at the horizon: %w", err)
		}
	}
	return nil
}
