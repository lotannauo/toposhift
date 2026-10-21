package xtmodel

import (
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// rebuildStream is a stream with late records, a shrinking deadline, a delete and two
// producers, in batches, for what a planner remembers to matter.
func rebuildStream() [][]engine.Record {
	return [][]engine.Record{
		{observe(1, "a", 10, 0), observe(2, "b", 20, 30)},
		{observe(3, "a", 40, 5), del(4, "b", 25)},
		{observe(5, "a", 15, 10)},
		{observe(6, "a", 50, 0), observe(7, "b", 45, 9)},
	}
}

// TestARebuiltPlannerPlansAsTheOneItWasBuiltFrom: a planner rebuilt from the records of the
// log, whatever order they come in, remembers what the planner that wrote them does, and
// so plans the rest of a stream with the same statements.
func TestARebuiltPlannerPlansAsTheOneItWasBuiltFrom(t *testing.T) {
	stream := rebuildStream()
	live := newRig()
	var all []engine.Record
	for _, b := range stream[:3] {
		live.write(b...)
		all = append(all, b...)
	}

	for seed := uint64(1); seed <= 20; seed++ {
		shuffled := append([]engine.Record(nil), all...)
		rng := rand.New(rand.NewPCG(seed, seed))
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		rb := NewRebuilder()
		for _, r := range shuffled {
			rb.Add(r)
		}
		if rb.Len() != 2 {
			t.Fatalf("%d (subject, producer) pairs seen, want 2", rb.Len())
		}
		p := rb.Planner(live.img)
		if !reflect.DeepEqual(p.state, live.pl.state) {
			t.Fatalf("seed %d: a rebuilt planner remembers %v, the live one %v", seed, p.state, live.pl.state)
		}
		want, err := live.pl.Plan(live.n+1, stream[3])
		if err != nil {
			t.Fatal(err)
		}
		got, err := p.Plan(live.n+1, stream[3])
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got.Txn, want.Txn) || !reflect.DeepEqual(got.Counts, want.Counts) {
			t.Fatalf("seed %d: a rebuilt planner plans %+v, the live one %+v", seed, got.Txn.Ops, want.Txn.Ops)
		}
	}
}

// TestARebuilderKeepsTheLaterRecordOfAnInstant: of two records of one producer at one event
// time, the later by Seq is the one the planner remembers, in either order; and a payload
// is not kept.
func TestARebuilderKeepsTheLaterRecordOfAnInstant(t *testing.T) {
	first, second := observe(1, "a", 10, 50), observe(2, "a", 10, 5)
	for name, order := range map[string][]engine.Record{"in order": {first, second}, "reversed": {second, first}} {
		rb := NewRebuilder()
		for _, r := range order {
			rb.Add(r)
		}
		p := rb.Planner(newImage())
		st := p.state[segKey{edgeA, "a"}]
		if st.last != 10*sec || st.end != Instant(15*sec) {
			t.Errorf("%s: remembers %+v, want the record with the TTL of 5s (ends at 15s)", name, st)
		}
		for _, c := range rb.latest {
			if c.Payload != nil {
				t.Errorf("%s: a payload was kept", name)
			}
		}
	}
}
