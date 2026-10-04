package runner_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func fp(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

// The digest of an answer tells two different answers apart, by anything in
// them: who the neighbors are, how they are related, their order, which list of a
// batch each belongs to, whether an entity is alive, and every field of every
// record in a window. The reference engine and the candidates are reduced by the
// same function, so this is the only thing that checks it.
func TestAnswerDigestsTellAnswersApart(t *testing.T) {
	t.Parallel()

	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	pods := []identity.Fingerprint{
		fp(t, catalog.K8sPod, catalog.K8sPodUID, "a"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "b"), fp(t, catalog.K8sPod, catalog.K8sPodUID, "c"),
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	world := func(edit func(rs []engine.Record) []engine.Record) *oracle.Oracle {
		var rs []engine.Record
		for i, p := range pods[:2] {
			rs = append(rs, engine.Record{
				Layer: catalog.L2, Subject: engine.EdgeSubject(p, node, catalog.ScheduledOn), Producer: "a",
				EventTime: start.Add(time.Duration(i) * time.Second), Kind: lifecycle.Observe, Payload: []byte{byte('a' + i)},
			})
		}
		rs = append(rs, engine.Record{
			Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "a", EventTime: start, Kind: lifecycle.Observe, Payload: []byte("e"),
		})
		o := oracle.New()
		rs = edit(rs)
		for i := range rs {
			rs[i].Seq = uint64(i + 1)
		}
		if err := o.Write(rs); err != nil {
			t.Fatal(err)
		}
		return o
	}
	same := func(rs []engine.Record) []engine.Record { return rs }
	ask := func(o engine.Engine, q runner.Query) runner.Answer {
		t.Helper()
		q.Layer, q.AsOf, q.At, q.From, q.To = catalog.L2, engine.Latest, start.Add(time.Hour), start, start.Add(time.Hour)
		a, err := runner.Ask(o, q)
		if err != nil {
			t.Fatal(err)
		}
		return a
	}

	reverse := runner.Query{Op: runner.OpNeighbors, Dir: engine.Reverse, Fps: []identity.Fingerprint{node}}
	window := runner.Query{Op: runner.OpWindow, Dir: engine.Reverse, Fps: []identity.Fingerprint{node}}
	batch := runner.Query{Op: runner.OpBatch, Dir: engine.Reverse, Fps: []identity.Fingerprint{node, pods[2]}}
	alive := runner.Query{Op: runner.OpAlive, Fps: []identity.Fingerprint{node}}

	base := world(same)
	for name, c := range map[string]struct {
		q    runner.Query
		edit func(rs []engine.Record) []engine.Record
	}{
		"a neighbor fewer": {reverse, func(rs []engine.Record) []engine.Record { return rs[1:] }},
		"another relation": {reverse, func(rs []engine.Record) []engine.Record { rs[0].Subject.Relation = catalog.RunsOn; return rs }},
		"another peer":     {reverse, func(rs []engine.Record) []engine.Record { rs[0].Subject.A = pods[2]; return rs }},
		"a record fewer":   {window, func(rs []engine.Record) []engine.Record { return rs[1:] }},
		"another payload":  {window, func(rs []engine.Record) []engine.Record { rs[0].Payload = []byte("z"); return rs }},
		"another event time": {window, func(rs []engine.Record) []engine.Record {
			rs[0].EventTime = rs[0].EventTime.Add(time.Millisecond)
			return rs
		}},
		"another producer":         {window, func(rs []engine.Record) []engine.Record { rs[0].Producer = "b"; return rs }},
		"another ttl":              {window, func(rs []engine.Record) []engine.Record { rs[0].TTL = time.Hour; return rs }},
		"a delete":                 {window, func(rs []engine.Record) []engine.Record { rs[1].Kind, rs[1].Payload = lifecycle.Delete, nil; return rs }},
		"a neighbor moved":         {batch, func(rs []engine.Record) []engine.Record { rs[0].Subject.B = pods[2]; return rs }},
		"the entity is gone":       {alive, func(rs []engine.Record) []engine.Record { return rs[:2] }},
		"control: nothing changed": {alive, func(rs []engine.Record) []engine.Record { return rs }},
	} {
		got, was := ask(world(c.edit), c.q), ask(base, c.q)
		if name == "control: nothing changed" {
			if got != was {
				t.Errorf("%s: the same world answered %+v, then %+v", name, was, got)
			}
			continue
		}
		if got.Digest == was.Digest {
			t.Errorf("%s: the answer changed and its digest did not (%+v)", name, got)
		}
	}

	if d := ask(base, reverse).Digest; len(d) != 32 {
		t.Errorf("a digest of %d characters: %q", len(d), d)
	}

	// Sizes: the number of items in the answer.
	if a := ask(base, reverse); a.Size != 2 {
		t.Errorf("two neighbors have size %d", a.Size)
	}
	if a := ask(base, window); a.Size != 3-1 {
		t.Errorf("a window of two edge records has size %d", a.Size)
	}
	if a := ask(base, batch); a.Size != 2 {
		t.Errorf("a batch with two neighbors in all has size %d", a.Size)
	}
	if a := ask(base, alive); a.Size != 1 {
		t.Errorf("a live entity has size %d", a.Size)
	}
	dead := world(func(rs []engine.Record) []engine.Record { return rs[:2] })
	if a := ask(dead, alive); a.Size != 0 {
		t.Errorf("an entity that does not exist has size %d", a.Size)
	}

	// Where the boundary between the lists of a batch falls is part of the answer.
	two := runner.Query{Op: runner.OpBatch, Dir: engine.Reverse, Fps: []identity.Fingerprint{node, node}}
	if a := ask(base, two); a.Size != 4 {
		t.Errorf("the same entity twice in a batch has size %d", a.Size)
	}
	if ask(base, batch).Digest == ask(base, two).Digest {
		t.Error("a batch of two different entities and a batch of one entity twice have the same digest")
	}
	if _, err := runner.Ask(base, runner.Query{Op: "nonsense", Layer: catalog.L2}); err == nil {
		t.Error("a read of an unknown kind was answered")
	}
}

// The digest of the questions leaves out the answers.
func TestQueriesDigestLeavesOutTheAnswers(t *testing.T) {
	t.Parallel()

	qs := []runner.Query{{Group: "g", Op: runner.OpAlive, Age: runner.AgeNow, Expect: "aa", Size: 1}}
	d1, err := runner.QueriesDigest(qs)
	if err != nil {
		t.Fatal(err)
	}
	qs[0].Expect, qs[0].Size = "bb", 0
	if d2, _ := runner.QueriesDigest(qs); d2 != d1 {
		t.Error("the digest of the questions changed with the answers")
	}
	qs[0].Age = runner.Age1d
	if d3, _ := runner.QueriesDigest(qs); d3 == d1 {
		t.Error("the digest of the questions did not change with a question")
	}
}
