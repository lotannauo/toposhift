package mcp

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// numbered is the fingerprint of the n-th entity of a type, so that the large
// scenarios need no hashing.
func numbered(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	binary.BigEndian.PutUint32(h[:4], uint32(n))
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// scenario builds a store from records.
type scenario struct {
	t    testing.TB
	st   *memstore.Store
	recs []store.Record
}

func newScenario(t testing.TB) *scenario {
	return &scenario{t: t, st: openMem(t, memstore.Options{Policy: lifecycle.Policy{Rank: rankObjs}})}
}

func (sc *scenario) add(rs ...store.Record) { sc.recs = append(sc.recs, rs...) }

// entity observes an entity with a name, from nine o'clock.
func (sc *scenario) entity(fp identity.Fingerprint, name string, at time.Time) {
	sc.add(observe(ent(fp), objs, at, 0, `{"name":"`+name+`"}`))
}

func (sc *scenario) sched(pod, node identity.Fingerprint, at time.Time) {
	sc.add(observe(edge(pod, node, catalog.ScheduledOn), objs, at, 0, `{}`))
}

func (sc *scenario) unsched(pod, node identity.Fingerprint, at time.Time) {
	sc.add(deleted(edge(pod, node, catalog.ScheduledOn), objs, at))
}

func (sc *scenario) runsOn(node, host identity.Fingerprint, at time.Time) {
	sc.add(observe(edge(node, host, catalog.RunsOn), agent, at, 0, `{}`))
}

func (sc *scenario) commit() *Source {
	sc.t.Helper()
	newFeed(sc.t, sc.st).write(sc.recs...)
	s, err := NewSource(Config{Store: sc.st, Rank: rankObjs, RangeReads: true})
	if err != nil {
		sc.t.Fatal(err)
	}
	return s
}

// bigNode is a host with a node on it and n pods scheduled on the node from at.
func bigNode(t testing.TB, n int, at time.Time) (s *Source, st *memstore.Store, host, node identity.Fingerprint, pods []identity.Fingerprint) {
	sc := newScenario(t)
	host, node = numbered(catalog.Host, 1), numbered(catalog.K8sNode, 1)
	sc.add(observe(ent(host), agent, hm(9, 0), 0, `{}`))
	sc.entity(node, "big-node", hm(9, 0))
	sc.runsOn(node, host, hm(9, 0))
	for i := range n {
		p := numbered(catalog.K8sPod, i+1)
		pods = append(pods, p)
		sc.entity(p, fmt.Sprintf("pod-%04d", i), at)
		sc.sched(p, node, at)
	}
	return sc.commit(), sc.st, host, node, pods
}

// Every cap of the walk gives its sentence, and the counts are exact. The oracle
// is the package's own result.
func TestBlastRadiusCaps(t *testing.T) {
	s, st, host, _, _ := bigNode(t, 4100, hm(9, 0))
	win := interval.Window{From: hm(10, 0), To: hm(10, 30), AsOf: store.Latest}
	oracle, err := blastradius.NewWalker(st).Impact(ctx, host, win, blastradius.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if oracle.Truncated.Members == 0 || len(oracle.Truncated.Frontier) == 0 {
		t.Fatalf("the scenario cuts nothing: %+v", oracle.Truncated)
	}

	out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: host.String(), From: w10, To: w1030, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	total := len(oracle.Members) + oracle.Truncated.Members
	if out.Counts.Members != total || !out.Truncation.Incomplete || out.Truncation.MembersUnlisted != oracle.Truncated.Members {
		t.Errorf("counts %+v, truncation %+v; oracle %d members and %+v", out.Counts, out.Truncation, total, oracle.Truncated)
	}
	for i, c := range oracle.Truncated.Frontier {
		if out.Truncation.FrontierCuts[i] != (FrontierCut{Depth: c.Depth, Dropped: len(c.Dropped)}) {
			t.Errorf("frontier cut %d: %+v, want depth %d, %d dropped", i, out.Truncation.FrontierCuts[i], c.Depth, len(c.Dropped))
		}
		want := fmt.Sprintf("At depth %d, %d entities were reached but not expanded: the walk expands at most 1024 entities from each depth.", c.Depth, len(c.Dropped))
		if !contains(out.Notes, want) {
			t.Errorf("no sentence %q in %q", want, out.Notes)
		}
	}
	want := fmt.Sprintf("The walk found %d more members than the 4096 it lists; they are counted and not listed.", oracle.Truncated.Members)
	if !contains(out.Notes, want) {
		t.Errorf("no sentence %q in %q", want, out.Notes)
	}

	// The display cap, and the hard cap, are other things: 500 members are 500 KiB.
	b, _ := json.Marshal(out)
	if len(b) > maxAnswerBytes {
		t.Errorf("the answer is %d bytes, over the cap of %d", len(b), maxAnswerBytes)
	}
	cut := 500 - len(out.Members)
	if cut <= 0 || out.Counts.Shown != len(out.Members) {
		t.Fatalf("%d members shown, %d cut", len(out.Members), cut)
	}
	if want := fmt.Sprintf("The answer was cut at 256 KiB: the last %d entries of members were left out.", cut); !contains(out.Notes, want) {
		t.Errorf("no sentence %q in %q", want, out.Notes)
	}
	if want := fmt.Sprintf("Showing 500 of %d members, nearest first.", total); !contains(out.Notes, want) {
		t.Errorf("no sentence %q in %q", want, out.Notes)
	}
	if out.Counts.Members != total {
		t.Errorf("the count of members is %d after the cap, want the exact %d", out.Counts.Members, total)
	}
}

// The depth the client asked for is a cap too, and is reported with the number of
// members whose edges were not followed.
func TestBlastRadiusDepthCap(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	out, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, Depth: 1})
	if err != nil {
		t.Fatal(err)
	}
	// N1 and N2 are the members at depth 1, and pods are scheduled on both.
	want := "The walk stopped at depth 1, the depth limit; 2 members at that depth have edges that were not followed."
	if !contains(out.Notes, want) || out.Truncation.DepthCut != 2 || !out.Truncation.Incomplete || len(out.Members) != 2 {
		t.Errorf("notes %q, truncation %+v, %d members", out.Notes, out.Truncation, len(out.Members))
	}
	whole, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	if whole.Truncation.Incomplete || whole.Truncation.DepthCut != 0 {
		t.Errorf("the whole walk is incomplete: %+v", whole.Truncation)
	}
}

// The display cap shortens a complete answer, with exact counts; quarantined
// entries are listed whatever the cap.
func TestDisplayCapKeepsQuarantine(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	in := WhatChangedInput{
		Scope: []string{w.h2.String(), w.n3.String(), w.p5.String(), w.n1.String(), w.p1.String(), w.p2.String(), w.p3.String(), w.p4.String(), w.p6.String()},
		From:  w10, To: w1030, Limit: 1,
	}
	out, err := s.WhatChanged(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Quarantined) < 2 || out.Counts.Quarantined.Entities+out.Counts.Quarantined.Relations != len(out.Quarantined) {
		t.Errorf("%d quarantined listed, counts %+v", len(out.Quarantined), out.Counts.Quarantined)
	}
	for name, l := range map[string][]ChangeEntry{"created": out.Created, "deleted": out.Deleted, "changed": out.Changed, "transient": out.Transient} {
		if len(l) > 1 {
			t.Errorf("%s lists %d entries under limit 1", name, len(l))
		}
	}
	if n := out.Counts.Deleted.Entities + out.Counts.Deleted.Relations; n < 2 {
		t.Fatalf("the scenario deletes %d elements", n)
	}
	want := fmt.Sprintf("Showing 1 of %d deleted entries, entities first.", out.Counts.Deleted.Entities+out.Counts.Deleted.Relations)
	if !contains(out.Notes, want) {
		t.Errorf("no sentence %q in %q", want, out.Notes)
	}
	if len(out.Events) != 1 || out.Counts.Events < 2 {
		t.Errorf("%d events listed of %d", len(out.Events), out.Counts.Events)
	}

	// Two nodes run on the quarantined host: two edges are listed under limit 1.
	n8 := fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, "node-uid-8")
	w.feed.write(observe(ent(n8), objs, hm(9, 0), 0, `{"name":"worker-8"}`), observe(edge(n8, w.h2, catalog.RunsOn), agent, hm(9, 0), 0, `{}`))
	blast, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h2.String(), From: w10, To: w1030, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(blast.Quarantined) != 2 || blast.Counts.Quarantined != 2 {
		t.Errorf("blast: %d quarantined listed of %d", len(blast.Quarantined), blast.Counts.Quarantined)
	}
	limited, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(limited.Members) != 1 || limited.Counts.Members <= 1 ||
		!contains(limited.Notes, fmt.Sprintf("Showing 1 of %d members, nearest first.", limited.Counts.Members)) {
		t.Errorf("limit 1: %d members shown of %d; notes %q", len(limited.Members), limited.Counts.Members, limited.Notes)
	}
}

// The caps of the diff: past 4096 entries a bucket lists the first and counts the
// rest, and says that the event log lacks their boundaries.
func TestWhatChangedCaps(t *testing.T) {
	s, st, _, node, _ := bigNode(t, 4100, hm(10, 10))
	oracle, err := graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{node}, hm(10, 0), hm(10, 30), store.Latest, graphdiff.Options{Rank: rankObjs})
	if err != nil {
		t.Fatal(err)
	}
	if oracle.Truncated.Created == 0 {
		t.Fatalf("the scenario cuts nothing: %+v", oracle.Truncated)
	}
	out, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{node.String()}, From: w10, To: w1030, Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if !out.Truncation.Incomplete || out.Truncation.Created != oracle.Truncated.Created ||
		out.Counts.Created != (Pair{Entities: oracle.Counts.Created.Entities, Relations: oracle.Counts.Created.Relations}) {
		t.Errorf("truncation %+v, counts %+v; oracle %+v, %+v", out.Truncation, out.Counts.Created, oracle.Truncated, oracle.Counts.Created)
	}
	want := fmt.Sprintf("The diff lists at most 4096 created entries; %d more are counted and not listed, and the event log lacks their boundaries.", oracle.Truncated.Created)
	if !contains(out.Notes, want) {
		t.Errorf("no sentence %q in %q", want, out.Notes)
	}
	b, _ := json.Marshal(out)
	if len(b) > maxAnswerBytes {
		t.Errorf("the answer is %d bytes", len(b))
	}
	if !strings.Contains(strings.Join(out.Notes, "\n"), "The answer was cut at 256 KiB") {
		t.Errorf("no cut sentence in %q", out.Notes)
	}
}

// The caps of the co-residency query, each with its sentence.
func TestCoResidentsCaps(t *testing.T) {
	win := interval.Window{From: hm(10, 0), To: hm(10, 30), AsOf: store.Latest}
	check := func(t *testing.T, s *Source, st store.Store, seed identity.Fingerprint) (CoResidentsOutput, coresidency.Result) {
		t.Helper()
		oracle, err := coresidency.NewFinder(st).Find(ctx, seed, win, coresidency.Options{})
		if err != nil {
			t.Fatal(err)
		}
		out, err := s.CoResidents(ctx, CoResidentsInput{Pod: seed.String(), From: w10, To: w1030, Limit: 5})
		if err != nil {
			t.Fatal(err)
		}
		if out.Counts.CoResidents != len(oracle.CoResidents) {
			t.Errorf("%d co-residents, oracle %d", out.Counts.CoResidents, len(oracle.CoResidents))
		}
		return out, oracle
	}

	t.Run("per node", func(t *testing.T) {
		s, st, _, node, pods := bigNode(t, 1100, hm(9, 0))
		_ = node
		out, oracle := check(t, s, st, pods[0])
		dropped := 0
		for _, v := range oracle.Truncated.PerVia {
			dropped += v.Dropped
		}
		if dropped == 0 || out.Truncation.PerViaDropped != dropped || !out.Truncation.Incomplete {
			t.Fatalf("truncation %+v, oracle %+v", out.Truncation, oracle.Truncated)
		}
		if want := fmt.Sprintf("%d entries were dropped: the query keeps at most 1024 for each node or host.", dropped); !contains(out.Notes, want) {
			t.Errorf("no sentence %q in %q", want, out.Notes)
		}
		if want := fmt.Sprintf("Showing 5 of %d co-residents, longest overlap first.", len(oracle.CoResidents)); !contains(out.Notes, want) {
			t.Errorf("no sentence %q in %q", want, out.Notes)
		}
	})

	t.Run("nodes and hosts", func(t *testing.T) {
		sc := newScenario(t)
		seed := numbered(catalog.K8sPod, 1)
		sc.entity(seed, "seed", hm(9, 0))
		// The seed moves through 70 nodes, 20 s on each, and the first 20 of them
		// are on a host each.
		for i := range 70 {
			n := numbered(catalog.K8sNode, i+1)
			sc.entity(n, fmt.Sprintf("node-%02d", i), hm(9, 0))
			start := hm(10, 0).Add(time.Duration(i) * 20 * time.Second)
			sc.sched(seed, n, start)
			sc.unsched(seed, n, start.Add(20*time.Second))
			if i < 20 {
				h := numbered(catalog.Host, i+1)
				sc.add(observe(ent(h), agent, hm(9, 0), 0, `{}`))
				sc.runsOn(n, h, hm(9, 0))
			}
		}
		s := sc.commit()
		out, oracle := check(t, s, sc.st, seed)
		if len(oracle.Truncated.Nodes) == 0 || len(oracle.Truncated.Hosts) == 0 {
			t.Fatalf("the scenario cuts nothing: %+v", oracle.Truncated)
		}
		if out.Truncation.NodesNotRead != len(oracle.Truncated.Nodes) || out.Truncation.HostsNotRead != len(oracle.Truncated.Hosts) {
			t.Errorf("truncation %+v, oracle %+v", out.Truncation, oracle.Truncated)
		}
		for _, want := range []string{
			fmt.Sprintf("%d nodes of the pod were not read: the query reads at most 64.", len(oracle.Truncated.Nodes)),
			fmt.Sprintf("%d hosts were not read: the query reads at most 16.", len(oracle.Truncated.Hosts)),
		} {
			if !contains(out.Notes, want) {
				t.Errorf("no sentence %q in %q", want, out.Notes)
			}
		}
	})

	t.Run("results", func(t *testing.T) {
		sc := newScenario(t)
		seed := numbered(catalog.K8sPod, 1)
		sc.entity(seed, "seed", hm(9, 0))
		id := 1
		for i := range 5 {
			n := numbered(catalog.K8sNode, i+1)
			sc.entity(n, fmt.Sprintf("node-%d", i), hm(9, 0))
			sc.sched(seed, n, hm(10, i*2))
			sc.unsched(seed, n, hm(10, i*2+1))
			for range 1000 {
				id++
				p := numbered(catalog.K8sPod, id)
				sc.entity(p, "peer", hm(9, 0))
				sc.sched(p, n, hm(9, 0))
			}
		}
		s := sc.commit()
		out, oracle := check(t, s, sc.st, seed)
		if oracle.Truncated.Results == 0 || out.Truncation.ResultsDropped != oracle.Truncated.Results {
			t.Fatalf("truncation %+v, oracle %+v", out.Truncation, oracle.Truncated)
		}
		if want := fmt.Sprintf("%d co-residents were dropped: the query keeps at most 4096.", oracle.Truncated.Results); !contains(out.Notes, want) {
			t.Errorf("no sentence %q in %q", want, out.Notes)
		}
	})
}

// The caps that are too costly to reach end to end give the sentences their
// numbers say.
func TestCapSentences(t *testing.T) {
	ids := func(n int) []identity.Fingerprint {
		return slices.Repeat([]identity.Fingerprint{numbered(catalog.K8sPod, 1)}, n)
	}
	got := blastNotes(blastradius.Truncation{
		Depth: ids(3), Frontier: []blastradius.FrontierCut{{Depth: 2, Dropped: ids(7)}}, Members: 9,
	}, 4, blastradius.DefaultPolicy())
	want := []string{
		"The walk stopped at depth 4, the depth limit; 3 members at that depth have edges that were not followed.",
		"At depth 2, 7 entities were reached but not expanded: the walk expands at most 1024 entities from each depth.",
		"The walk found 9 more members than the 4096 it lists; they are counted and not listed.",
	}
	if !slices.Equal(got, want) {
		t.Errorf("blast notes %q", got)
	}

	got = diffNotes(graphdiff.Truncation{Deleted: 2, Changed: 3, Transient: 4, Quarantined: 5})
	want = []string{
		"The diff lists at most 4096 deleted entries; 2 more are counted and not listed, and the event log lacks their boundaries.",
		"The diff lists at most 4096 changed entries; 3 more are counted and not listed, and the event log lacks their boundaries.",
		"The diff lists at most 4096 transient entries; 4 more are counted and not listed, and the event log lacks their boundaries.",
		"The diff lists at most 4096 quarantined entries; 5 more are counted and not listed, and the event log lacks their boundaries.",
	}
	if !slices.Equal(got, want) {
		t.Errorf("diff notes %q", got)
	}

	got = coResidentNotes(CoResidentCut{SuppressedDropped: 6})
	if want := []string{"6 suppressed co-residents were dropped: the query keeps at most 4096."}; !slices.Equal(got, want) {
		t.Errorf("co-resident notes %q", got)
	}
}

type pile struct {
	Head  string   `json:"head"`
	First []string `json:"first"`
	Last  []string `json:"last"`
}

// The hard cap drops whole trailing entries, from the first list given until it
// is empty and then from the next, and says how many.
func TestFitAnswer(t *testing.T) {
	mk := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = strings.Repeat("x", 100)
		}
		return out
	}
	size := func(p pile) int { b, _ := json.Marshal(p); return len(b) }

	p := pile{Head: "h", First: mk(10), Last: mk(10)}
	if notes, err := fitAnswer(&p, 1<<20, namedTail{"first", listTail(&p.First)}, namedTail{"last", listTail(&p.Last)}); err != nil || notes != nil || len(p.First) != 10 {
		t.Errorf("an answer that fits was cut: %v %v %d", notes, err, len(p.First))
	}

	p = pile{Head: "h", First: mk(10), Last: mk(10)}
	// Room for the slack and a few entries.
	max := budgetSlack + 1500
	notes, err := fitAnswer(&p, max, namedTail{"first", listTail(&p.First)}, namedTail{"last", listTail(&p.Last)})
	if err != nil {
		t.Fatal(err)
	}
	if size(p) > max-budgetSlack || len(p.Last) != 10 || len(p.First) == 0 || len(p.First) >= 10 {
		t.Errorf("%d bytes, %d first, %d last", size(p), len(p.First), len(p.Last))
	}
	if want := fmt.Sprintf("The answer was cut at %d KiB: the last %d entries of first were left out.", max>>10, 10-len(p.First)); len(notes) != 1 || notes[0] != want {
		t.Errorf("notes %q, want %q", notes, want)
	}
	// One more entry would not have fit.
	more := pile{Head: "h", First: mk(len(p.First) + 1), Last: mk(10)}
	if size(more) <= max-budgetSlack {
		t.Errorf("an entry too many was dropped: %d bytes fit in %d", size(more), max-budgetSlack)
	}

	// When the first list is empty the next is cut.
	p = pile{Head: "h", First: mk(3), Last: mk(10)}
	max = budgetSlack + 500
	notes, err = fitAnswer(&p, max, namedTail{"first", listTail(&p.First)}, namedTail{"last", listTail(&p.Last)})
	if err != nil {
		t.Fatal(err)
	}
	if len(p.First) != 0 || len(p.Last) == 0 || len(p.Last) >= 10 || len(notes) != 2 {
		t.Errorf("%d first, %d last, notes %q", len(p.First), len(p.Last), notes)
	}
}
