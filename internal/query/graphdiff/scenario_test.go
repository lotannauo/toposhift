package graphdiff_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/graphdiff"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The scenarios fix exact answers, written before the code. Times are on
// 2026-01-01 UTC, written hh:mm; t1 is 10:00 and t2 10:30, so the window read
// is [10:00, 10:30:00.000000001), whose end is written To.

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

var (
	T1, T2 = hm(10, 0), hm(10, 30)
	To     = T2.Add(time.Nanosecond)
)

var (
	S, A, B, C, D = pod(1), pod(2), pod(3), pod(4), pod(5)
	N             = node(1)
	H             = host(1)
	pk            = []lifecycle.Producer{k8s}
)

// pc is an expected piece.
func pc(start, end time.Time, before bool, src lifecycle.EndSource, prods []lifecycle.Producer) interval.Piece {
	return interval.Piece{Span: interval.Span{Start: start, End: end}, Before: before, EndSource: src, Producers: prods}
}

// elem is an expected element, with its class and transitions.
func elem(sub store.Subject, class interval.Class, transitions int, ps ...interval.Piece) interval.Element {
	return interval.Element{Subject: sub, Layer: layerOf(sub), Pieces: ps, Class: class, Transitions: transitions}
}

func entries(els ...interval.Element) []string {
	out := []string{}
	for _, el := range els {
		out = append(out, entryString(graphdiff.Entry{Element: el}))
	}
	return out
}

func deletedEntry(el interval.Element, src lifecycle.EndSource, at time.Time) string {
	return entryString(graphdiff.Entry{Element: el, DeleteSource: src, DeletedAt: at})
}

func expect(t *testing.T, what string, got []string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if got == nil {
		got = []string{}
	}
	if d := diffLines(what, got, want); d != "" {
		t.Error(d)
	}
}

func expectReads(t *testing.T, got graphdiff.Reads, calls, windows, alive, ew int) {
	t.Helper()
	if want := (graphdiff.Reads{Calls: calls, Windows: windows, Alive: alive, EntityWindows: ew}); got != want {
		t.Errorf("reads %+v, want %+v", got, want)
	}
}

// world is a store with entities observed at 09:00 with no end and the
// payload {}.
func world(t *testing.T, opts memstore.Options, fps ...identity.Fingerprint) (*memstore.Store, *feed) {
	st := openStore(t, opts)
	f := newFeed(t, st)
	var rs []store.Record
	for _, fp := range fps {
		rs = append(rs, observe(ent(fp), k8s, hm(9, 0), 0))
	}
	if len(rs) > 0 {
		f.write(rs...)
	}
	return st, f
}

func diff30(t *testing.T, st store.Store, o graphdiff.Options, scope ...identity.Fingerprint) graphdiff.Result {
	t.Helper()
	return diffOf(t, st, scope, T1, T2, store.Latest, o)
}

// expectEmpty says that no bucket and no count holds anything.
func expectEmpty(t *testing.T, res graphdiff.Result) {
	t.Helper()
	if len(res.Created)+len(res.Deleted)+len(res.Changed)+len(res.Transient)+len(res.Quarantined) != 0 ||
		res.Counts != (graphdiff.Counts{}) || res.Truncated != (graphdiff.Truncation{}) {
		t.Errorf("a bucket is not empty: %+v", res)
	}
}

func scenarioA(t *testing.T, opts memstore.Options) *memstore.Store {
	st, f := world(t, opts, S, A, B, C, D, N, H)
	f.write(
		observe(sched(S, N), k8s, hm(9, 0), 0),
		observe(sched(A, N), k8s, hm(9, 58), 0), deleted(sched(A, N), k8s, hm(10, 5)),
		observe(sched(B, N), k8s, hm(10, 6), 0),
		observe(sched(C, N), k8s, hm(10, 10), 0), deleted(sched(C, N), k8s, hm(10, 20)),
		observe(sched(D, N), k8s, hm(9, 0), 0), deleted(sched(D, N), k8s, hm(10, 5)), observe(sched(D, N), k8s, hm(10, 12), 0),
	)
	return st
}

func TestScenarioATheBucketsAroundANode(t *testing.T) {
	sp := &spy{Store: scenarioA(t, memstore.Options{})}
	res := diff30(t, sp, graphdiff.Options{}, N)
	expect(t, "created", entryStrings(res.Created),
		entries(elem(sched(B, N), interval.ClassAppeared, 1, pc(hm(10, 6), To, false, lifecycle.EndUnknown, pk)))...)
	expect(t, "deleted", entryStrings(res.Deleted),
		deletedEntry(elem(sched(A, N), interval.ClassDisappeared, 1, pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk)), lifecycle.EndProducer, hm(10, 5)))
	expect(t, "changed", entryStrings(res.Changed))
	expect(t, "transient", entryStrings(res.Transient), entries(
		elem(sched(C, N), interval.ClassTransient, 2, pc(hm(10, 10), hm(10, 20), false, lifecycle.EndProducer, pk)),
		elem(sched(D, N), interval.ClassFlapping, 2,
			pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk), pc(hm(10, 12), To, false, lifecycle.EndUnknown, pk)),
	)...)
	if want := (graphdiff.Counts{Created: graphdiff.Count{Relations: 1}, Deleted: graphdiff.Count{Relations: 1}, Transient: graphdiff.Count{Relations: 2}}); res.Counts != want {
		t.Errorf("counts %+v, want %+v", res.Counts, want)
	}
	expect(t, "events", eventStrings(graphdiff.Events(res)),
		ts(hm(10, 5))+" disappear "+subjectString(sched(A, N))+" producer",
		ts(hm(10, 5))+" disappear "+subjectString(sched(D, N))+" producer",
		ts(hm(10, 6))+" appear "+subjectString(sched(B, N)),
		ts(hm(10, 10))+" appear "+subjectString(sched(C, N)),
		ts(hm(10, 12))+" appear "+subjectString(sched(D, N)),
		ts(hm(10, 20))+" disappear "+subjectString(sched(C, N))+" producer",
	)
	expectReads(t, res.Reads, 3, 3, 7, 7)
	if res.Token != 9+7 || res.PolicyVersion != 1 || res.IntervalPolicy != interval.PolicyVersion || !res.T1.Equal(T1) || !res.T2.Equal(T2) ||
		fmt.Sprint(res.Scope) != fmt.Sprint([]identity.Fingerprint{N}) {
		t.Errorf("header: token %d, versions %d %d, %s %s %v", res.Token, res.PolicyVersion, res.IntervalPolicy, ts(res.T1), ts(res.T2), res.Scope)
	}
	if sp.writes != 0 {
		t.Errorf("%d writes or retentions", sp.writes)
	}
}

func TestScenarioBAbsenceVersusIgnorance(t *testing.T) {
	E, F := pod(6), pod(7)
	st, f := world(t, memstore.Options{}, E, N)
	f.write(
		run(ent(F), k8s, hm(9, 0), 20*time.Minute, hm(9, 50)),
		observe(sched(E, N), k8s, hm(9, 50), 15*time.Minute),
		observe(sched(F, N), k8s, hm(9, 0), 0),
	)
	sp := &spy{Store: st}
	res := diff30(t, sp, graphdiff.Options{}, N)
	expect(t, "deleted", entryStrings(res.Deleted),
		deletedEntry(elem(sched(E, N), interval.ClassDisappeared, 1, pc(T1, hm(10, 5), true, lifecycle.EndLivenessExpiry, pk)), lifecycle.EndLivenessExpiry, hm(10, 5)),
		deletedEntry(elem(sched(F, N), interval.ClassDisappeared, 1, pc(T1, hm(10, 10), true, lifecycle.EndCascade, pk)), lifecycle.EndCascade, hm(10, 10)),
	)
	expect(t, "created", entryStrings(res.Created))
	expect(t, "transient", entryStrings(res.Transient))
	expectReads(t, res.Reads, 3, 3, 4, 4)
	if sp.writes != 0 {
		t.Errorf("%d writes or retentions", sp.writes)
	}
}

// scenarioC is a pod with no edges, described as from at 09:00 and as to at
// 10:10, diffed alone.
func scenarioC(t *testing.T, from, to string) graphdiff.Result {
	P := pod(9)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(observeP(ent(P), k8s, hm(9, 0), 0, from), observeP(ent(P), k8s, hm(10, 10), 0, to))
	sp := &spy{Store: st}
	res := diff30(t, sp, graphdiff.Options{}, P)
	if sp.writes != 0 {
		t.Errorf("%d writes or retentions", sp.writes)
	}
	expect(t, "created", entryStrings(res.Created))
	expect(t, "deleted", entryStrings(res.Deleted))
	expect(t, "transient", entryStrings(res.Transient))
	expectReads(t, res.Reads, 3, 2, 1, 1)
	return res
}

func changes(res graphdiff.Result) []string {
	out := []string{}
	for _, e := range res.Changed {
		out = append(out, fmt.Sprintf("%s %s", subjectString(e.Element.Subject), changeString(e.Change)))
	}
	return out
}

func keysChange(state, attrs []string) string {
	return changeString(&graphdiff.DescriptionChange{Kind: graphdiff.ChangeKeys, StateKeys: state, AttributeKeys: attrs, FromProducer: k8s, ToProducer: k8s})
}

func TestScenarioCStateVersusAttribute(t *testing.T) {
	P := subjectString(ent(pod(9)))
	pending := `{"name":"p","namespace":"ns","phase":"Pending"}`
	for _, tc := range []struct {
		name, to string
		want     []string
	}{
		{"C", `{"name":"p","namespace":"ns","phase":"Running"}`, []string{P + " " + keysChange([]string{"phase"}, nil)}},
		{"C′", `{"name":"p","namespace":"ns2","phase":"Pending"}`, []string{P + " " + keysChange(nil, []string{"namespace"})}},
		{"C″", `{"name":"p","namespace":"ns2","phase":"Running","pod_ips":["10.0.0.1"]}`, []string{P + " " + keysChange([]string{"phase", "pod_ips"}, []string{"namespace"})}},
		{"C⁗", pending, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			res := scenarioC(t, pending, tc.to)
			expect(t, "changed", changes(res), tc.want...)
			if want := len(tc.want); res.Counts.Changed != (graphdiff.Count{Entities: want}) {
				t.Errorf("changed count %+v, want %d entities", res.Counts.Changed, want)
			}
			if len(res.Changed) == 1 {
				el := res.Changed[0].Element
				if el.Class != interval.ClassStable || descriptionString(el.AtFrom) != descriptionString(&interval.Description{Producer: k8s, Payload: []byte(pending)}) ||
					descriptionString(el.AtLast) != descriptionString(&interval.Description{Producer: k8s, Payload: []byte(tc.to)}) {
					t.Errorf("changed entry %s", entryString(res.Changed[0]))
				}
			}
		})
	}
	t.Run("C‴", func(t *testing.T) {
		res := scenarioC(t, "x", "y")
		expect(t, "changed", changes(res), P+" "+changeString(&graphdiff.DescriptionChange{Kind: graphdiff.ChangeOpaque, FromProducer: k8s, ToProducer: k8s}))
	})
}

// TestDescriptionChanges pins the rule of description changes at its edges:
// the empty payload is {}, values compare as JSON values with numbers by their
// text, and null, trailing data and invalid UTF-8 are not objects.
func TestDescriptionChanges(t *testing.T) {
	P := subjectString(ent(pod(9)))
	opaque := P + " " + changeString(&graphdiff.DescriptionChange{Kind: graphdiff.ChangeOpaque, FromProducer: k8s, ToProducer: k8s})
	for _, tc := range []struct {
		name, from, to string
		want           []string
	}{
		{"empty and {}", "", "{}", nil},
		{"empty and a state key", "", `{"phase":"a"}`, []string{P + " " + keysChange([]string{"phase"}, nil)}},
		{"a key removed", `{"name":"p","phase":"a"}`, `{"phase":"a"}`, []string{P + " " + keysChange(nil, []string{"name"})}},
		{"reordered and respaced", `{"a":1,"b":{"c":[1,2],"d":null}}`, ` { "b" : { "d":null, "c":[1,2] }, "a":1 } `, nil},
		{"a nested change", `{"conditions":{"ready":true}}`, `{"conditions":{"ready":false}}`, []string{P + " " + keysChange([]string{"conditions"}, nil)}},
		{"a number by its text", `{"a":1}`, `{"a":1.0}`, []string{P + " " + keysChange(nil, []string{"a"})}},
		{
			"the judgement keys", `{"node_name":"n1","k8s.node.name":"n1","container.image.tag":"v1"}`, `{"node_name":"n2","k8s.node.name":"n2","container.image.tag":"v2"}`,
			[]string{P + " " + keysChange([]string{"container.image.tag", "k8s.node.name", "node_name"}, nil)},
		},
		{"null", `null`, `{}`, []string{opaque}},
		{"an array", `[]`, `{}`, []string{opaque}},
		{"trailing data", `{"a":1}`, `{"a":1}{}`, []string{opaque}},
		{"invalid UTF-8", "{\"a\":\"\xff\"}", "{\"a\":\"\xfe\"}", []string{opaque}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expect(t, "changed", changes(scenarioC(t, tc.from, tc.to)), tc.want...)
		})
	}
}

func TestScenarioDASourceSwitch(t *testing.T) {
	P := pod(9)
	st := openStore(t, memstore.Options{})
	newFeed(t, st).write(
		func() store.Record {
			r := observeP(ent(P), k8s, hm(9, 0), 10*time.Minute, `{"name":"p","phase":"Running"}`)
			r.Through = hm(9, 55)
			return r
		}(),
		observeP(ent(P), cluster, hm(9, 0), 0, `{"k8s.pod.name":"p","k8s.pod.phase":"Running"}`),
	)
	res := diff30(t, st, graphdiff.Options{Rank: map[lifecycle.Producer]int{k8s: 2, cluster: 1}}, P)
	expect(t, "changed", changes(res), subjectString(ent(P))+" "+changeString(&graphdiff.DescriptionChange{Kind: graphdiff.ChangeSourceChanged, FromProducer: k8s, ToProducer: cluster}))
	expect(t, "deleted", entryStrings(res.Deleted))
	expect(t, "transient", entryStrings(res.Transient))
	if len(res.Changed) == 1 {
		want := elem(ent(P), interval.ClassStable, 0, pc(T1, To, true, lifecycle.EndUnknown, []lifecycle.Producer{cluster, k8s}))
		want.AtFrom = &interval.Description{Producer: k8s, Payload: []byte(`{"name":"p","phase":"Running"}`)}
		want.AtLast = &interval.Description{Producer: cluster, Payload: []byte(`{"k8s.pod.name":"p","k8s.pod.phase":"Running"}`)}
		if got := elementString(res.Changed[0].Element); got != elementString(want) {
			t.Errorf("element %s, want %s", got, elementString(want))
		}
	}
	res = diff30(t, st, graphdiff.Options{}, P)
	expect(t, "changed with no ranks", changes(res))
}

func TestScenarioEAnIdentityChangeIsADeletePlusACreate(t *testing.T) {
	P1, P2 := pod(11), pod(12)
	web := `{"name":"web-0"}`
	t.Run("edges", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, N)
		f.write(observeP(ent(P1), k8s, hm(9, 0), 0, web), observeP(ent(P2), k8s, hm(9, 0), 0, web),
			observe(sched(P1, N), k8s, hm(9, 0), 0), deleted(sched(P1, N), k8s, hm(10, 5)),
			observe(sched(P2, N), k8s, hm(10, 6), 0))
		res := diff30(t, st, graphdiff.Options{}, N)
		expect(t, "deleted", entryStrings(res.Deleted),
			deletedEntry(elem(sched(P1, N), interval.ClassDisappeared, 1, pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk)), lifecycle.EndProducer, hm(10, 5)))
		expect(t, "created", entryStrings(res.Created),
			entries(elem(sched(P2, N), interval.ClassAppeared, 1, pc(hm(10, 6), To, false, lifecycle.EndUnknown, pk)))...)
		expect(t, "changed", changes(res))
	})
	t.Run("entities", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, N)
		f.write(observeP(ent(P1), k8s, hm(9, 0), 0, web), deleted(ent(P1), k8s, hm(10, 5)),
			observeP(ent(P2), k8s, hm(10, 6), 0, web),
			observe(sched(P1, N), k8s, hm(9, 0), 0), deleted(sched(P1, N), k8s, hm(10, 5)),
			observe(sched(P2, N), k8s, hm(10, 6), 0))
		res := diff30(t, st, graphdiff.Options{}, P1, P2)
		want := graphdiff.Counts{Created: graphdiff.Count{Entities: 1, Relations: 1}, Deleted: graphdiff.Count{Entities: 1, Relations: 1}}
		if res.Counts != want {
			t.Errorf("counts %+v, want %+v", res.Counts, want)
		}
		del := elem(ent(P1), interval.ClassDisappeared, 1, pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk))
		del.AtFrom = &interval.Description{Producer: k8s, Payload: []byte(web)}
		cre := elem(ent(P2), interval.ClassAppeared, 1, pc(hm(10, 6), To, false, lifecycle.EndUnknown, pk))
		cre.AtLast = &interval.Description{Producer: k8s, Payload: []byte(web)}
		expect(t, "deleted", entryStrings(res.Deleted),
			deletedEntry(del, lifecycle.EndProducer, hm(10, 5)),
			deletedEntry(elem(sched(P1, N), interval.ClassDisappeared, 1, pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk)), lifecycle.EndProducer, hm(10, 5)))
		expect(t, "created", entryStrings(res.Created), entries(cre,
			elem(sched(P2, N), interval.ClassAppeared, 1, pc(hm(10, 6), To, false, lifecycle.EndUnknown, pk)))...)
		expect(t, "changed", changes(res))
	})
}

func TestScenarioFQuarantine(t *testing.T) {
	st := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	f := newFeed(t, st)
	boot := func(id string, at time.Time) store.Record {
		r := observe(ent(H), agent, at, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(observe(ent(N), k8s, hm(9, 0), 0), boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)),
		observe(runsOn(N, H), agent, hm(9, 0), 0))
	res, err := graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{N, H}, T1, T2, store.Latest, graphdiff.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, el := range res.Quarantined {
		got = append(got, fmt.Sprintf("%s quarantine=%t uncascaded=%t", subjectString(el.Subject),
			el.Quarantine != nil && errors.Is(el.Quarantine, store.ErrQuarantined),
			el.Uncascaded != nil && errors.Is(el.Uncascaded, store.ErrQuarantined)))
	}
	expect(t, "quarantined", got,
		subjectString(ent(H))+" quarantine=true uncascaded=false",
		subjectString(runsOn(N, H))+" quarantine=false uncascaded=true")
	want := graphdiff.Counts{Quarantined: graphdiff.Count{Entities: 1, Relations: 1}}
	if len(res.Created)+len(res.Deleted)+len(res.Changed)+len(res.Transient) != 0 || res.Counts != want {
		t.Errorf("buckets %+v, counts %+v, want only %+v", res, res.Counts, want)
	}
	expectReads(t, res.Reads, 3, 5, 6, 3)
}

// The quarantined list is capped like the bucket lists, with its counts
// exact.
func TestScenarioFQuarantineCapped(t *testing.T) {
	N1, N2, N3 := node(1), node(2), node(3)
	st := openStore(t, memstore.Options{Policy: storetest.QuarantinePolicy()})
	f := newFeed(t, st)
	boot := func(id string, at time.Time) store.Record {
		r := observe(ent(H), agent, at, 15*time.Minute)
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)))
	for _, n := range []identity.Fingerprint{N1, N2, N3} {
		f.write(observe(ent(n), k8s, hm(9, 0), 0), observe(runsOn(n, H), agent, hm(9, 0), 0))
	}
	all := diff30(t, st, graphdiff.Options{}, H)
	expect(t, "quarantined", quarantinedSubjects(all.Quarantined),
		subjectString(ent(H)), subjectString(runsOn(N1, H)), subjectString(runsOn(N2, H)), subjectString(runsOn(N3, H)))
	if want := (graphdiff.Counts{Quarantined: graphdiff.Count{Entities: 1, Relations: 3}}); all.Counts != want || all.Truncated != (graphdiff.Truncation{}) {
		t.Errorf("counts %+v, truncated %+v, want %+v and none", all.Counts, all.Truncated, want)
	}
	capped := diff30(t, st, graphdiff.Options{Policy: graphdiff.Policy{MaxScope: 1, MaxListed: 2}}, H)
	expect(t, "quarantined under a cap of 2", quarantinedSubjects(capped.Quarantined), subjectString(ent(H)), subjectString(runsOn(N1, H)))
	if capped.Counts != all.Counts || capped.Truncated != (graphdiff.Truncation{Quarantined: 2}) {
		t.Errorf("counts %+v, truncated %+v, want %+v and 2 quarantined left out", capped.Counts, capped.Truncated, all.Counts)
	}
}

func quarantinedSubjects(els []interval.Element) []string {
	out := []string{}
	for _, el := range els {
		out = append(out, subjectString(el.Subject))
	}
	return out
}

func TestScenarioGBoundaries(t *testing.T) {
	X, Y, Z, Q := pod(21), pod(22), pod(23), pod(24)
	st, f := world(t, memstore.Options{}, X, Y, Z, Q, N)
	f.write(
		observe(sched(X, N), k8s, T2, 0),
		observe(sched(Y, N), k8s, hm(9, 0), 0), deleted(sched(Y, N), k8s, T2),
		observe(sched(Z, N), k8s, T1, 0),
		observe(sched(Q, N), k8s, hm(9, 0), 0), deleted(sched(Q, N), k8s, T1),
	)
	res := diff30(t, st, graphdiff.Options{}, N)
	expect(t, "created", entryStrings(res.Created),
		entries(elem(sched(X, N), interval.ClassAppeared, 1, pc(T2, To, false, lifecycle.EndUnknown, pk)))...)
	expect(t, "deleted", entryStrings(res.Deleted),
		deletedEntry(elem(sched(Y, N), interval.ClassDisappeared, 1, pc(T1, T2, true, lifecycle.EndProducer, pk)), lifecycle.EndProducer, T2))
	expect(t, "changed", changes(res))
	expect(t, "transient", entryStrings(res.Transient))
	expect(t, "events", eventStrings(graphdiff.Events(res)),
		ts(T2)+" appear "+subjectString(sched(X, N)),
		ts(T2)+" disappear "+subjectString(sched(Y, N))+" producer")
	expectReads(t, res.Reads, 3, 3, 5, 5)
}

func TestScenarioHFlapThenDeletion(t *testing.T) {
	X := pod(21)
	st, f := world(t, memstore.Options{}, X, N)
	f.write(observe(sched(X, N), k8s, hm(9, 0), 0), deleted(sched(X, N), k8s, hm(10, 5)), observe(sched(X, N), k8s, hm(10, 10), 5*time.Minute))
	res := diff30(t, st, graphdiff.Options{}, N)
	el := elem(sched(X, N), interval.ClassFlapping, 3,
		pc(T1, hm(10, 5), true, lifecycle.EndProducer, pk), pc(hm(10, 10), hm(10, 15), false, lifecycle.EndLivenessExpiry, pk))
	expect(t, "deleted", entryStrings(res.Deleted), deletedEntry(el, lifecycle.EndLivenessExpiry, hm(10, 15)))
	expect(t, "transient", entryStrings(res.Transient), entries(el)...)
	expect(t, "created", entryStrings(res.Created))
	// The two entries of one element share nothing a caller may change.
	if len(res.Deleted) == 1 && len(res.Deleted[0].Element.Pieces) == 2 {
		res.Deleted[0].Element.Pieces[0].Producers[0] = "changed"
		res.Deleted[0].Element.Pieces[1].End = T2
	}
	expect(t, "transient after a change to the deleted entry", entryStrings(res.Transient), entries(el)...)
}

func TestScenarioIOneEdgeSeenFromBothEnds(t *testing.T) {
	P := pod(9)
	st, f := world(t, memstore.Options{}, P, N)
	f.write(observe(sched(P, N), k8s, hm(10, 6), 0))
	res := diff30(t, st, graphdiff.Options{}, P, N, P)
	expect(t, "created", entryStrings(res.Created),
		entries(elem(sched(P, N), interval.ClassAppeared, 1, pc(hm(10, 6), To, false, lifecycle.EndUnknown, pk)))...)
	if res.Counts.Created != (graphdiff.Count{Relations: 1}) || len(graphdiff.Events(res)) != 1 {
		t.Errorf("counts %+v, events %v", res.Counts, eventStrings(graphdiff.Events(res)))
	}
	if fmt.Sprint(res.Scope) != fmt.Sprint([]identity.Fingerprint{N, P}) {
		t.Errorf("scope %v, want [%s %s]", res.Scope, N, P)
	}
	expectReads(t, res.Reads, 3, 5, 6, 6)
}

func TestScenarioJTheTokenIsPinnedOnce(t *testing.T) {
	inner := scenarioA(t, memstore.Options{})
	want := diff30(t, inner, graphdiff.Options{}, N)
	before := inner.LastSeq()
	late := newFeed(t, inner)
	st := &spy{Store: inner, onWindow: func(n int) {
		if n == 1 {
			late.write(deleted(sched(S, N), k8s, hm(10, 20)))
		}
	}}
	res := diff30(t, st, graphdiff.Options{}, N)
	if inner.LastSeq() != before+1 {
		t.Fatal("the hook did not write")
	}
	if d := sameResult(res, want); d != "" || res.Token != before {
		t.Errorf("token %d, want %d: %s", res.Token, before, d)
	}
	for _, tok := range st.tokens {
		if tok != before {
			t.Fatalf("a read at token %d, want %d: %v", tok, before, st.tokens)
		}
	}
	if st.writes != 0 {
		t.Errorf("%d writes through the differ's store", st.writes)
	}
}

// TestScenarioKRetention: a diff of a retained layer answers as on a store that
// kept everything while every description it gives was asserted at or after the
// horizon. One it must give that was asserted before the horizon, whose payload
// is not kept, is refused with ErrRetained (and ErrUnsupported), never guessed;
// and a T1 before the horizon is refused by the store, with ErrBeforeHorizon.
// Either refusal has a zero result.
func TestScenarioKRetention(t *testing.T) {
	zero := func(t *testing.T, res graphdiff.Result, err error, target error) {
		t.Helper()
		if !errors.Is(err, target) || res.Token != 0 || res.Scope != nil || res.Reads != (graphdiff.Reads{}) {
			t.Fatalf("%v, %+v; want %v with a zero result", err, res, target)
		}
	}
	t.Run("L1 retained", func(t *testing.T) {
		twin := scenarioA(t, memstore.Options{})
		st := scenarioA(t, memstore.Options{Keep: [4]bool{true, false, true, true}})
		if err := st.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		for _, scope := range []identity.Fingerprint{N, S} {
			if got, want := diff30(t, st, graphdiff.Options{}, scope), diff30(t, twin, graphdiff.Options{}, scope); sameResult(got, want) != "" || got.Reads != want.Reads {
				t.Errorf("Diff(%s) after the retention: %s", scope, sameResult(got, want))
			}
		}
		res, err := graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{N}, hm(9, 59), T2, store.Latest, graphdiff.Options{})
		zero(t, res, err, store.ErrBeforeHorizon)
	})
	t.Run("L2 retained", func(t *testing.T) {
		inner := scenarioA(t, memstore.Options{Keep: [4]bool{true, true, false, true}})
		if err := inner.Retain(ctx, hm(10, 0)); err != nil {
			t.Fatal(err)
		}
		st := &spy{Store: inner}
		// The node's own existence, asserted before the horizon, is described at
		// T1 and T2 (and so is S's placement, asserted at 9:00).
		res, err := graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{N}, T1, T2, store.Latest, graphdiff.Options{})
		zero(t, res, err, interval.ErrRetained)
		if !errors.Is(err, errors.ErrUnsupported) || !strings.Contains(err.Error(), N.String()) {
			t.Errorf("%v does not name %s", err, N)
		}
		res, err = graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{N}, hm(9, 59), T2, store.Latest, graphdiff.Options{})
		zero(t, res, err, store.ErrBeforeHorizon)
	})
}

func TestScenarioLArguments(t *testing.T) {
	inner := scenarioA(t, memstore.Options{})
	st := &spy{Store: inner}
	d := graphdiff.NewDiffer(st)
	unknown, err := identity.FingerprintFromHash("x.unknown", [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	many := make([]identity.Fingerprint, 257)
	for i := range many {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i>>8), byte(i)
		if many[i], err = identity.FingerprintFromHash(catalog.K8sPod, h); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name   string
		scope  []identity.Fingerprint
		t1, t2 time.Time
		o      graphdiff.Options
	}{
		{"t1 == t2", []identity.Fingerprint{N}, T1, T1, graphdiff.Options{}},
		{"t1 > t2", []identity.Fingerprint{N}, T2, T1, graphdiff.Options{}},
		{"empty scope", nil, T1, T2, graphdiff.Options{}},
		{"zero fingerprint", []identity.Fingerprint{N, {}}, T1, T2, graphdiff.Options{}},
		{"unknown type", []identity.Fingerprint{unknown}, T1, T2, graphdiff.Options{}},
		{"257 entities", many, T1, T2, graphdiff.Options{}},
		{"MaxListed 0", []identity.Fingerprint{N}, T1, T2, graphdiff.Options{Policy: graphdiff.Policy{MaxScope: 256}}},
		{"MaxScope 0", []identity.Fingerprint{N}, T1, T2, graphdiff.Options{Policy: graphdiff.Policy{MaxListed: 1}}},
	} {
		res, err := d.Diff(ctx, tc.scope, tc.t1, tc.t2, store.Latest, tc.o)
		if !errors.Is(err, store.ErrInvalid) || res.Token != 0 || res.Scope != nil {
			t.Errorf("%s: %v", tc.name, err)
		}
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for invalid arguments", st.calls)
	}
	// 256 distinct entities, one of them named twice, are within the cap.
	if _, err := d.Diff(ctx, append(many[:256:256], many[0]), T1, T2, store.Latest, graphdiff.Options{}); err != nil {
		t.Errorf("256 distinct entities: %v", err)
	}
	st.calls = 0
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := d.Diff(cancelled, []identity.Fingerprint{N}, T1, T2, store.Latest, graphdiff.Options{}); !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled: %v", err)
	}
	if st.calls != 0 {
		t.Errorf("%d store calls for a cancelled context", st.calls)
	}
	if err := inner.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Diff(ctx, []identity.Fingerprint{N}, T1, T2, store.Latest, graphdiff.Options{}); !errors.Is(err, store.ErrClosed) {
		t.Errorf("closed: %v", err)
	}
}

func TestScenarioMAHotNode(t *testing.T) {
	podN := func(i int) identity.Fingerprint {
		var h [identity.FingerprintBytes]byte
		h[0], h[1] = byte(i>>8), byte(i)
		fp, err := identity.FingerprintFromHash(catalog.K8sPod, h)
		if err != nil {
			t.Fatal(err)
		}
		return fp
	}
	st, f := world(t, memstore.Options{}, N, H)
	f.write(observe(runsOn(N, H), agent, hm(9, 0), 0))
	for i := 1; i <= 110; i++ {
		f.write(observe(ent(podN(i)), k8s, hm(9, 0), 0), observe(sched(podN(i), N), k8s, hm(9, 0), 0))
	}
	for i := range 40 {
		p := podN(300 + i)
		f.write(observe(ent(p), k8s, hm(7, 0), 0), observe(sched(p, N), k8s, hm(8, 0), 0), deleted(sched(p, N), k8s, hm(9, i)))
	}
	res := diff30(t, st, graphdiff.Options{}, N)
	expectEmpty(t, res)
	expectReads(t, res.Reads, 3, 3, 114, 114)
	if res.Reads.Alive > 3+110+1 || res.Reads.Alive > graphdiff.BudgetReferenceExistence {
		t.Errorf("%d existence reads, over 3 + D + C + A = 114 or the budget of %d", res.Reads.Alive, graphdiff.BudgetReferenceExistence)
	}
}

// An edge's payload changing is no change: edges are diffed by existence only.
func TestScenarioNAnEdgePayloadIsNotDiffed(t *testing.T) {
	st, f := world(t, memstore.Options{}, S, N)
	f.write(
		observeP(sched(S, N), k8s, hm(9, 0), 0, `{"node_name":"n","resolution":{"node_interval_start":"a"}}`),
		observeP(sched(S, N), k8s, hm(10, 10), 0, `{"node_name":"n","resolution":{"node_interval_start":"b"}}`),
	)
	expectEmpty(t, diff30(t, st, graphdiff.Options{}, N, S))
}

// A direction in which no scope entity has an edge layer is not read.
func TestScenarioOSkippedSteps(t *testing.T) {
	port, rack := mkfp(catalog.Port, 1), mkfp(catalog.Rack, 1)
	st, _ := world(t, memstore.Options{}, port, rack)
	res := diff30(t, st, graphdiff.Options{}, port)
	expectEmpty(t, res)
	expectReads(t, res.Reads, 1, 0, 1, 1)
	res = diff30(t, st, graphdiff.Options{}, rack)
	expectReads(t, res.Reads, 2, 1, 1, 1)
}

// The lists are capped; the counts and the change log's order are not.
func TestScenarioPTruncation(t *testing.T) {
	st := scenarioA(t, memstore.Options{})
	res := diff30(t, st, graphdiff.Options{Policy: graphdiff.Policy{MaxScope: 1, MaxListed: 1}}, N)
	if len(res.Transient) != 1 || res.Transient[0].Element.Subject != sched(C, N) || res.Truncated != (graphdiff.Truncation{Transient: 1}) ||
		res.Counts.Transient != (graphdiff.Count{Relations: 2}) {
		t.Errorf("transient %v, truncated %+v, counts %+v", entryStrings(res.Transient), res.Truncated, res.Counts)
	}
	if _, err := graphdiff.NewDiffer(st).Diff(ctx, []identity.Fingerprint{N, S}, T1, T2, store.Latest, graphdiff.Options{Policy: graphdiff.Policy{MaxScope: 1, MaxListed: 1}}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a scope of 2 under MaxScope 1: %v", err)
	}
}
