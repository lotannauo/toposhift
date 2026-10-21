package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/wire"
)

// sgAsk is one subgraph question on the address fixture: what its request names,
// and, worked out by hand, what is withheld and which endpoints the answer would
// print if nothing were.
type sgAsk struct {
	name     string
	question string
	on       askOn
	named    []identity.Fingerprint
	withheld SubgraphWithheld
	leaks    []identity.Fingerprint
	do       func(s *Source) (SubgraphAnswer, error)
}

func scopeAsk(name string, on askOn, scope []identity.Fingerprint, named []identity.Fingerprint, withheld SubgraphWithheld, leaks ...identity.Fingerprint) sgAsk {
	return sgAsk{
		name: name, question: QuestionScope, on: on, named: named, withheld: withheld, leaks: leaks,
		do: func(s *Source) (SubgraphAnswer, error) {
			return s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf(scope), From: w10, To: w1030})
		},
	}
}

// sgAsks are the questions of the address fixture, over [10:00, 10:30). In the
// window P claims E1, E2, E5 and E6 and N claims E3, E4 and E8; Q claims E7.
func (w *addrWorld) sgAsks() []sgAsk {
	e := w.e
	return []sgAsk{
		scopeAsk("scope of P and N", onPlain, []identity.Fingerprint{w.p, w.n}, nil,
			SubgraphWithheld{Edges: 7}, e[1], e[2], e[3], e[4], e[5], e[6], e[8]),
		scopeAsk("scope of Q", onPlain, []identity.Fingerprint{w.q}, nil, SubgraphWithheld{Edges: 1}, e[7]),
		// Q is quarantined: its edge to E7 is uncut, in the static section.
		scopeAsk("scope of a quarantined Q", onQuarQ, []identity.Fingerprint{w.q}, nil, SubgraphWithheld{QuarantinedEdges: 1}, e[7]),
		// E7 is quarantined: Q's edge to it is uncut, in the static section.
		scopeAsk("scope of Q with a quarantined E7", onQuarE7, []identity.Fingerprint{w.q}, nil, SubgraphWithheld{QuarantinedEdges: 1}, e[7]),
		{name: "co-residents of P", question: QuestionCoResidents, do: func(s *Source) (SubgraphAnswer, error) {
			return s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030})
		}},
		{name: "co-residents of R, node only", question: QuestionCoResidents, do: func(s *Source) (SubgraphAnswer, error) {
			return s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.r.String(), From: w10, To: w1030, NodeOnly: true})
		}},
		{name: "service context of the service on P", question: QuestionServiceContext, do: func(s *Source) (SubgraphAnswer, error) {
			return s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030})
		}},
		{name: "blast radius of N", question: QuestionBlastRadius, do: func(s *Source) (SubgraphAnswer, error) {
			return s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030})
		}},
		{name: "blast radius of P, depth 1", question: QuestionBlastRadius, do: func(s *Source) (SubgraphAnswer, error) {
			return s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.p.String(), From: w10, To: w1030, Depth: 1})
		}},
	}
}

func sgJSON(tb testing.TB, a SubgraphAnswer) []byte {
	tb.Helper()
	b, err := json.Marshal(a)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}

// docView is the part of a document's JSON the tests read.
type docView struct {
	Basis  string         `json:"basis"`
	Token  string         `json:"token"`
	From   string         `json:"from"`
	To     string         `json:"to"`
	Policy map[string]any `json:"policy"`
	Query  struct {
		Kind    string `json:"kind"`
		Seeds   []string
		Options []struct{ Name, Value string }
		AsOf    string `json:"asof"`
	} `json:"query"`
	Entities []struct {
		Fingerprint string `json:"fingerprint"`
		Boundary    bool   `json:"boundary"`
	} `json:"entities"`
	Elements   []viewElement `json:"elements"`
	Quarantine struct {
		Entities []struct {
			Fingerprint string `json:"fingerprint"`
			Cause       struct {
				Entity    string `json:"entity"`
				Collision *struct {
					StaleBoot string `json:"stale_boot"`
					NewerBoot string `json:"newer_boot"`
				} `json:"collision"`
			} `json:"cause"`
		} `json:"entities"`
		Edges []viewElement `json:"edges"`
	} `json:"quarantine"`
}

type viewElement struct {
	Key struct {
		Kind     string `json:"kind"`
		A        string `json:"a"`
		B        string `json:"b"`
		Relation string `json:"relation"`
		Rule     string `json:"rule"`
	} `json:"key"`
	Pieces []struct{ Start, End string } `json:"pieces"`
}

func viewOf(tb testing.TB, a SubgraphAnswer) docView {
	tb.Helper()
	var v docView
	if err := json.Unmarshal(a.Document, &v); err != nil {
		tb.Fatal(err)
	}
	return v
}

// presentAt says the element has a piece that holds the instant rel, in
// nanoseconds after From.
func (e viewElement) presentAt(tb testing.TB, rel uint64) bool {
	tb.Helper()
	for _, p := range e.Pieces {
		s, err1 := strconv.ParseUint(p.Start, 10, 64)
		end, err2 := strconv.ParseUint(p.End, 10, 64)
		if err1 != nil || err2 != nil {
			tb.Fatalf("a piece [%s, %s)", p.Start, p.End)
		}
		if s <= rel && rel < end {
			return true
		}
	}
	return false
}

func (e viewElement) edgeKey() string { return e.Key.A + " -[" + e.Key.Relation + "]-> " + e.Key.B }

// checkAligned checks what binds the envelope to its document: names[i] names
// entities[i], the token is the document's, the document is derived, and the
// policy versions the envelope carries are the document's.
func checkAligned(t *testing.T, what string, a SubgraphAnswer) {
	t.Helper()
	v := viewOf(t, a)
	if len(a.Names) != len(v.Entities) {
		t.Fatalf("%s: %d names for %d entities", what, len(a.Names), len(v.Entities))
	}
	for i, n := range a.Names {
		if n.ID != v.Entities[i].Fingerprint || n.Type != string(n.FP.Type()) || n.FP.String() != n.ID {
			t.Errorf("%s: names[%d] is %s, entities[%d] is %s", what, i, n.ID, i, v.Entities[i].Fingerprint)
		}
	}
	if a.Token != v.Token || !a.Derived || v.Basis != "derived" {
		t.Errorf("%s: token %s, document token %s, derived %t, basis %s", what, a.Token, v.Token, a.Derived, v.Basis)
	}
	for key, got := range map[string]int{"interval": a.Policy.Interval, "coresidency": a.Policy.CoResidency, "extract": a.Policy.Extract, "blastradius": a.Policy.BlastRadius} {
		if got == 0 {
			continue
		}
		if doc, ok := v.Policy[key].(float64); !ok || int(doc) != got {
			t.Errorf("%s: policy.%s is %d, the document's %v", what, key, got, v.Policy[key])
		}
	}
	if a.Policy.Interval == 0 || a.Policy.Rank == "" || a.Policy.Names != NameTableVersion || a.Policy.Withhold != WithholdVersion {
		t.Errorf("%s: policy %+v", what, a.Policy)
	}
	if n := len(a.Notes); n == 0 || a.Notes[n-1] != passAsOf(mustToken(t, a.Token)) {
		t.Errorf("%s: the notes do not end with the token's sentence: %q", what, a.Notes)
	}
}

func mustToken(tb testing.TB, s string) uint64 {
	tb.Helper()
	n, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		tb.Fatal(err)
	}
	return n
}

// Every question answers on the address fixture, which reaches endpoints, on
// memstore and pebblestore alike: an endpoint that Without left in a section or a
// list would refuse the answer here. No answer prints an endpoint id the request
// did not name or an address, and each counts what it withheld as worked out by
// hand.
func TestSubgraphEveryQuestionAnswersOnTheAddressFixture(t *testing.T) {
	wm := newAddrWorld(t)
	wp := newAddrWorldOn(t, openPeb(t, servePolicy(rankObjs), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous))
	sm, sp := wm.sources(t, nil), wp.sources(t, nil)
	questions := map[string]bool{}
	for _, a := range wm.sgAsks() {
		got, err := a.do(sm.of(a.on))
		if err != nil {
			t.Errorf("%s: refused: %v", a.name, err)
			continue
		}
		questions[got.Question] = true
		if got.Question != a.question {
			t.Errorf("%s: question %q", a.name, got.Question)
		}
		b := sgJSON(t, got)
		gp, err := a.do(sp.of(a.on))
		if err != nil {
			t.Errorf("%s on pebblestore: %v", a.name, err)
		} else if bp := sgJSON(t, gp); !bytes.Equal(b, bp) {
			t.Errorf("%s differs:\nmemstore    %s\npebblestore %s", a.name, b, bp)
		}
		if ids := unnamedEndpoints(b, idSet(a.named...)); len(ids) > 0 {
			t.Errorf("%s: the answer prints %v", a.name, ids)
		}
		for _, addr := range wm.addrs[1:] {
			if strings.Contains(string(b), addr) {
				t.Errorf("%s: the answer prints the address %s", a.name, addr)
			}
		}
		if got.Counts.Withheld != a.withheld {
			t.Errorf("%s: withheld %+v, want %+v", a.name, got.Counts.Withheld, a.withheld)
		}
		hasNote := slices.ContainsFunc(got.Notes, func(n string) bool { return strings.HasPrefix(n, "Withheld because each is or names an endpoint") })
		if hasNote != (a.withheld.total() > 0) {
			t.Errorf("%s: a withheld note %t with %+v withheld: %q", a.name, hasNote, a.withheld, got.Notes)
		}
		if got.Incomplete {
			t.Errorf("%s: incomplete", a.name)
		}
		checkAligned(t, a.name, got)
	}
	if len(questions) != 4 {
		t.Errorf("the fixture asked %d questions, want all 4: %v", len(questions), questions)
	}
	want := "Withheld because each is or names an endpoint the request did not name (an endpoint's id reveals its address): 7 edges. Name the endpoint, by its id, to see it."
	got, err := sm.plain.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{wm.p, wm.n}), From: w10, To: w1030})
	if err != nil || !contains(got.Notes, want) {
		t.Errorf("notes %q (%v), want %q", got.Notes, err, want)
	}
}

// With the filter and both backstops off, the questions print the fixture's
// endpoint ids, exactly those worked out by hand, and all eight of them between
// them: without this the main test could pass on a fixture that reaches no
// endpoint.
func TestSubgraphWithholdNegativeControl(t *testing.T) {
	w := newAddrWorld(t)
	srcs := w.sources(t, &egressHooks{noFilter: true, noBackstop: true})
	leaked := map[string]bool{}
	for _, a := range w.sgAsks() {
		got, err := a.do(srcs.of(a.on))
		if err != nil {
			t.Fatalf("%s: %v", a.name, err)
		}
		printed := unnamedEndpoints(sgJSON(t, got), idSet(a.named...))
		if want := ids(a.leaks...); !slices.Equal(printed, want) {
			t.Errorf("%s: with nothing withheld the answer prints %v, want %v", a.name, printed, want)
		}
		for _, id := range printed {
			leaked[id] = true
		}
		if got.Counts.Withheld != (SubgraphWithheld{}) {
			t.Errorf("%s: withheld %+v with the filter off", a.name, got.Counts.Withheld)
		}
	}
	if len(leaked) != 8 {
		t.Errorf("the control reached %d endpoints, want all 8", len(leaked))
	}
}

// A request that names an endpoint sees it, wherever in the request it is named;
// and the named set is read from the text as sent, before parsing, so a refused
// argument that holds a named id is quoted as sent.
func TestSubgraphNamedPositives(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	e := w.e
	for _, tc := range []struct {
		name     string
		scope    []identity.Fingerprint
		named    []identity.Fingerprint
		withheld int
	}{
		{"P and E1", []identity.Fingerprint{w.p, e[1]}, []identity.Fingerprint{e[1]}, 3},
		{"N, P and E1, E1 third", []identity.Fingerprint{w.n, w.p, e[1]}, []identity.Fingerprint{e[1]}, 6},
		{"N, P, E1 and E3, E3 last", []identity.Fingerprint{w.n, w.p, e[1], e[3]}, []identity.Fingerprint{e[1], e[3]}, 5},
	} {
		got, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf(tc.scope), From: w10, To: w1030})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		b := sgJSON(t, got)
		if ids := unnamedEndpoints(b, idSet(tc.named...)); len(ids) > 0 {
			t.Errorf("%s: prints %v", tc.name, ids)
		}
		if got.Counts.Withheld != (SubgraphWithheld{Edges: tc.withheld}) {
			t.Errorf("%s: withheld %+v, want %d edges", tc.name, got.Counts.Withheld, tc.withheld)
		}
		v := viewOf(t, got)
		for _, ep := range tc.named {
			holder := w.p
			if ep == e[3] {
				holder = w.n
			}
			if !slices.ContainsFunc(v.Elements, func(el viewElement) bool { return el.Key.Kind == "entity" && el.Key.A == ep.String() }) {
				t.Errorf("%s: no element of the named %s", tc.name, ep)
			}
			if !slices.ContainsFunc(v.Elements, func(el viewElement) bool {
				return el.Key.Kind == "edge" && el.Key.A == holder.String() && el.Key.B == ep.String() && el.Key.Relation == string(catalog.HasAddress)
			}) {
				t.Errorf("%s: no has_address edge to the named %s", tc.name, ep)
			}
		}
		checkAligned(t, tc.name, got)
	}

	// The id is named by the text sent: a scope entry that does not parse and
	// holds E1's id is quoted with it.
	bad := e[1].String() + "x"
	_, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{w.p.String(), bad}, From: w10, To: w1030})
	wantErr(t, "an entry that does not parse", err, `The argument "scope[1]" must be an entity id such as host:0123456789abcdef0123456789abcdef; got "`+bad+`".`, store.ErrInvalid)
	// An id in upper case parses as nothing, so it cannot name an endpoint the
	// filter does not see.
	_, err = s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{w.p.String(), strings.ToUpper(e[1].String())}, From: w10, To: w1030})
	wantErr(t, "an id in upper case", err, `The argument "scope[1]" must be an entity id`, store.ErrInvalid)
}

// The last check refuses an answer that would print an endpoint id the request
// did not name, with the egress sentence, which carries no id: with the filter
// off, and when an id is put in a note or in the document's text just before the
// check, on every question.
func TestSubgraphBackstop(t *testing.T) {
	w := newAddrWorld(t)
	e1 := w.e[1].String()
	t.Run("filter off", func(t *testing.T) {
		var logs logSink
		srcs := addrSources{
			plain:  sourceOn(t, w.st, &egressHooks{noFilter: true}, logs.logf),
			quarQ:  sourceOn(t, quarStore{w.st, map[identity.Fingerprint]bool{w.q: true}}, &egressHooks{noFilter: true}, logs.logf),
			quarE7: sourceOn(t, quarStore{w.st, map[identity.Fingerprint]bool{w.e[7]: true}}, &egressHooks{noFilter: true}, logs.logf),
		}
		refused := 0
		for _, a := range w.sgAsks() {
			_, err := a.do(srcs.of(a.on))
			if len(a.leaks) == 0 {
				if err != nil {
					t.Errorf("%s: %v", a.name, err)
				}
				continue
			}
			refused++
			wantEgress(t, a.name, err)
		}
		if refused == 0 || len(logs.all()) != refused {
			t.Errorf("%d refusals, %d log lines", refused, len(logs.all()))
		}
		for _, l := range logs.all() {
			if endpointID.MatchString(l) {
				t.Errorf("the log prints an endpoint id: %s", l)
			}
		}
	})
	for _, tc := range []struct {
		name   string
		inject func(*SubgraphAnswer)
	}{
		{"an id in a note", func(a *SubgraphAnswer) { a.Notes = append(a.Notes, "see "+e1) }},
		{"an id in the document", func(a *SubgraphAnswer) {
			a.Document = bytes.Replace(a.Document, []byte(`"basis": "derived"`), []byte(`"basis": "`+e1+`"`), 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sourceOn(t, w.st, &egressHooks{inject: func(out any) {
				a, ok := out.(*SubgraphAnswer)
				if !ok {
					t.Fatalf("the hook got a %T", out)
				}
				tc.inject(a)
			}}, nil)
			questions := map[string]bool{}
			for _, a := range w.sgAsks() {
				if a.on != onPlain {
					continue
				}
				questions[a.question] = true
				_, err := a.do(s)
				wantEgress(t, a.name, err)
			}
			if len(questions) != 4 {
				t.Errorf("%d questions asked", len(questions))
			}
		})
	}
}

func wantEgress(t *testing.T, what string, err error) {
	t.Helper()
	var te *Error
	if !errors.As(err, &te) || te.Message != egressSentence {
		t.Errorf("%s: %v, want the egress sentence", what, err)
		return
	}
	var ee *egressError
	if !errors.As(err, &ee) || errors.Is(err, store.ErrInvalid) {
		t.Errorf("%s: the cause is %v", what, te.Cause)
	}
}

// withAlter sets the knob that alters a built subgraph for one test.
func withAlter(t *testing.T, f func(*subgraph.Subgraph)) {
	t.Helper()
	subgraphKnobs.alter = f
	t.Cleanup(func() { subgraphKnobs.alter = nil })
}

// A section that names an endpoint keeps it in the dictionary, and the answer is
// refused whole before anything is written, with the egress sentence and no id,
// even with the last check off: the two nets are independent.
func TestSubgraphDictionaryBackstop(t *testing.T) {
	w := newAddrWorld(t)
	withAlter(t, func(sg *subgraph.Subgraph) {
		sg.Sections.Truncated.Nodes = append(sg.Sections.Truncated.Nodes, w.e[3])
	})
	var logs logSink
	s := sourceOn(t, w.st, &egressHooks{noBackstop: true}, logs.logf)
	_, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n}), From: w10, To: w1030})
	wantEgress(t, "a section naming E3", err)
	lines := logs.all()
	if len(lines) != 1 || !strings.Contains(lines[0], "dictionary still names 1 entities it dropped") || endpointID.MatchString(lines[0]) {
		t.Errorf("log %q", lines)
	}

	// Named, E3 is printed, and the answer is incomplete for the cut that names it.
	got, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n, w.e[3]}), From: w10, To: w1030})
	if err != nil || !got.Incomplete {
		t.Errorf("named: %v, incomplete %t", err, got.Incomplete)
	}
}

// A subgraph Without refuses, here one whose pod section lists as quarantined an
// edge it takes out, fails the whole answer as an internal error: logged once,
// redacted, never told as a refused argument, and never answered with the
// subgraph from before.
func TestSubgraphWithoutRefusalIsInternal(t *testing.T) {
	w := newAddrWorld(t)
	q, e7 := w.q, w.e[7]
	withAlter(t, func(sg *subgraph.Subgraph) {
		sg.Sections.Pods = []subgraph.PodSection{{Seed: q, Quarantined: []subgraph.Key{subgraph.EdgeKey(q, e7, catalog.HasAddress)}}}
	})
	var logs logSink
	s := sourceOn(t, quarStore{w.st, map[identity.Fingerprint]bool{e7: true}}, nil, logs.logf)
	got, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{q.String()}, From: w10, To: w1030})
	wantErr(t, "a pod section Without cannot keep", err, "Internal error: withholding from the subgraph: ")
	if !reflect.DeepEqual(got, SubgraphAnswer{}) {
		t.Errorf("an answer came with the refusal: %+v", got)
	}
	if errors.Is(err, store.ErrInvalid) {
		t.Errorf("the refusal is told as an invalid argument: %v", err)
	}
	if endpointID.MatchString(err.Error()) || !strings.Contains(err.Error(), withheldEndpoint) {
		t.Errorf("the sentence does not withhold E7: %v", err)
	}
	lines := logs.all()
	if len(lines) != 1 || endpointID.MatchString(lines[0]) || !strings.Contains(lines[0], "pod section 0 lists") {
		t.Errorf("log %q", lines)
	}

	// Named, E7 is not taken out, and the same subgraph answers.
	if _, err := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{q.String(), e7.String()}, From: w10, To: w1030}); err != nil {
		t.Errorf("with E7 named: %v", err)
	}
}

// A quarantine's collision prints its two boot ids, and the answer passes the
// last check; the check scans the whole document, so an endpoint id in place of a
// boot id refuses it.
func TestSubgraphQuarantineCollision(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	in := ScopeSubgraphInput{Scope: []string{w.h2.String(), w.n3.String()}, From: w10, To: w1030}
	got, err := s.ScopeSubgraph(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	v := viewOf(t, got)
	if len(v.Quarantine.Entities) != 1 || v.Quarantine.Entities[0].Fingerprint != w.h2.String() {
		t.Fatalf("quarantine %+v", v.Quarantine)
	}
	c := v.Quarantine.Entities[0].Cause.Collision
	if c == nil || c.StaleBoot == "" || c.NewerBoot == "" || c.StaleBoot == c.NewerBoot {
		t.Fatalf("collision %+v", c)
	}
	if len(v.Quarantine.Edges) == 0 {
		t.Errorf("no uncut edge to the quarantined host")
	}
	checkAligned(t, "quarantined host", got)

	e := endpointFP(t, "k8s/c1", "10.9.9.9").String()
	boot := []byte(`"stale_boot": "` + c.StaleBoot + `"`)
	hooked := sourceOn(t, w.st, &egressHooks{inject: func(out any) {
		a := out.(*SubgraphAnswer)
		a.Document = bytes.Replace(a.Document, boot, []byte(`"stale_boot": "`+e+`"`), 1)
	}}, nil)
	_, err = hooked.ScopeSubgraph(ctx, in)
	wantEgress(t, "an endpoint id in place of a boot id", err)
}

// The names are the tools' names: for every entity of a blast radius's subgraph,
// the name is the one the blast_radius tool prints for the same seed, window and
// token, and the print rule withholds what it withholds there.
func TestSubgraphNamesTwin(t *testing.T) {
	sc := newScenario(t)
	host, node := numbered(catalog.Host, 1), numbered(catalog.K8sNode, 1)
	sc.add(observe(ent(host), agent, hm(9, 0), 0, `{}`))
	sc.entity(node, "names-node", hm(9, 0))
	sc.runsOn(node, host, hm(9, 0))
	names := []string{"ok-pod", "bidi\u202epod", strings.Repeat("a", 300)}
	pods := make([]identity.Fingerprint, len(names))
	for i, n := range names {
		pods[i] = numbered(catalog.K8sPod, i+1)
		b, _ := json.Marshal(map[string]string{"name": n, "namespace": "shop"})
		sc.add(observe(ent(pods[i]), objs, hm(9, 0), 0, string(b)))
		sc.sched(pods[i], node, hm(9, 0))
	}
	scenarioSource := sc.commit()

	aw := newAddrWorld(t)
	ww := newWorld(t, rankObjs)
	for _, tc := range []struct {
		name string
		s    *Source
		seed identity.Fingerprint
	}{
		{"the print rule scenario", scenarioSource, host},
		{"the address fixture's N", sourceOn(t, aw.st, nil, nil), aw.n},
		{"the world's host", ww.source(t, rankObjs, true), ww.h1},
		{"the world's quarantined host", ww.source(t, rankObjs, true), ww.h2},
	} {
		token := strconv.FormatUint(tc.s.st.LastSeq(), 10)
		got, err := tc.s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: tc.seed.String(), From: w10, To: w1030, AsOf: token})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		tool, err := tc.s.BlastRadius(ctx, BlastRadiusInput{Entity: tc.seed.String(), From: w10, To: w1030, AsOf: token, Limit: maxLimit})
		if err != nil {
			t.Fatal(err)
		}
		printed := map[string]Ref{}
		eachRef(reflect.ValueOf(&tool), func(r *Ref) { printed[r.ID] = *r })
		for _, n := range got.Names {
			want, ok := printed[n.ID]
			if !ok {
				t.Errorf("%s: blast_radius does not print %s", tc.name, n.ID)
				continue
			}
			if !reflect.DeepEqual(n, want) {
				t.Errorf("%s: %s is named %+v, blast_radius prints %+v", tc.name, n.ID, n, want)
			}
		}
		if len(got.Names) < 2 || got.Policy.Rank != tool.Policy.Rank || got.Policy.Rank == "" {
			t.Errorf("%s: %d names, rank %q and %q", tc.name, len(got.Names), got.Policy.Rank, tool.Policy.Rank)
		}
		checkAligned(t, tc.name, got)
	}

	// The print rule, on the scope of the node too, whose pods are boundaries.
	got, err := scenarioSource.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{node.String()}, From: w10, To: w1030})
	if err != nil {
		t.Fatal(err)
	}
	byID := map[string]Ref{}
	for _, n := range got.Names {
		byID[n.ID] = n
	}
	if n := byID[pods[0].String()]; n.Name == nil || *n.Name != "ok-pod" || n.NameFrom != string(objs) {
		t.Errorf("ok-pod: %+v", n)
	}
	for _, p := range pods[1:] {
		if n := byID[p.String()]; n.Name != nil || n.NameNote != nameWithheld || n.Namespace == nil {
			t.Errorf("%s: %+v, want a withheld name", p, n)
		}
	}
	if n := byID[host.String()]; n.Name != nil || n.NameNote != "" {
		t.Errorf("the host is named: %+v", n)
	}
}

// One token per answer: a write made after the first read of a question changes
// nothing in its answer, every read and the names use the token, the answer and
// its document name it, and the same token reads the same world again.
func TestSubgraphOneToken(t *testing.T) {
	asks := func(w *addrWorld) map[string]func(*Source, string) (SubgraphAnswer, error) {
		return map[string]func(*Source, string) (SubgraphAnswer, error){
			QuestionScope: func(s *Source, asof string) (SubgraphAnswer, error) {
				return s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n}), From: w10, To: w1030, AsOf: asof})
			},
			QuestionCoResidents: func(s *Source, asof string) (SubgraphAnswer, error) {
				return s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030, AsOf: asof})
			},
			QuestionServiceContext: func(s *Source, asof string) (SubgraphAnswer, error) {
				return s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030, AsOf: asof})
			},
			QuestionBlastRadius: func(s *Source, asof string) (SubgraphAnswer, error) {
				return s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030, AsOf: asof})
			},
		}
	}
	for _, q := range []string{QuestionScope, QuestionCoResidents, QuestionServiceContext, QuestionBlastRadius} {
		t.Run(q, func(t *testing.T) {
			w := newAddrWorld(t)
			ask := asks(w)[q]
			token := w.st.LastSeq()
			before, err := ask(sourceOn(t, w.st, nil, nil), "")
			if err != nil {
				t.Fatal(err)
			}
			x := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "addr-pod-x")
			spy := &spyStore{Store: w.st}
			spy.afterFirst = func() {
				w.feed.write(
					observe(ent(w.p), objs, hm(10, 20), 0, `{"name":"web-renamed","namespace":"shop"}`),
					observe(ent(x), objs, hm(10, 0), 0, `{"name":"web-x","namespace":"shop"}`),
					observe(edge(x, w.n, catalog.ScheduledOn), objs, hm(10, 0), 0, `{}`),
				)
			}
			s := sourceOn(t, spy, nil, nil)
			during, err := ask(s, "")
			if err != nil {
				t.Fatal(err)
			}
			if w.st.LastSeq() == token || spy.reads == 0 {
				t.Fatalf("the write did not happen, or nothing was read")
			}
			for i, got := range spy.asOf {
				if got != token {
					t.Fatalf("read %d of %d used the token %d, want %d", i+1, len(spy.asOf), got, token)
				}
			}
			if b, d := sgJSON(t, before), sgJSON(t, during); !bytes.Equal(b, d) {
				t.Errorf("a write during the call changed the answer:\nbefore %s\nduring %s", b, d)
			}
			if during.Token != tokenString(token) || viewOf(t, during).Token != during.Token {
				t.Errorf("token %s, document %s, want %d", during.Token, viewOf(t, during).Token, token)
			}
			checkAligned(t, q, during)

			// The same token again: the same world; only the echo's asof differs.
			again, err := ask(s, tokenString(token))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(again.Names, before.Names) || !reflect.DeepEqual(withoutQuery(t, again), withoutQuery(t, before)) {
				t.Errorf("asof=%d read another world", token)
			}
			if a, b := viewOf(t, again).Query.AsOf, viewOf(t, before).Query.AsOf; a != tokenString(token) || b != "latest" {
				t.Errorf("echo asof %s and %s", a, b)
			}

			// And the latest token sees the write, so the test can tell a pinned
			// token from a moving one.
			now, err := ask(s, "")
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(now.Names, func(r Ref) bool { return r.Name != nil && *r.Name == "web-renamed" }) {
				t.Errorf("the rename is not seen at the latest token")
			}
			if slices.ContainsFunc(during.Names, func(r Ref) bool { return r.Name != nil && *r.Name == "web-renamed" }) {
				t.Errorf("the rename is seen at the pinned token")
			}
		})
	}
}

// withoutQuery is a document's JSON without its query echo.
func withoutQuery(tb testing.TB, a SubgraphAnswer) map[string]any {
	tb.Helper()
	m := decode(tb, a.Document)
	delete(m, "query")
	return m
}

// A token ahead of the store is refused, never clamped, on every question; and a
// token or a window below the retention horizon is refused with the horizon
// sentence.
func TestSubgraphTokenRefusals(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	last := w.st.LastSeq()
	all := func(asof, from string) map[string]error {
		_, e1 := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{w.p.String()}, From: from, To: w1030, AsOf: asof})
		_, e2 := s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: from, To: w1030, AsOf: asof})
		_, e3 := s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: from, To: w1030, AsOf: asof})
		_, e4 := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: from, To: w1030, AsOf: asof})
		return map[string]error{QuestionScope: e1, QuestionCoResidents: e2, QuestionServiceContext: e3, QuestionBlastRadius: e4}
	}
	for _, ahead := range []uint64{last + 1, store.Latest} {
		asof := strconv.FormatUint(ahead, 10)
		for q, err := range all(asof, w10) {
			wantErr(t, q+" asof="+asof, err, fmt.Sprintf("Token %s is ahead of the store (at %d); it is never clamped here.", asof, last), ErrFutureToken)
		}
	}
	for _, bad := range []string{"abc", "-1", "01", " 5", "1.5", "18446744073709551616"} {
		for q, err := range all(bad, w10) {
			wantErr(t, q+" asof="+bad, err, `The argument "asof" must be a token as a decimal number, such as 81234; got `, store.ErrInvalid)
		}
	}

	if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
		t.Fatal(err)
	}
	for q, err := range all("", "2026-01-01T09:00:00Z") {
		wantErr(t, q+" from before the horizon", err, "The store no longer keeps history of layer", store.ErrBeforeHorizon)
	}
	for q, err := range all("1", w10) {
		wantErr(t, q+" below the horizon's token", err, "The store no longer keeps history of layer", store.ErrBeforeHorizon)
	}
}

// The sentences of the two sentinels, and their place in the order: before an
// invalid argument, which a cause may also wrap.
func TestSubgraphSentinelSentences(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	for _, tc := range []struct {
		err  error
		want string
	}{
		{ErrFutureToken, "The token is ahead of the store; it is never clamped here."},
		{ErrAnswerTooLarge, "The answer is over the size cap; it is never cut. Narrow the window or the scope."},
		{fmt.Errorf("%w and %w", store.ErrInvalid, ErrFutureToken), "The token is ahead of the store; it is never clamped here."},
		{fmt.Errorf("%w and %w", ErrAnswerTooLarge, store.ErrInvalid), "The answer is over the size cap; it is never cut. Narrow the window or the scope."},
	} {
		if msg, internal := s.sentence(tc.err); msg != tc.want || internal {
			t.Errorf("sentence(%v) = %q, %t", tc.err, msg, internal)
		}
	}
}

// The echo of each question's query is pinned by a golden: the kind, the seeds,
// every policy value as an option, ascending by name, and the requested token.
// The requested token is in no option: two requests for one world at Latest and
// at its token differ only in asof; and a scope given in another order, with
// repeats, is the one scope.
func TestSubgraphQueryEcho(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	token := tokenString(w.st.LastSeq())
	must := func(a SubgraphAnswer, err error) SubgraphAnswer {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	scope := ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n}), From: w10, To: w1030}
	answers := map[string]SubgraphAnswer{
		"scope":                             must(s.ScopeSubgraph(ctx, scope)),
		"co_residents":                      must(s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030})),
		"co_residents, node only":           must(s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030, NodeOnly: true})),
		"service_context":                   must(s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030})),
		"service_context, node only":        must(s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030, NodeOnly: true})),
		"blast_radius":                      must(s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030})),
		"blast_radius, depth 2, at a token": must(s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030, Depth: 2, AsOf: token})),
	}
	echoes := map[string]any{}
	for name, a := range answers {
		echoes[name] = decode(t, a.Document)["query"]
	}
	got, err := json.MarshalIndent(echoes, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	checkGolden(t, "subgraph_echo", append(got, '\n'))

	scope.AsOf = token
	pinned := must(s.ScopeSubgraph(ctx, scope))
	latest, at := viewOf(t, answers["scope"]).Query, viewOf(t, pinned).Query
	if latest.AsOf != "latest" || at.AsOf != token {
		t.Errorf("asof %s and %s", latest.AsOf, at.AsOf)
	}
	latest.AsOf, at.AsOf = "", ""
	if !reflect.DeepEqual(latest, at) {
		t.Errorf("the echoes at Latest and at the token differ apart from asof:\n%+v\n%+v", latest, at)
	}

	permuted := must(s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.n, w.p, w.n, w.p}), From: w10, To: w1030}))
	if !bytes.Equal(permuted.Document, answers["scope"].Document) {
		t.Errorf("a permuted scope with repeats gives another document")
	}
	if seeds := viewOf(t, permuted).Query.Seeds; len(seeds) != 2 {
		t.Errorf("seeds %v", seeds)
	}
}

// checkGolden compares got with testdata/<name>.golden.json, or writes it with
// -update.
func checkGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name+".golden.json")
	if *update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to write it)", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("%s differs from %s; run with -update if the shape changed on purpose.\ngot:\n%s", name, path, got)
	}
}

// The answers of the four questions on the address fixture, pinned whole.
func TestSubgraphGoldenAnswers(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	seen := map[string]bool{}
	for _, a := range w.sgAsks() {
		if a.on != onPlain || seen[a.question] {
			continue
		}
		seen[a.question] = true
		got, err := a.do(s)
		if err != nil {
			t.Fatal(err)
		}
		b, err := json.MarshalIndent(got, "", "  ")
		if err != nil {
			t.Fatal(err)
		}
		checkGolden(t, "subgraph_"+a.question, append(b, '\n'))
	}
	if len(seen) != 4 {
		t.Errorf("%d questions", len(seen))
	}
}

// The envelope carries the document as wire.JSON writes the subgraph that the
// query packages and Without give, read here independently at the answer's token.
func TestSubgraphEnvelopeDocument(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	token := w.st.LastSeq()
	from, to := hm(10, 0), hm(10, 30)
	win := interval.Window{From: from, To: to, AsOf: token}
	named := idSet(w.e[1])
	drop := func(fp identity.Fingerprint) bool {
		return fp.Type() == catalog.NetworkEndpoint && !named[fp.String()]
	}
	for _, tc := range []struct {
		name  string
		ask   func() (SubgraphAnswer, error)
		build func() (subgraph.Subgraph, error)
	}{
		{"scope", func() (SubgraphAnswer, error) {
			return s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n, w.e[1]}), From: w10, To: w1030})
		}, func() (subgraph.Subgraph, error) {
			return subgraph.ReadScope(ctx, w.st, []identity.Fingerprint{w.p, w.n, w.e[1]}, win, subgraph.ScopePolicy{})
		}},
		{"co_residents", func() (SubgraphAnswer, error) {
			return s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030})
		}, func() (subgraph.Subgraph, error) {
			res, err := coresidency.NewFinder(w.st).Find(ctx, w.p, win, coresidency.Options{})
			if err != nil {
				return subgraph.Subgraph{}, err
			}
			return subgraph.FromCoResidency(ctx, w.st, res, subgraph.ReadPolicy{})
		}},
		{"service_context", func() (SubgraphAnswer, error) {
			return s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030})
		}, func() (subgraph.Subgraph, error) {
			res, err := extract.NewExtractor(w.st).Extract(ctx, extract.Exploratory{Service: w.svc}, win, extract.Options{})
			if err != nil {
				return subgraph.Subgraph{}, err
			}
			return subgraph.FromExtraction(ctx, w.st, res, subgraph.ReadPolicy{})
		}},
		{"blast_radius", func() (SubgraphAnswer, error) {
			return s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030})
		}, func() (subgraph.Subgraph, error) {
			res, err := blastradius.NewWalker(w.st).Impact(ctx, w.n, win, blastradius.Options{})
			if err != nil {
				return subgraph.Subgraph{}, err
			}
			return subgraph.FromBlastRadius(ctx, w.st, res, subgraph.ReadPolicy{})
		}},
	} {
		got, err := tc.ask()
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		sg, err := tc.build()
		if err != nil {
			t.Fatal(err)
		}
		kept, _, err := subgraph.Without(sg, drop)
		if err != nil {
			t.Fatal(err)
		}
		// The query echo is pinned by its own golden; any valid one will do here.
		want, err := wire.JSON(wire.Document{Query: wire.Query{Kind: wire.QueryScope}, Policy: wire.CurrentPolicy(), Subgraph: kept})
		if err != nil {
			t.Fatal(err)
		}
		wm := decode(t, want)
		delete(wm, "query")
		if gm := withoutQuery(t, got); !reflect.DeepEqual(gm, wm) {
			t.Errorf("%s: the document is not the independent one:\n%s\n%s", tc.name, got.Document, want)
		}
		var doc any
		if err := json.Unmarshal(sgJSON(t, got), &struct {
			Document *any `json:"document"`
		}{&doc}); err != nil || !reflect.DeepEqual(doc, decode(t, got.Document)) {
			t.Errorf("%s: the marshalled answer's document decodes to another value (%v)", tc.name, err)
		}
		if got.Window != windowOf(from, to) || got.Token != tokenString(token) {
			t.Errorf("%s: window %+v, token %s", tc.name, got.Window, got.Token)
		}
	}
}

// A cap of each question's query marks the answer incomplete and adds the
// sentence the tool of the same question writes; a whole answer is not
// incomplete.
func TestSubgraphIncomplete(t *testing.T) {
	t.Run("co_residents", func(t *testing.T) {
		sc := newScenario(t)
		seed := numbered(catalog.K8sPod, 1)
		sc.entity(seed, "seed", hm(9, 0))
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
		got, err := s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: seed.String(), From: w10, To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		tool, err := s.CoResidents(ctx, CoResidentsInput{Pod: seed.String(), From: w10, To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		wantCut(t, got, coResidentNotes(tool.Truncation), "6 nodes of the pod were not read: the query reads at most 64.")
	})
	t.Run("service_context", func(t *testing.T) {
		n := newNumberedWorld(t)
		seed := numbered(catalog.Service, 0)
		n.service(seed, "seed")
		for i := 1; i <= 20; i++ {
			c := numbered(catalog.Service, i)
			n.service(c, fmt.Sprintf("caller-%02d", i))
			n.depends(c, seed)
		}
		s := n.commit()
		got, err := s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: seed.String(), From: w10, To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		tool := serviceContext(t, s, ServiceContextInput{Service: seed.String(), From: w10, To: w1030})
		wantCut(t, got, contextCutNotes(tool.Truncation, extract.DefaultPolicy()), "5 neighbours were not read: the hop keeps at most 16 services, the service included, longest dependency first.")
	})
	t.Run("blast_radius", func(t *testing.T) {
		w := newWorld(t, rankObjs)
		s := w.source(t, rankObjs, true)
		got, err := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.h1.String(), From: w10, To: w1030, Depth: 1})
		if err != nil {
			t.Fatal(err)
		}
		tool, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: w.h1.String(), From: w10, To: w1030, Depth: 1})
		if err != nil {
			t.Fatal(err)
		}
		var cut []string
		for _, n := range tool.Notes {
			if strings.HasPrefix(n, "The walk stopped at depth") {
				cut = append(cut, n)
			}
		}
		wantCut(t, got, cut, "The walk stopped at depth 1, the depth limit; 2 members at that depth have edges that were not followed.")

		whole, err := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.h1.String(), From: w10, To: w1030})
		if err != nil || whole.Incomplete {
			t.Errorf("a whole walk: %v, incomplete %t", err, whole.Incomplete)
		}
	})
}

func wantCut(t *testing.T, got SubgraphAnswer, toolCut []string, want string) {
	t.Helper()
	if !got.Incomplete {
		t.Errorf("not incomplete")
	}
	if !contains(toolCut, want) {
		t.Errorf("the tool's cut sentences %q lack %q", toolCut, want)
	}
	for _, n := range toolCut {
		if !contains(got.Notes, n) {
			t.Errorf("no sentence %q in %q", n, got.Notes)
		}
	}
	checkAligned(t, "a cut answer", got)
}

// An answer over the size cap is refused whole, with both sizes; at the cap it is
// answered.
func TestSubgraphSizeCap(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	in := ScopeSubgraphInput{Scope: stringsOf([]identity.Fingerprint{w.p, w.n}), From: w10, To: w1030}
	whole, err := s.ScopeSubgraph(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	size := len(sgJSON(t, whole))
	if size >= MaxSubgraphAnswerBytes || MaxSubgraphAnswerBytes != 16<<20 {
		t.Fatalf("size %d, cap %d", size, MaxSubgraphAnswerBytes)
	}
	t.Cleanup(func() { subgraphKnobs.maxBytes = 0 })

	subgraphKnobs.maxBytes = size
	if got, err := s.ScopeSubgraph(ctx, in); err != nil || !reflect.DeepEqual(got, whole) {
		t.Errorf("at the cap: %v", err)
	}
	subgraphKnobs.maxBytes = size - 1
	got, err := s.ScopeSubgraph(ctx, in)
	wantErr(t, "over the cap", err, fmt.Sprintf("The answer is %d bytes, over the cap of %d bytes; it is never cut. Narrow the window or the scope.", size, size-1), ErrAnswerTooLarge)
	if !reflect.DeepEqual(got, SubgraphAnswer{}) {
		t.Errorf("a part of the answer came with the refusal")
	}
}

// The edges of a blast radius's subgraph are the edges on the paths of the
// blast_radius tool's answer, and its quarantined ones; the edges of a scope's
// subgraph present at an instant are the edges neighbors_at lists for each scope
// entity at that instant. Both hold with endpoints withheld, and with nothing
// withheld, so they are withheld the same way on both surfaces.
func TestSubgraphAgreesWithTheTools(t *testing.T) {
	aw, ww := newAddrWorld(t), newWorld(t, rankObjs)
	for _, filter := range []bool{true, false} {
		var h *egressHooks
		if !filter {
			h = &egressHooks{noFilter: true, noBackstop: true}
		}
		as, ws := aw.sources(t, h), sourceOn(t, ww.st, h, nil)

		t.Run(fmt.Sprintf("blast radius, filter %t", filter), func(t *testing.T) {
			for _, tc := range []struct {
				s    *Source
				seed identity.Fingerprint
			}{{as.plain, aw.n}, {as.plain, aw.p}, {as.quarQ, aw.n}, {ws, ww.h1}, {ws, ww.h2}, {ws, ww.n1}} {
				got, err := tc.s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: tc.seed.String(), From: w10, To: w1030})
				if err != nil {
					t.Fatal(err)
				}
				tool, err := tc.s.BlastRadius(ctx, BlastRadiusInput{Entity: tc.seed.String(), From: w10, To: w1030, Limit: maxLimit})
				if err != nil {
					t.Fatal(err)
				}
				want := map[string]bool{}
				for _, m := range tool.Members {
					for _, v := range m.Via {
						want[m.Entity.ID+" -["+v.Relation+"]-> "+v.Parent.ID] = true
					}
				}
				for _, q := range tool.Quarantined {
					if q.Edge != nil {
						want[q.Edge.From.ID+" -["+q.Edge.Relation+"]-> "+q.Edge.To.ID] = true
					}
				}
				v := viewOf(t, got)
				have := map[string]bool{}
				for _, el := range slices.Concat(v.Elements, v.Quarantine.Edges) {
					if el.Key.Kind == "edge" {
						have[el.edgeKey()] = true
					}
				}
				if len(want) == 0 || !reflect.DeepEqual(have, want) {
					t.Errorf("blast radius of %s: the subgraph's edges\n%v\nthe tool's\n%v", tc.seed, have, want)
				}
			}
		})

		t.Run(fmt.Sprintf("scope at instants, filter %t", filter), func(t *testing.T) {
			instants := []time.Time{hm(10, 0), hm(10, 5).Add(-time.Second), hm(10, 5), hm(10, 7), hm(10, 10), hm(10, 12), hm(10, 15), hm(10, 20), hm(10, 30).Add(-time.Nanosecond)}
			compared := 0
			for _, tc := range []struct {
				s  *Source
				fp identity.Fingerprint
			}{
				{as.plain, aw.p},
				{as.plain, aw.n},
				{as.plain, aw.q},
				{as.plain, aw.r},
				{as.quarQ, aw.q},
				{as.quarE7, aw.q},
				{ws, ww.n1},
				{ws, ww.p1},
				{ws, ww.p4},
				{ws, ww.p6},
				{ws, ww.h1},
				{ws, ww.h2},
				{ws, ww.n3},
				{ws, ww.c1},
			} {
				got, err := tc.s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{tc.fp.String()}, From: w10, To: w1030})
				if err != nil {
					t.Fatal(err)
				}
				v := viewOf(t, got)
				for _, at := range instants {
					rel := uint64(at.Sub(hm(10, 0)))
					have := map[string]bool{}
					for _, el := range slices.Concat(v.Elements, v.Quarantine.Edges) {
						if el.Key.Kind == "edge" && el.presentAt(t, rel) {
							have[el.edgeKey()] = true
						}
					}
					nb, err := tc.s.NeighborsAt(ctx, NeighborsAtInput{Entity: tc.fp.String(), At: fmtTime(at)})
					if err != nil {
						t.Fatal(err)
					}
					want := map[string]bool{}
					for _, e := range nb.Edges {
						if e.Direction == "forward" {
							want[tc.fp.String()+" -["+e.Relation+"]-> "+e.Peer.ID] = true
						} else {
							want[e.Peer.ID+" -["+e.Relation+"]-> "+tc.fp.String()] = true
						}
					}
					compared += len(want)
					if !reflect.DeepEqual(have, want) {
						t.Errorf("scope of %s at %s: the subgraph's edges\n%v\nneighbors_at\n%v", tc.fp, fmtTime(at), have, want)
					}
				}
			}
			if compared == 0 {
				t.Error("no edge was compared")
			}
		})
	}
}

// The refusals of the questions' arguments are the tools' sentences, and the
// frame's refusals are the tools' too.
func TestSubgraphRefusals(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	p, good := w.p.String(), w.svc.String()
	many := make([]string, maxScope+1)
	for i := range many {
		many[i] = p
	}
	for _, tc := range []struct {
		name string
		do   func() error
		want string
	}{
		{"scope missing", func() error { _, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{From: w10, To: w1030}); return e }, `The argument "scope" must name at least one entity.`},
		{"scope over the cap", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: many, From: w10, To: w1030})
			return e
		}, `The argument "scope" names 257 entities; at most 256.`},
		{"scope of an unknown type", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{"nothing:" + strings.Repeat("a", 32)}, From: w10, To: w1030})
			return e
		}, `The argument "scope[0]" names the entity type "nothing", which is not in the catalog.`},
		{"from missing", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{p}, To: w1030})
			return e
		}, `The argument "from" is required.`},
		{"to missing", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{p}, From: w10})
			return e
		}, `The argument "to" is required.`},
		{"from after to", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{p}, From: w1030, To: w10})
			return e
		}, `The argument "from" (2026-01-01T10:30:00Z) must be before "to" (2026-01-01T10:00:00Z).`},
		{"a time without a zone", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{p}, From: "2026-01-01T10:00:00", To: w1030})
			return e
		}, `The argument "from" must be an RFC 3339 time with a zone`},
		// A window outside the event times a store holds is the builder's refusal of
		// an argument, never an internal error of the codec behind it.
		{"a scope window outside the event times", func() error {
			_, e := s.ScopeSubgraph(ctx, ScopeSubgraphInput{Scope: []string{p}, From: "1000-01-01T00:00:00Z", To: w1030})
			return e
		}, `The request was refused: subgraph: ReadScope: subgraph: the window [`},
		{"a blast radius window outside the event times", func() error {
			_, e := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: w.n.String(), From: "1000-01-01T00:00:00Z", To: w1030})
			return e
		}, `The request was refused: `},
		{"pod missing", func() error {
			_, e := s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{From: w10, To: w1030})
			return e
		}, `The argument "pod" is required.`},
		{"pod not a pod", func() error {
			_, e := s.CoResidentsSubgraph(ctx, CoResidentsSubgraphInput{Pod: w.n.String(), From: w10, To: w1030})
			return e
		}, `The request was refused: `},
		{"service missing", func() error {
			_, e := s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{From: w10, To: w1030})
			return e
		}, `The argument "service" is required.`},
		{"service not a service", func() error {
			_, e := s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: p, From: w10, To: w1030})
			return e
		}, `The argument "service" must be a service id; got a k8s.pod id.`},
		{"service window", func() error {
			_, e := s.ServiceContextSubgraph(ctx, ServiceContextSubgraphInput{Service: good, From: w1030, To: w10})
			return e
		}, `The argument "from" (2026-01-01T10:30:00Z) must be before "to"`},
		{"entity missing", func() error {
			_, e := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{From: w10, To: w1030})
			return e
		}, `The argument "entity" is required.`},
		{"depth over 6", func() error {
			_, e := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: p, From: w10, To: w1030, Depth: 7})
			return e
		}, `The argument "depth" must be between 1 and 6; got 7.`},
		{"depth negative", func() error {
			_, e := s.BlastRadiusSubgraph(ctx, BlastRadiusSubgraphInput{Entity: p, From: w10, To: w1030, Depth: -1})
			return e
		}, `The argument "depth" must be between 1 and 6; got -1.`},
	} {
		wantErr(t, tc.name, tc.do(), tc.want, store.ErrInvalid)
	}

	t.Run("frame", func(t *testing.T) {
		in := ScopeSubgraphInput{Scope: []string{p}, From: w10, To: w1030}

		noRange, err := NewSource(Config{Store: w.st, Rank: rankObjs})
		if err != nil {
			t.Fatal(err)
		}
		_, err = noRange.ScopeSubgraph(ctx, in)
		wantErr(t, "no range reads", err, "Range answers are not available on this store.", ErrNoRangeReads)

		busy, err := NewSource(Config{Store: w.st, Rank: rankObjs, RangeReads: true, MaxConcurrent: 1, BusyAfter: 10 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		_, release, err := busy.begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_, err = busy.ScopeSubgraph(ctx, in)
		wantErr(t, "busy", err, "The server is busy; try again shortly.", ErrBusy)
		release()
		if _, err := busy.ScopeSubgraph(ctx, in); err != nil {
			t.Errorf("after the slot is free: %v", err)
		}

		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		_, err = s.ScopeSubgraph(cancelled, in)
		wantErr(t, "cancelled", err, "The call was cancelled.", context.Canceled)

		stalled := &stallStore{Store: w.st}
		stalled.block.Store(true)
		slow, err := NewSource(Config{Store: stalled, Rank: rankObjs, RangeReads: true, CallTimeout: 20 * time.Millisecond})
		if err != nil {
			t.Fatal(err)
		}
		_, err = slow.ScopeSubgraph(ctx, in)
		wantErr(t, "deadline", err, "The call took longer than 20ms and was stopped.", context.DeadlineExceeded)

		var logs logSink
		boom := errors.New("boom at " + w.e[2].String())
		broken := sourceOn(t, failAlive{w.st, w.p, boom}, nil, logs.logf)
		_, err = broken.ScopeSubgraph(ctx, in)
		wantErr(t, "a store that fails", err, "Internal error: ", boom)
		if endpointID.MatchString(err.Error()) || len(logs.all()) != 1 || endpointID.MatchString(logs.all()[0]) {
			t.Errorf("internal error %v, log %q", err, logs.all())
		}

		drained := sourceOn(t, w.st, nil, nil)
		if err := drained.Drain(ctx); err != nil {
			t.Fatal(err)
		}
		_, err = drained.ScopeSubgraph(ctx, in)
		wantErr(t, "drained", err, "The server is shutting down.", ErrShuttingDown)

		closed := openMem(t, memstore.Options{Policy: servePolicy(rankObjs)})
		cs := sourceOn(t, closed, nil, nil)
		if err := closed.Close(); err != nil {
			t.Fatal(err)
		}
		_, err = cs.ScopeSubgraph(ctx, in)
		wantErr(t, "closed", err, "The server is shutting down.", store.ErrClosed)
	})
}

// The subgraph questions are not tools: the tools' table is the nine tools.
func TestSubgraphQuestionsAreNotTools(t *testing.T) {
	for _, tl := range Tools() {
		if strings.Contains(strings.ToLower(tl.Name), "subgraph") {
			t.Errorf("a tool %q", tl.Name)
		}
	}
	if n := len(Tools()); n != 9 {
		t.Errorf("%d tools", n)
	}
}
