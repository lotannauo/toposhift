package mcp

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// otherProducer is a second producer of a dependency, for the case where the
// freshest of several has no TTL.
const otherProducer lifecycle.Producer = "other/c1"

// hopByName finds the caller or callee of the answer with the name.
func hopByName(t testing.TB, hs []HopService, name string) HopService {
	t.Helper()
	for _, h := range hs {
		if str(h.Service.Name) == name {
			return h
		}
	}
	t.Fatalf("no hop to %q in %d hops", name, len(hs))
	return HopService{}
}

// A window that ends while an edge is still live shows how long its producer has
// not been heard, less the extension interval of its TTL, at the window's last
// instant. Every dependency of the service world is sighted every minute with a
// TTL of five minutes, so the extension interval is 2m30s and the half-life
// 37.5s. The window [10:00, 10:14) ends at 10:13:59.999999999; legacy-pay's edge was
// last sighted at 10:10 and ends at 10:15, so it has been silent for 3m59.999999999s
// and the bound is 1m29.999999999s, a confidence of 2^-2.4, 0.1895. The edges
// sighted to the last minute but one show no silence: 59.999999999s is shorter
// than 2m30s.
func TestServiceContextRecencyAtTheWindowsLastInstant(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	s := w.source(t)
	out := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: w10, To: "2026-01-01T10:14:00Z"})

	derived := func(last, silent string, confidence float64) *Recency {
		return &Recency{
			Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency, Producer: string(graphProducer),
			LastSeen: last, SilentAtLeast: silent, Confidence: confidence,
		}
	}
	legacy := hopByName(t, out.Callers, "legacy-pay")
	if want := derived("2026-01-01T10:10:00Z", "1m29.999999999s", 0.1895); legacy.Recency == nil || *legacy.Recency != *want {
		t.Errorf("legacy-pay: %+v, want %+v", legacy.Recency, want)
	}
	// Existence is not recency: the edge is still a member, with a span that reaches the
	// end of the window, and the expiry sentence is not in the answer.
	if len(legacy.Spans) != 1 || spanTimes(legacy.Spans[0]) != "10:00-10:14" || legacy.Spans[0].EndReason != nil {
		t.Errorf("legacy-pay spans %+v", legacy.Spans)
	}
	for _, h := range append(append([]HopService(nil), out.Callers...), out.Callees...) {
		if name := str(h.Service.Name); name != "legacy-pay" {
			if want := derived("2026-01-01T10:13:00Z", "0s", 1); h.Recency == nil || *h.Recency != *want {
				t.Errorf("%s: %+v, want %+v", name, h.Recency, want)
			}
		}
	}

	// The silence is the log's, whatever the window's first instant: from 10:12 the
	// answer is the same.
	late := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: "2026-01-01T10:12:00Z", To: "2026-01-01T10:14:00Z"})
	if l := hopByName(t, late.Callers, "legacy-pay"); l.Recency == nil || *l.Recency != *legacy.Recency {
		t.Errorf("legacy-pay from 10:12: %+v, want %+v", l.Recency, legacy.Recency)
	}

	// A silence shorter than the extension interval does not show, at 10:11:59.999999999.
	short := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: w10, To: "2026-01-01T10:12:00Z"})
	if l := hopByName(t, short.Callers, "legacy-pay"); l.Recency == nil || *l.Recency != *derived("2026-01-01T10:10:00Z", "0s", 1) {
		t.Errorf("legacy-pay to 10:12: %+v", l.Recency)
	}

	// The hop of this window, as the file keeps it.
	hop := struct {
		Window  Window       `json:"window"`
		Callers []HopService `json:"callers"`
		Callees []HopService `json:"callees"`
	}{out.Window, out.Callers, out.Callees}
	got, err := json.MarshalIndent(hop, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	got = append(got, '\n')
	path := filepath.Join("testdata", "service_context_recency.golden.json")
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
		t.Errorf("the hop differs from %s; run with -update if the shape changed on purpose.\ngot:\n%s", path, got)
	}
}

// Where the log proves no silence the recency is null, never a confidence of 1: an
// edge that no span of reaches the window's last instant, one that was deleted
// there, one a producer with no TTL holds (it ends only by a delete, so silence says
// nothing), and one a producer with no TTL holds beside a producer with one.
func TestServiceContextRecencyIsNullWhereTheLogProvesNoSilence(t *testing.T) {
	n := newNumberedWorld(t)
	seed := numbered(catalog.Service, 0)
	n.service(seed, "seed")
	names := []string{"watched", "sighted", "deleted", "mixed", "expired"}
	callers := map[string]identity.Fingerprint{}
	for i, name := range names {
		callers[name] = numbered(catalog.Service, i+1)
		n.service(callers[name], name)
	}
	dep := func(name string) store.Subject { return edge(callers[name], seed, catalog.DependsOn) }
	n.recs = append(n.recs,
		// No TTL: watch mode.
		observe(dep("watched"), graphProducer, hm(9, 0), 0, `{}`),
		// A TTL, last sighted at 10:26, live to 10:31.
		observe(dep("sighted"), graphProducer, hm(10, 26), 5*time.Minute, `{}`),
		// Sighted, then deleted by its producer at 10:10.
		observe(dep("deleted"), graphProducer, hm(10, 0), 5*time.Minute, `{}`),
		deleted(dep("deleted"), graphProducer, hm(10, 10)),
		// Two producers, the fresher with a TTL and the other with none.
		observe(dep("mixed"), graphProducer, hm(10, 26), 5*time.Minute, `{}`),
		observe(dep("mixed"), otherProducer, hm(9, 0), 0, `{}`),
		// Last sighted at 10:20, so it ended at 10:25.
		observe(dep("expired"), graphProducer, hm(10, 20), 5*time.Minute, `{}`),
	)
	out := serviceContext(t, n.commit(), ServiceContextInput{Service: seed.String(), From: w10, To: w1030})
	if len(out.Callers) != len(names) {
		t.Fatalf("%d callers, want %d", len(out.Callers), len(names))
	}

	for _, name := range []string{"watched", "deleted", "mixed", "expired"} {
		if h := hopByName(t, out.Callers, name); h.Recency != nil {
			t.Errorf("%s: recency %+v, want null", name, *h.Recency)
		}
	}
	// The one with a TTL shows the bound: 10:29:59.999999999 less 10:26 less 2m30s.
	want := Recency{
		Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency, Producer: string(graphProducer),
		LastSeen: "2026-01-01T10:26:00Z", SilentAtLeast: "1m29.999999999s", Confidence: 0.1895,
	}
	if h := hopByName(t, out.Callers, "sighted"); h.Recency == nil || *h.Recency != want {
		t.Errorf("sighted: %+v, want %+v", h.Recency, want)
	}

	// Null is in the bytes of the answer, as the word null.
	b, err := json.Marshal(hopByName(t, out.Callers, "watched"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(b, []byte(`"recency":null`)) {
		t.Errorf("a hop with no recency prints %s", b)
	}
	// The existence of those edges is the same with or without a recency: all five are
	// members, and the ones with no recency are not dropped.
	if out.Counts.Callers != len(names) {
		t.Errorf("counts %+v", out.Counts)
	}
}

// Recency is read at the answer's token: a sighting written after it moves the
// answer read at a later token, and not the one read at this.
func TestServiceContextRecencyIsReadAtTheAnswersToken(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	s := w.source(t)
	in := ServiceContextInput{Service: w.id("checkout"), From: w10, To: "2026-01-01T10:14:00Z"}
	before := serviceContext(t, s, in)
	if r := hopByName(t, before.Callers, "legacy-pay").Recency; r == nil || r.Confidence != 0.1895 {
		t.Fatalf("legacy-pay before: %+v", r)
	}

	// legacy-pay is sighted again at 10:13, after the token was taken.
	w.add(observe(edge(w.svc["legacy-pay"], w.svc["checkout"], catalog.DependsOn), graphProducer, minute(73), svcTTL, `{}`))
	w.flush()

	in.AsOf = before.Token
	again := serviceContext(t, s, in)
	if a, b := asJSON(t, before), asJSON(t, again); !bytes.Equal(a, b) {
		t.Errorf("at the token %s the answer changed:\n%s\n%s", before.Token, a, b)
	}
	in.AsOf = ""
	now := serviceContext(t, s, in)
	want := Recency{
		Basis: BasisDerived, Subject: SubjectDerived, Rule: RuleRecency, Producer: string(graphProducer),
		LastSeen: "2026-01-01T10:13:00Z", SilentAtLeast: "0s", Confidence: 1,
	}
	if r := hopByName(t, now.Callers, "legacy-pay").Recency; r == nil || *r != want {
		t.Errorf("legacy-pay at the later token: %+v, want %+v", r, want)
	}
	if now.Token == before.Token {
		t.Errorf("the token did not move: %s", now.Token)
	}
}

// Recency decides no member, no order and no cut. The same world is read twice, the
// second time with three edges of checkout's hop sighted for less long, so that the
// confidences of its callers, and of its callees, run the other way from their order
// in the answer. The existence of every edge in the window is the same, and so is
// every list, cap and cut of the answer: the answers are equal in every byte but the
// token and the recencies, for each seed, with and without node_only, and under every
// display limit.
func TestServiceContextRecencyDecidesNothing(t *testing.T) {
	full := newSvcWorld(t, newSvcMem(t))
	frontend, checkout, cart := svcFP(t, "frontend"), svcFP(t, "checkout"), svcFP(t, "cart")
	// Last sightings in the short world, for the window [10:00, 10:12): cart to checkout
	// and checkout to cart at 10:08, frontend to checkout at 10:09, every other edge to
	// the end. Their TTL still carries each past the window's end. In the answer the
	// callers run cart, frontend, legacy-pay and the callees cart, payments, so the
	// confidences run the other way round: the first is the lowest.
	cutAfter := map[[2]identity.Fingerprint]time.Time{
		{cart, checkout}: hm(10, 8), {checkout, cart}: hm(10, 8), {frontend, checkout}: hm(10, 9),
	}
	short := newSvcWorldKeeping(t, newSvcMem(t), func(r store.Record) bool {
		sub := r.Subject
		if sub.Kind != store.SubjectEdge || sub.Relation != catalog.DependsOn {
			return true
		}
		cut, ok := cutAfter[[2]identity.Fingerprint{sub.A, sub.B}]
		return !ok || !r.EventTime.After(cut)
	})
	sf, ss := full.source(t), short.source(t)

	strip := func(o ServiceContextOutput) []byte {
		// The two worlds have different tokens, which a note names.
		notes := make([]string, len(o.Notes))
		for i, n := range o.Notes {
			notes[i] = strings.ReplaceAll(n, "asof="+o.Token, "asof=T")
		}
		o.Notes, o.Token = notes, ""
		for _, hs := range [][]HopService{o.Callers, o.Callees} {
			for i := range hs {
				hs[i].Recency = nil
			}
		}
		return asJSON(t, o)
	}
	differ, compared := 0, 0
	for _, name := range []string{"checkout", "cart", "payments", "frontend", "legacy-pay"} {
		for _, nodeOnly := range []bool{false, true} {
			for _, limit := range []int{0, 1, 2, 500} {
				in := func(w *svcWorld) ServiceContextInput {
					return ServiceContextInput{Service: w.id(name), From: w10, To: "2026-01-01T10:12:00Z", NodeOnly: nodeOnly, Limit: limit}
				}
				a, b := serviceContext(t, sf, in(full)), serviceContext(t, ss, in(short))
				for _, pair := range [][2][]HopService{{a.Callers, b.Callers}, {a.Callees, b.Callees}} {
					for j := range pair[0] {
						x, y := pair[0][j].Recency, pair[1][j].Recency
						if x != nil && y != nil && x.Confidence != y.Confidence {
							differ++
						}
					}
				}
				if x, y := strip(a), strip(b); !bytes.Equal(x, y) {
					t.Errorf("%s node only %t limit %d: the answers differ beyond their recency:\n%s\n%s", name, nodeOnly, limit, x, y)
				}
				compared++
			}
		}
	}
	if differ == 0 {
		t.Error("no recency differs between the two worlds, so the test compares nothing")
	}
	t.Logf("%d answers compared, %d hops with another confidence", compared, differ)
}

// The recency is a derivation that is not stored: a call writes nothing to the store
// and adds no field to it.
func TestServiceContextRecencyWritesNothing(t *testing.T) {
	w := newSvcWorld(t, newSvcMem(t))
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	before := w.st.LastSeq()
	out := serviceContext(t, s, ServiceContextInput{Service: w.id("checkout"), From: w10, To: w1030})
	if spy.writes != 0 || spy.retains != 0 || w.st.LastSeq() != before {
		t.Errorf("%d writes, %d retains, LastSeq %d to %d", spy.writes, spy.retains, before, w.st.LastSeq())
	}
	// Every recency is labelled derived, once, on the object that holds it.
	b := asJSON(t, out)
	if n := strings.Count(string(b), `"rule":"decay.recency"`); n != 4 {
		t.Errorf("%d recencies in the answer, want the 4 live edges of the hop", n)
	}
}
