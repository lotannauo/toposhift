package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

const w1000 = "2026-01-01T10:00:00Z"

// found asks find_entities and fails on an error.
func found(t testing.TB, s *Source, in FindEntitiesInput) FindEntitiesOutput {
	t.Helper()
	out, err := s.FindEntities(ctx, in)
	if err != nil {
		t.Fatalf("find_entities %+v: %v", in, err)
	}
	return out
}

// foundNames lists the names of the matches, in the order of the answer, with "-" for none.
func foundNames(out FindEntitiesOutput) []string {
	names := []string{}
	for _, m := range out.Matches {
		if m.Entity.Name == nil {
			names = append(names, "-")
			continue
		}
		names = append(names, *m.Entity.Name)
	}
	return names
}

func foundIDs(out FindEntitiesOutput) []string {
	var ids []string
	for _, m := range out.Matches {
		ids = append(ids, m.Entity.ID)
	}
	return ids
}

// The tool on the world fixture: what matches, how it is labelled, what is counted
// and what the notes say.
func TestFindEntitiesOnTheWorld(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)

	out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "WEB", From: w10, To: w1030})
	if want := []string{"web-0", "web-1", "web-2", "web-3", "web-4", "web-5", "web-6"}; !slices.Equal(foundNames(out), want) {
		t.Fatalf("names %v, want %v", foundNames(out), want)
	}
	if out.Token != tokenString(w.st.LastSeq()) || !out.Derived || !out.Complete || out.NextAfter != nil || out.Type != "k8s.pod" {
		t.Errorf("token %s, derived %t, complete %t, next_after %v, type %s", out.Token, out.Derived, out.Complete, out.NextAfter, out.Type)
	}
	wantPolicy := Policy{Interval: interval.PolicyVersion, Rank: digestRank(rankObjs), Names: NameTableVersion, Withhold: WithholdVersion, Find: FindVersion}
	if out.Policy != wantPolicy {
		t.Errorf("policy %+v, want %+v", out.Policy, wantPolicy)
	}
	if out.Window != (Window{From: "2026-01-01T10:00:00Z", To: "2026-01-01T10:30:00Z"}) {
		t.Errorf("window %+v", out.Window)
	}
	if want := (FindCounts{Checked: 7, Matched: 7}); out.Counts != want {
		t.Errorf("counts %+v, want %+v", out.Counts, want)
	}
	wantNotes := []string{
		"Checked every k8s.pod id the store holds (7).",
		"Matches are listed by name; the scan goes in id order.",
		passAsOf(w.st.LastSeq()),
	}
	if !slices.Equal(out.Notes, wantNotes) {
		t.Errorf("notes %q, want %q", out.Notes, wantNotes)
	}
	byName := map[string]FoundEntity{}
	for _, m := range out.Matches {
		byName[*m.Entity.Name] = m
		if m.Subject != SubjectAsserted || m.Basis != BasisDerived || m.Rule != RuleExistence || m.Entity.Type != "k8s.pod" || m.SpansTotal != len(m.Spans) {
			t.Errorf("%s: labels %+v", *m.Entity.Name, m)
		}
		if m.Entity.Namespace == nil || *m.Entity.Namespace != "shop" || m.Entity.NameFrom != string(objs) || m.Entity.NameNote != "" {
			t.Errorf("%s: ref %+v", *m.Entity.Name, m.Entity)
		}
	}
	for name, want := range map[string]struct {
		class string
		spans int
	}{
		"web-0": {"stable", 1}, "web-1": {"disappeared", 1}, "web-2": {"appeared", 1},
		"web-3": {"stable", 1}, "web-5": {"disappeared", 1},
	} {
		if m := byName[name]; m.Class != want.class || m.SpansTotal != want.spans {
			t.Errorf("%s: class %s with %d spans, want %s with %d", name, m.Class, m.SpansTotal, want.class, want.spans)
		}
	}

	t.Run("an instant, and a name", func(t *testing.T) {
		out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web-0", At: "2026-01-01T10:08:00Z"})
		if !slices.Equal(foundIDs(out), []string{w.p1.String()}) || out.Window != (Window{From: "2026-01-01T10:08:00Z", To: "2026-01-01T10:08:00.000000001Z"}) {
			t.Errorf("ids %v, window %+v", foundIDs(out), out.Window)
		}
		// web-1 was deleted at 10:05, web-2 appeared at 10:06.
		out = found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web", At: "2026-01-01T10:08:00Z"})
		if want := []string{"web-0", "web-2", "web-3", "web-4", "web-5", "web-6"}; !slices.Equal(foundNames(out), want) {
			t.Errorf("names %v, want %v", foundNames(out), want)
		}
		// web-5's TTL lapses at 10:10.
		out = found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web", At: "2026-01-01T10:12:00Z"})
		if want := []string{"web-0", "web-2", "web-3", "web-4", "web-6"}; !slices.Equal(foundNames(out), want) {
			t.Errorf("names %v, want %v", foundNames(out), want)
		}
	})

	t.Run("a namespace is exact, and a name needs none", func(t *testing.T) {
		for ns, want := range map[string]int{"shop": 7, "Shop": 0, "sho": 0, "shop ": 0} {
			if out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Namespace: ns, From: w10, To: w1030}); len(out.Matches) != want {
				t.Errorf("namespace %q: %d matches, want %d", ns, len(out.Matches), want)
			}
		}
		out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web-4", Namespace: "shop", At: w1000})
		if want := []string{"web-4"}; !slices.Equal(foundNames(out), want) {
			t.Errorf("names %v, want %v", foundNames(out), want)
		}
	})

	t.Run("nodes and containers", func(t *testing.T) {
		out := found(t, s, FindEntitiesInput{Type: "k8s.node", At: w1000})
		if want := []string{"worker-1", "worker-2", "worker-3"}; !slices.Equal(foundNames(out), want) {
			t.Errorf("nodes %v, want %v", foundNames(out), want)
		}
		for _, m := range out.Matches {
			if m.Entity.Namespace != nil {
				t.Errorf("a node has the namespace %s", *m.Entity.Namespace)
			}
		}
		out = found(t, s, FindEntitiesInput{Type: "container", Name: "NGIN", From: w10, To: w1030})
		if want := []string{"nginx"}; !slices.Equal(foundNames(out), want) || out.Matches[0].Entity.FP != w.c1 {
			t.Errorf("containers %v", foundNames(out))
		}
		// The cluster receiver's namespace loses under the ranks: no container has one.
		out = found(t, s, FindEntitiesInput{Type: "container", Namespace: "shop-cluster", From: w10, To: w1030})
		if len(out.Matches) != 0 || out.Counts.Checked != 1 {
			t.Errorf("a namespace of the losing producer matched: %+v", out)
		}
	})

	t.Run("the quarantined, the unborn and the gone are not listed", func(t *testing.T) {
		// Nothing of this type is alive before 09:00.
		out := found(t, s, FindEntitiesInput{Type: "k8s.pod", At: "2026-01-01T08:00:00Z"})
		if len(out.Matches) != 0 || out.Counts.Checked != 7 || !out.Complete {
			t.Errorf("pods at 08:00: %+v", out)
		}
		if out.Counts.Matched != 0 || slices.ContainsFunc(out.Notes, func(n string) bool { return strings.HasPrefix(n, "Matches are") }) {
			t.Errorf("counts %+v, notes %q", out.Counts, out.Notes)
		}
	})
}

// The id cursor: a call that stops at its limit gives the last id it checked, and
// the next call, with the same token, goes on from it.
func TestFindLimitStopsAndTheCursorContinues(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	all := found(t, s, FindEntitiesInput{Type: "k8s.pod", From: w10, To: w1030, Limit: 200})
	if len(all.Matches) != 7 || !all.Complete {
		t.Fatalf("%d pods, complete %t", len(all.Matches), all.Complete)
	}
	token := all.Token
	var pages []FindEntitiesOutput
	var got []string
	after := ""
	for len(pages) < 20 {
		out := found(t, s, FindEntitiesInput{Type: "k8s.pod", From: w10, To: w1030, Limit: 3, AsOf: token, After: after})
		pages = append(pages, out)
		got = append(got, foundIDs(out)...)
		if out.Complete {
			break
		}
		if out.NextAfter == nil {
			t.Fatal("an incomplete answer has no next_after")
		}
		after = *out.NextAfter
	}
	if len(pages) != 3 {
		t.Fatalf("%d pages of 3 for 7 pods", len(pages))
	}
	slices.Sort(got)
	want := foundIDs(all)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("the pages hold %v, want %v", got, want)
	}
	first := pages[0]
	if len(first.Matches) != 3 || first.Counts.Checked != 3 || first.Complete {
		t.Errorf("first page: %d matches, checked %d, complete %t", len(first.Matches), first.Counts.Checked, first.Complete)
	}
	wantNote := fmt.Sprintf("Stopped at 3 matches after checking 3 k8s.pod ids, through %s. Pass after=%s and asof=%s to continue.", *first.NextAfter, *first.NextAfter, token)
	if first.Notes[0] != wantNote {
		t.Errorf("first note %q, want %q", first.Notes[0], wantNote)
	}
	last := pages[2]
	if last.Notes[0] != "Checked every k8s.pod id the store holds after the one given (1)." || last.Counts.Checked != 1 {
		t.Errorf("last page: %q, checked %d", last.Notes[0], last.Counts.Checked)
	}
	// A limit that stops at the last id is a complete answer.
	out := found(t, s, FindEntitiesInput{Type: "k8s.pod", From: w10, To: w1030, Limit: 7})
	if !out.Complete || out.NextAfter != nil || out.Counts.Checked != 7 {
		t.Errorf("limit 7 of 7: complete %t, next_after %v, checked %d", out.Complete, out.NextAfter, out.Counts.Checked)
	}
}

// The refusals, with their sentences, and none of them reads the store or asks for
// a listing.
func TestFindRefusals(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		in   FindEntitiesInput
		want string
	}{
		{"unknown type", FindEntitiesInput{Type: "x", At: w1000}, `The argument "type" names the entity type "x", which is not in the catalog. The types are ` + s.typeList() + `.`},
		{"no type", FindEntitiesInput{At: w1000}, `The argument "type" names the entity type "", which is not in the catalog. The types are ` + s.typeList() + `.`},
		{"endpoints", FindEntitiesInput{Type: "topo.endpoint", At: w1000}, "find_entities never lists endpoints: an endpoint's id reveals its address. Name one by its realm and address through resolve_entity."},
		{"hosts", FindEntitiesInput{Type: "host", At: w1000}, "Hosts are unnamed on purpose, so they cannot be found by name. A node's host is the target of its runs_on edge: ask neighbors_at of the node."},
		{"a type with no name", FindEntitiesInput{Type: "process", At: w1000}, "The type process has no name, so it cannot be found by one. find_entities lists k8s.pod, k8s.node, container and service."},
		{"an instance has no name", FindEntitiesInput{Type: "service.instance", At: w1000}, "The type service.instance has no name, so it cannot be found by one. find_entities lists k8s.pod, k8s.node, container and service."},
		{"a long name", FindEntitiesInput{Type: "k8s.pod", Name: strings.Repeat("a", 257), At: w1000}, `The argument "name" must be at most 256 bytes of UTF-8 text with a character that is not a space.`},
		{"a blank name", FindEntitiesInput{Type: "k8s.pod", Name: " \t", At: w1000}, `The argument "name" must be at most 256 bytes of UTF-8 text with a character that is not a space.`},
		{"a name that is not UTF-8", FindEntitiesInput{Type: "k8s.pod", Name: "a\xffb", At: w1000}, `The argument "name" must be at most 256 bytes of UTF-8 text with a character that is not a space.`},
		{"a blank namespace", FindEntitiesInput{Type: "k8s.pod", Namespace: "   ", At: w1000}, `The argument "namespace" must be at most 256 bytes of UTF-8 text with a character that is not a space.`},
		{"a namespace for nodes", FindEntitiesInput{Type: "k8s.node", Namespace: "shop", At: w1000}, `Nodes have no namespace; leave "namespace" out.`},
		{"no window", FindEntitiesInput{Type: "k8s.pod"}, `Give "at", or "from" and "to".`},
		{"at and from", FindEntitiesInput{Type: "k8s.pod", At: w1000, From: w10, To: w1030}, `Give either "at", or "from" and "to", not both.`},
		{"an empty window", FindEntitiesInput{Type: "k8s.pod", From: w1030, To: w10}, `The argument "from" (2026-01-01T10:30:00Z) must be before "to" (2026-01-01T10:00:00Z).`},
		{"a limit below 1", FindEntitiesInput{Type: "k8s.pod", At: w1000, Limit: -1}, `The argument "limit" must be between 1 and 200; got -1.`},
		{"a limit above 200", FindEntitiesInput{Type: "k8s.pod", At: w1000, Limit: 201}, `The argument "limit" must be between 1 and 200; got 201.`},
		{"after malformed", FindEntitiesInput{Type: "k8s.pod", At: w1000, After: "x"}, `The argument "after" must be an entity id such as host:0123456789abcdef0123456789abcdef; got "x".`},
		{"after of another type", FindEntitiesInput{Type: "k8s.pod", At: w1000, After: w.n1.String()}, `The argument "after" must be an id of type k8s.pod, from the next_after of an earlier answer.`},
		{"asof", FindEntitiesInput{Type: "k8s.pod", At: w1000, AsOf: "x"}, `The argument "asof" must be a token as a decimal number, such as 81234; got "x".`},
	} {
		_, err := s.FindEntities(ctx, tc.in)
		var te *Error
		if !errors.As(err, &te) || te.Message != tc.want || !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: %v, want %q wrapping ErrInvalid", tc.name, err, tc.want)
		}
	}
	if spy.reads != 0 || spy.lists != 0 {
		t.Errorf("%d reads and %d listings for refused arguments", spy.reads, spy.lists)
	}
	// A valid call does ask for a listing, once.
	found(t, s, FindEntitiesInput{Type: "k8s.pod", At: w1000})
	if spy.lists != 1 || spy.reads == 0 {
		t.Errorf("%d listings and %d reads for a valid call", spy.lists, spy.reads)
	}

	t.Run("a store without range reads", func(t *testing.T) {
		spy := &spyStore{Store: w.st}
		s, err := NewSource(Config{Store: spy, Rank: rankObjs})
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range []FindEntitiesInput{{Type: "k8s.pod", At: w1000}, {Type: "host"}} {
			_, err := s.FindEntities(ctx, in)
			var te *Error
			if !errors.As(err, &te) || te.Message != "Range answers are not available on this store." || !errors.Is(err, ErrNoRangeReads) {
				t.Errorf("%+v: %v", in, err)
			}
		}
		if spy.reads != 0 || spy.lists != 0 {
			t.Errorf("%d reads and %d listings", spy.reads, spy.lists)
		}
	})

	t.Run("a store with range reads and no listing read", func(t *testing.T) {
		spy := &spyStore{Store: w.st}
		s, err := NewSource(Config{Store: nilCollisionStore{spy}, Rank: rankObjs, RangeReads: true})
		if err != nil {
			t.Fatal(err)
		}
		_, err = s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.pod", At: w1000})
		var te *Error
		if !errors.As(err, &te) || te.Message != "find_entities needs a listing read, which this store does not offer." || !errors.Is(err, ErrNoListing) {
			t.Errorf("valid call: %v", err)
		}
		// An argument is named on any store.
		_, err = s.FindEntities(ctx, FindEntitiesInput{Type: "host", At: w1000})
		if !errors.As(err, &te) || !strings.HasPrefix(te.Message, "Hosts are unnamed on purpose") {
			t.Errorf("a refused argument: %v", err)
		}
		// The other tools still answer.
		if _, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: w.n1.String(), At: w1000}); err != nil {
			t.Errorf("neighbors_at: %v", err)
		}
		if _, err := s.DescribeSchema(ctx, DescribeSchemaInput{}); err != nil {
			t.Errorf("describe_schema: %v", err)
		}
		if spy.lists != 0 {
			t.Errorf("%d listings through a store that hides the method", spy.lists)
		}
	})
}

// listStores are the two stores that have a listing read.
func listStores(t *testing.T, rank map[lifecycle.Producer]int) map[string]store.Store {
	t.Helper()
	return map[string]store.Store{
		"memstore":    openMem(t, memstore.Options{Policy: servePolicy(rank)}),
		"pebblestore": openPeb(t, servePolicy(rank), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous),
	}
}

// The types find_entities lists are exactly the types the name table names.
func TestFindTypesAreTheNamedTypes(t *testing.T) {
	if len(findTypes) != len(nameTable) {
		t.Errorf("%d types listed, %d named", len(findTypes), len(nameTable))
	}
	for _, typ := range findTypes {
		if _, ok := nameTable[typ]; !ok {
			t.Errorf("%s is listed and not named", typ)
		}
	}
}

// The source finds the listing read of the store it is given.
func TestFindUsesTheListerOfTheStoreGiven(t *testing.T) {
	for name, st := range listStores(t, rankObjs) {
		s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true})
		if err != nil {
			t.Fatal(err)
		}
		if s.lister == nil {
			t.Errorf("%s: no lister", name)
		}
	}
	s, err := NewSource(Config{Store: nilCollisionStore{openMem(t, memstore.Options{})}, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	if s.lister != nil {
		t.Error("a lister was found on a wrapper that does not have the method")
	}
}

// pagedWorld is 300 pods, pod-000 to pod-299, in two namespaces, 40 of them deleted
// before the instant, and three written after the token.
type pagedWorld struct {
	pods  []identity.Fingerprint
	late  []identity.Fingerprint
	token uint64
	live  map[identity.Fingerprint]string // the name of each pod alive at the instant
}

func newPagedWorld(tb testing.TB, st store.Store) *pagedWorld {
	tb.Helper()
	w := &pagedWorld{live: map[identity.Fingerprint]string{}}
	f := newFeed(tb, st)
	var recs []store.Record
	for i := range 300 {
		fp := fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("paged-%d", i))
		ns := "alpha"
		if i%2 == 1 {
			ns = "beta"
		}
		name := fmt.Sprintf("pod-%03d", i)
		recs = append(recs, observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":%q,"namespace":%q}`, name, ns)))
		w.pods = append(w.pods, fp)
		if i%7 == 0 && i < 280 {
			recs = append(recs, deleted(ent(fp), objs, hm(9, 30)))
			continue
		}
		w.live[fp] = name
	}
	f.write(recs...)
	w.token = st.LastSeq()
	for i := range 3 {
		fp := fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("paged-late-%d", i))
		f.write(observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"pod-1late-%d","namespace":"alpha"}`, i)))
		w.late = append(w.late, fp)
	}
	return w
}

// bruteForce is every pod alive at the instant whose name contains text, read one
// by one at the token.
func (w *pagedWorld) bruteForce(t *testing.T, st store.Store, text string) []string {
	t.Helper()
	rd := interval.NewReader(st, interval.Options{Describe: true, Rank: rankObjs})
	instant := hm(10, 0)
	var out []string
	for _, fp := range append(slices.Clone(w.pods), w.late...) {
		res, err := rd.Entity(ctx, fp, interval.Window{From: instant, To: instant.Add(time.Nanosecond), AsOf: w.token})
		if err != nil {
			t.Fatal(err)
		}
		el := res.Elements[0]
		if len(el.Pieces) == 0 {
			continue
		}
		name, _, _ := nameFields(catalog.K8sPod, el.AtLast.Payload, printable)
		if name != nil && strings.Contains(strings.ToLower(*name), text) {
			out = append(out, fp.String())
		}
	}
	slices.Sort(out)
	return out
}

// Pages at one token are exact: following next_after with the same asof lists every
// pod alive at the instant whose name matches, once, none of the pods written after
// the token, in no more than 256 ids a call, and the two stores give the same bytes.
func TestFindPagesAreExact(t *testing.T) {
	stores := listStores(t, rankObjs)
	worlds := map[string]*pagedWorld{}
	sources := map[string]*Source{}
	for name, st := range stores {
		worlds[name] = newPagedWorld(t, st)
		s, err := NewSource(Config{Store: st, Rank: rankObjs, RangeReads: true})
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = s
	}
	w := worlds["memstore"]
	want := w.bruteForce(t, stores["memstore"], "pod-1")
	if len(want) < 50 {
		t.Fatalf("the fixture has only %d matches", len(want))
	}
	allIDs := slices.Clone(w.pods)
	allIDs = append(allIDs, w.late...)
	slices.SortFunc(allIDs, store.CompareFingerprints)

	for _, limit := range []int{7, 50, 200} {
		t.Run(fmt.Sprintf("limit %d", limit), func(t *testing.T) {
			var union []string
			var checkedTotal, pages int
			after := ""
			for pages < 1000 {
				var answers [][]byte
				var out FindEntitiesOutput
				for _, name := range []string{"memstore", "pebblestore"} {
					in := FindEntitiesInput{Type: "k8s.pod", Name: "pod-1", At: w1000, Limit: limit, AsOf: tokenString(worlds[name].token), After: after}
					out = found(t, sources[name], in)
					b, err := json.Marshal(out)
					if err != nil {
						t.Fatal(err)
					}
					answers = append(answers, b)
				}
				if string(answers[0]) != string(answers[1]) {
					t.Fatalf("page %d differs:\nmemstore    %s\npebblestore %s", pages, answers[0], answers[1])
				}
				pages++
				if out.Counts.Checked > 256 || len(out.Matches) > limit {
					t.Errorf("page %d: checked %d, %d matches", pages, out.Counts.Checked, len(out.Matches))
				}
				union = append(union, foundIDs(out)...)
				checkedTotal += out.Counts.Checked
				if out.Complete {
					break
				}
				after = *out.NextAfter
			}
			slices.Sort(union)
			if !slices.Equal(union, want) {
				t.Errorf("%d pages list %d ids, want %d: %v", pages, len(union), len(want), union)
			}
			if checkedTotal != len(allIDs) {
				t.Errorf("the pages checked %d ids in all, the store holds %d: one was read twice or skipped", checkedTotal, len(allIDs))
			}
			t.Logf("limit %d: %d pages, %d matches, %d ids checked", limit, pages, len(union), checkedTotal)
		})
	}

	t.Run("the scan cap", func(t *testing.T) {
		// 10 pods match and sit among 303 ids: a call checks 256 and says so.
		s := sources["memstore"]
		first := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "pod-29", At: w1000, Limit: 200, AsOf: tokenString(w.token)})
		if first.Complete || first.Counts.Checked != 256 || first.NextAfter == nil || *first.NextAfter != allIDs[255].String() {
			t.Fatalf("complete %t, checked %d, next_after %v", first.Complete, first.Counts.Checked, first.NextAfter)
		}
		wantNote := fmt.Sprintf("Checked 256 k8s.pod ids, in id order, through %s, and stopped at the scan cap of 256; more remain. Pass after=%s and asof=%d to continue.", allIDs[255], allIDs[255], w.token)
		if first.Notes[0] != wantNote {
			t.Errorf("note %q, want %q", first.Notes[0], wantNote)
		}
		second := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "pod-29", At: w1000, Limit: 200, AsOf: tokenString(w.token), After: *first.NextAfter})
		if !second.Complete || second.Counts.Checked != len(allIDs)-256 {
			t.Errorf("second page: complete %t, checked %d", second.Complete, second.Counts.Checked)
		}
		if got := len(first.Matches) + len(second.Matches); got != 10 {
			t.Errorf("%d matches in all, want 10", got)
		}
	})

	t.Run("no name lists a page and stops at the limit", func(t *testing.T) {
		out := found(t, sources["memstore"], FindEntitiesInput{Type: "k8s.pod", At: w1000, Limit: 200, AsOf: tokenString(w.token)})
		if len(out.Matches) != 200 || out.Complete || out.Counts.Checked > 256 {
			t.Errorf("%d matches, complete %t, checked %d", len(out.Matches), out.Complete, out.Counts.Checked)
		}
	})
}

// The scan cap is exactly 256 ids: 256 are a whole answer, 257 are not, and the
// next call has the one that remains.
func TestFindScanCapBoundary(t *testing.T) {
	for _, n := range []int{255, 256, 257} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			st := openMem(t, memstore.Options{Policy: servePolicy(rankObjs)})
			f := newFeed(t, st)
			var recs []store.Record
			var fps []identity.Fingerprint
			for i := range n {
				fp := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("cap-%d", i))
				fps = append(fps, fp)
				recs = append(recs, observe(ent(fp), objs, hm(9, 0), 0, `{"name":"n"}`))
			}
			f.write(recs...)
			slices.SortFunc(fps, store.CompareFingerprints)
			spy := &spyStore{Store: st}
			s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
			if err != nil {
				t.Fatal(err)
			}
			out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "zzz", At: w1000})
			if n <= 256 {
				if !out.Complete || out.NextAfter != nil || out.Counts.Checked != n {
					t.Fatalf("complete %t, next_after %v, checked %d", out.Complete, out.NextAfter, out.Counts.Checked)
				}
				return
			}
			if out.Complete || out.Counts.Checked != 256 || out.NextAfter == nil || *out.NextAfter != fps[255].String() {
				t.Fatalf("complete %t, checked %d, next_after %v", out.Complete, out.Counts.Checked, out.NextAfter)
			}
			next := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "zzz", At: w1000, After: *out.NextAfter, AsOf: out.Token})
			if !next.Complete || next.Counts.Checked != 1 {
				t.Errorf("the rest: complete %t, checked %d", next.Complete, next.Counts.Checked)
			}
		})
	}
}

// serviceWorld writes services the way the service translators do: a payload with
// the name and the namespace, from two producers that are not ranked, with the same
// bytes.
func serviceWorld(tb testing.TB, st store.Store) (*feed, map[string]identity.Fingerprint) {
	tb.Helper()
	f := newFeed(tb, st)
	svc := func(ns, name string) identity.Fingerprint {
		if ns == "" {
			return fpOf(tb, catalog.Service, catalog.ServiceName, name)
		}
		return fpOf(tb, catalog.Service, catalog.ServiceNamespace, ns, catalog.ServiceName, name)
	}
	fps := map[string]identity.Fingerprint{
		"checkout": svc("shop", "checkout"), "frontend": svc("shop", "frontend"), "cart": svc("", "Cart"), "anon": svc("", "anon"),
	}
	var recs []store.Record
	for _, p := range []lifecycle.Producer{"svcgraph/c1", "spancount/c1"} {
		for name, payload := range map[string]string{
			"checkout": `{"name":"checkout","namespace":"shop"}`,
			"frontend": `{"name":"frontend","namespace":"shop"}`,
			"cart":     `{"name":"Cart"}`,
			"anon":     `{"namespace":"elsewhere"}`,
		} {
			recs = append(recs, observe(ent(fps[name]), p, hm(9, 0), 6*time.Hour, payload))
		}
	}
	f.write(recs...)
	return f, fps
}

// Services are found by the name and the namespace of their description.
func TestFindServices(t *testing.T) {
	for sname, st := range listStores(t, nil) {
		t.Run(sname, func(t *testing.T) {
			_, fps := serviceWorld(t, st)
			s, err := NewSource(Config{Store: st, RangeReads: true})
			if err != nil {
				t.Fatal(err)
			}
			in := FindEntitiesInput{Type: "service", From: w10, To: w1030}

			all := found(t, s, in)
			// By name in bytes, a service with none last.
			if want := []string{"Cart", "checkout", "frontend", "-"}; !slices.Equal(foundNames(all), want) {
				t.Fatalf("names %v, want %v", foundNames(all), want)
			}
			if all.Matches[0].Entity.FP != fps["cart"] || all.Matches[0].Entity.Namespace != nil {
				t.Errorf("Cart: %+v", all.Matches[0].Entity)
			}
			// Two producers, no ranks: the smaller name wins, and the name is the same.
			if all.Matches[0].Entity.NameFrom != "spancount/c1" {
				t.Errorf("name_from %q", all.Matches[0].Entity.NameFrom)
			}
			if anon := all.Matches[3].Entity; anon.Name != nil || anon.Namespace == nil || *anon.Namespace != "elsewhere" || anon.NameFrom != "" || anon.NameNote != "" {
				t.Errorf("a service with a namespace and no name: %+v", anon)
			}

			in.Name = "CART"
			if got := found(t, s, in); !slices.Equal(foundIDs(got), []string{fps["cart"].String()}) {
				t.Errorf("name CART: %v", foundNames(got))
			}
			in.Name = ""
			in.Namespace = "shop"
			if got := found(t, s, in); !slices.Equal(foundNames(got), []string{"checkout", "frontend"}) {
				t.Errorf("namespace shop: %v", foundNames(got))
			}
			in.Name, in.Namespace = "check", "sho"
			if got := found(t, s, in); len(got.Matches) != 0 || got.Counts.Checked != 4 {
				t.Errorf("a namespace is exact: %v, checked %d", foundNames(got), got.Counts.Checked)
			}
			in.Name, in.Namespace = "check", "shop"
			if got := found(t, s, in); !slices.Equal(foundNames(got), []string{"checkout"}) {
				t.Errorf("check in shop: %v", foundNames(got))
			}
			// A service has no namespace to match when its description gives none.
			in.Name, in.Namespace = "cart", "shop"
			if got := found(t, s, in); len(got.Matches) != 0 {
				t.Errorf("Cart has no namespace but matched shop: %v", foundNames(got))
			}
		})
	}
}

// A name is the winning producer's, never a losing one's.
func TestFindReadsTheNameOfTheWinner(t *testing.T) {
	window := FindEntitiesInput{Type: "k8s.pod", At: "2026-01-01T10:08:00Z"}
	objsWorld := newWorld(t, rankObjs)
	clusWorld := newWorld(t, rankClus)
	so, sc := objsWorld.source(t, rankObjs, true), clusWorld.source(t, rankClus, true)

	window.Name = "cluster"
	if out := found(t, so, window); len(out.Matches) != 0 {
		t.Errorf("under the ranks of k8sobjects, %v matched the name of the cluster receiver", foundNames(out))
	}
	out := found(t, sc, window)
	if !slices.Equal(foundIDs(out), []string{clusWorld.p1.String()}) || out.Matches[0].Entity.NameFrom != string(clus) || *out.Matches[0].Entity.Name != "web-0-cluster" {
		t.Errorf("under the ranks of the cluster receiver: %v, %+v", foundNames(out), out.Matches)
	}
	window.Name = "web-0"
	if out := found(t, so, window); len(out.Matches) != 1 || out.Matches[0].Entity.NameFrom != string(objs) || *out.Matches[0].Entity.Name != "web-0" {
		t.Errorf("web-0 under the ranks of k8sobjects: %+v", out.Matches)
	}
}

// An entity that exists in the window but at neither end of it has no description to
// read a name from.
func TestFindCountsTheUndescribed(t *testing.T) {
	w := newWorld(t, rankObjs)
	mid := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-mid")
	w.feed.write(
		observe(ent(mid), objs, hm(10, 10), 0, `{"name":"web-mid","namespace":"shop"}`),
		deleted(ent(mid), objs, hm(10, 20)),
	)
	s := w.source(t, rankObjs, true)

	out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web", From: w10, To: w1030})
	if out.Counts.Undescribed != 1 || len(out.Matches) != 7 || slices.Contains(foundIDs(out), mid.String()) {
		t.Errorf("counts %+v, %d matches", out.Counts, len(out.Matches))
	}
	wantNote := "1 k8s.pod entities existed in the window but have no description at either end of it, so their names are not known; a window that ends while they exist names them."
	if !contains(out.Notes, wantNote) {
		t.Errorf("notes %q", out.Notes)
	}

	// With no filter it is listed, with no name and the reason.
	all := found(t, s, FindEntitiesInput{Type: "k8s.pod", From: w10, To: w1030})
	if len(all.Matches) != 8 || all.Counts.Undescribed != 0 {
		t.Fatalf("%d matches, counts %+v", len(all.Matches), all.Counts)
	}
	last := all.Matches[7]
	if last.Entity.FP != mid || last.Entity.Name != nil || last.Entity.NameNote != notPresent {
		t.Errorf("the undescribed pod: %+v", last.Entity)
	}
	// A window that ends while it exists names it.
	at := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "mid", At: "2026-01-01T10:15:00Z"})
	if !slices.Equal(foundIDs(at), []string{mid.String()}) {
		t.Errorf("at 10:15: %v", foundNames(at))
	}
}

// After a retention a description recorded before the horizon is gone: the call
// counts the entities it cannot read and answers for the others, and a window before
// the horizon gets the horizon's sentence.
func TestFindAfterARetention(t *testing.T) {
	for sname, mk := range map[string]func(testing.TB) store.Store{
		"memstore": func(tb testing.TB) store.Store { return openMem(tb, memstore.Options{Policy: servePolicy(rankObjs)}) },
		"pebblestore": func(tb testing.TB) store.Store {
			return openPeb(tb, servePolicy(rankObjs), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous)
		},
	} {
		t.Run(sname, func(t *testing.T) {
			w := newWorldOn(t, mk(t))
			if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
				t.Fatal(err)
			}
			s := w.source(t, rankObjs, true)
			out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web", From: w10, To: w1030})
			// web-2 (from 10:06) and web-5 (written at 10:00) are described by records
			// the window holds; the five others by a record of 09:00.
			if want := []string{"web-2", "web-5"}; !slices.Equal(foundNames(out), want) {
				t.Errorf("names %v, want %v", foundNames(out), want)
			}
			if want := (FindCounts{Checked: 7, Matched: 2, Retained: 5}); out.Counts != want || !out.Complete {
				t.Errorf("counts %+v, complete %t, want %+v", out.Counts, out.Complete, want)
			}
			wantNote := "5 had a description recorded before the retention horizon, which the store no longer keeps, so they could not be matched."
			if !contains(out.Notes, wantNote) {
				t.Errorf("notes %q", out.Notes)
			}

			_, err := s.FindEntities(ctx, FindEntitiesInput{Type: "k8s.pod", From: "2026-01-01T09:00:00Z", To: w1030})
			var te *Error
			if !errors.As(err, &te) || !strings.HasPrefix(te.Message, "The store no longer keeps history of layer L2 before 2026-01-01T09:30:00Z") || !errors.Is(err, store.ErrBeforeHorizon) {
				t.Errorf("a window before the horizon: %v", err)
			}
		})
	}
}

// countStore counts the reads that cost an entity's history.
type countStore struct {
	store.Store
	mu       sync.Mutex
	alive    int
	since    int
	sinceFPs int
	lists    []int
}

func (c *countStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	c.mu.Lock()
	c.alive++
	c.mu.Unlock()
	return c.Store.Alive(ctx, fp, t, sc)
}

func (c *countStore) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.mu.Lock()
	c.since++
	c.sinceFPs += len(fps)
	c.mu.Unlock()
	return c.Store.WindowSince(ctx, fps, d, from, to, sc, o)
}

func (c *countStore) ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, n int) ([]identity.Fingerprint, error) {
	c.mu.Lock()
	c.lists = append(c.lists, n)
	c.mu.Unlock()
	return c.Store.(Lister).ListEntities(ctx, typ, after, n)
}

func (c *countStore) reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.alive, c.since, c.sinceFPs, c.lists = 0, 0, 0, nil
}

// A call reads each id it checks once: one existence read and one history read per
// id, in batches of 32, one listing of 257, and no second read of the names.
func TestFindCosts(t *testing.T) {
	st := openMem(t, memstore.Options{Policy: servePolicy(rankObjs)})
	f := newFeed(t, st)
	var recs []store.Record
	for i := range 100 {
		fp := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("cost-%d", i))
		recs = append(recs, observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"cost-%03d","namespace":"shop"}`, i)))
	}
	f.write(recs...)
	cs := &countStore{Store: st}
	s, err := NewSource(Config{Store: cs, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}

	out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "cost-07", From: w10, To: w1030})
	if out.Counts.Checked != 100 || len(out.Matches) != 10 {
		t.Fatalf("checked %d, %d matches", out.Counts.Checked, len(out.Matches))
	}
	if cs.alive != out.Counts.Checked || cs.since != 4 || cs.sinceFPs != out.Counts.Checked || !slices.Equal(cs.lists, []int{257}) {
		t.Errorf("alive %d, history reads %d of %d ids, listings %v for %d checked", cs.alive, cs.since, cs.sinceFPs, cs.lists, out.Counts.Checked)
	}

	// A call that stops at its limit reads only the batches it needed.
	cs.reset()
	out = found(t, s, FindEntitiesInput{Type: "k8s.pod", From: w10, To: w1030, Limit: 5})
	if out.Counts.Checked != 5 || cs.alive != 32 || cs.since != 1 || cs.sinceFPs != 32 || !slices.Equal(cs.lists, []int{257}) {
		t.Errorf("limit 5: checked %d, alive %d, history reads %d of %d ids, listings %v", out.Counts.Checked, cs.alive, cs.since, cs.sinceFPs, cs.lists)
	}

	// After a retention the batch fails and its ids are read one by one.
	w := newWorld(t, rankObjs)
	if err := w.st.Retain(ctx, hm(9, 30)); err != nil {
		t.Fatal(err)
	}
	cs = &countStore{Store: w.st}
	s, err = NewSource(Config{Store: cs, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	out = found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "web", From: w10, To: w1030})
	if out.Counts.Checked != 7 || cs.alive != 14 || cs.since != 8 || cs.sinceFPs != 14 {
		t.Errorf("retained: checked %d, alive %d, history reads %d of %d ids", out.Counts.Checked, cs.alive, cs.since, cs.sinceFPs)
	}
}

// A match that carries a name of several kilobytes (the print rule is off, which
// is how a text beyond 253 bytes reaches the answer at all): the answer is cut at 256 KiB
// whole, the cursor moves back to the id before the first match left out, and the
// next page lists it. No match is lost.
func TestFindCutsAtTheSizeCapAndLosesNoMatch(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			pad := strings.Repeat("x", 5000)
			var recs []store.Record
			var fps []identity.Fingerprint
			for i := range 200 {
				fp := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("big-%d", i))
				fps = append(fps, fp)
				recs = append(recs, observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"big-%03d-%s","namespace":"shop"}`, i, pad)))
			}
			f.write(recs...)
			slices.SortFunc(fps, store.CompareFingerprints)
			s := sourceOn(t, st, &egressHooks{noPrintRule: true}, nil)
			var union []string
			after, pages, position := "", 0, 0
			for pages < 100 {
				out := found(t, s, FindEntitiesInput{Type: "k8s.pod", Name: "big-", At: w1000, Limit: 200, After: after})
				pages++
				b, err := json.Marshal(out)
				if err != nil {
					t.Fatal(err)
				}
				if len(b) > maxAnswerBytes {
					t.Errorf("page %d is %d bytes", pages, len(b))
				}
				union = append(union, foundIDs(out)...)
				position += out.Counts.Checked
				if out.Complete {
					break
				}
				if pages == 1 {
					if len(out.Matches) == 0 || len(out.Matches) >= 200 || out.Counts.Matched != len(out.Matches) {
						t.Fatalf("the first page has %d matches, counted %d", len(out.Matches), out.Counts.Matched)
					}
					wantNote := fmt.Sprintf("The answer was cut at 256 KiB after %d %s, through %s. Pass after=%s and asof=%s to continue.", len(out.Matches), matchWord(len(out.Matches)), *out.NextAfter, *out.NextAfter, out.Token)
					if out.Notes[0] != wantNote {
						t.Errorf("note %q, want %q", out.Notes[0], wantNote)
					}
				}
				// The cursor is the id at the position the page counted through.
				if *out.NextAfter != fps[position-1].String() {
					t.Fatalf("page %d: next_after %s, but %d ids were checked, through %s", pages, *out.NextAfter, position, fps[position-1])
				}
				after = *out.NextAfter
			}
			if pages < 3 {
				t.Errorf("only %d pages", pages)
			}
			slices.Sort(union)
			if len(union) != 200 || len(slices.Compact(slices.Clone(union))) != 200 {
				t.Errorf("%d matches in %d pages, want 200 and no duplicate", len(union), pages)
			}
			if position != 200 {
				t.Errorf("%d ids checked in all, want 200", position)
			}
		})
	}
}

// A first match that alone is larger than the answer budget: it is still listed,
// so that the cursor moves past it, but its name, or its namespace if that is the
// larger, is withheld with a note, and the answer stays within the hard cap. The
// print rule is off, because it bounds every name and namespace to 253 bytes and
// leaves this backstop nothing to catch. Both stores, with a name of 300 KiB, a name of 50 KiB of characters that grow six-fold
// when escaped, and a namespace of 300 KiB.
func TestFindWithholdsTheTextOfAMatchLargerThanTheCap(t *testing.T) {
	cases := []struct {
		label     string
		name, ns  string
		wantNote  string
		keepsName bool // else the namespace stays
	}{
		{"name 300 KiB", strings.Repeat("n", 300<<10), "shop", nameTooLarge, false},
		{"name 50 KiB escaped six-fold", strings.Repeat("<", 50<<10), "shop", nameTooLarge, false},
		{"namespace 300 KiB", "tiny", strings.Repeat("s", 300<<10), nsTooLarge, true},
	}
	opens := map[string]func(t *testing.T) store.Store{
		"memstore": func(t *testing.T) store.Store { return openMem(t, memstore.Options{Policy: servePolicy(rankObjs)}) },
		"pebblestore": func(t *testing.T) store.Store {
			return openPeb(t, servePolicy(rankObjs), pebblestore.DefaultCheckpoints(), pebblestore.Synchronous)
		},
	}
	for sname, open := range opens {
		for _, c := range cases {
			t.Run(sname+"/"+c.label, func(t *testing.T) {
				st := open(t)
				fps := make([]identity.Fingerprint, 5)
				for i := range fps {
					fps[i] = fpOf(t, catalog.K8sPod, catalog.K8sPodUID, fmt.Sprintf("huge-%d", i))
				}
				slices.SortFunc(fps, store.CompareFingerprints)
				f := newFeed(t, st)
				var recs []store.Record
				for i, fp := range fps {
					name, ns := fmt.Sprintf("small-%d", i), "shop"
					if i == 0 {
						name, ns = c.name, c.ns
					}
					recs = append(recs, observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":%q,"namespace":%q}`, name, ns)))
				}
				f.write(recs...)
				s := sourceOn(t, st, &egressHooks{noPrintRule: true}, nil)

				var seen []string
				after := ""
				for page := 1; ; page++ {
					if page > len(fps)+1 {
						t.Fatalf("still paging after %d pages", page-1)
					}
					out := found(t, s, FindEntitiesInput{Type: "k8s.pod", At: w1000, After: after})
					b, err := json.Marshal(out)
					if err != nil {
						t.Fatal(err)
					}
					if len(b) > maxAnswerBytes {
						t.Fatalf("page %d is %d bytes, over the cap of %d", page, len(b), maxAnswerBytes)
					}
					for _, m := range out.Matches {
						seen = append(seen, m.Entity.ID)
					}
					switch page {
					case 1:
						if len(out.Matches) != 1 || out.Matches[0].Entity.ID != fps[0].String() {
							t.Fatalf("the first page lists %v, want only %s", foundIDs(out), fps[0])
						}
						r := out.Matches[0].Entity
						if r.Type != string(catalog.K8sPod) || len(out.Matches[0].Spans) == 0 {
							t.Errorf("the kept match lost its type or its existence: %+v", out.Matches[0])
						}
						if r.NameNote != c.wantNote {
							t.Errorf("name_note %q, want %q", r.NameNote, c.wantNote)
						}
						if c.keepsName {
							if r.Name == nil || *r.Name != c.name || r.Namespace != nil {
								t.Errorf("the name should stay and the namespace go: %+v", r)
							}
						} else if r.Name != nil || r.Namespace == nil || *r.Namespace != c.ns {
							t.Errorf("the name should go and the namespace stay: %+v", r)
						}
						if !c.keepsName && strings.Contains(string(b), c.name[:64]) {
							t.Errorf("the withheld name is in the answer")
						}
						if c.keepsName && strings.Contains(string(b), c.ns[:64]) {
							t.Errorf("the withheld namespace is in the answer")
						}
						if out.Complete || out.NextAfter == nil {
							t.Fatalf("complete %v, next_after %v after one match of five", out.Complete, out.NextAfter)
						}
						wantNote := fmt.Sprintf("The answer was cut at 256 KiB after 1 match, through %s. Pass after=%s and asof=%s to continue.", *out.NextAfter, *out.NextAfter, out.Token)
						if out.Notes[0] != wantNote {
							t.Errorf("note %q, want %q", out.Notes[0], wantNote)
						}
					case 2:
						for _, m := range out.Matches {
							if m.Entity.NameNote != "" {
								t.Errorf("page 2 withheld the text of %s", m.Entity.ID)
							}
						}
					}
					if out.Complete {
						break
					}
					cursor, err := identity.ParseFingerprint(*out.NextAfter)
					if err != nil {
						t.Fatal(err)
					}
					// The cursor is strictly past where the page began, and past the kept match.
					if page > 1 {
						prev, err := identity.ParseFingerprint(after)
						if err != nil {
							t.Fatal(err)
						}
						if store.CompareFingerprints(cursor, prev) <= 0 {
							t.Fatalf("page %d: next_after %s did not advance past after %s", page, cursor, prev)
						}
					}
					if page == 1 && store.CompareFingerprints(cursor, fps[0]) < 0 {
						t.Fatalf("next_after %s is before the kept match %s", cursor, fps[0])
					}
					after = *out.NextAfter
				}
				want := make([]string, len(fps))
				for i, fp := range fps {
					want[i] = fp.String()
				}
				slices.Sort(want)
				slices.Sort(seen)
				if !slices.Equal(seen, want) {
					t.Errorf("the pages listed %v, want each of %v once", seen, want)
				}
			})
		}
	}
}

// A first match that fits alone is never changed.
func TestFindKeepsTheTextOfAMatchThatFits(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			fp := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "fits")
			f.write(observe(ent(fp), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":%q,"namespace":"shop"}`, strings.Repeat("n", 100<<10))))
			s := sourceOn(t, st, &egressHooks{noPrintRule: true}, nil)
			out := found(t, s, FindEntitiesInput{Type: "k8s.pod", At: w1000})
			if len(out.Matches) != 1 || out.Matches[0].Entity.Name == nil || out.Matches[0].Entity.NameNote != "" || !out.Complete {
				t.Errorf("%+v", out)
			}
		})
	}
}
