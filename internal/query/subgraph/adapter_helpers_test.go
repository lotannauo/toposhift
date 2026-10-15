package subgraph_test

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
)

func svc(n int) identity.Fingerprint  { return mkfp(catalog.Service, n) }
func inst(n int) identity.Fingerprint { return mkfp(catalog.ServiceInstance, n) }

func partOf(i, p identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(i, p, catalog.PartOf)
}

func instOf(i, s identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(i, s, catalog.InstanceOf)
}

func dependsOn(a, b identity.Fingerprint) subgraph.Key {
	return subgraph.EdgeKey(a, b, catalog.DependsOn)
}

// win30 is the window [base, base+30s) read at the newest token.
func win30() interval.Window {
	return interval.Window{From: at(0), To: at(30), AsOf: store.Latest}
}

// obs writes an observation with no end, sec seconds after base.
func obs(k subgraph.Key, sec int) store.Record { return observe(k, k8s, sec, 0) }

// del writes a deletion by the producer, sec seconds after base.
func del(k subgraph.Key, sec int) store.Record { return deleted(k, k8s, sec) }

// inputOf is the input that would make sg: the elements as sources, so the
// oracle and the bounds checks of the builder's tests run on what an adapter
// built. It adds nothing the subgraph lacks.
func inputOf(sg subgraph.Subgraph) subgraph.Input {
	in := subgraph.Input{Token: sg.Token, From: sg.From, To: sg.To}
	for _, el := range sg.Elements {
		src := subgraph.Source{Key: el.Key, Class: el.Class, Transitions: el.Transitions}
		for _, p := range el.Pieces {
			src.Pieces = append(src.Pieces, subgraph.SourcePiece{
				Start: sg.From.Add(time.Duration(p.Start)), End: sg.From.Add(time.Duration(p.End)),
				Before: p.Before, EndReason: p.EndReason, Producers: p.Producers, Ends: p.Ends,
			})
		}
		for _, path := range el.Paths {
			var keys []subgraph.Key
			for _, id := range path {
				keys = append(keys, sg.Elements[id].Key)
			}
			src.Paths = append(src.Paths, keys)
		}
		in.Sources = append(in.Sources, src)
	}
	return in
}

// derivedCounts counts the derived elements in a set of element ids by their
// two ends and rule.
func derivedCounts(sg subgraph.Subgraph, set []uint32) map[string]int {
	out := map[string]int{}
	for _, id := range set {
		if k := sg.Elements[id].Key; k.Kind == subgraph.KindDerived {
			out[fmt.Sprintf("%s %s %s", k.Rule, k.A, k.B)]++
		}
	}
	return out
}

// checkDerived checks the derived elements of a subgraph at every instant of
// interest against want, the result's own answer at that instant, as counts by
// rule and ends; and the asserted elements against the store's point reads at
// the subgraph's token, restricted to the elements the subgraph has. It also
// runs the builder's oracle and bounds on what the adapter built.
func checkDerived(t tb, st store.Store, sg subgraph.Subgraph, want func(x time.Time) map[string]int) {
	t.Helper()
	check(t, inputOf(sg), sg)
	// Derived keys are unique, name their two ends as dictionary entries and keep
	// them out of the detail; every derived subject has a rule of the table.
	keys := map[subgraph.Key]bool{}
	for _, el := range sg.Elements {
		k := el.Key
		if keys[k] {
			t.Fatalf("two elements with the key %s", k)
		}
		keys[k] = true
		if k.Kind != subgraph.KindDerived {
			continue
		}
		for _, fp := range []identity.Fingerprint{k.A, k.B} {
			if _, ok := sg.EntityID(fp); !ok {
				t.Fatalf("%s: the end %s is not a dictionary entry", k, fp)
			}
			if strings.Contains(k.Detail, fp.String()) {
				t.Fatalf("%s: the end %s is in the detail", k, fp)
			}
		}
		if _, ok := subgraph.DerivedKindOf(k.Rule); !ok || el.Rule != k.Rule || el.Subject != subgraph.SubjectDerived {
			t.Fatalf("%s: rule %q subject %s", k, el.Rule, el.Subject)
		}
	}
	var scope []identity.Fingerprint
	for _, e := range sg.Entities {
		scope = append(scope, e.Fingerprint)
	}
	elementKeys := map[subgraph.Key]bool{}
	for _, el := range sg.Elements {
		elementKeys[el.Key] = true
	}
	for _, rel := range instants(sg) {
		x := sg.From.Add(time.Duration(rel))
		set := forward(t, sg, rel)
		if got, w := derivedCounts(sg, set), want(x); fmt.Sprint(got) != fmt.Sprint(w) {
			t.Fatalf("derived elements at +%dns: %v, the result says %v", rel, got, w)
		}
		points := pointPresence(t, st, scope, x, sg.Token)
		got := map[subgraph.Key]bool{}
		for _, id := range set {
			if k := sg.Elements[id].Key; k.Kind != subgraph.KindDerived {
				got[k] = true
			}
		}
		for k := range elementKeys {
			if k.Kind != subgraph.KindDerived && got[k] != points[k] {
				t.Fatalf("%s at +%dns: the subgraph says present=%t, the point reads say %t", k, rel, got[k], points[k])
			}
		}
	}
}

// derived finds the derived elements of a rule between two ends.
func derivedOf(sg subgraph.Subgraph, rule string, a, b identity.Fingerprint) []subgraph.Element {
	var out []subgraph.Element
	for _, el := range sg.Elements {
		if k := el.Key; k.Kind == subgraph.KindDerived && k.Rule == rule && k.A == a && k.B == b {
			out = append(out, el)
		}
	}
	return out
}

// secs is a piece in whole seconds after base, as a (start, end) pair.
func secs(p subgraph.Piece) [2]int {
	return [2]int{int(p.Start / uint64(time.Second)), int(p.End / uint64(time.Second))}
}

func pieceSecs(el subgraph.Element) [][2]int {
	var out [][2]int
	for _, p := range el.Pieces {
		out = append(out, secs(p))
	}
	return out
}

// sectionSecs is a span of a section in whole seconds after base.
func sectionSecs(ss []subgraph.Span) [][2]int {
	var out [][2]int
	for _, s := range ss {
		out = append(out, [2]int{int(s.Start / uint64(time.Second)), int(s.End / uint64(time.Second))})
	}
	return out
}

// legName writes the legs of a path as "from>to relation".
func legNames(sg subgraph.Subgraph, path []uint32) string {
	parts := make([]string, len(path))
	for i, id := range path {
		k := sg.Elements[id].Key
		parts[i] = fmt.Sprintf("%s>%s %s", k.A, k.B, k.Relation)
	}
	return strings.Join(parts, "; ")
}

func hasKey(sg subgraph.Subgraph, k subgraph.Key) bool {
	_, ok := sg.ElementID(k)
	return ok
}

// quarantinedKeys lists the keys of the static section.
func quarantinedKeys(sg subgraph.Subgraph) []subgraph.Key {
	var out []subgraph.Key
	for _, q := range sg.Quarantine.Entities {
		out = append(out, ent(q.Fingerprint))
	}
	for _, e := range sg.Quarantine.Edges {
		out = append(out, e.Key)
	}
	slices.SortFunc(out, subgraph.Key.Compare)
	return out
}
