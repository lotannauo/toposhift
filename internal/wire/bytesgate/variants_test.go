package bytesgate_test

import (
	"math"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/wire"
	"github.com/lotannauo/toposhift/internal/wire/bytesgate"
)

// buildVariant builds the fixtures a variant applies to.
func buildVariant(t testing.TB, v bytesgate.Variant) map[string]wire.Document {
	t.Helper()
	out := map[string]wire.Document{}
	for _, f := range bytesgate.Fixtures(v.Scale) {
		for _, name := range v.Fixtures {
			if f.Name != name {
				continue
			}
			d, err := f.Build(ctx)
			if err != nil {
				t.Fatalf("variant %s, fixture %s: %v", v.Name, f.Name, err)
			}
			out[f.Name] = d
		}
	}
	if len(out) != len(v.Fixtures) {
		t.Fatalf("variant %s builds %d of its %d fixtures", v.Name, len(out), len(v.Fixtures))
	}
	return out
}

func variantNamed(t testing.TB, name string) bytesgate.Variant {
	t.Helper()
	return variantAt(t, small(), name)
}

func variantAt(t testing.TB, s bytesgate.Scale, name string) bytesgate.Variant {
	t.Helper()
	for _, v := range bytesgate.Variants(s) {
		if v.Name == name {
			return v
		}
	}
	t.Fatalf("no variant %s", name)
	return bytesgate.Variant{}
}

func TestFourVariantsOfTheRegisteredFixtures(t *testing.T) {
	vs := bytesgate.Variants(bytesgate.Reference())
	var names, fixtures []string
	for _, v := range vs {
		names = append(names, v.Name)
		fixtures = append(fixtures, strings.Join(v.Fixtures, ""))
	}
	if strings.Join(names, " ") != "i ii iii iv" || strings.Join(fixtures, " ") != "abc bc de de" {
		t.Errorf("variants %v on fixtures %v", names, fixtures)
	}
	ref := bytesgate.Reference()
	if got := vs[0].Scale.ReplaceEvery; got.Seconds() != 300 {
		t.Errorf("variant i replaces a pod every %s", got)
	}
	if vs[1].Scale.FillerServices != 23 || !vs[2].Scale.Containers || !vs[3].Scale.StableScopes {
		t.Errorf("scales %+v", vs)
	}
	// A variant changes one thing: the registered scale is the variant's with that
	// thing put back.
	for i, v := range vs {
		back := v.Scale
		switch i {
		case 0:
			back.ReplaceEvery = ref.ReplaceEvery
		case 1:
			back.FillerServices = 0
		case 2:
			back.Containers = false
		case 3:
			back.StableScopes = false
		}
		if back != ref {
			t.Errorf("variant %s changes more than one thing: %+v", v.Name, v.Scale)
		}
	}
}

// Every variant builds, and its document is one the format accepts and that
// rebuilds from its Arrow streams.
func TestVariantsBuildAndRebuildFromArrow(t *testing.T) {
	for _, v := range bytesgate.Variants(small()) {
		for name, d := range buildVariant(t, v) {
			for mode, write := range encoders {
				b, err := write(d)
				if err != nil {
					t.Fatalf("variant %s, fixture %s, %s: %v", v.Name, name, mode, err)
				}
				requireSame(t, "variant "+v.Name+" fixture "+name+" "+mode, d, rebuild(t, b))
			}
		}
	}
}

// Each variant changes what it says it changes, and the registered fixtures are
// the ones the other tests build.
func TestVariantsChangeWhatTheySay(t *testing.T) {
	reg := build(t, small())

	// i: fewer replacements, so fewer deltas.
	for name, d := range buildVariant(t, variantNamed(t, "i")) {
		if len(d.Subgraph.Deltas) > len(reg[name].Subgraph.Deltas) {
			t.Errorf("i, fixture %s: %d deltas, the registered fixture has %d", name, len(d.Subgraph.Deltas), len(reg[name].Subgraph.Deltas))
		}
		if name == "a" && len(d.Subgraph.Deltas) >= len(reg[name].Subgraph.Deltas) {
			t.Errorf("i, fixture a: %d deltas, the registered fixture has %d", len(d.Subgraph.Deltas), len(reg[name].Subgraph.Deltas))
		}
	}

	// The next two are checked on a scale with more pods on a node than the
	// variants have services, and more pods in a scope: at the small scale a node
	// has fewer pods than 23, and a scope too few for the proportions.
	wide := small()
	wide.Pods, wide.SmallScope, wide.LargeScope = 30, 400, 400
	wideReg := build(t, wide)

	// ii: every pod of a node is mapped, to one of 23 services, so none is left
	// unmapped.
	for name, d := range buildVariant(t, variantAt(t, wide, "ii")) {
		before, after := len(wideReg[name].Subgraph.Sections.Unmapped), len(d.Subgraph.Sections.Unmapped)
		if before == 0 || after != 0 {
			t.Errorf("ii, fixture %s: %d unmapped pods, the registered fixture has %d", name, after, before)
		}
		services := func(d wire.Document) (n int) {
			for _, e := range d.Subgraph.Entities {
				if e.Fingerprint.Type() == catalog.Service {
					n++
				}
			}
			return n
		}
		if name == "b" && (services(d) != 23 || services(wideReg[name]) != 7) {
			t.Errorf("ii, fixture b: %d services, the registered fixture has %d", services(d), services(wideReg[name]))
		}
	}

	// iii: each pod has 1, 2 or 3 containers, each with its part_of edge.
	for name, d := range buildVariant(t, variantAt(t, wide, "iii")) {
		reg := wideReg
		pods := len(reg[name].Query.Seeds)
		perPod := map[string]int{}
		for _, el := range d.Subgraph.Elements {
			if el.Key.Kind == subgraph.KindEdge && el.Key.Relation == catalog.PartOf {
				if el.Key.A.Type() != catalog.Container || el.Key.B.Type() != catalog.K8sPod {
					t.Fatalf("iii, fixture %s: the edge %s", name, el.Key)
				}
				perPod[el.Key.B.String()]++
			}
		}
		if len(perPod) != pods {
			t.Errorf("iii, fixture %s: %d pods have containers, of %d", name, len(perPod), pods)
		}
		counts := [4]int{}
		total := 0
		for _, n := range perPod {
			if n < 1 || n > 3 {
				t.Fatalf("iii, fixture %s: a pod with %d containers", name, n)
			}
			counts[n]++
			total += n
		}
		// 69.7, 24.2 and 6.1 in 100, over 200 pods: loose bounds on a fixed draw.
		if p := func(n int) float64 { return float64(counts[n]) / float64(pods) }; p(1) < 0.55 || p(1) > 0.85 || p(2) < 0.15 || p(2) > 0.35 || p(3) < 0.02 || p(3) > 0.12 {
			t.Errorf("iii, fixture %s: pods with 1, 2 and 3 containers: %v of %d", name, counts[1:], pods)
		}
		if want := len(reg[name].Subgraph.Elements) + total; len(d.Subgraph.Elements) != want {
			t.Errorf("iii, fixture %s: %d elements, want the registered %d and %d edges", name, len(d.Subgraph.Elements), len(reg[name].Subgraph.Elements), total)
		}
	}

	// iv: nothing changes inside the window.
	for name, d := range buildVariant(t, variantNamed(t, "iv")) {
		sg := d.Subgraph
		if len(sg.Deltas) != 0 || len(sg.Keyframes) != 1 || len(sg.Elements) != len(reg[name].Subgraph.Elements) {
			t.Errorf("iv, fixture %s: %d deltas, %d keyframes, %d elements", name, len(sg.Deltas), len(sg.Keyframes), len(sg.Elements))
		}
		for _, el := range sg.Elements {
			if el.Class != subgraph.ClassStable {
				t.Fatalf("iv, fixture %s: %s is %s", name, el.Key, el.Class)
			}
		}
	}
}

// A variant is measured beside its registered fixture, and the registered
// report, its geometric mean and its largest ratio are the same with the
// variants run or not. Parallel with the small-fixtures test (see there).
func TestRunVariantsBesideTheRegisteredFixtures(t *testing.T) {
	t.Parallel()
	rep, err := bytesgate.Run(ctx, small())
	if err != nil {
		t.Fatal(err)
	}
	mean, big := rep.GeoMean(), rep.Rows
	rows, err := bytesgate.RunVariants(ctx, small(), rep)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3+2+2+2 {
		t.Fatalf("%d variant rows", len(rows))
	}
	for _, vr := range rows {
		if vr.Row.Name != vr.Registered.Name || vr.Registered != rep.Rows[int(vr.Row.Name[0]-'a')] {
			t.Errorf("variant %s, fixture %s: registered row %s", vr.Variant, vr.Row.Name, vr.Registered.Name)
		}
		if r := vr.Row.Ratio(); math.IsNaN(r) || r <= 0 {
			t.Errorf("variant %s, fixture %s: ratio %v", vr.Variant, vr.Row.Name, r)
		}
	}
	if rep.GeoMean() != mean || len(rep.Rows) != len(big) {
		t.Error("running the variants changed the registered report")
	}
	var table strings.Builder
	if err := rep.WriteTable(&table); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(table.String(), "sensitivity") || strings.Contains(table.String(), "variant") {
		t.Errorf("the registered table says something of the variants:\n%s", table.String())
	}
}

// The block of variants is labelled as informational, shows each variant's r with
// the registered fixture's, and says nothing about what the numbers mean or what
// they come to over the variants.
func TestVariantBlockStatesNoVerdict(t *testing.T) {
	a, b := row("a", 100, 150), row("b", 100, 100)
	rows := []bytesgate.VariantRow{{Variant: "i", Row: a, Registered: b}, {Variant: "iv", Row: b, Registered: a}}
	var out strings.Builder
	if err := bytesgate.WriteVariants(&out, small(), rows); err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(out.String())
	if !strings.Contains(text, "sensitivity variants (informational)") {
		t.Errorf("the block is not labelled:\n%s", out.String())
	}
	for _, want := range []string{"1.5000", "1.0000", "r registered"} {
		if !strings.Contains(text, want) {
			t.Errorf("the block lacks %q:\n%s", want, out.String())
		}
	}
	for _, word := range []string{
		"pass", "fail", "threshold", "within", "exceed", "accept", "reject", "limit", " ok", "t1", "t2",
		"mean", "largest", "geometric", "verdict", "worse", "better", "wins",
	} {
		if strings.Contains(text, word) {
			t.Errorf("the block says %q:\n%s", word, out.String())
		}
	}
}

// In variant ii the seven services of the cell keep their one pod on each node,
// and the services they depend on and are depended on by stay three and three:
// the filler pods are mapped to the other sixteen services and to none of the
// seven, so the variant changes one thing.
func TestVariantIIKeepsTheMembersOnePodOnEachNode(t *testing.T) {
	wide := small()
	wide.Pods, wide.SmallScope, wide.LargeScope = 30, 400, 400
	wideReg := build(t, wide)
	for name, d := range buildVariant(t, variantAt(t, wide, "ii")) {
		// The seven services of the cell keep their one pod on each node: the
		// filler pods are mapped to the other sixteen and to none of them.
		nodes := 1
		if name == "c" {
			nodes = wide.Hosts
		}
		for label, doc := range map[string]wire.Document{"ii": d, "registered": wideReg[name]} {
			members := map[string]bool{doc.Query.Seeds[0].String(): true}
			for _, el := range doc.Subgraph.Elements {
				if el.Key.Kind == subgraph.KindEdge && el.Key.Relation == catalog.DependsOn {
					members[el.Key.A.String()], members[el.Key.B.String()] = true, true
				}
			}
			perMember := map[string]int{}
			for _, el := range doc.Subgraph.Elements {
				if el.Key.Kind == subgraph.KindEdge && el.Key.Relation == catalog.InstanceOf && members[el.Key.B.String()] && el.Pieces[0].Before {
					perMember[el.Key.B.String()]++
				}
			}
			if len(members) != 7 || len(perMember) != 7 {
				t.Fatalf("%s, fixture %s: %d members, %d with instances", label, name, len(members), len(perMember))
			}
			for svc, n := range perMember {
				if n != nodes {
					t.Errorf("%s, fixture %s: the member %s has %d pods at From over %d nodes, want one on each", label, name, svc, n, nodes)
				}
			}
		}
	}
}
