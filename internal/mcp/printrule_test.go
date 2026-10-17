package mcp

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// The print rule: a name or a namespace (and a service instance's version, in a
// description payload) that is over 253 bytes, holds a control character, holds a
// format character (Unicode Cf) or holds an endpoint id's text is not printed,
// wherever MCP prints it.

const (
	hexLower = "0123456789abcdef0123456789abcdef"
	hexUpper = "0123456789ABCDEF0123456789ABCDEF"
	hexMixed = "0123456789aBcDeF0123456789AbCdEf"
)

// printCase is a value for a name or a namespace and whether the rule withholds
// it. build makes the value from a tag that is unique to the entity, so that a
// search for the tag finds that entity only, and the value has exactly the length
// and the content the case is about.
type printCase struct {
	label    string
	build    func(tag string) string
	withheld bool
	// bytes, if not zero, is the length in bytes the value must have.
	bytes int
}

// padTo is the tag followed by x up to n bytes.
func padTo(tag string, n int) string { return tag + strings.Repeat("x", n-len(tag)) }

func printCases() []printCase {
	around := func(mid string) func(string) string {
		return func(tag string) string { return tag + "a" + mid + "_z" }
	}
	twoByte := func(total int) func(string) string {
		return func(tag string) string {
			rest := total - len(tag)
			return tag + strings.Repeat("é", rest/2) + strings.Repeat("x", rest%2)
		}
	}
	return []printCase{
		{"an ordinary value", func(tag string) string { return tag + "checkout" }, false, 0},
		{"exactly 253 bytes", func(tag string) string { return padTo(tag, 253) }, false, 253},
		{"254 bytes", func(tag string) string { return padTo(tag, 254) }, true, 254},
		{"4096 bytes", func(tag string) string { return padTo(tag, 4096) }, true, 4096},
		{"exactly 253 bytes, mostly two-byte runes", twoByte(253), false, 253},
		{"254 bytes, mostly two-byte runes", twoByte(254), true, 254},
		{"over 253 bytes by bytes, under it by runes", func(tag string) string { return tag + strings.Repeat("é", 200) }, true, 0},
		{"over 253 bytes in three-byte runes, 85 runes", func(tag string) string { return tag + strings.Repeat("世", 84) }, true, 0},
		{"a space", around(" "), false, 0},
		{"a no-break space", around("\u00a0"), false, 0},
		{"a zero-width space, U+200B (format)", around("\u200b"), true, 0},
		{"a right-to-left override, U+202E (format)", around("\u202e"), true, 0},
		{"a left-to-right isolate, U+2066 (format)", around("\u2066"), true, 0},
		{"a tag letter A, U+E0041 (format)", around("\U000e0041"), true, 0},
		{"a soft hyphen, U+00AD (format)", around("\u00ad"), true, 0},
		{"a zero-width joiner, U+200D (format)", around("\u200d"), true, 0},
		{"a tab", around("\t"), true, 0},
		{"a newline", around("\n"), true, 0},
		{"a carriage return", around("\r"), true, 0},
		{"a null", around("\x00"), true, 0},
		{"a delete", around("\x7f"), true, 0},
		{"an escape", around("\x1b"), true, 0},
		{"a next line, U+0085", around("\u0085"), true, 0},
		{"a line separator, U+2028", around("\u2028"), true, 0},
		{"a paragraph separator, U+2029", around("\u2029"), true, 0},
		{"an endpoint id in lower case", around("topo.endpoint:" + hexLower), true, 0},
		{"an endpoint id in upper case", around("topo.endpoint:" + hexUpper), true, 0},
		{"an endpoint id in mixed case", around("topo.endpoint:" + hexMixed), true, 0},
		{"an endpoint id and more digits", around("topo.endpoint:" + hexLower + "00"), true, 0},
		{"only the endpoint id", func(string) string { return "topo.endpoint:" + hexLower }, true, 0},
		{"an endpoint prefix and 31 digits", around("topo.endpoint:" + hexLower[:31]), false, 0},
		{"an endpoint prefix and a non-hex digit", around("topo.endpoint:" + hexLower[:31] + "g"), false, 0},
		{"the digits of an endpoint id with no prefix", around(hexLower), false, 0},
		{"the id of another type", around("k8s.pod:" + hexLower), false, 0},
	}
}

func TestPrintable(t *testing.T) {
	for i, c := range printCases() {
		v := c.build(fmt.Sprintf("t%02d-", i))
		if c.bytes != 0 && len(v) != c.bytes {
			t.Fatalf("%s: the test value has %d bytes, not %d", c.label, len(v), c.bytes)
		}
		if got := printable(v); got == c.withheld {
			t.Errorf("%s: printable is %t, want %t", c.label, got, !c.withheld)
		}
	}
	// The length counts bytes, and the bound is 253 exactly.
	for n, want := range map[int]bool{0: true, 1: true, 252: true, 253: true, 254: false, 255: false, 1000: false} {
		if got := printable(strings.Repeat("a", n)); got != want {
			t.Errorf("%d bytes: printable %t, want %t", n, got, want)
		}
	}
	if v := strings.Repeat("é", 200); utf8.RuneCountInString(v) > 253 || printable(v) {
		t.Errorf("200 two-byte runes: %d runes, printable %t; the bytes count, not the runes", utf8.RuneCountInString(v), printable(v))
	}
	if v := strings.Repeat("é", 126) + "a"; len(v) != 253 || !printable(v) {
		t.Errorf("a multi-byte value of 253 bytes: %d bytes, printable %t", len(v), printable(v))
	}
}

// A type the print rule reaches, with the keys of a description that names it.
type printType struct {
	typ      catalog.EntityType
	producer lifecycle.Producer
	id       func(testing.TB, string) identity.Fingerprint
	payload  func(name, ns string) string
	hasNS    bool
}

func jsonObject(kv ...string) string {
	m := map[string]string{}
	for i := 0; i < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	b, _ := json.Marshal(m)
	return string(b)
}

func printTypes() []printType {
	return []printType{
		{
			catalog.Service, "svcgraph/c1",
			func(tb testing.TB, uid string) identity.Fingerprint {
				return fpOf(tb, catalog.Service, catalog.ServiceName, uid)
			},
			func(name, ns string) string { return jsonObject("name", name, "namespace", ns) }, true,
		},
		{
			catalog.K8sPod, objs,
			func(tb testing.TB, uid string) identity.Fingerprint {
				return fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, uid)
			},
			func(name, ns string) string { return jsonObject("name", name, "namespace", ns) }, true,
		},
		{
			catalog.K8sNode, objs,
			func(tb testing.TB, uid string) identity.Fingerprint {
				return fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, uid)
			},
			func(name, ns string) string { return jsonObject("name", name) }, false,
		},
		{
			catalog.Container, clus,
			func(tb testing.TB, uid string) identity.Fingerprint {
				return fpOf(tb, catalog.Container, catalog.ContainerID, uid)
			},
			func(name, ns string) string { return jsonObject("k8s.container.name", name, "k8s.namespace.name", ns) }, true,
		},
	}
}

// A printed entity: the value under test is in its name (field "name") or its
// namespace (field "namespace"), and the other field is an ordinary value.
type printEntity struct {
	fp       identity.Fingerprint
	field    string
	tag      string
	value    string
	withheld bool
	label    string
}

// writePrintWorld writes, for the type, one entity per case and field.
func writePrintWorld(tb testing.TB, st store.Store, pt printType) []printEntity {
	tb.Helper()
	f := newFeed(tb, st)
	var out []printEntity
	var recs []store.Record
	for i, c := range printCases() {
		for _, field := range []string{"name", "namespace"} {
			if field == "namespace" && !pt.hasNS {
				continue
			}
			tag := fmt.Sprintf("t%02d%c-", i, map[string]byte{"name": 'n', "namespace": 's'}[field])
			value := c.build(tag)
			name, ns := tag+"nm", tag+"ns"
			if field == "name" {
				name = value
			} else {
				ns = value
			}
			fp := pt.id(tb, fmt.Sprintf("%s-%02d-%s", pt.typ, i, field))
			recs = append(recs, observe(ent(fp), pt.producer, hm(9, 0), 0, pt.payload(name, ns)))
			out = append(out, printEntity{fp: fp, field: field, tag: tag, value: value, withheld: c.withheld, label: c.label})
		}
	}
	f.write(recs...)
	return out
}

func (e printEntity) String() string { return fmt.Sprintf("%s (%s)", e.label, e.field) }

// escaped is what the text of v becomes inside a JSON string.
func escaped(v string) string {
	b, _ := json.Marshal(v)
	return string(b[1 : len(b)-1])
}

// leaks says an answer holds the text of a value that was withheld, as it is or
// as JSON writes it.
func leaks(b []byte, e printEntity) bool {
	s := string(b)
	return strings.Contains(s, e.value) || strings.Contains(s, escaped(e.value))
}

// wantRef checks a ref of an entity: a withheld value is null with the fixed note,
// a kept value is as given, and the other field is untouched.
func wantRef(t *testing.T, pt printType, e printEntity, r Ref) {
	t.Helper()
	value, other := r.Name, r.Namespace
	wantNote, otherWant := nameWithheld, e.tag+"ns"
	if e.field == "namespace" {
		value, other = r.Namespace, r.Name
		wantNote, otherWant = nsWithheld, e.tag+"nm"
	}
	switch {
	case e.withheld && (value != nil || r.NameNote != wantNote):
		t.Errorf("%s %s: the value is %s with the note %q; want null and %q", pt.typ, e, str(value), r.NameNote, wantNote)
	case !e.withheld && (value == nil || *value != e.value || r.NameNote != ""):
		t.Errorf("%s %s: the value is %s with the note %q; want it printed", pt.typ, e, str(value), r.NameNote)
	}
	if pt.hasNS && (other == nil || *other != otherWant) {
		t.Errorf("%s %s: the field beside the value is %s, want %s", pt.typ, e, str(other), otherWant)
	}
	if e.withheld && e.field == "name" && r.NameFrom != "" {
		t.Errorf("%s %s: name_from %q beside a withheld name", pt.typ, e, r.NameFrom)
	}
}

// find_entities lists a withheld value as null with a fixed note, never prints its
// text, and never matches it by name or by namespace, which it counts. A name
// search cannot tell what a withheld name says, so it counts every entity of the
// type whose name is withheld, as a search counts every undescribed one.
func TestFindEntitiesAppliesThePrintRule(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			type world struct {
				pt printType
				es []printEntity
			}
			var worlds []world
			for _, pt := range printTypes() {
				worlds = append(worlds, world{pt, writePrintWorld(t, st, pt)})
			}
			s := sourceOn(t, st, nil, nil)
			for _, w := range worlds {
				pt, es := w.pt, w.es
				t.Run(string(pt.typ), func(t *testing.T) {
					in := FindEntitiesInput{Type: string(pt.typ), At: w1000, Limit: 200}
					withheldNames, withheldNamespaces := 0, 0
					for _, e := range es {
						if e.withheld && e.field == "name" {
							withheldNames++
						}
						if e.withheld && e.field == "namespace" {
							withheldNamespaces++
						}
					}
					if withheldNames < 10 {
						t.Fatalf("only %d withheld names; the test lost its input", withheldNames)
					}
					countNote := func(n int) string {
						return fmt.Sprintf("%d %s entities have a name or namespace that is not printed (over 253 bytes, or it holds a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text), so they could not be matched.", n, pt.typ)
					}

					// Every entity is listed, with its value printed or withheld.
					all := found(t, s, in)
					b, _ := json.Marshal(all)
					byID := map[string]Ref{}
					for _, m := range all.Matches {
						byID[m.Entity.ID] = m.Entity
					}
					if len(all.Matches) != len(es) || !all.Complete {
						t.Fatalf("%d matches of %d entities, complete %t", len(all.Matches), len(es), all.Complete)
					}
					for _, e := range es {
						wantRef(t, pt, e, byID[e.fp.String()])
						if e.withheld && leaks(b, e) {
							t.Errorf("%s: the answer holds the text of the withheld value", e)
						}
					}
					if all.Counts.NotPrinted != 0 {
						t.Errorf("an unfiltered list counts %d not printed; nothing was searched for", all.Counts.NotPrinted)
					}

					for _, e := range es {
						// A search by the tag, which the printed field holds, finds the entity if
						// that field is printed.
						by := in
						by.Name = e.tag
						got := found(t, s, by)
						wantMatch := 1
						if e.withheld && e.field == "name" {
							wantMatch = 0
						}
						if len(got.Matches) != wantMatch || got.Counts.NotPrinted != withheldNames {
							t.Errorf("%s: search by the tag matched %d and counted %d not printed; want %d and %d", e, len(got.Matches), got.Counts.NotPrinted, wantMatch, withheldNames)
						}
						if wantMatch == 1 {
							wantRef(t, pt, e, got.Matches[0].Entity)
						}
						if !contains(got.Notes, countNote(withheldNames)) {
							t.Errorf("%s: notes %q lack the count of the withheld names", e, got.Notes)
						}
						if gb, _ := json.Marshal(got); e.withheld && leaks(gb, e) {
							t.Errorf("%s: the answer to a search holds the withheld text", e)
						}

						// A search by the whole value never matches a withheld one. An argument is
						// at most 256 bytes of UTF-8.
						if len(e.value) > findMaxText {
							continue
						}
						by = in
						wantWithheld := withheldNames
						if e.field == "name" {
							by.Name = e.value
						} else {
							by.Namespace = e.value
							wantWithheld = withheldNamespaces
						}
						got = found(t, s, by)
						want := 1
						if e.withheld {
							want = 0
						}
						if len(got.Matches) != want || got.Counts.NotPrinted != wantWithheld {
							t.Errorf("%s: search by the whole value matched %d and counted %d not printed; want %d and %d", e, len(got.Matches), got.Counts.NotPrinted, want, wantWithheld)
						}
					}
				})
			}
		})
	}
}

// A search by name or namespace counts a withheld value as withheld only where the
// value decided the answer, and combines the two filters.
func TestFindEntitiesFiltersAndWithheldValues(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			bad := "line\nbreak"
			svc := func(uid string) identity.Fingerprint { return fpOf(t, catalog.Service, catalog.ServiceName, uid) }
			okName, badName, badNS := svc("ok-name"), svc("bad-name"), svc("bad-ns")
			f.write(
				observe(ent(okName), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "checkout", "namespace", "shop")),
				observe(ent(badName), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "check"+bad, "namespace", "shop")),
				observe(ent(badNS), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "checkin", "namespace", "shop"+bad)),
			)
			s := sourceOn(t, st, nil, nil)
			in := FindEntitiesInput{Type: "service", At: w1000}
			for _, tc := range []struct {
				label        string
				name, ns     string
				wantNames    []string
				wantWithheld int
			}{
				{"the name filter skips a withheld name and counts it", "check", "", []string{"checkin", "checkout"}, 1},
				{"the namespace filter skips a withheld namespace and counts it", "", "shop", []string{"checkout", "-"}, 1},
				{"the withheld namespace itself never matches", "", "shop" + bad, nil, 1},
				{"both filters: a withheld name and a withheld namespace both count", "check", "shop", []string{"checkout"}, 2},
				{"both filters: a withheld namespace, a name that matches", "checkin", "shop", nil, 2},
				{"both filters: a name that does not match decides, not the withheld namespace", "nomatch", "shop" + bad, nil, 0},
				{"both filters: a namespace that does not match decides, not the withheld name", "check" + bad, "elsewhere", nil, 0},
			} {
				by := in
				by.Name, by.Namespace = tc.name, tc.ns
				got := found(t, s, by)
				names := foundNames(got)
				if tc.wantNames == nil {
					tc.wantNames = []string{}
				}
				slices.Sort(names)
				slices.Sort(tc.wantNames)
				if !slices.Equal(names, tc.wantNames) || got.Counts.NotPrinted != tc.wantWithheld {
					t.Errorf("%s: matched %v and counted %d not printed; want %v and %d", tc.label, names, got.Counts.NotPrinted, tc.wantNames, tc.wantWithheld)
				}
			}
		})
	}
}

// Every other tool fills the names of the entities it lists through the same rule.
func TestEveryToolPrintsAWithheldNameAsNull(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			svc := func(uid string) identity.Fingerprint { return fpOf(t, catalog.Service, catalog.ServiceName, uid) }
			caller, callee, shown := svc("caller"), svc("callee"), svc("shown")
			hexName := "svc-topo.endpoint:" + hexUpper
			ns := fpOf(t, catalog.K8sNode, catalog.K8sNodeUID, "node-1")
			pod1 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-1")
			pod2 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-2")
			longNS := strings.Repeat("n", 254)
			f.write(
				observe(ent(caller), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "caller\tone", "namespace", "shop")),
				observe(ent(callee), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", hexName, "namespace", longNS)),
				observe(ent(shown), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "shown", "namespace", "shop")),
				observe(edge(caller, callee, catalog.DependsOn), "svcgraph/c1", hm(9, 0), 0, `{}`),
				observe(edge(shown, caller, catalog.DependsOn), "svcgraph/c1", hm(9, 0), 0, `{}`),

				observe(ent(ns), objs, hm(9, 0), 0, jsonObject("name", "node-1")),
				observe(ent(pod1), objs, hm(9, 0), 0, jsonObject("name", "pod-one", "namespace", "shop")),
				observe(ent(pod2), objs, hm(9, 0), 0, jsonObject("name", "pod two", "namespace", "shop")),
				observe(edge(pod1, ns, catalog.ScheduledOn), objs, hm(9, 0), 0, `{}`),
				observe(edge(pod2, ns, catalog.ScheduledOn), objs, hm(9, 0), 0, `{}`),
			)
			s := sourceOn(t, st, nil, nil)

			// wantNull checks every ref of an answer that is one of the withheld entities.
			wantNull := map[identity.Fingerprint]struct {
				name, ns bool
				note     string
			}{
				caller: {name: true, note: nameWithheld},
				callee: {name: true, ns: true, note: nameNsWithheld},
				pod2:   {name: true, note: nameWithheld},
			}
			secrets := []string{"caller\tone", `caller\tone`, "topo.endpoint:" + hexUpper, longNS, "pod two", `pod two`}
			check := func(tool string, out any) {
				t.Helper()
				n := 0
				eachRef(reflect.ValueOf(&out).Elem(), func(r *Ref) {
					if w, ok := wantNull[r.FP]; ok {
						n++
						if (r.Name == nil) != w.name || (r.Namespace == nil) != w.ns || r.NameNote != w.note {
							t.Errorf("%s: %s has name %s, namespace %s, note %q; want name withheld %t, namespace withheld %t, note %q",
								tool, r.ID, str(r.Name), str(r.Namespace), r.NameNote, w.name, w.ns, w.note)
						}
					}
				})
				if n == 0 {
					t.Errorf("%s: no withheld entity in the answer; the test checks nothing", tool)
				}
				b, _ := json.Marshal(out)
				for _, secret := range secrets {
					if strings.Contains(string(b), secret) {
						t.Errorf("%s: the answer holds %q", tool, secret)
					}
				}
			}
			at := "2026-01-01T10:00:00Z"
			nb, err := s.NeighborsAt(ctx, NeighborsAtInput{Entity: caller.String(), At: at})
			if err != nil {
				t.Fatal(err)
			}
			check("neighbors_at", &nb)
			if nb.Entity.NameNote != nameWithheld {
				t.Errorf("neighbors_at: the entity asked for is not withheld: %+v", nb.Entity)
			}
			br, err := s.BlastRadius(ctx, BlastRadiusInput{Entity: callee.String(), From: w10, To: w1030})
			if err != nil {
				t.Fatal(err)
			}
			check("blast_radius", &br)
			wc, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{caller.String(), callee.String(), shown.String()}, From: w10, To: w1030})
			if err != nil {
				t.Fatal(err)
			}
			check("what_changed", &wc)
			co, err := s.CoResidents(ctx, CoResidentsInput{Pod: pod1.String(), From: w10, To: w1030})
			if err != nil {
				t.Fatal(err)
			}
			check("co_residents", &co)
			eh, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: callee.String(), From: "2026-01-01T09:00:00Z", To: w1030})
			if err != nil {
				t.Fatalf("entity_history of a service whose name holds an endpoint id's text: %v", err)
			}
			check("entity_history", &eh)
			// The printed ones stay printed.
			printed := map[identity.Fingerprint]string{shown: "shown", pod1: "pod-one"}
			for _, r := range append(append([]Ref{}, wc.Scope...), refsOf(&co)...) {
				if want, ok := printed[r.FP]; ok && (r.Name == nil || *r.Name != want || r.NameNote != "") {
					t.Errorf("%s lost its name %q: %+v", r.ID, want, r)
				}
			}
		})
	}
}

func refsOf(out any) []Ref {
	var refs []Ref
	eachRef(reflect.ValueOf(out), func(r *Ref) { refs = append(refs, *r) })
	return refs
}

// resolve_entity names the entity it resolves from its description through the same
// rule.
func TestResolveEntityPrintsAWithheldNameAsNull(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			fp := fpOf(t, catalog.Service, catalog.ServiceName, "resolved")
			f.write(observe(ent(fp), "svcgraph/c1", hm(9, 0), 0, jsonObject("name", "bad\nname", "namespace", "shop")))
			s := sourceOn(t, st, nil, nil)
			out, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "service", Attributes: rawAttrs(`{"service.name":"resolved"}`), At: "2026-01-01T10:00:00Z"})
			if err != nil {
				t.Fatal(err)
			}
			if out.Entity.FP != fp || out.Entity.Name != nil || out.Entity.NameNote != nameWithheld || out.Entity.Namespace == nil || *out.Entity.Namespace != "shop" {
				t.Errorf("%+v", out.Entity)
			}
		})
	}
}

// entity_history prints a description and the stored records through the payload
// table: a name or a namespace value the rule withholds is null, its key is named
// among the withheld keys, and a fixed note says why.
func TestEntityHistoryAppliesThePrintRule(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			var worlds []struct {
				pt printType
				es []printEntity
			}
			types := printTypes()
			for _, pt := range types {
				worlds = append(worlds, struct {
					pt printType
					es []printEntity
				}{pt, writePrintWorld(t, st, pt)})
			}
			s := sourceOn(t, st, nil, nil)
			for _, w := range worlds {
				pt := w.pt
				nameKey, nsKey := "name", "namespace"
				if pt.typ == catalog.Container {
					nameKey, nsKey = "k8s.container.name", "k8s.namespace.name"
				}
				for _, e := range w.es {
					out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: e.fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
					if err != nil {
						t.Fatalf("%s %s: %v", pt.typ, e, err)
					}
					b, _ := json.Marshal(out)
					wantRef(t, pt, e, out.Entity)
					key := nameKey
					if e.field == "namespace" {
						key = nsKey
					}
					check := func(where string, payload map[string]any, withheldKeys []string, note string) {
						t.Helper()
						v, present := payload[key]
						switch {
						case e.withheld && (!present || v != nil):
							t.Errorf("%s %s %s: %q is %v (present %t); want null", pt.typ, e, where, key, v, present)
						case e.withheld && (!slices.Contains(withheldKeys, key) || note != valueWithheld):
							t.Errorf("%s %s %s: withheld keys %v, note %q; want %q named and %q", pt.typ, e, where, withheldKeys, note, key, valueWithheld)
						case !e.withheld && (v != e.value || len(withheldKeys) != 0 || note != ""):
							t.Errorf("%s %s %s: %q is %v, withheld keys %v, note %q; want the value printed", pt.typ, e, where, key, v, withheldKeys, note)
						}
						// The other key of the payload is untouched.
						otherKey, otherWant := nsKey, e.tag+"ns"
						if e.field == "namespace" {
							otherKey, otherWant = nameKey, e.tag+"nm"
						}
						if pt.hasNS && payload[otherKey] != otherWant {
							t.Errorf("%s %s %s: %q is %v, want %q", pt.typ, e, where, otherKey, payload[otherKey], otherWant)
						}
					}
					d := out.Existence.DescriptionAtEnd
					if d == nil {
						t.Fatalf("%s %s: no description at the end", pt.typ, e)
					}
					check("description_at_end", d.Payload, d.WithheldKeys, d.Note)
					if d := out.Existence.DescriptionAtStart; d != nil {
						check("description_at_start", d.Payload, d.WithheldKeys, d.Note)
					}
					if len(out.Records) != 1 {
						t.Fatalf("%s %s: %d records", pt.typ, e, len(out.Records))
					}
					r := out.Records[0]
					check("record", r.Payload, r.WithheldKeys, r.PayloadNote)
					if e.withheld && leaks(b, e) {
						t.Errorf("%s %s: the answer holds the text of the withheld value", pt.typ, e)
					}
				}
			}
		})
	}
}

// A service instance's version is text the sender chose, and entity_history prints
// it through the same rule: a version the rule withholds is null, its key is named
// among the withheld keys, and the fixed note says why. The other keys of the
// instance are not listed, so they are withheld by the table, not the rule.
func TestEntityHistoryAppliesThePrintRuleToAVersion(t *testing.T) {
	for sname, st := range listStores(t, rankObjs) {
		t.Run(sname, func(t *testing.T) {
			f := newFeed(t, st)
			type versioned struct {
				fp       identity.Fingerprint
				label    string
				value    string
				withheld bool
			}
			var es []versioned
			var recs []store.Record
			for i, c := range printCases() {
				value := c.build(fmt.Sprintf("v%02d-", i))
				fp := fpOf(t, catalog.ServiceInstance, catalog.ServiceName, "cart", catalog.ServiceInstanceID, fmt.Sprintf("i-%02d", i))
				recs = append(recs, observe(ent(fp), "spancount/c1", hm(9, 0), 0, jsonObject("service.version", value)))
				es = append(es, versioned{fp, c.label, value, c.withheld})
			}
			f.write(recs...)
			s := sourceOn(t, st, nil, nil)
			for _, e := range es {
				out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: e.fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
				if err != nil {
					t.Fatalf("%s: %v", e.label, err)
				}
				d := out.Existence.DescriptionAtEnd
				if d == nil || len(out.Records) != 1 {
					t.Fatalf("%s: description %v, %d records", e.label, d, len(out.Records))
				}
				r := out.Records[0]
				for where, got := range map[string]struct {
					payload map[string]any
					keys    []string
					note    string
				}{
					"description_at_end": {d.Payload, d.WithheldKeys, d.Note},
					"record":             {r.Payload, r.WithheldKeys, r.PayloadNote},
				} {
					v, present := got.payload["service.version"]
					switch {
					case e.withheld && (!present || v != nil):
						t.Errorf("%s %s: service.version is %v (present %t); want null", e.label, where, v, present)
					case e.withheld && (!slices.Equal(got.keys, []string{"service.version"}) || got.note != valueWithheld):
						t.Errorf("%s %s: withheld keys %v, note %q; want service.version named and %q", e.label, where, got.keys, got.note, valueWithheld)
					case !e.withheld && (v != e.value || len(got.keys) != 0 || got.note != ""):
						t.Errorf("%s %s: service.version is %v, withheld keys %v, note %q; want the value printed", e.label, where, v, got.keys, got.note)
					}
				}
				if b, _ := json.Marshal(out); e.withheld && (strings.Contains(string(b), e.value) || strings.Contains(string(b), escaped(e.value))) {
					t.Errorf("%s: the answer holds the text of the withheld version", e.label)
				}
			}
		})
	}
}

// The rule reaches the name keys of every type, and the sender-text keys, and only
// them.
func TestPayloadPrintRuleOnEveryNameKey(t *testing.T) {
	bad := "a\nb"
	for typ, keys := range printRuleKeys {
		for _, key := range keys {
			if payloadClass(typ, key) != keyAllowed {
				t.Errorf("%s: the print-rule key %q is not an allowed key", typ, key)
			}
			kept, withheld, note := filterPayload(typ, []byte(jsonObject(key, bad, "x-other", "y")))
			wantKeys := []string{key, "x-other"}
			slices.Sort(wantKeys)
			if v, ok := kept[key]; !ok || v != nil || !slices.Equal(withheld, wantKeys) || note != valueWithheld {
				t.Errorf("%s %s: kept %v, withheld %v, note %q", typ, key, kept, withheld, note)
			}
			kept, withheld, note = filterPayload(typ, []byte(jsonObject(key, strings.Repeat("k", 253))))
			if kept[key] != strings.Repeat("k", 253) || len(withheld) != 0 || note != "" {
				t.Errorf("%s %s: a printable value was changed: withheld %v, note %q", typ, key, withheld, note)
			}
			kept, _, _ = filterPayload(typ, []byte(jsonObject(key, strings.Repeat("k", 254))))
			if v, ok := kept[key]; !ok || v != nil {
				t.Errorf("%s %s: 254 bytes kept as %v", typ, key, v)
			}
		}
	}
	// A value that is not a string is left alone, and so is a key that is not under the rule.
	kept, withheld, note := filterPayload(catalog.K8sPod, []byte(`{"name":5,"phase":"Run\nning","conditions":[{"type":"a\nb"}]}`))
	if len(withheld) != 0 || note != "" || kept["name"] == nil || kept["phase"] != "Run\nning" {
		t.Errorf("kept %v, withheld %v, note %q", kept, withheld, note)
	}
	// Every allowed key whose last word is name or namespace is a name key or says why not.
	for typ, t2 := range payloadTable {
		for _, key := range t2.allowed {
			last := key[strings.LastIndexAny(key, "._")+1:]
			if (last == "name" || last == "namespace") && !slices.Contains(printRuleKeys[typ], key) && !slices.Contains(notNameKeys, key) {
				t.Errorf("%s: the allowed key %q ends in %s and is not under the print rule", typ, key, last)
			}
		}
	}
	// The allowed keys that hold text a sender chose and do not end in name or
	// namespace are listed here, because the suffix rule above cannot find them.
	senderTextKeys := map[catalog.EntityType][]string{
		catalog.ServiceInstance: {"service.version"},
	}
	for typ, keys := range senderTextKeys {
		for _, key := range keys {
			if payloadClass(typ, key) != keyAllowed {
				t.Errorf("%s: the sender-text key %q is not an allowed key", typ, key)
			}
			if !slices.Contains(printRuleKeys[typ], key) {
				t.Errorf("%s: the sender-text key %q is not under the print rule", typ, key)
			}
		}
	}
	for _, key := range notNameKeys {
		found := false
		for _, t2 := range payloadTable {
			found = found || slices.Contains(t2.allowed, key)
		}
		if !found {
			t.Errorf("notNameKeys lists %q, which no type allows", key)
		}
	}
	// A key of the table is the key of a type the name table reads, or of its own.
	for typ, keys := range nameTable {
		for _, key := range slices.Concat(keys.name, keys.namespace) {
			if !slices.Contains(printRuleKeys[typ], key) {
				t.Errorf("%s: the name table reads %q, which the print rule does not reach in a description", typ, key)
			}
		}
	}
}

// With the rule off the same value is printed, so that the tests above are about
// the rule and not about the value.
func TestPrintRuleHookTurnsItOff(t *testing.T) {
	st := listStores(t, rankObjs)["memstore"]
	f := newFeed(t, st)
	fp := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "hooked")
	f.write(observe(ent(fp), objs, hm(9, 0), 0, jsonObject("name", "a\nb", "namespace", "shop")))
	out := found(t, sourceOn(t, st, &egressHooks{noPrintRule: true}, nil), FindEntitiesInput{Type: "k8s.pod", At: w1000})
	if len(out.Matches) != 1 || out.Matches[0].Entity.Name == nil || *out.Matches[0].Entity.Name != "a\nb" || out.Matches[0].Entity.NameNote != "" {
		t.Errorf("%+v", out.Matches)
	}
}
