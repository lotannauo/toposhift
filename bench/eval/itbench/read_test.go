package itbench

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/eval/factload"
)

func TestParseMap(t *testing.T) {
	good := []struct {
		in   string
		want map[string]string
	}{
		{`{}`, map[string]string{}},
		{`{'a': 'b'}`, map[string]string{"a": "b"}},
		{`{'a': 'b', 'c.d': "it's"}`, map[string]string{"a": "b", "c.d": "it's"}},
		{`{'a': 'x\'y', 'b': 'back\\slash', 'c': 'line\nnext\ttab'}`, map[string]string{"a": "x'y", "b": `back\slash`, "c": "line\nnext\ttab"}},
		{`{'a': '\x41é\U0001F600'}`, map[string]string{"a": "Aé\U0001F600"}},
		{`{'a': '["x","y"]', }`, map[string]string{"a": `["x","y"]`}},
		{`  { 'a' : 'b' }  `, map[string]string{"a": "b"}},
		{`{'a': ''}`, map[string]string{"a": ""}},
	}
	for _, c := range good {
		got, err := parseMap(c.in)
		if err != nil {
			t.Errorf("%q: %v", c.in, err)
			continue
		}
		if len(got) != len(c.want) {
			t.Errorf("%q: %v, want %v", c.in, got, c.want)
			continue
		}
		for k, v := range c.want {
			if got[k] != v {
				t.Errorf("%q: %q = %q, want %q", c.in, k, got[k], v)
			}
		}
	}
	for _, in := range []string{``, `[]`, `{'a': 1}`, `{'a'}`, `{'a': 'b'`, `{'a': 'b'} x`, `{'a': 'b' 'c': 'd'}`, `{'a': 'unterminated}`, `{'a': '\x4'}`, `{a: 'b'}`, `[np.datetime64('2025')]`} {
		if _, err := parseMap(in); err == nil {
			t.Errorf("%q was accepted", in)
		}
	}
}

func TestRowsQuoting(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "x.tsv")
	body := "a\tb\nc \"q\" d"
	content := tsvLine("one", "two", "three") + tsvLine("1", body, "") + tsvLine("2", "plain", "z")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	var got [][2]string
	err := rows(path, []string{"one", "two"}, func(r *row) error {
		got = append(got, [2]string{r.get("two"), r.get("three")})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0][0] != body || got[1][0] != "plain" || got[1][1] != "z" {
		t.Errorf("rows %q", got)
	}
	if err := rows(path, []string{"missing"}, func(*row) error { return nil }); err == nil || !strings.Contains(err.Error(), "no column") {
		t.Errorf("missing column: %v", err)
	}
	// A row with the wrong number of fields is an error that names the row.
	bad := filepath.Join(dir, "bad.tsv")
	if err := os.WriteFile(bad, []byte("a\tb\n1\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := rows(bad, nil, func(*row) error { return nil }); err == nil || !strings.Contains(err.Error(), "row 1") {
		t.Errorf("short row: %v", err)
	}
}

func TestReadClusterReadsOneSnapshot(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, ObjectsFile)
	if err := os.WriteFile(path, []byte(shopObjects()), 0o644); err != nil {
		t.Fatal(err)
	}
	cl, err := ReadCluster(path)
	if err != nil {
		t.Fatal(err)
	}
	if cl.Rows["pods"] != 7 || cl.Rows["nodes"] != 2 || cl.Rows["configmaps"] != 1 || cl.Rows["services"] != 5 || cl.Rows["deployments"] != 4 || cl.Rows["statefulsets"] != 1 || cl.Rows["replicasets"] != 4 {
		t.Errorf("rows by resource: %v", cl.Rows)
	}
	// Only pods and nodes go to the loader.
	if len(cl.Load) != 9 || len(cl.Nodes) != 2 {
		t.Errorf("%d records for the loader, %d nodes; want 9, 2", len(cl.Load), len(cl.Nodes))
	}
	// One pull: its time, no interval, and the loader's single-pull lifetime.
	if want := time.Date(2026, 3, 4, 10, 12, 0, 250000000, time.UTC); !cl.First.Equal(want) || !cl.Last.Equal(want) || cl.Pulls != 1 || cl.Interval != 0 || cl.Lifetime != factload.ObjectTTL {
		t.Errorf("first %s, last %s, %d pulls, interval %s, lifetime %s; want one pull at %s", cl.First, cl.Last, cl.Pulls, cl.Interval, cl.Lifetime, want)
	}
	// The record the loader gets is a pull-mode record: the observed time and the
	// resource name.
	rec := cl.Load[2].Record
	if rec.ObservedTimeUnixNano == 0 || len(rec.Attributes) != 1 || rec.Attributes[0].Key != "k8s.resource.name" {
		t.Errorf("record %+v", rec)
	}
	// A service selects pods by an exact selector, a workload owns them by uid.
	var front, svc *Object
	for _, o := range cl.Objects {
		switch {
		case o.Kind == "Deployment" && o.Name == "shop-front":
			front = o
		case o.Kind == "Service" && o.Name == "shop-front":
			svc = o
		}
	}
	if got := len(cl.pods(front)); got != 2 {
		t.Errorf("the front deployment owns %d pods, want 2", got)
	}
	if got := len(cl.pods(svc)); got != 2 {
		t.Errorf("the front service selects %d pods, want 2", got)
	}
	if names, _ := cl.Components(svc); !slices.Equal(names, []string{"shop-front"}) {
		t.Errorf("components %v", names)
	}
	// Placement is an exact node name that is listed.
	for _, o := range cl.Objects {
		if o.Kind == "Pod" && cl.Placed(o) != (o.Name != "shop-lost-r1-p1") {
			t.Errorf("pod %s placed = %t", o.Name, cl.Placed(o))
		}
	}
}

func TestReadClusterRefusesWhatItCannotRead(t *testing.T) {
	dir := t.TempDir()
	write := func(name, content string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	// A pods row whose body is a node.
	p := write("a.tsv", tsvLine(objectColumns...)+objectRow("pods", nodeBody("n", "n"), snapshot))
	if _, err := ReadCluster(p); err == nil {
		t.Error("a pods row holding a node was read")
	}
	p = write("b.tsv", tsvLine(objectColumns...)+objectRow("pods", "{not json", snapshot))
	if _, err := ReadCluster(p); err == nil {
		t.Error("a body that is not JSON was read")
	}
	p = write("c.tsv", tsvLine(objectColumns...)+objectRow("pods", podBody("u", "n", "", "x", ""), "yesterday"))
	if _, err := ReadCluster(p); err == nil {
		t.Error("a time that is not a time was read")
	}
}

func TestReadSpansTakesIdentityFromTheResourceOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, TracesFile)
	at := time.Date(2026, 3, 4, 10, 0, 1, 500000000, time.UTC)
	content := tsvLine(spanColumns...)
	// A resource with a pod name and an address and no pod uid; one with a uid; one
	// with no namespace; one that names no service, so the column does.
	content += spanRow(at, "t1", "s1", "", "Server", "shop-front", resourceAttrs("shop-front", "inst-1", "", true))
	content += spanRow(at, "t1", "s2", "s1", "Client", "shop-front", resourceAttrs("shop-front", "pod-front-1", "pod-front-1", true))
	content += spanRow(at, "t1", "s3", "s2", "Server", "shop-cart", resourceAttrs("shop-cart", "", "", false))
	content += spanRow(at, "t1", "s4", "", "Internal", "from-the-column", "{'k8s.namespace.name': 'x'}")
	content += spanRow(at, "t1", "s5", "", "SPAN_KIND_PRODUCER", "shop-cart", resourceAttrs("shop-cart", "", "", true))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	spans, err := ReadSpans(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) != 5 {
		t.Fatalf("%d spans", len(spans))
	}
	want := []factload.Span{
		{Start: at, TraceID: "t1", SpanID: "s1", Kind: factload.KindServer, Service: "shop-front", Namespace: appNS, InstanceID: "inst-1"},
		{Start: at, TraceID: "t1", SpanID: "s2", ParentID: "s1", Kind: factload.KindClient, Service: "shop-front", Namespace: appNS, InstanceID: "pod-front-1", PodUID: "pod-front-1"},
		{Start: at, TraceID: "t1", SpanID: "s3", ParentID: "s2", Kind: factload.KindServer, Service: "shop-cart"},
		{Start: at, TraceID: "t1", SpanID: "s4", Kind: factload.KindInternal, Service: "from-the-column"},
		{Start: at, TraceID: "t1", SpanID: "s5", Kind: factload.KindProducer, Service: "shop-cart", Namespace: appNS},
	}
	for i := range want {
		if spans[i] != want[i] {
			t.Errorf("span %d is %+v, want %+v", i, spans[i], want[i])
		}
	}
}

func TestParseGroundTruth(t *testing.T) {
	flat := `
fault:
  - entity: {name: n, kind: Pod, group_id: g}
    category: Change
    fault_mechanism: custom
groups:
  - id: g
    kind: Pod
    namespace: shop
    filter: ["a-.*"]
    root_cause: true
  - id: h
    kind: Service
    name: exact
propagations:
  - {source: g, target: h}
`
	g, err := ParseGroundTruth([]byte(flat))
	if err != nil {
		t.Fatal(err)
	}
	root, ok := g.Root()
	if len(g.Groups) != 2 || !ok || root.ID != "g" || g.Fault[0].Entity.GroupID != "g" || g.Fault[0].Mechanism != "custom" {
		t.Errorf("flat: %+v", g)
	}
	if !slices.Equal(g.Chain(), []string{"g", "h"}) || !slices.Equal(g.Targets(), []string{"h"}) {
		t.Errorf("chain %v, targets %v", g.Chain(), g.Targets())
	}

	wrapped := "apiVersion: itbench.io/v1\nkind: GroundTruth\nmetadata:\n  name: x\nspec:\n" + indent(flat)
	w, err := ParseGroundTruth([]byte(wrapped))
	if err != nil {
		t.Fatal(err)
	}
	if len(w.Groups) != 2 || len(w.Propagations) != 1 {
		t.Errorf("wrapped: %+v", w)
	}

	// A repeated key is an error: the file is listed as unreadable, not repaired.
	dup := "fault: []\ngroups:\n  - id: a\n    kind: Pod\n    id: b\n"
	if _, err := ParseGroundTruth([]byte(dup)); err == nil || !strings.Contains(err.Error(), "already defined") {
		t.Errorf("repeated key: %v", err)
	}
	// Also in a section the loader does not use.
	dupAlerts := "fault: []\nalerts:\n  - id: a\n    group_id: x\n    group_id: y\n"
	if _, err := ParseGroundTruth([]byte(dupAlerts)); err == nil || !strings.Contains(err.Error(), "already defined") {
		t.Errorf("repeated key in an unused section: %v", err)
	}
	if _, err := ParseGroundTruth([]byte("fault: [")); err == nil {
		t.Error("broken YAML was read")
	}
	// Two roots are no root.
	two := "groups:\n  - {id: a, root_cause: true}\n  - {id: b, root_cause: true}\n"
	g2, err := ParseGroundTruth([]byte(two))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := g2.Root(); ok {
		t.Error("two groups mark the root and one was taken")
	}
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		if l != "" {
			lines[i] = "  " + l
		}
	}
	return strings.Join(lines, "\n") + "\n"
}

// The fixtures of the scorer's package mirror the schema its documentation
// records, in both shapes.
func TestTheRecordedFixturesParse(t *testing.T) {
	for _, name := range []string{"ground_truth_flat.yaml", "ground_truth_wrapped.yaml"} {
		g, err := ReadGroundTruth(filepath.Join("..", "blastscore", "testdata", name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(g.Fault) == 0 || len(g.Groups) == 0 || len(g.Propagations) == 0 {
			t.Errorf("%s: %+v", name, g)
		}
		if _, ok := g.Root(); !ok {
			t.Errorf("%s: no single root", name)
		}
	}
}

func TestMatcher(t *testing.T) {
	m := Group{Kind: "Service", Filter: []string{`shop-front\b`}}.Matcher()
	if m.Via != ViaFilter || !m.Match("shop-front") || m.Match("shop-front-proxy") || m.Match("xshop-front") || m.Match("shop-fronts") {
		t.Errorf("a filter is a whole-name match: %+v", m)
	}
	many := Group{Kind: "Pod", Filter: []string{"a-.*", "b"}}.Matcher()
	if !many.Match("a-1") || !many.Match("b") || many.Match("ab") || many.Match("c") {
		t.Error("several filters are alternatives, each a whole-name match")
	}
	if m := (Group{Kind: "ConfigMap", Name: "flags"}).Matcher(); m.Via != ViaName || !m.Match("flags") || m.Match("flags-2") {
		t.Errorf("a name is exact: %+v", m)
	}
	for _, g := range []Group{
		{Kind: "Pod"},
		{Kind: "Pod", Name: "shop-*"},
		{Kind: "Pod", Name: "shop-?"},
		{Kind: "Pod", Name: "shop-*", Filter: []string{"*.*"}},
		{Kind: "Pod", Filter: []string{"(unclosed"}},
	} {
		if m := g.Matcher(); m.Why == "" || m.Match != nil {
			t.Errorf("%+v names objects: %+v", g, m)
		}
	}
}

func TestScenariosAreOrderedByNumber(t *testing.T) {
	dir := t.TempDir()
	for _, n := range []string{"Scenario-10", "Scenario-2", "Scenario-1", "Scenario-x", "notes"} {
		if err := os.Mkdir(filepath.Join(dir, n), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(dir, "Scenario-3"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := Scenarios(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, g := range got {
		names = append(names, filepath.Base(g))
	}
	if !slices.Equal(names, []string{"Scenario-1", "Scenario-2", "Scenario-10"}) {
		t.Errorf("scenarios %v", names)
	}
}
