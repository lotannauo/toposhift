package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/demo"
)

// The end-to-end test of the subgraph routes of the demo: toposhift demo is
// started on a loopback port and asked, over HTTP and nothing else, for the
// temporal subgraph of node-2 and of two of its pods over the minutes it fails,
// and for its blast radius. As in the end-to-end test of the tools, what each
// answer must hold is computed from the generated world alone (the oracle of
// demo_e2e_test.go): the entities in the answer's dictionary, with the names the
// answer gives them, the elements that name their keys, the intervals of the
// placements, and the number of edges withheld because they hold an address. The
// live answer is then projected into the words of the golden file (names in place
// of ids, absolute times, labels) and compared with it byte for byte; the file is
// written, with -update, only when the oracle agreed and nothing else failed.

const e2eAPIGolden = "testdata/demo_api_e2e.golden.json"

// apiView is one answer in the words of the golden file.
type apiView struct {
	Route      string         `json:"route"`
	Args       map[string]any `json:"args"`
	Question   string         `json:"question"`
	Derived    bool           `json:"derived"`
	Window     map[string]any `json:"window"`
	Policy     map[string]any `json:"policy"`
	Incomplete bool           `json:"incomplete"`
	Withheld   map[string]any `json:"withheld"`
	Notes      []string       `json:"notes"`
	// Entities are the dictionary's entities in its order, as the names list
	// says them, with whether the entity is outside the question's own.
	Entities []string `json:"entities"`
	// Elements are the document's elements in its order.
	Elements []string `json:"elements"`
	// Document counts what the document holds besides.
	Document map[string]any `json:"document"`
}

// apiPost sends a request body to a route of the demo and returns the status, the
// headers and the body.
func apiPost(t testing.TB, base, path string, args map[string]any) (int, http.Header, []byte) {
	t.Helper()
	status, header, out := apiPostCtx(t.Context(), base, path, args)
	if status == 0 {
		t.Fatalf("POST %s: no answer", path)
	}
	return status, header, out
}

// apiPostCtx is apiPost that does not fail the test, so that a goroutine can
// call it; a request that could not be sent has status 0.
func apiPostCtx(ctx context.Context, base, path string, args map[string]any) (int, http.Header, []byte) {
	b, err := json.Marshal(args)
	if err != nil {
		return 0, nil, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+path, bytes.NewReader(b))
	if err != nil {
		return 0, nil, nil
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, nil
	}
	defer func() { _ = resp.Body.Close() }()
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, nil
	}
	return resp.StatusCode, resp.Header, out
}

func decodeObject(t testing.TB, b []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("%v in %.300s", err, b)
	}
	return m
}

// TestDemoServesTemporalSubgraphs asks the routes of a started demo.
func TestDemoServesTemporalSubgraphs(t *testing.T) {
	o := newOracle(t)
	q := askedAbout(o)
	s := startDemo(t)

	banner := tokenInBanner.FindStringSubmatch(s.stderr.String())
	if banner == nil {
		t.Fatalf("no token in the banner:\n%s", s.stderr)
	}
	token := banner[1]
	if want := "and the subgraph routes under " + s.base + apiPrefix + " (POST), loopback only"; !strings.Contains(s.stderr.String(), want) {
		t.Errorf("the command does not say where the routes are (%q):\n%s", want, s.stderr)
	}

	type asked struct {
		name  string
		route string
		args  map[string]any
		win   iv
		body  []byte
		ans   map[string]any
	}
	rfc := func(tm time.Time) string { return tm.UTC().Format(time.RFC3339) }
	evicted, replacement := q.story.Evicted[0], q.story.Replacements[0]
	pair := iv{q.e0, q.e1}
	asks := []*asked{
		{
			name: "q1_scope_node", route: "scope", win: q.w1,
			args: map[string]any{"scope": []string{q.story.Failed.ID}, "from": rfc(q.w1.from), "to": rfc(q.w1.to)},
		},
		{name: "q2_blast_radius_node", route: "blast_radius", win: q.w1, args: q.blastArgs()},
		{
			name: "q3_scope_evicted_pod_and_its_replacement", route: "scope", win: pair,
			args: map[string]any{"scope": []string{evicted.ID, replacement.ID}, "from": rfc(pair.from), "to": rfc(pair.to)},
		},
	}
	for _, a := range asks {
		status, header, body := apiPost(t, s.base, apiPrefix+a.route, a.args)
		if status != http.StatusOK {
			t.Fatalf("%s: status %d: %s", a.name, status, body)
		}
		for k, want := range map[string]string{
			"Content-Type": "application/json; charset=utf-8", "Cache-Control": "no-store",
			"X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
			"Content-Length": strconv.Itoa(len(body)),
		} {
			if got := header.Get(k); got != want {
				t.Errorf("%s: %s is %q, want %q", a.name, k, got, want)
			}
		}
		a.body, a.ans = body, decodeObject(t, body)
		if endpointIDText.Match(body) {
			t.Errorf("%s: the answer prints an endpoint id: %.200s", a.name, endpointIDText.Find(body))
		}
		doc := obj(a.ans["document"])
		if a.ans["token"] != token || doc["token"] != token {
			t.Errorf("%s: token %v, document token %v, want the banner's %s", a.name, a.ans["token"], doc["token"], token)
		}
		if a.ans["derived"] != true || doc["basis"] != "derived" {
			t.Errorf("%s: derived %v, basis %v", a.name, a.ans["derived"], doc["basis"])
		}
	}

	got := map[string]apiView{}
	for _, a := range asks {
		got[a.name] = project(t, o, a.route, a.args, a.win, a.ans)
	}

	// The oracle first: what the world says each answer holds.
	o.wantScopeOfNode(q.w1).compare(t, "q1", got["q1_scope_node"], nil)
	pods := []*demo.Pod{o.byID[evicted.ID], o.byID[replacement.ID]}
	o.wantScopeOfPods(pair, pods...).compare(t, "q3", got["q3_scope_evicted_pod_and_its_replacement"], nil)
	blast := o.wantBlastOfNode(q)
	blast.withheld = 0
	// The entity elements of a walk are the existence of each entity, which the
	// tools do not list; the derived and the edge elements are compared.
	blast.compare(t, "q2", got["q2_blast_radius_node"], func(r string) bool { return !strings.HasPrefix(r, "entity ") })
	for name, w := range map[string]int{"q1_scope_node": 1, "q3_scope_evicted_pod_and_its_replacement": 2} {
		if n := int(got[name].Withheld["edges"].(float64)); n == 0 || n != w {
			t.Errorf("%s: %d edges withheld, want %d (above zero)", name, n, w)
		}
	}

	// The other two routes answer too, with no endpoint id in them.
	for _, c := range []struct {
		route string
		args  map[string]any
	}{
		{"co_residents", map[string]any{"pod": q.checkout.ID, "from": rfc(q.coFrom), "to": rfc(q.coTo)}},
		{"service_context", map[string]any{"service": q.story.Service("shop", "checkout").ID, "from": rfc(q.context.from), "to": rfc(q.context.to)}},
	} {
		status, _, body := apiPost(t, s.base, apiPrefix+c.route, c.args)
		ans := decodeObject(t, body)
		if status != http.StatusOK || ans["token"] != token || ans["question"] != c.route || ans["derived"] != true || endpointIDText.Match(body) {
			t.Errorf("%s: status %d, token %v, question %v: %.300s", c.route, status, ans["token"], ans["question"], body)
		}
	}

	// Asked again at its token, the same world: the same document but for the
	// request's echo of the token it named.
	again := asks[0]
	args := map[string]any{"asof": token}
	for k, v := range again.args {
		args[k] = v
	}
	status, _, body := apiPost(t, s.base, apiPrefix+again.route, args)
	if status != http.StatusOK {
		t.Fatalf("asked at its token: %d %s", status, body)
	}
	a, b := decodeObject(t, body), decodeObject(t, again.body)
	for _, m := range []map[string]any{a, b} {
		delete(obj(obj(m["document"])["query"]), "asof")
	}
	if x, y := encode(t, a), encode(t, b); !bytes.Equal(x, y) {
		t.Errorf("at asof %s the answer differs:\n%s", token, firstDifference(x, y))
	}

	if code := s.stop(t); code != 0 {
		t.Errorf("exit %d after the signal, want 0:\n%s", code, s.stderr)
	}
	if endpointIDText.MatchString(s.stderr.String()) {
		t.Error("the log prints an endpoint id")
	}

	live := encode(t, got)
	if t.Failed() {
		if *update {
			t.Log("-update did not write the golden file: the test failed first")
		}
		return
	}
	if *update {
		if err := os.MkdirAll(filepath.Dir(e2eAPIGolden), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(e2eAPIGolden, live, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("wrote %s", e2eAPIGolden)
		return
	}
	golden, err := os.ReadFile(e2eAPIGolden)
	if err != nil {
		t.Fatalf("%v; write it with: go test -run 'TestDemoServesTemporalSubgraphs$' ./cmd/toposhift -update", err)
	}
	if !bytes.Equal(live, golden) {
		t.Errorf("the answers differ from %s:\n%s", e2eAPIGolden, firstDifference(live, golden))
	}
}

// project writes an answer in the words of the golden file.
func project(t testing.TB, o *oracle, route string, args map[string]any, win iv, ans map[string]any) apiView {
	t.Helper()
	r := reader{t, o}
	doc := obj(ans["document"])
	from, err := strconv.ParseInt(str(doc["from"]), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	abs := func(rel any) string {
		d, err := strconv.ParseInt(str(rel), 10, 64)
		if err != nil {
			t.Fatalf("%v is not a time", rel)
		}
		return fmtTime(time.Unix(0, from+d))
	}
	label := func(id string) string {
		info, ok := o.refs[id]
		if !ok {
			t.Errorf("the answer names %s, which the world does not have", id)
			return "?"
		}
		return info.typ + " " + info.name
	}

	names := arr(ans["names"])
	entities := arr(doc["entities"])
	if len(names) != len(entities) {
		t.Fatalf("%s: %d names for %d entities", route, len(names), len(entities))
	}
	v := apiView{
		Route: route, Args: o.argLabels(args), Question: str(ans["question"]), Derived: ans["derived"] == true,
		Window: obj(ans["window"]), Policy: obj(ans["policy"]), Incomplete: ans["incomplete"] == true,
		Withheld: obj(obj(ans["counts"])["withheld"]),
	}
	for i, n := range names {
		e := obj(entities[i])
		if obj(n)["id"] != e["fingerprint"] {
			t.Errorf("%s: names[%d] is %v, the dictionary's entity is %v", route, i, obj(n)["id"], e["fingerprint"])
		}
		s := r.ref(n, win)
		if e["boundary"] == true {
			s += " (boundary)"
		}
		v.Entities = append(v.Entities, s)
	}
	for _, e := range arr(doc["elements"]) {
		m := obj(e)
		k := obj(m["key"])
		what := str(k["kind"]) + " " + label(str(k["a"]))
		switch str(k["kind"]) {
		case "edge":
			what += " " + str(k["relation"]) + " " + label(str(k["b"]))
		case "derived":
			what += " " + str(k["rule"]) + " " + label(str(k["b"])) + " " + str(k["detail"])
		}
		var pieces []string
		for _, p := range arr(m["pieces"]) {
			pm := obj(p)
			s := abs(pm["start"]) + ".." + abs(pm["end"]) + " (" + str(pm["end_reason"]) + ")"
			if pm["before"] == true {
				s += " before"
			}
			pieces = append(pieces, s)
		}
		v.Elements = append(v.Elements, row(what, str(m["subject"])+"/"+str(m["rule"]), str(m["class"])+" "+fmt.Sprint(m["transitions"]), strings.Join(pieces, "; ")))
	}
	for _, n := range arr(ans["notes"]) {
		if s := str(n); strings.HasPrefix(s, "Pass asof=") {
			if !strings.HasPrefix(s, "Pass asof="+str(ans["token"])+" ") {
				t.Errorf("the note %q does not name the answer's token %v", s, ans["token"])
			}
		} else {
			v.Notes = append(v.Notes, s)
		}
	}
	v.Document = map[string]any{
		"format": doc["format"], "keyframes": len(arr(doc["keyframes"])), "deltas": len(arr(doc["deltas"])),
		"quarantine": map[string]any{"entities": len(arr(obj(doc["quarantine"])["entities"])), "edges": len(arr(obj(doc["quarantine"])["edges"]))},
		"query":      queryView(o, obj(doc["query"])),
	}
	slices.Sort(v.Notes)
	return v
}

// queryView is the document's echo of the question, with its seeds named.
func queryView(o *oracle, q map[string]any) map[string]any {
	var seeds []string
	for _, s := range strs(q["seeds"]) {
		seeds = append(seeds, o.label(s))
	}
	return map[string]any{"kind": q["kind"], "seeds": seeds, "options": q["options"], "asof": q["asof"]}
}

// ---- the oracle ----

// apiWant is what the world says an answer holds: the entities of its
// dictionary, as the names list says them, the elements by what they are about
// ("kind key | class transitions | start..end"), and the edges withheld.
type apiWant struct {
	entities []string
	rows     []string
	withheld int
}

// core is a row of the view reduced to what the oracle knows: its key, its class
// and where each of its pieces runs, without the reason it ends or whether it
// began before the window.
func core(row string) string {
	cols := strings.Split(row, " | ")
	if len(cols) != 4 {
		return row
	}
	var pieces []string
	for _, p := range strings.Split(cols[3], "; ") {
		p, _, _ = strings.Cut(p, " (")
		pieces = append(pieces, p)
	}
	return row0(cols[0], cols[2], strings.Join(pieces, "; "))
}

func row0(what, class, span string) string { return row(what, class, span) }

// reachRow is the row of something that lasts over reach inside the window.
func reachRow(what string, reach, win iv) string {
	class, n := classOf(reach, win)
	return row0(what, class+" "+strconv.Itoa(n), fmtTime(reach.from)+".."+fmtTime(reach.to))
}

// plain is the name an element's key gives an id.
func (o *oracle) plain(id string) string {
	r, ok := o.refs[id]
	if !ok {
		o.t.Fatalf("the oracle has no entity %s", id)
	}
	return r.typ + " " + r.name
}

// wantScopeOfNode is the scope question about the node that fails: the node, the
// host it runs on, and each pod scheduled on it in the window, with the edges
// between them, and the node's address withheld.
func (o *oracle) wantScopeOfNode(win iv) apiWant {
	failed := o.failed()
	node := o.w.Nodes[failed]
	on := o.podsOnNode(failed)
	o.checkMargin("scope", "the window start", win.from, on, o.w.Outages[0].From, o.w.Outages[0].To)
	o.checkMargin("scope", "the window end", win.to, on, o.w.Outages[0].From, o.w.Outages[0].To)
	nid, hid := o.nodeID[failed], o.hostID[node.Machine]
	want := apiWant{
		entities: []string{o.ref(nid, win), o.ref(hid, win) + " (boundary)"},
		rows: []string{
			reachRow("entity "+o.plain(nid), win, win),
			reachRow("edge "+o.plain(nid)+" runs_on "+o.plain(hid), win, win),
		},
		withheld: nodeClaims(node),
	}
	for _, p := range on {
		reach := o.placed(p).and(win)
		if reach.empty() {
			continue
		}
		want.entities = append(want.entities, o.ref(o.podID[p], win)+" (boundary)")
		want.rows = append(want.rows, reachRow("edge "+o.plain(o.podID[p])+" scheduled_on "+o.plain(nid), reach, win))
	}
	return want
}

// wantScopeOfPods is the scope question about pods: each pod, its container, its
// node and its service instance, with the edges between them, and the addresses
// they hold withheld.
func (o *oracle) wantScopeOfPods(win iv, pods ...*demo.Pod) apiWant {
	o.checkMargin("scope", "the window start", win.from, pods)
	o.checkMargin("scope", "the window end", win.to, pods)
	var want apiWant
	seen := map[string]bool{}
	add := func(id string, boundary bool) {
		if seen[id] {
			return
		}
		seen[id] = true
		s := o.ref(id, win)
		if boundary {
			s += " (boundary)"
		}
		want.entities = append(want.entities, s)
	}
	for _, p := range pods {
		pid, nid := o.podID[p], o.nodeID[p.Node]
		add(pid, false)
		want.rows = append(want.rows, reachRow("entity "+o.plain(pid), o.lives(p).and(win), win))
		if reach := o.placed(p).and(win); !reach.empty() {
			add(nid, true)
			want.rows = append(want.rows, reachRow("edge "+o.plain(pid)+" scheduled_on "+o.plain(nid), reach, win))
		}
		if reach := o.running(p).and(win); !reach.empty() {
			add(o.containerID[p], true)
			want.rows = append(want.rows, reachRow("edge "+o.plain(o.containerID[p])+" part_of "+o.plain(pid), reach, win))
		}
		if b, ok := o.bridge(p); ok {
			if reach := b.and(win); !reach.empty() {
				add(o.instID[p], true)
				want.rows = append(want.rows, reachRow("edge "+o.plain(o.instID[p])+" part_of "+o.plain(pid), reach, win))
			}
		}
		want.withheld += o.podClaims(p, win)
	}
	return want
}

// wantBlastOfNode is the subgraph of the blast radius of the node that fails: the
// node and each member of the walk, a derived element for each member from the
// node, as the tool's answer lists its reach, and the edges the paths run on.
func (o *oracle) wantBlastOfNode(q questions) apiWant {
	win := q.w1
	tool := o.wantBlast(q)
	failed := o.failed()
	nid := o.nodeID[failed]
	want := apiWant{entities: []string{o.ref(nid, win)}}
	for _, m := range tool.Lists["members"] {
		cols := strings.Split(m, " | ")
		want.entities = append(want.entities, cols[1])
		reach := strings.TrimPrefix(cols[4], "reach ")
		reach = strings.TrimSuffix(reach, " (reach_ended)")
		want.rows = append(want.rows, row0("derived "+o.plain(nid)+" blast_radius.reach "+o.plainOfRef(cols[1])+" ", cols[2], reach))
	}
	// The edges the paths run on: each member pod on the node, each container and
	// each instance part of a member pod.
	for _, p := range o.podsOnNode(failed) {
		reach := o.placed(p).and(win)
		if reach.empty() {
			continue
		}
		pid := o.podID[p]
		want.rows = append(want.rows, reachRow("edge "+o.plain(pid)+" scheduled_on "+o.plain(nid), reach, win))
		if creach := o.running(p).and(win); !creach.empty() {
			want.rows = append(want.rows, reachRow("edge "+o.plain(o.containerID[p])+" part_of "+o.plain(pid), creach, win))
		}
		if b, ok := o.bridge(p); ok && !b.and(win).empty() {
			want.rows = append(want.rows, reachRow("edge "+o.plain(o.instID[p])+" part_of "+o.plain(pid), b.and(win), win))
		}
	}
	return want
}

// plainOfRef is the type and name of an entity written as a listing writes it,
// "type name [namespace]" or "type name (unnamed)".
func (o *oracle) plainOfRef(ref string) string {
	ref, _, _ = strings.Cut(ref, " [")
	return strings.TrimSuffix(ref, " (unnamed)")
}

// compare checks a projected answer against what the world says: the same
// entities, the same elements by key, class and interval (those the world knows),
// and the same count of edges withheld.
func (w apiWant) compare(t testing.TB, name string, v apiView, only func(row string) bool) {
	t.Helper()
	if got, want := sorted(v.Entities), sorted(w.entities); !slices.Equal(got, want) {
		t.Errorf("%s: the dictionary holds\n  %s\nthe world says\n  %s", name, strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
	var got []string
	for _, r := range v.Elements {
		if only == nil || only(r) {
			got = append(got, core(r))
		}
	}
	if want := sorted(w.rows); !slices.Equal(sorted(got), want) {
		t.Errorf("%s: the elements are\n  %s\nthe world says\n  %s", name, strings.Join(sorted(got), "\n  "), strings.Join(want, "\n  "))
	}
	if n := int(v.Withheld["edges"].(float64)); w.withheld >= 0 && n != w.withheld {
		t.Errorf("%s: %d edges withheld, the world gives the entities %d addresses", name, n, w.withheld)
	}
}
