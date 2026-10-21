package apihttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/mcp"
)

// endpointID is the text of an endpoint's id.
var endpointID = regexp.MustCompile(`topo\.endpoint:[0-9a-f]{32}`)

func body(tb testing.TB, m map[string]any) string {
	tb.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		tb.Fatal(err)
	}
	return string(b)
}

// ask is one question to one route, and what its answer must show.
type ask struct {
	name string
	path string
	body string
	// withheld is the number of edges to endpoints the request did not name,
	// counted by hand from the fixture.
	withheld int
	// named is the endpoints the request names, so that its answer may print them.
	named []identity.Fingerprint
	// direct is the same question to the source, for the bytes it writes.
	direct func(*mcp.Source) (mcp.SubgraphAnswer, error)
}

func (w *world) asks(tb testing.TB) []ask {
	tb.Helper()
	scope := func(ids ...identity.Fingerprint) (string, mcp.ScopeSubgraphInput) {
		in := mcp.ScopeSubgraphInput{Scope: strs(ids...), From: w10, To: w1030}
		return body(tb, map[string]any{"scope": in.Scope, "from": w10, "to": w1030}), in
	}
	var out []ask
	for _, tc := range []struct {
		name     string
		ids      []identity.Fingerprint
		withheld int
		named    []identity.Fingerprint
	}{
		{"scope of P", []identity.Fingerprint{w.p}, 2, nil},
		{"scope of N", []identity.Fingerprint{w.n}, 1, nil},
		{"scope of P and N", []identity.Fingerprint{w.p, w.n}, 3, nil},
		{"scope of P and E1", []identity.Fingerprint{w.p, w.e1}, 1, []identity.Fingerprint{w.e1}},
	} {
		b, in := scope(tc.ids...)
		out = append(out, ask{
			name: tc.name, path: PathScope, body: b, withheld: tc.withheld, named: tc.named,
			direct: func(s *mcp.Source) (mcp.SubgraphAnswer, error) { return s.ScopeSubgraph(tb.Context(), in) },
		})
	}
	co := mcp.CoResidentsSubgraphInput{Pod: w.p.String(), From: w10, To: w1030}
	sc := mcp.ServiceContextSubgraphInput{Service: w.svc.String(), From: w10, To: w1030}
	br := mcp.BlastRadiusSubgraphInput{Entity: w.n.String(), From: w10, To: w1030, Depth: 3}
	return append(out,
		ask{
			name: "co_residents of P", path: PathCoResidents,
			body:   body(tb, map[string]any{"pod": co.Pod, "from": w10, "to": w1030}),
			direct: func(s *mcp.Source) (mcp.SubgraphAnswer, error) { return s.CoResidentsSubgraph(tb.Context(), co) },
		},
		ask{
			name: "service_context of svc", path: PathServiceContext,
			body:   body(tb, map[string]any{"service": sc.Service, "from": w10, "to": w1030}),
			direct: func(s *mcp.Source) (mcp.SubgraphAnswer, error) { return s.ServiceContextSubgraph(tb.Context(), sc) },
		},
		ask{
			name: "blast_radius of N", path: PathBlastRadius,
			body:   body(tb, map[string]any{"entity": br.Entity, "from": w10, "to": w1030, "depth": 3}),
			direct: func(s *mcp.Source) (mcp.SubgraphAnswer, error) { return s.BlastRadiusSubgraph(tb.Context(), br) },
		},
	)
}

// docOf is the part of the document's JSON twin the tests read.
type docOf struct {
	Token    string `json:"token"`
	Basis    string `json:"basis"`
	Entities []struct {
		Fingerprint string `json:"fingerprint"`
	} `json:"entities"`
	Elements []docElement `json:"elements"`
}

type docElement struct {
	Key struct {
		Kind     string `json:"kind"`
		A        string `json:"a"`
		B        string `json:"b"`
		Relation string `json:"relation"`
	} `json:"key"`
}

func decodeAnswer(tb testing.TB, b []byte) (mcp.SubgraphAnswer, docOf) {
	tb.Helper()
	var a mcp.SubgraphAnswer
	if err := json.Unmarshal(b, &a); err != nil {
		tb.Fatalf("%v in %s", err, b)
	}
	var d docOf
	if err := json.Unmarshal(a.Document, &d); err != nil {
		tb.Fatalf("the document: %v", err)
	}
	return a, d
}

// Every route answers its question with 200: the envelope's token is the
// document's and the store's, the answer is labelled derived, its names are
// aligned with the document's dictionary, what was withheld is what the fixture
// says, no endpoint id the request did not name is anywhere in the bytes, and the
// bytes are the ones the source scanned.
func TestRoutesAnswerEveryQuestion(t *testing.T) {
	w := newWorld(t)
	api, src := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	for _, a := range w.asks(t) {
		t.Run(a.name, func(t *testing.T) {
			got := post(t, base, a.path, a.body)
			if got.status != http.StatusOK {
				t.Fatalf("status %d: %s", got.status, got.body)
			}
			if ct := got.header.Get("Content-Type"); ct != "application/json; charset=utf-8" {
				t.Errorf("Content-Type %q", ct)
			}
			if cl := got.header.Get("Content-Length"); cl != strconv.Itoa(len(got.body)) {
				t.Errorf("Content-Length %q, body %d bytes", cl, len(got.body))
			}
			ans, doc := decodeAnswer(t, got.body)
			if want := strconv.FormatUint(w.st.LastSeq(), 10); ans.Token != want || doc.Token != want {
				t.Errorf("token %q, document token %q, store %s", ans.Token, doc.Token, want)
			}
			if !ans.Derived || doc.Basis != "derived" {
				t.Errorf("derived %v, basis %q", ans.Derived, doc.Basis)
			}
			if len(ans.Names) != len(doc.Entities) {
				t.Fatalf("%d names for %d entities", len(ans.Names), len(doc.Entities))
			}
			for i, n := range ans.Names {
				if n.ID != doc.Entities[i].Fingerprint {
					t.Errorf("names[%d] is %s, the dictionary's entity is %s", i, n.ID, doc.Entities[i].Fingerprint)
				}
			}
			if ans.Counts.Withheld != (mcp.SubgraphWithheld{Edges: a.withheld}) {
				t.Errorf("withheld %+v, want %d edges", ans.Counts.Withheld, a.withheld)
			}
			named := map[string]bool{}
			for _, fp := range a.named {
				named[fp.String()] = true
			}
			for _, id := range endpointID.FindAllString(got.text(), -1) {
				if !named[id] {
					t.Errorf("the answer prints %s, which the request did not name", id)
				}
			}
			for fp := range named {
				if !strings.Contains(got.text(), fp) {
					t.Errorf("the request named %s and the answer does not print it", fp)
				}
			}
			// The same question to the source gives the same bytes: nothing is
			// written but what its last check scanned.
			direct, err := a.direct(src)
			if err != nil {
				t.Fatal(err)
			}
			want, err := direct.JSON()
			if err != nil || !bytes.Equal(got.body, want) {
				t.Errorf("the bytes written differ from the source's (%d and %d bytes)", len(got.body), len(want))
			}
		})
	}
}

// A request that names an endpoint gets its edge listed and its entity named;
// one that does not gets neither, and says in a note that it withheld them.
func TestRoutesNameWhatTheRequestNames(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	hasEdge := func(d docOf, a, b identity.Fingerprint) bool {
		return slices.ContainsFunc(d.Elements, func(e docElement) bool {
			return e.Key.Kind == "edge" && e.Key.A == a.String() && e.Key.B == b.String() && e.Key.Relation == string(catalog.HasAddress)
		})
	}
	plain := post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030}))
	_, d := decodeAnswer(t, plain.body)
	if hasEdge(d, w.p, w.e1) || hasEdge(d, w.p, w.e2) {
		t.Error("an address claim of P is listed though the request names no endpoint")
	}
	if !strings.Contains(plain.text(), "Withheld because each is or names an endpoint the request did not name") {
		t.Errorf("no note says what was withheld: %s", plain.body)
	}

	// E1 named as the last of three ids still names it.
	named := post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.n, w.p, w.e1), "from": w10, "to": w1030}))
	a, d := decodeAnswer(t, named.body)
	if !hasEdge(d, w.p, w.e1) || hasEdge(d, w.p, w.e2) {
		t.Errorf("with E1 named, P's claims are not exactly E1's: %s", named.body)
	}
	if a.Counts.Withheld.Edges != 2 {
		t.Errorf("withheld %+v, want 2 edges (E2 and E3)", a.Counts.Withheld)
	}
}

// What a client may rely on in every response, a refusal included.
func TestEveryResponseCarriesTheHeaders(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	for _, tc := range []struct {
		name    string
		path    string
		body    string
		headers map[string]string
		status  int
		json    bool
	}{
		{"an answer", PathScope, good, nil, 200, true},
		{"malformed JSON", PathScope, "{", nil, 400, true},
		{"an invalid argument", PathScope, body(t, map[string]any{"scope": []string{}, "from": w10, "to": w1030}), nil, 400, true},
		{"a content type that is not JSON", PathScope, good, map[string]string{"Content-Type": "text/plain"}, 415, true},
		{"a body over the cap", PathScope, strings.Repeat(" ", MaxBodyBytes+1), nil, 413, true},
		{"a Host that is not loopback", PathScope, good, map[string]string{"Host": "evil.example"}, 403, false},
		{"a cross-site browser request", PathScope, good, map[string]string{"Sec-Fetch-Site": "cross-site"}, 403, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := send(t, base, tc.path, tc.body, tc.headers)
			if got.status != tc.status {
				t.Fatalf("status %d, want %d: %s", got.status, tc.status, got.body)
			}
			for k, want := range map[string]string{
				"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer",
			} {
				if v := got.header.Get(k); v != want {
					t.Errorf("%s is %q, want %q", k, v, want)
				}
			}
			if tc.json && got.header.Get("Content-Type") != "application/json; charset=utf-8" {
				t.Errorf("Content-Type %q", got.header.Get("Content-Type"))
			}
			for k := range got.header {
				if strings.HasPrefix(strings.ToLower(k), "access-control-") {
					t.Errorf("the response carries %s: the routes are same-origin only", k)
				}
			}
			if got.header.Get("Set-Cookie") != "" {
				t.Error("the response sets a cookie")
			}
			if tc.json && tc.status != 200 {
				var e struct {
					Error struct{ Code, Message string } `json:"error"`
				}
				if err := json.Unmarshal(got.body, &e); err != nil || e.Error.Code == "" || e.Error.Message == "" {
					t.Errorf("a refusal is not {error:{code,message}}: %s", got.body)
				}
			}
		})
	}
}

// What a request must be to be read at all, and what the routes refuse before
// they read a store.
func TestRoutesRefuseWhatTheyDoNotTake(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	good := map[string]any{"scope": strs(w.p), "from": w10, "to": w1030}
	with := func(k string, v any) string {
		m := map[string]any{}
		for kk, vv := range good {
			m[kk] = vv
		}
		m[k] = v
		return body(t, m)
	}
	padded := func(n int) string {
		g := body(t, good)
		return g + strings.Repeat(" ", n-len(g))
	}
	for _, tc := range []struct {
		name    string
		method  string
		body    string
		headers map[string]string
		status  int
		code    string
	}{
		{"a plain request", "POST", body(t, good), nil, 200, ""},
		{"a content type with a charset", "POST", body(t, good), map[string]string{"Content-Type": "application/json; charset=utf-8"}, 200, ""},
		{"a body of exactly the cap", "POST", padded(MaxBodyBytes), nil, 200, ""},
		{"a body over the cap", "POST", padded(MaxBodyBytes + 1), nil, 413, "request_too_large"},
		{"text/plain", "POST", body(t, good), map[string]string{"Content-Type": "text/plain"}, 415, "unsupported_media_type"},
		{"a form", "POST", "scope=1", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, 415, "unsupported_media_type"},
		{"no content type", "POST", body(t, good), map[string]string{"Content-Type": "-"}, 415, "unsupported_media_type"},
		{"GET", "GET", "", nil, 405, "method_not_allowed"},
		{"PUT", "PUT", body(t, good), nil, 405, "method_not_allowed"},
		{"DELETE", "DELETE", "", nil, 405, "method_not_allowed"},
		{"an unknown field", "POST", with("limit", 5), nil, 400, "invalid_request"},
		{"a field of another route", "POST", with("pod", w.p.String()), nil, 400, "invalid_request"},
		{"a wrong type", "POST", with("scope", w.p.String()), nil, 400, "invalid_request"},
		{"a wrong type of a time", "POST", with("from", 5), nil, 400, "invalid_request"},
		{"data after the object", "POST", body(t, good) + `{}`, nil, 400, "invalid_request"},
		{"two objects", "POST", body(t, good) + body(t, good), nil, 400, "invalid_request"},
		{"an empty body", "POST", "", nil, 400, "invalid_request"},
		{"a body that is not JSON", "POST", "scope", nil, 400, "invalid_request"},
		{"an array", "POST", "[]", nil, 400, "invalid_request"},
		{"null", "POST", "null", nil, 400, "invalid_argument"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, err := http.NewRequestWithContext(t.Context(), tc.method, base+PathScope, strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Content-Type", "application/json")
			for k, v := range tc.headers {
				if v == "-" {
					req.Header.Del(k)
				} else {
					req.Header.Set(k, v)
				}
			}
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = resp.Body.Close() }()
			var buf bytes.Buffer
			_, _ = buf.ReadFrom(resp.Body)
			if resp.StatusCode != tc.status {
				t.Fatalf("status %d, want %d: %s", resp.StatusCode, tc.status, buf.Bytes())
			}
			// A refusal by the method or the body carries the headers of an answer.
			for k, want := range map[string]string{"Cache-Control": "no-store", "X-Content-Type-Options": "nosniff", "Referrer-Policy": "no-referrer"} {
				if got := resp.Header.Get(k); got != want {
					t.Errorf("%s is %q, want %q", k, got, want)
				}
			}
			if tc.code != "" {
				var e struct {
					Error struct{ Code, Message string } `json:"error"`
				}
				if err := json.Unmarshal(buf.Bytes(), &e); err != nil || e.Error.Code != tc.code {
					t.Errorf("code %q, want %q: %s", e.Error.Code, tc.code, buf.Bytes())
				}
				if tc.method != "POST" && resp.Header.Get("Allow") != "POST" {
					t.Errorf("Allow is %q", resp.Header.Get("Allow"))
				}
				// A sentence of the package's own echoes nothing the client sent.
				if strings.Contains(e.Error.Message, "limit") && tc.name == "an unknown field" {
					t.Errorf("the sentence echoes the field: %s", e.Error.Message)
				}
			}
		})
	}
}

// The other routes take the fields of their own questions and none other.
func TestRoutesTakeTheFieldsOfTheirQuestions(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{})
	base := serve(t, api)
	for _, tc := range []struct {
		path string
		ok   map[string]any
		bad  string
	}{
		{PathCoResidents, map[string]any{"pod": w.p.String(), "from": w10, "to": w1030, "node_only": true}, "depth"},
		{PathServiceContext, map[string]any{"service": w.svc.String(), "from": w10, "to": w1030, "node_only": true}, "limit"},
		{PathBlastRadius, map[string]any{"entity": w.n.String(), "from": w10, "to": w1030, "depth": 2}, "at"},
		{PathScope, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030, "asof": strconv.FormatUint(w.st.LastSeq(), 10)}, "entity"},
	} {
		if got := post(t, base, tc.path, body(t, tc.ok)); got.status != 200 {
			t.Errorf("%s: status %d: %s", tc.path, got.status, got.body)
		}
		bad := map[string]any{}
		for k, v := range tc.ok {
			bad[k] = v
		}
		bad[tc.bad] = 1
		if got := post(t, base, tc.path, body(t, bad)); got.status != 400 {
			t.Errorf("%s with %q: status %d, want 400", tc.path, tc.bad, got.status)
		}
	}
}
