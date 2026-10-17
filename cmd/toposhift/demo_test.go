package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/demo"
	"github.com/lotannauo/toposhift/internal/mcp"
)

// demoRun runs `toposhift demo args...` to its end in this process, with no
// environment. A run that serves, as it must not for the arguments the tests
// refuse, is ended by the deadline and fails them on its status.
func demoRun(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	var out, errb bytes.Buffer
	code = run(ctx, append([]string{"demo"}, args...), noEnv, &out, &errb)
	return code, out.String(), errb.String()
}

// bannerLine is the first line of the banner, the one with the digest.
var bannerLine = regexp.MustCompile(`(?m)^toposhift demo: .*$`)

// The demo builds the cluster, prints what it built to stderr, writes nothing to
// stdout, and ends with status 0. Two runs print the same banner: it is a function
// of the seed. Nothing in it is an endpoint's id.
func TestDemoNoServePrintsTheBanner(t *testing.T) {
	var lines [2]string
	for i := range lines {
		code, out, errs := demoRun(t, "--no-serve")
		if code != 0 || out != "" {
			t.Fatalf("exit %d, stdout %q, stderr:\n%s", code, out, errs)
		}
		lines[i] = bannerLine.FindString(errs)
		if lines[i] == "" || !strings.Contains(lines[i], "digest ") || !strings.Contains(lines[i], "seed 1") {
			t.Fatalf("no banner line in stderr:\n%s", errs)
		}
		if strings.Contains(errs, "topo.endpoint:") {
			t.Errorf("stderr prints an endpoint id:\n%s", errs)
		}
		if strings.Contains(errs, "listening") {
			t.Errorf("--no-serve listened:\n%s", errs)
		}
		if !strings.Contains(errs, demo.NewWorld(demo.DefaultSeed).Story().Failed.ID) {
			t.Errorf("the banner does not name node-2:\n%s", errs)
		}
	}
	if lines[0] != lines[1] {
		t.Errorf("two runs print two banners:\n%s\n%s", lines[0], lines[1])
	}
}

// A mistake in the command line is a sentence and status 2, nothing is built, and
// nothing listens.
func TestDemoRefusesWhatItCannotServe(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"extra"}, `unexpected argument "extra"`},
		{[]string{"--nope"}, "flag provided but not defined"},
		{[]string{"--stdio", "--listen", "127.0.0.1:0"}, "at most one of --listen, --stdio and --no-serve"},
		{[]string{"--no-serve", "--stdio"}, "at most one of --listen, --stdio and --no-serve"},
		{[]string{"--no-serve", "--listen", "127.0.0.1:0"}, "at most one of --listen, --stdio and --no-serve"},
		{[]string{"--listen", "0.0.0.0:0"}, "loopback only"},
		{[]string{"--listen", ":0"}, "loopback only"},
		{[]string{"--listen", "192.0.2.1:7070"}, "loopback only"},
	} {
		code, out, errs := demoRun(t, tc.args...)
		if code != 2 || out != "" || !strings.Contains(errs, tc.want) {
			t.Errorf("demo %v: exit %d, stdout %q, stderr %q; want exit 2 and %q", tc.args, code, out, errs, tc.want)
		}
		if strings.Contains(errs, "listening") || strings.Contains(errs, "built the cluster") {
			t.Errorf("demo %v went on after the refusal:\n%s", tc.args, errs)
		}
	}
}

// The help is the command's own: it is printed to stdout with status 0, it names
// the routes, the default address, the three flags, the fixed window, and what the
// command does not do, and it is not serve's.
func TestDemoHelp(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"-h"}, {"--help"}} {
		code, out, errs := demoRun(t, args...)
		if code != 0 || errs != "" || !strings.HasPrefix(out, "Usage: toposhift demo") {
			t.Fatalf("demo %v: exit %d, stdout %.60q, stderr %q", args, code, out, errs)
		}
		for _, want := range []string{
			"127.0.0.1:7070", "POST /mcp", "GET /healthz", "--stdio", "--no-serve", "--listen",
			"2026-10-01T00:00:00Z", "2026-10-01T06:00:00Z", "synthetic", "deleted at once",
			"reads no environment variable", "writes nothing to disk", "toposhift serve",
			"15 minutes", "services", "/v1/metrics",
		} {
			if !strings.Contains(out, want) {
				t.Errorf("demo %v: the help lacks %q", args, want)
			}
		}
		if strings.Contains(out, "--data-dir") && !strings.Contains(out, "no data directory") {
			t.Errorf("demo %v: the help names a data directory the demo does not have", args)
		}
	}
	if code, out, _ := cli(t, "help"); code != 0 || !strings.Contains(out, "  demo ") {
		t.Errorf("the top-level usage does not list demo: %q", out)
	}
}

// startDemo runs `demo --listen 127.0.0.1:0 args...` and waits until it listens.
func startDemo(t *testing.T, args ...string) *served {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	s := &served{stderr: &lockedBuffer{}, cancel: cancel, done: make(chan int, 1)}
	go func() {
		s.done <- run(ctx, append([]string{"demo", "--listen", "127.0.0.1:0"}, args...), noEnv, io.Discard, s.stderr)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case code := <-s.done:
			s.done <- code
		case <-time.After(15 * time.Second):
			t.Error("demo did not stop")
		}
	})
	deadline := time.After(30 * time.Second)
	for {
		if m := listeningRE.FindStringSubmatch(s.stderr.String()); m != nil {
			s.base = "http://" + m[1]
			// The URL the command prints is the bound address, not the requested one.
			if !strings.Contains(s.stderr.String(), "serving the tools at "+s.base+"/mcp (POST)") {
				t.Errorf("the printed URL is not the bound address %s:\n%s", m[1], s.stderr)
			}
			return s
		}
		select {
		case code := <-s.done:
			s.done <- code
			t.Fatalf("demo exited with %d before listening:\n%s", code, s.stderr)
		case <-deadline:
			t.Fatalf("demo did not listen:\n%s", s.stderr)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func asList(t testing.TB, v any, what string) []map[string]any {
	t.Helper()
	l, ok := v.([]any)
	if !ok {
		t.Fatalf("%s is %T, want a list", what, v)
	}
	var out []map[string]any
	for _, e := range l {
		m, _ := e.(map[string]any)
		out = append(out, m)
	}
	return out
}

func refName(m map[string]any) string {
	r, _ := m["entity"].(map[string]any)
	s, _ := r["name"].(string)
	return s
}

func refType(m map[string]any) string {
	r, _ := m["entity"].(map[string]any)
	s, _ := r["type"].(string)
	return s
}

// The range tools answer from the in-memory store, over HTTP on loopback, with the
// ids the story names: the blast radius of node-2 over the minutes it fails is its
// pods and their containers and no service, the co-residents of a pod on it share
// its node and its host, the diff of its pods over the eviction has deletions and
// creations, and an endpoint's id is printed nowhere.
func TestDemoHTTPAnswersRangeQuestions(t *testing.T) {
	story := demo.NewWorld(demo.DefaultSeed).Story()
	s := startDemo(t)
	rfc := func(tm time.Time) string { return tm.UTC().Format(time.RFC3339) }
	var texts []string
	call := func(name string, args map[string]any) map[string]any {
		t.Helper()
		res := mcpOK(t, s.base, name, args)
		b, _ := json.Marshal(res.Structured)
		texts = append(texts, res.Text, string(b))
		if res.IsError {
			t.Fatalf("%s: %s", name, res.Text)
		}
		return res.Structured
	}

	blast := call("blast_radius", map[string]any{
		"entity": story.Failed.ID, "from": rfc(story.NotReady), "to": rfc(story.NotReady.Add(30 * time.Minute)),
	})
	names := map[string]bool{}
	instances := 0
	for _, m := range asList(t, blast["members"], "members") {
		names[refName(m)] = true
		switch typ := refType(m); typ {
		case "k8s.pod", "container":
		case "service.instance":
			instances++
		default:
			// A node's failure reaches what runs on it, and the instances in its pods,
			// and no service: an instance is never followed to its service.
			t.Errorf("the blast radius of node-2 reaches a %s, want pods, containers and service instances only", typ)
		}
	}
	if instances == 0 {
		t.Error("the blast radius of node-2 reaches no service instance")
	}
	for _, v := range story.Evicted {
		if !names[v.Name] {
			t.Errorf("the blast radius of node-2 does not reach %s", v.Name)
		}
	}
	if len(names) < 2*(len(story.Evicted)+2) {
		t.Errorf("the blast radius has %d members, want the pods of node-2 and their containers", len(names))
	}

	co := call("co_residents", map[string]any{
		"pod": story.Checkout.ID, "from": rfc(story.Anchor), "to": rfc(story.NotReady),
	})
	mech := map[string]bool{}
	for _, c := range asList(t, co["co_residents"], "co_residents") {
		m, _ := c["mechanism"].(string)
		mech[m] = true
	}
	if !mech["shared_node"] || !mech["shared_host"] {
		t.Errorf("the checkout pod's co-residents are by %v, want a shared node and a shared host", mech)
	}

	var scope []string
	for i, v := range story.Evicted {
		scope = append(scope, v.ID, story.Replacements[i].ID)
	}
	diff := call("what_changed", map[string]any{
		"scope": scope, "from": rfc(story.Evict.Add(-10 * time.Minute)), "to": rfc(story.Evict.Add(10 * time.Minute)),
	})
	if len(asList(t, diff["deleted"], "deleted")) == 0 || len(asList(t, diff["created"], "created")) == 0 {
		t.Errorf("the diff over the eviction has %v deleted and %v created, want both", diff["deleted"], diff["created"])
	}

	nb := call("neighbors_at", map[string]any{"entity": story.Checkout.ID, "at": rfc(story.NotReady.Add(-time.Minute))})
	counts, _ := nb["counts"].(map[string]any)
	withheld, _ := counts["withheld"].(map[string]any)
	if edges, _ := withheld["edges"].(float64); edges < 1 {
		t.Errorf("neighbors_at of a pod withheld %v edges, want its address claim counted and not listed", withheld)
	}
	hist := call("entity_history", map[string]any{
		"entity": story.Evicted[0].ID, "from": rfc(story.Anchor), "to": rfc(story.End),
	})
	if hist["existence"] == nil {
		t.Errorf("entity_history of an evicted pod has no existence")
	}

	for _, tx := range texts {
		if strings.Contains(tx, "topo.endpoint:") {
			t.Errorf("an answer prints an endpoint id: %.200s", tx)
		}
	}
	if code := s.stop(t); code != 0 {
		t.Errorf("exit %d after the signal, want 0:\n%s", code, s.stderr)
	}
	if strings.Contains(s.stderr.String(), "topo.endpoint:") {
		t.Error("the log prints an endpoint id")
	}
}

// The tools' instructions carry the window, and only the tools are served: no
// receiver and no status.
func TestDemoServesOnlyTheToolsAndTheirInstructions(t *testing.T) {
	s := startDemo(t)
	body := `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"x","version":"0"}}}`
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, s.base+"/mcp", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(b), "2026-10-01T00:00:00Z") || !strings.Contains(string(b), "Call describe_schema first") {
		t.Errorf("initialize answered %d %.300s, want the server's instructions and the window", resp.StatusCode, b)
	}

	for _, tc := range []struct {
		method, path string
		want         int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodPost, "/v1/logs", http.StatusNotFound},
		{http.MethodPost, "/v1/metrics", http.StatusNotFound},
		{http.MethodGet, "/status", http.StatusNotFound},
	} {
		req, _ := http.NewRequestWithContext(t.Context(), tc.method, s.base+tc.path, strings.NewReader("{}"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != tc.want {
			t.Errorf("%s %s: %d, want %d", tc.method, tc.path, resp.StatusCode, tc.want)
		}
	}
}

// A port that is taken ends the command with status 1 and a sentence that says what
// to do.
func TestDemoOnATakenPort(t *testing.T) {
	first := startDemo(t)
	addr := strings.TrimPrefix(first.base, "http://")
	code, _, errs := demoRun(t, "--listen", addr)
	if code != 1 || !strings.Contains(errs, "give another address with --listen") {
		t.Errorf("exit %d, stderr:\n%s", code, errs)
	}
}

// Over stdio the tools answer one client, and what is written to stdout is the
// protocol's messages and nothing else.
func TestDemoStdioSmoke(t *testing.T) {
	story := demo.NewWorld(demo.DefaultSeed).Story()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	setSeam(t, &mcpStdin, io.Reader(inR))
	stdout, stderr := &lockedBuffer{}, &lockedBuffer{}
	done := make(chan int, 1)
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		code := run(t.Context(), []string{"demo", "--stdio"}, noEnv, io.MultiWriter(outW, stdout), stderr)
		_ = outW.Close()
		done <- code
	}()
	// Closing both pipes unblocks the command's reads and writes, and the test's
	// pending scan, so the command ends on every path; the test waits for it.
	t.Cleanup(func() {
		_ = inW.Close()
		_ = outR.Close()
		select {
		case <-exited:
		case <-time.After(30 * time.Second):
			t.Errorf("the demo did not end after its pipes were closed:\n%s", stderr)
		}
	})

	frames := bufio.NewScanner(outR)
	frames.Buffer(nil, 4<<20)
	send := func(line string) {
		t.Helper()
		if _, err := io.WriteString(inW, line+"\n"); err != nil {
			t.Fatal(err)
		}
	}
	reply := func() map[string]any {
		t.Helper()
		scanned := make(chan bool, 1)
		go func() { scanned <- frames.Scan() }()
		select {
		case ok := <-scanned:
			if !ok {
				t.Fatalf("no reply (%v); stderr:\n%s", frames.Err(), stderr)
			}
		case <-time.After(30 * time.Second):
			t.Fatalf("no reply within 30s; stderr:\n%s", stderr)
		}
		var m map[string]any
		if err := json.Unmarshal(frames.Bytes(), &m); err != nil {
			t.Fatalf("a reply that is not JSON: %.200q", frames.Text())
		}
		return m
	}
	send(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"x","version":"0"}}}`)
	initRes, _ := reply()["result"].(map[string]any)
	if ins, _ := initRes["instructions"].(string); !strings.Contains(ins, "2026-10-01T06:00:00Z") {
		t.Errorf("the instructions do not carry the window: %q", ins)
	}
	send(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	args, _ := json.Marshal(map[string]any{
		"entity": story.Failed.ID, "from": story.NotReady.Format(time.RFC3339), "to": story.NotReady.Add(30 * time.Minute).Format(time.RFC3339),
	})
	send(`{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"blast_radius","arguments":` + string(args) + `}}`)
	res, _ := reply()["result"].(map[string]any)
	if isErr, _ := res["isError"].(bool); isErr {
		t.Errorf("blast_radius over stdio failed: %v", res["content"])
	}
	if sc, _ := res["structuredContent"].(map[string]any); len(asList(t, sc["members"], "members")) == 0 {
		t.Errorf("blast_radius over stdio has no members: %v", sc)
	}

	// The tools a client is offered are the tools the package lists, each with an
	// input schema of type object.
	send(`{"jsonrpc":"2.0","id":3,"method":"tools/list"}`)
	list, _ := reply()["result"].(map[string]any)
	var offered, listed []string
	for _, tl := range asList(t, list["tools"], "tools") {
		name, _ := tl["name"].(string)
		offered = append(offered, name)
		if schema, _ := tl["inputSchema"].(map[string]any); schema["type"] != "object" {
			t.Errorf("tool %s has input schema %v, want one of type object", name, tl["inputSchema"])
		}
	}
	for _, tl := range mcp.Tools() {
		listed = append(listed, tl.Name)
	}
	slices.Sort(offered)
	slices.Sort(listed)
	if !slices.Equal(offered, listed) {
		t.Errorf("tools/list offers %v, want %v", offered, listed)
	}

	// The first tool an agent calls: from a name to ids. The answer is the one the
	// end-to-end test checks over HTTP, at the banner's token.
	o := newOracle(t)
	q := askedAbout(o)
	findArgs, _ := json.Marshal(q.findArgs())
	send(`{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"find_entities","arguments":` + string(findArgs) + `}}`)
	found, _ := reply()["result"].(map[string]any)
	if isErr, _ := found["isError"].(bool); isErr {
		t.Fatalf("find_entities over stdio failed: %v", found["content"])
	}
	structured, _ := found["structuredContent"].(map[string]any)
	got := reader{t, o}.find(structured, q.findArgs(), q.i1)
	got.Policy = nil
	if a, b := encode(t, got), encode(t, o.wantFind(q)); !bytes.Equal(a, b) {
		t.Errorf("find_entities over stdio differs from what the world says:\n%s", firstDifference(a, b))
	}
	if m := tokenInBanner.FindStringSubmatch(stderr.String()); m == nil || structured["token"] != m[1] {
		t.Errorf("find_entities token %v, want the banner's", structured["token"])
	}

	_ = inW.Close()
	select {
	case code := <-done:
		if code != 0 {
			t.Errorf("exit %d after the client closed its input:\n%s", code, stderr)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("demo did not end:\n%s", stderr)
	}
	if n := onlyFrames(t, stdout.String()); n != 4 {
		t.Errorf("%d messages on stdout, want the four replies", n)
	}
	if strings.Contains(stdout.String(), "topo.endpoint:") || strings.Contains(stderr.String(), "topo.endpoint:") {
		t.Error("an endpoint id is printed")
	}
}
