package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/demo"
)

// readmeText is the README at the root of the repository, the only file these
// tests read.
func readmeText(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile("../../README.md")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// readmeSection is the text under the heading "## name", up to the next "## ".
func readmeSection(t *testing.T, text, name string) string {
	t.Helper()
	_, rest, ok := strings.Cut(text, "\n## "+name+"\n")
	if !ok {
		t.Fatalf("the README has no section %q", name)
	}
	body, _, _ := strings.Cut(rest, "\n## ")
	return body
}

// shellWords splits one command line into words the way a shell does for the
// forms the README uses: words separated by spaces, single-quoted and
// double-quoted strings, and no expansion.
func shellWords(t *testing.T, line string) []string {
	t.Helper()
	var words []string
	var cur strings.Builder
	in := false
	var quote rune
	for _, r := range line {
		switch {
		case quote != 0:
			if r == quote {
				quote = 0
			} else {
				cur.WriteRune(r)
			}
		case r == '\'' || r == '"':
			quote = r
			in = true
		case r == ' ' || r == '\t':
			if in {
				words = append(words, cur.String())
				cur.Reset()
				in = false
			}
		default:
			cur.WriteRune(r)
			in = true
		}
	}
	if quote != 0 {
		t.Fatalf("an unclosed quote in %q", line)
	}
	if in {
		words = append(words, cur.String())
	}
	return words
}

// readmeCommands returns the commands in the fenced sh blocks of text, one per
// entry, with a line that ends in a backslash joined to the next.
func readmeCommands(text string) []string {
	var cmds []string
	inBlock := false
	var cur string
	for _, line := range strings.Split(text, "\n") {
		switch {
		case strings.HasPrefix(line, "```sh"):
			inBlock = true
		case strings.HasPrefix(line, "```"):
			inBlock = false
		case inBlock:
			if joined, ok := strings.CutSuffix(line, "\\"); ok {
				cur += strings.TrimSpace(joined) + " "
				continue
			}
			cur += strings.TrimSpace(line)
			if cur != "" {
				cmds = append(cmds, cur)
			}
			cur = ""
		}
	}
	return cmds
}

// readmeCurl is a curl command of the README, parsed.
type readmeCurl struct {
	method  string
	url     string
	headers [][2]string
	body    string
	tool    string
}

// parseCurl reads the options the README uses and refuses any other, so a new
// option is a decision about this test and not something it skips.
func parseCurl(t *testing.T, cmd string) readmeCurl {
	t.Helper()
	words := shellWords(t, cmd)
	var c readmeCurl
	for i := 1; i < len(words); i++ {
		w := words[i]
		next := func() string {
			i++
			if i >= len(words) {
				t.Fatalf("%q: %s has no value", cmd, w)
			}
			return words[i]
		}
		switch w {
		case "-s":
		case "-X":
			c.method = next()
		case "-H":
			name, value, ok := strings.Cut(next(), ":")
			if !ok {
				t.Fatalf("%q: a header without a colon", cmd)
			}
			c.headers = append(c.headers, [2]string{strings.TrimSpace(name), strings.TrimSpace(value)})
		case "-d":
			c.body = next()
		default:
			if !strings.HasPrefix(w, "http://") || c.url != "" {
				t.Fatalf("%q: the test does not know the argument %q", cmd, w)
			}
			c.url = w
		}
	}
	if c.url == "" {
		t.Fatalf("%q: no URL", cmd)
	}
	if c.method == "" {
		c.method = http.MethodGet
		if c.body != "" {
			c.method = http.MethodPost
		}
	}
	var rpc struct {
		Params struct {
			Name string `json:"name"`
		} `json:"params"`
	}
	if c.body != "" && json.Unmarshal([]byte(c.body), &rpc) == nil {
		c.tool = rpc.Params.Name
	}
	return c
}

// header returns the value the command gives a header, and whether it gives it.
func (c readmeCurl) header(name string) (string, bool) {
	for _, h := range c.headers {
		if strings.EqualFold(h[0], name) {
			return h[1], true
		}
	}
	return "", false
}

// Every `./bin/toposhift <word>` the README shows names a command that
// `toposhift help` lists.
func TestReadmeNamesOnlyCommandsThatExist(t *testing.T) {
	_, usageText, _ := cli(t, "help")
	listed := map[string]bool{}
	for _, m := range regexp.MustCompile(`(?m)^  (\w+) `).FindAllStringSubmatch(usageText, -1) {
		listed[m[1]] = true
	}
	if !listed["serve"] || !listed["demo"] {
		t.Fatalf("the usage lists no serve or demo:\n%s", usageText)
	}
	shown := 0
	for _, cmd := range readmeCommands(readmeText(t)) {
		w := strings.Fields(cmd)
		if len(w) < 2 || w[0] != "./bin/toposhift" {
			continue
		}
		shown++
		if !listed[w[1]] {
			t.Errorf("the README runs %q, and %q is not a command in the usage", cmd, w[1])
		}
	}
	if shown == 0 {
		t.Error("the README shows no ./bin/toposhift command")
	}
}

// The curl commands of "Try it without a cluster" are sent to a demo exactly as
// written: their method, their headers and their body. Each answers 200 with
// isError false and a non-empty list, the first names node-2, and the node id and
// the instants the README quotes are the ones the demo's story has, so a change
// to the demo that moves one fails here and the README is edited with it.
func TestReadmeCurlCommandsAnswer(t *testing.T) {
	text := readmeText(t)
	story := demo.NewWorld(demo.DefaultSeed).Story()
	rfc := func(tm time.Time) string { return tm.UTC().Format(time.RFC3339) }

	// Every instant the section quotes is one of the demo's story: the ends of its
	// history, the minute node-2 stops being ready, the minute before, and the end
	// of the half hour the blast radius asks about.
	known := map[string]bool{}
	for _, tm := range []time.Time{
		story.Anchor, story.End, story.NotReady,
		story.NotReady.Add(-time.Minute), story.NotReady.Add(30 * time.Minute),
	} {
		known[rfc(tm)] = true
	}
	quoted := regexp.MustCompile(`\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ`).FindAllString(readmeSection(t, text, "Try it without a cluster"), -1)
	if len(quoted) == 0 {
		t.Error("the section quotes no instant")
	}
	for _, in := range quoted {
		if !known[in] {
			t.Errorf("the README quotes the instant %s, which is not one of the demo's story instants %v", in, known)
		}
	}

	if strings.Contains(text, "k8s.pod:") {
		t.Error("the README quotes a pod id; the demo's pods may move, so only node ids and instants belong in it")
	}
	for _, m := range regexp.MustCompile(`k8s\.node:[0-9a-f]+`).FindAllString(text, -1) {
		if m != story.Failed.ID {
			t.Errorf("the README quotes the node id %s, and the demo's node-2 is %s", m, story.Failed.ID)
		}
	}

	var curls []readmeCurl
	for _, cmd := range readmeCommands(readmeSection(t, text, "Try it without a cluster")) {
		if strings.HasPrefix(cmd, "curl ") {
			curls = append(curls, parseCurl(t, cmd))
		}
	}
	if len(curls) != 3 {
		t.Fatalf("the section shows %d curl commands, want 3", len(curls))
	}

	s := startDemo(t)
	bound, err := url.Parse(s.base)
	if err != nil {
		t.Fatal(err)
	}
	// lists is the field of each answer that holds its list.
	lists := map[string]string{"find_entities": "matches", "neighbors_at": "edges", "blast_radius": "members"}
	var nodeID string
	for i, c := range curls {
		if want := []string{"find_entities", "neighbors_at", "blast_radius"}[i]; c.tool != want {
			t.Fatalf("curl %d calls %q, want %q", i+1, c.tool, want)
		}
		// The two headers the server needs are in the command, spelled as the
		// SDK's stateless path reads them.
		if v, ok := c.header("Content-Type"); !ok || v != "application/json" {
			t.Errorf("curl %d: Content-Type is %q (given %v)", i+1, v, ok)
		}
		if v, ok := c.header("Accept"); !ok || !strings.Contains(v, "application/json") || !strings.Contains(v, "text/event-stream") {
			t.Errorf("curl %d: Accept is %q (given %v), want it to list application/json and text/event-stream", i+1, v, ok)
		}

		u, err := url.Parse(c.url)
		if err != nil || u.Path != "/mcp" {
			t.Fatalf("curl %d: the URL %q is not an /mcp URL", i+1, c.url)
		}
		u.Host = bound.Host
		req, err := http.NewRequestWithContext(t.Context(), c.method, u.String(), strings.NewReader(c.body))
		if err != nil {
			t.Fatal(err)
		}
		for _, h := range c.headers {
			req.Header.Set(h[0], h[1])
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("curl %d: %v", i+1, err)
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("curl %d (%s): status %d: %s", i+1, c.tool, resp.StatusCode, b)
		}
		var rpc struct {
			Result struct {
				IsError    bool           `json:"isError"`
				Structured map[string]any `json:"structuredContent"`
			} `json:"result"`
			Error any `json:"error"`
		}
		if err := json.NewDecoder(bytes.NewReader(b)).Decode(&rpc); err != nil || rpc.Error != nil {
			t.Fatalf("curl %d (%s): %v, error %v: %s", i+1, c.tool, err, rpc.Error, b)
		}
		if rpc.Result.IsError {
			t.Fatalf("curl %d (%s): isError is true: %s", i+1, c.tool, b)
		}
		list, ok := rpc.Result.Structured[lists[c.tool]].([]any)
		if !ok || len(list) == 0 {
			t.Fatalf("curl %d (%s): %q is %v, want a non-empty list: %s", i+1, c.tool, lists[c.tool], rpc.Result.Structured[lists[c.tool]], b)
		}
		if d, _ := rpc.Result.Structured["derived"].(bool); !d {
			t.Errorf("curl %d (%s): the answer is not marked derived", i+1, c.tool)
		}

		var call struct {
			Params struct {
				Arguments map[string]string `json:"arguments"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(c.body), &call); err != nil {
			t.Fatalf("curl %d: %v", i+1, err)
		}
		args := call.Params.Arguments
		switch i {
		case 0:
			if args["at"] != rfc(story.NotReady.Add(-time.Minute)) {
				t.Errorf("find_entities asks at %s, a minute before node-2 stops being ready is %s", args["at"], rfc(story.NotReady.Add(-time.Minute)))
			}
			for _, e := range asList(t, list, "matches") {
				if refName(e) == "node-2" {
					nodeID, _ = e["entity"].(map[string]any)["id"].(string)
				}
			}
			if nodeID == "" || nodeID != story.Failed.ID {
				t.Fatalf("find_entities does not name node-2 with the story's id %s: %s", story.Failed.ID, b)
			}
		case 1:
			if args["entity"] != nodeID || args["at"] != rfc(story.NotReady.Add(-time.Minute)) {
				t.Errorf("neighbors_at asks %v, want %s at %s", args, nodeID, rfc(story.NotReady.Add(-time.Minute)))
			}
		case 2:
			if args["entity"] != nodeID || args["from"] != rfc(story.NotReady) || args["to"] != rfc(story.NotReady.Add(30*time.Minute)) {
				t.Errorf("blast_radius asks %v, want %s from %s to %s", args, nodeID, rfc(story.NotReady), rfc(story.NotReady.Add(30*time.Minute)))
			}
		}
	}
}
