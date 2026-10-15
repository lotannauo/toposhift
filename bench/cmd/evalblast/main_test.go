package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

// An invented scenario in the format of the dataset's exports: a pod fault on the
// payment pod, which the cart calls.

const truth = `
fault:
  - entity: {name: pay-pods, kind: Pod, group_id: pay-pods}
    category: Change
    fault_mechanism: custom
groups:
  - {id: pay-pods, kind: Pod, namespace: shop, filter: ["shop-pay-.*"], root_cause: true}
  - {id: cart-svc, kind: Service, namespace: shop, filter: ['shop-cart\b']}
propagations:
  - {source: pay-pods, target: cart-svc}
`

const created = "2026-03-04T08:00:00Z"

func field(s string) string {
	if strings.ContainsAny(s, "\t\n\"") {
		return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
	}
	return s
}

func line(cols ...string) string {
	for i := range cols {
		cols[i] = field(cols[i])
	}
	return strings.Join(cols, "\t") + "\n"
}

func objectRow(resource, body string) string {
	ts := "2026-03-04 10:12:00.250000000"
	return line(ts, ts[:19], "", "", "0", "", "0", "", body, "", "{}", "", "s", "1", "{}", fmt.Sprintf("{'k8s.resource.name': '%s'}", resource), "")
}

func pod(uid, name, component string) string {
	return fmt.Sprintf(`{"kind":"Pod","metadata":{"uid":%q,"name":%q,"namespace":"shop","creationTimestamp":%q,"labels":{"app.kubernetes.io/component":%q}},"spec":{"nodeName":"node-a"},"status":{}}`, uid, name, created, component)
}

func objects() string {
	head := line("Timestamp", "TimestampTime", "TraceId", "SpanId", "TraceFlags", "SeverityText", "SeverityNumber", "ServiceName", "Body",
		"ResourceSchemaUrl", "ResourceAttributes", "ScopeSchemaUrl", "ScopeName", "ScopeVersion", "ScopeAttributes", "LogAttributes", "EventName")
	return head +
		objectRow("nodes", fmt.Sprintf(`{"kind":"Node","metadata":{"uid":"node-uid-a","name":"node-a","creationTimestamp":%q}}`, created)) +
		objectRow("pods", pod("pod-pay-1", "shop-pay-1", "shop-pay")) +
		objectRow("pods", pod("pod-cart-1", "shop-cart-1", "shop-cart")) +
		objectRow("services", fmt.Sprintf(`{"kind":"Service","metadata":{"uid":"svc-cart","name":"shop-cart","namespace":"shop","creationTimestamp":%q},"spec":{"selector":{"app.kubernetes.io/component":"shop-cart"}}}`, created))
}

func resource(service, pod string) string {
	return fmt.Sprintf("{'k8s.pod.uid': '%s', 'service.instance.id': '%s', 'service.name': '%s', 'service.namespace': 'shop-app'}", pod, pod, service)
}

func traces() string {
	cols := []string{
		"Timestamp", "TraceId", "SpanId", "ParentSpanId", "TraceState", "SpanName", "SpanKind", "ServiceName", "ResourceAttributes",
		"ScopeName", "ScopeVersion", "SpanAttributes", "Duration", "StatusCode", "StatusMessage", "Events.Timestamp", "Events.Name",
		"Events.Attributes", "Links.TraceId", "Links.SpanId", "Links.TraceState", "Links.Attributes",
	}
	out := line(cols...)
	span := func(at time.Time, trace, id, parent, kind, service, res string) string {
		return line(at.Format("2006-01-02 15:04:05.000000000"), trace, id, parent, "", "op", kind, service, res, "s", "1", "{}", "1", "Unset", "", "[]", "[]", "[]", "[]", "[]", "[]", "[]")
	}
	base := time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)
	for m := 0; m < 15; m++ {
		at := base.Add(time.Duration(m)*time.Minute + 2*time.Second)
		tr := fmt.Sprintf("t%02d", m)
		out += span(at, tr, tr+"c", "", "Client", "shop-cart", resource("shop-cart", "pod-cart-1"))
		out += span(at, tr, tr+"s", tr+"c", "Server", "shop-pay", resource("shop-pay", "pod-pay-1"))
	}
	return out
}

func writeScenario(t *testing.T, root string, n int) {
	t.Helper()
	dir := filepath.Join(root, fmt.Sprintf("Scenario-%d", n))
	if err := os.MkdirAll(filepath.Join(dir, "alerts"), 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"ground_truth.yaml": truth,
		"alerts/alerts_at_2026-03-04T10-15-00.000000.json": `[{"state":"firing","activeAt":"2026-03-04T10:05:00Z"}]`,
		ObjectsFile: objects(),
		TracesFile:  traces(),
	} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

const (
	ObjectsFile = "k8s_objects_raw.tsv"
	TracesFile  = "otel_traces_raw.tsv"
)

func invoke(args ...string) (code int, stdout, stderr string) {
	var o, e bytes.Buffer
	code = run(args, &o, &e)
	return code, o.String(), e.String()
}

var hashLine = regexp.MustCompile(`(?:table )?([AB]|services) sha256 ([0-9a-f]{64})`)

func hashOf(t *testing.T, stdout, table string) string {
	t.Helper()
	for _, m := range hashLine.FindAllStringSubmatch(stdout, -1) {
		if m[1] == table {
			return m[2]
		}
	}
	t.Fatalf("no sha256 of table %s in:\n%s", table, stdout)
	return ""
}

func TestStagesWriteTheirTablesOutsideTheRepository(t *testing.T) {
	data := t.TempDir()
	writeScenario(t, data, 1)
	writeScenario(t, data, 2)
	out := t.TempDir()

	code, stdout, stderr := invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "truth")
	if code != 0 {
		t.Fatalf("truth: %d %s", code, stderr)
	}
	a, err := os.ReadFile(filepath.Join(out, "table_a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(stdout, string(a)) || !strings.Contains(stdout, "Scenario-1") || !strings.Contains(stdout, "Scenario-2") || !strings.Contains(stdout, "seed:     pod") {
		t.Errorf("table A:\n%s", stdout)
	}
	sumA := hashOf(t, stdout, "A")
	if sum(string(a)) != sumA {
		t.Error("the sha256 printed is not that of the file")
	}
	if _, err := os.Stat(filepath.Join(out, "report.json")); err == nil {
		t.Error("the truth stage wrote a report")
	}

	// Table A over scenario 2 alone is another table: its own hash.
	code, stdout, _ = invoke("-source", "itbench", "-data", data, "-out", t.TempDir(), "-stage", "truth", "-scenarios", "2")
	if code != 0 || hashOf(t, stdout, "A") == sumA {
		t.Errorf("a subset has the hash of the whole (exit %d)", code)
	}

	code, stdout, stderr = invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "map", "-table-a", sumA)
	if code != 0 {
		t.Fatalf("map: %d %s", code, stderr)
	}
	if !strings.Contains(stdout, `seed:     service "shop-pay"`) || !strings.Contains(stdout, `services [shop-cart]`) || !strings.Contains(stdout, "unmappable truth groups by kind: none") {
		t.Errorf("table B:\n%s", stdout)
	}
	if _, err := os.Stat(filepath.Join(out, "table_b.txt")); err != nil {
		t.Error(err)
	}
	hashOf(t, stdout, "B")
}

func tables(t *testing.T, data string) (a, b, services string) {
	t.Helper()
	out := t.TempDir()
	_, stdout, _ := invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "truth")
	a = hashOf(t, stdout, "A")
	_, stdout, _ = invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "map", "-table-a", a)
	b = hashOf(t, stdout, "B")
	_, stdout, _ = invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "pool")
	return a, b, hashOf(t, stdout, "services")
}

func TestPoolIsTheUnionOfTracesAndProbeNames(t *testing.T) {
	data := t.TempDir()
	writeScenario(t, data, 1) // spans of shop-cart and shop-pay
	out := t.TempDir()
	code, stdout, stderr := invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "pool")
	if code != 0 {
		t.Fatalf("pool: %d %s", code, stderr)
	}
	file, err := os.ReadFile(filepath.Join(out, "services_pooled.txt"))
	if err != nil || string(file) != "shop-cart\nshop-pay\n" || sum(string(file)) != hashOf(t, stdout, "services") {
		t.Fatalf("pooled file %q, %v", file, err)
	}
	// A probe names file of a scenario with no export adds its names; the set only grows.
	if err := os.MkdirAll(filepath.Join(data, "probe"), 0o755); err != nil {
		t.Fatal(err)
	}
	probe := filepath.Join(data, "probe", "Scenario-9.services")
	if err := os.WriteFile(probe, []byte("shop-front\nshop-pay\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, stdout, _ = invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "pool")
	file, _ = os.ReadFile(filepath.Join(out, "services_pooled.txt"))
	if code != 0 || string(file) != "shop-cart\nshop-front\nshop-pay\n" || sum(string(file)) != hashOf(t, stdout, "services") {
		t.Errorf("pooled file %q (exit %d)", file, code)
	}
	// A names file that is not sorted and unique is refused.
	if err := os.WriteFile(probe, []byte("shop-pay\nshop-front\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if code, _, _ := invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "pool"); code != 1 {
		t.Errorf("an unsorted names file: exit %d, want 1", code)
	}
}

func TestScoreWritesAReportThatNamesNoEntity(t *testing.T) {
	data := t.TempDir()
	writeScenario(t, data, 1)
	// A scenario that cannot be read stays in the count and is listed.
	if err := os.MkdirAll(filepath.Join(data, "Scenario-2"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(data, "Scenario-2", "ground_truth.yaml"), []byte("fault: [\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	a, b, svc := tables(t, data)
	out := t.TempDir()
	code, stdout, stderr := invoke("-source", "itbench", "-data", data, "-out", out, "-stage", "score", "-table-a", a, "-table-b", b, "-services", svc)
	if code != 0 {
		t.Fatalf("score: %d %s", code, stderr)
	}
	for _, want := range []string{
		"dataset itbench-lite@d0916b0, loader factload-1", "policy versions: blastradius", "scenarios in the count: 2; not fully loaded: 1",
		"not fully loaded: Scenario-2: unreadable_ground_truth", "1 with unreadable labels", "address-associated", "service names with a span in the trace data read: 2",
	} {
		if !strings.Contains(stdout, want) {
			t.Errorf("the report lacks %q:\n%s", want, stdout)
		}
	}
	for _, banned := range []string{"µs", "ms ", " ns", "elapsed", "seconds"} {
		if strings.Contains(stdout, banned) {
			t.Errorf("the report prints a timing (%q)", banned)
		}
	}
	js, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	// Results hold numbers keyed by case id, and no name, uid, label or time.
	for _, banned := range []string{"shop", "pod-", "node-", "2026-03", "10:"} {
		if strings.Contains(string(js), banned) {
			t.Errorf("report.json holds %q", banned)
		}
	}
	if !strings.Contains(string(js), `"Scenario-1"`) || !strings.Contains(string(js), `"Scenario-2"`) {
		t.Error("report.json lacks a case")
	}
}

func TestOrderOfStagesCannotBeSkipped(t *testing.T) {
	data := t.TempDir()
	writeScenario(t, data, 1)
	a, b, svc := tables(t, data)
	zero := strings.Repeat("0", 64)
	base := []string{"-source", "itbench", "-data", data}
	for _, c := range []struct {
		name string
		args []string
	}{
		{"no stage", nil},
		{"map without table A", []string{"-stage", "map"}},
		{"map with a malformed hash", []string{"-stage", "map", "-table-a", "abc"}},
		{"map with another table A", []string{"-stage", "map", "-table-a", zero}},
		{"score without any hash", []string{"-stage", "score"}},
		{"score without table B", []string{"-stage", "score", "-table-a", a, "-services", svc}},
		{"score without the services", []string{"-stage", "score", "-table-a", a, "-table-b", b}},
		{"score with another table B", []string{"-stage", "score", "-table-a", a, "-table-b", zero, "-services", svc}},
		{"score with another table A", []string{"-stage", "score", "-table-a", zero, "-table-b", b, "-services", svc}},
		{"score with another set of services", []string{"-stage", "score", "-table-a", a, "-table-b", b, "-services", zero}},
		{"score on a subset", []string{"-stage", "score", "-table-a", a, "-table-b", b, "-services", svc, "-scenarios", "1"}},
		{"pool on a subset", []string{"-stage", "pool", "-scenarios", "1"}},
	} {
		out := t.TempDir()
		code, _, _ := invoke(append(append(slices.Clone(base), "-out", out), c.args...)...)
		if code != 2 {
			t.Errorf("%s: exit %d, want 2", c.name, code)
		}
		if _, err := os.Stat(filepath.Join(out, "report.json")); err == nil {
			t.Errorf("%s: a report was written", c.name)
		}
	}
}

func TestUsageAndRefusals(t *testing.T) {
	data := t.TempDir()
	repo := t.TempDir()
	if err := os.Mkdir(filepath.Join(repo, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name string
		args []string
		code int
	}{
		{"no source", []string{"-data", data, "-out", t.TempDir(), "-stage", "truth"}, 2},
		{"another source", []string{"-source", "openrca", "-data", data, "-out", t.TempDir(), "-stage", "truth"}, 2},
		{"no data", []string{"-source", "itbench", "-out", t.TempDir(), "-stage", "truth"}, 2},
		{"no out", []string{"-source", "itbench", "-data", data, "-stage", "truth"}, 2},
		{"a stage that is not one", []string{"-source", "itbench", "-data", data, "-out", t.TempDir(), "-stage", "everything"}, 2},
		{"a scenario that is not a number", []string{"-source", "itbench", "-data", data, "-out", t.TempDir(), "-stage", "truth", "-scenarios", "1,x"}, 2},
		{"an output inside a repository", []string{"-source", "itbench", "-data", data, "-out", filepath.Join(repo, "out", "deeper"), "-stage", "truth"}, 2},
		{"an unknown flag", []string{"-nope"}, 2},
	} {
		if code, _, _ := invoke(c.args...); code != c.code {
			t.Errorf("%s: exit %d, want %d", c.name, code, c.code)
		}
	}
	if _, err := os.Stat(filepath.Join(repo, "out")); err == nil {
		t.Error("a directory was made inside the repository")
	}
}
