// Package otelcollector holds no code. Its test checks the collector
// configurations shipped beside it, toposhift.yaml with its two opt-in forms and
// the optional nodeagent.yaml and tracemetrics.yaml, and the README that explains
// them.
//
// The repository has no YAML module, so the test reads the file with the
// standard library. It understands only the small subset of YAML this file uses
// (block mappings, block lists, plain scalars, whole-line comments) and fails on
// anything else. It catches a scalar path that appears twice, but not a mapping
// key repeated with different children (two "extensions:" blocks, say). That is
// weaker than parsing the file: it does not know that the collector accepts it. The collector's own validate
// command is the check for that, and it is run by hand, not here.
package otelcollector

import (
	"fmt"
	"maps"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
)

// doc is the file flattened to one entry per scalar, keyed by its path. A
// mapping key adds ".key" and a list item adds "[n]", counted from 0, so the two
// pull objects of the receiver are "...objects[2]" and "...objects[3]" and a
// value under one cannot be taken for the other.
type doc map[string]string

// frame is an open level of the file. A key frame is a "key:" with nothing after
// it, and holds what is indented deeper than the key. A map frame is the mapping
// of one list item, whose keys sit at its indent.
type frame struct {
	isMap  bool
	indent int
	path   string
	items  int // list items seen so far, for a key frame that holds a list
	child  int // indent of the first thing under a key frame; 0 until there is one
}

func parse(text string) (doc, error) {
	out := doc{}
	var err error
	fail := func(lineNo int, format string, args ...any) {
		if err == nil {
			err = fmt.Errorf("line %d: %s", lineNo, fmt.Sprintf(format, args...))
		}
	}
	stack := []*frame{{isMap: true, indent: 0}}
	top := func() *frame { return stack[len(stack)-1] }
	addKey := func(lineNo, indent int, k, v string) {
		// Pop what the line closes: a key frame at or past this indent, a map
		// frame deeper than it.
		for len(stack) > 1 && ((!top().isMap && top().indent >= indent) || (top().isMap && top().indent > indent)) {
			stack = stack[:len(stack)-1]
		}
		parent := top()
		if parent.isMap != (parent.indent == indent) || (!parent.isMap && !sameChild(parent, indent)) {
			fail(lineNo, "indentation does not match the line above")
			return
		}
		path := k
		if parent.path != "" {
			path = parent.path + "." + k
		}
		if v == "" {
			stack = append(stack, &frame{indent: indent, path: path})
			return
		}
		if _, dup := out[path]; dup {
			fail(lineNo, "%s appears twice", path)
		}
		out[path] = v
	}
	for i, raw := range strings.Split(text, "\n") {
		lineNo := i + 1
		if strings.Contains(raw, "\t") {
			fail(lineNo, "tab character")
		}
		line := strings.TrimRight(raw, " ")
		trimmed := strings.TrimLeft(line, " ")
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.Contains(trimmed, " #") {
			fail(lineNo, "inline comment is not supported by this check")
		}
		if strings.ContainsAny(trimmed[:1], "{[&*!|>'\"") {
			fail(lineNo, "%q is outside the YAML this check understands", trimmed)
		}
		indent := len(line) - len(trimmed)

		if trimmed != "-" && !strings.HasPrefix(trimmed, "- ") {
			k, v, ok := splitKey(trimmed)
			if !ok {
				fail(lineNo, "%q is not a key", trimmed)
				continue
			}
			addKey(lineNo, indent, k, v)
			continue
		}

		// A list item. Its dash must be deeper than the key that holds the list.
		if t := top(); !t.isMap && t.indent == indent {
			fail(lineNo, "a list item at the indent of its key is not supported by this check")
			continue
		}
		for len(stack) > 1 && ((!top().isMap && top().indent >= indent) || (top().isMap && top().indent > indent)) {
			stack = stack[:len(stack)-1]
		}
		list := top()
		if list.isMap {
			fail(lineNo, "a list item outside a list")
			continue
		}
		if !sameChild(list, indent) {
			fail(lineNo, "indentation does not match the line above")
			continue
		}
		itemPath := list.path + "[" + strconv.Itoa(list.items) + "]"
		list.items++
		rest := strings.TrimSpace(strings.TrimPrefix(trimmed, "-"))
		if rest == "" {
			fail(lineNo, "a bare list item is not supported by this check")
			continue
		}
		if k, v, ok := splitKey(rest); ok {
			// "- key: value" opens a mapping whose other keys sit two columns in.
			stack = append(stack, &frame{isMap: true, indent: indent + 2, path: itemPath})
			addKey(lineNo, indent+2, k, v)
		} else {
			out[itemPath] = rest
		}
	}
	return out, err
}

// sameChild records the indent of a key frame's first child and reports whether
// indent is that indent.
func sameChild(f *frame, indent int) bool {
	if f.child == 0 {
		f.child = indent
	}
	return f.child == indent
}

// splitKey splits "key: value" or "key:" at the first colon that ends the key.
func splitKey(s string) (key, value string, ok bool) {
	if strings.HasSuffix(s, ":") {
		return strings.TrimSuffix(s, ":"), "", true
	}
	k, v, found := strings.Cut(s, ": ")
	if !found {
		return "", "", false
	}
	return k, strings.TrimSpace(v), true
}

func read(t *testing.T, name string) string {
	t.Helper()
	b, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestParse(t *testing.T) {
	d, err := parse("a:\n  b: 1\n  c:\n    - x: 2\n      y: 3\n    - x: 4\n  e:\n    - p\n    - q\nz: 5\n")
	if err != nil {
		t.Fatal(err)
	}
	want := doc{"a.b": "1", "a.c[0].x": "2", "a.c[0].y": "3", "a.c[1].x": "4", "a.e[0]": "p", "a.e[1]": "q", "z": "5"}
	if len(d) != len(want) {
		t.Fatalf("got %v, want %v", d, want)
	}
	for k, v := range want {
		if d[k] != v {
			t.Errorf("%s = %q, want %q", k, d[k], v)
		}
	}
}

func TestParseRefusesWhatItDoesNotUnderstand(t *testing.T) {
	for name, text := range map[string]string{
		"duplicate key":  "a:\n  b: 1\n  b: 2\n",
		"duplicate item": "a:\n  - b: 1\n    b: 2\n",
		"tab":            "a:\n\tb: 1\n",
		"flow mapping":   "a: 1\n{b: 2}\n",
		"inline comment": "a: 1 # note\n",
		"bad indent":     "a:\n  b: 1\n   c: 2\n",
		"dash at key":    "a:\n- b: 1\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parse(text); err == nil {
				t.Fatalf("%q was accepted", text)
			}
		})
	}
}

func TestConfig(t *testing.T) {
	text := read(t, "toposhift.yaml")
	d, err := parse(text)
	if err != nil {
		t.Fatal(err)
	}

	eq := func(path, want string) {
		t.Helper()
		got, ok := d[path]
		if !ok {
			t.Errorf("%s is missing", path)
			return
		}
		if got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	match := func(path string, re *regexp.Regexp) {
		t.Helper()
		got, ok := d[path]
		if !ok {
			t.Errorf("%s is missing", path)
			return
		}
		if !re.MatchString(got) {
			t.Errorf("%s = %q, want a match for %s", path, got, re)
		}
	}

	// The Kubernetes objects: each of pods and nodes is read once in watch mode and
	// once in pull mode, and each pull object has its own interval.
	const objects = "receivers.k8s_objects.objects"
	eq("receivers.k8s_objects.auth_type", "serviceAccount")
	type seen struct{ watch, pull int }
	kinds := map[string]*seen{"pods": {}, "nodes": {}}
	n := 0
	for ; ; n++ {
		item := objects + "[" + strconv.Itoa(n) + "]"
		name, ok := d[item+".name"]
		if !ok {
			break
		}
		k := kinds[name]
		if k == nil {
			t.Errorf("%s reads %q, which this configuration does not translate", item, name)
			continue
		}
		switch d[item+".mode"] {
		case "watch":
			k.watch++
			if _, has := d[item+".interval"]; has {
				t.Errorf("%s is a watch and has an interval", item)
			}
		case "pull":
			k.pull++
			eq(item+".interval", "5m")
		default:
			t.Errorf("%s has mode %q", item, d[item+".mode"])
		}
	}
	if n != 4 {
		t.Errorf("k8s_objects reads %d objects, want 4", n)
	}
	for name, k := range kinds {
		if k.watch != 1 || k.pull != 1 {
			t.Errorf("%s is read in watch mode %d times and in pull mode %d times, want once each", name, k.watch, k.pull)
		}
	}

	eq("receivers.k8s_cluster.auth_type", "serviceAccount")
	eq("receivers.k8s_cluster.metadata_collection_interval", "5m")

	// No port is open by default: an otlp receiver would take records from anything
	// that can reach the collector, and the pipeline routes them by what they are.
	// The node agents' receiver and the trace collector's are the opt-in steps of
	// the README, in comments.
	for k := range d {
		if strings.HasPrefix(k, "receivers.otlp") {
			t.Errorf("%s is set: the node agents' and the trace collector's receivers are off by default", k)
		}
	}
	without := strings.ReplaceAll(text, optInPrefix+"        endpoint: 0.0.0.0:4318", "")
	without = strings.ReplaceAll(without, traceMetricsPrefix+"        endpoint: 0.0.0.0:4319", "")
	if strings.Contains(without, "0.0.0.0") {
		t.Error("toposhift.yaml names 0.0.0.0 outside the two opt-in lines")
	}

	// The cluster name: the one value a user sets, in a placeholder that the README
	// explains.
	placeholder := regexp.MustCompile(`^<[A-Z][A-Z_]*>$`)
	eq("processors.resource.attributes[0].key", "k8s.cluster.name")
	match("processors.resource.attributes[0].value", placeholder)
	eq("processors.resource.attributes[0].action", "upsert")

	const ex = "exporters.otlphttp/toposhift"
	match(ex+".endpoint", regexp.MustCompile(`^http://<[A-Z][A-Z_]*>:7070$`))
	eq(ex+".encoding", "proto")
	eq(ex+".compression", "gzip")
	eq(ex+".timeout", "30s")
	eq(ex+".retry_on_failure.enabled", "true")
	eq(ex+".retry_on_failure.initial_interval", "5s")
	eq(ex+".retry_on_failure.max_interval", "30s")
	eq(ex+".retry_on_failure.max_elapsed_time", "0")

	const q = ex + ".sending_queue"
	eq(q+".enabled", "true")
	eq(q+".storage", "file_storage/toposhift")
	eq(q+".num_consumers", "1")
	eq(q+".sizer", "bytes")
	eq(q+".queue_size", "2000000000")
	if _, has := d[q+".block_on_overflow"]; has {
		t.Errorf("%s.block_on_overflow is set: a full queue is meant to drop", q)
	}
	eq(q+".batch.sizer", "bytes")
	eq(q+".batch.max_size", "16000000")
	eq(q+".batch.flush_timeout", "1s")

	const fs = "extensions.file_storage/toposhift"
	match(fs+".directory", regexp.MustCompile(`^/`))
	eq(fs+".create_directory", "true")
	eq(fs+".fsync", "true")
	eq(fs+".compaction.on_start", "true")
	eq(fs+".compaction.on_rebound", "true")
	eq(fs+".compaction.directory", d[fs+".directory"])

	// The pipeline: the extension is loaded, and there is one logs pipeline.
	eq("service.extensions[0]", "file_storage/toposhift")
	eq("service.pipelines.logs.receivers[0]", "k8s_objects")
	eq("service.pipelines.logs.receivers[1]", "k8s_cluster")
	if v, has := d["service.pipelines.logs.receivers[2]"]; has {
		t.Errorf("the pipeline has a third receiver, %q: the node agents' port is opt-in and off by default", v)
	}
	eq("service.pipelines.logs.processors[0]", "resource")
	eq("service.pipelines.logs.exporters[0]", "otlphttp/toposhift")

	// Nothing else is in the file: every key is one the checks above looked at.
	// A key added later has to be added to this test, with the reason it is set.
	known := map[string]bool{
		"receivers.k8s_objects.auth_type":                    true,
		"receivers.k8s_cluster.auth_type":                    true,
		"receivers.k8s_cluster.metadata_collection_interval": true,
		"processors.resource.attributes[0].key":              true,
		"processors.resource.attributes[0].value":            true,
		"processors.resource.attributes[0].action":           true,
		"service.extensions[0]":                              true,
		"service.pipelines.logs.receivers[0]":                true,
		"service.pipelines.logs.receivers[1]":                true,
		"service.pipelines.logs.processors[0]":               true,
		"service.pipelines.logs.exporters[0]":                true,
		ex + ".endpoint":                                     true,
		ex + ".encoding":                                     true,
		ex + ".compression":                                  true,
		ex + ".timeout":                                      true,
		ex + ".retry_on_failure.enabled":                     true,
		ex + ".retry_on_failure.initial_interval":            true,
		ex + ".retry_on_failure.max_interval":                true,
		ex + ".retry_on_failure.max_elapsed_time":            true,
		q + ".enabled":                                       true,
		q + ".storage":                                       true,
		q + ".num_consumers":                                 true,
		q + ".sizer":                                         true,
		q + ".queue_size":                                    true,
		q + ".batch.sizer":                                   true,
		q + ".batch.max_size":                                true,
		q + ".batch.flush_timeout":                           true,
		fs + ".directory":                                    true,
		fs + ".create_directory":                             true,
		fs + ".fsync":                                        true,
		fs + ".compaction.on_start":                          true,
		fs + ".compaction.on_rebound":                        true,
		fs + ".compaction.directory":                         true,
	}
	var extra []string
	for k := range d {
		if known[k] || strings.HasPrefix(k, objects+"[") {
			continue
		}
		extra = append(extra, k)
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("keys no check looks at: %v", extra)
	}
}

// optInPrefix starts each line of toposhift.yaml that is the node agents' opt-in: the
// receiver and its entry in the pipeline, commented out. Removing the prefix, with
// the one space after it, gives the line as it is when enabled.
const optInPrefix = "#NODEAGENTS "

// optInLines are the lines that carry the prefix, without it.
func optInLines(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, optInPrefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

// TestNodeAgentOptIn checks the opt-in form of toposhift.yaml: the commented lines
// are the node agents' otlp receiver (HTTP, on port 4318) and its entry in the one
// logs pipeline; uncommented, the file still parses, and it has the one pipeline,
// the one exporter and the one in-order queue it has without them; and the README
// shows the same lines, and says what enabling them opens.
func TestNodeAgentOptIn(t *testing.T) {
	text := read(t, "toposhift.yaml")
	lines := optInLines(text)
	want := []string{
		"  otlp:",
		"    protocols:",
		"      http:",
		"        endpoint: 0.0.0.0:4318",
		"        - otlp",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the opt-in lines are %q, want %q", lines, want)
	}

	// Enabled: the marker and its space removed, nothing else changed.
	var enabled []string
	for _, line := range strings.Split(text, "\n") {
		enabled = append(enabled, strings.TrimPrefix(line, optInPrefix))
	}
	d, err := parse(strings.Join(enabled, "\n"))
	if err != nil {
		t.Fatalf("the enabled form does not parse: %v", err)
	}
	eq := func(path, want string) {
		t.Helper()
		if got := d[path]; got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}
	eq("receivers.otlp.protocols.http.endpoint", "0.0.0.0:4318")
	eq("service.pipelines.logs.receivers[0]", "k8s_objects")
	eq("service.pipelines.logs.receivers[1]", "k8s_cluster")
	eq("service.pipelines.logs.receivers[2]", "otlp")
	eq("service.pipelines.logs.exporters[0]", "otlphttp/toposhift")
	eq("exporters.otlphttp/toposhift.sending_queue.num_consumers", "1")
	if _, has := d["service.pipelines.logs.exporters[1]"]; has {
		t.Error("the enabled form has a second exporter")
	}
	for k := range d {
		if strings.HasPrefix(k, "service.pipelines.") && !strings.HasPrefix(k, "service.pipelines.logs.") {
			t.Errorf("the enabled form has a pipeline besides logs: %s", k)
		}
	}

	// The README carries the same lines, and says plainly what enabling them opens.
	readme := read(t, "README.md")
	for _, line := range []string{
		strings.Join(want[:4], "\n"),
		strings.TrimLeft(want[4], " "),
	} {
		if !strings.Contains(readme, line) {
			t.Errorf("README.md does not show the opt-in line(s) %q", line)
		}
	}
	for _, must := range []string{optInPrefix, "unauthenticated", "network policy", "off by default"} {
		if !strings.Contains(readme, must) {
			t.Errorf("README.md does not mention %q in the opt-in step", must)
		}
	}
}

// TestReadmeNamesEveryPlaceholder fails when a configuration holds a
// placeholder the README does not explain, as when one is renamed in one file
// and not the other.
func TestReadmeNamesEveryPlaceholder(t *testing.T) {
	readme := read(t, "README.md")
	for _, name := range []string{"toposhift.yaml", "nodeagent.yaml", "tracemetrics.yaml"} {
		cfg := read(t, name)
		found := regexp.MustCompile(`<[A-Z][A-Z_]*>`).FindAllString(cfg, -1)
		if len(found) == 0 {
			t.Fatalf("%s has no placeholder to edit", name)
		}
		seen := map[string]bool{}
		for _, p := range found {
			if seen[p] {
				continue
			}
			seen[p] = true
			if !strings.Contains(readme, p) {
				t.Errorf("README.md does not mention %s, which %s asks to edit", p, name)
			}
		}
	}
	// The feature gate is in every file, and the README says how to reach toposhift.
	const gate = "pkg.experimentalmetricmetadata.useEntityEventsSpecification"
	for name, text := range map[string]string{"toposhift.yaml": read(t, "toposhift.yaml"), "nodeagent.yaml": read(t, "nodeagent.yaml"), "README.md": readme} {
		if !strings.Contains(text, gate) {
			t.Errorf("%s does not mention %s", name, gate)
		}
	}
	if !strings.Contains(readme, "TOPOSHIFT_LISTEN") {
		t.Error("README.md does not mention TOPOSHIFT_LISTEN")
	}
}

// TestNodeAgentConfig checks nodeagent.yaml for the values that matter: the gate
// is named, the entity event is reported at the interval toposhift expects, the
// detectors come in the order that makes host.id the provider's on a cloud, the
// boot id is read and its newline removed, nothing is hashed or renamed, and the
// agent sends only to the cluster collector.
func TestNodeAgentConfig(t *testing.T) {
	text := read(t, "nodeagent.yaml")
	d, err := parse(text)
	if err != nil {
		t.Fatal(err)
	}
	eq := func(path, want string) {
		t.Helper()
		got, ok := d[path]
		if !ok {
			t.Errorf("%s is missing", path)
			return
		}
		if got != want {
			t.Errorf("%s = %q, want %q", path, got, want)
		}
	}

	// The gate: the file says to start the agent with it, and the README does too.
	const gate = "--feature-gates=pkg.experimentalmetricmetadata.useEntityEventsSpecification"
	if !strings.Contains(text, gate) {
		t.Errorf("nodeagent.yaml does not carry the start command with %s", gate)
	}

	// The interval of the entity event is the one toposhift serve expects by
	// default: its records live for three of them.
	const hm = "receivers.host_metrics"
	meta, err := time.ParseDuration(d[hm+".metadata_collection_interval"])
	if err != nil {
		t.Fatalf("%s.metadata_collection_interval = %q: %v", hm, d[hm+".metadata_collection_interval"], err)
	}
	if meta != 300*time.Second {
		t.Errorf("metadata_collection_interval is %s, want 300s", meta)
	}
	if meta != sequencer.DefaultNodeInterval {
		t.Errorf("metadata_collection_interval is %s, but the default of --node-interval is %s", meta, sequencer.DefaultNodeInterval)
	}
	if _, ok := d[hm+".scrapers.load"]; !ok {
		t.Error("host_metrics has no scraper: it sends no entity event without one")
	}

	// The detectors: env first, the node, the clouds, and system last, so that on
	// a cloud host.id is the instance id and on other machines the machine id.
	var detectors []string
	for i := 0; ; i++ {
		v, ok := d["processors.resource_detection.detectors["+strconv.Itoa(i)+"]"]
		if !ok {
			break
		}
		detectors = append(detectors, v)
	}
	want := []string{"env", "k8s_api", "ec2", "gcp", "azure", "oraclecloud", "system"}
	if strings.Join(detectors, ",") != strings.Join(want, ",") {
		t.Errorf("detectors %v, want %v", detectors, want)
	}
	// The collector may not refuse a detector id it does not know, so the order is
	// asserted here and not left to its validation: every cloud detector is listed,
	// and each comes before system.
	index := func(name string) int { return slices.Index(detectors, name) }
	for _, cloud := range []string{"ec2", "gcp", "azure", "oraclecloud"} {
		if i := index(cloud); i < 0 || i > index("system") {
			t.Errorf("the %s detector is at %d, system at %d: it must come first", cloud, i, index("system"))
		}
	}
	eq("processors.resource_detection.override", "false")
	eq("processors.resource_detection.k8s_api.node_from_env_var", "K8S_NODE_NAME")
	eq("processors.resource_detection.k8s_api.resource_attributes.k8s.cluster.uid.enabled", "false")
	eq("processors.resource_detection.system.resource_attributes.host.id.enabled", "true")
	eq("processors.resource_detection.system.resource_attributes.host.arch.enabled", "true")

	// The boot id, read at start, and its trailing newline removed.
	eq("processors.resource/boot.attributes[0].key", "topo.host.boot.id")
	eq("processors.resource/boot.attributes[0].value", "${file:/proc/sys/kernel/random/boot_id}")
	eq("processors.resource/boot.attributes[0].action", "upsert")
	eq("processors.transform/boot.log_statements[0].context", "resource")
	eq("processors.transform/boot.log_statements[0].statements[0]", `replace_pattern(attributes["topo.host.boot.id"], "\\s+$", "")`)

	// Only to the cluster collector: its otlp port, never toposhift's own.
	const ex = "exporters.otlp_http/gateway"
	if m := regexp.MustCompile(`^http://<[A-Z][A-Z_]*>:4318$`); !m.MatchString(d[ex+".endpoint"]) {
		t.Errorf("%s.endpoint = %q, want the gateway's otlp port 4318 on a placeholder host", ex, d[ex+".endpoint"])
	}
	if strings.Contains(d[ex+".endpoint"], "7070") {
		t.Errorf("the agent's exporter points at toposhift's port: %s", d[ex+".endpoint"])
	}
	eq(ex+".encoding", "proto")
	eq(ex+".compression", "gzip")
	eq(ex+".timeout", "30s")

	// One logs pipeline, in this order, and no metrics pipeline.
	eq("service.telemetry.metrics.level", "none")
	eq("service.pipelines.logs.receivers[0]", "host_metrics")
	eq("service.pipelines.logs.processors[0]", "resource_detection")
	eq("service.pipelines.logs.processors[1]", "resource/boot")
	eq("service.pipelines.logs.processors[2]", "transform/boot")
	eq("service.pipelines.logs.exporters[0]", "otlp_http/gateway")
	for k := range d {
		switch {
		case strings.HasPrefix(k, "service.pipelines.metrics"), strings.HasPrefix(k, "service.pipelines.traces"):
			t.Errorf("%s: the agent has a pipeline that is not the logs one", k)
		case strings.Contains(k, "hostNetwork"):
			t.Errorf("%s: the agent does not use the host's network", k)
		}
	}
	// Nothing is hashed, and host.name is not set from the node's name: neither
	// is in any line that is not a comment.
	for i, line := range strings.Split(text, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		for _, banned := range []string{"SHA256", "SHA1", "MD5", "Hash", "hash(", "host.name"} {
			if strings.Contains(trimmed, banned) {
				t.Errorf("nodeagent.yaml line %d has %q: host.id is taken as given, and host.name is never used", i+1, banned)
			}
		}
	}

	// Nothing else is in the file: every key is one the checks above looked at.
	known := map[string]bool{
		hm + ".collection_interval":                                                         true,
		hm + ".metadata_collection_interval":                                                true,
		hm + ".scrapers.load":                                                               true,
		"processors.resource_detection.timeout":                                             true,
		"processors.resource_detection.override":                                            true,
		"processors.resource_detection.k8s_api.auth_type":                                   true,
		"processors.resource_detection.k8s_api.node_from_env_var":                           true,
		"processors.resource_detection.k8s_api.resource_attributes.k8s.cluster.uid.enabled": true,
		"processors.resource_detection.system.hostname_sources[0]":                          true,
		"processors.resource_detection.system.resource_attributes.host.id.enabled":          true,
		"processors.resource_detection.system.resource_attributes.host.arch.enabled":        true,
		"processors.resource_detection.system.resource_attributes.os.type.enabled":          true,
		"processors.resource/boot.attributes[0].key":                                        true,
		"processors.resource/boot.attributes[0].value":                                      true,
		"processors.resource/boot.attributes[0].action":                                     true,
		"processors.transform/boot.log_statements[0].context":                               true,
		"processors.transform/boot.log_statements[0].statements[0]":                         true,
		ex + ".endpoint":                       true,
		ex + ".encoding":                       true,
		ex + ".compression":                    true,
		ex + ".timeout":                        true,
		"service.telemetry.metrics.level":      true,
		"service.pipelines.logs.receivers[0]":  true,
		"service.pipelines.logs.processors[0]": true,
		"service.pipelines.logs.processors[1]": true,
		"service.pipelines.logs.processors[2]": true,
		"service.pipelines.logs.exporters[0]":  true,
	}
	var extra []string
	for k := range d {
		if known[k] || strings.HasPrefix(k, "processors.resource_detection.detectors[") {
			continue
		}
		extra = append(extra, k)
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("keys no check looks at: %v", extra)
	}
	eq(hm+".collection_interval", "5m")
	eq("processors.resource_detection.timeout", "5s")
	eq("processors.resource_detection.k8s_api.auth_type", "serviceAccount")
	eq("processors.resource_detection.system.hostname_sources[0]", "os")
}

// TestReadmeDescribesTheNodeAgents fails when the README leaves out what an
// operator of the optional agents has to know.
func TestReadmeDescribesTheNodeAgents(t *testing.T) {
	readme := read(t, "README.md")
	for _, must := range []string{
		"nodeagent.yaml", "otlp", "--node-interval", "TOPOSHIFT_NODE_INTERVAL", "/etc/machine-id", "hostNetwork",
		"Development", "k8s.node.uid", "legacy", "machine id", "confidential", "never to toposhift", "host.id",
		"ec2", "oraclecloud", "K8S_NODE_NAME", "host.arch", "nodeagent/<cluster", "/status",
	} {
		if !strings.Contains(readme, must) {
			t.Errorf("README.md does not mention %q", must)
		}
	}
	// The constants the README states are the code's.
	if nodeagent.DefaultTTLMultiple != 3 || sequencer.DefaultNodeInterval != 300*time.Second {
		t.Errorf("the README says three times 300 seconds; the code has %d times %s", nodeagent.DefaultTTLMultiple, sequencer.DefaultNodeInterval)
	}
}

// traceMetricsPrefix starts each line of toposhift.yaml that is the trace metrics
// opt-in: the receiver, the filter, the exporter and the pipeline, commented out.
// Removing the prefix, with the one space after it, gives the line as it is when
// enabled.
const traceMetricsPrefix = "#TRACEMETRICS "

// metricsFilter is the condition of both filters, the trace collector's and the
// cluster collector's: the two metrics toposhift reads, and nothing else. It is
// built from the names the translators read, so a rename there fails here.
func metricsFilter() string {
	return `'name != "` + svcgraph.Metric + `" and name != "` + spancount.Metric + `"'`
}

// traceMetricsLines are the lines that carry the trace metrics prefix, without it.
func traceMetricsLines(text string) []string {
	var out []string
	for _, line := range strings.Split(text, "\n") {
		if rest, ok := strings.CutPrefix(line, traceMetricsPrefix); ok {
			out = append(out, rest)
		}
	}
	return out
}

// withoutPrefixes returns text with each given prefix (and the space it ends in)
// removed from the lines that start with it.
func withoutPrefixes(text string, prefixes ...string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		for _, p := range prefixes {
			lines[i] = strings.TrimPrefix(lines[i], p)
			if lines[i] != line {
				break
			}
		}
	}
	return strings.Join(lines, "\n")
}

// listAt returns the items of the list at path, in order.
func listAt(d doc, path string) []string {
	var out []string
	for i := 0; ; i++ {
		v, ok := d[path+"["+strconv.Itoa(i)+"]"]
		if !ok {
			return out
		}
		out = append(out, v)
	}
}

// pipelineNames returns the names of the pipelines the file declares, sorted.
func pipelineNames(d doc) []string {
	seen := map[string]bool{}
	for k := range d {
		if rest, ok := strings.CutPrefix(k, "service.pipelines."); ok {
			name, _, _ := strings.Cut(rest, ".")
			seen[name] = true
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// TestTraceCollectorConfig checks tracemetrics.yaml: the trace collector keeps the
// connector settings the translators assume, makes no pod or address association,
// sends only the connectors' two metrics, and only to the cluster collector.
func TestTraceCollectorConfig(t *testing.T) {
	text := read(t, "tracemetrics.yaml")
	d, err := parse(text)
	if err != nil {
		t.Fatal(err)
	}

	// The connector settings the translators assume: toposhift has no flag for them.
	const sg = "connectors.service_graph"
	for path, want := range map[string]time.Duration{
		sg + ".metrics_flush_interval":        svcgraph.DefaultFlush,
		sg + ".cache_loop":                    svcgraph.DefaultCacheLoop,
		sg + ".metrics_timestamp_offset":      0,
		sg + ".store.ttl":                     2 * time.Second,
		"exporters.otlp_http/gateway.timeout": 30 * time.Second,
	} {
		got, err := time.ParseDuration(d[path])
		if err != nil {
			t.Errorf("%s = %q: %v", path, d[path], err)
		} else if got != want {
			t.Errorf("%s is %s, want %s", path, got, want)
		}
	}

	// The namespace is the one dimension, and the file spells the lists out in blocks.
	if got := listAt(d, sg+".dimensions"); !slices.Equal(got, []string{"service.namespace"}) {
		t.Errorf("dimensions are %v, want [service.namespace]", got)
	}

	// The pipelines: traces make the two metrics, the metrics pipeline carries those
	// and nothing an application sends, and the SDKs' own metrics and logs are
	// accepted and dropped.
	pipes := map[string]struct{ receivers, processors, exporters []string }{
		"traces":      {[]string{"otlp"}, []string{"memory_limiter", "batch"}, []string{"service_graph", "count"}},
		"metrics":     {[]string{"service_graph", "count"}, []string{"memory_limiter", "filter/tracemetrics"}, []string{"otlp_http/gateway"}},
		"metrics/sdk": {[]string{"otlp"}, nil, []string{"nop"}},
		"logs/sdk":    {[]string{"otlp"}, nil, []string{"nop"}},
	}
	if got := pipelineNames(d); !slices.Equal(got, []string{"logs/sdk", "metrics", "metrics/sdk", "traces"}) {
		t.Errorf("pipelines are %v", got)
	}
	for name, want := range pipes {
		base := "service.pipelines." + name
		for part, w := range map[string][]string{"receivers": want.receivers, "processors": want.processors, "exporters": want.exporters} {
			if got := listAt(d, base+"."+part); !slices.Equal(got, w) {
				t.Errorf("%s.%s is %v, want %v", base, part, got, w)
			}
		}
		// The otlp receiver is in the traces pipeline and in the two that drop what else
		// the SDKs send. It is not in the pipeline that sends on, so no application can
		// send a metric through it.
		if slices.Contains(listAt(d, base+".receivers"), "otlp") && !slices.Contains([]string{"traces", "metrics/sdk", "logs/sdk"}, name) {
			t.Errorf("the otlp receiver is in the %s pipeline: an application could send metrics through it", name)
		}
	}
	for _, name := range []string{"metrics/sdk", "logs/sdk"} {
		if got := listAt(d, "service.pipelines."+name+".exporters"); !slices.Equal(got, []string{"nop"}) {
			t.Errorf("the %s pipeline exports to %v, want only nop", name, got)
		}
	}

	// The filter is the cluster collector's, built from the translators' names.
	if got := d["processors.filter/tracemetrics.metrics.metric[0]"]; got != metricsFilter() {
		t.Errorf("the filter is %s, want %s", got, metricsFilter())
	}

	// Only to the cluster collector's trace metrics port, on a placeholder host; the
	// queue is in memory, in bytes, and sends in order.
	const ex = "exporters.otlp_http/gateway"
	if m := regexp.MustCompile(`^http://<[A-Z][A-Z_]*>:4319$`); !m.MatchString(d[ex+".endpoint"]) {
		t.Errorf("%s.endpoint = %q, want the cluster collector's port 4319 on a placeholder host", ex, d[ex+".endpoint"])
	}
	if got := regexp.MustCompile(`<[A-Z][A-Z_]*>`).FindAllString(text, -1); !slices.Equal(slices.Compact(got), []string{"<GATEWAY_HOST>"}) {
		t.Errorf("the placeholders are %v, want <GATEWAY_HOST> only", got)
	}
	if _, has := d[ex+".sending_queue.storage"]; has {
		t.Error("the trace collector's queue is on disk: it keeps no state")
	}

	// No pod or address association, and no resource detection, anywhere in the file.
	for _, banned := range []string{"k8sattributes", "k8s_attributes", "resourcedetection", "resource_detection", "k8s.pod.ip"} {
		if strings.Contains(text, banned) {
			t.Errorf("tracemetrics.yaml names %q: a pod uid is never derived from an address", banned)
		}
	}
	if !strings.Contains(strings.ToLower(text), "one replica") {
		t.Error("tracemetrics.yaml does not say it runs as one replica")
	}

	// Nothing else is in the file: every key is one of these, with this value.
	want := map[string]string{
		"receivers.otlp.protocols.grpc.endpoint":           "0.0.0.0:4317",
		"receivers.otlp.protocols.http.endpoint":           "0.0.0.0:4318",
		"processors.memory_limiter.check_interval":         "1s",
		"processors.memory_limiter.limit_mib":              "600",
		"processors.memory_limiter.spike_limit_mib":        "150",
		"processors.batch.timeout":                         "1s",
		"processors.filter/tracemetrics.error_mode":        "ignore",
		"processors.filter/tracemetrics.metrics.metric[0]": metricsFilter(),
		sg + ".dimensions[0]":                              "service.namespace",
		sg + ".virtual_node_peer_attributes":               "[]",
		sg + ".virtual_node_extra_label":                   "true",
		sg + ".metrics_flush_interval":                     d[sg+".metrics_flush_interval"],
		sg + ".metrics_timestamp_offset":                   d[sg+".metrics_timestamp_offset"],
		sg + ".cache_loop":                                 d[sg+".cache_loop"],
		sg + ".store.ttl":                                  d[sg+".store.ttl"],
		sg + ".store.max_items":                            "1000",
		"connectors.count":                                 "{}",
		ex + ".endpoint":                                   d[ex+".endpoint"],
		ex + ".encoding":                                   "proto",
		ex + ".compression":                                "gzip",
		ex + ".timeout":                                    d[ex+".timeout"],
		ex + ".retry_on_failure.enabled":                   "true",
		ex + ".retry_on_failure.initial_interval":          "5s",
		ex + ".retry_on_failure.max_interval":              "30s",
		ex + ".retry_on_failure.max_elapsed_time":          "0",
		ex + ".sending_queue.enabled":                      "true",
		ex + ".sending_queue.num_consumers":                "1",
		ex + ".sending_queue.sizer":                        "bytes",
		ex + ".sending_queue.queue_size":                   "100000000",
		"exporters.nop":                                    "{}",
		"service.telemetry.metrics.level":                  "none",
		"service.pipelines.traces.receivers[0]":            "otlp",
		"service.pipelines.traces.processors[0]":           "memory_limiter",
		"service.pipelines.traces.processors[1]":           "batch",
		"service.pipelines.traces.exporters[0]":            "service_graph",
		"service.pipelines.traces.exporters[1]":            "count",
		"service.pipelines.metrics.receivers[0]":           "service_graph",
		"service.pipelines.metrics.receivers[1]":           "count",
		"service.pipelines.metrics.processors[0]":          "memory_limiter",
		"service.pipelines.metrics.processors[1]":          "filter/tracemetrics",
		"service.pipelines.metrics.exporters[0]":           "otlp_http/gateway",
		"service.pipelines.metrics/sdk.receivers[0]":       "otlp",
		"service.pipelines.metrics/sdk.exporters[0]":       "nop",
		"service.pipelines.logs/sdk.receivers[0]":          "otlp",
		"service.pipelines.logs/sdk.exporters[0]":          "nop",
	}
	for k, w := range want {
		if got, ok := d[k]; !ok {
			t.Errorf("%s is missing", k)
		} else if got != w {
			t.Errorf("%s = %q, want %q", k, got, w)
		}
	}
	var extra []string
	for k := range d {
		if _, ok := want[k]; !ok {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("keys no check looks at: %v", extra)
	}
}

// TestTraceMetricsOptIn checks the trace metrics opt-in form of toposhift.yaml: the
// commented lines are the receiver on its own port, the filter, the exporter with
// its own queue and the metrics pipeline; uncommented, alone or with the node
// agents', the file still parses and leaves the logs pipeline as it is; and the
// README shows the step and says what enabling it opens.
func TestTraceMetricsOptIn(t *testing.T) {
	text := read(t, "toposhift.yaml")
	lines := traceMetricsLines(text)
	want := []string{
		"  otlp/tracemetrics:",
		"    protocols:",
		"      http:",
		"        endpoint: 0.0.0.0:4319",
		"  filter/tracemetrics:",
		"    error_mode: ignore",
		"    metrics:",
		"      metric:",
		"        - " + metricsFilter(),
		"  otlphttp/toposhift_metrics:",
		"    endpoint: http://<TOPOSHIFT_HOST>:7070",
		"    encoding: proto",
		"    compression: gzip",
		"    timeout: 30s",
		"    retry_on_failure:",
		"      enabled: true",
		"      initial_interval: 5s",
		"      max_interval: 30s",
		"      max_elapsed_time: 0",
		"    sending_queue:",
		"      enabled: true",
		"      storage: file_storage/toposhift",
		"      num_consumers: 1",
		"      sizer: bytes",
		"      queue_size: 500000000",
		"      batch:",
		"        sizer: bytes",
		"        max_size: 16000000",
		"        flush_timeout: 1s",
		"    metrics:",
		"      receivers:",
		"        - otlp/tracemetrics",
		"      processors:",
		"        - filter/tracemetrics",
		"        - resource",
		"      exporters:",
		"        - otlphttp/toposhift_metrics",
	}
	if len(want) != 37 {
		t.Fatalf("the test lists %d opt-in lines, want 37", len(want))
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("the opt-in lines are %q, want %q", lines, want)
	}

	// Off by default: no line that is not a comment names the opt-in or its port.
	for i, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		for _, on := range []string{"tracemetrics", "toposhift_metrics", "4319"} {
			if strings.Contains(line, on) {
				t.Errorf("toposhift.yaml line %d names %q outside the opt-in: it is off by default", i+1, on)
			}
		}
	}

	// Enabled: the marker and its space removed, nothing else changed. The keys the
	// default file has keep their values, and the keys the 37 lines add are these.
	def, err := parse(text)
	if err != nil {
		t.Fatal(err)
	}
	d, err := parse(withoutPrefixes(text, traceMetricsPrefix))
	if err != nil {
		t.Fatalf("the enabled form does not parse: %v", err)
	}
	const ex = "exporters.otlphttp/toposhift_metrics"
	added := map[string]string{
		"receivers.otlp/tracemetrics.protocols.http.endpoint": "0.0.0.0:4319",
		"processors.filter/tracemetrics.error_mode":           "ignore",
		"processors.filter/tracemetrics.metrics.metric[0]":    metricsFilter(),
		ex + ".endpoint":                          "http://<TOPOSHIFT_HOST>:7070",
		ex + ".encoding":                          "proto",
		ex + ".compression":                       "gzip",
		ex + ".timeout":                           "30s",
		ex + ".retry_on_failure.enabled":          "true",
		ex + ".retry_on_failure.initial_interval": "5s",
		ex + ".retry_on_failure.max_interval":     "30s",
		ex + ".retry_on_failure.max_elapsed_time": "0",
		ex + ".sending_queue.enabled":             "true",
		ex + ".sending_queue.storage":             "file_storage/toposhift",
		ex + ".sending_queue.num_consumers":       "1",
		ex + ".sending_queue.sizer":               "bytes",
		ex + ".sending_queue.queue_size":          "500000000",
		ex + ".sending_queue.batch.sizer":         "bytes",
		ex + ".sending_queue.batch.max_size":      "16000000",
		ex + ".sending_queue.batch.flush_timeout": "1s",
		"service.pipelines.metrics.receivers[0]":  "otlp/tracemetrics",
		"service.pipelines.metrics.processors[0]": "filter/tracemetrics",
		"service.pipelines.metrics.processors[1]": "resource",
		"service.pipelines.metrics.exporters[0]":  "otlphttp/toposhift_metrics",
	}
	if len(d) != len(def)+len(added) {
		t.Errorf("the enabled form has %d keys, want the default file's %d and the opt-in's %d", len(d), len(def), len(added))
	}
	for k, v := range def {
		if d[k] != v {
			t.Errorf("%s = %q enabled, %q by default: the opt-in changes a key it does not add", k, d[k], v)
		}
	}
	for k, v := range added {
		if got, ok := d[k]; !ok {
			t.Errorf("%s is missing", k)
		} else if got != v {
			t.Errorf("%s = %q, want %q", k, got, v)
		}
		if _, has := def[k]; has {
			t.Errorf("%s is in the default file", k)
		}
	}
	var extra []string
	for k := range d {
		_, isDef := def[k]
		_, isAdded := added[k]
		if !isDef && !isAdded {
			extra = append(extra, k)
		}
	}
	sort.Strings(extra)
	if len(extra) > 0 {
		t.Errorf("keys no check looks at: %v", extra)
	}

	// One consumer on each queue, so the shipped collector has one sender for logs and
	// one for metrics (the exporter's own default is ten). The default file has the one
	// uncommented num_consumers line that check:collector's control misspells; the
	// opt-in's is a commented line, which the enabled form makes a second.
	if n := len(regexp.MustCompile(`(?m)^ *num_consumers:`).FindAllString(text, -1)); n != 1 {
		t.Errorf("the default file has %d uncommented num_consumers lines, want 1", n)
	}
	for _, key := range []string{"exporters.otlphttp/toposhift.sending_queue.num_consumers", ex + ".sending_queue.num_consumers"} {
		if d[key] != "1" {
			t.Errorf("%s = %q in the enabled form, want 1", key, d[key])
		}
	}

	// The receiver is HTTP only, and the exporter is the logs one with a queue of its
	// own: same place, same encoding, same retry, same storage directory (each exporter gets its own file in it), one consumer.
	if _, has := d["receivers.otlp/tracemetrics.protocols.grpc.endpoint"]; has {
		t.Error("the trace metrics receiver listens on grpc too")
	}
	const logs = "exporters.otlphttp/toposhift"
	for _, key := range []string{
		"endpoint", "encoding", "compression", "timeout", "retry_on_failure.max_elapsed_time",
		"sending_queue.storage", "sending_queue.num_consumers", "sending_queue.sizer", "sending_queue.batch.max_size",
	} {
		if d[ex+"."+key] != d[logs+"."+key] {
			t.Errorf("%s.%s = %q, but the logs exporter's is %q", ex, key, d[ex+"."+key], d[logs+"."+key])
		}
	}
	if d[ex+".encoding"] != "proto" {
		t.Errorf("the metrics exporter's encoding is %q, want proto: toposhift's metrics path takes protobuf only", d[ex+".encoding"])
	}

	// The pipelines: logs unchanged, and metrics fed by the new receiver only. The
	// receiver is in no other pipeline, so a sender on its port cannot write what the
	// logs pipeline takes. With the node agents' receiver too, each is in its own.
	both, err := parse(withoutPrefixes(text, optInPrefix, traceMetricsPrefix))
	if err != nil {
		t.Fatalf("both opt-ins together do not parse: %v", err)
	}
	for form, dd := range map[string]doc{"trace metrics alone": d, "both opt-ins": both} {
		if got, w := listAt(dd, "service.pipelines.logs.processors"), []string{"resource"}; !slices.Equal(got, w) {
			t.Errorf("%s: the logs processors are %v, want %v", form, got, w)
		}
		if got, w := listAt(dd, "service.pipelines.logs.exporters"), []string{"otlphttp/toposhift"}; !slices.Equal(got, w) {
			t.Errorf("%s: the logs exporters are %v, want %v", form, got, w)
		}
		if got, w := listAt(dd, "service.pipelines.metrics.receivers"), []string{"otlp/tracemetrics"}; !slices.Equal(got, w) {
			t.Errorf("%s: the metrics receivers are %v, want %v", form, got, w)
		}
		if got := pipelineNames(dd); !slices.Equal(got, []string{"logs", "metrics"}) {
			t.Errorf("%s: the pipelines are %v, want logs and metrics", form, got)
		}
	}
	if got, w := listAt(d, "service.pipelines.logs.receivers"), []string{"k8s_objects", "k8s_cluster"}; !slices.Equal(got, w) {
		t.Errorf("trace metrics alone: the logs receivers are %v, want %v", got, w)
	}
	if got, w := listAt(both, "service.pipelines.logs.receivers"), []string{"k8s_objects", "k8s_cluster", "otlp"}; !slices.Equal(got, w) {
		t.Errorf("both opt-ins: the logs receivers are %v, want %v", got, w)
	}
	for _, dd := range []doc{d, both} {
		for k, v := range dd {
			switch {
			case v == "otlp" && strings.HasPrefix(k, "service.pipelines.") && !strings.HasPrefix(k, "service.pipelines.logs."):
				t.Errorf("%s: the node agents' receiver is in a pipeline besides logs", k)
			case v == "otlp/tracemetrics" && strings.HasPrefix(k, "service.pipelines.") && !strings.HasPrefix(k, "service.pipelines.metrics."):
				t.Errorf("%s: the trace metrics receiver is in a pipeline besides metrics", k)
			}
		}
	}

	// The README carries the step, and says plainly what enabling it opens.
	readme := read(t, "README.md")
	for _, must := range []string{
		"sed 's/^" + traceMetricsPrefix + "//' toposhift.yaml", traceMetricsPrefix, "4319",
		"unauthenticated", "network policy", "off by default", "otlp/tracemetrics", "otlphttp/toposhift_metrics",
	} {
		if !strings.Contains(readme, must) {
			t.Errorf("README.md does not mention %q in the trace metrics opt-in step", must)
		}
	}
}

// TestReadmeDescribesTheTraceCollector fails when the README leaves out what an
// operator of the optional trace collector has to know, or says what it must not.
func TestReadmeDescribesTheTraceCollector(t *testing.T) {
	readme := read(t, "README.md")
	for _, must := range []string{
		"tracemetrics.yaml", "one replica", "<GATEWAY_HOST>", "POD_UID", "k8s.pod.uid", "service.instance.id",
		"k8sattributes", "--dependency-expiry", "--instance-ttl", "liveness_expiry",
		"spancount.skipped_no_instance_id", "POST /v1/metrics",
		// Restriction (d) holds only while the resource processor does.
		"only while the `resource` processor stays", "--cluster-name",
		// The first policy names a namespace for the trace collector, and a policy
		// only works where the network plugin enforces it.
		"namespaceSelector: {matchLabels: {kubernetes.io/metadata.name:", "enforces them", "depends on that",
		// The empty peer-attribute list turns the conversion of expired edges into
		// virtual nodes off; a deployment that sets attributes gets points the
		// translator sets aside under this counter.
		"empty list", "no other switch", "no expired edge", "svcgraph.skipped_connection_virtual_node", "peer attributes were set",
	} {
		if !strings.Contains(readme, must) {
			t.Errorf("README.md does not mention %q", must)
		}
	}
	// POD_UID must come first in the list, since Kubernetes expands $(VAR) only for a
	// variable defined earlier.
	if !regexp.MustCompile("`POD_UID` from `fieldRef: metadata.uid`, defined \\*\\*before\\*\\* `OTEL_RESOURCE_ATTRIBUTES`").MatchString(readme) {
		t.Error("README.md does not say that POD_UID is defined before OTEL_RESOURCE_ATTRIBUTES")
	}
	// This version has no other switch, but the empty list converts no expired edge,
	// so the README names neither a node the collector does not make nor a count in
	// the shipped setting that is called normal.
	for _, bad := range []string{
		"cannot be told not to make virtual nodes", "named `unknown`", "`user`",
		"virtual nodes are off", "cannot be switched off", "means a misconfiguration", "is not a misconfiguration",
	} {
		if strings.Contains(readme, bad) {
			t.Errorf("README.md says %q: the empty peer-attribute list makes the collector convert no expired edge into a virtual node", bad)
		}
	}
	// The README is published: it gives reasons and settings, no measurement and no
	// reference to the notes that are not.
	if m := regexp.MustCompile(`\bS0\b|\bADR[- ]?\d`).FindString(readme); m != "" {
		t.Errorf("README.md cites %q", m)
	}
}
