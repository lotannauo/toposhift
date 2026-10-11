// Package otelcollector holds no code. Its test checks the collector
// configuration shipped beside it, toposhift.yaml, and the README that explains
// it.
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
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
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
	d, err := parse(read(t, "toposhift.yaml"))
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
	eq(q+".queue_size", "1000")
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

// TestReadmeNamesEveryPlaceholder fails when the configuration holds a
// placeholder the README does not explain, as when one is renamed in one file
// and not the other.
func TestReadmeNamesEveryPlaceholder(t *testing.T) {
	cfg := read(t, "toposhift.yaml")
	readme := read(t, "README.md")
	found := regexp.MustCompile(`<[A-Z][A-Z_]*>`).FindAllString(cfg, -1)
	if len(found) == 0 {
		t.Fatal("toposhift.yaml has no placeholder to edit")
	}
	seen := map[string]bool{}
	for _, p := range found {
		if seen[p] {
			continue
		}
		seen[p] = true
		if !strings.Contains(readme, p) {
			t.Errorf("README.md does not mention %s", p)
		}
	}
	// The feature gate is in both files, and the README says how to reach toposhift.
	const gate = "pkg.experimentalmetricmetadata.useEntityEventsSpecification"
	for name, text := range map[string]string{"toposhift.yaml": cfg, "README.md": readme} {
		if !strings.Contains(text, gate) {
			t.Errorf("%s does not mention %s", name, gate)
		}
	}
	if !strings.Contains(readme, "TOPOSHIFT_LISTEN") {
		t.Error("README.md does not mention TOPOSHIFT_LISTEN")
	}
}
