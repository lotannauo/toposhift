package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// Builders for small captures: a pod pull record, an entity event, and the files of
// runs made of lines of them.

func str(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func obj(kvs ...capture.KeyValue) capture.AnyValue {
	return capture.AnyValue{KVListValue: &capture.KeyValueList{Values: kvs}}
}

func kv(k string, v capture.AnyValue) capture.KeyValue { return capture.KeyValue{Key: k, Value: v} }

func arr(vs ...capture.AnyValue) capture.AnyValue {
	return capture.AnyValue{ArrayValue: &capture.ArrayValue{Values: vs}}
}

// podObject is a pod as the receiver lists it.
func podObject(uid, name, node string, created time.Time, containerIDs ...string) capture.AnyValue {
	spec := obj()
	if node != "" {
		spec = obj(kv("nodeName", str(node)))
	}
	status := []capture.KeyValue{kv("phase", str("Running"))}
	if len(containerIDs) > 0 {
		var sts []capture.AnyValue
		for i, id := range containerIDs {
			sts = append(sts, obj(kv("name", str("c"+string(rune('a'+i)))), kv("containerID", str(id))))
		}
		status = append(status, kv("containerStatuses", arr(sts...)))
	}
	return obj(
		kv("kind", str("Pod")),
		kv("metadata", obj(
			kv("name", str(name)), kv("namespace", str("default")), kv("uid", str(uid)),
			kv("creationTimestamp", str(created.UTC().Format(time.RFC3339))),
		)),
		kv("spec", spec),
		kv("status", obj(status...)),
	)
}

func nanos(t time.Time) capture.Uint64 { return capture.Uint64(t.UnixNano()) }

// pullRecord is a pull-mode record of an object, observed at t.
func pullRecord(o capture.AnyValue, t time.Time) capture.LogRecord {
	return capture.LogRecord{
		ObservedTimeUnixNano: nanos(t),
		Body:                 &o,
		Attributes:           []capture.KeyValue{kv("k8s.resource.name", str("pods"))},
	}
}

// watchRecord is a watch-mode record.
func watchRecord(typ string, o capture.AnyValue, t time.Time) capture.LogRecord {
	body := obj(kv("type", str(typ)), kv("object", o))
	return capture.LogRecord{
		ObservedTimeUnixNano: nanos(t),
		Body:                 &body,
		Attributes:           []capture.KeyValue{kv("k8s.resource.name", str("pods"))},
	}
}

// podState is an entity.state event of the cluster receiver for a pod.
func podState(uid, name, node string, created, t time.Time) capture.LogRecord {
	desc := []capture.KeyValue{
		kv("k8s.pod.name", str(name)), kv("k8s.namespace.name", str("default")), kv("k8s.pod.phase", str("Running")),
		kv("pod.creation_timestamp", str(created.UTC().Format(time.RFC3339))),
	}
	if node != "" {
		desc = append(desc, kv("k8s.node.name", str(node)))
	}
	secs := capture.Int64(300)
	return capture.LogRecord{
		TimeUnixNano: nanos(t),
		EventName:    "entity.state",
		Attributes: []capture.KeyValue{
			kv("entity.id", obj(kv("k8s.pod.uid", str(uid)))),
			kv("entity.type", str("k8s.pod")),
			{Key: "entity.report.interval", Value: capture.AnyValue{IntValue: &secs}},
			kv("entity.description", obj(desc...)),
		},
	}
}

// writeRun writes the lines of a run file; each line is one export of the given records.
func writeRun(t testing.TB, path string, lines ...[]capture.LogRecord) {
	t.Helper()
	var out []byte
	for _, recs := range lines {
		d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
			Resource:  capture.Resource{},
			ScopeLogs: []capture.ScopeLogs{{LogRecords: recs}},
		}}}
		b, err := capture.EncodeLogs(d)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, b...)
		out = append(out, '\n')
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, out, 0o644); err != nil {
		t.Fatal(err)
	}
}

// runFilesIn writes the runs and returns their paths.
func runFilesIn(t testing.TB, runs ...[][]capture.LogRecord) []string {
	t.Helper()
	dir := t.TempDir()
	var paths []string
	for i, lines := range runs {
		p := filepath.Join(dir, "out", "run"+string(rune('1'+i))+".jsonl")
		writeRun(t, p, lines...)
		paths = append(paths, p)
	}
	return paths
}

// t0 is a whole second, so that times built from it have no fraction.
var t0 = time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
