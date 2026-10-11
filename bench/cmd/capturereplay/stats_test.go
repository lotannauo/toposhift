package main

import (
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

func TestIDShapes(t *testing.T) {
	hex := strings.Repeat("ab", 32)
	for id, want := range map[string]string{
		"":                        "empty",
		"containerd://" + hex:     "containerd://<64 hex>",
		"cri-o://" + hex:          "cri-o://<64 hex>",
		hex:                       "<64 hex> without a runtime prefix",
		"containerd://" + hex[1:]: "other",
		"containerd://":           "other",
		"abc":                     "other",
	} {
		if got := idShape(id); got != want {
			t.Errorf("idShape(%q) = %q, want %q", id, got, want)
		}
	}
	if bareID("containerd://x") != "x" || bareID("x") != "x" {
		t.Error("bareID")
	}
}

func TestClassifyRecords(t *testing.T) {
	created := t0.Add(-time.Hour)
	pod := podObject("u1", "n", "node1", created, "containerd://"+strings.Repeat("a", 64), "")
	for _, c := range []struct {
		name string
		lr   capture.LogRecord
		want recordKind
	}{
		{"watch", watchRecord("MODIFIED", pod, t0), recordKind{Source: "watch", Type: "MODIFIED", Kind: "Pod", UID: "u1"}},
		{"pull", pullRecord(pod, t0), recordKind{Source: "pull", Kind: "Pod", UID: "u1"}},
		{"entity", podState("u2", "n", "", created, t0), recordKind{Source: "entity", Type: "entity.state", Kind: "k8s.pod", UID: "u2"}},
	} {
		got := classify(c.lr)
		got.Object, got.HasObj = capture.AnyValue{}, false
		if got != c.want {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
	// A pull record with no kind in its object is told by the resource's name.
	o := obj(kv("metadata", obj(kv("uid", str("n1")))))
	lr := capture.LogRecord{ObservedTimeUnixNano: nanos(t0), Body: &o, Attributes: []capture.KeyValue{kv("k8s.resource.name", str("nodes"))}}
	if rk := classify(lr); rk.Kind != "Node" || rk.UID != "n1" {
		t.Errorf("%+v", rk)
	}
	// The container ids of a pod object, as written, empty ones included.
	ids := containerIDs(classify(pullRecord(pod, t0)).Object)
	if len(ids) != 2 || !strings.HasPrefix(ids[0], "containerd://") || ids[1] != "" {
		t.Errorf("container ids %q", ids)
	}
}

func TestRunStatsCountsAPullLine(t *testing.T) {
	created := t0.Add(-time.Hour)
	p := func(uid string) capture.LogRecord { return pullRecord(podObject(uid, uid, "", created), t0) }
	paths := runFilesIn(t, [][]capture.LogRecord{
		{p("a"), p("b"), p("c")},
		{watchRecord("ADDED", podObject("d", "d", "", created), t0.Add(time.Minute)), podState("d", "d", "", created, t0.Add(time.Minute))},
	})
	rf, err := scanRun(paths[0], 1)
	if err != nil {
		t.Fatal(err)
	}
	rs := newRunStats(rf)
	if err := rf.walk(rf.Refs, func(b batch) error { rs.observe(b); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(rs.Pulls) != 1 || rs.Pulls[0].Pods != 3 || rs.Pulls[0].Records != 3 || rs.Pulls[0].Line != 1 || rs.Pulls[0].Bytes <= 0 {
		t.Errorf("pull lines: %+v", rs.Pulls)
	}
	if rs.BySource["pull"] != 3 || rs.BySource["watch"] != 1 || rs.BySource["entity"] != 1 || rs.Records != 5 {
		t.Errorf("by source: %v", rs.BySource)
	}
	if rs.Watch["ADDED Pod"] != 1 || rs.Entity["entity.state k8s.pod"] != 1 || rs.ReportIntervals["300"] != 1 {
		t.Errorf("watch %v entity %v intervals %v", rs.Watch, rs.Entity, rs.ReportIntervals)
	}
	if !rs.PodPulls.Min.Equal(t0) || !rs.Observed.Max.Equal(t0.Add(time.Minute)) || !rs.EntityTime.Min.Equal(t0.Add(time.Minute)) {
		t.Errorf("ranges: %+v %+v %+v", rs.PodPulls, rs.Observed, rs.EntityTime)
	}
	if _, ok := rs.Added["d"]; !ok {
		t.Errorf("added: %v", rs.Added)
	}
	if rs.LargestPull.Line != 1 {
		t.Errorf("largest pull: %+v", rs.LargestPull)
	}
	// The pod UIDs of the two sources meet in d.
	if _, ok := rs.PodUIDs[srcObjects]["d"]; !ok {
		t.Errorf("pod uids %v", rs.PodUIDs)
	}
	if _, ok := rs.PodUIDs[srcCluster]["d"]; !ok {
		t.Errorf("pod uids %v", rs.PodUIDs)
	}
}
