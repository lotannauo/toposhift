package main

import (
	"os"
	"strings"
	"testing"
	"time"
)

const (
	uidA = "11111111-1111-4111-8111-111111111111"
	uidB = "22222222-2222-4222-8222-222222222222"
	n1   = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	n2   = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	n3   = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

func loadPods(t *testing.T) *podsTruth {
	t.Helper()
	f, err := os.Open("testdata/truth-pods.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	pt, err := parsePods(f)
	if err != nil {
		t.Fatal(err)
	}
	return pt
}

func loadNodes(t *testing.T) *nodesTruth {
	t.Helper()
	f, err := os.Open("testdata/truth-nodes.tsv")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	nt, err := parseNodes(f)
	if err != nil {
		t.Fatal(err)
	}
	return nt
}

func TestParsePodsTruth(t *testing.T) {
	pt := loadPods(t)
	if len(pt.Times) != 5 {
		t.Fatalf("%d samples, want 5 (the empty one and the unavailable one count)", len(pt.Times))
	}
	wantUnavail := []bool{false, false, true, false, false}
	for i, w := range wantUnavail {
		if pt.Unavail[i] != w {
			t.Errorf("sample %d unavailable = %v, want %v", i, pt.Unavail[i], w)
		}
	}
	a, b := pt.Pods[uidA], pt.Pods[uidB]
	if a == nil || b == nil || len(pt.UIDs) != 2 {
		t.Fatalf("pods %v", pt.UIDs)
	}
	for i, w := range []bool{true, false, false, true, true} {
		if a.Present[i] != w {
			t.Errorf("pod a present at sample %d = %v, want %v", i, a.Present[i], w)
		}
	}
	if a.First != 0 || a.Last != 4 || b.First != 0 || b.Last != 0 {
		t.Errorf("first and last: a %d..%d, b %d..%d", a.First, a.Last, b.First, b.Last)
	}
	if got := strings.Fields(b.Rows[0].CIDs); len(got) != 2 || !strings.HasPrefix(got[1], "containerd://") {
		t.Errorf("pod b container ids %q, want two", b.Rows[0].CIDs)
	}
	if r := a.Rows[1]; r.Sample != 3 || r.Phase != "Pending" || r.Node != "" || r.CIDs != "" {
		t.Errorf("pod a's pending row: %+v", r)
	}
	if r := a.Rows[2]; r.Deletion == "" {
		t.Errorf("pod a's last row has no deletion timestamp: %+v", r)
	}
	if got := pt.ByName["default/web-a"]; len(got) != 1 || got[0] != uidA {
		t.Errorf("by name: %v", got)
	}
	w := waitingContainers(pt)
	if w.PodsEverWithoutID != 1 || w.NeverGotOne != 0 || w.MedianSeconds != 30 {
		t.Errorf("waiting: %+v; pod a had no id in one sample of 30 seconds", w)
	}
}

func TestParsePodsTruthRefusesWhatItCannotRead(t *testing.T) {
	line := "2026-10-10T12:00:00Z\t" + uidA + "\tdefault/a\tn\tRunning\t\t\t0\n"
	for name, in := range map[string]string{
		"two fields where eight are due": "2026-10-10T12:00:00Z\tx\ty\n",
		"the same uid twice in a sample": line + line,
		"a time that goes back":          line + "2026-10-10T11:59:00Z\t" + uidB + "\tdefault/b\tn\tRunning\t\t\t0\n",
		"a time that is not a time":      "noon\t" + uidA + "\tdefault/a\tn\tRunning\t\t\t0\n",
		"no uid":                         "2026-10-10T12:00:00Z\t\tdefault/a\tn\tRunning\t\t\t0\n",
	} {
		if _, err := parsePods(strings.NewReader(in)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestParseNodesTruth(t *testing.T) {
	nt := loadNodes(t)
	if len(nt.Times) != 5 || len(nt.UIDs) != 3 {
		t.Fatalf("%d samples, %d uids", len(nt.Times), len(nt.UIDs))
	}
	if !nt.Unavail[2] || nt.Unavail[3] {
		t.Errorf("unavailable: %v", nt.Unavail)
	}
	at := func(s string) time.Time {
		v, err := time.Parse(time.RFC3339, s)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range []struct {
		name, when, want string
	}{
		{"node1", "2026-10-10T12:00:10Z", n1},
		{"node2", "2026-10-10T12:00:10Z", n2},
		{"node2", "2026-10-10T12:00:40Z", ""}, // node2 is not in the second sample
		{"node1", "2026-10-10T11:00:00Z", ""}, // before the first sample
		{"node1", "2026-10-10T12:02:10Z", n3}, // node1 was re-created under a new uid
		{"node1", "2026-10-10T12:00:30Z", n1}, // a sample exactly at the time
		{"nodeX", "2026-10-10T12:00:10Z", ""},
	} {
		if got := nt.nodeUIDAt(c.name, at(c.when)); got != c.want {
			t.Errorf("node %s at %s: %q, want %q", c.name, c.when, got, c.want)
		}
	}
	if d := nt.spacing(); d[0] != 30*time.Second || d[len(d)-1] != 30*time.Second {
		t.Errorf("spacing %v", d)
	}
}
