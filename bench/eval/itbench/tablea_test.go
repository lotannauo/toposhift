package itbench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
)

func utc(s string) time.Time {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		panic(err)
	}
	return t
}

func TestAlertsGiveTheWindow(t *testing.T) {
	dir := t.TempDir()
	d := scenario{n: 1, truth: "fault: []\n", alerts: true}.write(t, dir)
	a, err := ReadAlerts(d)
	if err != nil {
		t.Fatal(err)
	}
	// The earliest activeAt of any file; the latest alerts_at file's time; the time of
	// the alerts_in_alerting_state file.
	if !a.FirstActive.Equal(utc(winStart)) || !a.LastAt.Equal(utc("2026-03-04T10:15:00Z")) || !a.AlertingState.Equal(utc("2026-03-04T10:16:00Z")) || a.Files != 3 {
		t.Errorf("%+v", a)
	}
	w := windowOf(a)
	if !w.OK() || !w.From.Equal(a.FirstActive) || !w.To.Equal(a.LastAt) || !w.T0Proxy || w.T1Proxy {
		t.Errorf("window %+v", w)
	}

	// With no alerts_at file the end is the alerting-state file's time, a proxy.
	for _, f := range []string{"alerts_at_2026-03-04T10-10-00.000000.json", "alerts_at_" + lastAlert + ".json"} {
		if err := os.Remove(filepath.Join(d, "alerts", f)); err != nil {
			t.Fatal(err)
		}
	}
	a, err = ReadAlerts(d)
	if err != nil {
		t.Fatal(err)
	}
	if w := windowOf(a); !w.OK() || !w.To.Equal(utc("2026-03-04T10:16:00Z")) || !w.T1Proxy || !w.From.Equal(utc("2026-03-04T10:08:00Z")) {
		t.Errorf("window %+v", w)
	}

	// The alerting-state file may sit in the scenario's own folder.
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "alerts_in_alerting_state_2026-03-04T101700.500000Z.json"), []byte(`{"data":{"alerts":[{"activeAt":"2026-03-04T10:01:00Z"}]}}`), 0o644); err != nil {
		t.Fatal(err)
	}
	a, err = ReadAlerts(root)
	if err != nil {
		t.Fatal(err)
	}
	if !a.AlertingState.Equal(utc("2026-03-04T10:17:00.5Z")) || !a.FirstActive.Equal(utc("2026-03-04T10:01:00Z")) {
		t.Errorf("%+v", a)
	}
}

func TestWindowsThatCannotBeFound(t *testing.T) {
	for _, c := range []struct {
		a   Alerts
		why string
	}{
		{Alerts{}, "no_start_time"},
		{Alerts{FirstActive: utc("2026-03-04T10:00:00Z")}, "no_end_time"},
		{Alerts{FirstActive: utc("2026-03-04T10:00:00Z"), LastAt: utc("2026-03-04T10:00:00Z")}, "start_not_before_end"},
		{Alerts{FirstActive: utc("2026-03-04T10:01:00Z"), LastAt: utc("2026-03-04T10:00:00Z")}, "start_not_before_end"},
	} {
		if w := windowOf(c.a); w.Why != c.why || w.OK() {
			t.Errorf("%+v: window %+v, want %s", c.a, w, c.why)
		}
	}
}

func TestAnAlertsFileOfAnotherShapeIsAnError(t *testing.T) {
	d := t.TempDir()
	if err := os.MkdirAll(filepath.Join(d, "alerts"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(d, "alerts", "alerts_at_2026-03-04T10-10-00.000000.json"), []byte(`{"not":"an array"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAlerts(d); err == nil {
		t.Error("an alerts_at file that is an object was read")
	}
}

func TestClassComesFromTheLabelsAndSaysWhatFired(t *testing.T) {
	root := Group{ID: "r", Name: "rootname", Kind: "Pod"}
	fault := func(group, category, mech string) Fault {
		return Fault{Entity: FaultEntity{GroupID: group}, Category: category, Mechanism: mech}
	}
	cases := []struct {
		name   string
		faults []Fault
		want   blastscore.Class
		rule   string
	}{
		{"configuration", []Fault{fault("r", "Configuration Setting", "custom(flagd)")}, blastscore.ClassCallGraph, ""},
		{"resource category", []Fault{fault("r", "Resource", "x")}, blastscore.ClassCoResidency, "category=Resource"},
		{"saturation", []Fault{fault("r", "Resource Saturation", "x")}, blastscore.ClassCoResidency, "category=Resource Saturation"},
		{"stress mechanism", []Fault{fault("r", "Other", "memory_stress")}, blastscore.ClassCoResidency, "fault_mechanism=memory_stress"},
		{"network is not bandwidth", []Fault{fault("r", "Network", "network_partition")}, blastscore.ClassCallGraph, ""},
		{"exact strings only", []Fault{fault("r", "Resources", "memory_stress_test")}, blastscore.ClassCallGraph, ""},
		// Only the entries about the root decide, when there are any.
		{"a symptom entry does not decide", []Fault{fault("other", "Resource", "x"), fault("r", "Change", "custom")}, blastscore.ClassCallGraph, ""},
		{"by the name of the root", []Fault{{Entity: FaultEntity{Name: "rootname"}, Category: "Resource"}}, blastscore.ClassCoResidency, "category=Resource"},
		{"no entry about the root: all decide", []Fault{fault("other", "Resource", "x")}, blastscore.ClassCoResidency, "category=Resource"},
	}
	for _, c := range cases {
		got, rule := classOf(GroundTruth{Fault: c.faults}, root, true)
		if got != c.want || rule != c.rule {
			t.Errorf("%s: %s (%q), want %s (%q)", c.name, got, rule, c.want, c.rule)
		}
	}
}

func TestTableAReadsLabelsAndAlertsOnly(t *testing.T) {
	root := t.TempDir()
	scenario{n: 7, truth: scenario1Truth, alerts: true}.write(t, root) // no objects, no traces
	scenario{n: 8, truth: "fault: [\ngroups: x", alerts: true}.write(t, root)
	scenario{n: 9, truth: scenario2Truth}.write(t, root) // no alerts
	rows, err := TableA(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[0].ID != "Scenario-7" || rows[1].ID != "Scenario-8" || rows[2].ID != "Scenario-9" {
		t.Fatalf("rows %+v", rows)
	}
	r := rows[0]
	if !r.HasRoot || r.Root.ID != "pay-pods" || r.Seed != SeedPod || r.Class != blastscore.ClassCallGraph || !r.Window.OK() || len(r.Truth) != 3 {
		t.Errorf("row: %+v", r)
	}
	if rows[1].Unreadable == "" {
		t.Error("a file that does not parse is not listed as unreadable")
	}
	if r := rows[2]; r.Window.OK() || r.Window.Why != "no_start_time" || r.Seed != SeedMoved || len(r.Targets) != 3 {
		t.Errorf("row without alerts: %+v", r)
	}

	text := FormatTableA(rows)
	for _, want := range []string{"Scenario-7", "seed:     pod", "class:    call-graph", "window:   [2026-03-04T10:05:00Z (proxy), 2026-03-04T10:15:00Z)", "unreadable:", "seed:     moved (candidates, in order: cart-svc, front-svc, pay-svc)", "window:   none (no_start_time)"} {
		if !strings.Contains(text, want) {
			t.Errorf("table A lacks %q:\n%s", want, text)
		}
	}
}

// Table A is frozen by its hash, so its text must not change with anything else in
// this package: the text of the three invented scenarios is a golden file, and a
// change to it is a change to table A.
func TestTableAIsUnchanged(t *testing.T) {
	rows, err := TableA(shop(t))
	if err != nil {
		t.Fatal(err)
	}
	got := FormatTableA(rows)
	golden := filepath.Join("testdata", "table_a_three_scenarios.txt")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Errorf("table A differs from %s: the truth stage's output must not change", golden)
	}
}
