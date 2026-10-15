package itbench

import (
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
)

// An invented probe file, in the probe's format: comments, the column line, then a
// line a scenario.
const probeText = "# an invented probe\n# its comments are never read\n" + probeHeader + "\n" +
	"Scenario-1\ttrue\t7\ttrue\tcovers_window\n" +
	"Scenario-2\tfalse\t0\tfalse\tdoes_not_cover_window\n" +
	"Scenario-3\tna\tna\tfalse\tno_window_in_table_a\n" +
	"Scenario-4\ttrue\tna\ttrue\ttail_has_no_row_start\n"

func show(c Coverage) string {
	s := func(b *bool) string {
		if b == nil {
			return "nil"
		}
		if *b {
			return "true"
		}
		return "false"
	}
	i := func(m *int) string {
		if m == nil {
			return "nil"
		}
		return strings.Repeat("|", *m)
	}
	return s(c.CoversWindow) + "/" + i(c.OverlapMinutes)
}

func TestReadProbe(t *testing.T) {
	p, err := ReadProbe([]byte(probeText))
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Scenario-1": "true/|||||||",
		"Scenario-2": "false/",
		// What could not be computed stays unknown: never false, never zero.
		"Scenario-3": "nil/nil",
		"Scenario-4": "true/nil",
	}
	if len(p) != len(want) {
		t.Errorf("%d scenarios, want %d", len(p), len(want))
	}
	for id, w := range want {
		c, ok := p[id]
		if !ok {
			t.Errorf("%s is missing", id)
			continue
		}
		if got := show(c); got != w {
			t.Errorf("%s: %s, want %s", id, got, w)
		}
	}
}

func TestReadProbeRefusesWhatIsNotAProbe(t *testing.T) {
	head := probeHeader + "\n"
	ok := "Scenario-1\ttrue\t7\ttrue\tcovers_window\n"
	for _, c := range []struct{ name, text string }{
		{"no column line", "# only a comment\n"},
		{"another column line", "scenario\tcovers\tminutes\tto_fetch\treason\n" + ok},
		{"a row before the column line", ok + head},
		{"a comment after the column line", head + "# late\n" + ok},
		{"an empty line", head + ok + "\n"},
		{"too few fields", head + "Scenario-1\ttrue\t7\ttrue\n"},
		{"too many fields", head + "Scenario-1\ttrue\t7\ttrue\tcovers_window\textra\n"},
		{"a name for a case id", head + "frontend\ttrue\t7\ttrue\tcovers_window\n"},
		{"a scenario twice", head + ok + ok},
		{"a covers that is not one", head + "Scenario-1\tyes\t7\ttrue\tcovers_window\n"},
		{"a negative overlap", head + "Scenario-1\ttrue\t-1\ttrue\tcovers_window\n"},
		{"a fractional overlap", head + "Scenario-1\ttrue\t1.5\ttrue\tcovers_window\n"},
		{"an overlap with a sign", head + "Scenario-1\ttrue\t+7\ttrue\tcovers_window\n"},
		{"a time for an overlap", head + "Scenario-1\ttrue\t17:30\ttrue\tcovers_window\n"},
		{"an overlap with no coverage", head + "Scenario-1\tna\t7\tfalse\tno_window_in_table_a\n"},
		{"an overlap of an export that does not cover", head + "Scenario-1\tfalse\t3\tfalse\tdoes_not_cover_window\n"},
		{"a to_fetch that is not one", head + "Scenario-1\ttrue\t7\tmaybe\tcovers_window\n"},
	} {
		if _, err := ReadProbe([]byte(c.text)); err == nil {
			t.Errorf("%s: read", c.name)
		}
	}
}

func TestProbeApplySetsEachCaseFromItsOwnLine(t *testing.T) {
	p, err := ReadProbe([]byte(probeText))
	if err != nil {
		t.Fatal(err)
	}
	runs := []blastscore.CaseRun{{ID: "Scenario-4"}, {ID: "Scenario-2"}, {ID: "Scenario-3"}, {ID: "Scenario-1"}}
	if err := p.Apply(runs); err != nil {
		t.Fatal(err)
	}
	for _, r := range runs {
		got := show(Coverage{CoversWindow: r.Diag.TraceCoversWindow, OverlapMinutes: r.Diag.TraceOverlapMinutes})
		if want := show(p[r.ID]); got != want {
			t.Errorf("%s: %s, want %s", r.ID, got, want)
		}
	}

	// A run the probe does not name, and a line no run has, are both refused.
	if err := p.Apply([]blastscore.CaseRun{{ID: "Scenario-1"}, {ID: "Scenario-2"}, {ID: "Scenario-3"}, {ID: "Scenario-4"}, {ID: "Scenario-5"}}); err == nil {
		t.Error("a case without a probe line was given a coverage")
	}
	if err := p.Apply([]blastscore.CaseRun{{ID: "Scenario-1"}, {ID: "Scenario-2"}, {ID: "Scenario-3"}}); err == nil {
		t.Error("a probe line without a case was accepted")
	}
}

func TestResultsFileNamesTheRevision(t *testing.T) {
	rev := strings.TrimSuffix(strings.TrimPrefix(ResultsFile, "itbench-lite-"), ".json")
	if len(rev) != 8 || !strings.HasPrefix(rev, Revision) || !strings.HasPrefix("d0916b08ba421ce5e672e9ad68aa947d938dfef0", rev) {
		t.Errorf("%s does not name the first eight hex digits of revision %s", ResultsFile, Revision)
	}
}
