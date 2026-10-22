package doltspike

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The plans in testdata/explain are what Dolt 2.4.2 printed (EXPLAIN FORMAT=TREE) for each
// kind of statement of the reads, on a loaded table of the synthetic plan of a test.

func fixture(t *testing.T, name string) []string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "explain", name))
	if err != nil {
		t.Fatal(err)
	}
	return strings.Split(strings.TrimSuffix(string(b), "\n"), "\n")
}

type tableRead struct {
	table  string
	bound  int
	ranged bool
	lookup bool
}

func readsOf(as []Access) []tableRead {
	var out []tableRead
	for _, a := range as {
		out = append(out, tableRead{a.Table, a.Bound, a.Ranged, a.Lookup})
	}
	return out
}

var fixtures = []struct {
	file, kind, variant string
	reads               []tableRead
}{
	{"rule-neighbors.txt", "rule", "", []tableRead{{"state", 3, false, false}, {"state", 3, false, false}, {"log", 6, false, true}}},
	{"rule-batch.txt", "rule", "", []tableRead{{"state", 3, false, true}, {"state", 3, false, true}, {"log", 6, false, true}}},
	{"log-neighbors-primary_key.txt", "log", variantByKey, []tableRead{{"log", 3, false, false}}},
	{"log-neighbors-index_on_event_time.txt", "log", variantTime, []tableRead{{"log", 3, false, false}}},
	{"alive-rule-alive.txt", "alive-rule", "", []tableRead{{"ent_state", 1, false, false}, {"ent_state", 1, false, false}, {"ent_log", 2, false, true}}},
	{"alive-log-alive.txt", "alive-log", "", []tableRead{{"ent_log", 1, false, false}}},
	{"window-window-index_on_event_time.txt", "window", variantTime, []tableRead{{"log", 3, true, false}}},
	{"window-window-primary_key.txt", "window", variantByKey, []tableRead{{"log", 3, false, false}}},
}

func TestThePlansTheServerPrintedAreReadAsTheStatementsAssume(t *testing.T) {
	for _, f := range fixtures {
		t.Run(f.file, func(t *testing.T) {
			as, scans, err := ParsePlan(fixture(t, f.file))
			if err != nil {
				t.Fatal(err)
			}
			if scans {
				t.Error("the plan has a table scan")
			}
			if got := readsOf(as); !slices.Equal(got, f.reads) {
				t.Fatalf("the plan reads %+v, want %+v", got, f.reads)
			}
			problems, variant := CheckAccesses(f.kind, as, scans)
			if len(problems) != 0 || variant != f.variant {
				t.Fatalf("problems %v, variant %q (want %q)", problems, variant, f.variant)
			}
		})
	}
}

// Every way a plan can differ from the assumption, made from a plan that is as assumed, is
// found: the check can fail.
func TestAPlanThatDiffersFromTheAssumptionIsFound(t *testing.T) {
	rule := strings.Join(fixture(t, "rule-neighbors.txt"), "\n")
	window := strings.Join(fixture(t, "window-window-index_on_event_time.txt"), "\n")
	for _, tc := range []struct {
		name, kind, plan, want string
	}{
		{"a table scan", "rule", strings.Replace(rule, "IndexedTableAccess(state)", "Table", 1), "scans a table"},
		{"a table scan by another name", "rule", strings.Replace(rule, "IndexedTableAccess(state)", "Table(state)", 1), "scans a table"},
		{"a table the kind does not read", "rule", strings.Replace(rule, "IndexedTableAccess(log)", "IndexedTableAccess(ent_log)", 1), "ent_log"},
		{"the whole index", "window", strings.Replace(window, "filters: ", "filtres: ", 1), "read whole"},
		{"a fingerprint left unbounded", "rule", strings.Replace(rule, "[[107 56 115 46 110 111 100 101 0 33 62 211 164 133 202 23 73 108 18 57 224 52 158 3 71], [107 56 115 46 110 111 100 101 0 33 62 211 164 133 202 23 73 108 18 57 224 52 158 3 71]]", "[NULL, ∞)", 1), "leading columns fixed"},
		{"a lookup by fewer columns", "rule", strings.Replace(rule, "keys: 3, 2, k.a, k.peer, k.rel, k.producer", "keys: 3, 2, k.a", 1), "leading columns fixed"},
		{"another index", "rule", strings.Replace(rule, "index: [log.layer,log.dir,log.a,log.peer,log.rel,log.producer,log.event_ns,log.seq]", "index: [log.layer,log.dir,log.a,log.event_ns,log.seq]", 1), "none of the assumed ways"},
		{"a window with no range on the event time", "window", strings.Replace(window, "[1767225600000000000, 1767229200000000000)", "[NULL, ∞)", 1), "none of the assumed ways"},
		{"no table at all", "rule", "Project\n └─ Values", "reads no table"},
		{"a statement of a kind the check does not know", "walk", rule, "not assumed to"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			as, scans, err := ParsePlan(strings.Split(tc.plan, "\n"))
			if err != nil {
				t.Fatal(err)
			}
			problems, _ := CheckAccesses(tc.kind, as, scans)
			if len(problems) == 0 || !strings.Contains(strings.Join(problems, "\n"), tc.want) {
				t.Fatalf("problems %v; want one about %q", problems, tc.want)
			}
		})
	}
	// The unchanged plans pass, so those failures are of the changes.
	for _, tc := range []struct{ kind, plan string }{{"rule", rule}, {"window", window}} {
		as, scans, _ := ParsePlan(strings.Split(tc.plan, "\n"))
		if problems, _ := CheckAccesses(tc.kind, as, scans); len(problems) != 0 {
			t.Errorf("%s: %v", tc.kind, problems)
		}
	}
}

func TestAPlanThatCannotBeReadIsAnError(t *testing.T) {
	for name, plan := range map[string]string{
		"filters that are not ranges": "IndexedTableAccess(log)\n ├─ index: [log.layer,log.dir]\n └─ filters: not a list",
		"a range of one end":          "IndexedTableAccess(log)\n ├─ index: [log.layer,log.dir]\n └─ filters: [{[3], [2, 2]}]",
		"an access with no index":     "IndexedTableAccess(log)\n └─ keys: 3, 2",
	} {
		if _, _, err := ParsePlan(strings.Split(plan, "\n")); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}

func TestTheTextOfALineIsWhatFollowsTheDrawing(t *testing.T) {
	for _, tc := range []struct {
		line string
		col  int
		text string
	}{
		{"Project", 0, "Project"},
		{" ├─ columns: [a, b]", 4, "columns: [a, b]"},
		{"     │   └─ IndexedTableAccess(state)", 12, "IndexedTableAccess(state)"},
		{"   │", 0, ""},
		{"", 0, ""},
	} {
		if col, text := textOf(tc.line); col != tc.col || text != tc.text {
			t.Errorf("%q: %d, %q; want %d, %q", tc.line, col, text, tc.col, tc.text)
		}
	}
}

func TestListsAreSplitOnlyAtTheTopLevel(t *testing.T) {
	got := splitTop("[1, 2], (a, b), {c, d}, unhex(k.a_hex), plain")
	want := []string{"[1, 2]", "(a, b)", "{c, d}", "unhex(k.a_hex)", "plain"}
	if !slices.Equal(got, want) {
		t.Fatalf("%q, want %q", got, want)
	}
	lo, hi, ok := rangeEnds("[[1 2], [3 4])")
	if !ok || lo != "[1 2]" || hi != "[3 4]" {
		t.Fatalf("%q, %q, %v", lo, hi, ok)
	}
	for _, bad := range []string{"", "x", "[1]", "[1, 2, 3]", "1, 2"} {
		if _, _, ok := rangeEnds(bad); ok {
			t.Errorf("%q was a range", bad)
		}
	}
}
