package duckengine

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func fixture(t *testing.T, name string) RawProfile {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", "profile", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var raw RawProfile
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestRecordedProfilesAreRead reads each profile DuckDB reported, as recorded from the
// tiny plan, and checks what it keeps.
func TestRecordedProfilesAreRead(t *testing.T) {
	for _, c := range []struct {
		name      string
		scans     int
		function  string
		root      string
		filtersOn []string // columns the scan's filters name
	}{
		{"neighbors_fwd_a2_a", 1, "READ_PARQUET", "HASH_GROUP_BY", []string{"layer", "subject_kind", "source", "event_time_ns", "seq"}},
		{"neighbors_rev_a1", 1, "READ_PARQUET", "HASH_GROUP_BY", []string{"layer", "subject_kind", "target", "event_time_ns", "seq"}},
		{"batch_rev_a2", 1, "READ_PARQUET", "HASH_GROUP_BY", []string{"layer", "subject_kind", "target"}},
		{"alive_i", 1, "READ_PARQUET", "UNGROUPED_AGGREGATE", []string{"layer", "a", "end_ns"}},
	} {
		raw := fixture(t, c.name)
		p, err := ParseProfile(raw)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		scans := p.Scans()
		if len(scans) != c.scans || p.Nodes[0].Name != c.root || p.Nodes[0].Depth != 0 {
			t.Errorf("%s: %d scans, plan %s", c.name, len(scans), p.Shape())
			continue
		}
		s := scans[0]
		if s.Function != c.function || s.RowsScanned <= 0 || s.Cardinality <= 0 && c.name != "alive_i" {
			t.Errorf("%s: scan %+v", c.name, s)
		}
		// Each column the statement filters is named by a conjunct the scan was given: a
		// conjunct DuckDB proved true from the footer may be gone, but none is invented.
		for _, f := range s.Filters {
			named := false
			for _, col := range c.filtersOn {
				if strings.Contains(f, col) {
					named = true
				}
			}
			if !named {
				t.Errorf("%s: the scan was given %q, which names none of %v", c.name, f, c.filtersOn)
			}
		}
		if len(s.Filters) == 0 {
			t.Errorf("%s: the scan was given no filter", c.name)
		}
		if p.RowsReturned <= 0 && c.name != "alive_i" {
			t.Errorf("%s: rows returned %d", c.name, p.RowsReturned)
		}
		// The scan's rows are the profile's cumulative figure, once: summing the
		// cumulative figure of every operator would count the scan once for each above it.
		if p.RowsScanned() != p.CumulativeRowsScanned {
			t.Errorf("%s: the scans were handed %d rows, the profile says %d", c.name, p.RowsScanned(), p.CumulativeRowsScanned)
		}
		var cumulative int64
		var walk func(r RawProfile)
		walk = func(r RawProfile) {
			if v, ok := r.Metrics["CUMULATIVE_ROWS_SCANNED"]; ok {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				cumulative += n
			}
			for _, c := range r.Children {
				walk(c)
			}
		}
		walk(raw)
		if len(p.Nodes) > 1 && cumulative == p.RowsScanned() {
			t.Errorf("%s: summing the cumulative figures of every node gives the same as the scans, so this fixture cannot tell the two apart", c.name)
		}
	}
}

// TestTwoReadsOfOneStatementHaveOneStrippedProfile: the two recordings of one read differ
// in every timing, in memory and in what the connection had cached, and have one digest
// once those are stripped; another read has another.
func TestTwoReadsOfOneStatementHaveOneStrippedProfile(t *testing.T) {
	a, b := fixture(t, "neighbors_fwd_a2_a"), fixture(t, "neighbors_fwd_a2_b")
	ja, _ := json.Marshal(a)
	jb, _ := json.Marshal(b)
	if string(ja) == string(jb) {
		t.Fatal("the two recordings are identical, so they cannot show that timings are stripped")
	}
	differ := 0
	for _, k := range stripped {
		if a.Metrics[k] != b.Metrics[k] {
			differ++
		}
	}
	if differ == 0 {
		t.Fatal("no stripped metric differs between the recordings")
	}
	pa, err := ParseProfile(a)
	if err != nil {
		t.Fatal(err)
	}
	pb, err := ParseProfile(b)
	if err != nil {
		t.Fatal(err)
	}
	if pa.Digest() != pb.Digest() {
		t.Errorf("the stripped profiles differ:\n%+v\n%+v", pa, pb)
	}
	other, err := ParseProfile(fixture(t, "neighbors_rev_a1"))
	if err != nil {
		t.Fatal(err)
	}
	if other.Digest() == pa.Digest() {
		t.Error("two different reads have one digest")
	}
	// No timing, memory or cache metric is among what is kept.
	for _, k := range append(slices.Clone(queryKept), operatorKept...) {
		if slices.Contains(stripped, k) {
			t.Errorf("%s is both kept and stripped", k)
		}
	}
	for _, k := range stripped {
		measure := []string{"LATENCY", "TIME", "TIMING", "BYTES", "MEMORY", "SIZE", "WAL"}
		if k != "QUERY_NAME" && !slices.ContainsFunc(measure, func(m string) bool { return strings.Contains(k, m) }) {
			t.Errorf("%s is stripped but is not a time, memory, size or byte count", k)
		}
	}
}

func TestEveryMetricOfTheFixturesIsClassified(t *testing.T) {
	var walk func(r RawProfile, root bool)
	walk = func(r RawProfile, root bool) {
		for k := range r.Metrics {
			if !classified(k, root) {
				t.Errorf("%s is neither kept nor stripped", k)
			}
		}
		for _, c := range r.Children {
			walk(c, false)
		}
	}
	for _, n := range []string{"neighbors_fwd_a2_a", "neighbors_rev_a1", "batch_rev_a2", "alive_i"} {
		walk(fixture(t, n), true)
	}
}

func TestProfileRefusals(t *testing.T) {
	good := fixture(t, "neighbors_fwd_a2_a")
	clone := func() RawProfile {
		b, _ := json.Marshal(good)
		var r RawProfile
		_ = json.Unmarshal(b, &r)
		return r
	}
	r := clone()
	r.Metrics["A_NEW_METRIC"] = "1"
	if _, err := ParseProfile(r); !errors.Is(err, ErrUnknownMetric) {
		t.Errorf("an unknown query metric: %v", err)
	}
	r = clone()
	r.Children[0].Metrics["A_NEW_METRIC"] = "1"
	if _, err := ParseProfile(r); !errors.Is(err, ErrUnknownMetric) {
		t.Errorf("an unknown operator metric: %v", err)
	}
	r = clone()
	r.Metrics["ROWS_RETURNED"] = "many"
	if _, err := ParseProfile(r); err == nil {
		t.Error("a count that is not a number")
	}
	r = clone()
	delete(r.Metrics, "CUMULATIVE_ROWS_SCANNED")
	if _, err := ParseProfile(r); err == nil {
		t.Error("a profile without a count")
	}
	r = clone()
	r.Children = nil
	if _, err := ParseProfile(r); err == nil {
		t.Error("a profile without an operator")
	}
	r = clone()
	delete(r.Children[0].Metrics, "OPERATOR_NAME")
	if _, err := ParseProfile(r); err == nil {
		t.Error("an operator without a name")
	}
}

func TestExtraInfoIsParsed(t *testing.T) {
	got, err := parseExtra(`{Function=READ_PARQUET, Projections='a` + "\n" + `b', Filters='x=\'k, 1\'` + "\n" + `y<=2', __estimated_cardinality__=20000, Total Files Read=1, 'Filename(s)'=/p/f.parquet}`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Function": "READ_PARQUET", "Projections": "a\nb", "Filters": "x='k, 1'\ny<=2",
		"__estimated_cardinality__": "20000", "Total Files Read": "1", "Filename(s)": "/p/f.parquet",
	}
	if len(got) != len(want) {
		t.Fatalf("%q", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s is %q, want %q", k, got[k], v)
		}
	}
	for _, bad := range []string{"Function=x", "{Function}", "{Filters='unclosed}"} {
		if _, err := parseExtra(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
	}
	if m, err := parseExtra("{}"); err != nil || len(m) != 0 {
		t.Errorf("an empty one: %v %v", m, err)
	}
}
