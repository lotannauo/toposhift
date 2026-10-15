package blastscore

import (
	"encoding/json"
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"
)

// The fixtures in testdata mirror the schema the package documentation records,
// with invented names. No parser reads them here; the loader that does is
// tested against them. These tests keep the fixtures and the recorded keys in
// step: a key the documentation lists must be in a fixture, and a fixture must
// not carry a key the documentation does not list.

var (
	groundTruthKeys = []string{"fault", "alerts", "groups", "aliases", "propagations", "recommended_actions"}
	wrapperKeys     = []string{"apiVersion", "kind", "metadata", "spec"}
	keyLine         = regexp.MustCompile(`^( *)([A-Za-z_]+):`)
)

// keysAt lists the mapping keys of a YAML file at one indent, in order.
func keysAt(t *testing.T, path string, indent int) []string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, line := range strings.Split(string(b), "\n") {
		if m := keyLine.FindStringSubmatch(line); m != nil && len(m[1]) == indent {
			out = append(out, m[2])
		}
	}
	return out
}

func TestFixturesMirrorTheRecordedSchema(t *testing.T) {
	flat := keysAt(t, "testdata/ground_truth_flat.yaml", 0)
	for _, k := range groundTruthKeys {
		if !slices.Contains(flat, k) {
			t.Errorf("the flat fixture lacks %q", k)
		}
	}
	for _, k := range flat {
		if k != "metadata" && !slices.Contains(groundTruthKeys, k) {
			t.Errorf("the flat fixture has the undocumented key %q", k)
		}
	}

	if got := keysAt(t, "testdata/ground_truth_wrapped.yaml", 0); !slices.Equal(got, wrapperKeys) {
		t.Errorf("the wrapped fixture has the top-level keys %v, want %v", got, wrapperKeys)
	}
	inner := keysAt(t, "testdata/ground_truth_wrapped.yaml", 2)
	for _, k := range inner {
		if k != "name" && !slices.Contains(groundTruthKeys, k) {
			t.Errorf("the wrapped fixture has the undocumented key %q under spec", k)
		}
	}
	for _, k := range groundTruthKeys {
		if !slices.Contains(inner, k) {
			t.Errorf("the wrapped fixture lacks %q under spec", k)
		}
	}
}

func TestAlertFixturesHoldTheRecordedShapes(t *testing.T) {
	alert := []string{"labels", "annotations", "state", "activeAt", "value"}
	read := func(path string, into any) {
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, into); err != nil {
			t.Fatalf("%s: %v", path, err)
		}
	}
	check := func(where string, alerts []map[string]json.RawMessage) {
		if len(alerts) == 0 {
			t.Errorf("%s: no alert", where)
		}
		for _, a := range alerts {
			for _, k := range alert {
				if _, ok := a[k]; !ok {
					t.Errorf("%s: an alert lacks %q", where, k)
				}
			}
		}
	}
	var arr []map[string]json.RawMessage
	read("testdata/alerts_at_synthetic.json", &arr)
	check("alerts_at", arr)

	var obj struct {
		Data struct {
			Alerts []map[string]json.RawMessage `json:"alerts"`
		} `json:"data"`
	}
	read("testdata/alerts_in_alerting_state_synthetic.json", &obj)
	check("alerts_in_alerting_state", obj.Data.Alerts)
}
