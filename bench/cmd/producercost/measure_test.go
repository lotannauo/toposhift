package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// otlp is a Go value as an OTLP/JSON value: strings, lists and maps.
func otlp(v any) any {
	switch x := v.(type) {
	case string:
		return map[string]any{"stringValue": x}
	case []any:
		vals := make([]any, len(x))
		for i, e := range x {
			vals[i] = otlp(e)
		}
		return map[string]any{"arrayValue": map[string]any{"values": vals}}
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var vals []any
		for _, k := range keys {
			vals = append(vals, map[string]any{"key": k, "value": otlp(x[k])})
		}
		return map[string]any{"kvlistValue": map[string]any{"values": vals}}
	}
	panic(fmt.Sprintf("otlp: %T", v))
}

// line is one line of a capture: one export holding one log record, an object of a pull.
func line(observed time.Time, resource string, object map[string]any) string {
	b, err := json.Marshal(map[string]any{"resourceLogs": []any{map[string]any{
		"resource": map[string]any{},
		"scopeLogs": []any{map[string]any{
			"scope": map[string]any{"name": "k8sobjectsreceiver"},
			"logRecords": []any{map[string]any{
				"observedTimeUnixNano": fmt.Sprint(observed.UnixNano()),
				"body":                 otlp(object),
				"attributes":           []any{map[string]any{"key": "k8s.resource.name", "value": map[string]any{"stringValue": resource}}},
			}},
		}},
	}}})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// writeCapture writes a capture of two lines, a node and a pod scheduled on it, with the
// pod's line first in the file and observed last: the replay order is by time.
func writeCapture(t *testing.T) string {
	t.Helper()
	created := time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)
	ts := func(d time.Duration) string { return created.Add(d).Format(time.RFC3339) }
	node := map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "node-uid-1", "name": "n1", "creationTimestamp": ts(0), "resourceVersion": "1"},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "reason": "KubeletReady", "lastTransitionTime": ts(0),
		}}},
	}
	pod := map[string]any{
		"kind": "Pod", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "pod-uid-1", "name": "p1", "namespace": "default", "creationTimestamp": ts(5 * time.Minute), "resourceVersion": "2"},
		"spec":     map[string]any{"nodeName": "n1"},
		"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{
			"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(5*time.Minute + time.Second),
		}}},
	}
	path := filepath.Join(t.TempDir(), "run.jsonl")
	text := line(created.Add(6*time.Minute), "pods", pod) + "\n" + line(created.Add(time.Minute), "nodes", node) + "\n"
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestProducerNamesHaveTheLengthAsked(t *testing.T) {
	t.Parallel()

	for n, want := range map[int]string{18: "k8sobjects/default", 32: "k8sobjects/" + workload.ProducerFill(21), 64: "k8sobjects/" + workload.ProducerFill(53)} {
		if got, err := producerName(n); err != nil || string(got) != want || len(got) != n {
			t.Errorf("producerName(%d) = %q, %v; want %q", n, got, err, want)
		}
	}
	// The filler is not one repeated character, which compression would hide.
	long, _ := producerName(64)
	if rest := string(long)[len(producerPrefix):]; strings.Trim(rest, rest[:1]) == "" {
		t.Errorf("%q is padded with one repeated character", long)
	}
	for _, n := range []int{0, 5, 11} {
		if got, err := producerName(n); err == nil {
			t.Errorf("producerName(%d) = %q, want an error", n, got)
		}
	}
}

// Every record the translation gives has a producer of exactly N bytes, the edge records
// as much as the entity records, and the records are in Seq order from 1.
func TestEveryRecordCarriesAProducerOfExactlyNBytes(t *testing.T) {
	t.Parallel()

	in, err := loadCapture(writeCapture(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(in) != 2 || len(in[0]) != 1 || len(in[1]) != 1 {
		t.Fatalf("batches %v, want two of one record each", in)
	}
	if o, _ := json.Marshal(in[0][0].rec.Attributes); !bytes.Contains(o, []byte("nodes")) {
		t.Errorf("the first batch is not the node's, which was observed first: %s", o)
	}
	for _, n := range []int{18, 32, 40, 64} {
		p, err := producerName(n)
		if err != nil {
			t.Fatal(err)
		}
		batches, counted, err := translate(in, p)
		if err != nil {
			t.Fatal(err)
		}
		if counted != (counts{captured: 2, translated: 3, written: 3}) {
			t.Errorf("N = %d: counts %+v, want 2 log records, 3 translated and 3 written", n, counted)
		}
		var entities, edges int
		seq := uint64(0)
		for _, b := range batches {
			for _, r := range b {
				if len(r.Producer) != n {
					t.Errorf("N = %d: a %v record (%s) has a producer of %d bytes: %q", n, r.Subject.Kind, r.Subject.Relation, len(r.Producer), r.Producer)
				}
				switch r.Subject.Kind {
				case store.SubjectEntity:
					entities++
				case store.SubjectEdge:
					edges++
					if r.Subject.Relation != catalog.ScheduledOn {
						t.Errorf("the edge is %s, want scheduled_on", r.Subject.Relation)
					}
				}
				if seq++; r.Seq != seq {
					t.Errorf("N = %d: Seq %d, want %d", n, r.Seq, seq)
				}
				if r.Kind == lifecycle.Observe && len(r.Payload) == 0 {
					t.Errorf("N = %d: an observation with no payload", n)
				}
			}
		}
		if entities != 2 || edges != 1 {
			t.Errorf("N = %d: %d entity records and %d edge records, want 2 and 1", n, entities, edges)
		}
	}
}

func TestTheCostIsTheGrowthOverTheGrowthOfTheShorterNameOverTheEmptyStore(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		live64, live18, empty uint64
		want                  float64
	}{
		{1100, 1000, 0, 0.1},
		{1050, 1000, 500, 0.1}, // a name of 32 bytes: 50 over a base of 500
		{1100, 1000, 500, 0.2}, // the denominator is 500, not 1000
		{1000, 1000, 500, 0},
		{900, 1000, 500, -0.2},
		{49309, 47902, 855, 1407.0 / 47047.0},
	} {
		got, err := Cost(c.live64, c.live18, c.empty)
		if err != nil || math.Abs(got-c.want) > 1e-12 {
			t.Errorf("Cost(%d, %d, %d) = %v, %v; want %v", c.live64, c.live18, c.empty, got, err, c.want)
		}
	}
	for _, c := range [][3]uint64{{1, 500, 500}, {1, 400, 500}} {
		if got, err := Cost(c[0], c[1], c[2]); err == nil {
			t.Errorf("Cost(%v) = %v, want an error: nothing to divide by", c, got)
		}
	}
}

// A measurement leaves the store compacted to the bottom level, counts what it wrote, and
// removes the directory it made.
func TestMeasureCompactsCountsAndCleansUp(t *testing.T) {
	t.Parallel()

	in, err := loadCapture(writeCapture(t))
	if err != nil {
		t.Fatal(err)
	}
	tmp := t.TempDir()
	res, err := run(context.Background(), tmp, in, []int{32, 40, 64})
	if err != nil {
		t.Fatal(err)
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Errorf("the temporary directory holds %v after the measurement (%v)", left, err)
	}
	rows := res.Rows()
	if len(rows) != 5 {
		t.Fatalf("%d rows, want the empty store, 18, 32, 40 and 64", len(rows))
	}
	for i, want := range []struct {
		label string
		n     int
	}{{"empty", 0}, {"18", 18}, {"32", 32}, {"40", 40}, {"64", 64}} {
		if rows[i].Label != want.label || rows[i].N != want.n {
			t.Errorf("row %d is %q (N %d), want %q (%d)", i, rows[i].Label, rows[i].N, want.label, want.n)
		}
		full := want.n != 0
		wantRecords, wantCaptured := 0, 0
		if full {
			wantRecords, wantCaptured = 3, 2
		}
		if rows[i].Records != wantRecords || rows[i].Captured != wantCaptured || rows[i].Translated != wantRecords {
			t.Errorf("%s: %d captured, %d translated, %d written; want %d, %d, %d", want.label, rows[i].Captured, rows[i].Translated, rows[i].Records, wantCaptured, wantRecords, wantRecords)
		}
		if full && (rows[i].PayloadBytes == 0 || rows[i].PayloadBytes != res.Baseline.PayloadBytes) {
			t.Errorf("%s: payload bytes %d and %d; the producer name must not change them", want.label, rows[i].PayloadBytes, res.Baseline.PayloadBytes)
		}
	}
	for _, r := range rows {
		if r.LiveTableBytes == 0 || r.BytesWritten == 0 {
			t.Errorf("%s: %d live table bytes and %d written", r.Label, r.LiveTableBytes, r.BytesWritten)
		}
		// Compacted to the bottom: no table is left in a level above the last one that holds any.
		last := -1
		for l, n := range r.TablesPerLevel {
			if n > 0 {
				last = l
			}
		}
		for l := 0; l < last; l++ {
			if r.TablesPerLevel[l] != 0 {
				t.Errorf("%s: tables per level %v, want all in one, the bottom, level", r.Label, r.TablesPerLevel)
				break
			}
		}
		if r.TablesPerLevel[6] == 0 {
			t.Errorf("%s: tables per level %v, want them in level 6", r.Label, r.TablesPerLevel)
		}
	}
	if res.Baseline.LiveTableBytes <= res.Empty.LiveTableBytes {
		t.Errorf("a store with records (%d live bytes) is no larger than the empty one (%d)", res.Baseline.LiveTableBytes, res.Empty.LiveTableBytes)
	}
	for _, r := range res.Lengths {
		got, err := res.CostOf(r)
		want := (float64(r.LiveTableBytes) - float64(res.Baseline.LiveTableBytes)) / (float64(res.Baseline.LiveTableBytes) - float64(res.Empty.LiveTableBytes))
		if err != nil || got != want {
			t.Errorf("CostOf(%s) = %v, %v; want %v", r.Label, got, err, want)
		}
	}
}

func TestLengthsAreParsedAndRefused(t *testing.T) {
	t.Parallel()

	for list, want := range map[string][]int{
		"32,64":     {32, 64},
		"64,32":     {64, 32}, // the order asked
		" 40 , 14 ": {40, 14},
		"64":        {64},
		"19,17":     {19, 17},
	} {
		if got, err := parseLengths(list); err != nil || !slices.Equal(got, want) {
			t.Errorf("parseLengths(%q) = %v, %v; want %v", list, got, err, want)
		}
	}
	for list, why := range map[string]string{
		"18":       "the baseline",
		"32,18,64": "the baseline",
		"13":       "below 14",
		"65":       "above 64",
		"0":        "below 14",
		"-1":       "below 14",
		"32,32":    "a duplicate",
		"32,64,32": "a duplicate",
		"":         "empty",
		"32,":      "an empty entry",
		"x":        "not a number",
	} {
		if got, err := parseLengths(list); err == nil {
			t.Errorf("parseLengths(%q) = %v, want an error (%s)", list, got, why)
		}
	}
}

// The table has the empty store, the baseline and a row for each length, in the order asked,
// then a cost line for each length against 18.
func TestTheReportHasARowAndACostLineForEachLength(t *testing.T) {
	t.Parallel()

	src, err := os.ReadFile(writeCapture(t))
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	for _, name := range []string{"run1.jsonl", "run2.jsonl"} {
		if err := os.WriteFile(filepath.Join(dir, name), src, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	tmp := t.TempDir()
	var out bytes.Buffer
	if err := report(&out, dir, tmp, []int{32, 40, 64}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 1+5+3 {
		t.Fatalf("%d lines, want a header, five rows and three cost lines:\n%s", len(lines), out.String())
	}
	for i, label := range []string{"empty", "18", "32", "40", "64"} {
		if f := strings.Fields(lines[1+i]); f[0] != label {
			t.Errorf("row %d is %q, want %q", i, lines[1+i], label)
		}
	}
	for i, n := range []string{"32", "40", "64"} {
		if want := "cost_a(" + n + ") = (live_" + n + " - live_18) / (live_18 - live_empty) = ("; !strings.HasPrefix(lines[6+i], want) {
			t.Errorf("cost line %d is %q, want it to begin %q", i, lines[6+i], want)
		}
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Errorf("the temporary directory holds %v (%v)", left, err)
	}
}
