package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
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

// created is the instant the objects of the synthetic captures were created.
var created = time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC)

func ts(d time.Duration) string { return created.Add(d).Format(time.RFC3339) }

// nodeObject is the node n1 as a pull lists it.
func nodeObject() map[string]any {
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "node-uid-1", "name": "n1", "creationTimestamp": ts(0), "resourceVersion": "1"},
		"status": map[string]any{"conditions": []any{map[string]any{
			"type": "Ready", "status": "True", "reason": "KubeletReady", "lastTransitionTime": ts(0),
		}}},
	}
}

// podObject is the pod p1 scheduled on the node n1 as a pull lists it.
func podObject() map[string]any {
	return map[string]any{
		"kind": "Pod", "apiVersion": "v1",
		"metadata": map[string]any{"uid": "pod-uid-1", "name": "p1", "namespace": "default", "creationTimestamp": ts(5 * time.Minute), "resourceVersion": "2"},
		"spec":     map[string]any{"nodeName": "n1"},
		"status": map[string]any{"phase": "Running", "conditions": []any{map[string]any{
			"type": "PodScheduled", "status": "True", "lastTransitionTime": ts(5*time.Minute + time.Second),
		}}},
	}
}

// entityLine is one line of a capture: one export holding one entity state event of the
// cluster receiver, which reports every 300 seconds.
func entityLine(at time.Time, typ, idKey, id string, desc map[string]any) string {
	str := func(v string) map[string]any { return map[string]any{"stringValue": v} }
	attr := func(k string, v any) map[string]any { return map[string]any{"key": k, "value": v} }
	b, err := json.Marshal(map[string]any{"resourceLogs": []any{map[string]any{
		"resource": map[string]any{},
		"scopeLogs": []any{map[string]any{
			"scope": map[string]any{"name": "k8sclusterreceiver"},
			"logRecords": []any{map[string]any{
				"timeUnixNano": fmt.Sprint(at.UnixNano()),
				"body":         map[string]any{},
				"eventName":    "entity.state",
				"attributes": []any{
					attr("entity.id", otlp(map[string]any{idKey: id})),
					attr("entity.type", str(typ)),
					attr("entity.report.interval", map[string]any{"intValue": "300"}),
					attr("entity.description", otlp(desc)),
				},
			}},
		}},
	}}})
	if err != nil {
		panic(err)
	}
	return string(b)
}

// entityLines are the entity events of the cluster receiver for the node and the pod of
// writeCapture, and a stateful set, a type the catalog does not have.
func entityLines() []string {
	return []string{
		entityLine(created.Add(7*time.Minute), "k8s.node", "k8s.node.uid", "node-uid-1", map[string]any{
			"k8s.node.name": "n1", "node.creation_timestamp": ts(0),
		}),
		entityLine(created.Add(8*time.Minute), "k8s.pod", "k8s.pod.uid", "pod-uid-1", map[string]any{
			"k8s.pod.name": "p1", "k8s.node.name": "n1", "k8s.namespace.name": "default", "pod.creation_timestamp": ts(5 * time.Minute),
		}),
		entityLine(created.Add(9*time.Minute), "k8s.statefulset", "k8s.statefulset.uid", "ss-uid-1", map[string]any{
			"k8s.workload.name": "db", "k8s.namespace.name": "default",
		}),
	}
}

// writeFile writes lines to a file of dir.
func writeFile(t *testing.T, dir, name string, lines ...string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

// writeCapture writes a capture of two lines, a node and a pod scheduled on it, with the
// pod's line first in the file and observed last: the replay order is by time.
func writeCapture(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "run.jsonl",
		line(created.Add(6*time.Minute), "pods", podObject()),
		line(created.Add(time.Minute), "nodes", nodeObject()))
	return filepath.Join(dir, "run.jsonl")
}

// writeMixed writes a capture directory of two files: run1.jsonl, the two lines of
// writeCapture, and run2.jsonl, three entity events observed after them.
func writeMixed(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	writeFile(t, dir, "run1.jsonl",
		line(created.Add(6*time.Minute), "pods", podObject()),
		line(created.Add(time.Minute), "nodes", nodeObject()))
	writeFile(t, dir, "run2.jsonl", entityLines()...)
	return dir
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
		batches, counted, err := translate(in, n, objectsOnly)
		if err != nil {
			t.Fatal(err)
		}
		if counted.captured != 2 || counted.translated != 3 || counted.written != 3 || counted.objects != 3 || counted.cluster != 0 || counted.clusterSkipped != nil {
			t.Errorf("N = %d: counts %+v, want 2 log records, 3 translated (all by the objects translator) and 3 written", n, counted)
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
	res, err := run(context.Background(), tmp, in, []int{32, 40, 64}, objectsOnly)
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
// then a cost line for each length against 18. With the defaults the columns are those the
// tool has always printed, and nothing is said of the cluster translator.
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
	files, err := parseFiles(defaultFiles)
	if err != nil || !slices.Equal(files, []string{"run1.jsonl", "run2.jsonl"}) {
		t.Fatalf("the default files are %v, %v; want run1.jsonl and run2.jsonl", files, err)
	}
	m, err := parseTranslators(defaultTranslators)
	if err != nil || m != objectsOnly {
		t.Fatalf("the default translators are %v, %v; want the objects translator alone", m, err)
	}
	tmp := t.TempDir()
	var out bytes.Buffer
	if err := report(&out, dir, files, tmp, m, []int{32, 40, 64}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 1+5+3 {
		t.Fatalf("%d lines, want a header, five rows and three cost lines:\n%s", len(lines), out.String())
	}
	if got, want := strings.Join(strings.Fields(lines[0]), " "), "N capture log records translated written (records) live table bytes bytes written live bytes per record mean payload bytes"; got != want {
		t.Errorf("the header is %q, want %q", got, want)
	}
	for i, label := range []string{"empty", "18", "32", "40", "64"} {
		f := strings.Fields(lines[1+i])
		if f[0] != label || len(f) != 8 {
			t.Errorf("row %d is %q, want %q and 8 columns", i, lines[1+i], label)
		}
	}
	for i, n := range []string{"32", "40", "64"} {
		if want := "cost_a(" + n + ") = (live_" + n + " - live_18) / (live_18 - live_empty) = ("; !strings.HasPrefix(lines[6+i], want) {
			t.Errorf("cost line %d is %q, want it to begin %q", i, lines[6+i], want)
		}
	}
	if strings.Contains(out.String(), "k8scluster") {
		t.Errorf("the default output speaks of the cluster translator:\n%s", out.String())
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Errorf("the temporary directory holds %v (%v)", left, err)
	}
}

// pulled is a capture of an hour and a half of pulls every eight minutes of the node
// and the pod of writeCapture, with an entity event of each at the same instants. A refresh
// that comes between the bound of 7.5 minutes of a run's age that a TTL of fifteen minutes
// gives and the fixed bound of ten minutes is an extension under one and the start of a new
// run under the other, so the records tell the two apart.
func pulled(t *testing.T) []batch {
	t.Helper()
	dir := t.TempDir()
	var objects, entities []string
	for k := 0; k <= 12; k++ {
		at := created.Add(time.Duration(k) * 8 * time.Minute)
		objects = append(objects, line(at.Add(time.Minute), "nodes", nodeObject()), line(at.Add(6*time.Minute), "pods", podObject()))
		entities = append(entities,
			entityLine(at.Add(2*time.Minute), "k8s.node", "k8s.node.uid", "node-uid-1", map[string]any{"k8s.node.name": "n1", "node.creation_timestamp": ts(0)}),
			entityLine(at.Add(7*time.Minute), "k8s.pod", "k8s.pod.uid", "pod-uid-1", map[string]any{"k8s.pod.name": "p1", "k8s.node.name": "n1", "pod.creation_timestamp": ts(5 * time.Minute)}))
	}
	writeFile(t, dir, "objects.jsonl", objects...)
	writeFile(t, dir, "entities.jsonl", entities...)
	in, err := loadDir(dir, []string{"objects.jsonl", "entities.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	return in
}

// The default, the objects translator alone, is the pipeline the tool began with: the
// objects translator with a fixed TTL of fifteen minutes, the coalescer with its runs
// bounded at ten minutes of age, and every record of the capture, entity events included,
// going to the translator. The reference below is written out in full and not from the
// tool's constants.
func TestTheDefaultModeIsTheObjectsTranslatorAloneWithAFixedRunBound(t *testing.T) {
	t.Parallel()

	in := pulled(t)
	for _, n := range []int{18, 40} {
		p, err := producerName(n)
		if err != nil {
			t.Fatal(err)
		}
		tr, err := k8sobjects.New(k8sobjects.Config{Producer: p, Resolver: identity.NewResolver(catalog.Default()), EntityTTL: 15 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		fixed, err := coalesce.New(coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		ttlBound, err := coalesce.New(coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true})
		if err != nil {
			t.Fatal(err)
		}
		var want [][]store.Record
		var seq uint64
		var asFixed, asTTL []store.Record
		for _, b := range in {
			var recs []store.Record
			for _, it := range b {
				res, err := tr.Translate(it.res, it.rec)
				if err != nil {
					t.Fatal(err)
				}
				for _, r := range res.Records {
					recs = fixed.Add(r, recs)
					asTTL = ttlBound.Add(r, asTTL)
				}
			}
			asFixed = append(asFixed, recs...)
			for i := range recs {
				seq++
				recs[i].Seq = seq
			}
			if len(recs) > 0 {
				want = append(want, recs)
			}
		}
		if reflect.DeepEqual(asTTL, asFixed) {
			t.Fatalf("the two bounds on a run's age write the same %d records: this capture cannot tell them apart", len(asFixed))
		}
		got, counted, err := translate(in, n, objectsOnly)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("N = %d: the default pipeline wrote %d batches that differ from the objects translator alone with the fixed run bound (%d)", n, len(got), len(want))
		}
		if counted.cluster != 0 || counted.objects != counted.translated || counted.clusterSkipped != nil {
			t.Errorf("N = %d: counts %+v: the default must not use the cluster translator", n, counted)
		}
	}
}

// With both translators the entity events go to the cluster translator and the other
// records to the objects translator, each with its own producer of exactly N bytes, and
// all of it through one coalescer and one numbering.
func TestBothTranslatorsWriteTheirOwnProducersOfExactlyNBytes(t *testing.T) {
	t.Parallel()

	in, err := loadDir(writeMixed(t), []string{"run1.jsonl", "run2.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	for _, n := range []int{18, 32, 40, 64} {
		objects, err := producerName(n)
		if err != nil {
			t.Fatal(err)
		}
		cluster, err := clusterProducerName(n)
		if err != nil {
			t.Fatal(err)
		}
		if len(objects) != n || len(cluster) != n || objects == cluster || !strings.HasPrefix(string(objects), "k8sobjects/") || !strings.HasPrefix(string(cluster), "k8scluster/") {
			t.Fatalf("N = %d: producers %q and %q, want two of %d bytes, k8sobjects/... and k8scluster/...", n, objects, cluster, n)
		}
		if n == 18 && (objects != "k8sobjects/default" || cluster != "k8scluster/default") {
			t.Errorf("at 18 bytes the producers are %q and %q", objects, cluster)
		}
		batches, counted, err := translate(in, n, bothTranslators)
		if err != nil {
			t.Fatal(err)
		}
		// 5 log records: the node and the pod give 3 records through the objects translator; the
		// node, the pod and its edge give 3 through the cluster translator; the stateful set is set aside.
		if counted.captured != 5 || counted.objects != 3 || counted.cluster != 3 || counted.translated != 6 || counted.written != 6 {
			t.Errorf("N = %d: counts %+v, want 5 log records, 3 and 3 translated and 6 written", n, counted)
		}
		if want := map[string]int{k8scluster.SkipTypeNotInCatalog: 1}; !reflect.DeepEqual(counted.clusterSkipped, want) {
			t.Errorf("N = %d: the cluster translator set aside %v, want %v", n, counted.clusterSkipped, want)
		}
		byProducer := map[lifecycle.Producer]int{}
		seq := uint64(0)
		for _, b := range batches {
			for _, r := range b {
				byProducer[r.Producer]++
				if seq++; r.Seq != seq {
					t.Errorf("N = %d: Seq %d, want %d", n, r.Seq, seq)
				}
			}
		}
		if want := map[lifecycle.Producer]int{objects: 3, cluster: 3}; !reflect.DeepEqual(byProducer, want) {
			t.Errorf("N = %d: records by producer %v, want %v", n, byProducer, want)
		}
	}
}

// The cluster translator alone translates the entity events and sets aside nothing but
// what it cannot use; the objects records of the capture are not its input.
func TestClusterAloneTranslatesTheEntityEventsOnly(t *testing.T) {
	t.Parallel()

	in, err := loadDir(writeMixed(t), []string{"run1.jsonl", "run2.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	batches, counted, err := translate(in, 32, clusterOnly)
	if err != nil {
		t.Fatal(err)
	}
	cluster, _ := clusterProducerName(32)
	if counted.objects != 0 || counted.cluster != 3 || counted.written != 3 || counted.clusterSkipped[k8scluster.SkipTypeNotInCatalog] != 1 || len(counted.clusterSkipped) != 1 {
		t.Errorf("counts %+v, want 3 records of the cluster translator, 3 written, and the stateful set set aside", counted)
	}
	for _, b := range batches {
		for _, r := range b {
			if r.Producer != cluster {
				t.Errorf("a record of %q, want only %q", r.Producer, cluster)
			}
		}
	}
}

// The coalescer is set as the shipped one when the cluster translator is used (a run's age
// is bounded by its own extension interval, and the two ways to bound it are exclusive), and
// as before for the objects translator alone.
func TestTheCoalescerIsSetAsShippedWhenTheClusterTranslatorIsUsed(t *testing.T) {
	t.Parallel()

	for _, m := range []mode{clusterOnly, bothTranslators} {
		want := coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
		if got := coalesceConfig(m); got != want {
			t.Errorf("mode %d: the coalescer is set %+v, want %+v", m, got, want)
		}
		if _, err := coalesce.New(coalesceConfig(m)); err != nil {
			t.Errorf("mode %d: %v", m, err)
		}
	}
	if got, want := coalesceConfig(objectsOnly), (coalesce.Config{ExtendTTLFraction: 0.5, RunMaxAge: 10 * time.Minute}); got != want {
		t.Errorf("the objects translator alone has the coalescer set %+v, want %+v", got, want)
	}
}

// Each mode is measured: the store is compacted, the cost is the formula, the records
// written are counted, and no directory is left behind.
func TestEveryModeIsMeasuredAndCleansUp(t *testing.T) {
	t.Parallel()

	in, err := loadDir(writeMixed(t), []string{"run1.jsonl", "run2.jsonl"})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		m                       mode
		objects, cluster, wrote int
	}{{objectsOnly, 3, 0, 3}, {clusterOnly, 0, 3, 3}, {bothTranslators, 3, 3, 6}} {
		tmp := t.TempDir()
		res, err := run(context.Background(), tmp, in, []int{32, 64}, c.m)
		if err != nil {
			t.Fatal(err)
		}
		if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
			t.Errorf("mode %d: the temporary directory holds %v (%v)", c.m, left, err)
		}
		for _, r := range res.Rows()[1:] {
			if r.Captured != 5 || r.ByObjects != c.objects || r.ByCluster != c.cluster || r.Records != c.wrote || r.Translated != c.objects+c.cluster {
				t.Errorf("mode %d, N %s: %d captured, %d and %d translated, %d written; want 5, %d and %d, %d", c.m, r.Label, r.Captured, r.ByObjects, r.ByCluster, r.Records, c.objects, c.cluster, c.wrote)
			}
			if r.LiveTableBytes <= res.Empty.LiveTableBytes {
				t.Errorf("mode %d, N %s: %d live bytes, no more than the empty store's %d", c.m, r.Label, r.LiveTableBytes, res.Empty.LiveTableBytes)
			}
		}
		for _, r := range res.Lengths {
			got, err := res.CostOf(r)
			want := (float64(r.LiveTableBytes) - float64(res.Baseline.LiveTableBytes)) / (float64(res.Baseline.LiveTableBytes) - float64(res.Empty.LiveTableBytes))
			if err != nil || got != want {
				t.Errorf("mode %d: CostOf(%s) = %v, %v; want %v", c.m, r.Label, got, err, want)
			}
		}
	}
}

// With the cluster translator the table has two more columns, the records each translator
// made, and a last line says what the cluster translator set aside.
func TestTheReportOfBothTranslatorsHasTheExtraColumns(t *testing.T) {
	t.Parallel()

	tmp := t.TempDir()
	var out bytes.Buffer
	if err := report(&out, writeMixed(t), []string{"run1.jsonl", "run2.jsonl"}, tmp, bothTranslators, []int{32, 64}, time.Minute); err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(out.String(), "\n"), "\n")
	if len(lines) != 1+4+2+1 {
		t.Fatalf("%d lines, want a header, four rows, two cost lines and the line of what was set aside:\n%s", len(lines), out.String())
	}
	if got, want := strings.Join(strings.Fields(lines[0]), " "), "N capture log records translated written (records) live table bytes bytes written live bytes per record mean payload bytes translated by k8sobjects translated by k8scluster"; got != want {
		t.Errorf("the header is %q, want %q", got, want)
	}
	for i, label := range []string{"empty", "18", "32", "64"} {
		f := strings.Fields(lines[1+i])
		if f[0] != label || len(f) != 10 {
			t.Errorf("row %d is %q, want %q and 10 columns", i, lines[1+i], label)
		}
		if label != "empty" && (f[1] != "5" || f[2] != "6" || f[3] != "6" || f[8] != "3" || f[9] != "3") {
			t.Errorf("row %d is %q, want 5 captured, 6 translated, 6 written, 3 and 3", i, lines[1+i])
		}
	}
	if want := "k8scluster set aside, at every N: " + k8scluster.SkipTypeNotInCatalog + ": 1"; lines[len(lines)-1] != want {
		t.Errorf("the last line is %q, want %q", lines[len(lines)-1], want)
	}
	if left, err := os.ReadDir(tmp); err != nil || len(left) != 0 {
		t.Errorf("the temporary directory holds %v (%v)", left, err)
	}
}

// The files are read in the order given, each in its own replay order; one that is not
// there is refused.
func TestFilesAreReadInTheOrderGivenAndAMissingOneIsRefused(t *testing.T) {
	t.Parallel()

	dir := writeMixed(t)
	attrs := func(it item) []byte {
		b, _ := json.Marshal(it.rec.Attributes)
		return b
	}
	// Of a batch: whether it holds an entity event, or the pod's or the node's object.
	kinds := func(files ...string) []string {
		in, err := loadDir(dir, files)
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for _, b := range in {
			for _, it := range b {
				switch {
				case isEntityEvent(it.rec):
					out = append(out, "entity")
				case strings.Contains(string(attrs(it)), "nodes"):
					out = append(out, "node")
				default:
					out = append(out, "pod")
				}
			}
		}
		return out
	}
	if got, want := kinds("run1.jsonl", "run2.jsonl"), []string{"node", "pod", "entity", "entity", "entity"}; !slices.Equal(got, want) {
		t.Errorf("run1 then run2: %v, want %v (each file in the order of time)", got, want)
	}
	if got, want := kinds("run2.jsonl", "run1.jsonl"), []string{"entity", "entity", "entity", "node", "pod"}; !slices.Equal(got, want) {
		t.Errorf("run2 then run1: %v, want %v", got, want)
	}
	if got, want := kinds("run2.jsonl"), []string{"entity", "entity", "entity"}; !slices.Equal(got, want) {
		t.Errorf("run2 alone: %v, want %v", got, want)
	}
	if in, err := loadDir(dir, []string{"run1.jsonl", "run9.jsonl"}); err == nil {
		t.Errorf("a file that is not there was read: %v", in)
	}
	var out bytes.Buffer
	if err := report(&out, dir, []string{"run1.jsonl", "run9.jsonl"}, t.TempDir(), bothTranslators, []int{32}, time.Minute); err == nil || !strings.Contains(err.Error(), "run9.jsonl") {
		t.Errorf("a report of a missing file: %v, want an error that names it", err)
	}
	if out.Len() != 0 {
		t.Errorf("a refused report printed %q", out.String())
	}
}

func TestTranslatorsAndFilesAreParsedAndRefused(t *testing.T) {
	t.Parallel()

	for s, want := range map[string]mode{"objects": objectsOnly, "cluster": clusterOnly, "both": bothTranslators} {
		if got, err := parseTranslators(s); err != nil || got != want {
			t.Errorf("parseTranslators(%q) = %v, %v; want %v", s, got, err, want)
		}
	}
	for _, s := range []string{"", "all", "Both", "object", "objects,cluster", " both"} {
		if got, err := parseTranslators(s); err == nil {
			t.Errorf("parseTranslators(%q) = %v, want an error", s, got)
		}
	}
	for list, want := range map[string][]string{
		"run1.jsonl":            {"run1.jsonl"},
		"run5.jsonl,run1.jsonl": {"run5.jsonl", "run1.jsonl"}, // the order given
		" a.jsonl , b.jsonl ":   {"a.jsonl", "b.jsonl"},
	} {
		if got, err := parseFiles(list); err != nil || !slices.Equal(got, want) {
			t.Errorf("parseFiles(%q) = %v, %v; want %v", list, got, err, want)
		}
	}
	for _, list := range []string{"", " ", ",", "a.jsonl,", ",a.jsonl", "a.jsonl,,b.jsonl", "a.jsonl, ,b.jsonl"} {
		if got, err := parseFiles(list); err == nil {
			t.Errorf("parseFiles(%q) = %v, want an error: an empty entry", list, got)
		}
	}
}
