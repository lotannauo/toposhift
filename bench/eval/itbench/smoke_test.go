package itbench

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/eval/factload"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The smoke tests read the dataset's fetched files, if they are there, and assert
// facts about their structure only: row counts, kinds, the presence of fields. They
// write nothing, copy nothing and print nothing from them. Where the data is not
// fetched they are skipped, as in CI.

// dataEnv names the directory of the fetched dataset.
const dataEnv = "TOPOSHIFT_EVAL_ITBENCH"

func dataDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv(dataEnv)
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Skip("no home directory")
		}
		dir = filepath.Join(home, "Data", "toposhift-eval", "itbench-lite", "d0916b08")
	}
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("the dataset is not fetched: %v", err)
	}
	return dir
}

func need(t *testing.T, path string) string {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Skipf("not fetched: %s", filepath.Base(path))
	}
	return path
}

func TestSmokeKubernetesObjectsFile(t *testing.T) {
	path := need(t, filepath.Join(dataDir(t), "Scenario-38", ObjectsFile))
	cl, err := ReadCluster(path)
	if err != nil {
		t.Fatal(err)
	}
	total := 0
	for _, n := range cl.Rows {
		total += n
	}
	if total != 784 || cl.Rows["pods"] != 177 || cl.Rows["nodes"] != 5 || len(cl.Nodes) != 5 || len(cl.Load) != 182 {
		t.Errorf("%d rows, %d pods, %d nodes (%d read), %d records for the loader", total, cl.Rows["pods"], cl.Rows["nodes"], len(cl.Nodes), len(cl.Load))
	}
	// One snapshot: every row lies within one minute.
	var lo, hi uint64
	for _, o := range cl.Load {
		ts := uint64(o.Record.ObservedTimeUnixNano)
		if lo == 0 || ts < lo {
			lo = ts
		}
		hi = max(hi, ts)
	}
	if hi-lo >= 60e9 {
		t.Errorf("the rows span %d s", (hi-lo)/1e9)
	}
	for _, o := range cl.Objects {
		if o.UID == "" || o.Name == "" || o.Created.IsZero() {
			t.Fatalf("a %s without a uid, a name or a creation time", o.Kind)
		}
		if o.Kind == "Pod" && o.Namespace == "" {
			t.Fatal("a pod without a namespace")
		}
	}

	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stats, err := factload.Load(context.Background(), st, factload.Scenario{Objects: cl.Load, Source: Dataset})
	if err != nil {
		t.Fatal(err)
	}
	if stats.ObjectRecords == 0 || stats.Resume.New == 0 || len(stats.Unplaced) != 0 {
		t.Errorf("records %d, resume %+v, unplaced %d", stats.ObjectRecords, stats.Resume, len(stats.Unplaced))
	}
}

func TestSmokeTracesFile(t *testing.T) {
	path := need(t, filepath.Join(dataDir(t), "Scenario-35", TracesFile))
	spans, err := ReadSpans(path)
	if err != nil {
		t.Fatal(err)
	}
	kinds := map[factload.SpanKind]int{}
	noNS, noPod := 0, 0
	for _, s := range spans {
		kinds[s.Kind]++
		if s.Service == "" || s.TraceID == "" || s.SpanID == "" || s.Start.IsZero() {
			t.Fatal("a span without a service, a trace, a span id or a start")
		}
		if s.Namespace == "" {
			noNS++
		}
		if s.PodUID == "" {
			noPod++
		}
	}
	if len(spans) != 10000 || kinds[factload.KindServer] != 4048 || kinds[factload.KindClient] != 3901 || kinds[factload.KindInternal] != 1915 ||
		kinds[factload.KindConsumer] != 101 || kinds[factload.KindProducer] != 35 {
		t.Errorf("%d spans, kinds %v", len(spans), kinds)
	}
	if noNS != 7 || noPod != 0 {
		t.Errorf("%d spans without a service namespace, %d without a pod uid; want 7, 0", noNS, noPod)
	}

	st, err := memstore.Open(memstore.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	stats, err := factload.Load(context.Background(), st, factload.Scenario{Spans: spans, Source: Dataset})
	if err != nil {
		t.Fatal(err)
	}
	// The spans with no namespace are of a service that has none with one: an
	// ordinary service, none of them dropped.
	bare := 0
	for k := range stats.Instrumented {
		if k.Namespace == "" {
			bare++
		}
	}
	if bare != 1 {
		t.Errorf("%d services without a namespace, want 1", bare)
	}
	if stats.Spans != 10000 || stats.NamespaceSplit != 0 || stats.Pairs == 0 || stats.DependsOnRuns == 0 || stats.InstanceRecords == 0 || stats.BridgeRecords == 0 {
		t.Errorf("%+v", stats)
	}
}

func TestSmokeGroundTruthFiles(t *testing.T) {
	dir := dataDir(t)
	scs, err := Scenarios(dir)
	if err != nil || len(scs) != 35 {
		t.Skipf("the 35 scenarios are not all there: %v", err)
	}
	rows, err := TableA(dir)
	if err != nil {
		t.Fatal(err)
	}
	unreadable, wrapped, filters, compiled := 0, 0, 0, 0
	for _, r := range rows {
		if r.Unreadable != "" {
			unreadable++
			continue
		}
		if b, err := os.ReadFile(filepath.Join(dir, r.ID, "ground_truth.yaml")); err == nil && strings.Contains(string(b), "kind: GroundTruth") {
			wrapped++
		}
		for _, g := range r.GT.Groups {
			for _, f := range g.Filter {
				filters++
				if _, err := regexp.Compile(`^(?:` + f + `)$`); err == nil {
					compiled++
				}
			}
		}
		if !r.HasRoot {
			t.Errorf("%s: no single root", r.ID)
		}
	}
	if len(rows) != 35 || unreadable != 1 || wrapped != 4 || filters != 217 || compiled != 216 {
		t.Errorf("%d rows, %d unreadable, %d wrapped, %d filters, %d compile; want 35, 1, 4, 217, 216", len(rows), unreadable, wrapped, filters, compiled)
	}
}
