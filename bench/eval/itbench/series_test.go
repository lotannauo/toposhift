package itbench

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
)

// Most of the dataset's objects exports are not one pull but a series, at a fixed
// interval that jitters by a fraction of a second. The fixtures here are invented
// series of the invented shop.

var seriesBase = time.Date(2026, 3, 4, 10, 0, 0, 0, time.UTC)

// seriesOffsets are six pulls about five minutes apart: 300.4, 299.6, 300.4, ... s.
var seriesOffsets = []time.Duration{0, 300400 * time.Millisecond, 600 * time.Second, 899600 * time.Millisecond, 1200200 * time.Millisecond, 1499800 * time.Millisecond}

func pullTime(i int) time.Time { return seriesBase.Add(seriesOffsets[i]) }

func stampOf(t time.Time) string { return t.Format("2006-01-02 15:04:05.000000000") }

// series is an objects export of the six pulls; extra gives rows to add to pull i, and
// skip the uids a pull leaves out.
func series(extra func(i int, ts string) string, skip func(i int) map[string]bool) string {
	var b strings.Builder
	b.WriteString(tsvLine(objectColumns...))
	for i := range seriesOffsets {
		var sk map[string]bool
		if skip != nil {
			sk = skip(i)
		}
		ts := stampOf(pullTime(i))
		b.WriteString(shopPull(ts, sk))
		if extra != nil {
			b.WriteString(extra(i, ts))
		}
	}
	return b.String()
}

func readSeries(t testing.TB, content string) *Cluster {
	t.Helper()
	path := filepath.Join(t.TempDir(), ObjectsFile)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	cl, err := ReadCluster(path)
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

// A pod listed in six pulls is one pod, and five nodes listed in six pulls are five
// nodes. The pulls are found, with the interval between them and the lifetime the
// loader gives an object from it.
func TestASeriesOfPullsIsOneObjectOverTime(t *testing.T) {
	cl := readSeries(t, series(nil, nil))
	pods := 0
	for _, o := range cl.Objects {
		if o.Kind != "Pod" {
			continue
		}
		pods++
		if !o.First.Equal(pullTime(0)) || !o.Last.Equal(pullTime(5)) {
			t.Errorf("pod %s listed from %s to %s, want the whole series", o.Name, o.First, o.Last)
		}
	}
	if pods != 7 || len(cl.Nodes) != 2 || cl.Rows["pods"] != 42 || cl.Rows["nodes"] != 12 {
		t.Errorf("%d pods, %d nodes, rows %v; want 7 and 2 from 42 and 12 rows", pods, len(cl.Nodes), cl.Rows)
	}
	if cl.Pulls != 6 || cl.Interval != 5*time.Minute || cl.Lifetime != 15*time.Minute || !cl.First.Equal(pullTime(0)) || !cl.Last.Equal(pullTime(5)) {
		t.Errorf("%d pulls, interval %s, lifetime %s, %s .. %s", cl.Pulls, cl.Interval, cl.Lifetime, cl.First, cl.Last)
	}
	if len(cl.Load) != 6*9 {
		t.Errorf("%d records for the loader, want one for each listing", len(cl.Load))
	}
	for i := 1; i < len(cl.Load); i++ {
		if cl.Load[i].Record.ObservedTimeUnixNano < cl.Load[i-1].Record.ObservedTimeUnixNano {
			t.Fatal("the records of a series are not in time order")
		}
	}
	for _, o := range cl.Objects {
		if o.Kind == "Pod" && o.Name != "shop-lost-r1-p1" && !cl.Placed(o) {
			t.Errorf("pod %s is not placed on one of two nodes", o.Name)
		}
	}
	// Groups count objects, not listings.
	if m := MapGroup(cl, group("Deployment", `shop-front\b`)); m.Objects != 1 {
		t.Errorf("the front deployment is %d objects", m.Objects)
	}
	if m := MapGroup(cl, group("Pod", `shop-front-r1-.*`)); m.Objects != 2 || len(m.Pods) != 2 {
		t.Errorf("the front's pods are %d objects", m.Objects)
	}
	text := FormatTableB(rowFor(t, scenario1Truth), cl)
	for _, want := range []string{"cluster:  2 nodes, 6 pull(s), 2026-03-04T10:00:00Z .. 2026-03-04T10:24:59Z, 5m0s apart"} {
		if !strings.Contains(text, want) {
			t.Errorf("table B lacks %q:\n%s", want, text)
		}
	}
}

// A pod alive at the start of the window is one pod however many pulls list it, and
// one replaced before the window's start by a pod created after it leaves one pod
// alive. The pod is alive until the last pull that listed it plus the lifetime.
func TestSeedPodsAreCountedOncePerPod(t *testing.T) {
	replacement := strings.Replace(podBody("pod-cart-2", "shop-cart-r2-p1", "node-a", "shop-cart", ""), created, "2026-03-04T10:20:00Z", 1)
	content := series(
		func(i int, ts string) string {
			if i >= 4 {
				return objectRow("pods", replacement, ts)
			}
			return ""
		},
		func(i int) map[string]bool {
			if i >= 4 {
				return map[string]bool{"pod-cart-1": true}
			}
			return nil
		})
	cl := readSeries(t, content)
	truth := `
groups:
  - {id: root, kind: Pod, namespace: shop, filter: ["shop-cart-.*"], root_cause: true}
  - {id: front, kind: Service, namespace: shop, filter: ['shop-front\b']}
propagations:
  - {source: root, target: front}
`
	row := rowFor(t, truth)
	t0 := time.Date(2026, 3, 4, 10, 12, 0, 0, time.UTC)
	s := PlanSeed(row, cl, t0)
	if !s.OK() || s.Service != "shop-cart" || len(s.Pods) != 1 || s.Pods[0].UID != "pod-cart-1" || s.PodsWhy != "" {
		t.Fatalf("one pod was alive at the start, listed in four pulls: %+v", s)
	}
	// After the replacement is created and the old pod's lifetime has run out, the
	// replacement alone is alive; in between, both are, and the pod is not unique.
	var old *Object
	for _, o := range cl.Objects {
		if o.UID == "pod-cart-1" {
			old = o
		}
	}
	if !old.Last.Equal(pullTime(3)) || old.First != cl.First {
		t.Fatalf("the old pod was listed %s .. %s", old.First, old.Last)
	}
	if !cl.aliveAt(old, old.Last.Add(15*time.Minute-time.Nanosecond)) || cl.aliveAt(old, old.Last.Add(15*time.Minute)) {
		t.Error("a pod is alive until its last listing plus the lifetime")
	}
	both := PlanSeed(row, cl, time.Date(2026, 3, 4, 10, 21, 0, 0, time.UTC))
	if both.PodsWhy != "pod_not_unique_at_window_start" {
		t.Errorf("the old and the new pod were both alive: %+v", both)
	}
	late := PlanSeed(row, cl, pullTime(3).Add(15*time.Minute))
	if len(late.Pods) != 1 || late.Pods[0].UID != "pod-cart-2" {
		t.Errorf("after the old pod lapsed: %+v", late)
	}
}

// The facts of a series, end to end: the numbers are those of one pull of the same
// objects, because the objects have no gap between pulls.
func TestASeriesGivesTheNumbersOfOnePull(t *testing.T) {
	root := t.TempDir()
	scenario{n: 1, truth: scenario1Truth, objects: series(nil, nil), traces: shopTraces(), alerts: true}.write(t, root)
	r := runShop(t, root)["Scenario-1"]
	if r.Run.Outcome != blastscore.OutcomeScored {
		t.Fatalf("%+v", r.Run)
	}
	for _, w := range []struct {
		l              blastscore.Level
		p              blastscore.Predictor
		n, hits, truth int
	}{
		{blastscore.LevelService, blastscore.PredictorW, 3, 2, 2},
		{blastscore.LevelPod, blastscore.PredictorWPod, 3, 1, 2},
		{blastscore.LevelPod, blastscore.PredictorC, 2, 1, 2},
		{blastscore.LevelPod, blastscore.PredictorWC, 5, 2, 2},
		{blastscore.LevelPod, blastscore.PredictorBAll, 6, 2, 2},
	} {
		if n, hits, truth, ok := counts(r.Run, w.l, w.p); !ok || n != w.n || hits != w.hits || truth != w.truth {
			t.Errorf("%s %s = %d,%d,%d (scored %t), want %d,%d,%d", w.l, w.p, n, hits, truth, ok, w.n, w.hits, w.truth)
		}
	}
	d := r.Run.Diag
	if d.Nodes != 2 || !d.SnapshotKnown || !d.SnapshotInWindow {
		t.Errorf("diagnostics %+v: two nodes, and pulls that overlap the window", d)
	}
}
