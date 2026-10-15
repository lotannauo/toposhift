package itbench

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
	"github.com/lotannauo/toposhift/bench/eval/factload"
)

var update = flag.Bool("update", false, "rewrite the golden file in testdata")

// shop writes the three invented scenarios and returns their directory.
func shop(t testing.TB) string {
	t.Helper()
	root := t.TempDir()
	for i, truth := range []string{scenario1Truth, scenario2Truth, scenario3Truth} {
		scenario{n: i + 1, truth: truth, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	}
	return root
}

func runShop(t testing.TB, root string) map[string]Result {
	t.Helper()
	scs, err := Scenarios(root)
	if err != nil {
		t.Fatal(err)
	}
	pooled, err := PooledServices(root)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Result{}
	for _, s := range scs {
		r, err := RunScenario(context.Background(), s, pooled, blastscore.Config{})
		if err != nil {
			t.Fatal(err)
		}
		out[r.Row.ID] = r
	}
	return out
}

func counts(run blastscore.CaseRun, l blastscore.Level, p blastscore.Predictor) (n, hits, truth int, ok bool) {
	for _, s := range run.Scores {
		if s.Level == l && s.Predictor == p {
			return s.N, s.Hits, s.T, true
		}
	}
	return 0, 0, 0, false
}

func TestEndToEndOnInventedScenarios(t *testing.T) {
	res := runShop(t, shop(t))
	type want struct {
		l              blastscore.Level
		p              blastscore.Predictor
		n, hits, truth int
	}
	svc, pod := blastscore.LevelService, blastscore.LevelPod
	for id, ws := range map[string][]want{
		// A pod fault on the payment pods; the cart, the front and the proxy depend
		// on it, in that order, and the truth is the cart and the front.
		"Scenario-1": {
			{svc, blastscore.PredictorW, 3, 2, 2},
			{svc, blastscore.PredictorBAll, 3, 2, 2},
			{svc, blastscore.PredictorCSvc, 0, 0, 2},
			{svc, blastscore.PredictorB1Hop, 1, 1, 2},
			{svc, blastscore.PredictorBUnion, 3, 2, 2},
			{pod, blastscore.PredictorWPod, 3, 1, 2},
			{pod, blastscore.PredictorC, 2, 1, 2},
			{pod, blastscore.PredictorWC, 5, 2, 2},
			{pod, blastscore.PredictorBAll, 6, 2, 2},
		},
		// A configuration fault whose seed moves to the cart. The front and the proxy
		// depend on the cart, and the payment service, which is in the truth, is its
		// callee.
		"Scenario-2": {
			{svc, blastscore.PredictorW, 2, 1, 2},
			{svc, blastscore.PredictorCSvc, 2, 1, 2},
			{svc, blastscore.PredictorB1Hop, 2, 2, 2},
			{svc, blastscore.PredictorBUnion, 2, 1, 2},
			{pod, blastscore.PredictorWPod, 2, 0, 0},
		},
		// Memory pressure on the cart's pod, which shares a node with the front's
		// first pod and the proxy's.
		"Scenario-3": {
			{svc, blastscore.PredictorW, 2, 2, 2},
			{svc, blastscore.PredictorCSvc, 2, 2, 2},
			{pod, blastscore.PredictorWPod, 2, 2, 2},
			{pod, blastscore.PredictorC, 2, 2, 2},
			{pod, blastscore.PredictorBAll, 6, 2, 2},
		},
	} {
		r, ok := res[id]
		if !ok || r.Unavailable != "" || r.Run.Outcome != blastscore.OutcomeScored {
			t.Fatalf("%s: %+v", id, r.Run)
		}
		for _, w := range ws {
			n, hits, truth, ok := counts(r.Run, w.l, w.p)
			if !ok || n != w.n || hits != w.hits || truth != w.truth {
				t.Errorf("%s %s %s: n,hits,truth = %d,%d,%d (scored %t), want %d,%d,%d", id, w.l, w.p, n, hits, truth, ok, w.n, w.hits, w.truth)
			}
		}
	}

	d := res["Scenario-1"].Run.Diag
	if d.Nodes != 2 || !d.SnapshotKnown || !d.SnapshotInWindow || d.LabelFilterMapped != 3 || d.SeedMoved || !d.T0Proxy || d.T1Proxy ||
		d.SpansLoaded != 105 || d.DependsOnPieces != 3 || d.NamespaceSplit != 0 || d.MessagingEdgesOnPath != 0 || d.Unmappable != 0 || d.Unplaced != 0 {
		t.Errorf("Scenario-1 diagnostics: %+v", d)
	}
	if res["Scenario-1"].Run.Class != blastscore.ClassCallGraph || res["Scenario-3"].Run.Class != blastscore.ClassCoResidency {
		t.Error("classes")
	}
	if d := res["Scenario-2"].Run.Diag; !d.SeedMoved || d.AgainstDirection != 1 {
		t.Errorf("Scenario-2 diagnostics: %+v", d)
	}
	if res["Scenario-1"].Stats.Resume.New == 0 || len(res["Scenario-1"].Stats.Unplaced) != 1 {
		t.Errorf("load stats: resume %+v, unplaced %d", res["Scenario-1"].Stats.Resume, len(res["Scenario-1"].Stats.Unplaced))
	}
}

// The report of the three invented scenarios, as a golden file: the pooled strata
// are all "cannot say" at three cases, and the per-case numbers are the ones
// above.
func TestGoldenReportOfThreeScenarios(t *testing.T) {
	res := runShop(t, shop(t))
	var runs []blastscore.CaseRun
	for _, id := range []string{"Scenario-1", "Scenario-2", "Scenario-3"} {
		runs = append(runs, res[id].Run)
	}
	rep, err := blastscore.Evaluate(runs, blastscore.Meta{Dataset: Dataset, Loader: factload.Version})
	if err != nil {
		t.Fatal(err)
	}
	got := rep.Text()
	golden := filepath.Join("testdata", "report_three_scenarios.txt")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("rewrote %s", golden)
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if string(want) != got {
		t.Errorf("the report differs from %s; run with -update to see the new one", golden)
	}
	for _, s := range rep.Strata {
		if s.Verdict != "" && s.Verdict != blastscore.CannotSay {
			t.Errorf("%s %s %s: %s with three cases", s.Level, s.Class, s.Predictor, s.Verdict)
		}
	}
	if !rep.Dataset.Pass || rep.Dataset.Cases != 3 {
		t.Errorf("dataset check %+v", rep.Dataset)
	}
}

// Every scenario of the dataset stays in its count. Labels that are missing or do
// not parse, and missing objects, exclude the case and keep it in the count; a
// missing traces export leaves the seed absent, which is a load failure.
func TestScenariosThatCannotBeLoadedStayInTheCount(t *testing.T) {
	root := t.TempDir()
	scenario{n: 1, truth: "fault: [\n", alerts: true}.write(t, root)
	scenario{n: 2, truth: scenario1Truth, alerts: true, objects: shopObjects()}.write(t, root)         // no traces
	scenario{n: 3, truth: scenario1Truth, alerts: true, traces: shopTraces()}.write(t, root)           // no objects
	scenario{n: 4, truth: scenario1Truth, objects: shopObjects(), traces: shopTraces()}.write(t, root) // no alerts: no window
	scenario{n: 5, truth: scenario2Truth, alerts: true, objects: shopObjects(), traces: shopTraces()}.write(t, root)
	if err := os.MkdirAll(filepath.Join(root, "Scenario-6"), 0o755); err != nil { // no labels at all
		t.Fatal(err)
	}
	res := runShop(t, root)
	for id, c := range map[string]struct {
		why string
		ex  blastscore.Exclusion
	}{
		"Scenario-1": {UnavailableLabels, blastscore.ExcludedUnreadableLabels},
		"Scenario-6": {UnavailableNoLabel, blastscore.ExcludedUnreadableLabels},
		"Scenario-3": {UnavailableObjects, blastscore.ExcludedMissingObjects},
	} {
		r := res[id]
		if r.Unavailable != c.why || r.Run.Outcome != blastscore.OutcomeExcluded || r.Run.Exclusion != c.ex || r.Run.LoadFailed {
			t.Errorf("%s: %q, %+v", id, r.Unavailable, r.Run)
		}
	}
	if r := res["Scenario-2"]; r.Unavailable != UnavailableTraces || r.Run.Outcome != blastscore.OutcomeCannotSay || r.Run.Reason != blastscore.ReasonSeedAbsent || !r.Run.LoadFailed {
		t.Errorf("no traces: %q, %+v", r.Unavailable, r.Run)
	}
	if r := res["Scenario-4"]; r.Unavailable != "" || r.Run.Outcome != blastscore.OutcomeExcluded || r.Run.Exclusion != blastscore.ExcludedNoWindow {
		t.Errorf("no window: %+v", r.Run)
	}
	if r := res["Scenario-5"]; r.Run.Outcome != blastscore.OutcomeScored {
		t.Errorf("%+v", r.Run)
	}

	// All six are in the dataset's count, and only the missing traces fail the load check.
	var runs []blastscore.CaseRun
	for _, r := range res {
		runs = append(runs, r.Run)
	}
	rep, err := blastscore.Evaluate(runs, blastscore.Meta{Dataset: Dataset, Loader: factload.Version})
	if err != nil {
		t.Fatal(err)
	}
	d := rep.Dataset
	if d.Cases != 6 || d.Counted != 6 || d.LoadFailures != 1 || d.UnreadableLabels != 2 || d.MissingObjects != 1 || d.NoWindow != 1 {
		t.Errorf("%+v", d)
	}
}

// A node fault: the seed is the node, and only the pod level is scored. The walk
// from a node reaches the pods scheduled on it.
func TestANodeSeedIsScoredAtThePodLevelOnly(t *testing.T) {
	root := t.TempDir()
	truth := `
fault:
  - entity: {name: node-a, kind: Node, group_id: n}
    category: Infrastructure
    fault_mechanism: custom
groups:
  - {id: n, kind: Node, name: node-a, root_cause: true}
  - {id: cart, kind: Pod, namespace: shop, filter: ["shop-cart-r1-.*"]}
  - {id: pay, kind: Pod, namespace: shop, filter: ["shop-pay-r1-.*"]}
propagations:
  - {source: n, target: cart}
  - {source: n, target: pay}
`
	scenario{n: 4, truth: truth, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	pooled, err := PooledServices(root)
	if err != nil {
		t.Fatal(err)
	}
	r, err := RunScenario(context.Background(), filepath.Join(root, "Scenario-4"), pooled, blastscore.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if r.Run.Outcome != blastscore.OutcomeScored || r.Seed.Kind != SeedNode {
		t.Fatalf("%+v %+v", r.Seed, r.Run)
	}
	for _, s := range r.Run.Scores {
		if s.Level != blastscore.LevelPod {
			t.Errorf("a service-level score for a node seed: %s", s.Predictor)
		}
	}
	// node-a holds the front's first pod, the cart's and the proxy's; the truth is
	// the cart's pod, which is there, and the payment pod, which is on node-b.
	if n, hits, truth, ok := counts(r.Run, blastscore.LevelPod, blastscore.PredictorW); !ok || n != 3 || hits != 1 || truth != 2 {
		t.Errorf("W = %d,%d,%d (scored %t), want 3,1,2", n, hits, truth, ok)
	}
	if n, hits, truth, ok := counts(r.Run, blastscore.LevelPod, blastscore.PredictorBAll); !ok || n != 7 || hits != 2 || truth != 2 {
		t.Errorf("B_all = %d,%d,%d (scored %t), want 7,2,2", n, hits, truth, ok)
	}
}

// A truth service the traces never show is a miss and counted as uninstrumented; a
// seed that is one is excluded before anything is scored and is not a load failure.
func TestUninstrumentedServices(t *testing.T) {
	root := t.TempDir()
	truth := strings.NewReplacer("filter: ['shop-front\\b']", "filter: ['shop-db']").Replace(scenario2Truth)
	scenario{n: 1, truth: truth, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	seeded := `
groups:
  - {id: db, kind: StatefulSet, namespace: shop, filter: ["shop-db"], root_cause: true}
  - {id: cart, kind: Service, namespace: shop, filter: ['shop-cart\b']}
propagations:
  - {source: db, target: cart}
`
	scenario{n: 2, truth: seeded, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	res := runShop(t, root)

	// Scenario-1: the database is in the truth and is not in the store.
	r := res["Scenario-1"].Run
	if r.Outcome != blastscore.OutcomeScored || r.Diag.Uninstrumented != 1 {
		t.Fatalf("%+v", r)
	}
	if n, hits, truth, _ := counts(r, blastscore.LevelService, blastscore.PredictorW); n != 2 || hits != 0 || truth != 2 {
		// The front and the proxy are reached, and the truth is the database and the
		// payment service: a miss for both.
		t.Errorf("W = %d,%d,%d, want 2,0,2", n, hits, truth)
	}
	// Scenario-2: the seed is the database.
	r = res["Scenario-2"].Run
	if r.Outcome != blastscore.OutcomeExcluded || r.Exclusion != blastscore.ExcludedUninstrumentedSeed || r.LoadFailed {
		t.Errorf("%+v", r)
	}
}

// The uninstrumented set is pooled over all the trace data read. A seed that has
// spans in another scenario's data but none in its own is a load failure, not an
// exclusion; a seed with no span anywhere is excluded.
func TestThePooledSetDecidesWhoIsUninstrumented(t *testing.T) {
	root := t.TempDir()
	// The payment service has spans in Scenario-2 only.
	scenario{n: 1, truth: scenario1Truth, objects: shopObjects(), traces: onlyTraces("shop-front"), alerts: true}.write(t, root)
	scenario{n: 2, truth: scenario2Truth, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	res := runShop(t, root)
	pooled, err := PooledServices(root)
	if err != nil {
		t.Fatal(err)
	}
	if !pooled["shop-pay"] || !pooled["shop-front"] || pooled["shop-db"] {
		t.Errorf("pooled %v", pooled)
	}
	r := res["Scenario-1"].Run // the seed is the payment service, with no span in this scenario's file
	if r.Outcome != blastscore.OutcomeCannotSay || r.Reason != blastscore.ReasonSeedAbsent || !r.LoadFailed {
		t.Errorf("a seed with spans elsewhere: %+v", r)
	}
	if r := res["Scenario-2"].Run; r.Outcome != blastscore.OutcomeScored {
		t.Errorf("%+v", r)
	}
}

// A service whose spans never carry a namespace is an ordinary service, (none, name),
// and a truth name maps to it; none of its spans is dropped.
func TestANamespaceLessServiceStillExists(t *testing.T) {
	root := t.TempDir()
	scenario{n: 1, truth: scenario1Truth, objects: shopObjects(), traces: traces(false, "shop-front"), alerts: true}.write(t, root)
	r := runShop(t, root)["Scenario-1"]
	if r.Run.Outcome != blastscore.OutcomeScored || r.Stats.NamespaceSplit != 0 {
		t.Fatalf("%+v", r.Run)
	}
	// The front is in the truth and is reached: the same numbers as with a namespace.
	if n, hits, truth, ok := counts(r.Run, blastscore.LevelService, blastscore.PredictorW); !ok || n != 3 || hits != 2 || truth != 2 {
		t.Errorf("W = %d,%d,%d (scored %t), want 3,2,2", n, hits, truth, ok)
	}
	if r.Run.Diag.Unmappable != 0 || r.Run.Diag.Uninstrumented != 0 {
		t.Errorf("%+v", r.Run.Diag)
	}
}

func TestMessagingOnThePathIsCounted(t *testing.T) {
	root := t.TempDir()
	scenario{n: 1, truth: scenario1Truth, objects: shopObjects(), traces: traces(true), alerts: true}.write(t, root)
	r := runShop(t, root)["Scenario-1"]
	// The payment pods to the cart service is a producer-to-consumer pair, in the
	// other direction; the other steps are not.
	if r.Run.Diag.MessagingEdgesOnPath != 1 {
		t.Errorf("messaging_edges_on_path = %d, want 1", r.Run.Diag.MessagingEdgesOnPath)
	}
	if len(r.Stats.Messaging) != 1 {
		t.Errorf("messaging pairs %v", r.Stats.Messaging)
	}
	// The messaging dependency is a dependency like another for the walk: it
	// leaves the first scenario's answer as it was.
	if n, hits, truth, _ := counts(r.Run, blastscore.LevelService, blastscore.PredictorW); n != 3 || hits != 2 || truth != 2 {
		t.Errorf("W = %d,%d,%d, want 3,2,2", n, hits, truth)
	}
}

func TestNamespacesAreResolvedFromTheSpans(t *testing.T) {
	inst := map[factload.ServiceKey]struct{}{
		{Namespace: "a", Name: "one"}: {}, {Namespace: "a", Name: "two"}: {},
		{Namespace: "b", Name: "split"}: {}, {Namespace: "c", Name: "split"}: {},
	}
	n := newNamespaces(inst)
	for _, c := range []struct {
		name         string
		ns           string
		instrumented bool
		ok           bool
	}{
		{"one", "a", true, true},
		{"split", "", true, false},
		{"absent", "", false, false}, // services in several namespaces: no shared one
	} {
		ns, in, ok := n.of(c.name)
		if ns != c.ns || in != c.instrumented || ok != c.ok {
			t.Errorf("%s: %q, %t, %t; want %q, %t, %t", c.name, ns, in, ok, c.ns, c.instrumented, c.ok)
		}
	}
	one := newNamespaces(map[factload.ServiceKey]struct{}{{Namespace: "a", Name: "x"}: {}, {Namespace: "a", Name: "y"}: {}})
	if ns, in, ok := one.of("absent"); ns != "a" || in || !ok {
		t.Errorf("a name with no spans takes the namespace all share: %q %t %t", ns, in, ok)
	}
	// A name whose spans carry no namespace is (none, name); one seen both ways is the
	// namespaced service.
	bare := newNamespaces(map[factload.ServiceKey]struct{}{
		{Namespace: "", Name: "bare"}: {}, {Namespace: "a", Name: "both"}: {}, {Namespace: "", Name: "both"}: {}, {Namespace: "a", Name: "other"}: {},
	})
	if ns, in, ok := bare.of("bare"); ns != "" || !in || !ok {
		t.Errorf("a name with no namespace: %q %t %t", ns, in, ok)
	}
	if ns, in, ok := bare.of("both"); ns != "a" || !in || !ok {
		t.Errorf("a name seen both ways: %q %t %t", ns, in, ok)
	}
	none := newNamespaces(nil)
	if _, in, ok := none.of("x"); in || ok {
		t.Error("with no spans at all nothing resolves")
	}
}

// A seed seen only in a probe names file is instrumented: with no traces export its
// case is a load failure, not an exclusion. A service in no data stays uninstrumented.
func TestAProbeNamesFileMakesAServiceInstrumented(t *testing.T) {
	root := t.TempDir()
	scenario{n: 1, truth: scenario1Truth, objects: shopObjects(), alerts: true}.write(t, root) // seed shop-pay, no traces
	scenario{n: 2, truth: scenario2Truth, objects: shopObjects(), traces: onlyTraces("shop-front"), alerts: true}.write(t, root)
	run := func() map[string]Result { return runShop(t, root) }

	if r := run()["Scenario-1"].Run; r.Outcome != blastscore.OutcomeExcluded || r.Exclusion != blastscore.ExcludedUninstrumentedSeed {
		t.Fatalf("without a probe: %+v", r)
	}
	if err := os.MkdirAll(filepath.Join(root, ProbeDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ProbeDir, "Scenario-1.services"), []byte("shop-front\nshop-pay\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	r := run()["Scenario-1"]
	if r.Run.Outcome != blastscore.OutcomeCannotSay || r.Run.Reason != blastscore.ReasonSeedAbsent || !r.Run.LoadFailed || r.Unavailable != UnavailableTraces {
		t.Errorf("with a probe: %+v", r.Run)
	}
}

func TestProbeNamesFileFormat(t *testing.T) {
	dir := t.TempDir()
	write := func(content string) string {
		p := filepath.Join(dir, "x.services")
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
		return p
	}
	if got, err := ReadProbeNames(write("a\nb\nc-d\n")); err != nil || len(got) != 3 {
		t.Errorf("%v %v", got, err)
	}
	if got, err := ReadProbeNames(write("")); err != nil || len(got) != 0 {
		t.Errorf("an empty file: %v %v", got, err)
	}
	for _, bad := range []string{"a\nb", "b\na\n", "a\na\n", "a\n\nb\n", " a\n", "a \n"} {
		if _, err := ReadProbeNames(write(bad)); err == nil {
			t.Errorf("%q was read", bad)
		}
	}
	if got := PooledText(map[string]bool{"b": true, "a": true}); got != "a\nb\n" {
		t.Errorf("%q", got)
	}
}

// A node seed with no traces export is not a service that is absent: its facts are
// the objects, the node's existence decides the load check, and the pod level is
// scored.
func TestANodeSeedWithNoTracesIsLoadedFromTheObjects(t *testing.T) {
	root := t.TempDir()
	truth := `
groups:
  - {id: n, kind: Node, name: node-a, root_cause: true}
  - {id: cart, kind: Pod, namespace: shop, filter: ["shop-cart-r1-.*"]}
  - {id: pay, kind: Pod, namespace: shop, filter: ["shop-pay-r1-.*"]}
propagations:
  - {source: n, target: cart}
  - {source: n, target: pay}
`
	scenario{n: 4, truth: truth, objects: shopObjects(), alerts: true}.write(t, root)
	r := runShop(t, root)["Scenario-4"]
	if r.Unavailable != UnavailableTraces || r.Run.Outcome != blastscore.OutcomeScored || r.Run.LoadFailed {
		t.Fatalf("%q %+v", r.Unavailable, r.Run)
	}
	// The pods on the node: the cart's, the front's first and the proxy's; the truth
	// is the cart's and the payment pod, which is on the other node.
	if n, hits, truth, ok := counts(r.Run, blastscore.LevelPod, blastscore.PredictorW); !ok || n != 3 || hits != 1 || truth != 2 {
		t.Errorf("W = %d,%d,%d (scored %t), want 3,1,2", n, hits, truth, ok)
	}
	for _, s := range r.Run.Scores {
		if s.Level != blastscore.LevelPod {
			t.Errorf("a service-level score for a node seed: %s", s.Predictor)
		}
	}
	// A service seed with no traces is still a load failure.
	scenario{n: 5, truth: scenario1Truth, objects: shopObjects(), alerts: true}.write(t, root)
	scenario{n: 6, truth: scenario2Truth, objects: shopObjects(), traces: shopTraces(), alerts: true}.write(t, root)
	if r := runShop(t, root)["Scenario-5"]; !r.Run.LoadFailed || r.Run.Reason != blastscore.ReasonSeedAbsent {
		t.Errorf("a service seed with no traces: %+v", r.Run)
	}
}
