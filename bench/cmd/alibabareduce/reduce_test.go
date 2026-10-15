package main

import (
	"bytes"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"slices"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func series(ms, inst, node string, minutes ...int) []pl {
	var out []pl
	for _, m := range minutes {
		out = append(out, pl{min: m, ms: ms, inst: inst, node: node})
	}
	return out
}

func join(parts ...[]pl) []pl {
	var out []pl
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// gapPods is a pod seen with 0, 1, 2 and 3 samples missing after its first.
func gapPods() []pl {
	return join(
		series("MS_G", "MS_G_POD_A", "NODE_G", 0, 1),
		series("MS_G", "MS_G_POD_B", "NODE_G", 0, 2),
		series("MS_G", "MS_G_POD_C", "NODE_G", 0, 3),
		series("MS_G", "MS_G_POD_D", "NODE_G", 0, 4),
	)
}

// checkGaps reports the first way the intervals of gapPods are not the rule's.
func checkGaps(t testing.TB, cfg Config) error {
	want := map[string][]string{
		"MS_G_POD_A": {"NODE_G:0-4"},               // no sample missing: [0, 1 + 3)
		"MS_G_POD_B": {"NODE_G:0-5"},               // one missing
		"MS_G_POD_C": {"NODE_G:0-6"},               // two missing: a gap of exactly one TTL continues
		"MS_G_POD_D": {"NODE_G:0-3", "NODE_G:4-7"}, // three missing: one minute more splits it
	}
	for pod, w := range want {
		if got := intervalsOf(t, cfg, "c60", pod); !slices.Equal(got, w) {
			return &checkError{pod, got, w}
		}
	}
	return nil
}

type checkError struct {
	what      string
	got, want []string
}

func (e *checkError) Error() string {
	return e.what + ": got " + strings.Join(e.got, " ") + ", want " + strings.Join(e.want, " ")
}

func TestGapsSplitOnlyBeyondOneTTL(t *testing.T) {
	_, cfg := run(t, dataset{placement: gapPods()}, nil)
	if err := checkGaps(t, cfg); err != nil {
		t.Fatal(err)
	}
	// At C300 only minutes 0 and 5 are sampled and the TTL is 15 minutes: pod D's
	// samples at 0 and 4 leave one sample (0), one interval.
	if got := intervalsOf(t, cfg, "c300", "MS_G_POD_D"); !slices.Equal(got, []string{"NODE_G:0-15"}) {
		t.Fatalf("c300 intervals of pod D = %v", got)
	}
}

// boundaryPod is seen across the first archive's last minute and the second's first.
func boundaryPod() []pl { return series("MS_B", "MS_B_POD_0", "NODE_B", 4, 5, 6, 7) }

func checkBoundary(t testing.TB, cfg Config) error {
	if got := intervalsOf(t, cfg, "c60", "MS_B_POD_0"); !slices.Equal(got, []string{"NODE_B:4-10"}) {
		return &checkError{"boundary pod", got, []string{"NODE_B:4-10"}}
	}
	return nil
}

func TestIntervalAcrossFileBoundaryIsOne(t *testing.T) {
	_, cfg := run(t, dataset{placement: boundaryPod()}, nil)
	if err := checkBoundary(t, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestDirtIsCountedAndKept(t *testing.T) {
	d := dataset{placement: join(
		series("MS_D", "MS_D_POD_DUP", "NODE_1", 2, 2), // identical rows
		series("MS_D", "MS_D_POD_TWO", "NODE_1", 3),
		series("MS_D", "MS_D_POD_TWO", "NODE_2", 3), // two nodes at one instant
		series("MS_D", "MS_D_POD_MOVE", "NODE_1", 1),
		series("MS_D", "MS_D_POD_MOVE", "NODE_2", 2), // a move
		series("MS_X", "MS_D_POD_NAME", "NODE_1", 1),
		series("MS_Y", "MS_D_POD_NAME", "NODE_1", 2), // an msname change
	)}
	res, cfg := run(t, d, nil)

	// The duplicate row is counted once, however many times the samples are read.
	if res.place.dupRows != 1 {
		t.Errorf("%d duplicate rows counted, want 1", res.place.dupRows)
	}
	counts := res.place.dirt.counts
	want := map[string]int64{dirtDuplicate: 1, dirtDoublePlacement: 1, dirtMove: 1, dirtMSNameChange: 1}
	for k, n := range want {
		if counts[k] != n {
			t.Errorf("dirt %s = %d, want %d (all: %v)", k, counts[k], n, counts)
		}
	}
	cases := table(t, outPath(cfg, "dirt_cases.tsv"))
	if len(cases) != 4 {
		t.Fatalf("dirt_cases has %d rows, want 4: %v", len(cases), cases)
	}
	// A duplicate is idempotent: the pod is one sample of one interval.
	if got := intervalsOf(t, cfg, "c60", "MS_D_POD_DUP"); !slices.Equal(got, []string{"NODE_1:2-5"}) {
		t.Errorf("duplicate pod intervals = %v", got)
	}
	// Both edges of a double placement are asserted: the pod is on two nodes at once.
	if got := intervalsOf(t, cfg, "c60", "MS_D_POD_TWO"); !slices.Equal(got, []string{"NODE_1:3-6", "NODE_2:3-6"}) {
		t.Errorf("double placement intervals = %v", got)
	}
	if got := intervalsOf(t, cfg, "c60", "MS_D_POD_MOVE"); !slices.Equal(got, []string{"NODE_1:1-4", "NODE_2:2-5"}) {
		t.Errorf("move intervals = %v", got)
	}
}

func TestEverySkipReasonIsCountedAndAccounted(t *testing.T) {
	d := dataset{rawPlacement: snippet(t, "snippet_placement.csv"), rawCalls: snippet(t, "snippet_calls.csv")}
	res, cfg := run(t, d, nil)
	got := map[string]int64{}
	for _, row := range table(t, outPath(cfg, "skips.tsv")) {
		if row["file"] == "MSMetricsUpdate/MSMetricsUpdate_0.tar.gz" || row["file"] == "CallGraph/CallGraph_0.tar.gz" {
			got[row["table"]+"/"+row["key"]] = atoi(t, row["value"])
		}
	}
	want := map[string]int64{
		"MSMetricsUpdate/columns": 1, "MSMetricsUpdate/timestamp": 2, "MSMetricsUpdate/empty_id": 2, "MSMetricsUpdate/shifted": 0,
		"CallGraph/columns": 2, "CallGraph/shifted": 1, "CallGraph/timestamp": 1, "CallGraph/empty_id": 1, "CallGraph/unknown_endpoint": 4,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("skips %s = %d, want %d", k, got[k], v)
		}
	}
	// Rows read are rows used plus the skips, for every file.
	for _, f := range res.files {
		var sum int64
		for _, n := range f.Skips {
			sum += n
		}
		if f.Rows != f.Used+sum {
			t.Errorf("%s: %d rows, %d used, %d skipped", f.Name, f.Rows, f.Used, sum)
		}
	}
}

func atoi(t testing.TB, s string) int64 {
	t.Helper()
	var n int64
	for _, c := range s {
		if c < '0' || c > '9' {
			t.Fatalf("not a number: %q", s)
		}
		n = n*10 + int64(c-'0')
	}
	return n
}

// callsData has an unknown endpoint, a self-call, and two calls in one cell.
func callsData() dataset {
	return dataset{calls: []cl{
		{ts: 0, trace: "T_1", um: "MS_A", umi: "MS_A_POD_1", dm: "MS_B", dmi: "MS_B_POD_1", rt: "2.5"},
		{ts: 100, trace: "T_2", um: "MS_A", umi: "MS_A_POD_2", dm: "MS_B", dmi: "MS_B_POD_1", rt: "3.0"},
		{ts: 200, trace: "T_2", um: "MS_A", umi: "MS_A_POD_2", dm: "MS_B", dmi: "MS_B_POD_1", rt: "None"},
		{ts: 300, trace: "T_3", um: "MS_C", umi: "MS_C_POD_1", dm: "MS_C", dmi: "MS_C_POD_2"},
		{ts: 400, trace: "T_4", um: "UNKNOWN", umi: "UNKNOWN_POD_1", dm: "MS_B", dmi: "MS_B_POD_1"},
	}}
}

func checkUnknownIsNoService(res *results) error {
	for _, bad := range []string{"UNKNOWN", "UNAVAILABLE"} {
		if _, ok := res.ids.svcs.ids[bad]; ok {
			return &checkError{what: "service " + bad + " exists"}
		}
	}
	return nil
}

func TestCallsRollupEdgesAndUnknownEndpoints(t *testing.T) {
	res, cfg := run(t, callsData(), nil)
	if err := checkUnknownIsNoService(res); err != nil {
		t.Fatal(err)
	}
	var cell map[string]string
	for _, row := range table(t, outPath(cfg, "call_rollup_minutes.tsv.gz")) {
		if row["um"] == "MS_A" && row["dm"] == "MS_B" && row["minute"] == "0" {
			cell = row
		}
	}
	if cell == nil {
		t.Fatal("no rollup cell for MS_A -> MS_B at minute 0")
	}
	// Three calls, two with a response time (2.5 + 3.0 in thousandths), in two traces.
	for k, v := range map[string]string{"calls": "3", "rt_count": "2", "rt_sum_milli": "5500", "traces": "2", "rpctype": "rpc"} {
		if cell[k] != v {
			t.Errorf("cell %s = %s, want %s", k, cell[k], v)
		}
	}
	// The self-call is in the rollup and not among the edges.
	var self bool
	for _, row := range table(t, outPath(cfg, "call_rollup_minutes.tsv.gz")) {
		self = self || (row["um"] == "MS_C" && row["dm"] == "MS_C")
	}
	if !self || res.calls.selfCalls != 1 {
		t.Errorf("self-call rows kept = %v, counted = %d", self, res.calls.selfCalls)
	}
	for _, row := range table(t, outPath(cfg, "call_edges_intervals.tsv.gz")) {
		if row["um"] == row["dm"] || row["um"] == "UNKNOWN" {
			t.Errorf("edge %v", row)
		}
	}
	if res.calls.rtMissing != 1 {
		t.Errorf("missing response times = %d, want 1", res.calls.rtMissing)
	}
}

func TestPerFileTimestampsGiveTheSameReduction(t *testing.T) {
	d := dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}
	abs, absCfg := run(t, d, nil)
	d.perFile = true
	rel, relCfg := run(t, d, nil)
	for _, f := range rel.files {
		if f.Index > 0 && f.Layout != "per-file" {
			t.Errorf("%s: layout %q, want per-file", f.Name, f.Layout)
		}
	}
	for _, f := range abs.files {
		if f.Layout != "absolute" {
			t.Errorf("%s: layout %q, want absolute", f.Name, f.Layout)
		}
	}
	for _, name := range []string{"placement_intervals.tsv.gz", "call_rollup_minutes.tsv.gz", "call_edges_intervals.tsv.gz", "placement_c60.parquet", "calls.parquet"} {
		a, b := readFile(t, outPath(absCfg, name)), readFile(t, outPath(relCfg, name))
		if !bytes.Equal(a, b) {
			t.Errorf("%s differs between absolute and per-file timestamps", name)
		}
	}
}

func readFile(t testing.TB, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRowOrderWithinAnArchiveDoesNotMatter(t *testing.T) {
	d := dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}
	_, a := run(t, d, nil)
	d.shuffle = true
	_, b := run(t, d, nil)
	for _, name := range []string{"placement_intervals.tsv.gz", "call_rollup_minutes.tsv.gz"} {
		if !bytes.Equal(readFile(t, outPath(a, name)), readFile(t, outPath(b, name))) {
			t.Errorf("%s depends on the order of rows", name)
		}
	}
}

func TestRunsAreByteIdentical(t *testing.T) {
	d := dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}
	in := d.withFiller().write(t)
	var cfgs [2]Config
	for i := range cfgs {
		cfgs[i] = testConfig(t, in)
		if _, err := reduce(cfgs[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{
		"placement_c60.parquet", "placement_c300.parquet", "calls.parquet", "fixture_4min_200nodes.parquet",
		"placement_intervals.tsv.gz", "placement_minutes.tsv.gz", "call_rollup_minutes.tsv.gz", "call_edges_intervals.tsv.gz",
		"dirt_cases.tsv", "skips.tsv",
	} {
		if !bytes.Equal(readFile(t, outPath(cfgs[0], name)), readFile(t, outPath(cfgs[1], name))) {
			t.Errorf("%s differs between two runs", name)
		}
	}
	for _, name := range []string{"params.json", "REDUCTION.md"} {
		if !bytes.Equal(readFile(t, filepath.Join(cfgs[0].OutDir, name)), readFile(t, filepath.Join(cfgs[1].OutDir, name))) {
			t.Errorf("%s differs between two runs", name)
		}
	}
}

// TestStreamsAreTheProductsRecords reads every stream back and checks it as the
// product would: each record passes Validate, Seq runs from 1 with no gap, the
// producers and layers are the mapping's, and no record is a Delete.
func TestStreamsAreTheProductsRecords(t *testing.T) {
	d := dataset{placement: join(gapPods(), boundaryPod()), calls: callsData().calls}
	res, cfg := run(t, d, nil) // VerifyStreams has also replayed every stream into the memory store
	for _, name := range []string{"placement_c60.parquet", "placement_c300.parquet", "calls.parquet", "fixture_4min_200nodes.parquet"} {
		recs := records(t, outPath(cfg, name))
		if len(recs) == 0 {
			t.Fatalf("%s is empty", name)
		}
		for i, r := range recs {
			if err := r.Validate(); err != nil {
				t.Fatalf("%s: %v", name, err)
			}
			if r.Seq != uint64(i+1) {
				t.Fatalf("%s: record %d has Seq %d", name, i, r.Seq)
			}
			if r.Kind != lifecycle.Observe {
				t.Fatalf("%s: record %d is %s", name, i, r.Kind)
			}
			if r.EventTimeBasis != store.BasisProducerEvent {
				t.Fatalf("%s: basis %s", name, r.EventTimeBasis)
			}
			switch r.Subject.Kind {
			case store.SubjectEdge:
				if r.Subject.Relation == catalog.DependsOn && (r.Producer != callsProducer || r.Layer != catalog.L3) {
					t.Fatalf("%s: depends_on by %s in %s", name, r.Producer, r.Layer)
				}
				if r.Subject.Relation == catalog.ScheduledOn && (r.Producer != placementProducer || r.Layer != catalog.L2) {
					t.Fatalf("%s: scheduled_on by %s in %s", name, r.Producer, r.Layer)
				}
			default:
				if r.Producer != placementProducer {
					t.Fatalf("%s: entity by %s", name, r.Producer)
				}
			}
		}
	}
	for _, name := range []string{"placement_c60", "placement_c300", "calls"} {
		if res.asked[name] == 0 {
			t.Errorf("the in-process store was asked nothing about %s", name)
		}
	}
	if res.asked["fixture"] == 0 {
		t.Error("the in-process store was asked nothing about the fixture")
	}
}

// payloadCheck is whether every pod record carries exactly its msname.
func checkPayloads(t testing.TB, cfg Config) error {
	pods := map[string]int{}
	for _, r := range records(t, outPath(cfg, "placement_c60.parquet")) {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sPod {
			if !strings.HasPrefix(string(r.Payload), `{"msname":"MS_`) || strings.Contains(string(r.Payload), "cpu") || strings.Count(string(r.Payload), ",") > 0 {
				return &checkError{what: "payload " + string(r.Payload)}
			}
			pods[r.Subject.A.String()]++
		}
	}
	// A pod seen every minute with a changing CPU utilization is one run, then
	// one more at most from the coalescer's age bound: well under one record per sample.
	pod := fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, "MS_P_POD_0")
	if n := pods[pod.String()]; n > hourLen*3/4 {
		return &checkError{what: "pod records", got: []string{itoa(int64(n))}}
	}
	return nil
}

func fingerprintOf(typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		panic(err)
	}
	return id.Fingerprint()
}

func cpuPod() []pl {
	var out []pl
	for m := 0; m < hourLen; m++ {
		out = append(out, pl{min: m, ms: "MS_P", inst: "MS_P_POD_0", node: "NODE_P", cpu: "0." + itoa(int64(100+m))})
	}
	return out
}

func TestMeasurementsAreNotInThePayload(t *testing.T) {
	_, cfg := run(t, dataset{placement: cpuPod()}, nil)
	if err := checkPayloads(t, cfg); err != nil {
		t.Fatal(err)
	}
}

// The service is the msname column, never read off the instance token.
func serviceOfPod() []pl { return series("MS_REAL", "MS_OTHER_POD_0", "NODE_T", 1, 2) }

func checkServiceColumn(res *results) error {
	if _, ok := res.ids.names.ids["MS_REAL"]; !ok {
		return &checkError{what: "the msname column is not the service"}
	}
	if _, ok := res.ids.names.ids["MS_OTHER"]; ok {
		return &checkError{what: "the service was read off the instance token"}
	}
	return nil
}

func TestServiceIsTheMSNameColumn(t *testing.T) {
	res, _ := run(t, dataset{placement: serviceOfPod()}, nil)
	if err := checkServiceColumn(res); err != nil {
		t.Fatal(err)
	}
}

// fixtureData has forty nodes with four pods each for the first minutes.
func fixtureData() dataset {
	var rows []pl
	for n := 0; n < 40; n++ {
		for p := 0; p < 4; p++ {
			for m := 0; m < 4; m++ {
				rows = append(rows, pl{min: m, ms: "MS_FX" + itoa(int64(p)), inst: "MS_FX" + itoa(int64(p)) + "_POD_" + itoa(int64(n)), node: "NODE_FX" + itoa(int64(n))})
			}
		}
	}
	return dataset{placement: rows}
}

// checkFixtureNodes is whether the fixture keeps exactly the nodes the hash rule
// picks, each with all of its samples (4 pods, 4 minutes).
func checkFixtureNodes(res *results) error {
	if len(res.fixtureNodeRows) == 0 {
		return &checkError{what: "the fixture has no node"}
	}
	for node, rows := range res.fixtureNodeRows {
		if node == "NODE_FILL" {
			continue
		}
		if rows != 16 {
			return &checkError{what: "node " + node + " has " + itoa(int64(rows)) + " fixture samples, want 16"}
		}
	}
	return nil
}

func TestFixtureKeepsWholeNodes(t *testing.T) {
	res, _ := run(t, fixtureData(), func(c *Config) { c.FixtureNodes = 8 })
	if err := checkFixtureNodes(res); err != nil {
		t.Fatal(err)
	}
	// The kept set is the rule's, computed here from the threshold's definition.
	nodes := res.ids.nodes.size()
	limit := referenceThreshold(nodes, 8)
	var want []string
	for _, name := range res.ids.nodes.names {
		if nodeHash(name) < limit && name != "NODE_FILL" {
			want = append(want, name)
		}
	}
	var got []string
	for n := range res.fixtureNodeRows {
		if n != "NODE_FILL" {
			got = append(got, n)
		}
	}
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(want, got) || len(got) == 0 {
		t.Fatalf("fixture nodes = %v, want %v", got, want)
	}
}

func TestNodeSamplingIsDeterministic(t *testing.T) {
	var names []string
	for i := 0; i < 1000; i++ {
		names = append(names, "NODE_"+itoa(int64(i)))
	}
	pick := func() []string {
		limit := keepThreshold(len(names), 200)
		var kept []string
		for _, n := range names {
			if nodeHash(n) < limit {
				kept = append(kept, n)
			}
		}
		return kept
	}
	a, b := pick(), pick()
	if !slices.Equal(a, b) {
		t.Fatal("the kept set changes between calls")
	}
	// About 200 of 1000: the count of a fixed set, bounded by five standard deviations.
	if len(a) < 137 || len(a) > 263 {
		t.Fatalf("kept %d of 1000 nodes, want about 200", len(a))
	}
	if keepThreshold(150, 200) != ^uint64(0) {
		t.Fatal("with fewer nodes than wanted every node is kept")
	}
	if got, want := keepThreshold(1000, 200), referenceThreshold(1000, 200); got != want {
		t.Fatalf("threshold = %d, want %d", got, want)
	}
}

func TestMemoryPlanBehaviours(t *testing.T) {
	if got := unsafe.Sizeof(prow{}); got != 12 {
		t.Errorf("a placement row is %d bytes, want 12", got)
	}
	// Tokens are interned: the same token is the same id, and a clone is held.
	in := newInterner()
	s := string([]byte("MS_1"))
	if a, b := in.id(s), in.id("MS_1"); a != b || a != 1 {
		t.Errorf("ids %d and %d", a, b)
	}
	// Call archives are read one at a time: after a run no cell survives in the run.
	res, _ := run(t, callsData(), nil)
	if len(res.calls.files) != hourLen/callSpan {
		t.Errorf("%d call archives read", len(res.calls.files))
	}
	// Every archive is listed once, however many passes read it.
	if want := 2 + len(res.calls.files); len(res.files) != want {
		t.Errorf("%d archives listed, want %d", len(res.files), want)
	}
	// A finished stream lets go of its coalescer and its flush buffer.
	for _, c := range res.cads {
		if c.sink.co != nil || c.sink.buf != nil {
			t.Errorf("%s keeps its coalescer or its buffer after it finished", c.sink.name)
		}
	}
	// A finished tracker keeps its intervals and none of its open state.
	for _, c := range res.cads {
		if c.pods.open() != 0 || c.nodes.open() != 0 || c.edges.open() != 0 {
			t.Errorf("%s: a finished tracker has open intervals", c.spec.name)
		}
	}
	// The stream numbers records from 1.
	for _, s := range res.sinks {
		if s.Written && s.MinSeq != 1 {
			t.Errorf("%s starts at Seq %d", s.Name, s.MinSeq)
		}
	}
}

func TestDistinctTracesAreHashedAndExact(t *testing.T) {
	var cs []cl
	for i := 0; i < 50; i++ {
		cs = append(cs, cl{ts: 10, trace: "T_" + itoa(int64(i%20)), um: "MS_A", umi: "MS_A_POD_1", dm: "MS_B", dmi: "MS_B_POD_1"})
	}
	_, cfg := run(t, dataset{calls: cs}, nil)
	for _, row := range table(t, outPath(cfg, "call_rollup_minutes.tsv.gz")) {
		if row["um"] == "MS_A" && row["dm"] == "MS_B" {
			if row["calls"] != "50" || row["traces"] != "20" {
				t.Fatalf("cell = %v, want 50 calls in 20 traces", row)
			}
		}
	}
}

func TestLimitsStopOrDropAsPlanned(t *testing.T) {
	d := dataset{placement: gapPods(), calls: callsData().calls}
	cfg := testConfig(t, d.withFiller().write(t))
	cfg.MaxCallKeys = 1
	if _, err := reduce(cfg); err == nil || !strings.Contains(err.Error(), "rollup cells") {
		t.Fatalf("a call archive over the cell limit: %v", err)
	}

	cfg = testConfig(t, d.withFiller().write(t))
	cfg.MaxFileBytes = 64
	cfg.VerifyStreams = false
	res, err := reduce(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range res.sinks {
		if !s.DroppedOverLimit || s.Records == 0 || s.Written {
			t.Errorf("%s: dropped %v, records %d, written %v", s.Name, s.DroppedOverLimit, s.Records, s.Written)
		}
		if _, err := os.Stat(filepath.Join(cfg.OutDir, "out", s.Name+".parquet")); err == nil {
			t.Errorf("%s: the file was kept", s.Name)
		}
	}

	cfg = testConfig(t, d.withFiller().write(t))
	cfg.WriteC60 = false
	cfg.VerifyStreams = false
	res, err = reduce(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if res.sinks[0].Written || res.sinks[0].Records == 0 {
		t.Errorf("c60 not written but counted: %+v", res.sinks[0])
	}
}

func TestInputsAreChecked(t *testing.T) {
	d := dataset{placement: gapPods()}.withFiller()

	t.Run("checksum", func(t *testing.T) {
		in := d.write(t)
		p := filepath.Join(in, placementTable, "MSMetricsUpdate_1.tar.gz")
		b := readFile(t, p)
		b[len(b)/2] ^= 1
		if err := os.WriteFile(p, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := reduce(testConfig(t, in)); err == nil {
			t.Fatal("a changed archive was read")
		}
	})
	t.Run("header", func(t *testing.T) {
		in := d.write(t)
		blob := tarGz(t, "x.csv", "timestamp,msname\n1,2\n")
		if err := os.WriteFile(filepath.Join(in, placementTable, "MSMetricsUpdate_0.tar.gz"), blob, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := reduce(testConfig(t, in)); err == nil || !strings.Contains(err.Error(), "header") {
			t.Fatalf("a wrong header: %v", err)
		}
	})
	t.Run("missing archive", func(t *testing.T) {
		in := d.write(t)
		if err := os.Remove(filepath.Join(in, callsTable, "CallGraph_1.tar.gz")); err != nil {
			t.Fatal(err)
		}
		if _, err := reduce(testConfig(t, in)); err == nil {
			t.Fatal("an hour with an archive missing was reduced")
		}
	})
	t.Run("gap in the hour", func(t *testing.T) {
		g := dataset{}
		for m := 0; m < hourLen; m++ {
			if m != 3 {
				g.placement = append(g.placement, pl{min: m, ms: "MS_FILL", inst: "MS_FILL_POD_0", node: "NODE_FILL"})
			}
		}
		for f := 0; f < hourLen/callSpan; f++ {
			g.calls = append(g.calls, cl{ts: int64(f) * callSpan * 60000, trace: "T", um: "MS_A", umi: "MS_A_POD_0", dm: "MS_B", dmi: "MS_B_POD_0"})
		}
		if _, err := reduce(testConfig(t, g.write(t))); err == nil || !strings.Contains(err.Error(), "tile") {
			t.Fatalf("a minute without a sample: %v", err)
		}
	})
	t.Run("layout", func(t *testing.T) {
		g := d
		g.rawPlacement = []string{"6000000,MS_Z,MS_Z_POD_0,NODE_Z,0.1,0.2"}
		if _, err := reduce(testConfig(t, g.write(t))); err == nil || !strings.Contains(err.Error(), "tile") {
			t.Fatalf("a timestamp outside the archive's half hour: %v", err)
		}
	})
	t.Run("output in a work tree", func(t *testing.T) {
		cfg := testConfig(t, d.write(t))
		cfg.OutDir = filepath.Join(".", "out-in-the-repository")
		if _, err := reduce(cfg); err == nil || !strings.Contains(err.Error(), "git work tree") {
			t.Fatalf("an output directory inside a work tree: %v", err)
		}
		if _, err := os.Stat(cfg.OutDir); err == nil {
			t.Fatal("something was created inside the work tree")
		}
	})
}

// TestFlushClockFollowsTheProductsPeriod checks that the simulated ingest clock
// flushes every coalesce.FlushPeriod, so that the cadence follows the product's.
func TestFlushClockFollowsTheProductsPeriod(t *testing.T) {
	s, err := newSink("t", "", false, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	fp := fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, "P")
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		if err := s.assert(store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: placementProducer,
			EventTime: start.Add(time.Duration(i) * 10 * time.Minute), Kind: lifecycle.Observe, TTL: 3 * time.Minute,
		}); err != nil {
			t.Fatal(err)
		}
	}
	first := start.Add(time.Second).Add(coalesce.FlushPeriod)
	if d := s.next.Sub(first); d < 0 || d%coalesce.FlushPeriod != 0 {
		t.Fatalf("the next flush is %s after the first, not a whole number of %s", d, coalesce.FlushPeriod)
	}
	if !s.next.After(s.last.Add(-coalesce.FlushPeriod)) {
		t.Fatalf("the clock has fallen behind the ingest time: next %s, last %s", s.next, s.last)
	}
}

// TestWriteDatasetForOutsideCheck writes a small hour and its reduction into the
// directory named by ALIBABA_REDUCE_SMOKE_DIR, for trying the SQL check on. It
// does nothing without the variable.
func TestWriteDatasetForOutsideCheck(t *testing.T) {
	dir := os.Getenv("ALIBABA_REDUCE_SMOKE_DIR")
	if dir == "" {
		t.Skip("ALIBABA_REDUCE_SMOKE_DIR is not set")
	}
	d := dataset{placement: join(gapPods(), boundaryPod(), series("MS_D", "MS_D_POD_DUP", "NODE_1", 2, 2)), calls: callsData().calls}
	d.rawPlacement = snippet(t, "snippet_placement.csv")
	d.rawCalls = snippet(t, "snippet_calls.csv")
	in := d.withFiller().writeTo(t, filepath.Join(dir, "in"))
	cfg := testConfig(t, in)
	cfg.OutDir = filepath.Join(dir, "result")
	if _, err := reduce(cfg); err != nil {
		t.Fatal(err)
	}
}

// TestStoreCheckCanFail shows that the in-process check is not vacuous: given
// samples that differ from the stream's, it reports the difference.
func TestStoreCheckCanFail(t *testing.T) {
	res, cfg := run(t, dataset{placement: gapPods()}, nil)
	path := outPath(cfg, "placement_c60.parquet")
	if _, err := verifyStream(path, res.sinkRaw["placement_c60"]); err != nil {
		t.Fatal(err)
	}
	raw := slices.Clone(res.sinkRaw["placement_c60"])
	// Drop the last assertion of an edge: the stream says it existed longer.
	for i := len(raw) - 1; i >= 0; i-- {
		if raw[i].subject.Kind == store.SubjectEdge {
			raw = slices.Delete(raw, i, i+1)
			break
		}
	}
	if _, err := verifyStream(path, raw); err == nil {
		t.Fatal("a stream that says more than its samples was accepted")
	}
}

// TestMinuteBucketsKeepTheArchivesOrder checks that rows are grouped by minute in
// the order the archive had them, and that an archive with more minutes than it
// may have keeps the minutes it met first and drops the rest.
func TestMinuteBucketsKeepTheArchivesOrder(t *testing.T) {
	b := minuteBuckets{max: 2}
	for _, c := range []struct {
		min  uint32
		inst uint32
	}{{7, 1}, {7, 2}, {9, 3}, {7, 4}, {8, 5}, {9, 6}, {8, 7}} {
		b.add(c.min, prow{c.inst, 1, 1})
	}
	var got [][]uint32
	for _, m := range b.list {
		got = append(got, []uint32{m.min})
		for _, w := range m.rows {
			got[len(got)-1] = append(got[len(got)-1], w.inst)
		}
	}
	if want := [][]uint32{{7, 1, 2, 4}, {9, 3, 6}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("buckets = %v, want %v", got, want)
	}
}

// TestEveryCadencePassStartsAtMinuteZero checks that a pod seen only in the hour's
// first minute is in the stream of each cadence, which the reduction reads in turn.
func TestEveryCadencePassStartsAtMinuteZero(t *testing.T) {
	res, cfg := run(t, dataset{placement: series("MS_O", "MS_O_POD_0", "NODE_O", 0)}, nil)
	node := uint64(res.ids.nodes.id("NODE_O"))
	for cadence, want := range map[string]string{"c60": "NODE_O:0-3", "c300": "NODE_O:0-15"} {
		if got := intervalsOf(t, cfg, cadence, "MS_O_POD_0"); !slices.Equal(got, []string{want}) {
			t.Errorf("%s intervals of the pod = %v, want %s", cadence, got, want)
		}
		// The node is seen too, not only the pod on it.
		if !slices.ContainsFunc(res.nodeIvs[cadence], func(iv interval) bool { return iv.key == node }) {
			t.Errorf("%s has no interval for the node", cadence)
		}
	}
}

// flushed1 is what the split coalescer writes for a flush at now.
func flushed1(t *testing.T, c *shardedCoalescer, now time.Time) []store.Record {
	t.Helper()
	var out []store.Record
	if err := c.flush(now, func(recs []store.Record) error {
		out = append(out, recs...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestShardedCoalescerIsTheWhole drives the coalescer and its split with the same
// stream of records, flushed on the same clock, and requires the same records from
// every call, in the same order. The stream has pods, nodes and edges, two
// producers (some subjects have both), TTLs of three, nine and fifteen minutes,
// subjects that stop, return after a gap and change what they say, and deletes.
func TestShardedCoalescerIsTheWhole(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 7, 0, time.UTC)
	var subjects []store.Subject
	nodes := make([]identity.Fingerprint, 6)
	for i := range nodes {
		nodes[i] = fingerprintOf(catalog.K8sNode, catalog.K8sNodeUID, "N"+itoa(int64(i)))
	}
	for i := 0; i < 150; i++ {
		pod := fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, "P"+itoa(int64(i)))
		subjects = append(subjects, store.EntitySubject(pod), store.EdgeSubject(pod, nodes[i%len(nodes)], catalog.ScheduledOn))
	}
	// Nodes are a second entity type, so the parts of a stream are cut by type too.
	for _, n := range nodes {
		subjects = append(subjects, store.EntitySubject(n))
	}
	ttls := [...]time.Duration{3, 9, 15}
	for seed := uint64(1); seed <= 12; seed++ {
		rng := rand.New(rand.NewPCG(seed, 99))
		whole, err := coalesce.New(coalesceConfig())
		if err != nil {
			t.Fatal(err)
		}
		split, err := newShardedCoalescer(coalesceConfig())
		if err != nil {
			t.Fatal(err)
		}
		same := func(what string, a, b []store.Record) {
			t.Helper()
			if len(a) != len(b) || !slices.EqualFunc(a, b, func(x, y store.Record) bool { return reflect.DeepEqual(x, y) }) {
				t.Fatalf("seed %d, %s: the whole wrote %d records, the split %d, or they differ", seed, what, len(a), len(b))
			}
		}
		next := start.Add(time.Second).Add(coalesce.FlushPeriod)
		var last time.Time
		var maxTTL time.Duration
		var flushed int
		for m := 0; m < 100; m++ {
			at := start.Add(time.Duration(m) * time.Minute)
			for i, subj := range subjects {
				period, first, final := 1+i%3, i%17, 70+(i*7)%30
				if m < first || m > final || (m-first)%period != 0 || (i%5 == 0 && m%11 < 4) {
					continue
				}
				// Every fifth subject is asserted by both producers: the producer breaks the
				// tie between two runs of one subject.
				producers := []lifecycle.Producer{[...]lifecycle.Producer{placementProducer, callsProducer}[i%2]}
				if i%5 == 0 {
					producers = []lifecycle.Producer{placementProducer, callsProducer}
				}
				for _, prod := range producers {
					r := store.Record{
						Layer: catalog.L2, Subject: subj, Producer: prod,
						EventTime: at, Kind: lifecycle.Observe, TTL: ttls[i%3] * time.Minute,
					}
					if i%7 == 0 && m > 30 {
						r.Payload = []byte(`{"msname":"changed"}`)
					}
					if rng.IntN(200) == 0 {
						r.Kind, r.TTL = lifecycle.Delete, 0
					}
					ingest := at.Add(time.Second)
					for next.Before(ingest) {
						a := whole.Flush(next, nil)
						flushed += len(a)
						same("flush", a, flushed1(t, split, next))
						next = next.Add(coalesce.FlushPeriod)
					}
					last, maxTTL = ingest, max(maxTTL, r.TTL)
					b, err := split.add(r, nil)
					if err != nil {
						t.Fatal(err)
					}
					same("add", whole.Add(r, nil), b)
				}
			}
		}
		for !next.After(last) {
			same("flush", whole.Flush(next, nil), flushed1(t, split, next))
			next = next.Add(coalesce.FlushPeriod)
		}
		a := whole.FlushAll(nil)
		var b []store.Record
		if err := split.flushAll(last, maxTTL, func(recs []store.Record) error {
			b = append(b, recs...)
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		same("final flush", a, b)
		if flushed == 0 || len(a) < 30 {
			t.Fatalf("seed %d: %d records flushed on the clock and %d at the end: the stream does not exercise the flush", seed, flushed, len(a))
		}
	}
}

// TestShardedCoalescerRefusesWhatItCannotOrder checks that a record off the minute
// grid of its stream, or before the stream's first, is refused and not misplaced.
func TestShardedCoalescerRefusesWhatItCannotOrder(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	fp := fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, "P")
	rec := func(at time.Time, ttl time.Duration) store.Record {
		return store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(fp), Producer: placementProducer,
			EventTime: at, Kind: lifecycle.Observe, TTL: ttl,
		}
	}
	c, err := newShardedCoalescer(coalesceConfig())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.add(rec(start, 3*time.Minute), nil); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]store.Record{
		"event time off the grid": rec(start.Add(90*time.Second), 3*time.Minute),
		"TTL off the grid":        rec(start.Add(time.Minute), 150*time.Second),
		"before the first":        rec(start.Add(-time.Minute), 3*time.Minute),
		"TTL too short to flush":  rec(start.Add(time.Minute), 2*time.Minute),
	} {
		if _, err := c.add(r, nil); err == nil {
			t.Errorf("%s was taken", name)
		}
	}
	// The margin is the TTL minus the extension interval as well as the interval: with
	// runs extended after nine tenths of the TTL, a ten-minute TTL leaves one minute.
	late, err := newShardedCoalescer(coalesce.Config{ExtendTTLFraction: 0.9, RunAgeFromTTL: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := late.add(rec(start, 10*time.Minute), nil); err == nil {
		t.Error("a TTL with a short margin after the extension interval was taken")
	}
	if _, err := late.add(rec(start, 20*time.Minute), nil); err != nil {
		t.Errorf("a TTL with a margin of two minutes was refused: %v", err)
	}
	// A delete keeps no run, so its time and TTL do not matter to the order.
	del := rec(start.Add(90*time.Second), 0)
	del.Kind = lifecycle.Delete
	if _, err := c.add(del, nil); err != nil {
		t.Errorf("a delete was refused: %v", err)
	}
	// A refresh inside the extension interval is absorbed. A final flush that stops
	// short of its deadline leaves it behind, and that is an error; one that goes
	// as far as the deadline writes it and leaves nothing.
	absorbed := func() *shardedCoalescer {
		c, err := newShardedCoalescer(coalesceConfig())
		if err != nil {
			t.Fatal(err)
		}
		for _, at := range []time.Time{start, start.Add(time.Minute)} {
			if _, err := c.add(rec(at, 3*time.Minute), nil); err != nil {
				t.Fatal(err)
			}
		}
		return c
	}
	var written int
	count := func(recs []store.Record) error { written += len(recs); return nil }
	if err := absorbed().flushAll(start, 0, count); err == nil {
		t.Error("a final flush that stopped before a deadline left a refresh behind without an error")
	}
	written = 0
	if err := absorbed().flushAll(start.Add(time.Minute), 3*time.Minute, count); err != nil || written != 1 {
		t.Errorf("final flush: %d records, %v; want 1 and no error", written, err)
	}
}

// TestGCPercentIsSetUnlessGOGCSaysOtherwise checks that the command lowers the
// collector's growth on its own, and leaves it alone when the environment sets it.
func TestGCPercentIsSetUnlessGOGCSaysOtherwise(t *testing.T) {
	defer debug.SetGCPercent(debug.SetGCPercent(100))

	t.Setenv("GOGC", "200") // registers the restore of the variable
	if err := os.Unsetenv("GOGC"); err != nil {
		t.Fatal(err)
	}
	debug.SetGCPercent(100)
	tuneGC()
	if got := debug.SetGCPercent(100); got != gcPercent {
		t.Errorf("without GOGC the percent is %d, want %d", got, gcPercent)
	}

	t.Setenv("GOGC", "200")
	debug.SetGCPercent(100)
	tuneGC()
	if got := debug.SetGCPercent(100); got != 100 {
		t.Errorf("with GOGC set the percent was changed to %d", got)
	}
}
