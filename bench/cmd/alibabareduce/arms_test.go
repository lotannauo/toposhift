package main

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// armsData is the gap pods and the one used row of the placement snippet. By hand:
// at C60 the hour has 18 node, 21 pod and 21 edge assertions (the filler's node and
// pod at each of 12 minutes, the gap pods' node G at minutes 0 to 4 and pods A to D
// twice each, the snippet's node and pod once), 60 in all; at C300 (minutes 0, 5 and
// 10) it has 4 nodes, 7 pods and 7 edges, 18 in all. The maximal intervals are, at
// C60, 3 nodes, 7 pods (pod D has two) and 7 edges, 17; at C300, 2 nodes, 5 pods and
// 5 edges, 12.
func armsData(t testing.TB) dataset {
	return dataset{placement: gapPods(), rawPlacement: snippet(t, "snippet_placement.csv")}
}

var (
	wantAssertions = map[string]int64{"c60": 60, "c300": 18}
	wantIntervals  = map[string]int64{"c60": 17, "c300": 12}
)

func arms(s, i bool) func(*Config) {
	return func(c *Config) { c.WriteS, c.WriteI = s, i }
}

// cadenceNamed is the cadence of a result by name.
func cadenceNamed(res *results, name string) *cadence {
	for _, c := range res.cads {
		if c.spec.name == name {
			return c
		}
	}
	return nil
}

// checkEveryAssertion reports the first way the S stream of a cadence is not the
// assertions the reduction made, one for one: it is compared with the raw
// assertions the coalesced stream was given, and with the count the coalesced
// stream reports.
func checkEveryAssertion(t testing.TB, res *results, cfg Config, cadence string) error {
	c := cadenceNamed(res, cadence)
	recs := records(t, outPath(cfg, "placement_"+cadence+"_s.parquet"))
	raw := res.sinkRaw["placement_"+cadence]
	if int64(len(recs)) != c.sink.res.Assertions || len(recs) != len(raw) {
		return &checkError{what: cadence + " S records", got: []string{itoa(int64(len(recs)))}, want: []string{itoa(c.sink.res.Assertions), itoa(int64(len(raw)))}}
	}
	for i, r := range recs {
		a := raw[i]
		if r.Subject != a.subject || r.Layer != a.layer || !r.EventTime.Equal(a.at) || r.TTL != a.ttl {
			return &checkError{what: fmt.Sprintf("%s S record %d", cadence, i+1), got: []string{describe(r.Subject), r.EventTime.String(), r.TTL.String()}, want: []string{describe(a.subject), a.at.String(), a.ttl.String()}}
		}
		if r.Seq != uint64(i+1) || r.Kind != lifecycle.Observe || !r.Through.IsZero() || r.TTL != c.spec.ttl() || r.Producer != placementProducer {
			return &checkError{what: fmt.Sprintf("%s S record %d: seq %d kind %s through %s ttl %s producer %s", cadence, i+1, r.Seq, r.Kind, r.Through, r.TTL, r.Producer)}
		}
	}
	return nil
}

func TestEveryAssertionIsInTheSStream(t *testing.T) {
	res, cfg := run(t, armsData(t), arms(true, false))
	for cadence, want := range wantAssertions {
		if err := checkEveryAssertion(t, res, cfg, cadence); err != nil {
			t.Error(err)
		}
		if got := cadenceNamed(res, cadence).every.res.Records; got != want {
			t.Errorf("%s S holds %d records, want %d by hand", cadence, got, want)
		}
		if !fileExists(outPath(cfg, "placement_"+cadence+"_s.parquet")) {
			t.Errorf("%s S file is missing", cadence)
		}
		if fileExists(outPath(cfg, "placement_"+cadence+"_i.parquet")) {
			t.Errorf("%s I file exists with only s asked for", cadence)
		}
	}
	// The pods carry their msname and nothing else; nodes and edges carry nothing.
	for _, r := range records(t, outPath(cfg, "placement_c60_s.parquet")) {
		switch {
		case r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sPod:
			if !strings.HasPrefix(string(r.Payload), `{"msname":"MS_`) || strings.Contains(string(r.Payload), ",") {
				t.Fatalf("pod payload %s", r.Payload)
			}
		case len(r.Payload) != 0:
			t.Fatalf("%s carries a payload", describe(r.Subject))
		}
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// iExpect is one record of an I stream: the minutes of its event time and of its
// Through (-1: none).
type iExpect struct{ start, through int }

// checkIntervalStream reports the first way the I stream of a cadence is not one
// record per maximal interval of the trackers, with the existence of the interval:
// [start, last + TTL), the record's last instant plus its TTL. The records must
// also be in ascending event time, numbered from 1, and carry a Through only if the
// interval has more than one sample.
func checkIntervalStream(t testing.TB, res *results, cfg Config, cadence string) error {
	c := cadenceNamed(res, cadence)
	recs := records(t, outPath(cfg, "placement_"+cadence+"_i.parquet"))
	type span2 struct{ from, to time.Time }
	want := map[store.Subject][]span2{}
	// order is the subjects as the stream must list them: by start, then nodes, pods
	// and edges, then by the key of the interval.
	type placed struct {
		start int32
		class int
		key   uint64
		sub   store.Subject
	}
	var order []placed
	add := func(class int, sub store.Subject, iv interval) {
		at := cfg.Epoch.Add(time.Duration(iv.start) * sampleInterval)
		end := cfg.Epoch.Add(time.Duration(iv.last) * sampleInterval).Add(c.spec.ttl())
		want[sub] = append(want[sub], span2{at, end})
		order = append(order, placed{iv.start, class, iv.key, sub})
	}
	for _, iv := range res.nodeIvs[cadence] {
		add(0, store.EntitySubject(fingerprintOf(catalog.K8sNode, catalog.K8sNodeUID, res.ids.nodes.name(uint32(iv.key)))), iv)
	}
	for _, iv := range res.podIvs[cadence] {
		add(1, store.EntitySubject(fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, res.ids.pods.name(uint32(iv.key)))), iv)
	}
	for _, iv := range res.edgeIvs[cadence] {
		p, n := unpair(iv.key)
		add(2, store.EdgeSubject(fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, res.ids.pods.name(p)),
			fingerprintOf(catalog.K8sNode, catalog.K8sNodeUID, res.ids.nodes.name(n)), catalog.ScheduledOn), iv)
	}
	if len(recs) != len(order) {
		return &checkError{what: cadence + " I records", got: []string{itoa(int64(len(recs)))}, want: []string{itoa(int64(len(order)))}}
	}
	slices.SortFunc(order, func(a, b placed) int {
		return cmp.Or(cmp.Compare(a.start, b.start), cmp.Compare(a.class, b.class), cmp.Compare(a.key, b.key))
	})
	for i, p := range order {
		if recs[i].Subject != p.sub {
			return &checkError{what: fmt.Sprintf("%s I record %d is out of order", cadence, i+1), got: []string{describe(recs[i].Subject)}, want: []string{describe(p.sub)}}
		}
	}
	got := map[store.Subject][]span2{}
	for i, r := range recs {
		if err := r.Validate(); err != nil {
			return err
		}
		last := r.EventTime
		if !r.Through.IsZero() {
			if !r.Through.After(r.EventTime) {
				return &checkError{what: fmt.Sprintf("%s I record %d: through %s is not after %s", cadence, i+1, r.Through, r.EventTime)}
			}
			last = r.Through
		}
		got[r.Subject] = append(got[r.Subject], span2{r.EventTime, last.Add(r.TTL)})
		if r.Seq != uint64(i+1) || r.Kind != lifecycle.Observe || r.TTL != c.spec.ttl() || r.Layer != catalog.L2 || r.Producer != placementProducer ||
			r.EventTimeBasis != store.BasisProducerEvent || (i > 0 && r.EventTime.Before(recs[i-1].EventTime)) {
			return &checkError{what: fmt.Sprintf("%s I record %d: seq %d kind %s ttl %s layer %s producer %s basis %s", cadence, i+1, r.Seq, r.Kind, r.TTL, r.Layer, r.Producer, r.EventTimeBasis)}
		}
	}
	if !reflect.DeepEqual(got, want) {
		return &checkError{what: cadence + " I existence differs from the trackers' intervals"}
	}
	return nil
}

// checkIGaps reports the first way the I records of the gap pods are not the
// rule's: a pod seen again after a gap of up to one TTL is one record whose Through
// is its last sample, and one seen again after more is two.
func checkIGaps(t testing.TB, res *results, cfg Config) error {
	want := map[string][]iExpect{
		"MS_G_POD_A": {{0, 1}}, "MS_G_POD_B": {{0, 2}}, "MS_G_POD_C": {{0, 3}}, "MS_G_POD_D": {{0, -1}, {4, -1}},
	}
	for pod, w := range want {
		sub := store.EntitySubject(fingerprintOf(catalog.K8sPod, catalog.K8sPodUID, pod))
		var got []iExpect
		for _, r := range records(t, outPath(cfg, "placement_c60_i.parquet")) {
			if r.Subject == sub {
				e := iExpect{int(r.EventTime.Sub(cfg.Epoch) / sampleInterval), -1}
				if !r.Through.IsZero() {
					e.through = int(r.Through.Sub(cfg.Epoch) / sampleInterval)
				}
				got = append(got, e)
			}
		}
		if !slices.Equal(got, w) {
			return &checkError{what: pod + " I records", got: []string{fmt.Sprint(got)}, want: []string{fmt.Sprint(w)}}
		}
	}
	return nil
}

func TestIntervalStreamIsOneRecordPerInterval(t *testing.T) {
	res, cfg := run(t, armsData(t), arms(false, true))
	for cadence, want := range wantIntervals {
		if err := checkIntervalStream(t, res, cfg, cadence); err != nil {
			t.Error(err)
		}
		if got := res.armSinks[slices.IndexFunc(res.armSinks, func(s sinkResult) bool { return s.Name == "placement_"+cadence+"_i" })].Records; got != want {
			t.Errorf("%s I holds %d records, want %d by hand", cadence, got, want)
		}
		if fileExists(outPath(cfg, "placement_"+cadence+"_s.parquet")) {
			t.Errorf("%s S file exists with only i asked for", cadence)
		}
	}
	if err := checkIGaps(t, res, cfg); err != nil {
		t.Fatal(err)
	}
	// A pod's record carries the msname of its first sample, and a node's and an edge's nothing.
	for _, r := range records(t, outPath(cfg, "placement_c60_i.parquet")) {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.K8sPod {
			if !bytes.HasPrefix(r.Payload, []byte(`{"msname":"MS_`)) || bytes.Contains(r.Payload, []byte(",")) {
				t.Fatalf("pod payload %s", r.Payload)
			}
		} else if len(r.Payload) != 0 {
			t.Fatalf("%s carries a payload", describe(r.Subject))
		}
	}
	// By the hour's checks: the in-process store agrees with the samples about the I stream.
	if res.asked["placement_c60_i"] == 0 || res.asked["placement_c300_i"] == 0 {
		t.Errorf("the in-process store was asked nothing about the I streams: %v", res.asked)
	}
}

func TestBothArmsTogetherAreInTheManifestAndTheReport(t *testing.T) {
	_, cfg := run(t, armsData(t), arms(true, true))
	var m struct {
		Flags   map[string]any `json:"flags"`
		Outputs []sinkResult   `json:"outputs"`
	}
	if err := json.Unmarshal(readFile(t, filepath.Join(cfg.OutDir, "manifest.json")), &m); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, o := range m.Outputs {
		names = append(names, o.Name)
	}
	want := []string{"placement_c60", "placement_c300", "calls", "placement_c60_s", "placement_c60_i", "placement_c300_s", "placement_c300_i"}
	if !slices.Equal(names[:len(want)], want) || names[len(want)] != "fixture" || len(names) != len(want)+1 {
		t.Fatalf("outputs = %v, want %v and the fixture", names, want)
	}
	for _, o := range m.Outputs {
		if !o.Written || o.DroppedOverLimit || o.Records == 0 || o.MinSeq != 1 || o.MaxSeq != uint64(o.Records) {
			t.Errorf("%s: %+v", o.Name, o)
		}
	}
	if m.Flags["arms"] != "s,i" || m.Flags["ttl_factor"] != float64(3) {
		t.Errorf("flags = %v", m.Flags)
	}
	report := string(readFile(t, filepath.Join(cfg.OutDir, "REDUCTION.md")))
	for _, line := range []string{"- records of the every-sample stream (S, no coalescer): C60 60 [M, local], C300 18 [M, local]", "- records of the maximal-interval stream (I, "} {
		if !strings.Contains(report, line) {
			t.Errorf("REDUCTION.md has no line %q", line)
		}
	}
}

// withoutArmLines is a report without the lines about the S and I streams.
func withoutArmLines(report string) string {
	var keep []string
	for _, l := range strings.Split(report, "\n") {
		if !strings.HasPrefix(l, "- records of the every-sample stream") && !strings.HasPrefix(l, "- records of the maximal-interval stream") {
			keep = append(keep, l)
		}
	}
	return strings.Join(keep, "\n")
}

// checkArmsLeaveCAlone reports the first way a run with the arms of cfg differs, in
// anything but the arms, from the same run with none: the coalesced streams, the
// counts the coalescer reports, the tables and the report.
func checkArmsLeaveCAlone(t testing.TB, res *results, cfg Config) error {
	base := cfg
	base.WriteS, base.WriteI, base.mut = false, false, mutNone
	base.OutDir = filepath.Join(t.TempDir(), "run")
	bres, err := reduce(base)
	if err != nil {
		return err
	}
	for i := range bres.sinks {
		a, b := bres.sinks[i], res.sinks[i]
		if a.Records != b.Records || a.Assertions != b.Assertions || a.Deletes != b.Deletes || a.WithThrough != b.WithThrough || a.Bytes != b.Bytes {
			return &checkError{what: "the counts of " + a.Name, got: []string{fmt.Sprintf("%+v", b)}, want: []string{fmt.Sprintf("%+v", a)}}
		}
	}
	for _, name := range []string{"placement_c60.parquet", "placement_c300.parquet", "calls.parquet"} {
		if !reflect.DeepEqual(records(t, outPath(base, name)), records(t, outPath(cfg, name))) {
			return &checkError{what: name + " differs with the arms on"}
		}
	}
	for _, name := range []string{"placement_intervals.tsv.gz", "placement_minutes.tsv.gz", "call_rollup_minutes.tsv.gz", "call_edges_intervals.tsv.gz", "dirt_cases.tsv", "skips.tsv"} {
		if !bytes.Equal(readFile(t, outPath(base, name)), readFile(t, outPath(cfg, name))) {
			return &checkError{what: name + " differs with the arms on"}
		}
	}
	for _, name := range []string{"params.json", "REDUCTION.md"} {
		a, b := string(readFile(t, filepath.Join(base.OutDir, name))), string(readFile(t, filepath.Join(cfg.OutDir, name)))
		if name == "REDUCTION.md" {
			b = withoutArmLines(b)
		}
		if a != b {
			return &checkError{what: name + " differs with the arms on"}
		}
	}
	return nil
}

func TestArmsLeaveTheCoalescedStreamsAndTheTablesAlone(t *testing.T) {
	// With no arms, no S or I file is written, and the manifest lists none.
	res, cfg := run(t, armsData(t), nil)
	ents, err := os.ReadDir(filepath.Join(cfg.OutDir, "out"))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), "_s.parquet") || strings.HasSuffix(e.Name(), "_i.parquet") {
			t.Errorf("%s was written with no arms", e.Name())
		}
	}
	if len(res.armSinks) != 0 {
		t.Errorf("%d arm streams reported with no arms", len(res.armSinks))
	}
	var m struct {
		Flags   map[string]any `json:"flags"`
		Outputs []sinkResult   `json:"outputs"`
	}
	if err := json.Unmarshal(readFile(t, filepath.Join(cfg.OutDir, "manifest.json")), &m); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.Flags["arms"]; ok || len(m.Outputs) != 4 {
		t.Errorf("manifest with no arms: flags %v, %d outputs", m.Flags, len(m.Outputs))
	}
	if strings.Contains(string(readFile(t, filepath.Join(cfg.OutDir, "REDUCTION.md"))), "every-sample") {
		t.Error("REDUCTION.md mentions the S stream with no arms")
	}

	res, cfg = run(t, armsData(t), arms(true, true))
	if err := checkArmsLeaveCAlone(t, res, cfg); err != nil {
		t.Fatal(err)
	}
}

func TestTwoRunsWithArmsAreByteIdentical(t *testing.T) {
	in := armsData(t).withFiller().write(t)
	var cfgs [2]Config
	for i := range cfgs {
		cfgs[i] = testConfig(t, in)
		cfgs[i].WriteS, cfgs[i].WriteI = true, true
		if _, err := reduce(cfgs[i]); err != nil {
			t.Fatal(err)
		}
	}
	for _, c := range []string{"c60", "c300"} {
		for _, arm := range []string{"s", "i"} {
			name := "placement_" + c + "_" + arm + ".parquet"
			if !bytes.Equal(readFile(t, outPath(cfgs[0], name)), readFile(t, outPath(cfgs[1], name))) {
				t.Errorf("%s differs between two runs", name)
			}
		}
	}
}

func TestArmStreamsRespectTheSizeLimit(t *testing.T) {
	cfg := testConfig(t, armsData(t).withFiller().write(t))
	cfg.WriteS, cfg.WriteI, cfg.VerifyStreams = true, true, false
	cfg.MaxFileBytes = 64
	res, err := reduce(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if len(res.armSinks) != 4 {
		t.Fatalf("%d arm streams", len(res.armSinks))
	}
	for _, s := range res.armSinks {
		if !s.DroppedOverLimit || s.Written || s.Records == 0 {
			t.Errorf("%s: %+v", s.Name, s)
		}
		if fileExists(filepath.Join(cfg.OutDir, "out", s.Name+".parquet")) {
			t.Errorf("%s: the file was kept", s.Name)
		}
	}
}

func TestArmsFlagIsParsed(t *testing.T) {
	for _, c := range []struct {
		in   string
		s, i bool
		bad  bool
	}{
		{"", false, false, false},
		{"s", true, false, false},
		{"i", false, true, false},
		{"s,i", true, true, false},
		{"i,s", true, true, false},
		{"S", false, false, true},
		{"s,", false, false, true},
		{"c", false, false, true},
		{"s,x", false, false, true},
	} {
		s, i, err := parseArms(c.in)
		if (err != nil) != c.bad || s != c.s || i != c.i {
			t.Errorf("parseArms(%q) = %t, %t, %v", c.in, s, i, err)
		}
	}
	cfg := defaultConfig()
	cfg.WriteS, cfg.WriteI = true, false
	if cfg.armsFlag() != "s" {
		t.Errorf("armsFlag = %q", cfg.armsFlag())
	}
}

func TestTTLFactorAndArmsAreValidated(t *testing.T) {
	for factor, bad := range map[int]bool{-3: true, 0: true, 1: false, 3: false, 15: false, 60: false, 61: true} {
		cfg := testConfig(t, t.TempDir())
		cfg.TTLFactor = factor
		if err := cfg.validate(); (err != nil) != bad {
			t.Errorf("TTL factor %d: %v", factor, err)
		}
	}
	cfg := testConfig(t, t.TempDir())
	cfg.WriteI, cfg.WriteC60 = true, false
	if err := cfg.validate(); err == nil {
		t.Error("the I stream was checked against a C60 stream that is not kept")
	}
	cfg.VerifyStreams = false
	if err := cfg.validate(); err != nil {
		t.Error(err)
	}
}

// ttlData has a pod with a gap, and a call edge that is seen again six minutes
// later: a gap of more than the calls' TTL of three minutes and less than a TTL of
// fifteen. The calls must not follow the placement's factor.
func ttlData() dataset {
	d := dataset{placement: gapPods(), calls: callsData().calls}
	d.calls = append(slices.Clone(d.calls), cl{ts: 6*60000 + 10, trace: "T_5", um: "MS_A", umi: "MS_A_POD_1", dm: "MS_B", dmi: "MS_B_POD_1"})
	return d
}

// checkTTLFactor reports the first way a run at factor 15 does not promise fifteen
// sampling intervals at each cadence while the calls keep their three minutes.
func checkTTLFactor(t testing.TB, res *results, cfg Config) error {
	base := cfg
	base.TTLFactor, base.mut = defaultTTLFactor, mutNone
	base.OutDir = filepath.Join(t.TempDir(), "run")
	if _, err := reduce(base); err != nil {
		return err
	}
	for _, c := range []struct {
		name string
		ttl  time.Duration
	}{{"placement_c60.parquet", 15 * time.Minute}, {"placement_c300.parquet", 75 * time.Minute}, {"placement_c60_s.parquet", 15 * time.Minute}} {
		if _, err := os.Stat(outPath(cfg, c.name)); err != nil && strings.HasSuffix(c.name, "_s.parquet") {
			continue
		}
		recs := records(t, outPath(cfg, c.name))
		if len(recs) == 0 {
			return &checkError{what: c.name + " is empty"}
		}
		for _, r := range recs {
			if r.TTL != c.ttl {
				return &checkError{what: c.name + " TTL", got: []string{r.TTL.String()}, want: []string{c.ttl.String()}}
			}
		}
	}
	for _, r := range records(t, outPath(cfg, "calls.parquet")) {
		if r.TTL != callEdgeTTL {
			return &checkError{what: "calls TTL", got: []string{r.TTL.String()}, want: []string{callEdgeTTL.String()}}
		}
	}
	if !reflect.DeepEqual(records(t, outPath(cfg, "calls.parquet")), records(t, outPath(base, "calls.parquet"))) {
		return &checkError{what: "calls.parquet follows the TTL factor"}
	}
	for _, name := range []string{"call_edges_intervals.tsv.gz", "call_rollup_minutes.tsv.gz"} {
		if !bytes.Equal(readFile(t, outPath(base, name)), readFile(t, outPath(cfg, name))) {
			return &checkError{what: name + " follows the TTL factor"}
		}
	}
	rows := 0
	for _, row := range table(t, outPath(cfg, "call_edges_intervals.tsv.gz")) {
		if row["um"] == "MS_A" && row["dm"] == "MS_B" {
			rows++
		}
	}
	if rows != 2 {
		return &checkError{what: "intervals of the call edge A to B", got: []string{itoa(int64(rows))}, want: []string{"2"}}
	}
	// A pod seen at minutes 0 and 4 is one interval with fifteen intervals of TTL, [0, 19), and two with three.
	if got := intervalsOf(t, cfg, "c60", "MS_G_POD_D"); !slices.Equal(got, []string{"NODE_G:0-19"}) {
		return &checkError{what: "pod D at factor 15", got: got, want: []string{"NODE_G:0-19"}}
	}
	if got := intervalsOf(t, base, "c60", "MS_G_POD_D"); !slices.Equal(got, []string{"NODE_G:0-3", "NODE_G:4-7"}) {
		return &checkError{what: "pod D at the default factor", got: got}
	}
	var m manifestFile
	if err := json.Unmarshal(readFile(t, filepath.Join(cfg.OutDir, "manifest.json")), &m); err != nil {
		return err
	}
	if m.Flags["ttl_factor"] != float64(15) || m.Cadences["c60"]["ttl_seconds"] != 900 || m.Cadences["c300"]["ttl_seconds"] != 4500 {
		return &checkError{what: "the manifest's TTL", got: []string{fmt.Sprint(m.Flags["ttl_factor"], m.Cadences)}}
	}
	return nil
}

func TestTTLFactorSetsThePlacementTTLOnly(t *testing.T) {
	res, cfg := run(t, ttlData(), func(c *Config) { c.TTLFactor, c.WriteS, c.WriteI = 15, true, true })
	if err := checkTTLFactor(t, res, cfg); err != nil {
		t.Fatal(err)
	}
	// The arms follow the factor too: the I records promise fifteen intervals.
	if err := checkIntervalStream(t, res, cfg, "c60"); err != nil {
		t.Fatal(err)
	}
	// The default is the product's three, in the manifest.
	_, def := run(t, ttlData(), nil)
	var m manifestFile
	if err := json.Unmarshal(readFile(t, filepath.Join(def.OutDir, "manifest.json")), &m); err != nil {
		t.Fatal(err)
	}
	if m.Flags["ttl_factor"] != float64(3) || m.Cadences["c60"]["ttl_seconds"] != 180 || m.Cadences["c300"]["ttl_seconds"] != 900 {
		t.Errorf("default manifest: %v %v", m.Flags["ttl_factor"], m.Cadences)
	}
}
