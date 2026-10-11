package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// minute is the instant m minutes after t0.
func minute(m float64) time.Time { return t0.Add(time.Duration(m * float64(time.Minute))) }

// every is the times from first to last, step apart.
func every(first, last time.Time, step time.Duration) []time.Time {
	var out []time.Time
	for t := first; !t.After(last); t = t.Add(step) {
		out = append(out, t)
	}
	return out
}

// refreshOf is the pull of a pod whose creation time is in the future, so that its record
// is timed at the pull: a pure refresh.
func refreshOf(uid string, at time.Time) capture.LogRecord {
	return pullRecord(podObject(uid, uid, "", at.Add(10*time.Minute)), at)
}

func replayWith(t *testing.T, flush []time.Time, runs ...[][]capture.LogRecord) *replayed {
	t.Helper()
	rep, err := replayRuns(context.Background(), runFilesIn(t, runs...), replayOptions{Tmp: t.TempDir(), FlushTimes: flush})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := rep.close(); err != nil {
			t.Error(err)
		}
	})
	return rep
}

func podFP(t *testing.T, p *pass, uid string) identity.Fingerprint {
	t.Helper()
	return fpOf(t, p.resolver, catalog.K8sPod, catalog.K8sPodUID, uid)
}

// storesAgree is whether the two stores hold the same records of the pod.
func storesAgree(t *testing.T, p *pass, uid string) []store.Record {
	t.Helper()
	fp := podFP(t, p, uid)
	from, to := minute(-90), minute(120)
	a, err := p.mem.EntityWindow(context.Background(), fp, from, to, store.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	b, err := p.peb.EntityWindow(context.Background(), fp, from, to, store.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Errorf("pod %s: the memory store holds %+v, the Pebble store %+v", uid, a, b)
	}
	return a
}

// R1: a pod that stops being pulled is stored to its last pull plus the TTL, in both
// passes and both stores, and the flush that does so happens before the batch whose time
// reaches it.
func TestAPodThatStopsBeingPulledIsStoredToItsLastPullPlusTheTTL(t *testing.T) {
	// p1 is pulled at 0 and 5 minutes and then never. p2 is pulled throughout. p3 is
	// pulled at 0 and 5 and deleted at 14.5 minutes, a batch that ends a run without
	// writing what it absorbed, so the flush before it is the only thing that writes it.
	deleted := minute(14.5)
	pulled := func(m float64, uids ...string) []capture.LogRecord {
		var out []capture.LogRecord
		for _, u := range uids {
			out = append(out, refreshOf(u, minute(m)))
		}
		return out
	}
	gone := podObject("p3", "p3", "", minute(14.5).Add(10*time.Minute))
	lines := [][]capture.LogRecord{
		pulled(0, "p1", "p2", "p3"), pulled(5, "p1", "p2", "p3"), pulled(10, "p2"),
		{watchRecord("DELETED", gone, deleted)},
		pulled(14.5, "p2"), pulled(20, "p2"),
	}
	flush := every(minute(0.5), minute(40), coalesce.FlushPeriod)
	rep := replayWith(t, flush, lines)

	for _, p := range []*pass{rep.A, rep.B} {
		if p.FlushRecords == 0 {
			t.Errorf("pass %s: no record written by a flush", p.Name)
		}
		for uid, last := range map[string]time.Time{"p1": minute(5), "p2": minute(20), "p3": minute(5)} {
			if got, want := p.Runs[0].Deadlines[podFP(t, p, uid)], last.Add(entityTTL); !got.Equal(want) {
				t.Errorf("pass %s: pod %s is stored to %s, want its last pull plus the TTL, %s", p.Name, uid, got.Sub(t0), want.Sub(t0))
			}
			storesAgree(t, p, uid)
		}
	}
	// What the store holds of p1: the run, and the extension through its last pull.
	recs := storesAgree(t, rep.A, "p1")
	var through time.Time
	for _, r := range recs {
		through = endOf(r)
	}
	if !through.Equal(minute(5)) {
		t.Errorf("p1 is held through %s, want 5m", through.Sub(t0))
	}
}

// R2: a pass that restarts writes what its coalescer absorbed before it forgets it, and
// the records belong to the run that ends.
func TestPassBFlushesEverythingAbsorbedBeforeARestart(t *testing.T) {
	// p1 is pulled at 0 and 5 minutes in run 1, which ends before the flush of the listing
	// would write the absorbed pull. Run 2 begins at 9 minutes with another pod.
	run1 := [][]capture.LogRecord{{refreshOf("p1", minute(0))}, {refreshOf("p1", minute(5))}}
	run2 := [][]capture.LogRecord{{refreshOf("p2", minute(9))}}
	rep := replayWith(t, every(minute(0.5), minute(40), coalesce.FlushPeriod), run1, run2)

	p1 := podFP(t, rep.A, "p1")
	want := minute(5).Add(entityTTL)
	b0, a0 := rep.B.Runs[0], rep.A.Runs[0]
	if got := b0.Deadlines[p1]; !got.Equal(want) {
		t.Errorf("pass B: p1 is stored to %s at the end of run 1, want %s: the records of the flush at the restart are in the run's deadlines", got.Sub(t0), want.Sub(t0))
	}
	if got, want := b0.Written, a0.Written+1; got != want {
		t.Errorf("run 1 wrote %d records in pass B and %d in pass A, want the flush's record more", b0.Written, a0.Written)
	}
	if got := a0.Deadlines[p1]; !got.Equal(minute(0).Add(entityTTL)) {
		t.Errorf("pass A: p1 is stored to %s at the end of run 1, want its first pull plus the TTL, %s (nothing was flushed yet)", got.Sub(t0), minute(0).Add(entityTTL).Sub(t0))
	}
	// At the end the two passes agree on the pod.
	if a, b := rep.A.Runs[1].Deadlines[p1], rep.B.Runs[1].Deadlines[p1]; !a.Equal(want) || !b.Equal(want) {
		t.Errorf("at the end p1 is stored to %s in pass A and %s in pass B, want %s in both", a.Sub(t0), b.Sub(t0), want.Sub(t0))
	}
	if rep.B.Restarts != 1 || rep.B.FlushRecords == 0 {
		t.Errorf("pass B: %d restarts, %d flush records", rep.B.Restarts, rep.B.FlushRecords)
	}
	// The record of the flush is in the stores, before the pass forgot its coalescer.
	for _, p := range []*pass{rep.A, rep.B} {
		recs := storesAgree(t, p, "p1")
		if len(recs) != 2 || !endOf(recs[1]).Equal(minute(5)) {
			t.Errorf("pass %s: p1's records are %+v, want the run and its extension through 5m", p.Name, recs)
		}
	}
}

// Without flush times, a pass that does not restart writes nothing more than before.
func TestWithNoFlushTimesAPassThatDoesNotRestartWritesNoFlushRecords(t *testing.T) {
	rep := replayWith(t, nil, [][]capture.LogRecord{{refreshOf("p1", minute(0))}, {refreshOf("p1", minute(5))}})
	if rep.A.FlushRecords != 0 {
		t.Errorf("pass A wrote %d flush records", rep.A.FlushRecords)
	}
}

// A flush time is used once, in order, before the first batch whose time reaches it, and
// the rest after the last run.
func TestFlushTimesAreUsedOnceInOrderAndTheRestAfterTheLastRun(t *testing.T) {
	flush := []time.Time{minute(1), minute(2), minute(3), minute(30), minute(31)}
	p := passForTest(t)
	p.flushTimes = flush
	if err := p.startRun(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		at   time.Time
		used int
	}{
		{minute(0), 0}, {minute(1), 1}, {minute(2.5), 2}, {minute(2), 2}, {minute(29), 3}, {minute(30), 4},
	} {
		if err := p.flushTo(context.Background(), tc.at); err != nil {
			t.Fatal(err)
		}
		if p.nextFlush != tc.used {
			t.Errorf("after a batch at %s, %d flush times used, want %d", tc.at.Sub(t0), p.nextFlush, tc.used)
		}
	}
	if err := p.flushRest(context.Background()); err != nil {
		t.Fatal(err)
	}
	if p.nextFlush != len(flush) {
		t.Errorf("after the last run %d flush times used, want all %d", p.nextFlush, len(flush))
	}
}

// R3: the rule texts are the ones decided.
func TestTheEdgeRuleTextsAreUnchanged(t *testing.T) {
	const afterPod = "BLOCKING edge-after-pod: the count of (T, pod) with T outside the +/-60 s margin where the pod is absent in the truth and not alive in the store, yet its scheduled_on or part_of edge is alive in the store; BLOCKING if the count > 0"
	const acrossGap = "edge-across-gap (informational): the count of (T, pod) where the pod is present in the truth but not alive in the store, while its edge is alive; expected from late-resolved edges carrying Through across an outage"
	if edgeAfterPodRule != afterPod {
		t.Errorf("edgeAfterPodRule is %q", edgeAfterPodRule)
	}
	if edgeAcrossGapRule != acrossGap {
		t.Errorf("edgeAcrossGapRule is %q", edgeAcrossGapRule)
	}
}

func TestTheLargestGapBetweenListingSamples(t *testing.T) {
	for _, tc := range []struct {
		times []time.Time
		want  time.Duration
	}{
		{nil, 0},
		{[]time.Time{minute(0)}, 0},
		{every(minute(0), minute(5), 30*time.Second), 30 * time.Second},
		{[]time.Time{minute(0), minute(0.5), minute(2), minute(2.5)}, 90 * time.Second},
	} {
		if got := largestGap(tc.times); got != tc.want {
			t.Errorf("largest gap of %d times is %s, want %s", len(tc.times), got, tc.want)
		}
	}
}

// miniatureCapture writes a whole capture in miniature, as TestExecuteOnAMiniatureCapture
// does, with the pod truth in the file, and returns its directory.
func miniatureCapture(t *testing.T, truthPods string) string {
	t.Helper()
	dir := t.TempDir()
	created := t0.Add(-time.Hour)
	var sizes, stopped strings.Builder
	for n := 1; n <= 5; n++ {
		at := t0.Add(time.Duration(n) * time.Minute)
		path := runPath(dir, n)
		writeRun(t, path,
			[]capture.LogRecord{pullRecord(podObject(uidA, "web-a", "node1", created), at)},
			[]capture.LogRecord{podState(uidA, "web-a", "node1", created, at)},
		)
		st, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		fmt.Fprintf(&sizes, "run%d %d\n", n, st.Size())
		fmt.Fprintf(&stopped, "2026-10-10T12:%02d:00Z collector run %d started, T0=%d\n2026-10-10T12:%02d:30Z collector run %d stopped: 2 lines, %d bytes\n", n*10, n, n, n*10, n, st.Size())
	}
	logs := filepath.Join(dir, "logs")
	if err := os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{"sizes.txt": sizes.String(), "workload.txt": stopped.String()} {
		if err := os.WriteFile(filepath.Join(logs, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	for name, src := range map[string]string{"truth-pods.tsv": truthPods, "truth-nodes.tsv": filepath.Join("testdata", "truth-nodes.tsv")} {
		b, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(logs, name), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// result is the report of a capture, as result.json holds it, without the fields that
// are about the machine and the moment.
func result(t *testing.T, dir string) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "report")
	var stdout bytes.Buffer
	if _, err := execute(config{Capture: dir, Tmp: t.TempDir(), Margin: 60 * time.Second}, out, &stdout, time.Minute); err != nil {
		t.Fatal(err)
	}
	js, err := os.ReadFile(filepath.Join(out, "result.json"))
	if err != nil {
		t.Fatal(err)
	}
	var back map[string]any
	if err := json.Unmarshal(js, &back); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"generated", "replay_elapsed", "analysis_elapsed"} {
		delete(back, k)
	}
	norm, err := json.MarshalIndent(back, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return norm
}

// R4: the same capture replayed twice gives the same result.
func TestTheSameCaptureGivesTheSameResultTwice(t *testing.T) {
	dir := miniatureCapture(t, filepath.Join("testdata", "truth-pods.tsv"))
	a, b := result(t, dir), result(t, dir)
	if !bytes.Equal(a, b) {
		t.Error("two replays of one capture gave different results")
	}
	var back struct {
		Passes []struct {
			Flush *int `json:"flush_records"`
		} `json:"passes"`
		Truth struct {
			Gap  string `json:"pod_sample_largest_gap"`
			Over bool   `json:"pod_sample_gap_over_flush_lead"`
		} `json:"truth"`
	}
	if err := json.Unmarshal(a, &back); err != nil {
		t.Fatal(err)
	}
	if len(back.Passes) != 2 || back.Passes[0].Flush == nil || back.Passes[1].Flush == nil {
		t.Errorf("the result lacks flush_records for both passes: %+v", back.Passes)
	}
	if back.Truth.Gap == "" || back.Truth.Over {
		t.Errorf("the largest gap between listing samples is %q (over the lead: %v)", back.Truth.Gap, back.Truth.Over)
	}
}

// A listing that is sampled less often than the lead is said so, in the findings.
func TestAListingSampledLessOftenThanTheLeadIsFlagged(t *testing.T) {
	dir := t.TempDir()
	truth := filepath.Join(dir, "truth-pods.tsv")
	body := "2026-10-10T12:00:00Z\t" + uidA + "\tdefault/web-a\tnode1\tRunning\t\t\t0\n" +
		"2026-10-10T12:00:30Z\t\n" +
		"2026-10-10T12:03:00Z\t" + uidA + "\tdefault/web-a\tnode1\tRunning\t\t\t0\n"
	if err := os.WriteFile(truth, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	capDir := miniatureCapture(t, truth)
	rep, err := build(context.Background(), config{Capture: capDir, Tmp: t.TempDir(), Margin: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.Truth.FlushGapOverLead || rep.Truth.PodSampleLargestGap != (150*time.Second).String() {
		t.Errorf("gap %s, over the lead %v", rep.Truth.PodSampleLargestGap, rep.Truth.FlushGapOverLead)
	}
	found := false
	for _, f := range rep.Findings {
		found = found || strings.Contains(f, "flush lead")
	}
	if !found {
		t.Errorf("no finding about the flush lead: %v", rep.Findings)
	}
	for _, b := range rep.Blocking {
		if strings.Contains(b, "flush") {
			t.Errorf("the flush gap is BLOCKING: %s", b)
		}
	}
}

// A batch is on the clock of its latest record: a listing time between its records'
// times is flushed before the batch is fed, not after.
func TestABatchIsOnTheClockOfItsLatestRecord(t *testing.T) {
	// p3 is pulled at 0 and 5 minutes. One line then holds a record of p2 at 10 minutes and
	// the deletion of p3 at 14.5 minutes; the only listing time is 14 minutes, between them.
	// The delete ends p3's run without writing what it absorbed, so only a flush before the
	// batch stores p3 through its last pull.
	gone := podObject("p3", "p3", "", minute(14.5).Add(10*time.Minute))
	lines := [][]capture.LogRecord{
		{refreshOf("p3", minute(0))},
		{refreshOf("p3", minute(5))},
		{refreshOf("p2", minute(10)), watchRecord("DELETED", gone, minute(14.5))},
	}
	rep := replayWith(t, []time.Time{minute(14)}, lines)
	for _, p := range []*pass{rep.A, rep.B} {
		got, want := p.Runs[0].Deadlines[podFP(t, p, "p3")], minute(5).Add(entityTTL)
		if !got.Equal(want) {
			t.Errorf("pass %s: p3 is stored to %s, want its last pull plus the TTL, %s", p.Name, got.Sub(t0), want.Sub(t0))
		}
		if p.FlushRecords != 1 {
			t.Errorf("pass %s: %d flush records, want 1", p.Name, p.FlushRecords)
		}
	}
}
