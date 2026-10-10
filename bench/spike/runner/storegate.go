package runner

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	rtmetrics "runtime/metrics"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The store gate judges the root store's retention, apart from the spike's gates: it
// never reads [Rules], and the spike's rules version and digest do not depend on it.
// Its limits are constants here, printed with a digest of their text by [WriteStoreGate],
// so that a change of a limit shows in a report and fails the test that pins the digest.

// The limits of the store gate. Q1 to Q5 are fixed before anything is measured.
const (
	// StoreGateRulesVersion is the version of the gate's rules text.
	StoreGateRulesVersion = 1
	// StoreGateQ1MaxWait is the most the longest batch inside a retention window may take.
	StoreGateQ1MaxWait = 250 * time.Millisecond
	// StoreGateQ2Factor and StoreGateQ2Floor make the limit of the 99th percentile of a
	// batch's commit inside windows: the factor times the same outside windows, or the
	// floor if that is more.
	StoreGateQ2Factor = 2
	StoreGateQ2Floor  = 50 * time.Millisecond
	// StoreGateQ3Max is the most the longest rewrite may take, with StoreGateQ3KeepDays
	// days of history kept.
	StoreGateQ3Max      = 2 * time.Hour
	StoreGateQ3KeepDays = 30
	// StoreGateRate is the writer's throughput inside windows that Q4 asks for, in
	// records per second, and the load outside windows below which Q3 is not judged, so
	// that a rewrite is not shown to be quick by writing slowly beside it.
	StoreGateRate = 3000
	// StoreGateQ5HeapMargin is how much live heap a build may peak above Lroot/off's,
	// and StoreGateRSSCeiling the peak resident set it may not exceed, both in bytes
	// and both in the decimal units the rules are worded in (512 MB, 14 GB): reading
	// them as MiB and GiB would relax them by 4.9% and 7.4%.
	StoreGateQ5HeapMargin = 512_000_000
	StoreGateRSSCeiling   = 14_000_000_000
)

// StoreGateRulesText is the text of the rules, made from the constants above: a change
// to a limit or to a word changes it, and with it [StoreGateRulesDigest].
func StoreGateRulesText() string {
	return fmt.Sprintf(`store gate rules %d
window: from a retention's publication to the end of its rewrite; a batch that overlaps it, its first and last instants included, is inside; the settle runs from the end of the rewrite to its own end, and a batch that overlaps it and no window is settling: it is counted in neither the inside nor the outside (so not in the control, and not in Q2's baseline) and its longest and 99th percentile are reported
Q1: the longest batch inside a window is at most %s; the longest batch outside every window and every settle is reported as the control
Q2: the 99th percentile of a batch's commit inside windows, read as the upper bound of its histogram bucket, is at most max(%dx the same outside windows and settles, read as the lower bound of its bucket, %s)
Q3: with %d days kept, the longest rewrite (from the publication to the end of the rewrite) is at most %s, at a load outside windows of at least %d records/s; a build below that load is not judged
Q4: the records of the batches inside windows over the windows' total time plus the time of those batches outside the windows are at least %d records/s
Q5: the peak live heap is at most that of Lroot/off on the same plan plus %d MB, both built with the same GOGC, GOMEMLIMIT and metrics interval; the peak resident set of the build process is at most %d GB; MB and GB are 10^6 and 10^9 bytes
`, StoreGateRulesVersion, StoreGateQ1MaxWait, StoreGateQ2Factor, StoreGateQ2Floor,
		StoreGateQ3KeepDays, StoreGateQ3Max, StoreGateRate, StoreGateRate,
		StoreGateQ5HeapMargin/1_000_000, int64(StoreGateRSSCeiling)/1_000_000_000)
}

// StoreGateRulesDigest is the SHA-256 of [StoreGateRulesText], as hex.
func StoreGateRulesDigest() string {
	sum := sha256.Sum256([]byte(StoreGateRulesText()))
	return hex.EncodeToString(sum[:])
}

const (
	// RetentionModeKey is the key of a manifest's Describe that gives how the root
	// store's retention ran in the build: "sync", the store's only mode. A manifest of a
	// candidate that is not built through the root store does not have it.
	RetentionModeKey = "retention_mode"
	// GoGCKey is the key of a manifest's Describe that gives the Go runtime's GC target
	// the build ran under (GOGC), in percent, or "off". A manifest without it was built
	// before it was recorded.
	GoGCKey = "go_gc"
)

// gateWindowOpen is the end of a window that has not ended.
const gateWindowOpen = math.MaxInt64

// GateWindow is one retention window of a build, in nanoseconds since the build began
// writing: the retention's publication, the end of its rewrite, the end of its settle,
// and the instant the call that made it returned to the writer. In synchronous mode
// the call returns after the settle and the writer is blocked until then; in the mode
// that runs beside the writer it returns at the publication.
type GateWindow struct {
	StartNs, RewriteEndNs, SettleEndNs, ReturnNs int64
}

// GateBatches counts the batches a build wrote inside, or outside, every window: how
// many, how many records, and how long they took to commit.
type GateBatches struct {
	Batches, Records int64
	Commit           Histogram
	// SpillNs is, for the batches inside windows, the time of their spans that lies
	// outside every window (a batch that begins before a publication or ends after a
	// rewrite): Q4 divides by the windows' time and this.
	SpillNs int64
}

// GateTrace is what a build records for the store gate: the retention windows, the
// batches inside and outside them, the time the builder spent writing and resting,
// the history it kept, the peak resident set, and the largest of the samples a
// retention reports (the recorder adds samples up; these are their maxima).
type GateTrace struct {
	// Mode is the retention mode of a root-store build ("sync"), empty for a candidate
	// that has none.
	Mode    string
	Windows []GateWindow
	// Inside are the batches that overlap a window, Settling those that overlap a
	// settle and no window, and Outside the rest.
	Inside, Settling, Outside GateBatches
	// WriteNs is the time from the first batch to the end of the stream, RestNs the part
	// of it the builder spent waiting for the database to be at rest after a retention,
	// and KeepNs the history the last retention kept.
	WriteNs, RestNs, KeepNs int64
	// PeakRSS is the largest resident set of the build process, in bytes; 0 if not read.
	PeakRSS int64
	// Maxima are the largest values of the samples "retain.max_prefix_records" and
	// "retain.chunk_hold_ns".
	Maxima map[string]int64
}

// gateMaxima are the samples whose largest value a build keeps.
var gateMaxima = []string{"retain.max_prefix_records", "retain.chunk_hold_ns"}

// maxRecorder passes everything to the recorder it wraps, and keeps the largest value
// of each sample in [gateMaxima].
type maxRecorder struct {
	engine.Recorder
	mu  sync.Mutex
	max map[string]int64
}

func newMaxRecorder(r engine.Recorder) *maxRecorder {
	return &maxRecorder{Recorder: r, max: map[string]int64{}}
}

// Sample implements [engine.Recorder].
func (m *maxRecorder) Sample(name string, v int64) {
	if slices.Contains(gateMaxima, name) {
		m.mu.Lock()
		m.max[name] = max(m.max[name], v)
		m.mu.Unlock()
	}
	m.Recorder.Sample(name, v)
}

func (m *maxRecorder) maxima() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int64{}
	for k, v := range m.max {
		out[k] = v
	}
	return out
}

// gateTracker builds a [GateTrace] as a build runs. The windows come in order and do
// not overlap; a batch is inside if it overlaps one, its first and last instants
// included. Every method accepts a nil receiver, and is safe to call from the
// goroutine that retains and the one that writes.
type gateTracker struct {
	mu                        sync.Mutex
	epoch                     time.Time
	windows                   []GateWindow
	inside, settling, outside GateBatches
	rest                      time.Duration
}

func newGateTracker(epoch time.Time) *gateTracker { return &gateTracker{epoch: epoch} }

func (g *gateTracker) off(t time.Time) int64 { return t.Sub(g.epoch).Nanoseconds() }

// publish opens a window at the publication of a retention. Until the rewrite ends the
// window is open, and a batch that ends after its start is inside.
func (g *gateTracker) publish(at time.Time) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.off(at)
	g.windows = append(g.windows, GateWindow{StartNs: s, RewriteEndNs: gateWindowOpen, SettleEndNs: gateWindowOpen, ReturnNs: s})
}

// rewriteEnded closes the open window at the end of its rewrite.
func (g *gateTracker) rewriteEnded(at time.Time) {
	if w := g.open(); w != nil {
		g.mu.Lock()
		w.RewriteEndNs = g.off(at)
		g.mu.Unlock()
	}
}

// settleEnded sets the end of the settle of the last window.
func (g *gateTracker) settleEnded(at time.Time) {
	if w := g.last(); w != nil {
		g.mu.Lock()
		w.SettleEndNs = g.off(at)
		g.mu.Unlock()
	}
}

// returned sets the instant the call that made the last window returned to the writer.
func (g *gateTracker) returned(at time.Time) {
	if w := g.last(); w != nil {
		g.mu.Lock()
		w.ReturnNs = g.off(at)
		g.mu.Unlock()
	}
}

func (g *gateTracker) last() *GateWindow {
	if g == nil {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.windows) == 0 {
		return nil
	}
	return &g.windows[len(g.windows)-1]
}

func (g *gateTracker) open() *GateWindow {
	if w := g.last(); w != nil && w.RewriteEndNs == gateWindowOpen {
		return w
	}
	return nil
}

// syncWindow records the window of a retention that did all of its work before it
// returned: the rewrite took work from the call at start, then the flush and the
// settle, and the call returned at ret. A retention that rewrote nothing makes none.
func (g *gateTracker) syncWindow(start, ret time.Time, work, flush, settle time.Duration) {
	if g == nil || work <= 0 {
		return
	}
	g.publish(start)
	g.rewriteEnded(start.Add(work))
	g.settleEnded(start.Add(work + flush + settle))
	g.returned(ret)
}

// batch records a batch that began at start and took d to commit.
func (g *gateTracker) batch(start time.Time, d time.Duration, records int) {
	if g == nil {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	s := g.off(start)
	e := s + d.Nanoseconds()
	in, settling := false, false
	var overlap int64 // the time of the batch inside windows
	for i := len(g.windows) - 1; i >= 0; i-- {
		w := g.windows[i]
		if w.SettleEndNs < s { // the windows are in order: the earlier ones end earlier
			break
		}
		switch {
		case w.StartNs <= e && s <= w.RewriteEndNs:
			in = true
			overlap += min(e, w.RewriteEndNs) - max(s, w.StartNs)
		case s <= w.SettleEndNs && e >= w.RewriteEndNs:
			settling = true
		}
	}
	b := &g.outside
	switch {
	case in:
		b = &g.inside
		b.SpillNs += max(e-s-overlap, 0)
	case settling:
		b = &g.settling
	}
	b.Batches++
	b.Records += int64(records)
	b.Commit.Add(d)
}

// rested adds the time the builder waited for the database to be at rest.
func (g *gateTracker) rested(d time.Duration) {
	if g == nil {
		return
	}
	g.mu.Lock()
	g.rest += d
	g.mu.Unlock()
}

// trace is the record of the build up to end. Windows still open stay open in it, and
// the judge refuses them.
func (g *gateTracker) trace(end time.Time, mode string, keep time.Duration, maxima map[string]int64) *GateTrace {
	g.mu.Lock()
	defer g.mu.Unlock()
	return &GateTrace{
		Mode: mode, Windows: slices.Clone(g.windows), Inside: g.inside, Settling: g.settling, Outside: g.outside,
		WriteNs: g.off(end), RestNs: g.rest.Nanoseconds(), KeepNs: keep.Nanoseconds(), Maxima: maxima,
	}
}

// bucketLower is the lower bound of a histogram bucket, in nanoseconds: its own
// duration for the exact buckets, and the start of its range for the others.
func bucketLower(b int) int64 {
	if b < 8 {
		return int64(b)
	}
	k, sub := (b-8)/8+4, (b-8)%8
	return int64(8+sub) << (k - 4)
}

// quantileBounds are the bounds of the bucket the q-quantile falls in: the quantile is
// at least lo and, for a bucket that is not exact, below hi ([Histogram.Quantile] is hi).
// Both are 0 for no durations.
func quantileBounds(h Histogram, q float64) (lo, hi int64) {
	if h.Count == 0 {
		return 0, 0
	}
	want := int64(float64(h.Count)*q + 0.999999999)
	want = min(max(want, 1), h.Count)
	var seen int64
	for b, n := range h.Buckets {
		if seen += n; seen >= want {
			return bucketLower(b), boundOf(b)
		}
	}
	return h.MaxNs, h.MaxNs
}

// processGC is the GC target the process started with, as a manifest records it ("" if
// the runtime does not say). It is read once, when the package is initialized, from the
// runtime's metrics: reading does not set anything, unlike a read through
// debug.SetGCPercent, which sets the target to read it and so shows -1 to a build that
// runs while another reads (and to code that sets it for a while, as a cold read does).
// A build records this value whatever is done to the target after the program starts.
var processGC = readGCPercent()

// readGCPercent reads the GC target from the runtime's metrics: a very large value, the
// unsigned form of -1, is "off".
func readGCPercent() string {
	s := []rtmetrics.Sample{{Name: "/gc/gogc:percent"}}
	rtmetrics.Read(s)
	if s[0].Value.Kind() != rtmetrics.KindUint64 {
		return ""
	}
	return gcPercentText(int(int64(s[0].Value.Uint64())))
}

// gcPercentText is how a manifest records the GC target: the percent in decimal, or
// "off" for none.
func gcPercentText(n int) string {
	if n < 0 {
		return "off"
	}
	return strconv.Itoa(n)
}

// StoreGateBuild is a build the gate judges: its manifest, its job if the bench
// workflow wrote one, and what its metrics file says of memory.
type StoreGateBuild struct {
	Dir      string
	Manifest *Manifest
	Job      *Job
	Metrics  StoreGateMetrics
}

// StoreGateMetrics is what the gate reads from a build's metrics file.
type StoreGateMetrics struct {
	// Lines is the number of lines, HeapLiveSamples those with a live heap above 0.
	Lines, HeapLiveSamples int
	HeapLivePeak           int64
	// TombstonesPeak is the most tombstones any line counted.
	TombstonesPeak int64
}

// LoadStoreGateBuild reads the build under dir.
func LoadStoreGateBuild(dir string) (*StoreGateBuild, error) {
	m, _, err := readManifest(dir)
	if err != nil {
		return nil, err
	}
	b := &StoreGateBuild{Dir: dir, Manifest: m}
	switch raw, err := os.ReadFile(filepath.Join(dir, JobFile)); {
	case err == nil:
		var j Job
		if err := json.Unmarshal(raw, &j); err != nil {
			return nil, fmt.Errorf("runner: %s: %w", filepath.Join(dir, JobFile), err)
		}
		b.Job = &j
	case !errors.Is(err, os.ErrNotExist):
		return nil, err
	}
	if b.Metrics, err = readStoreGateMetrics(filepath.Join(dir, MetricsFile)); err != nil {
		return nil, err
	}
	return b, nil
}

// readStoreGateMetrics reads the peak live heap and the peak tombstones off a metrics
// file; a file that is not there has none.
func readStoreGateMetrics(path string) (StoreGateMetrics, error) {
	var out StoreGateMetrics
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<22)
	for n := 1; sc.Scan(); n++ {
		var l metricLine
		if err := json.Unmarshal(sc.Bytes(), &l); err != nil {
			return out, fmt.Errorf("runner: %s line %d: %w", path, n, err)
		}
		out.Lines++
		if l.Go.HeapLive > 0 {
			out.HeapLiveSamples++
			out.HeapLivePeak = max(out.HeapLivePeak, l.Go.HeapLive)
		}
		out.TombstonesPeak = max(out.TombstonesPeak, l.Stats["tombstones"])
	}
	return out, sc.Err()
}

// LoadStoreGateBuilds loads the builds named by roots: a root with a manifest is one
// build, and one without is a directory of builds, each of its subdirectories that has
// a manifest.
func LoadStoreGateBuilds(roots []string) ([]*StoreGateBuild, error) {
	var out []*StoreGateBuild
	for _, root := range roots {
		if _, err := os.Stat(filepath.Join(root, ManifestFile)); err == nil {
			b, err := LoadStoreGateBuild(root)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
			continue
		}
		entries, err := os.ReadDir(root)
		if err != nil {
			return nil, err
		}
		found := 0
		for _, e := range entries {
			dir := filepath.Join(root, e.Name())
			if !e.IsDir() || e.Name() == DBDir {
				continue
			}
			if _, err := os.Stat(filepath.Join(dir, ManifestFile)); err != nil {
				continue
			}
			b, err := LoadStoreGateBuild(dir)
			if err != nil {
				return nil, err
			}
			out = append(out, b)
			found++
		}
		if found == 0 {
			return nil, fmt.Errorf("runner: %s is not a build and holds none (give a directory with a %s, or one whose subdirectories have)", root, ManifestFile)
		}
	}
	return out, nil
}

// The verdicts of the gate.
const (
	GateOK        = "ok"
	GateOver      = "OVER"
	GateNotJudged = "not judged"
	GateBaseline  = "baseline"
)

// The rows of the gate, in the order they are shown.
const (
	gateQ1 = iota
	gateQ2
	gateQ3
	gateQ4
	gateQ5Heap
	gateQ5RSS
	gateRows
)

var gateRowNames = [gateRows]string{
	"Q1 longest batch inside a window", "Q2 batch commit p99 inside windows", "Q3 longest rewrite",
	"Q4 writer throughput inside windows", "Q5 peak live heap", "Q5 peak resident set",
}

// StoreGateRow is one verdict, of one candidate on one architecture.
type StoreGateRow struct {
	Candidate, Arch, Gate, Value, Limit, State string
	// Partial says some repetitions of the row could not be judged.
	Partial bool
}

// StoreGateReport is the verdicts of the gate and what they stand on.
type StoreGateReport struct {
	RulesText, RulesDigest string
	// Modes are the retention modes of the builds, in order.
	Modes []string
	Rows  []StoreGateRow
	// Info is what the builds report that is not judged.
	Info []string
	// NotJudged says, for each gate a build was not judged on, why.
	NotJudged []string
	// Over is the number of rows over their limit, and Unexpected the number of rows of
	// a build that were not judged though its mode has something to judge in them.
	Over, Unexpected int
}

// gateRow is one build's verdict on one row.
type gateRow struct {
	judged, pass, baseline bool
	// byConstruction says a row that is not judged cannot be: the mode has nothing for
	// it to judge (no batch is written inside a window of a synchronous retention).
	byConstruction bool
	// margin is how much of the limit the build uses: above 1 is over, and the build
	// with the most is the worst of its repetitions.
	margin       float64
	value, limit string
	reason       string
	build        *StoreGateBuild
}

func fmtDur(ns int64) string {
	d := time.Duration(ns)
	if d >= time.Second {
		return d.Round(time.Millisecond).String()
	}
	return d.Round(time.Microsecond).String()
}

func fmtMB(n int64) string { return fmt.Sprintf("%.1f MB", float64(n)/1e6) }

func (b *StoreGateBuild) arch() string { return b.Manifest.Build.GOARCH }

func (b *StoreGateBuild) rep() int {
	if b.Job != nil {
		return b.Job.Rep
	}
	return 0
}

func (b *StoreGateBuild) label() string {
	s := b.Manifest.Candidate + " " + b.arch()
	if r := b.rep(); r > 0 {
		s += " rep " + strconv.Itoa(r)
	}
	return s + " (" + b.Dir + ")"
}

// refusal is why none of the build can be judged, or "".
func (b *StoreGateBuild) refusal() string {
	m := b.Manifest
	switch {
	case !strings.HasPrefix(m.Candidate, "Lroot/"):
		return "not a build through the root store (a candidate named Lroot/...)"
	case m.Untimed:
		return "an untimed build: a result does not come from it"
	case m.Gate == nil:
		return "built without the gate's record (by an earlier binary)"
	case m.Gate.Mode == "":
		return "no retention mode recorded"
	}
	for i, w := range m.Gate.Windows {
		switch {
		case w.RewriteEndNs == gateWindowOpen || w.SettleEndNs == gateWindowOpen:
			return fmt.Sprintf("window %d never ended", i+1)
		case w.RewriteEndNs < w.StartNs || w.SettleEndNs < w.RewriteEndNs || w.ReturnNs < w.StartNs:
			return fmt.Sprintf("window %d ends before it starts", i+1)
		}
	}
	return ""
}

// load is the builder's records per second outside the windows: the records of the
// batches outside every window (those of a settle included) over the time the stream took, less the rests and the
// time the builder was away in a retention.
func (g *GateTrace) load() (perSecond float64, ok bool) {
	away := int64(0)
	for _, w := range g.Windows {
		away += max(w.RewriteEndNs, w.ReturnNs) - w.StartNs
	}
	ns := g.WriteNs - g.RestNs - away
	// The builder writes beside a settle as it does outside everything, so the batches
	// of the settle are part of its load though they are not part of the control.
	records := g.Outside.Records + g.Settling.Records
	if ns <= 0 || records == 0 {
		return 0, false
	}
	return float64(records) / (float64(ns) / 1e9), true
}

// judgeBuild gives the build's verdict on each row; base is the Lroot/off build of the
// same plan, architecture and repetition (nil if there is none, with baseWhy saying why).
func judgeBuild(b, base *StoreGateBuild, baseWhy string) (rows [gateRows]gateRow, info string) {
	for i := range rows {
		rows[i].build = b
	}
	if why := b.refusal(); why != "" {
		for i := range rows {
			rows[i].reason = why
		}
		return rows, ""
	}
	m := b.Manifest
	t := m.Gate
	ni := func(i int, reason string) { rows[i].judged, rows[i].reason = false, reason }

	var rewriteMax, settleMax, returnMax, windowsNs int64
	for _, w := range t.Windows {
		rewriteMax = max(rewriteMax, w.RewriteEndNs-w.StartNs)
		settleMax = max(settleMax, w.SettleEndNs-w.StartNs)
		returnMax = max(returnMax, w.ReturnNs-w.StartNs)
		windowsNs += w.RewriteEndNs - w.StartNs
	}
	noInside := ""
	switch {
	case len(t.Windows) == 0:
		noInside = "no retention window (the build retained nothing, or its engine does not say how a retention spent its time)"
	case t.Inside.Batches == 0 && t.Mode == "sync":
		noInside = fmt.Sprintf("synchronous retention: no batch is written inside a window, the writer is blocked for all of it (the longest %s)", fmtDur(returnMax))
	case t.Inside.Batches == 0:
		noInside = "no batch was written inside a window"
	}

	byConstruction := len(t.Windows) > 0 && t.Inside.Batches == 0 && t.Mode == "sync"
	defer func() {
		for _, i := range []int{gateQ1, gateQ2, gateQ4} {
			rows[i].byConstruction = !rows[i].judged && byConstruction
		}
	}()

	// Q1: the longest batch inside a window, with the longest outside as the control.
	rows[gateQ1] = gateRow{build: b, judged: noInside == "", reason: noInside}
	if noInside == "" {
		v := t.Inside.Commit.MaxNs
		control := "none"
		if t.Outside.Batches > 0 {
			control = fmtDur(t.Outside.Commit.MaxNs)
		}
		rows[gateQ1].value = fmt.Sprintf("%s (control, longest outside windows: %s)", fmtDur(v), control)
		rows[gateQ1].limit = StoreGateQ1MaxWait.String()
		rows[gateQ1].pass = v <= int64(StoreGateQ1MaxWait)
		rows[gateQ1].margin = float64(v) / float64(StoreGateQ1MaxWait)
	}

	// Q2: the 99th percentile inside against twice the one outside, or the floor.
	rows[gateQ2] = gateRow{build: b, judged: noInside == "", reason: noInside}
	switch {
	case noInside != "":
	case t.Outside.Batches == 0:
		ni(gateQ2, "no batch outside the windows to compare with")
	default:
		// The inside is read at the top of its bucket and the outside at the bottom of
		// its: a bucket is within an eighth of the value it holds, and each reading is
		// the one that cannot let a batch pass that the true values would not.
		_, in := quantileBounds(t.Inside.Commit, 0.99)
		outLo, outHi := quantileBounds(t.Outside.Commit, 0.99)
		lim := max(StoreGateQ2Factor*outLo, int64(StoreGateQ2Floor))
		rows[gateQ2].value = fmt.Sprintf("%s (outside: from %s to %s)", fmtDur(in), fmtDur(outLo), fmtDur(outHi))
		rows[gateQ2].limit = fmt.Sprintf("max(%dx the outside's lower bound, %s) = %s", StoreGateQ2Factor, StoreGateQ2Floor, fmtDur(lim))
		rows[gateQ2].pass = in <= lim
		rows[gateQ2].margin = float64(in) / float64(lim)
	}

	// Q3: the longest rewrite, from the publication to its end, at the load the build ran at.
	load, loadOK := t.load()
	rows[gateQ3] = gateRow{build: b, judged: true}
	switch {
	case len(t.Windows) == 0:
		ni(gateQ3, noInside)
	default:
		loadText := "load not determinable"
		if loadOK {
			loadText = fmt.Sprintf("load %.0f records/s outside windows", load)
		}
		rows[gateQ3].value = fmt.Sprintf("%s (%s; settle ends %s after the publication)", fmtDur(rewriteMax), loadText, fmtDur(settleMax))
		rows[gateQ3].limit = fmt.Sprintf("%s with %d days kept, at a load of at least %d records/s", StoreGateQ3Max, StoreGateQ3KeepDays, StoreGateRate)
		rows[gateQ3].pass = rewriteMax <= int64(StoreGateQ3Max)
		rows[gateQ3].margin = float64(rewriteMax) / float64(StoreGateQ3Max)
		switch keep := time.Duration(t.KeepNs); {
		case !loadOK:
			ni(gateQ3, "the load outside the windows cannot be worked out (no batch outside them)")
		case load < StoreGateRate:
			ni(gateQ3, fmt.Sprintf("the load %.0f records/s is below the %d records/s the rule is judged at: a rewrite is not shown quick by writing slowly beside it", load, StoreGateRate))
		case keep < StoreGateQ3KeepDays*24*time.Hour:
			ni(gateQ3, fmt.Sprintf("the limit is for %d days kept, and this build keeps %.1f", StoreGateQ3KeepDays, keep.Hours()/24))
		}
	}

	// Q4: the records written inside windows over the windows' total time.
	rows[gateQ4] = gateRow{build: b, judged: noInside == "", reason: noInside}
	switch {
	case noInside != "":
	case windowsNs <= 0:
		ni(gateQ4, "the windows have no length")
	default:
		// A batch that overlaps a window only in part counts all its records, so the
		// time of its part outside the window is divided by too.
		rate := float64(t.Inside.Records) / (float64(windowsNs+t.Inside.SpillNs) / 1e9)
		rows[gateQ4].value = fmt.Sprintf("%.0f records/s", rate)
		rows[gateQ4].limit = fmt.Sprintf("at least %d records/s", StoreGateRate)
		rows[gateQ4].pass = rate >= StoreGateRate
		rows[gateQ4].margin = StoreGateRate / math.Max(rate, 1e-9)
	}

	// Q5: the peak live heap against Lroot/off's plus the margin, and the peak resident set.
	rows[gateQ5Heap] = gateRow{build: b, judged: true}
	switch {
	case b.Metrics.HeapLiveSamples == 0:
		ni(gateQ5Heap, "no live heap samples (build with -metrics-every)")
	case m.Candidate == "Lroot/off":
		rows[gateQ5Heap].baseline, rows[gateQ5Heap].pass = true, true
		rows[gateQ5Heap].value = fmtMB(b.Metrics.HeapLivePeak)
		rows[gateQ5Heap].limit = "the baseline"
	case base == nil:
		ni(gateQ5Heap, baseWhy)
	case base.refusal() != "":
		ni(gateQ5Heap, "the Lroot/off baseline cannot be used: "+base.refusal())
	case base.Metrics.HeapLiveSamples == 0:
		ni(gateQ5Heap, "Lroot/off has no live heap samples (build with -metrics-every)")
	default:
		if why := sameSettings(m, base.Manifest); why != "" {
			ni(gateQ5Heap, why)
			break
		}
		lim := base.Metrics.HeapLivePeak + StoreGateQ5HeapMargin
		rows[gateQ5Heap].value = fmtMB(b.Metrics.HeapLivePeak)
		rows[gateQ5Heap].limit = fmt.Sprintf("%s (Lroot/off's %s + %d MB)", fmtMB(lim), fmtMB(base.Metrics.HeapLivePeak), StoreGateQ5HeapMargin/1_000_000)
		rows[gateQ5Heap].pass = b.Metrics.HeapLivePeak <= lim
		rows[gateQ5Heap].margin = float64(b.Metrics.HeapLivePeak) / float64(lim)
	}
	rows[gateQ5RSS] = gateRow{build: b, judged: t.PeakRSS > 0, reason: "the peak resident set was not read"}
	if t.PeakRSS > 0 {
		rows[gateQ5RSS].reason = ""
		rows[gateQ5RSS].value = fmtMB(t.PeakRSS)
		rows[gateQ5RSS].limit = fmtMB(StoreGateRSSCeiling)
		rows[gateQ5RSS].pass = t.PeakRSS <= StoreGateRSSCeiling
		rows[gateQ5RSS].margin = float64(t.PeakRSS) / float64(int64(StoreGateRSSCeiling))
	}

	touch := "not recorded by this store"
	if n, ok := m.Counters["retain.touch_rewrites"]; ok {
		touch = strconv.FormatInt(n, 10)
	}
	maxima := func(k string) string {
		if v, ok := t.Maxima[k]; ok {
			return strconv.FormatInt(v, 10)
		}
		return "not recorded"
	}
	chunk := maxima("retain.chunk_hold_ns")
	if v, ok := t.Maxima["retain.chunk_hold_ns"]; ok {
		chunk = fmtDur(v)
	}
	loadText := "not determinable"
	if loadOK {
		loadText = fmt.Sprintf("%.0f records/s", load)
	}
	settling := "no batch"
	if st := t.Settling; st.Batches > 0 {
		_, p99 := quantileBounds(st.Commit, 0.99)
		settling = fmt.Sprintf("%d batches, longest %s, 99th percentile %s", st.Batches, fmtDur(st.Commit.MaxNs), fmtDur(p99))
	}
	info = fmt.Sprintf("%s: mode %s; %d windows, longest rewrite %s, longest settle end %s, longest call %s; retain.max_prefix_records max %s, retain.chunk_hold_ns max %s, retain.touch_rewrites %s; load outside windows %s; settling (inside no window): %s; tombstones: peak %d sampled, %d at the end of the stream, %d after the compaction (reported, not judged)",
		b.label(), t.Mode, len(t.Windows), fmtDur(rewriteMax), fmtDur(settleMax), fmtDur(returnMax),
		maxima("retain.max_prefix_records"), chunk, touch, loadText, settling,
		b.Metrics.TombstonesPeak, m.StatsBuilt["tombstones"], m.StatsCompacted["tombstones"])
	return rows, info
}

// sameSettings returns why the live heaps of two builds cannot be compared: their GC
// target, their memory limit or their metrics interval differ (the peak of a sample
// depends on how often it is taken) or, for the target, was not recorded.
func sameSettings(a, b *Manifest) string {
	ga, gb := a.Describe[GoGCKey], b.Describe[GoGCKey]
	switch {
	case ga == "" || gb == "":
		return fmt.Sprintf("%s is not recorded in both manifests: the live heaps of builds under settings that are not known to be the same are not compared", GoGCKey)
	case ga != gb:
		return fmt.Sprintf("%s differs (%s and %s): builds under different settings are not compared", GoGCKey, ga, gb)
	}
	if la, lb := describedOr(a, GoMemoryLimitKey, "none"), describedOr(b, GoMemoryLimitKey, "none"); la != lb {
		return fmt.Sprintf("%s differs (%s and %s): builds under different settings are not compared", GoMemoryLimitKey, la, lb)
	}
	if ia, ib := describedOr(a, MetricsKey, "0s"), describedOr(b, MetricsKey, "0s"); ia != ib {
		return fmt.Sprintf("%s differs (%s and %s): the sampled peaks of builds sampled at different intervals are not compared", MetricsKey, ia, ib)
	}
	return ""
}

// JudgeStoreGate judges the builds, each candidate on each architecture, from its
// repetitions: the worst of the repetitions that could be judged is the row.
func JudgeStoreGate(builds []*StoreGateBuild) *StoreGateReport {
	rep := &StoreGateReport{RulesText: StoreGateRulesText(), RulesDigest: StoreGateRulesDigest()}
	bs := slices.Clone(builds)
	sort.SliceStable(bs, func(i, j int) bool {
		a, b := bs[i], bs[j]
		if a.arch() != b.arch() {
			return a.arch() < b.arch()
		}
		if a.Manifest.Candidate != b.Manifest.Candidate {
			return a.Manifest.Candidate < b.Manifest.Candidate
		}
		if a.rep() != b.rep() {
			return a.rep() < b.rep()
		}
		return a.Dir < b.Dir
	})

	type baseKey struct {
		arch, plan string
		rep        int
	}
	bases := map[baseKey][]*StoreGateBuild{}
	for _, b := range bs {
		if b.Manifest.Candidate == "Lroot/off" {
			k := baseKey{b.arch(), b.Manifest.PlanDigest, b.rep()}
			bases[k] = append(bases[k], b)
		}
	}

	type group struct{ cand, arch string }
	var order []group
	groups := map[group][][gateRows]gateRow{}
	for _, b := range bs {
		var base *StoreGateBuild
		baseWhy := "no Lroot/off build of the same plan, architecture and repetition to compare with"
		switch found := bases[baseKey{b.arch(), b.Manifest.PlanDigest, b.rep()}]; len(found) {
		case 1:
			base = found[0]
		case 0:
		default:
			baseWhy = "more than one Lroot/off build of the same plan, architecture and repetition: which is the baseline?"
		}
		rows, info := judgeBuild(b, base, baseWhy)
		if info != "" {
			rep.Info = append(rep.Info, info)
		}
		if mode := b.Manifest.Describe[RetentionModeKey]; mode != "" && !slices.Contains(rep.Modes, mode) {
			rep.Modes = append(rep.Modes, mode)
		}
		g := group{b.Manifest.Candidate, b.arch()}
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], rows)
		for i, r := range rows {
			if !r.judged {
				if !r.byConstruction {
					rep.Unexpected++
				}
				rep.NotJudged = append(rep.NotJudged, fmt.Sprintf("%s: %s: %s", b.label(), gateRowNames[i], r.reason))
			}
		}
	}

	for _, g := range order {
		reps := groups[g]
		for i := 0; i < gateRows; i++ {
			var worst *gateRow
			judged, n := 0, len(reps)
			for k := range reps {
				r := &reps[k][i]
				if !r.judged {
					continue
				}
				judged++
				if worst == nil || r.margin > worst.margin || (!r.pass && worst.pass) {
					worst = r
				}
			}
			row := StoreGateRow{Candidate: g.cand, Arch: g.arch, Gate: gateRowNames[i], State: GateNotJudged}
			if worst == nil {
				// Not judged: show the value of the first repetition that has one.
				for k := range reps {
					if reps[k][i].value != "" {
						row.Value, row.Limit = reps[k][i].value, reps[k][i].limit
						break
					}
				}
				if row.Value == "" {
					row.Value = "-"
				}
				if row.Limit == "" {
					row.Limit = "-"
				}
				rep.Rows = append(rep.Rows, row)
				continue
			}
			row.Value, row.Limit = worst.value, worst.limit
			switch {
			case worst.baseline:
				row.State = GateBaseline
			case worst.pass:
				row.State = GateOK
			default:
				row.State = GateOver
				rep.Over++
			}
			if n > 1 {
				row.Value += fmt.Sprintf(" [worst of %d judged of %d]", judged, n)
			}
			row.Partial = judged < n
			rep.Rows = append(rep.Rows, row)
		}
	}
	return rep
}

// WriteStoreGate prints the report: the rules and their digest, the retention modes, one
// line per row, what the builds reported that is not judged, and what was not judged.
func WriteStoreGate(w io.Writer, rep *StoreGateReport) {
	fmt.Fprintf(w, "store retention gate; rules digest %s\n%s", rep.RulesDigest, rep.RulesText)
	modes := "none"
	if len(rep.Modes) > 0 {
		modes = strings.Join(rep.Modes, ", ")
	}
	fmt.Fprintf(w, "retention mode: %s\n", modes)
	if !slices.Contains(rep.Modes, "background") {
		fmt.Fprintln(w, "Background: not available on this store")
	}
	if slices.Contains(rep.Modes, "sync") {
		fmt.Fprintln(w, "synchronous retention: the writer is blocked for the whole of each window, so no batch is written inside one and Q1, Q2 and Q4 have nothing to judge")
	}
	fmt.Fprintln(w)
	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	for _, r := range rep.Rows {
		state := r.State
		if r.Partial {
			state += " (partial)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\tlimit %s\t%s\n", r.Candidate, r.Arch, r.Gate, r.Value, r.Limit, state)
	}
	_ = tw.Flush()
	if len(rep.Info) > 0 {
		fmt.Fprintln(w, "\nreported, not judged:")
		for _, s := range rep.Info {
			fmt.Fprintf(w, "  %s\n", s)
		}
	}
	if len(rep.NotJudged) > 0 {
		fmt.Fprintln(w, "\nNOT JUDGED:")
		for _, s := range rep.NotJudged {
			fmt.Fprintf(w, "  - %s\n", s)
		}
	}
}
