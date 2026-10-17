package demo

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// testPolicy is the policy a server of the cluster name opens its store with: the
// authority order of its producers, and the boots of a host told apart.
func testPolicy() lifecycle.Policy {
	return lifecycle.Policy{
		BootKey: lifecycle.BootID,
		Rank: map[lifecycle.Producer]int{
			"k8sobjects/" + ClusterName: 2,
			"k8scluster/" + ClusterName: 1,
		},
	}
}

func build(t testing.TB, seed uint64, o Options) *Built {
	t.Helper()
	if o.Policy.BootKey == "" {
		o.Policy = testPolicy()
	}
	b, err := Build(t.Context(), NewWorld(seed), o)
	if err != nil {
		t.Fatalf("Build(seed %d): %v", seed, err)
	}
	t.Cleanup(func() { _ = b.Store.Close() })
	return b
}

// canonical is the world of the default seed, built once for the tests that only
// read it.
var canonical struct {
	once sync.Once
	b    *Built
	err  error
}

func canonicalBuild(t testing.TB) *Built {
	t.Helper()
	canonical.once.Do(func() {
		canonical.b, canonical.err = Build(context.Background(), NewWorld(DefaultSeed), Options{Policy: testPolicy()})
	})
	if canonical.err != nil {
		t.Fatalf("Build: %v", canonical.err)
	}
	return canonical.b
}

func TestMain(m *testing.M) {
	code := m.Run()
	if canonical.b != nil {
		_ = canonical.b.Store.Close()
	}
	os.Exit(code)
}

// One seed gives one store, whatever the machine does between two builds: the same
// token, record count and digest, and another seed gives another digest.
func TestSameSeedSameStore(t *testing.T) {
	a := canonicalBuild(t)
	b := build(t, DefaultSeed, Options{})
	if a.LastSeq != b.LastSeq || a.Records != b.Records || a.Digest != b.Digest {
		t.Errorf("two builds of seed %d: token %d and %d, %d and %d records, digest %s and %s",
			DefaultSeed, a.LastSeq, b.LastSeq, a.Records, b.Records, a.Digest, b.Digest)
	}
	if other := build(t, 2, Options{}); other.Digest == a.Digest {
		t.Errorf("seeds 1 and 2 give one digest, %s", a.Digest)
	}
	if a.LastSeq == 0 || a.Records != int(a.LastSeq) {
		t.Errorf("token %d and %d records: the demo writes through the sequencer alone, one sequence number a record", a.LastSeq, a.Records)
	}
	if len(a.Digest) != 64 {
		t.Errorf("digest %q is not a SHA-256", a.Digest)
	}
}

// The build depends on the injected clock and on nothing else: moved by years, in
// either direction, the same world makes the same number of records. A wall-clock
// read on the path would show, because the real date is neither of the two. The
// test sets the package's anchor, so it cannot run beside another test of the
// package; none does.
func TestBuildDoesNotDependOnTheDate(t *testing.T) {
	saved := anchor
	t.Cleanup(func() { anchor = saved })
	want := canonicalBuild(t)
	for _, years := range []int{-3, 5} {
		anchor = saved.AddDate(years, 0, 0)
		b := build(t, DefaultSeed, Options{})
		if b.LastSeq != want.LastSeq || b.Records != want.Records || b.Warnings != 0 {
			t.Errorf("anchor moved %d years: token %d, %d records, %d warnings; want %d, %d, 0", years, b.LastSeq, b.Records, b.Warnings, want.LastSeq, want.Records)
		}
		if b.Digest == want.Digest {
			t.Errorf("anchor moved %d years: the digest is the same, but the records are timed differently", years)
		}
	}
}

// The synthetic requests are taken in whole: nothing is rejected, refused or
// logged at warn level or above, by the sequencer, the translators, the detector
// or the resume step.
func TestTheSequencerTakesTheWorldCleanly(t *testing.T) {
	var lines atomic.Int64
	b := build(t, DefaultSeed, Options{Logger: slog.New(countHandler{&lines})})
	if b.Warnings != 0 || lines.Load() != 0 {
		t.Errorf("%d warn-level lines counted, %d passed on; want none", b.Warnings, lines.Load())
	}
	// The same holds for the other seeds the shape is checked for.
	for _, seed := range seeds[1:4] {
		if o := build(t, seed, Options{}); o.Warnings != 0 {
			t.Errorf("seed %d: %d warn-level lines", seed, o.Warnings)
		}
	}
}

type countHandler struct{ n *atomic.Int64 }

func (countHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h countHandler) Handle(context.Context, slog.Record) error {
	h.n.Add(1)
	return nil
}
func (h countHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h countHandler) WithGroup(string) slog.Handler      { return h }

// The barrier writes nothing: the token before and after a request with no records
// is the same, after requests that did write and after a tick that wrote.
func TestTheBarrierChangesNothing(t *testing.T) {
	dr, err := newDriver(Options{Policy: testPolicy()})
	if err != nil {
		t.Fatal(err)
	}
	defer dr.abort()
	w := NewWorld(DefaultSeed)
	var at time.Time
	for _, r := range w.Requests() {
		if r.At.After(anchor) {
			break
		}
		at = r.At
		dr.clk.set(at)
		if err := dr.accept(t.Context(), r.Data(), r.What, at); err != nil {
			t.Fatal(err)
		}
	}
	before := dr.st.LastSeq()
	if before == 0 {
		t.Fatal("the first requests wrote nothing")
	}
	for range 3 {
		if err := dr.accept(t.Context(), capture.LogsData{}, "barrier", at); err != nil {
			t.Fatal(err)
		}
		if got := dr.st.LastSeq(); got != before {
			t.Fatalf("an empty request moved the token from %d to %d", before, got)
		}
	}
	// A tick does not, either, until a refresh falls due, and the barrier after
	// it changes nothing more.
	if err := dr.flush(t.Context(), anchor.Add(Step)); err != nil {
		t.Fatal(err)
	}
	after := dr.st.LastSeq()
	if err := dr.accept(t.Context(), capture.LogsData{}, "barrier", anchor.Add(Step)); err != nil {
		t.Fatal(err)
	}
	if got := dr.st.LastSeq(); got != after {
		t.Errorf("the barrier after a tick moved the token from %d to %d", after, got)
	}
}

// What a person and an agent are told names the window and the nodes, prints no
// endpoint id, and stays within a kilobyte where it is an instruction.
func TestWhatIsPrintedIsSafeAndBounded(t *testing.T) {
	b := canonicalBuild(t)
	ins, banner := b.Instructions(), b.Banner()
	if len(ins) > 1024 {
		t.Errorf("the instructions are %d bytes, want at most 1 KiB", len(ins))
	}
	for name, text := range map[string]string{"instructions": ins, "banner": banner} {
		for _, want := range []string{"2026-10-01T00:00:00Z", "2026-10-01T06:00:00Z", "synthetic"} {
			if !strings.Contains(text, want) {
				t.Errorf("the %s lack %q:\n%s", name, want, text)
			}
		}
		if strings.Contains(text, "topo.endpoint:") {
			t.Errorf("the %s print an endpoint id:\n%s", name, text)
		}
	}
	if !strings.Contains(banner, b.Story.Failed.ID) || !strings.Contains(banner, b.Digest[:16]) || strings.Contains(banner, b.Digest[:17]) {
		t.Errorf("the banner lacks node-2's id or names more than 16 digits of the digest:\n%s", banner)
	}
	for _, n := range b.Story.Nodes {
		if !strings.Contains(ins, n.ID) || !strings.Contains(ins, n.Name) {
			t.Errorf("the instructions do not name %s (%s)", n.Name, n.ID)
		}
	}
	for _, p := range append(b.Story.Evicted, b.Story.Checkout, b.Story.LegacyPay) {
		if strings.Contains(ins, p.Name) || strings.Contains(ins, p.ID) {
			t.Errorf("the instructions name the pod %s", p.Name)
		}
	}
	if strings.Count(banner, "\n") != 6 {
		t.Errorf("the banner is %d lines, want the build, three of story and two questions:\n%s", strings.Count(banner, "\n"), banner)
	}
}

// A build whose context is done stops with the context's error, and leaves no loop
// behind: the store it made is closed.
func TestBuildStopsWhenItsContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	b, err := Build(ctx, NewWorld(DefaultSeed), Options{Policy: testPolicy()})
	if !errors.Is(err, context.Canceled) || b != nil {
		t.Fatalf("Build with a done context: %v, %v; want the context's error and no result", b, err)
	}
}

// The only thing the translators set aside is the namespace event the cluster
// receiver sends in every report, which the world sends on purpose for the
// detector of cluster identity and which is not in the catalog. So the skips are
// that one reason of that one producer, as many as the world sent, which is
// every report's every namespace; and there is no note. The count is compared
// with the world's own: a message the sequencer worded otherwise would show as
// no skip at all.
func TestEverySkipIsANamespaceEventTheWorldSent(t *testing.T) {
	b := canonicalBuild(t)
	w := NewWorld(DefaultSeed)
	reports := 0
	for _, r := range w.Requests() {
		if r.Emitter == EmitClusterReport {
			reports++
		}
	}
	want := reports * len(w.Namespaces)
	if want == 0 || b.NamespaceEvents != want {
		t.Errorf("the driver counted %d namespace events, want %d reports of %d namespaces = %d", b.NamespaceEvents, reports, len(w.Namespaces), want)
	}
	wantSkips := []Count{{Producer: "k8scluster/" + ClusterName, Reason: k8scluster.SkipTypeNotInCatalog, N: want}}
	if !slices.Equal(b.Skips, wantSkips) {
		t.Errorf("skips %v, want %v", b.Skips, wantSkips)
	}
	if u := b.UnexplainedSkips(); u != 0 {
		t.Errorf("%d skips unexplained", u)
	}
	if len(b.Notes) != 0 {
		t.Errorf("notes %v, want none", b.Notes)
	}
}

// A skip beyond the namespace events, of the same reason or another, and a
// namespace event that was not set aside, are each unexplained.
func TestUnexplainedSkips(t *testing.T) {
	cluster := "k8scluster/" + ClusterName
	for _, tc := range []struct {
		name  string
		skips []Count
		sent  int
		want  int
	}{
		{"the namespace events alone", []Count{{cluster, k8scluster.SkipTypeNotInCatalog, 10}}, 10, 0},
		{"one more of the same reason", []Count{{cluster, k8scluster.SkipTypeNotInCatalog, 11}}, 10, 1},
		{"one fewer", []Count{{cluster, k8scluster.SkipTypeNotInCatalog, 9}}, 10, 1},
		{"another reason beside them", []Count{{cluster, k8scluster.SkipTypeNotInCatalog, 10}, {cluster, "x", 2}}, 10, 2},
		{"another producer beside them", []Count{{"k8sobjects/" + ClusterName, k8scluster.SkipTypeNotInCatalog, 10}}, 10, 20},
		{"none counted", nil, 10, 10},
	} {
		if got := (&Built{Skips: tc.skips, NamespaceEvents: tc.sent}).UnexplainedSkips(); got != tc.want {
			t.Errorf("%s: %d unexplained, want %d", tc.name, got, tc.want)
		}
	}
}

// The sequencer's logger reads the skip and note lines at the levels they are
// logged at, counts the lines of warn level or above, and passes on those lines
// and no others.
func TestTheLoggerCountsSkipsAndPassesOnWarnings(t *testing.T) {
	var passed atomic.Int64
	c := &logCounts{}
	log := slog.New(lineCounter{next: countHandler{&passed}, c: c})
	log.Debug(skipMessage, "producer", "p", "reason", "r", "count", 3)
	log.Debug(skipMessage, "producer", "p", "reason", "r", "count", 4)
	log.Info(noteMessage, "producer", "p", "reason", "n", "detail", "d", "count", 2)
	log.Info("something else", "count", 9)
	log.Debug("something else", "count", 9)
	if got := c.counts(c.skips); !slices.Equal(got, []Count{{"p", "r", 7}}) {
		t.Errorf("skips %v", got)
	}
	if got := c.counts(c.notes); !slices.Equal(got, []Count{{"p", "n", 2}}) {
		t.Errorf("notes %v", got)
	}
	if c.warns.Load() != 0 || passed.Load() != 0 {
		t.Errorf("%d warnings counted and %d passed on before any was logged", c.warns.Load(), passed.Load())
	}
	log.Warn("a warning")
	log.Error("an error")
	if c.warns.Load() != 2 || passed.Load() != 2 {
		t.Errorf("%d warnings counted and %d passed on, want 2 and 2", c.warns.Load(), passed.Load())
	}
}
