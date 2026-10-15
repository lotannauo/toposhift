package backup_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
)

// pods returns n Observes of distinct pods at base+at, numbered from seq.
func pods(t testing.TB, seq uint64, n int, at time.Duration) []store.Record {
	t.Helper()
	out := make([]store.Record, n)
	for i := range out {
		pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "pod-"+string(rune('a'+i)))
		out[i] = observe(catalog.L2, store.EntitySubject(pod), "k8sobjects", base.Add(at), shadowTTL)
		out[i].Seq = seq + uint64(i)
	}
	return out
}

func newCutter(t testing.TB, segs segment.Store) *backup.Cutter {
	t.Helper()
	o := testOptions(metaFor(lifecycle.Policy{}))
	o.TempDir = t.TempDir()
	c, err := backup.NewCutter(ctx, segs, prefix, o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestAddRefusesAGapAndKeepsNothingOfIt(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	c := newCutter(t, segs)
	wall := base.Add(time.Minute)
	if err := c.Add(pods(t, 2, 2, 0), wall); !errors.Is(err, backup.ErrGap) {
		t.Fatalf("a first batch at seq 2: %v, want ErrGap", err)
	}
	holed := pods(t, 1, 3, 0)
	holed[2].Seq = 4
	if err := c.Add(holed, wall); !errors.Is(err, backup.ErrGap) {
		t.Fatalf("a batch with a hole: %v, want ErrGap", err)
	}
	if err := c.Add(pods(t, 1, 3, 0), wall); err != nil {
		t.Fatalf("the batch that follows: %v", err)
	}
	if err := c.Add(pods(t, 5, 1, 0), wall); !errors.Is(err, backup.ErrGap) {
		t.Fatalf("a batch after a gap: %v, want ErrGap", err)
	}
	m, err := c.Cut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.Seq != 3 || len(m.Cuts) != 1 || m.Cuts[0].Records[backup.ClassL2] != 3 || len(m.Files) != 1 || m.Files[0].Records != 3 {
		t.Fatalf("after the refused batches the backup holds %+v", m)
	}
}

// A clock that steps back is raised to the one before and counted, not refused.
func TestAddClampsABackwardClock(t *testing.T) {
	t.Parallel()
	c := newCutter(t, newMemSegments())
	for i, at := range []time.Duration{2 * time.Minute, time.Minute, 3 * time.Minute, time.Minute} {
		if err := c.Add(pods(t, uint64(i+1), 1, 0), base.Add(at)); err != nil {
			t.Fatalf("batch %d at %s: %v", i, at, err)
		}
	}
	m, err := c.Cut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	f := m.Files[0]
	if f.WallClamps != 2 || f.WallFirstNS != base.Add(2*time.Minute).UnixNano() || f.WallLastNS != base.Add(3*time.Minute).UnixNano() {
		t.Fatalf("clamps %d, first %d, last %d", f.WallClamps, f.WallFirstNS, f.WallLastNS)
	}
}

func TestRetainedRefusesAnAskThatDecreases(t *testing.T) {
	t.Parallel()
	c := newCutter(t, newMemSegments())
	h := func(at time.Time) [4]store.Horizon {
		return [4]store.Horizon{{}, {Time: at.Add(-30 * time.Minute), Seq: 5}, {Time: at, Seq: 5}, {Time: at, Seq: 5}}
	}
	at := base.Add(time.Hour)
	if err := c.Retained(at, h(at), 5); err != nil {
		t.Fatal(err)
	}
	if err := c.Retained(at.Add(-time.Second), h(at), 5); err == nil || !strings.Contains(err.Error(), "never decrease") {
		t.Fatalf("a decreasing ask: %v", err)
	}
	if err := c.Retained(at.Add(time.Second), h(at.Add(-time.Minute)), 6); err == nil || !strings.Contains(err.Error(), "backward") {
		t.Fatalf("a horizon that moved backward: %v", err)
	}
	if err := c.Retained(at, h(at), 6); err != nil {
		t.Fatalf("an equal ask: %v", err)
	}
}

// Every manifest takes the next generation, so two Retires in a row, which add no
// Seq, write two manifests, and only the latest two are kept.
func TestRetiresInARowWriteGenerations(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	c := newCutter(t, segs)
	if err := c.Add(pods(t, 1, 2, 0), base); err != nil {
		t.Fatal(err)
	}
	var gens []uint64
	for range 4 {
		ret, err := c.Retire(ctx)
		if err != nil {
			t.Fatalf("Retire: %v", err)
		}
		gens = append(gens, ret.Manifest.Generation)
	}
	// The first Retire cuts the open file first, as generation 1.
	if gens[0] != 2 || gens[1] != 3 || gens[3] != 5 {
		t.Fatalf("generations %v", gens)
	}
	var left []string
	for info, err := range segs.List(ctx, prefix+"/v1/manifests/") {
		if err != nil {
			t.Fatal(err)
		}
		left = append(left, info.Name)
	}
	if len(left) != 2 || left[0] != backup.ManifestName(prefix, 4) || left[1] != backup.ManifestName(prefix, 5) {
		t.Fatalf("manifests left: %v", left)
	}
}

// A cutter goes on from the backup under its prefix: the next batch must follow
// it, and other options are refused.
func TestNewCutterResumes(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	c := newCutter(t, segs)
	if err := c.Add(pods(t, 1, 3, 0), base); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Cut(ctx); err != nil {
		t.Fatal(err)
	}
	again := newCutter(t, segs)
	if err := again.Add(pods(t, 1, 1, time.Minute), base); !errors.Is(err, backup.ErrGap) {
		t.Fatalf("a batch that repeats the backup's seqs: %v", err)
	}
	if err := again.Add(pods(t, 4, 2, time.Minute), base.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	m, err := again.Cut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if m.Generation != 2 || m.Seq != 5 || len(m.Files) != 2 || m.Files[1].AfterSeq != 3 {
		t.Fatalf("the resumed backup is %+v", m)
	}
	for name, mod := range map[string]func(*backup.Options){
		"another store meta": func(o *backup.Options) { o.Store.Offsets[2] = time.Hour },
		"another margin":     func(o *backup.Options) { o.Margin = time.Hour },
		"another clock bound": func(o *backup.Options) {
			o.MaxClockAhead = 2 * time.Minute
		},
	} {
		o := testOptions(metaFor(lifecycle.Policy{}))
		mod(&o)
		if _, err := backup.NewCutter(ctx, segs, prefix, o); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
}

func TestOptionsAreChecked(t *testing.T) {
	t.Parallel()
	for name, o := range map[string]backup.Options{
		"a margin not above the clock bound": {Margin: time.Minute, MaxClockAhead: time.Minute},
		"a negative margin":                  {Margin: -time.Hour},
		"files as large as a segment":        {MaxFileBytes: segment.MaxSize},
	} {
		if _, err := backup.NewCutter(ctx, newMemSegments(), prefix, o); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	if _, err := backup.NewCutter(ctx, newMemSegments(), "Bad/", backup.Options{}); !errors.Is(err, segment.ErrInvalidName) {
		t.Errorf("an invalid prefix: %v", err)
	}
}

// MaxFileBytes splits a class into several files within one cut, and the cut
// counts the class across them.
func TestAClassSplitsWithinACut(t *testing.T) {
	t.Parallel()
	segs := newMemSegments()
	o := testOptions(metaFor(lifecycle.Policy{}))
	o.TempDir, o.MaxFileBytes = t.TempDir(), 300
	c, err := backup.NewCutter(ctx, segs, prefix, o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	for i := range 6 {
		if err := c.Add(pods(t, uint64(2*i+1), 2, time.Duration(i)*time.Second), base); err != nil {
			t.Fatal(err)
		}
	}
	m, err := c.Cut(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Files) < 2 || len(m.Cuts) != 1 || m.Cuts[0].Records[backup.ClassL2] != 12 {
		t.Fatalf("%d files, cuts %+v", len(m.Files), m.Cuts)
	}
	r := openMem(t, m.Store)
	if res, err := backup.Restore(ctx, segs, m, r, backup.RestoreOptions{Store: m.Store, TempDir: t.TempDir()}); err != nil || res.Records != 12 {
		t.Fatalf("Restore: %v, %d records", err, res.Records)
	}
}

// Rule R's condition (a): a class's files of one cut go together. A cut whose
// first files are candidates and whose last is not keeps all of them; a cut that
// is all candidates goes whole.
func TestARetirementTakesWholeCuts(t *testing.T) {
	t.Parallel()
	s := newScript(t, lifecycle.Policy{}, backup.Hooks{})
	o := testOptions(s.meta)
	o.TempDir, o.MaxFileBytes = t.TempDir(), 300
	c, err := backup.NewCutter(ctx, s.segs, "split", o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	s.cut = c
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "split-pod")
	sub := store.EntitySubject(pod)
	at := func(m int) time.Time { return base.Add(time.Duration(m) * time.Minute) }
	for m := -40; m <= -30; m += 2 { // cut A: every file a candidate
		s.commit(at(m), observe(catalog.L2, sub, "k8sobjects", at(m), shadowTTL))
	}
	s.cutNow()
	for m := 0; m <= 20; m += 2 { // cut B: candidates up to 15 minutes only
		s.commit(at(m), observe(catalog.L2, sub, "k8sobjects", at(m), shadowTTL))
	}
	s.cutNow()
	before := c.Manifest()
	var inA, inB int
	for _, f := range before.Files {
		switch {
		case f.LastSeq <= before.Cuts[0].LastSeq:
			inA++
		default:
			inB++
		}
	}
	if inA < 2 || inB < 3 {
		t.Fatalf("%d files in cut A and %d in cut B: the class did not split", inA, inB)
	}
	s.retain(at(25))
	ret := s.retire()
	if ret.Files[backup.ClassL2] != inA {
		t.Fatalf("%d files retired, want cut A's %d and none of cut B's", ret.Files[backup.ClassL2], inA)
	}
	dst := openMem(t, s.meta)
	if _, err := backup.Restore(ctx, s.segs, ret.Manifest, dst, backup.RestoreOptions{Store: s.meta, TempDir: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
}

// The retired files are a prefix of the class's candidates in Seq order: a file
// that is not a candidate, here because a clock ahead of the ingest clock put its
// record's reach after h - M although its commit was long before, keeps every file
// after it, candidates or not.
func TestOnlyAPrefixOfCandidatesIsRetired(t *testing.T) {
	t.Parallel()
	h := base.Add(2 * time.Hour)
	a := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "ahead-pod")
	b := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "behind-pod")
	s := newScript(t, lifecycle.Policy{}, backup.Hooks{})
	ahead := observe(catalog.L2, store.EntitySubject(a), "k8sobjects", h.Add(-5*time.Minute), 9*time.Minute)
	s.commit(h.Add(-20*time.Minute), ahead) // not a candidate: its reach is after h - M
	s.cutNow()
	s.commit(h.Add(-15*time.Minute), observe(catalog.L2, store.EntitySubject(b), "k8sobjects", h.Add(-15*time.Minute), shadowTTL))
	s.cutNow() // a candidate
	s.commit(h.Add(-time.Minute), observe(catalog.L2, store.EntitySubject(b), "k8sobjects", h.Add(-time.Minute), shadowTTL))
	s.cutNow()
	s.retain(h)
	if ret := s.retire(); ret.Files[backup.ClassL2] != 0 {
		t.Fatalf("%d files retired behind a file that is not a candidate", ret.Files[backup.ClassL2])
	}
	for _, engine := range []string{engineMem, enginePebble} {
		if err := s.compareTo(engine, []identity.Fingerprint{a, b}, 10*time.Minute); err != nil {
			t.Fatalf("%s: %v", engine, err)
		}
	}
}
