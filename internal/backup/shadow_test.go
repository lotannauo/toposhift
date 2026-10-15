package backup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// T6: rule R's condition (b'). With a margin of 10 minutes and a TTL of 3, an
// Observe can be shadowed by a retired record of its reference only if its reach is
// more than 7 minutes after its EventTime. Each case has one reference in l2: a
// record r at h - M, which is a candidate in a cut of its own, and a kept record D
// of the same reference, ingested after it.
const shadowTTL = 3 * time.Minute

type shadowCase struct {
	name string
	// r is the record that may be retired, D the kept one; wallD is the clock D is
	// committed at.
	r, d  store.Record
	wallD time.Time
	// retired says whether rule R retires r's file.
	retired bool
	// differs says whether a restore that retires r regardless (condition (b')
	// dropped) answers differently from the live store.
	differs bool
}

func shadowCases(t testing.TB, pod identity.Fingerprint) []shadowCase {
	t.Helper()
	h := base.Add(2 * time.Hour)
	sub := store.EntitySubject(pod)
	const p = lifecycle.Producer("k8sobjects")
	eR := h.Add(-10 * time.Minute)
	obsR := observe(catalog.L2, sub, p, eR, shadowTTL)
	delR := deleteOf(catalog.L2, sub, p, eR)
	// through is an Observe of the reference at e with a reach span after it.
	through := func(e time.Time, span time.Duration) store.Record {
		d := observe(catalog.L2, sub, p, e, shadowTTL)
		d.Through = e.Add(span)
		return d
	}
	threshold := 7 * time.Minute
	// D just over the threshold: EventTime half a second before r, alive until half
	// a second after h.
	over := through(eR.Add(-500*time.Millisecond), threshold+time.Second)
	return []shadowCase{
		{name: "T6a, span just over M - TTL", r: obsR, d: over, wallD: reachOf(over), retired: false, differs: true},
		{
			name: "T6b, span exactly M - TTL", r: obsR, d: through(eR.Add(-time.Second), threshold),
			wallD: eR.Add(threshold), retired: true, differs: false,
		},
		{
			name: "T6c, span just under M - TTL", r: obsR, d: through(eR.Add(-time.Second), threshold-time.Second),
			wallD: eR.Add(threshold), retired: true, differs: false,
		},
		{
			name: "T6d, a backdated Observe", r: obsR, d: through(h.Add(-5*time.Hour), 5*time.Hour-shadowTTL+500*time.Millisecond),
			wallD: h.Add(-shadowTTL + time.Second), retired: false, differs: true,
		},
		{name: "T6f, the shadowing record is a Delete", r: delR, d: over, wallD: reachOf(over), retired: false, differs: true},
		{
			name: "T6g, a late Observe with no Through", r: obsR, d: observe(catalog.L2, sub, p, eR.Add(-30*time.Second), shadowTTL),
			wallD: eR.Add(time.Minute), retired: true, differs: false,
		},
	}
}

// runShadow scripts one case: r committed and cut at its own clock, D committed
// (and cut unless open), the live store retained to h, and the backup retired.
func runShadow(t *testing.T, c shadowCase, hooks backup.Hooks, open bool) (*script, backup.Retirement) {
	t.Helper()
	s := newScript(t, lifecycle.Policy{}, hooks)
	s.commit(reachOf(c.r), c.r)
	s.cutNow()
	s.commit(c.wallD, c.d)
	if !open {
		s.cutNow()
	}
	s.retain(base.Add(2 * time.Hour))
	return s, s.retire()
}

func TestShadowableObserves(t *testing.T) {
	t.Parallel()
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "shadow-pod")
	for _, c := range shadowCases(t, pod) {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			s, ret := runShadow(t, c, backup.Hooks{}, false)
			if got := ret.Files[backup.ClassL2] == 1; got != c.retired {
				t.Fatalf("r's file retired: %v, want %v", got, c.retired)
			}
			for _, engine := range []string{engineMem, enginePebble} {
				if err := s.compareTo(engine, []identity.Fingerprint{pod}, 10*time.Minute); err != nil {
					t.Fatalf("%s: %v", engine, err)
				}
			}
			// Without (b') r is retired, and the restore differs exactly when D can be
			// shadowed.
			var drop [backup.NumClasses]bool
			drop[backup.ClassL2] = true
			s, ret = runShadow(t, c, backup.Hooks{NoMinEvent: drop}, false)
			if ret.Files[backup.ClassL2] != 1 {
				t.Fatalf("without (b') r's file is not retired: %+v", ret.Files)
			}
			err := s.compareTo(engineMem, []identity.Fingerprint{pod}, 10*time.Minute)
			if c.differs != errors.Is(err, storetest.ErrMismatch) || (err != nil && !errors.Is(err, storetest.ErrMismatch)) {
				t.Fatalf("without (b') the restore's difference is %v, want a difference: %v", err, c.differs)
			}
		})
	}
}

// T6e: a run extended across the cut. Its older version is retired, and its newer
// version, at the same EventTime with a higher Seq, decides in both stores, so the
// answers are equal with condition (b') and without it.
func TestARunExtendedAcrossTheCut(t *testing.T) {
	t.Parallel()
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "run-pod")
	h := base.Add(2 * time.Hour)
	sub := store.EntitySubject(pod)
	start := h.Add(-13 * time.Minute)
	older := observe(catalog.L2, sub, "node-collector", start, shadowTTL)
	older.Through = h.Add(-10 * time.Minute)
	newer := older
	newer.Through = h.Add(-2 * time.Minute)
	c := shadowCase{r: older, d: newer, wallD: newer.Through}
	var drop [backup.NumClasses]bool
	drop[backup.ClassL2] = true
	for _, hooks := range []backup.Hooks{{}, {NoMinEvent: drop}} {
		s, ret := runShadow(t, c, hooks, false)
		if want := hooks.NoMinEvent[backup.ClassL2]; (ret.Files[backup.ClassL2] == 1) != want {
			t.Fatalf("hooks %+v: the older version retired: %v", hooks, !want)
		}
		for _, engine := range []string{engineMem, enginePebble} {
			if err := s.compareTo(engine, []identity.Fingerprint{pod}, 10*time.Minute); err != nil {
				t.Fatalf("hooks %+v, %s: %v", hooks, engine, err)
			}
		}
	}
}

// T6 with the shadowable Observe still in an open file when Retire is called. Retire cuts first, so it sees it and keeps r; a Retire that decided on
// what was cut alone would retire r, and the restore would differ.
func TestRetireSeesTheOpenFiles(t *testing.T) {
	t.Parallel()
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "open-pod")
	c := shadowCases(t, pod)[0] // T6a
	s, ret := runShadow(t, c, backup.Hooks{}, true)
	if ret.Files[backup.ClassL2] != 0 {
		t.Fatal("Retire retired r with the shadowable Observe still open")
	}
	if n := len(ret.Manifest.Files); n != 2 {
		t.Fatalf("Retire did not cut the open file: the manifest lists %d files", n)
	}
	if err := s.compareTo(engineMem, []identity.Fingerprint{pod}, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
	s, ret = runShadow(t, c, backup.Hooks{NoCutBeforeRetire: true}, true)
	if ret.Files[backup.ClassL2] != 1 {
		t.Fatal("deciding on the cut files alone did not retire r")
	}
	s.cutNow()
	if err := s.compareTo(engineMem, []identity.Fingerprint{pod}, 10*time.Minute); !errors.Is(err, storetest.ErrMismatch) {
		t.Fatalf("a retirement that did not see the open file restores without a difference: %v", err)
	}
}

// A record committed before the store's retention but handed to the cutter after
// it was logged could still shadow what Retire retires: T6a's shadowable Observe,
// written to the live store before Retain and added only later. Retire refuses to
// decide until the cutter holds every record through the retention's Seq; once
// it does, condition (b') sees the Observe and keeps the record it could shadow.
func TestRetireWaitsForTheRecordsBeforeTheRetention(t *testing.T) {
	t.Parallel()
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "lagging-pod")
	c := shadowCases(t, pod)[0] // T6a
	s := newScript(t, lifecycle.Policy{}, backup.Hooks{})
	s.commit(reachOf(c.r), c.r)
	s.cutNow()
	// D is committed to the live store, and not yet added to the cutter.
	d := c.d
	d.Seq = s.seq + 1
	if err := s.live.Write(ctx, []store.Record{d}); err != nil {
		t.Fatal(err)
	}
	s.retain(base.Add(2 * time.Hour)) // logged at the store's LastSeq, 2
	ret, err := s.cut.Retire(ctx)
	if !errors.Is(err, backup.ErrRetentionAhead) || ret.Files != [backup.NumClasses]int{} {
		t.Fatalf("Retire with the retention at seq 2 and the records added through 1: %+v, %v", ret.Files, err)
	}
	s.seq++
	s.written = append(s.written, d)
	if err := s.cut.Add([]store.Record{d}, c.wallD); err != nil {
		t.Fatal(err)
	}
	if ret := s.retire(); ret.Files[backup.ClassL2] != 0 {
		t.Fatal("r was retired although D, now added, could be shadowed by it")
	}
	if err := s.compareTo(engineMem, []identity.Fingerprint{pod}, 10*time.Minute); err != nil {
		t.Fatal(err)
	}
}
