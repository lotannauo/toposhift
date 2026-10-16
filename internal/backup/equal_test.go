package backup_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// equalWorkload is one of the refreshing workloads the equal-answers test runs.
type equalWorkload struct {
	name   string
	cfg    storetest.Config
	policy lifecycle.Policy
	// lateBound is the longest a record arrives after its event time, and span the
	// seconds after each horizon compared one by one: twice the longest TTL and
	// run age.
	lateBound, span time.Duration
}

// equalWorkloads are storetest's workloads other than watch mode, made to refresh:
// two hours each, a heartbeat every minute, a TTL of three heartbeats, and runs,
// where a workload has them, at most 90 seconds old (half the TTL, as serve).
func equalWorkloads(t testing.TB) []equalWorkload {
	t.Helper()
	var out []equalWorkload
	for _, w := range storetest.Workloads() {
		c := w.Config
		if c.HeartbeatInterval == 0 {
			continue // watch mode asserts once: TestAssertOnceIsRefused
		}
		c.Duration, c.HeartbeatInterval, c.TTLFactor = equalDuration(), time.Minute, 3
		c.Refresh.On = true
		var runAge time.Duration
		if c.Runs {
			c.Refresh.RunMaxAge = 90 * time.Second
			runAge = c.Refresh.RunMaxAge
		}
		ttl := time.Duration(c.TTLFactor) * c.HeartbeatInterval
		out = append(out, equalWorkload{
			name: w.Name, cfg: c, policy: w.Policy,
			lateBound: c.LateMax, span: 2 * (max(ttl, c.ConfirmTTL) + runAge),
		})
	}
	if len(out) != 5 {
		t.Fatalf("%d refreshing workloads, want 5", len(out))
	}
	return out
}

// The equal-answers stream: two hours, retained at half and four fifths of the
// stream to the ingest clock less half an hour, with the comparison asking every
// twentieth record instant past the dense span after each horizon. The trimmed
// tier, which is what runs under the race detector, streams 80 minutes, retains to
// the clock less a quarter of an hour, and asks every hundredth instant past the
// span: still long enough for both retentions to retire l1 and l2 files.
func equalDuration() time.Duration {
	if storetest.Trimmed() {
		return 80 * time.Minute
	}
	return 2 * time.Hour
}

func equalLag() time.Duration {
	if storetest.Trimmed() {
		return 15 * time.Minute
	}
	return 30 * time.Minute
}

// sideSpan is the dense span of the comparisons that check something other than
// the margin's edge (a whole replay, a refused class, a fault, a real directory):
// the full span, or one minute in the trimmed tier.
func sideSpan(full time.Duration) time.Duration {
	if storetest.Trimmed() {
		return min(full, time.Minute)
	}
	return full
}

func equalSample() int {
	if storetest.Trimmed() {
		return 100
	}
	return 20
}

func hooksFor(c storetest.Config) backup.Hooks {
	if c.FirstSeq == 1 {
		return backup.Hooks{}
	}
	return backup.Hooks{StartSeq: c.FirstSeq}
}

// restores are the engines and batch sizes each workload is restored with: the
// reference with batches of 1, 7 and 10,000, and the pebblestore in each retention
// mode with one of them, to bound the time (a batch of 1 is one commit a record).
var restores = []struct {
	engine string
	batch  int
}{
	{engineMem, 1},
	{engineMem, 7},
	{engineMem, 10_000},
	{enginePebble, 7},
	{enginePebbleBack, 10_000},
}

// TestRestoredStoreAnswersAsTheLiveStore is T1: a backup retired by rule R, and
// restored into each engine, answers as the live store at every instant at or
// after each layer's horizon. T1b: the same stream backed up with retirement off
// restores the same answers.
func TestRestoredStoreAnswersAsTheLiveStore(t *testing.T) {
	t.Parallel()
	ws := equalWorkloads(t)
	// The trimmed tier restores the tiny workload into the reference store only;
	// the scripted tests restore into the pebblestore under it.
	trimmed := map[string]string{ws[0].name: engineMem} // workload: the engine it is restored into
	for _, w := range ws {
		rs := restores
		if storetest.Trimmed() {
			engine, ok := trimmed[w.name]
			if !ok {
				continue
			}
			rs = []struct {
				engine string
				batch  int
			}{{engine, 7}}
		}
		t.Run(w.name, func(t *testing.T) {
			t.Parallel()
			d := drive{cfg: w.cfg, policy: w.policy, retainAt: []float64{0.5, 0.8}, lag: equalLag(), hooks: hooksFor(w.cfg), plain: true}
			out := d.run(t)
			if len(out.refusals) != 0 {
				t.Fatalf("Retire refused: %v", out.refusals)
			}
			// The guards that the run could have found a difference come after the
			// comparisons, so that a comparison that finds one is what fails first.
			defer checkVacuity(t, w, out)
			opts := out.restoredOptions(w.span)
			opts.Sample = equalSample()
			for _, rs := range rs {
				t.Run(fmt.Sprintf("%s, batch %d", rs.engine, rs.batch), func(t *testing.T) {
					r, res := restoreInto(t, out.segs, prefix, rs.engine, out.meta, rs.batch)
					if res.Records >= int64(len(out.written)) {
						t.Fatalf("the restore wrote %d records, the live store holds %d: nothing was retired", res.Records, len(out.written))
					}
					compare(t, out.live, r, opts)
				})
			}
			t.Run("retirement off", func(t *testing.T) {
				r, res := restoreInto(t, out.segs, "plain", engineMem, out.meta, 10_000)
				if res.Records != int64(len(out.written)) {
					t.Fatalf("the whole backup restored %d records of %d", res.Records, len(out.written))
				}
				whole := opts
				whole.Span = sideSpan(w.span)
				compare(t, out.live, r, whole)
			})
		})
	}
}

// checkVacuity fails a run of T1 that could not have found what it looks for.
func checkVacuity(t *testing.T, w equalWorkload, out *driven) {
	t.Helper()
	margin := testOptions(out.meta).Margin
	t.Logf("%d records written (%d dropped before a horizon), files retired per class %v", len(out.written), out.dropped, out.retired)
	for _, cl := range []backup.Class{backup.ClassL1, backup.ClassL2} {
		if out.retired[cl] == 0 {
			t.Fatalf("no %s file was retired", cl)
		}
	}
	var cutSpan time.Duration
	for i := 1; i < len(out.cutWalls); i++ {
		cutSpan = max(cutSpan, out.cutWalls[i].Sub(out.cutWalls[i-1]))
	}
	f := out.lastRetired[backup.ClassL2]
	edge := time.Unix(0, out.lastHorizon[backup.ClassL2]).Add(-margin)
	reach := time.Unix(0, f.MaxReachNS)
	gap := edge.Sub(reach)
	t.Logf("the last retired l2 file reaches %s before the horizon less the margin (a cut spans up to %s, the late bound is %s)", gap, cutSpan, w.lateBound)
	if gap < 0 || gap > 2*cutSpan+w.lateBound {
		t.Fatalf("the last retired l2 file %s reaches %s before h - M: more than two cuts (%s) and the late bound (%s); the margin's edge is not exercised",
			f.Name, gap, cutSpan, w.lateBound)
	}
	if w.policy.BootKey == "" {
		return
	}
	h := out.live.LayerHorizon(catalog.L1).Time
	old := 0
	for _, fp := range out.entities {
		if fp.Type() != catalog.Host {
			continue
		}
		_, err := out.live.Alive(ctx, fp, h, store.Current(catalog.L1))
		var qe *store.QuarantineError
		if errors.As(err, &qe) && qe.Collision != nil && qe.Collision.ObservedAt.Before(h.Add(-margin)) {
			old++
		}
	}
	t.Logf("%d hosts quarantined by a collision older than the L1 horizon less the margin", old)
	if old == 0 {
		t.Fatal("no host has a collision older than the L1 cut")
	}
}

// TestAssertOnceIsRefused is T2: a class that holds an Observe with no TTL (watch
// mode), or one whose TTL is above the margin less the clock bound, is not
// retired, its reason is recorded, every Retire says so, and the restore, which
// is then the whole replay, answers as the live store.
func TestAssertOnceIsRefused(t *testing.T) {
	t.Parallel()
	var watch storetest.Workload
	for _, w := range storetest.Workloads() {
		if w.Config.HeartbeatInterval == 0 {
			watch = w
		}
	}
	long := storetest.Tiny()
	long.Refresh.On, long.TTLFactor = true, 10 // a TTL of 10 minutes, above 10 - 1
	tests := []struct {
		name   string
		cfg    storetest.Config
		reason string
	}{
		{"watch mode", watch.Config, "no TTL"},
		{"a TTL above the margin less the clock bound", long, "a TTL of 10m0s"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			c := tt.cfg
			c.Duration = equalDuration()
			d := drive{cfg: c, retainAt: []float64{0.5, 0.8}, lag: equalLag(), hooks: hooksFor(c)}
			out := d.run(t)
			if len(out.refusals) != 2 {
				t.Fatalf("%d of 2 Retires refused: %v", len(out.refusals), out.refusals)
			}
			for _, err := range out.refusals {
				for _, cl := range []backup.Class{backup.ClassL1, backup.ClassL2, backup.ClassL3} {
					if !errors.Is(err, backup.ErrNotRetirable) || !strings.Contains(err.Error(), "class "+cl.String()+":") {
						t.Fatalf("the refusal does not name class %s: %v", cl, err)
					}
				}
			}
			if out.retired != [backup.NumClasses]int{} {
				t.Fatalf("files were retired: %v", out.retired)
			}
			m, err := backup.Latest(ctx, out.segs, prefix)
			if err != nil {
				t.Fatal(err)
			}
			for _, cl := range []backup.Class{backup.ClassL1, backup.ClassL2, backup.ClassL3} {
				if !strings.Contains(m.NotRetirable[cl], tt.reason) {
					t.Fatalf("not_retirable of %s is %q, want it to say %q", cl, m.NotRetirable[cl], tt.reason)
				}
			}
			r, res := restoreInto(t, out.segs, prefix, engineMem, out.meta, 10_000)
			if res.Records != int64(len(out.written)) {
				t.Fatalf("the restore wrote %d of %d records", res.Records, len(out.written))
			}
			opts := out.restoredOptions(sideSpan(10 * time.Minute))
			opts.Sample = equalSample()
			compare(t, out.live, r, opts)
		})
	}
}

// TestACutAtTheHorizonIsSeen is T3, the negative control: a backup that retires l2
// and l3 at exactly the horizon (no margin, no refusal, no condition (b')) must
// make the comparison report the false gap: an entity alive at the horizon in the
// live store, answered otherwise by the restore within a TTL of it. If the
// comparison reports equal, it cannot see what T1 looks for.
func TestACutAtTheHorizonIsSeen(t *testing.T) {
	t.Parallel()
	w := equalWorkloads(t)[0] // tiny
	var at [backup.NumClasses]bool
	at[backup.ClassL2], at[backup.ClassL3] = true, true
	d := drive{
		cfg: w.cfg, policy: w.policy, retainAt: []float64{0.5, 0.8}, lag: equalLag(), alignToCut: true,
		hooks: backup.Hooks{ZeroMargin: at, NoRefusal: at, NoMinEvent: at},
	}
	out := d.run(t)
	r, _ := restoreInto(t, out.segs, prefix, engineMem, out.meta, 10_000)
	opts := out.restoredOptions(w.span)
	opts.Sample = equalSample()
	err := storetest.CompareRestored(out.live, r, opts)
	var mm *storetest.RestoredMismatch
	if !errors.As(err, &mm) {
		t.Fatalf("a cut at the horizon compares as %v, want a mismatch", err)
	}
	t.Logf("first difference: %v", mm)
	h := out.live.LayerHorizon(mm.Layer).Time
	ttl := time.Duration(w.cfg.TTLFactor) * w.cfg.HeartbeatInterval
	if mm.Read != "Alive or Neighbors" || (mm.Layer != catalog.L2 && mm.Layer != catalog.L3) ||
		mm.At.Before(h) || mm.At.After(h.Add(ttl)) {
		t.Fatalf("the first difference is %s in %s at %s after the horizon, want Alive or Neighbors in L2 or L3 within %s of it",
			mm.Read, mm.Layer, mm.At.Sub(h), ttl)
	}
	sc := store.Current(mm.Layer)
	alive, _ := out.live.Alive(ctx, mm.Entity, h, sc)
	fwd, _ := out.live.Neighbors(ctx, mm.Entity, store.Forward, h, sc)
	rev, _ := out.live.Neighbors(ctx, mm.Entity, store.Reverse, h, sc)
	if !alive && len(fwd)+len(rev) == 0 {
		t.Fatalf("%s is neither alive nor connected at the horizon in the live store", mm.Entity)
	}
}
