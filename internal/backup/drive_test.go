package backup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// drive is a stream fed to a live store and to a backup, as the sequencer and a
// cutter would see it.
type drive struct {
	cfg    storetest.Config
	policy lifecycle.Policy
	// batch is the records per committed batch, cutEvery the batches per cut.
	batch, cutEvery int
	// retainAt are the fractions of the stream after which the live store is
	// retained, to the ingest clock less lag.
	retainAt []float64
	lag      time.Duration
	// alignToCut retains to the clock of the last cut at or before the clock less
	// lag instead, so that a horizon is exactly where a cut ends.
	alignToCut bool
	hooks      backup.Hooks
	// plain also feeds a second cutter, under the prefix "plain", that never
	// retires.
	plain bool
	// segs is the segment store; nil means a new memSegments.
	segs segment.Store
	// mid, if not nil, is called after each Cut and each Retire of the backup with
	// the error it returned; it may change segs' faults.
	mid func(step string, err error)
	// beforeRetire, if not nil, is called just before each Retire, and started
	// with what the drive will leave, before the first batch.
	beforeRetire func()
	started      func(*driven)
}

// driven is what a drive left.
type driven struct {
	meta     backup.StoreMeta
	live     *memstore.Store
	segs     segment.Store
	written  []store.Record
	entities []identity.Fingerprint
	dropped  int
	// retired counts the files retired per class; refusals are the errors Retire
	// returned.
	retired  [backup.NumClasses]int
	refusals []error
	// lastRetired is, per class, the last file retired, and cutWalls the ingest
	// clock at each cut.
	lastRetired [backup.NumClasses]backup.File
	lastHorizon [backup.NumClasses]int64
	cutWalls    []time.Time
	// retirements are each Retire's horizon of each layer.
	retirements []backup.Retention
}

const prefix = "backup"

func (d drive) run(t testing.TB) *driven {
	t.Helper()
	if d.batch == 0 {
		d.batch = 20
	}
	if d.cutEvery == 0 {
		d.cutEvery = 25
	}
	if d.lag == 0 {
		d.lag = time.Hour
	}
	out := &driven{meta: metaFor(d.policy), segs: d.segs}
	if out.segs == nil {
		out.segs = newMemSegments()
	}
	out.live = openMem(t, out.meta)
	if d.started != nil {
		d.started(out)
	}
	opts := testOptions(out.meta)
	opts.TempDir = t.TempDir()
	newCutter := func(p string) *backup.Cutter {
		c, err := backup.NewCutter(ctx, out.segs, p, opts)
		if err != nil {
			t.Fatal(err)
		}
		backup.SetHooks(c, d.hooks)
		t.Cleanup(func() { _ = c.Close() })
		return c
	}
	cut := newCutter(prefix)
	var plain *backup.Cutter
	if d.plain {
		plain = newCutter("plain")
	}
	gen, err := storetest.NewGenerator(d.cfg)
	if err != nil {
		t.Fatal(err)
	}
	out.entities = gen.Entities()
	count, err := storetest.NewGenerator(d.cfg)
	if err != nil {
		t.Fatal(err)
	}
	total := len(count.All())

	next := d.cfg.FirstSeq
	var wall time.Time
	consumed, batches, retained := 0, 0, 0
	doCut := func() {
		_, err := cut.Cut(ctx)
		if d.mid != nil {
			d.mid("cut", err)
		} else if err != nil {
			t.Fatalf("Cut: %v", err)
		}
		if plain != nil {
			if _, err := plain.Cut(ctx); err != nil {
				t.Fatalf("Cut of the plain backup: %v", err)
			}
		}
		out.cutWalls = append(out.cutWalls, wall)
	}
	for {
		recs := gen.Batch(d.batch)
		if len(recs) == 0 {
			break
		}
		consumed += len(recs)
		var kept []store.Record
		for _, r := range recs {
			if h := out.live.LayerHorizon(r.Layer); !h.IsZero() && r.EventTime.Before(h.Time) {
				out.dropped++ // the sequencer reruns a refused group without it
				continue
			}
			r.Seq = next
			next++
			kept = append(kept, r)
			if reach := reachOf(r); reach.After(wall) {
				wall = reach
			}
		}
		if len(kept) > 0 {
			if err := out.live.Write(ctx, kept); err != nil {
				t.Fatalf("live Write: %v", err)
			}
			out.written = append(out.written, kept...)
			if err := cut.Add(kept, wall); err != nil {
				t.Fatalf("Add: %v", err)
			}
			if plain != nil {
				if err := plain.Add(kept, wall); err != nil {
					t.Fatalf("Add to the plain backup: %v", err)
				}
			}
			batches++
			if batches%d.cutEvery == 0 {
				doCut()
			}
		}
		if retained < len(d.retainAt) && float64(consumed) >= d.retainAt[retained]*float64(total) {
			retained++
			asked := wall.Add(-d.lag)
			if d.alignToCut {
				asked = alignedTo(t, out.cutWalls, asked)
			}
			if err := out.live.Retain(ctx, asked); err != nil {
				t.Fatal(err)
			}
			gen.SetHorizon(asked)
			h := horizons(out.live)
			if err := cut.Retained(asked, h, out.live.LastSeq()); err != nil {
				t.Fatal(err)
			}
			if plain != nil {
				if err := plain.Retained(asked, h, out.live.LastSeq()); err != nil {
					t.Fatal(err)
				}
			}
			out.retire(t, d, cut)
		}
	}
	doCut()
	if plain != nil {
		if _, err := plain.Cut(ctx); err != nil {
			t.Fatal(err)
		}
	}
	return out
}

// alignedTo is the clock of the last cut at or before at.
func alignedTo(t testing.TB, walls []time.Time, at time.Time) time.Time {
	t.Helper()
	var out time.Time
	for _, w := range walls {
		if !w.After(at) {
			out = w
		}
	}
	if out.IsZero() {
		t.Fatalf("no cut at or before %s", at)
	}
	return out
}

// retire cuts, so the files the retirement may take are known, and retires.
func (out *driven) retire(t testing.TB, d drive, cut *backup.Cutter) {
	t.Helper()
	before, err := cut.Cut(ctx)
	if d.mid != nil {
		d.mid("cut", err)
	} else if err != nil {
		t.Fatalf("Cut: %v", err)
	}
	if d.beforeRetire != nil {
		d.beforeRetire()
	}
	ret, err := cut.Retire(ctx)
	if d.mid != nil {
		d.mid("retire", err)
	}
	if err != nil {
		if !errors.Is(err, backup.ErrNotRetirable) && d.mid == nil {
			t.Fatalf("Retire: %v", err)
		}
		out.refusals = append(out.refusals, err)
	}
	if last, ok := lastOf(ret.Manifest.Retentions); ok {
		out.retirements = append(out.retirements, last)
	}
	for cl := range backup.Class(backup.NumClasses) {
		out.retired[cl] += ret.Files[cl]
		if ret.Files[cl] == 0 {
			continue
		}
		out.lastHorizon[cl] = ret.Manifest.Retired[cl].HorizonNS
		through := ret.Manifest.Retired[cl].ThroughSeq
		for _, f := range before.Files {
			if f.Class == cl.String() && f.LastSeq == through {
				out.lastRetired[cl] = f
			}
		}
	}
}

func lastOf(rs []backup.Retention) (backup.Retention, bool) {
	if len(rs) == 0 {
		return backup.Retention{}, false
	}
	return rs[len(rs)-1], true
}

// restoredOptions are the comparator's options for a drive: every second of twice
// the longest TTL and run age after each horizon.
func (out *driven) restoredOptions(span time.Duration) storetest.RestoredOptions {
	return storetest.RestoredOptions{Entities: out.entities, Written: out.written, Span: span}
}
