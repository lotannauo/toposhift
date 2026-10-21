package decay_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// variant is a way of opening pebblestore: the checkpoint options and the
// retention mode.
type variant struct {
	name string
	ck   pebblestore.CheckpointOptions
	mode pebblestore.RetentionMode
}

// variants keep the checkpoint gaps small, so the seed walks stop at
// checkpoints, and retain both ways: a Background pass may not have reached a
// prefix when it is read.
func variants() []variant {
	return []variant{
		{"a checkpoint per record, synchronous", pebblestore.CheckpointOptions{On: true, KMin: 1}, pebblestore.Synchronous},
		{"k8 with a lag, background", pebblestore.CheckpointOptions{On: true, KMin: 8, Alpha: 1, Lag: time.Minute}, pebblestore.Background},
		{"the default, synchronous", pebblestore.DefaultCheckpoints(), pebblestore.Synchronous},
	}
}

func openPebble(t tb, v variant) *pebblestore.Store {
	t.Helper()
	o := pebblestore.DefaultOptions()
	o.Tuning = pebblekv.TinyTuning()
	o.Sync, o.SettleRetention = false, false
	o.FS = vfs.NewMem()
	o.Retention = v.mode
	ck := v.ck
	o.Checkpoints = &ck
	st, err := pebblestore.Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

// The same worlds written to pebblestore, with checkpoints and a retention in
// the middle of the stream, and to a memstore that is never retained: at every
// window that starts at or after the horizon and token at or above its Seq, the
// evidence of the two is the same, and the bound is sound and tight against the
// raw sightings. Evidence comes from seeds, which keep every field of a record
// but its payload whatever a retention has reached, so a deciding record from
// before the horizon still gives it and no read is refused.
func TestEvidenceOnPebblestoreAfterARetention(t *testing.T) {
	worlds := 60
	if raceEnabled {
		worlds = 15
	}
	for _, v := range variants() {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()
			var total tally
			for seed := range worlds {
				d := randDrawer{rand.New(rand.NewPCG(uint64(seed), 33))}
				w := drawWorld(d, true)
				peb, mem := openPebble(t, v), openMem(t)
				retain := func(h time.Time) {
					if err := peb.Retain(ctx, h); err != nil {
						t.Fatal(err)
					}
					if v.mode == pebblestore.Background {
						if err := peb.Instrument().WaitRetained(ctx); err != nil {
							t.Fatal(err)
						}
					}
				}
				r := build(t, w, []store.Store{peb, mem}, retain)
				total.add(r.query(t, d, 24))
				if err := peb.Close(); err != nil {
					t.Fatal(err)
				}
				_ = mem.Close()
			}
			t.Logf("%d worlds, %d retained, %d queries: %d with evidence, %d of them decided by a record from before the horizon; %d with a positive bound",
				total.worlds, total.retained, total.queries, total.withEvidence, total.beforeHorizon, total.positive)
			if total.retained < total.worlds/2 || total.beforeHorizon < 20 || total.positive < total.queries/50 {
				t.Errorf("the generator is too thin: %+v", total)
			}
		})
	}
}
