package rockslog_test

import (
	"encoding/hex"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/rocksdb/rockskv"
	"github.com/lotannauo/toposhift/bench/rocksdb/rockslog"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// The cases, streams and frozen values below are copied verbatim from the bench
// module's pebblelog/storebytes_test.go, which defines what layout L stores. RL
// must store the same data keys and values, byte for byte: these digests are what
// "the same layout" means, and a mismatch is a defect of the port, never a reason to
// change a value.

type storeBytesVariant struct {
	name        string
	checkpoints pebblelog.CheckpointOptions
}

var storeBytesVariants = []storeBytesVariant{
	{"off", pebblelog.CheckpointOptions{}},
	{"k8", pebblelog.CheckpointOptions{On: true, KMin: 8, Alpha: 1}},
	{"lag 1ns", pebblelog.CheckpointOptions{On: true, KMin: 1, Lag: time.Nanosecond}},
	{"default", pebblelog.CheckpointOptions{On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond}},
	{"lag 5m", pebblelog.CheckpointOptions{On: true, KMin: 1, Lag: 5 * time.Minute}},
	{"default 5m", pebblelog.CheckpointOptions{On: true, KMin: 64, Alpha: 2, Lag: 5 * time.Minute}},
}

type storeBytesStream struct {
	name           string
	config         func() workload.Config
	variants       []string
	minCheckpoints int64
}

func conformanceStream(i int) storeBytesStream {
	return storeBytesStream{
		name:           fmt.Sprintf("config %d", i),
		config:         func() workload.Config { return conformance.Configs()[i] },
		minCheckpoints: 1,
	}
}

var storeBytesStreams = []storeBytesStream{
	func() storeBytesStream {
		s := conformanceStream(0)
		s.variants = []string{"off", "k8", "lag 1ns", "default", "lag 5m"}
		return s
	}(),
	func() storeBytesStream {
		s := conformanceStream(5)
		s.variants = []string{"off", "k8", "lag 1ns", "lag 5m"}
		return s
	}(),
	func() storeBytesStream {
		s := conformanceStream(6)
		s.variants = []string{"off", "k8", "lag 1ns", "default"}
		return s
	}(),
	{
		name: "hot",
		config: func() workload.Config {
			c := conformance.Configs()[4]
			c.Duration *= 2
			return c
		},
		variants:       []string{"default", "default 5m"},
		minCheckpoints: 5,
	},
}

var (
	storeBytesRetains = []float64{0.3, 0.7}
	storeBytesReopen  = 0.5
)

const storeBytesBatch = 50

type storeBytesRun struct {
	digest               pebblekv.Digest
	retains, invalidated int
}

type storeBytesWant struct {
	keys, records, checkpoints, baselines int64
	sha256                                string
}

var storeBytesWants = map[string]storeBytesWant{
	"config 0 default": {2004, 1647, 1, 356, "5424789d5c2a075173f28f052ec9250b88561c975a7e3e0cc8ee09124b68e8ba"},
	"config 0 k8":      {2078, 1647, 75, 356, "802b6d64144b5632a3de50adc6ded826b2cbd3799777c65981da3c64c26756c2"},
	"config 0 lag 1ns": {2643, 1647, 640, 356, "3d56bb3f1087e9fe607819666a7101cfdb76b3ca1163e5d6bed7a34530a00b8c"},
	"config 0 lag 5m":  {2169, 1647, 166, 356, "daa4e01aadd1bea202407ae8723ee9677840d13f91067734d6a2f3c87cbcea3b"},
	"config 0 off":     {2003, 1647, 0, 356, "1b68c4bb76c9cdc009f9fadacf37ca22211a3f2fe44ba625e616c6e4d08ae6b9"},
	"config 5 k8":      {2044, 1644, 26, 374, "cb906c7f2f518bcac739e80b319cd8740b12906bdb70aaa03b57d65a4f1d8a79"},
	"config 5 lag 1ns": {2393, 1644, 375, 374, "8bf96404f065c750147a64ae84337aebc0fe4118cf376dc9052bfaa68df2dba9"},
	"config 5 lag 5m":  {2109, 1644, 91, 374, "ebe3554849d88a4315123297bddf79e7350134964f9bf9d3474640faa21845e0"},
	"config 5 off":     {2018, 1644, 0, 374, "1dbee277b0496c2c2dbb01c8500dfdfed323cf245b056a72d193f59b2e661d1a"},
	"config 6 default": {4030, 3636, 1, 393, "6d802b510bf67756be520d477d9b550ff4d24c5adb259215b5137f30bd0c10ab"},
	"config 6 k8":      {4064, 3636, 35, 393, "b8ccf02b95fa315424c35cb221c90a7a6b4c97fac4897f099904d95b6e5d73c5"},
	"config 6 lag 1ns": {6170, 3636, 2141, 393, "39c239c2a7b078a72b405a358495f76effc678907c99fa24d689b49b615d5fa5"},
	"config 6 off":     {4029, 3636, 0, 393, "1128e93cf79fee66cbfe34c8cb91ae48012e97c5e66f3ea4659073b2e3ef6504"},
	"hot default 5m":   {14807, 14320, 75, 412, "d04bb62234733c2a619563c3275f01d2a04d0e01b07186bb05e0a72ba4ba4b33"},
	"hot default":      {14739, 14320, 7, 412, "4734025957b286b3df7abc85cd533733cd08129cd6175666b304a457810ec731"},
}

// storeBytesEngine is what the driver needs of an engine: RL's, or Pebble's.
type storeBytesEngine interface {
	Write([]engine.Record) error
	Retain(time.Time) error
	Close() error
}

// storeBytesSide is one engine the stream is written to: how to open it over dir
// with a case's options, and how to digest its data keyspace once it is closed.
type storeBytesSide struct {
	open   func(dir string, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error)
	digest func(t *testing.T, dir string) pebblekv.Digest
}

// rlSide is RL, on disk with the tiny tuning. Its data keyspace is copied, once the
// engine is closed, into an in-memory Pebble database, and digested there by the
// same function that digests layout L on Pebble.
func rlSide() storeBytesSide {
	return storeBytesSide{
		open: func(dir string, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
			c := v.checkpoints
			return rockslog.Open(dir, rockslog.Options{
				Config:      rockskv.Config{Tuning: pebblekv.TinyTuning()},
				Checkpoints: rockslog.CheckpointOptions{On: c.On, KMin: c.KMin, Alpha: c.Alpha, Lag: c.Lag, Replace: c.Replace},
				Recorder:    rec,
			})
		},
		digest: digestRL,
	}
}

// pebbleSide is layout L on Pebble, in memory with the tiny tuning: the reference
// built in the same test.
func pebbleSide() storeBytesSide {
	fs := vfs.NewMem()
	return storeBytesSide{
		open: func(dir string, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
			return pebblelog.Open(dir, pebblelog.Options{
				Config:      pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs},
				Checkpoints: v.checkpoints,
				Recorder:    rec,
			})
		},
		digest: func(t *testing.T, dir string) pebblekv.Digest {
			t.Helper()
			db, err := pebble.Open(dir, &pebble.Options{FS: fs, ReadOnly: true, Logger: quietLogger{}})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = db.Close() }()
			d, err := pebblekv.DigestRange(db, pebblekv.DataLo, pebblekv.DataHi)
			if err != nil {
				t.Fatal(err)
			}
			return d
		},
	}
}

// digestRL opens the RocksDB database under dir read-only, copies its data keyspace
// into an in-memory Pebble database, and digests it there with
// [pebblekv.DigestRange].
func digestRL(t *testing.T, dir string) pebblekv.Digest {
	t.Helper()
	kv, err := rockskv.Open(dir, rockskv.Config{Tuning: pebblekv.TinyTuning(), ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	mem, err := pebble.Open("copy", &pebble.Options{FS: vfs.NewMem(), Logger: quietLogger{}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mem.Close() }()
	it, err := kv.NewIter(&rockskv.IterOptions{LowerBound: pebblekv.DataLo, UpperBound: pebblekv.DataHi})
	if err != nil {
		t.Fatal(err)
	}
	b := mem.NewBatch()
	for ok := it.First(); ok; ok = it.Next() {
		if err := b.Set(it.Key(), it.Value(), nil); err != nil { // the batch copies both
			t.Fatal(err)
		}
	}
	if err := it.Close(); err != nil {
		t.Fatal(err)
	}
	if err := b.Commit(pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	d, err := pebblekv.DigestRange(mem, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// writeStoreBytes is the driver of pebblelog's test: the stream in batches of
// exactly storeBytesBatch records, the horizon moved and the engine reopened at the
// fixed instants, a last close and reopening, and the digest of the data keyspace.
func writeStoreBytes(t *testing.T, cfg workload.Config, v storeBytesVariant, dir string, side storeBytesSide) storeBytesRun {
	t.Helper()
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	at := func(f float64) time.Time {
		return g.Start().Add(time.Duration(f * float64(g.End().Sub(g.Start()))))
	}
	var horizons []time.Time
	for _, f := range storeBytesRetains {
		horizons = append(horizons, at(f))
	}
	reopenAt := at(storeBytesReopen)
	rec := &engine.MemRecorder{}
	reopened := false

	e, err := side.open(dir, v, rec)
	if err != nil {
		t.Fatal(err)
	}
	open := true
	t.Cleanup(func() {
		if open {
			_ = e.Close()
		}
	})
	reopen := func() {
		if err := e.Close(); err != nil {
			open = false
			t.Fatal(err)
		}
		if e, err = side.open(dir, v, rec); err != nil {
			open = false
			t.Fatal(err)
		}
	}

	var horizon time.Time
	next, retains := 0, 0
	for {
		batch := g.Batch(storeBytesBatch)
		if len(batch) == 0 {
			break
		}
		batch = slices.DeleteFunc(batch, func(r engine.Record) bool { return r.EventTime.Before(horizon) })
		if len(batch) == 0 {
			continue
		}
		if err := e.Write(batch); err != nil {
			t.Fatalf("Write: %v", err)
		}
		last := batch[len(batch)-1].EventTime
		for next < len(horizons) && !last.Before(horizons[next]) {
			horizon = horizons[next]
			next++
			g.SetHorizon(horizon)
			if err := e.Retain(horizon); err != nil {
				t.Fatalf("Retain(%s): %v", horizon.Format(time.RFC3339), err)
			}
			retains++
		}
		if !reopened && !last.Before(reopenAt) {
			reopened = true
			reopen()
		}
	}
	if !reopened {
		t.Fatalf("the stream ended before the reopening instant %s", reopenAt.Format(time.RFC3339))
	}
	reopen()
	if err := e.Close(); err != nil {
		open = false
		t.Fatal(err)
	}
	open = false
	return storeBytesRun{side.digest(t, dir), retains, int(rec.Counter("checkpoint.invalidated"))}
}

// RL stores, for each of the 15 frozen cases of layout L, exactly the data keys and
// values layout L stores on Pebble: the frozen digest, and the digest of layout L
// built on Pebble in the same test from the same stream.
func TestStoredBytesAreLayoutLs(t *testing.T) {
	t.Parallel()
	cases := 0
	for _, stream := range storeBytesStreams {
		for _, v := range storeBytesVariants {
			if len(stream.variants) > 0 && !slices.Contains(stream.variants, v.name) {
				continue
			}
			cases++
			name := stream.name + " " + v.name
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				cfg := stream.config()
				run := writeStoreBytes(t, cfg, v, filepath.Join(t.TempDir(), "db"), rlSide())
				d := run.digest
				if d.Records <= 500 {
					t.Errorf("the store holds %d records, want more than 500", d.Records)
				}
				if run.retains != len(storeBytesRetains) {
					t.Errorf("retained %d times, want %d", run.retains, len(storeBytesRetains))
				}
				if v.checkpoints.On && (d.Checkpoints < stream.minCheckpoints || run.invalidated == 0) {
					t.Errorf("%d checkpoints, %d invalidated: the stream no longer reaches the checkpoint code", d.Checkpoints, run.invalidated)
				}
				got := storeBytesWant{d.Keys, d.Records, d.Checkpoints, d.Baselines, hex.EncodeToString(d.SHA256[:])}
				if want, ok := storeBytesWants[name]; !ok || got != want {
					t.Errorf("RL stores other data keys and values than layout L's frozen ones for this stream: a defect of the port.\n"+
						"computed: %q: {%d, %d, %d, %d, %q},\nfrozen:   %+v (present: %v)",
						name, got.keys, got.records, got.checkpoints, got.baselines, got.sha256, want, ok)
				}
				ref := writeStoreBytes(t, cfg, v, "db", pebbleSide())
				if ref != run {
					t.Errorf("RL left %+v, layout L on Pebble %+v", run, ref)
				}
			})
		}
	}
	if cases != len(storeBytesWants) {
		t.Errorf("%d cases ran, and %d values are frozen", cases, len(storeBytesWants))
	}
}
