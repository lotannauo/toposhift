package pebblelog_test

import (
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// storeBytesDirEnv, when set, makes TestStoredBytesAreUnchanged also write each
// case's store on the real disk under that directory, as <dir>/<case>/db, so the
// dbhash command can be checked against the digests the test computes.
const storeBytesDirEnv = "TOPOSHIFT_STOREBYTES_DIR"

// storeBytesVariant is one engine setting the stored bytes are frozen under. A
// setting that changes how the engine works but not what it stores (such as waiting
// for a retention's range deletions to be compacted) gets a field here, copied into
// the options by openStoreBytes, and a case whose expected digest is the one of the
// variant it settles.
type storeBytesVariant struct {
	name        string
	checkpoints pebblelog.CheckpointOptions
}

var storeBytesVariants = []storeBytesVariant{
	{"off", pebblelog.CheckpointOptions{}},
	{"k8", pebblelog.CheckpointOptions{On: true, KMin: 8, Alpha: 1}},
	{"lag 1ns", pebblelog.CheckpointOptions{On: true, KMin: 1, Lag: time.Nanosecond}},
	{"default", pebblelog.CheckpointOptions{On: true, KMin: 64, Alpha: 2, Lag: time.Nanosecond}},
}

// storeBytesStream is a workload the bytes are frozen for, and the variants that run
// it (all of them when variants is empty). minCheckpoints is how many checkpoints
// each variant with checkpoints on must have written, so that a case cannot go on
// passing after the stream stops reaching the code it is there for.
type storeBytesStream struct {
	name           string
	config         func() workload.Config
	variants       []string
	minCheckpoints int64
}

// conformanceStream is one of the conformance workloads: 0 is plain churn with a
// second producer, 5 has coalesced runs, lateness and outages, and 6 has fresh
// identities, pod heartbeats and a backlog.
func conformanceStream(i int) storeBytesStream {
	return storeBytesStream{
		name:           fmt.Sprintf("config %d", i),
		config:         func() workload.Config { return conformance.Configs()[i] },
		minCheckpoints: 1,
	}
}

// The default variant's threshold (64 records since a prefix's last checkpoint) is
// out of reach of the conformance streams on all but a few prefixes: it writes one
// checkpoint on streams 0 and 6 and none on 5, where it would store exactly what
// "off" does. So it runs on a stream with a hot prefix instead: the skewed one
// (config 4: a few pods take most of the churn, and the sequence numbers start
// near 2^63) for twice as long, which makes it write several.
var storeBytesStreams = []storeBytesStream{
	conformanceStream(0),
	func() storeBytesStream {
		s := conformanceStream(5)
		s.variants = []string{"off", "k8", "lag 1ns"}
		return s
	}(),
	conformanceStream(6),
	{
		name: "hot",
		config: func() workload.Config {
			c := conformance.Configs()[4]
			c.Duration *= 2
			return c
		},
		variants:       []string{"default"},
		minCheckpoints: 5,
	},
}

// storeBytesRetains are the fractions of the simulated period at which the horizon
// moves, and storeBytesReopen the one at which the engine is closed and opened
// again.
var (
	storeBytesRetains = []float64{0.3, 0.7}
	storeBytesReopen  = 0.5
)

const storeBytesBatch = 50

// storeBytesRun is what writing a stream into a store left, besides the bytes.
type storeBytesRun struct {
	digest pebblekv.Digest
	// retains is how many times the horizon moved, and invalidated how many
	// checkpoints a late record or a retention made the writer drop.
	retains, invalidated int
}

// storeBytesWant is what the data keyspace holds: the number of keys, by kind, and
// the SHA-256 of every key and value (see [pebblekv.DigestRange]).
type storeBytesWant struct {
	keys, records, checkpoints, baselines int64
	sha256                                string
}

// The values are frozen. They were computed with the layout-L code of main at
// 909947a, before it was promoted out of the bench module, so that the promoted
// code is judged against what this code stored and not against itself.
var storeBytesWants = map[string]storeBytesWant{
	"config 0 default": {2004, 1647, 1, 356, "5424789d5c2a075173f28f052ec9250b88561c975a7e3e0cc8ee09124b68e8ba"},
	"config 0 k8":      {2078, 1647, 75, 356, "802b6d64144b5632a3de50adc6ded826b2cbd3799777c65981da3c64c26756c2"},
	"config 0 lag 1ns": {2643, 1647, 640, 356, "3d56bb3f1087e9fe607819666a7101cfdb76b3ca1163e5d6bed7a34530a00b8c"},
	"config 0 off":     {2003, 1647, 0, 356, "1b68c4bb76c9cdc009f9fadacf37ca22211a3f2fe44ba625e616c6e4d08ae6b9"},
	"config 5 k8":      {2044, 1644, 26, 374, "cb906c7f2f518bcac739e80b319cd8740b12906bdb70aaa03b57d65a4f1d8a79"},
	"config 5 lag 1ns": {2393, 1644, 375, 374, "8bf96404f065c750147a64ae84337aebc0fe4118cf376dc9052bfaa68df2dba9"},
	"config 5 off":     {2018, 1644, 0, 374, "1dbee277b0496c2c2dbb01c8500dfdfed323cf245b056a72d193f59b2e661d1a"},
	"config 6 default": {4030, 3636, 1, 393, "6d802b510bf67756be520d477d9b550ff4d24c5adb259215b5137f30bd0c10ab"},
	"config 6 k8":      {4064, 3636, 35, 393, "b8ccf02b95fa315424c35cb221c90a7a6b4c97fac4897f099904d95b6e5d73c5"},
	"config 6 lag 1ns": {6170, 3636, 2141, 393, "39c239c2a7b078a72b405a358495f76effc678907c99fa24d689b49b615d5fa5"},
	"config 6 off":     {4029, 3636, 0, 393, "1128e93cf79fee66cbfe34c8cb91ae48012e97c5e66f3ea4659073b2e3ef6504"},
	"hot default":      {14739, 14320, 7, 412, "4734025957b286b3df7abc85cd533733cd08129cd6175666b304a457810ec731"},
}

// storeBytesEngine is what the driver needs of an engine: the spike's, or the
// promoted store behind a shim.
type storeBytesEngine interface {
	Write([]engine.Record) error
	Retain(time.Time) error
	Close() error
}

// storeBytesOpener opens an engine with the fixed options of a case.
type storeBytesOpener func(dir string, fs vfs.FS, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error)

// openStoreBytes opens an engine with the fixed options of a case. A nil fs is the
// real file system.
func openStoreBytes(dir string, fs vfs.FS, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
	return pebblelog.Open(dir, pebblelog.Options{
		Config:      pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs},
		Checkpoints: v.checkpoints,
		Recorder:    rec,
	})
}

// writeStoreBytes writes a stream into a new store under dir, in batches of exactly
// storeBytesBatch records, moves the horizon and reopens the engine at the fixed
// instants, closes and reopens it once more at the end, and returns the digest of
// the data keyspace and how many times it retained.
func writeStoreBytes(t *testing.T, cfg workload.Config, v storeBytesVariant, dir string, fs vfs.FS, opener storeBytesOpener) storeBytesRun {
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

	e, err := opener(dir, fs, v, rec)
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
		if e, err = opener(dir, fs, v, rec); err != nil {
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
		// The store refuses a batch with a record before its horizon, so the
		// stale records are left out.
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

	// The engine is closed: read what it left, with Pebble's own default
	// comparer, which is the bytewise one layout L uses.
	db, err := pebble.Open(dir, &pebble.Options{FS: fs, ReadOnly: true, Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("opening the store to read it: %v", err)
	}
	d, err := pebblekv.DigestRange(db, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return storeBytesRun{d, retains, int(rec.Counter("checkpoint.invalidated"))}
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// The data keys and values a layout-L store holds for a fixed stream do not change.
// Every comparison with earlier measurements depends on them, and the promotion of
// the layout out of this module must keep every one: it may change the meta keys
// (they are outside the digest) and nothing else.
//
// Each case writes a stream into an engine with fixed options on an in-memory file
// system, in batches of 50 records, moves the retention horizon twice and reopens
// the engine in the middle and at the end, then hashes the keys whose first byte is
// a layer. The hash is of what a reader sees, so it does not depend on how the
// tables are laid out, and a settled retention (which flushes and compacts) leaves
// it as it was. The digest depends on the Write granularity, so the promoted code
// must be given the same batches of 50 records.
func TestStoredBytesAreUnchanged(t *testing.T) {
	t.Parallel()
	diskDir := os.Getenv(storeBytesDirEnv)
	for _, stream := range storeBytesStreams {
		for _, v := range storeBytesVariants {
			if len(stream.variants) > 0 && !slices.Contains(stream.variants, v.name) {
				continue
			}
			name := stream.name + " " + v.name
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				cfg := stream.config()
				run := writeStoreBytes(t, cfg, v, "db", vfs.NewMem(), openStoreBytes)
				d := run.digest

				// A digest of an empty or trivial store would pass any
				// expectation that was computed from one.
				if d.Records <= 500 {
					t.Errorf("the store holds %d records, want more than 500: the case writes too little to freeze", d.Records)
				}
				if run.retains != len(storeBytesRetains) {
					t.Errorf("retained %d times, want %d", run.retains, len(storeBytesRetains))
				}
				if d.Baselines == 0 {
					t.Error("the store holds no baseline after a retention")
				}
				if !v.checkpoints.On {
					if d.Checkpoints != 0 || run.invalidated != 0 {
						t.Errorf("checkpoints are off and the store holds %d, with %d invalidated", d.Checkpoints, run.invalidated)
					}
				} else {
					if d.Checkpoints < stream.minCheckpoints {
						t.Errorf("the store holds %d checkpoints, want at least %d: the stream no longer reaches the code this case is for", d.Checkpoints, stream.minCheckpoints)
					}
					// The writer drops checkpoints a late record or a retention
					// makes untrue; a quieter stream would leave that code out.
					if run.invalidated == 0 {
						t.Error("no checkpoint was invalidated: the stream no longer reaches the code this case is for")
					}
				}
				t.Logf("%s: %d keys, %d records, %d checkpoints (%d invalidated), %d baselines", name, d.Keys, d.Records, d.Checkpoints, run.invalidated, d.Baselines)

				got := storeBytesWant{d.Keys, d.Records, d.Checkpoints, d.Baselines, hex.EncodeToString(d.SHA256[:])}
				if want, ok := storeBytesWants[name]; !ok || got != want {
					t.Errorf("the data keys and values a layout-L store holds for this stream changed (or this case has no frozen value yet).\n"+
						"Every comparison with earlier measurements depends on them. If the change is meant, it needs the owner's decision and new values; otherwise stop.\n"+
						"computed: %q: {%d, %d, %d, %d, %q},\nfrozen:   %+v (present: %v)",
						name, got.keys, got.records, got.checkpoints, got.baselines, got.sha256, want, ok)
				}

				if diskDir != "" {
					dir := filepath.Join(diskDir, strings.ReplaceAll(name, " ", "-"))
					if _, err := os.Lstat(dir); err == nil {
						t.Fatalf("%s already exists: %s names a directory for the stores of this test to be written to, and it must not hold the stores of an earlier run", dir, storeBytesDirEnv)
					} else if !os.IsNotExist(err) {
						t.Fatal(err)
					}
					if err := os.MkdirAll(dir, 0o755); err != nil {
						t.Fatal(err)
					}
					onDisk := writeStoreBytes(t, cfg, v, filepath.Join(dir, "db"), nil, openStoreBytes)
					if onDisk != run {
						t.Errorf("the store on disk is %+v, the one in memory %+v", onDisk, run)
					}
					t.Logf("%s: wrote %s", name, filepath.Join(dir, "db"))
				}
			})
		}
	}
}
