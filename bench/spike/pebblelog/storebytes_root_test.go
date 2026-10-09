package pebblelog_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/internal/store"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// rootStore lets the driver of the frozen store-bytes test write into the promoted
// store: it converts the spike's records to the store's, field by field, and calls
// the store with a background context.
type rootStore struct{ s *pebblestore.Store }

func (r rootStore) Write(recs []engine.Record) error {
	out := make([]store.Record, len(recs))
	for i, x := range recs {
		out[i] = store.Record{
			Layer: x.Layer,
			Subject: store.Subject{
				Kind: store.SubjectKind(x.Subject.Kind), A: x.Subject.A, B: x.Subject.B, Relation: x.Subject.Relation,
			},
			Producer: x.Producer, EventTime: x.EventTime, Seq: x.Seq, Kind: x.Kind, TTL: x.TTL, Through: x.Through,
			Payload: x.Payload,
		}
	}
	return r.s.Write(context.Background(), out)
}

func (r rootStore) Retain(h time.Time) error { return r.s.Retain(context.Background(), h) }
func (r rootStore) Close() error             { return r.s.Close() }

// countNames keeps the counts an engine reports, by name, next to the recorder the
// driver of the frozen test already gives it.
type countNames struct {
	mu     sync.Mutex
	counts map[string]int64
}

func (c *countNames) add(name string, n int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]int64{}
	}
	c.counts[name] += n
}

func (c *countNames) snapshot() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.counts)
}

// teeRecorder gives every count to the driver's recorder and to the names kept
// beside it. Samples are not compared: the engines time things differently.
type teeRecorder struct {
	engine.Recorder
	names *countNames
}

func (t teeRecorder) Count(name string, n int64) {
	t.Recorder.Count(name, n)
	t.names.add(name, n)
}

// rootCheckpoints is the root store's checkpoint policy for a variant of the
// bench: an explicit off, or the same numbers.
func rootCheckpoints(c pebblelog.CheckpointOptions) *pebblestore.CheckpointOptions {
	return &pebblestore.CheckpointOptions{On: c.On, KMin: c.KMin, Alpha: c.Alpha, Lag: c.Lag}
}

// openSpikeBytes opens the spike's engine, keeping the names it counts.
func openSpikeBytes(names *countNames) storeBytesOpener {
	return func(dir string, fs vfs.FS, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
		return openStoreBytes(dir, fs, v, teeRecorder{rec, names})
	}
}

// openRootStoreBytes opens the promoted store with the fixed options of a case,
// with or without the settling of a retention, and with the checkpoint policy of
// the case's variant. Its commits are not synced, like the spike's here.
func openRootStoreBytes(settle bool, names *countNames) storeBytesOpener {
	return func(dir string, fs vfs.FS, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
		s, err := pebblestore.Open(dir, pebblestore.Options{
			Config:      rootkv.Config{Tuning: rootkv.TinyTuning(), FS: fs, SettleRetention: settle},
			Recorder:    teeRecorder{rec, names},
			Checkpoints: rootCheckpoints(v.checkpoints),
		})
		if err != nil {
			return nil, err
		}
		return rootStore{s}, nil
	}
}

// rootOnlyCounts are the counts only the promoted store reports: the retention it
// does not settle in the spike's run, the boots it keeps prefixes whole for, and
// the cost it measures of checkpoints and of a Write's iterators. Every other
// count the spike reports is reported by the promoted store with the same value.
var rootOnlyCounts = []string{
	"retain.prefixes_kept_for_boots", "retain.flush_ns", "retain.settle_ns", "retain.settle_deadline_hits",
	"checkpoint.build_ns", "write.iterators",
}

// The promoted store writes the data keys and values the spike's layout L wrote
// for the same streams, byte for byte, checkpoints included: the twelve frozen
// digests are the ones it must reproduce, with and without the settling of its
// retentions (which changes no key). It also counts what the spike counted, the
// same number of times, for every count the spike reports: the checkpoints
// written, invalidated and looked up, the keys read to learn the writer's state,
// the prefixes a retention rewrites. The meta keys are outside the digest, and are
// the one thing the promotion was free to change.
func TestStoredBytesAreUnchangedInThePromotedStore(t *testing.T) {
	t.Parallel()
	for _, stream := range storeBytesStreams {
		for _, v := range storeBytesVariants {
			if len(stream.variants) > 0 && !slices.Contains(stream.variants, v.name) {
				continue
			}
			name := stream.name + " " + v.name
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				// The spike's counts are the oracle of the promoted store's.
				spikeNames := &countNames{}
				spike := writeStoreBytes(t, stream.config(), v, "db", vfs.NewMem(), openSpikeBytes(spikeNames))
				spikeCounts := spikeNames.snapshot()
				for _, settle := range []bool{false, true} {
					t.Run(fmt.Sprintf("settle %v", settle), func(t *testing.T) {
						t.Parallel()
						rootNames := &countNames{}
						run := writeStoreBytes(t, stream.config(), v, "db", vfs.NewMem(), openRootStoreBytes(settle, rootNames))
						d := run.digest
						if d.Records <= 500 || d.Baselines == 0 || run.retains != len(storeBytesRetains) {
							t.Errorf("the case writes too little to freeze: %d records, %d baselines, %d retentions", d.Records, d.Baselines, run.retains)
						}
						if v.checkpoints.On && (d.Checkpoints < stream.minCheckpoints || run.invalidated == 0) {
							t.Errorf("the store holds %d checkpoints (want at least %d) and invalidated %d: the stream no longer reaches the code this case is for",
								d.Checkpoints, stream.minCheckpoints, run.invalidated)
						}
						if !v.checkpoints.On && (d.Checkpoints != 0 || run.invalidated != 0) {
							t.Errorf("checkpoints are off and the store holds %d, with %d invalidated", d.Checkpoints, run.invalidated)
						}
						got := storeBytesWant{d.Keys, d.Records, d.Checkpoints, d.Baselines, hex.EncodeToString(d.SHA256[:])}
						t.Logf("%s settle %v: %d keys, %d records, %d checkpoints (%d invalidated), %d baselines, sha256 %s",
							name, settle, d.Keys, d.Records, d.Checkpoints, run.invalidated, d.Baselines, got.sha256)
						if want, ok := storeBytesWants[name]; !ok || got != want {
							t.Errorf("the promoted store holds other data keys and values than the spike's layout L did for this stream.\n"+
								"computed: %q: {%d, %d, %d, %d, %q},\nfrozen:   %+v (present: %v)",
								name, got.keys, got.records, got.checkpoints, got.baselines, got.sha256, want, ok)
						}
						if run != spike {
							t.Errorf("the promoted store's run is %+v, the spike's %+v", run, spike)
						}

						rootCounts := rootNames.snapshot()
						for _, n := range rootOnlyCounts {
							delete(rootCounts, n)
						}
						if !maps.Equal(rootCounts, spikeCounts) {
							for _, n := range slices.Sorted(maps.Keys(spikeCounts)) {
								if rootCounts[n] != spikeCounts[n] {
									t.Errorf("count %q: the promoted store reports %d, the spike %d", n, rootCounts[n], spikeCounts[n])
								}
							}
							for _, n := range slices.Sorted(maps.Keys(rootCounts)) {
								if _, ok := spikeCounts[n]; !ok {
									t.Errorf("count %q (%d) is reported by the promoted store and not by the spike", n, rootCounts[n])
								}
							}
						}
						if v.checkpoints.On && spikeCounts["checkpoint.written"] == 0 {
							t.Error("the spike wrote no checkpoint: the counts compare nothing")
						}
						t.Logf("counts equal: %v", spikeCounts)
					})
				}
			})
		}
	}
}
