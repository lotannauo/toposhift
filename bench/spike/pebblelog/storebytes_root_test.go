package pebblelog_test

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/internal/store"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// rootStore lets the driver of the frozen store-bytes test write into the promoted
// store: it converts the spike's records to the store's, field by field, and calls
// the store with a background context. With wait set it waits, after each
// retention, for the scan of the stored prefixes that follows it; and before it
// closes the store it checks that every checkpoint stored is true.
type rootStore struct {
	s    *pebblestore.Store
	wait bool
}

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

func (r rootStore) Retain(h time.Time) error {
	if err := r.s.Retain(context.Background(), h); err != nil {
		return err
	}
	if r.wait {
		return waitScanned(r.s)
	}
	return nil
}

func (r rootStore) Close() error {
	if err := r.s.Instrument().VerifyCheckpoints(); err != nil {
		return errors.Join(err, r.s.Close())
	}
	return r.s.Close()
}

// waitScanned waits for the scans of the stored prefixes the store has started.
func waitScanned(s *pebblestore.Store) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	return s.Instrument().WaitScanned(ctx)
}

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

// rootMode forces the promoted store's writer map one way for a run of the frozen
// streams. What the store keeps must not depend on it: the filter of the stored
// prefixes is only asked whether a prefix may hold keys, and the cache of states
// only saves reads.
type rootMode struct {
	name string
	// maybe makes the filter answer that every prefix may hold keys.
	maybe bool
	// states caps the cache at that many states; zero leaves the default budget.
	states int
	// wait makes the driver wait for the scan of the stored prefixes after each
	// opening and each retention, so that which prefixes are read before a filter is
	// installed, and so the counts, do not depend on how fast a scan is.
	wait bool
}

var rootModes = []rootMode{
	{name: "default budget", wait: true},
	{name: "filter says maybe", maybe: true, wait: true},
	{name: "64 states", states: 64},
}

// openRootStoreBytes opens the promoted store with the fixed options of a case,
// with or without the settling of a retention, with the checkpoint policy of the
// case's variant, and with its writer map forced as the mode says. Its commits are
// not synced, like the spike's here.
func openRootStoreBytes(settle bool, m rootMode, names *countNames) storeBytesOpener {
	return func(dir string, fs vfs.FS, v storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
		s, err := pebblestore.Open(dir, pebblestore.Options{
			Config:      rootkv.Config{Tuning: rootkv.TinyTuning(), FS: fs, SettleRetention: settle},
			Recorder:    teeRecorder{rec, names},
			Checkpoints: rootCheckpoints(v.checkpoints),
		})
		if err != nil {
			return nil, err
		}
		if m.maybe {
			s.Instrument().AssumeEveryPrefixMayExist()
		}
		if m.states > 0 {
			s.Instrument().CapCachedStates(m.states)
		}
		if m.wait {
			if err := waitScanned(s); err != nil {
				return nil, errors.Join(err, s.Close())
			}
		}
		return rootStore{s: s, wait: m.wait}, nil
	}
}

// rootOnlyCounts are the counts only the promoted store reports: the retention it
// does not settle in the spike's run, the cost it measures of checkpoints and of a
// Write's iterators, the reads it makes before the first scan of the stored
// prefixes has installed a filter, the chunks of a retention and the flushes that
// bound its unflushed range deletions (the samples among them are listed so that
// the list names every counter only the store has), and its writer map (every count named with
// rootOnlyPrefix). Every other count the spike reports is reported by the promoted
// store with the same value, but for mayDifferCounts.
var rootOnlyCounts = []string{
	"retain.flush_ns", "retain.settle_ns", "retain.settle_deadline_hits",
	"checkpoint.build_ns", "write.iterators", "checkpoint.loads_cold",
	"retain.chunk_prefixes", "retain.chunk_open_ns", "retain.unflushed_range_deletes", "retain.flushes",
	"retain.flush_errors", "retain.flush_deferred",
}

const rootOnlyPrefix = "writer_map."

// mayDifferCounts are the reads of a prefix's state: the spike keeps the state of
// every prefix it has met, and a retention gives it the state of every prefix it
// rewrites, so the promoted store, which keeps those it touched most recently, puts
// a retention's states only in those, and knows the others by a filter, reads
// otherwise: fewer where the filter saves a read, and some the spike never makes.
var mayDifferCounts = []string{"checkpoint.loads", "checkpoint.load_keys"}

// recordsAndBaselines is the SHA-256 of every record and baseline of the store
// under dir on fs, key and value, in key order: all of the data keyspace but the
// checkpoints.
func recordsAndBaselines(t *testing.T, fs vfs.FS, dir string) string {
	t.Helper()
	db, err := pebble.Open(dir, &pebble.Options{FS: fs, ReadOnly: true, Logger: quietLogger{}})
	if err != nil {
		t.Fatalf("opening the store to read it: %v", err)
	}
	defer func() { _ = db.Close() }()
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: pebblekv.DataLo, UpperBound: pebblekv.DataHi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	h := sha256.New()
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if k[len(k)-1] == 1 {
			continue // a checkpoint: the last byte of a key is its kind
		}
		for _, b := range [][]byte{k, it.Value()} {
			_, _ = h.Write(binary.AppendUvarint(nil, uint64(len(b))))
			_, _ = h.Write(b)
		}
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// The promoted store writes the data keys and values the spike's layout L wrote
// for the same streams, byte for byte, checkpoints included: the twelve frozen
// digests are the ones it must reproduce, with and without the settling of its
// retentions (which changes no key). It also counts what the spike counted, the
// same number of times, for every count the spike reports: the checkpoints
// written, invalidated and looked up, the keys read to learn the writer's state,
// the prefixes a retention rewrites. The meta keys are outside the digest, and are
// the one thing the promotion was free to change.
//
// Each run is made three ways: with the writer map's default budget, where nothing
// is evicted; with its filter made to answer that every prefix may hold keys, which
// reads more; and with a cache of 64 states, which evicts and reads states again.
// The first two must store the frozen bytes. With 64 states only the variant
// without checkpoints must: a state read again after it was evicted counts what was
// written since the newest checkpoint left, where the state kept in memory counts
// since the last it wrote (which a late record may have deleted), and under a lag
// it counts the record at the checkpoint's own instant, so the policy may
// legitimately place checkpoints elsewhere. There the records and baselines are the
// frozen ones, and every checkpoint stored is true, which the driver checks, in
// every run, each time it closes the store.
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
				spikeFS := vfs.NewMem()
				spike := writeStoreBytes(t, stream.config(), v, "db", spikeFS, openSpikeBytes(spikeNames))
				spikeCounts := spikeNames.snapshot()
				spikeRest := recordsAndBaselines(t, spikeFS, "db")
				for _, settle := range []bool{false, true} {
					for _, m := range rootModes {
						t.Run(fmt.Sprintf("settle %v/%s", settle, m.name), func(t *testing.T) {
							t.Parallel()
							rootNames := &countNames{}
							fs := vfs.NewMem()
							run := writeStoreBytes(t, stream.config(), v, "db", fs, openRootStoreBytes(settle, m, rootNames))
							d := run.digest
							if d.Records <= 500 || d.Baselines == 0 || run.retains != len(storeBytesRetains) {
								t.Errorf("the case writes too little to freeze: %d records, %d baselines, %d retentions", d.Records, d.Baselines, run.retains)
							}
							exact := m.states == 0 || !v.checkpoints.On
							if exact && v.checkpoints.On && (d.Checkpoints < stream.minCheckpoints || run.invalidated == 0) {
								t.Errorf("the store holds %d checkpoints (want at least %d) and invalidated %d: the stream no longer reaches the code this case is for",
									d.Checkpoints, stream.minCheckpoints, run.invalidated)
							}
							if !v.checkpoints.On && (d.Checkpoints != 0 || run.invalidated != 0) {
								t.Errorf("checkpoints are off and the store holds %d, with %d invalidated", d.Checkpoints, run.invalidated)
							}
							got := storeBytesWant{d.Keys, d.Records, d.Checkpoints, d.Baselines, hex.EncodeToString(d.SHA256[:])}
							t.Logf("%s settle %v %s: %d keys, %d records, %d checkpoints (%d invalidated), %d baselines, sha256 %s",
								name, settle, m.name, d.Keys, d.Records, d.Checkpoints, run.invalidated, d.Baselines, got.sha256)
							want, ok := storeBytesWants[name]
							if exact {
								if !ok || got != want {
									t.Errorf("the promoted store holds other data keys and values than the spike's layout L did for this stream.\n"+
										"computed: %q: {%d, %d, %d, %d, %q},\nfrozen:   %+v (present: %v)",
										name, got.keys, got.records, got.checkpoints, got.baselines, got.sha256, want, ok)
								}
								if run != spike {
									t.Errorf("the promoted store's run is %+v, the spike's %+v", run, spike)
								}
							} else {
								if !ok || got.records != want.records || got.baselines != want.baselines {
									t.Errorf("the promoted store holds %d records and %d baselines; the frozen case %+v (present: %v)", got.records, got.baselines, want, ok)
								}
								if rest := recordsAndBaselines(t, fs, "db"); rest != spikeRest {
									t.Errorf("the records and baselines differ from the spike's: sha256 %s, the spike's %s", rest, spikeRest)
								}
							}

							rootCounts := rootNames.snapshot()
							evictions := rootCounts[rootOnlyPrefix+"lru_evictions"]
							falsePositives := rootCounts[rootOnlyPrefix+"false_positive_reads"]
							for n := range rootCounts {
								if slices.Contains(rootOnlyCounts, n) || strings.HasPrefix(n, rootOnlyPrefix) {
									delete(rootCounts, n)
								}
							}
							spikeC := maps.Clone(spikeCounts)
							switch {
							case m.states == 0:
								if evictions != 0 {
									t.Errorf("the default budget evicted %d states", evictions)
								}
								for _, n := range mayDifferCounts {
									t.Logf("count %q: the promoted store reports %d, the spike %d", n, rootCounts[n], spikeC[n])
									delete(rootCounts, n)
									delete(spikeC, n)
								}
								if !maps.Equal(rootCounts, spikeC) {
									for _, n := range slices.Sorted(maps.Keys(spikeC)) {
										if rootCounts[n] != spikeC[n] {
											t.Errorf("count %q: the promoted store reports %d, the spike %d", n, rootCounts[n], spikeC[n])
										}
									}
									for _, n := range slices.Sorted(maps.Keys(rootCounts)) {
										if _, ok := spikeC[n]; !ok {
											t.Errorf("count %q (%d) is reported by the promoted store and not by the spike", n, rootCounts[n])
										}
									}
								}
								if v.checkpoints.On && spikeC["checkpoint.written"] == 0 {
									t.Error("the spike wrote no checkpoint: the counts compare nothing")
								}
								t.Logf("counts equal: %v", spikeC)
							case v.checkpoints.On && evictions == 0:
								t.Errorf("a cache of %d states evicted none: the case does not read states again", m.states)
							}
							if m.maybe && v.checkpoints.On && falsePositives == 0 {
								t.Error("the filter made to answer maybe read no prefix it would have known to be empty: the case forces nothing")
							}
						})
					}
				}
			})
		}
	}
}
