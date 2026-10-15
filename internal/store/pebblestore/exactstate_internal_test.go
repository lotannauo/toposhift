package pebblestore

import (
	"bytes"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// A remembered state is exact when what the writer holds of the prefix is what a
// read of the whole prefix finds: the newest record, the size and instant of the
// newest checkpoint, and the records after that checkpoint, which the writer's own
// count of what it wrote since the last one it placed is not once a checkpoint is
// placed behind the newest record (a lag). These tests are about the claim, and
// about the walk that builds a checkpoint counting what a read would find.

// exactStatesMismatch is the first way a state flagged exact is not what a whole
// read of its prefix finds, and how many exact states there are and how many of
// them have a count the writer's own does not equal (the lagged ones). The caller
// holds e.mu.
func exactStatesMismatch(e *Store) (exact, lagged int, err error) {
	probe := wholeReader(e)
	for k, st := range e.states.all() {
		if !st.exact {
			continue
		}
		if !e.anyCkpt {
			return exact, lagged, fmt.Errorf("the prefix %x is exact in a database that has never had a checkpoint, where nothing is read", k)
		}
		a, err := probe.state([]byte(k))
		if err != nil {
			return exact, lagged, err
		}
		exact++
		if st.since != st.readSince {
			lagged++
		}
		newest := func(c []int64) int64 {
			if len(c) == 0 {
				return -1
			}
			return c[len(c)-1]
		}
		if st.latest != a.latest || st.lastBytes != a.lastBytes || newest(st.ckpts) != newest(a.ckpts) ||
			st.readSince != a.since || st.readSinceBytes != a.sinceBytes {
			return exact, lagged, fmt.Errorf("the state of the prefix %x is exact and is %+v, where a whole read finds %+v", k, *st, *a)
		}
	}
	return exact, lagged, nil
}

func requireExactStates(t *testing.T, e *Store) (exact, lagged int) {
	t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	exact, lagged, err := exactStatesMismatch(e)
	if err != nil {
		t.Fatal(err)
	}
	return exact, lagged
}

// stateOf is the state the writer remembers of the prefix, a copy; nil if it holds none.
func stateOf(e *Store, prefix []byte) *prefixState {
	e.mu.Lock()
	defer e.mu.Unlock()
	if st := e.states.peek(string(prefix)); st != nil {
		c := *st
		return &c
	}
	return nil
}

// podForwardPrefix is the prefix of the pod's forward edges in L0.
func podForwardPrefix(t *testing.T, e *Store) []byte {
	t.Helper()
	p, ok := e.prefixOf(catalog.L0, podFP, byte(store.Forward))
	if !ok {
		t.Fatal("the pod's type is not in the catalog")
	}
	return p
}

// podEdge is an edge of the pod to the node in L0 at ns nanoseconds after the base.
func podEdge(ns int, seq uint64) store.Record {
	return edgeAt(catalog.L0, podFP, nodeFP, asyncAt(ns), seq)
}

// writeAt writes an edge of the pod to the node at ns nanoseconds after the base, with
// the next sequence number.
func writeAt(t *testing.T, s *Store, ns ...int) {
	t.Helper()
	for _, n := range ns {
		if err := s.Write(bg, []store.Record{podEdge(n, s.LastSeq()+1)}); err != nil {
			t.Fatal(err)
		}
	}
}

// withACheckpoint places a checkpoint on a prefix of another pod, so that the database
// has one. A state is read, or known to be empty, only in a database that has had a
// checkpoint; before it a write starts from an empty state by assumption, which is
// not what a read finds.
func withACheckpoint(t *testing.T, s *Store) {
	t.Helper()
	other := fingerprintOf(catalog.K8sPod, 0x70)
	if err := s.Write(bg, []store.Record{edgeAt(catalog.L3, other, nodeFP, asyncAt(1), s.LastSeq()+1)}); err != nil {
		t.Fatal(err)
	}
	if err := s.Instrument().CheckpointEdges(catalog.L3, other, store.Forward, asyncAt(2)); err != nil {
		t.Fatal(err)
	}
}

func openCounting(t *testing.T, fs vfs.FS, ck CheckpointOptions) (*Store, *countRecorder) {
	t.Helper()
	rec := &countRecorder{}
	o := chunkOptions(fs, &ck)
	o.Recorder = rec
	return mustOpen(t, o), rec
}

// After every Write, chunk and retention of the oracle's worlds, every exact state is
// what a whole read finds. The worlds run under a policy that places a checkpoint at
// every record, one with a lag, and none, in both modes, with the default budget and
// with 64 states and with 8, and their late records invalidate checkpoints. Some of
// the states must be exact with a count the writer's own does not equal: that is a
// checkpoint placed behind the newest record, whose count the walk that built it
// made.
func TestAnExactStateIsWhatAReadFinds(t *testing.T) {
	t.Parallel()
	seeds := 12
	if raceEnabled {
		seeds = 2
	}
	policies := []CheckpointOptions{{On: true, KMin: 1}, {On: true, KMin: 2, Lag: 3}, {}}
	caps := []int{0, 64, 8}
	type tally struct{ exact, lagged int }
	results := make(chan tally, seeds*len(policies)*len(caps)*2)
	t.Run("worlds", func(t *testing.T) {
		for seed := range seeds {
			for pi, ck := range policies {
				for _, capN := range caps {
					for _, mode := range []RetentionMode{Synchronous, Background} {
						t.Run(fmt.Sprintf("seed %d/policy %d/cap %d/mode %v", seed, pi, capN, mode), func(t *testing.T) {
							t.Parallel()
							var r tally
							w := newAsyncWorld(int64(seed))
							r.exact, r.lagged = runExactWorld(t, w, ck, mode, capN)
							results <- r
						})
					}
				}
			}
		}
	})
	close(results)
	var exact, lagged int
	for r := range results {
		exact += r.exact
		lagged += r.lagged
	}
	if exact == 0 || lagged == 0 {
		t.Fatalf("%d exact states were checked, of which %d have a count the writer's own is not: the worlds do not reach a checkpoint placed with a lag", exact, lagged)
	}
	t.Logf("%d exact states checked, %d of them with a lagged count", exact, lagged)
}

// runExactWorld writes the world to one store, one batch at a time, checking the exact
// states after each, and retains it at the world's horizon, writing the batches after
// it between the chunks of a background pass. It returns how many exact states it saw
// and how many of those were lagged.
func runExactWorld(t *testing.T, w *asyncWorld, ck CheckpointOptions, mode RetentionMode, capN int) (exact, lagged int) {
	t.Helper()
	check := func(e *Store) {
		e.mu.Lock()
		x, l, err := exactStatesMismatch(e)
		e.mu.Unlock()
		if err != nil {
			t.Error(err)
		}
		exact += x
		lagged += l
	}
	o := chunkOptions(vfs.NewMem(), &ck)
	o.Retention, o.retainBatchBytes, o.retainChunkTime = mode, 1, time.Hour
	var s *Store
	next := 0
	write := func(batch []store.Record) {
		if err := s.Write(bg, cloneAll(batch)); err != nil {
			t.Error(err)
		}
		check(s)
	}
	if mode == Background {
		o.beforeRetainChunk = func() {
			check(s)
			if next < len(w.later) {
				write(w.later[next])
				next++
			}
		}
	}
	s = mustOpen(t, o)
	if capN > 0 {
		s.Instrument().CapCachedStates(capN)
	}
	for _, batch := range w.pre {
		write(batch)
	}
	if err := s.Retain(bg, w.h); err != nil {
		t.Fatal(err)
	}
	if mode == Background {
		waitRetained(t, s)
	} else {
		check(s)
	}
	for ; next < len(w.later); next++ {
		write(w.later[next])
	}
	check(s)
	return exact, lagged
}

// A checkpoint placed behind the newest record leaves the state exact with the count
// of the records the walk that built it found after it; a later record at or after it
// adds to both counts, and one before it clears the flag.
func TestACheckpointPlacedWithALagLeavesTheStateExactWithTheCountOfTheRecordsAfterIt(t *testing.T) {
	t.Parallel()
	s, rec := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 4, Lag: 10})
	withACheckpoint(t, s)
	prefix := podForwardPrefix(t, s)
	// The fourth record makes the prefix due. The checkpoint is at 10+1-10 = 1: the
	// records at 5, 9 and 10 are after it, the one at 0 is before it.
	for _, ns := range []int{0, 5, 9, 10} {
		writeAt(t, s, ns)
		requireExactStates(t, s)
	}
	st := stateOf(s, prefix)
	if st == nil || !st.exact || len(st.ckpts) != 1 || st.ckpts[0] != asyncAt(1).UnixNano() {
		t.Fatalf("after the checkpoint the state is %+v; want exact with one checkpoint at 1", st)
	}
	if st.since != 0 || st.sinceBytes != 0 || st.readSince != 3 || st.readSinceBytes == 0 || st.lastBytes == 0 {
		t.Fatalf("the writer's count is %d and a read finds %d records (%d bytes) after the checkpoint; want 0 and 3", st.since, st.readSince, st.readSinceBytes)
	}
	if got := rec.counts()["checkpoint.shadow_records"]; got != 6 { // two prefixes, three records each
		t.Errorf("checkpoint.shadow_records = %d, want 6", got)
	}

	// A record after the checkpoint, and one exactly at it, add to both counts.
	for _, ns := range []int{12, 1} {
		writeAt(t, s, ns)
		requireExactStates(t, s)
	}
	st = stateOf(s, prefix)
	if !st.exact || st.since != 2 || st.readSince != 5 {
		t.Fatalf("after two more records the state is %+v; want exact, the writer's count 2 and a read's 5", st)
	}
	// A record before it deletes it: the state is no longer exact.
	writeAt(t, s, 0)
	if st = stateOf(s, prefix); st.exact || len(st.ckpts) != 0 {
		t.Fatalf("after a record before the checkpoint the state is %+v; want it not exact and without the checkpoint", st)
	}
	requireExactStates(t, s)
}

// Without a lag a checkpoint is after every record: a read counts none.
func TestACheckpointPlacedWithoutALagLeavesTheStateExactWithNoRecordsAfterIt(t *testing.T) {
	t.Parallel()
	s, rec := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 2})
	withACheckpoint(t, s)
	prefix := podForwardPrefix(t, s)
	for _, ns := range []int{3, 8, 20} {
		writeAt(t, s, ns)
		requireExactStates(t, s)
	}
	st := stateOf(s, prefix)
	if !st.exact || len(st.ckpts) == 0 || st.readSince != st.since {
		t.Fatalf("the state is %+v; want exact, with the writer's count a read's", st)
	}
	if rec.counts()["checkpoint.shadow_records"] != 0 {
		t.Errorf("a store without a lag counted shadow records: %v", rec.counts())
	}
}

// The count is given up beyond 1,024 records, the state is then not exact, and the
// checkpoint is the same either way.
func TestTheShadowCountIsGivenUpBeyondTheCap(t *testing.T) {
	t.Parallel()
	const records = 1100
	for _, tc := range []struct {
		lag    int
		capped bool
	}{{shadowCap, false}, {shadowCap + 1, true}} {
		t.Run(fmt.Sprintf("lag %d", tc.lag), func(t *testing.T) {
			t.Parallel()
			s, rec := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: records, Lag: time.Duration(tc.lag)})
			withACheckpoint(t, s)
			prefix := podForwardPrefix(t, s)
			batch := make([]store.Record, records)
			for i := range batch {
				batch[i] = podEdge(10+i, s.LastSeq()+1+uint64(i))
			}
			// Written together, so the prefix is due once, at records+1-lag.
			if err := s.Write(bg, batch); err != nil {
				t.Fatal(err)
			}
			st := stateOf(s, prefix)
			if st == nil || len(st.ckpts) != 1 {
				t.Fatalf("the state is %+v; want one checkpoint", st)
			}
			if st.exact == tc.capped {
				t.Errorf("exact = %v with %d records after the checkpoint, and the cap %d", st.exact, tc.lag, shadowCap)
			}
			if !tc.capped && st.readSince != tc.lag {
				t.Errorf("a read counts %d records after the checkpoint, want %d", st.readSince, tc.lag)
			}
			counts := rec.counts()
			wantCapped := int64(0)
			if tc.capped {
				wantCapped = 2 // both copies of the edge
			}
			if counts["checkpoint.shadow_capped"] != wantCapped {
				t.Errorf("checkpoint.shadow_capped = %d, want %d", counts["checkpoint.shadow_capped"], wantCapped)
			}
			requireExactStates(t, s)
		})
	}
}

// A checkpoint that exists already at the instant the policy would place one clears
// the flag: the writer counts nothing since, where a read counts the records after
// it.
func TestAStateThatFindsItsCheckpointAlreadyThereIsNotExact(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	ck := CheckpointOptions{On: true, KMin: 3, Lag: 10}
	s, _ := openCounting(t, fs, ck)
	for i, ns := range []int{100, 105, 110} {
		if err := s.Write(bg, []store.Record{podEdge(ns, uint64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopened, the state is read: the checkpoint at 101 is the newest, and two
	// records are after it. One more, at an instant already counted, makes the
	// prefix due at the same instant.
	s, rec := openCounting(t, fs, ck)
	prefix := podForwardPrefix(t, s)
	if err := s.Write(bg, []store.Record{podEdge(108, 4)}); err != nil {
		t.Fatal(err)
	}
	st := stateOf(s, prefix)
	if st.exact || st.since != 0 {
		t.Fatalf("the state is %+v; want it reset by the checkpoint that exists and not exact", st)
	}
	if got := rec.counts()["checkpoint.shadow_exists"]; got != 2 {
		t.Errorf("checkpoint.shadow_exists = %d, want 2 (both copies of the edge)", got)
	}
	requireExactStates(t, s)
}

// A checkpoint placed before the newest one (the lag is larger than it was) is not
// the newest a read finds, and the writer's size is its own: not exact.
func TestACheckpointPlacedBeforeTheNewestLeavesTheStateNotExact(t *testing.T) {
	t.Parallel()
	fs := vfs.NewMem()
	s, _ := openCounting(t, fs, CheckpointOptions{On: true, KMin: 3, Lag: 10})
	for i, ns := range []int{100, 105, 110} {
		if err := s.Write(bg, []store.Record{podEdge(ns, uint64(i+1))}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, _ = openCounting(t, fs, CheckpointOptions{On: true, KMin: 1, Lag: 30})
	prefix := podForwardPrefix(t, s)
	// At 111 the newest checkpoint is at 101; the policy now places one at 112-30.
	if err := s.Write(bg, []store.Record{podEdge(111, 4)}); err != nil {
		t.Fatal(err)
	}
	st := stateOf(s, prefix)
	if len(st.ckpts) != 2 || st.ckpts[0] != asyncAt(82).UnixNano() {
		t.Fatalf("the state is %+v; want a checkpoint at 82 before the one at 101", st)
	}
	if st.exact {
		t.Fatalf("the state is exact with a checkpoint placed before the newest: %+v", st)
	}
	requireExactStates(t, s)
}

// The instrument's hook places a checkpoint wherever it is told: not exact.
func TestACheckpointTheInstrumentPlacesLeavesTheStateNotExact(t *testing.T) {
	t.Parallel()
	s, _ := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 2})
	withACheckpoint(t, s)
	writeAt(t, s, 100, 105)
	prefix := podForwardPrefix(t, s)
	if st := stateOf(s, prefix); st == nil || !st.exact || len(st.ckpts) != 1 {
		t.Fatalf("before the hook the state is %+v; want exact, with the checkpoint the policy placed", st)
	}
	if err := s.Instrument().CheckpointEdges(catalog.L0, podFP, store.Forward, asyncAt(103)); err != nil {
		t.Fatal(err)
	}
	if st := stateOf(s, prefix); st == nil || st.exact || len(st.ckpts) != 2 {
		t.Fatalf("after the hook the state is %+v; want two checkpoints and not exact", st)
	}
	requireExactStates(t, s)
}

// In a database that has never had a checkpoint nothing is read and the state a
// write starts from is empty by assumption: it is not what a read finds, so no state
// is exact. Once a checkpoint exists, a prefix a complete map knows to hold nothing
// starts exact.
func TestOnlyWhatIsReadOrKnownEmptyThroughACompleteMapIsExact(t *testing.T) {
	t.Parallel()
	t.Run("no checkpoint in the database", func(t *testing.T) {
		t.Parallel()
		s, _ := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 1000})
		for i, ns := range []int{1, 2, 3} {
			if err := s.Write(bg, []store.Record{podEdge(ns, uint64(i+1))}); err != nil {
				t.Fatal(err)
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.anyCkpt {
			t.Fatal("the database has a checkpoint")
		}
		for k, st := range s.states.all() {
			if st.exact {
				t.Errorf("the state of %x is exact in a database without a checkpoint: %+v", k, *st)
			}
		}
		if s.states.len() == 0 {
			t.Error("no state is remembered: the test checks nothing")
		}
	})

	t.Run("a prefix a complete map knows to be empty", func(t *testing.T) {
		t.Parallel()
		s, _ := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 1})
		waitScanned(t, s)
		if err := s.Write(bg, []store.Record{podEdge(5, 1)}); err != nil { // places a checkpoint
			t.Fatal(err)
		}
		other := edgeAt(catalog.L1, podFP, nodeFP, asyncAt(7), 2)
		if !completeOf(s) {
			t.Fatal("the map is not complete")
		}
		if err := s.Write(bg, []store.Record{other}); err != nil {
			t.Fatal(err)
		}
		p, _ := s.prefixOf(catalog.L1, podFP, byte(store.Forward))
		if st := stateOf(s, p); st == nil || !st.exact {
			t.Fatalf("the state of a prefix the complete map knew to be empty is %+v; want exact", st)
		}
		requireExactStates(t, s)
	})
}

// A checkpoint built with the count is the checkpoint built by the seek that is the
// walk's other start, to the byte, for every instant it could be built at in every
// prefix of a world, with its checkpoints and baseline; the count is the records at
// or after the instant, and is not made where a checkpoint stands at or after it.
// "checkpoint.build_records_walked" counts the walk of the records before the
// instant and nothing of the count.
func TestBuildingWithTheCountBuildsTheSameCheckpoint(t *testing.T) {
	t.Parallel()
	for _, seed := range []int64{0, 1, 2, 3} {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			w := newAsyncWorld(seed)
			rec := &countRecorder{}
			o := chunkOptions(vfs.NewMem(), policy(CheckpointOptions{On: true, KMin: 2, Lag: 3}))
			o.Recorder = rec
			s := mustOpen(t, o)
			for _, batch := range w.pre {
				if err := s.Write(bg, cloneAll(batch)); err != nil {
					t.Fatal(err)
				}
			}
			// A retention gives the prefixes a baseline.
			if err := s.Retain(bg, w.h); err != nil {
				t.Fatal(err)
			}
			waitRetained(t, s)
			s.mu.Lock()
			defer s.mu.Unlock()
			var built, counted, withCkpt int
			for prefix, rows := range prefixKeys(t, s) {
				var instants []int64
				for _, k := range rows {
					instants = append(instants, k.ns-1, k.ns, k.ns+1)
				}
				slices.Sort(instants)
				for _, c := range slices.Compact(instants) {
					if c < 1 {
						continue
					}
					before := rec.counts()
					plain, err := s.build([]byte(prefix), c)
					if err != nil {
						t.Fatal(err)
					}
					mid := rec.counts()
					with, shadow, err := s.buildCounting([]byte(prefix), c, true)
					if err != nil {
						t.Fatal(err)
					}
					after := rec.counts()
					built++
					if !bytes.Equal(plain, with) {
						t.Fatalf("the checkpoint at %d of prefix %x differs when built with the count", c, prefix)
					}
					if a, b := mid["checkpoint.build_records_walked"]-before["checkpoint.build_records_walked"], after["checkpoint.build_records_walked"]-mid["checkpoint.build_records_walked"]; a != b {
						t.Fatalf("building at %d of prefix %x walked %d records, and with the count %d", c, prefix, a, b)
					}
					var records, valueBytes int
					blocked := false
					for _, k := range rows {
						if k.ns < c {
							break
						}
						if k.kind != kindRecord {
							blocked = true
							break
						}
						records++
						valueBytes += k.valueLen
					}
					if blocked {
						withCkpt++
					}
					if shadow.counted == blocked {
						t.Fatalf("at %d of prefix %x the count is complete: %v, and a checkpoint or baseline stands at or after it: %v", c, prefix, shadow.counted, blocked)
					}
					if shadow.counted {
						counted++
						if shadow.records != records || shadow.bytes != valueBytes {
							t.Fatalf("at %d of prefix %x the count is %d records and %d bytes; the records at or after it are %d and %d", c, prefix, shadow.records, shadow.bytes, records, valueBytes)
						}
						if got := after["checkpoint.shadow_records"] - mid["checkpoint.shadow_records"]; got != int64(records) {
							t.Fatalf("checkpoint.shadow_records grew by %d, want %d", got, records)
						}
					}
				}
			}
			if built == 0 || counted == 0 || withCkpt == 0 {
				t.Fatalf("%d checkpoints built, %d with a complete count and %d with a checkpoint or baseline at or after the instant: the world does not reach both", built, counted, withCkpt)
			}
		})
	}
}

type keyRow struct {
	ns       int64
	kind     byte
	valueLen int
}

// prefixKeys is the keys of every prefix of the store, newest first.
func prefixKeys(t *testing.T, s *Store) map[string][]keyRow {
	t.Helper()
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	out := map[string][]keyRow{}
	for ok := it.First(); ok; ok = it.Next() {
		prefix, ns, _, kind, err := parseKey(it.Key())
		if err != nil {
			t.Fatal(err)
		}
		out[string(prefix)] = append(out[string(prefix)], keyRow{ns, kind, len(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// A key at or after c that cannot be read fails nothing in the count: the build
// without it seeks past the key, so the count gives up and the walk seeks, and the
// checkpoint is the same.
func TestAKeyThatCannotBeReadAboveTheInstantDoesNotFailTheCount(t *testing.T) {
	t.Parallel()
	s, _ := openCounting(t, vfs.NewMem(), CheckpointOptions{On: true, KMin: 1000})
	writeAt(t, s, 10, 20, 30)
	prefix := podForwardPrefix(t, s)
	rawKeyInPrefix(t, s, recordKey(prefix, 1<<62, 1)[:keyLen-1]) // the newest key of the prefix, a byte short
	s.mu.Lock()
	defer s.mu.Unlock()
	c := asyncAt(25).UnixNano()
	plain, err := s.build(prefix, c)
	if err != nil {
		t.Fatalf("building without the count: %v", err)
	}
	with, shadow, err := s.buildCounting(prefix, c, true)
	if err != nil {
		t.Fatalf("building with the count failed on a key the build skips: %v", err)
	}
	if shadow.counted || !bytes.Equal(plain, with) {
		t.Fatalf("the count is complete: %v, and the checkpoints are equal: %v", shadow.counted, bytes.Equal(plain, with))
	}
}
