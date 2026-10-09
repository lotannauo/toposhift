package pebblestore

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The marker a retention leaves while it is unfinished is a stored format. The
// vectors in testdata were computed independently of the code, and a change to any
// of them is a change of the format, which needs a new tag and a migration, never a
// regenerated file.
type markerVectorFile struct {
	Key     string `json:"key"`
	Vectors []struct {
		Name       string `json:"name"`
		Generation uint64 `json:"generation"`
		Layers     []struct {
			Layer   int    `json:"layer"`
			Horizon string `json:"horizon"`
			Last    uint64 `json:"last"`
		} `json:"layers"`
		Resume  string `json:"resume"`
		Phase   byte   `json:"phase"`
		Encoded string `json:"encoded"`
	} `json:"vectors"`
	Refused []struct {
		Name    string `json:"name"`
		Encoded string `json:"encoded"`
	} `json:"refused"`
}

func TestRetainMarkerGoldenVectors(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/meta_retain_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f markerVectorFile
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(metaKey(metaRetain)); got != f.Key {
		t.Errorf("the key of the marker is %s, the vectors say %s", got, f.Key)
	}
	// Both phases, one layer and four, an empty resume key and one of 20 bytes.
	var sawPhase [3]bool
	sawLayers := map[int]bool{}
	sawResume := map[int]bool{}
	for _, v := range f.Vectors {
		t.Run(v.Name, func(t *testing.T) {
			m := retainMarker{generation: v.Generation, phase: v.Phase}
			for _, l := range v.Layers {
				h, err := time.Parse(time.RFC3339Nano, l.Horizon)
				if err != nil {
					t.Fatal(err)
				}
				m.layers = append(m.layers, retainLayer{layer: catalog.Layer(l.Layer), horizon: h, last: l.Last})
			}
			var err error
			if m.resume, err = hex.DecodeString(v.Resume); err != nil {
				t.Fatal(err)
			}
			want, err := hex.DecodeString(v.Encoded)
			if err != nil {
				t.Fatal(err)
			}
			got, err := appendRetainMarker(nil, m)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("encoded %x, %v; want %x", got, err, want)
			}
			back, err := decodeRetainMarker(want)
			if err != nil {
				t.Fatal(err)
			}
			if back.generation != m.generation || back.phase != m.phase || !bytes.Equal(back.resume, m.resume) || len(back.layers) != len(m.layers) {
				t.Fatalf("decoded %+v, want %+v", back, m)
			}
			for i, l := range back.layers {
				if l.layer != m.layers[i].layer || !l.horizon.Equal(m.layers[i].horizon) || l.last != m.layers[i].last {
					t.Fatalf("layer %d decoded as %+v, want %+v", i, l, m.layers[i])
				}
			}
			// Every proper prefix of a marker is refused, and so is anything after its end.
			for n := range want {
				if _, err := decodeRetainMarker(want[:n]); !errors.Is(err, pebblekv.ErrValue) {
					t.Errorf("the first %d bytes decode as %v, want a refusal", n, err)
				}
			}
			if _, err := decodeRetainMarker(append(slices.Clone(want), 0)); !errors.Is(err, pebblekv.ErrValue) {
				t.Errorf("a byte after the end decodes as %v, want a refusal", err)
			}
			sawPhase[v.Phase] = true
			sawLayers[len(v.Layers)] = true
			sawResume[len(m.resume)] = true
		})
	}
	if !sawPhase[phaseRewrite] || !sawPhase[phaseSettle] || !sawLayers[1] || !sawLayers[4] || !sawResume[0] || !sawResume[prefixLen] {
		t.Errorf("the vectors miss a case: phases %v, layer counts %v, resume lengths %v", sawPhase, sawLayers, sawResume)
	}
	for _, r := range f.Refused {
		b, err := hex.DecodeString(r.Encoded)
		if err != nil {
			t.Fatal(err)
		}
		if m, err := decodeRetainMarker(b); !errors.Is(err, pebblekv.ErrValue) {
			t.Errorf("%s: decoded as %+v, %v; want a refusal", r.Name, m, err)
		}
	}
	if len(f.Refused) < 10 {
		t.Errorf("only %d refusals are frozen", len(f.Refused))
	}
}

// The encoder writes only what the decoder takes.
func TestRetainMarkerEncoderRefusesWhatItWouldNotRead(t *testing.T) {
	t.Parallel()
	layer := func(l catalog.Layer) retainLayer { return retainLayer{layer: l, horizon: t0, last: 1} }
	good := retainMarker{generation: 1, layers: []retainLayer{layer(catalog.L1)}, resume: []byte{1}, phase: phaseRewrite}
	if _, err := appendRetainMarker(nil, good); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*retainMarker){
		"generation 0":       func(m *retainMarker) { m.generation = 0 },
		"no layers":          func(m *retainMarker) { m.layers = nil },
		"five layers":        func(m *retainMarker) { m.layers = slices.Repeat(m.layers, 5) },
		"a layer not in use": func(m *retainMarker) { m.layers = []retainLayer{layer(catalog.L3 + 1)} },
		"layers repeated":    func(m *retainMarker) { m.layers = []retainLayer{layer(catalog.L1), layer(catalog.L1)} },
		"layers descending":  func(m *retainMarker) { m.layers = []retainLayer{layer(catalog.L2), layer(catalog.L1)} },
		"phase 0":            func(m *retainMarker) { m.phase = 0 },
		"phase 3":            func(m *retainMarker) { m.phase = 3 },
	} {
		m := good
		mutate(&m)
		if b, err := appendRetainMarker(nil, m); err == nil {
			t.Errorf("%s: encoded as %x", name, b)
		}
	}
}

// ---------------------------------------------------------------------------
// A stream and the stores written from it, for the tests of chunks.

// chunkStream is one fixed stream, cut into batches, with the horizon at which the
// tests retain it, the entities it names, and some records to write afterwards.
type chunkStream struct {
	batches  [][]store.Record
	later    []store.Record
	h        time.Time
	entities []identity.Fingerprint
}

func newChunkStream(t *testing.T, span time.Duration) *chunkStream {
	return newSeededStream(t, 1, span)
}

func newSeededStream(t *testing.T, seed uint64, span time.Duration) *chunkStream {
	t.Helper()
	cfg := storetest.Tiny()
	cfg.Seed = seed
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	cs := &chunkStream{entities: g.Entities()}
	var first, last time.Time
	for range 400 {
		batch := g.Batch(40)
		if first.IsZero() {
			first = batch[0].EventTime
		}
		last = batch[len(batch)-1].EventTime
		cs.batches = append(cs.batches, batch)
		if last.Sub(first) > span {
			break
		}
	}
	if last.Sub(first) <= span {
		t.Fatalf("the workload spans only %s", last.Sub(first))
	}
	cs.h = first.Add(last.Sub(first) / 2)
	// Records written after the retention: not the late ones that straddle it, which
	// the store refuses.
	for _, r := range g.Batch(60) {
		if !r.EventTime.Before(cs.h) {
			cs.later = append(cs.later, r)
		}
	}
	if len(cs.later) == 0 {
		t.Fatal("no record is left to write after the horizon")
	}
	return cs
}

func (cs *chunkStream) writeTo(t *testing.T, stores ...store.Store) {
	t.Helper()
	for _, batch := range cs.batches {
		for _, s := range stores {
			if err := s.Write(bg, cloneAll(batch)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func (cs *chunkStream) writeLater(t *testing.T, stores ...store.Store) {
	t.Helper()
	for _, s := range stores {
		if err := s.Write(bg, cloneAll(cs.later)); err != nil {
			t.Fatal(err)
		}
	}
}

// chunkOptions open a store on fs.
func chunkOptions(fs vfs.FS, ck *CheckpointOptions) Options {
	return Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: fs}, Checkpoints: ck}
}

func mustOpen(t *testing.T, o Options) *Store {
	t.Helper()
	s, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// dataBytes is every data key and value as stored, in key order: what the frozen
// digests are taken over.
func dataBytes(t *testing.T, s *Store) [][2]string {
	t.Helper()
	lo, hi := dataBounds()
	it, err := s.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out [][2]string
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, [2]string{string(it.Key()), string(it.Value())})
	}
	if err := it.Error(); err != nil {
		t.Fatal(err)
	}
	return out
}

// byPrefix is dataBytes cut into the keys of each prefix.
func byPrefix(t *testing.T, s *Store) map[string][][2]string {
	t.Helper()
	out := map[string][][2]string{}
	for _, kv := range dataBytes(t, s) {
		p := kv[0][:prefixLen]
		out[p] = append(out[p], kv)
	}
	return out
}

// readMarker is the marker in the database, or nil.
func readMarker(t *testing.T, s *Store) *retainMarker {
	t.Helper()
	raw, err := s.kv.GetMeta(metaKey(metaRetain))
	if err != nil {
		t.Fatal(err)
	}
	if raw == nil {
		return nil
	}
	m, err := decodeRetainMarker(raw)
	if err != nil {
		t.Fatal(err)
	}
	return &m
}

// requireStoppedBetweenPrefixes fails unless the data is what a retention stopped
// at the marker m leaves: every prefix before the marker's resume key as the
// finished retention leaves it, every prefix at or after it as it was before.
func requireStoppedBetweenPrefixes(t *testing.T, s *Store, m *retainMarker, before, after map[string][][2]string) {
	t.Helper()
	cur := byPrefix(t, s)
	seen := map[string]struct{}{}
	for _, side := range []map[string][][2]string{before, after, cur} {
		for p := range side {
			seen[p] = struct{}{}
		}
	}
	for p := range seen {
		want, which := before[p], "before"
		if m.phase == phaseSettle || p < string(m.resume) {
			want, which = after[p], "after"
		}
		if !slices.Equal(cur[p], want) {
			t.Errorf("the prefix %x (resume %x, phase %d) is not as it is %s the retention: %d keys, want %d", p, m.resume, m.phase, which, len(cur[p]), len(want))
			return
		}
	}
}

// ---------------------------------------------------------------------------

// The horizons and the marker are one synced commit, so a restart that finds the
// one finds the other; a retention that rewrites nothing leaves no marker.
func TestTheMarkerIsInTheCommitOfTheHorizons(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	var s *Store
	var seen []*retainMarker
	o := chunkOptions(vfs.NewMem(), nil)
	o.afterHorizonApply = func() error {
		seen = append(seen, readMarker(t, s)) // the commit has landed
		return nil
	}
	o.beforeHorizonApply = func() error {
		if m := readMarker(t, s); m != nil {
			t.Errorf("a marker %+v before the commit of the horizons", m)
		}
		return nil
	}
	s = mustOpen(t, o)
	cs.writeTo(t, s)
	last := s.LastSeq()
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 1 || seen[0] == nil {
		t.Fatalf("after the commit of the horizons the marker was %v", seen)
	}
	m := seen[0]
	lo, _ := dataBounds()
	if m.generation != 1 || m.phase != phaseRewrite || !bytes.Equal(m.resume, lo) || len(m.layers) != layers {
		t.Fatalf("the marker with the horizons is %+v", m)
	}
	for i, l := range m.layers {
		if l.layer != catalog.L0+catalog.Layer(i) || !l.horizon.Equal(cs.h) || l.last != last {
			t.Errorf("layer %d of the marker is %+v, want the horizon %v and L %d", i, l, cs.h, last)
		}
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("a finished retention left the marker %+v", m)
	}

	// A horizon that rewrites nothing is committed without a marker.
	for name, h := range map[string]time.Time{"the start of the range": store.MinEventTime, "before the range": store.MinEventTime.Add(-time.Hour)} {
		var fresh *Store
		commits := 0
		fresh = openMem(t, Options{afterHorizonApply: func() error {
			commits++
			if m := readMarker(t, fresh); m != nil {
				t.Errorf("%s: a marker %+v for a retention that rewrites nothing", name, m)
			}
			return nil
		}})
		if err := fresh.Retain(bg, h); err != nil {
			t.Fatal(err)
		}
		if commits != 1 || !fresh.Horizon().Time.Equal(h) || readMarker(t, fresh) != nil {
			t.Errorf("%s: %d commits of the horizon, horizon %v, marker %+v", name, commits, fresh.Horizon(), readMarker(t, fresh))
		}
	}
}

// A chunk ends between prefixes, with the marker's resume key in its own commit:
// after every commit, each prefix before the resume key is as the finished
// retention leaves it and each prefix at or after it is as it was; and the chunks
// are what the limits say.
func TestChunksEndBetweenPrefixesAndTheMarkerMovesInTheirCommit(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	cases := []struct {
		name  string
		bytes int
		time  time.Duration
	}{
		{"every prefix by size", 1, time.Hour},
		{"every prefix by time", 1 << 30, time.Nanosecond},
		{"a few prefixes by size", 3000, time.Hour},
		{"one chunk", 1 << 30, time.Hour},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			recA, recB := newMemRecorder(), newMemRecorder()
			oa := chunkOptions(vfs.NewMem(), nil)
			oa.retainBatchBytes, oa.retainChunkTime, oa.Recorder = c.bytes, c.time, recA
			ob := chunkOptions(vfs.NewMem(), nil)
			ob.retainBatchBytes, ob.retainChunkTime, ob.Recorder = c.bytes, c.time, recB
			twin, s := mustOpen(t, oa), mustOpen(t, ob)
			ref := newRef(t)
			cs.writeTo(t, twin, s, ref)
			before := byPrefix(t, s)
			if err := twin.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			after := byPrefix(t, twin)
			if len(before) < 20 {
				t.Fatalf("the stream has %d prefixes: too few to cut into chunks", len(before))
			}

			var resumes [][]byte
			s.afterRetainCommit = func() {
				m := readMarker(t, s)
				if m == nil || m.generation != 1 {
					t.Errorf("after commit %d the marker is %+v", len(resumes)+1, m)
					return
				}
				if m.phase == phaseRewrite && len(m.resume) != prefixLen {
					t.Errorf("after commit %d the resume key %x is not a prefix", len(resumes)+1, m.resume)
				}
				if n := len(resumes); n > 0 && m.phase == phaseRewrite && bytes.Compare(m.resume, resumes[n-1]) <= 0 {
					// Fatal: a pass that does not move on would not end.
					t.Fatalf("the resume key %x does not follow the one before, %x", m.resume, resumes[n-1])
				}
				requireStoppedBetweenPrefixes(t, s, m, before, after)
				resumes = append(resumes, m.resume)
			}
			if err := s.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			if m := readMarker(t, s); m != nil {
				t.Errorf("a marker is left after the retention: %+v", m)
			}
			if !slices.Equal(dataBytes(t, s), dataBytes(t, twin)) {
				t.Error("two retentions of one stream left other bytes")
			}
			if n := len(resumes); n == 0 || len(resumes[n-1]) != 0 {
				t.Error("the last commit does not carry the settling phase")
			}

			chunks := recB.samples["retain.chunks"]
			holds := recB.samples["retain.chunk_hold_ns"]
			if len(chunks) != 1 || int(chunks[0]) != len(resumes) || len(holds) != len(resumes) || len(resumes) == 0 {
				t.Fatalf("%d commits, retain.chunks %v, %d hold samples", len(resumes), chunks, len(holds))
			}
			var longest int64
			for _, h := range holds {
				if h <= 0 {
					t.Errorf("a chunk held the lock for %d ns", h)
				}
				longest = max(longest, h)
			}
			t.Logf("%s: %d chunks for %d prefixes, longest chunk %v, longest prefix %v records", c.name, len(resumes),
				recB.Counter("retain.prefixes_visited"), time.Duration(longest), recB.samples["retain.max_prefix_records"])
			if got := recB.samples["retain.max_prefix_records"]; len(got) != 1 || got[0] < 1 {
				t.Errorf("retain.max_prefix_records = %v", got)
			}
			visited := recB.Counter("retain.prefixes_visited")
			switch c.name {
			case "one chunk":
				if len(resumes) != 1 {
					t.Errorf("%d chunks, want one", len(resumes))
				}
			case "every prefix by size", "every prefix by time":
				if int64(len(resumes)) != visited {
					t.Errorf("%d chunks for %d prefixes visited: every chunk is one prefix", len(resumes), visited)
				}
			default:
				if len(resumes) < 3 || int64(len(resumes)) >= visited {
					t.Errorf("%d chunks for %d prefixes visited", len(resumes), visited)
				}
			}
			for _, k := range []string{"retain.resumed"} {
				if _, ok := recB.counters[k]; ok {
					t.Errorf("an ordinary retention counted %s", k)
				}
			}
			compareToReference(t, s, ref, cs.entities[:6], cs.h, s.LastSeq(), store.Latest)
		})
	}
}

// Chunking changes no data byte: whatever the limits, and whatever the checkpoint
// policy, the same stream retained leaves the same keys and values, the writer's
// map is what a whole read finds and is complete, and the answers are the
// reference's.
func TestChunkingLeavesTheSameBytesAndTheSameWriterState(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	policies := map[string]*CheckpointOptions{"checkpoints on": policy(stress(0)), "checkpoints off": off()}
	for name, ck := range policies {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			whole := chunkOptions(vfs.NewMem(), ck)
			whole.retainBatchBytes, whole.retainChunkTime = 1<<30, time.Hour
			base := mustOpen(t, whole)
			ref := newRef(t)
			cs.writeTo(t, base, ref)
			if err := base.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			if err := ref.Retain(bg, cs.h); err != nil {
				t.Fatal(err)
			}
			want := dataBytes(t, base)
			if ck.On && !base.anyCkpt {
				t.Fatal("the stream writes no checkpoint, so the policy is not exercised")
			}
			compareToReference(t, base, ref, cs.entities[:6], cs.h, base.LastSeq(), store.Latest)
			for _, c := range []struct {
				bytes int
				time  time.Duration
			}{{1, time.Hour}, {1 << 30, time.Nanosecond}, {1500, time.Hour}, {defaultRetainBatchBytes, defaultRetainChunkTime}} {
				o := chunkOptions(vfs.NewMem(), ck)
				o.retainBatchBytes, o.retainChunkTime = c.bytes, c.time
				s := mustOpen(t, o)
				cs.writeTo(t, s)
				if err := s.Retain(bg, cs.h); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(dataBytes(t, s), want) {
					t.Fatalf("chunks of %d bytes or %v leave other data than one chunk", c.bytes, c.time)
				}
				if s.complete != base.complete || len(s.states) != len(base.states) {
					t.Errorf("chunks of %d bytes or %v leave a map of %d prefixes (complete %v), where one chunk leaves %d (%v)",
						c.bytes, c.time, len(s.states), s.complete, len(base.states), base.complete)
				}
				if ck.On {
					if !s.complete {
						t.Errorf("chunks of %d bytes or %v leave the writer's map incomplete", c.bytes, c.time)
					}
					requireWholeReads(t, s)
				}
				// The state carried across chunks serves the writes that follow.
				cs.writeLater(t, s)
				requireWholeReads(t, s)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Crash and resume.

// A retention that fails after any of its chunks leaves the horizons and a marker;
// opening the store finishes it before Open returns, to the bytes an uninterrupted
// retention leaves, with the marker gone and nothing remembered by the writer, and
// the store then goes on as the uninterrupted one does.
func TestARetentionStoppedAfterAnyChunkIsFinishedByOpen(t *testing.T) {
	t.Parallel()
	seeds := []uint64{1, 2, 3}
	if raceEnabled {
		seeds = seeds[:1] // one goroutine per stop: the race detector finds nothing more in other streams
	}
	for _, seed := range seeds {
		for name, ck := range map[string]*CheckpointOptions{"checkpoints on": policy(stress(0)), "checkpoints off": off()} {
			t.Run(fmt.Sprintf("seed %d, %s", seed, name), func(t *testing.T) {
				t.Parallel()
				stoppedAtEveryChunk(t, newSeededStream(t, seed, 90*time.Second), ck)
			})
		}
	}
}

func stoppedAtEveryChunk(t *testing.T, cs *chunkStream, ck *CheckpointOptions) {
	chunked := func(o Options) Options {
		o.retainBatchBytes, o.retainChunkTime = 1500, time.Hour
		return o
	}
	rec := newMemRecorder()
	oc := chunked(chunkOptions(vfs.NewMem(), ck))
	oc.Recorder = rec
	whole := mustOpen(t, oc)
	ref := newRef(t)
	cs.writeTo(t, whole, ref)
	if err := whole.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	if err := ref.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	n := int(rec.samples["retain.chunks"][0])
	if n < 4 {
		t.Fatalf("the retention has %d chunks: too few to stop between", n)
	}
	want := dataBytes(t, whole)
	compareToReference(t, whole, ref, cs.entities[:6], cs.h, whole.LastSeq(), store.Latest)
	cs.writeLater(t, whole, ref)
	wantLater := dataBytes(t, whole)

	stops := make([]int, 0, n)
	for k := 1; k <= n; k++ {
		stops = append(stops, k)
	}
	for _, k := range stops {
		t.Run(fmt.Sprintf("after chunk %d of %d", k, n), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			o := chunked(chunkOptions(fs, ck))
			o.retainStopAfter = k
			s, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			cs.writeTo(t, s)
			if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
				t.Fatalf("Retain = %v, want the injected failure", err)
			}
			m := readMarker(t, s)
			if m == nil || m.generation != 1 || (k < n) != (m.phase == phaseRewrite) {
				t.Fatalf("after %d of %d chunks the marker is %+v", k, n, m)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			rec := newMemRecorder()
			ro := chunked(chunkOptions(fs, ck))
			ro.Recorder = rec
			ro.SettleRetention, ro.SettleDeadline = k == n, 10*time.Millisecond
			s, err = Open("db", ro)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			if m := readMarker(t, s); m != nil {
				t.Errorf("Open returned with the marker %+v", m)
			}
			if rec.Counter("retain.resumed") != 1 {
				t.Errorf("retain.resumed = %d, want 1", rec.Counter("retain.resumed"))
			}
			// Finished before Open returned, to the same bytes, from the same
			// place: only the chunks after the stop ran.
			if got := rec.samples["retain.chunks"]; len(got) != 1 || int(got[0]) != n-k {
				t.Errorf("the resumed retention ran %v chunks, want %d", got, n-k)
			}
			if _, settled := rec.counters["retain.settle_ns"]; settled != (k == n) {
				t.Errorf("settled %v after stopping in chunk %d of %d", settled, k, n)
			}
			if !slices.Equal(dataBytes(t, s), want) {
				t.Fatal("the resumed retention left other data than an uninterrupted one")
			}
			if got := s.Horizon(); !got.Time.Equal(cs.h) {
				t.Errorf("the horizon is %v, want %v", got, cs.h)
			}
			// Nothing is remembered of the prefixes, and the map is not complete.
			if s.complete || len(s.states) != 0 {
				t.Errorf("after the resumed retention the map is complete %v and holds %d prefixes", s.complete, len(s.states))
			}
			// The writer then learns the prefixes as it meets them, and gets what
			// the uninterrupted store gets.
			cs.writeLater(t, s)
			requireWholeReads(t, s)
			if !slices.Equal(dataBytes(t, s), wantLater) {
				t.Fatal("writes after the resumed retention left other data than after an uninterrupted one")
			}
			if k == 1 || k == n {
				compareToReference(t, s, ref, cs.entities[:6], cs.h, s.LastSeq(), store.Latest)
			}
		})
	}
}

// Open can itself be stopped, and the next Open picks up where that one left the
// marker, under the same generation.
func TestAResumeThatStopsIsResumedAgain(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	rec := newMemRecorder()
	oc := chunkOptions(vfs.NewMem(), nil)
	oc.retainBatchBytes, oc.retainChunkTime, oc.Recorder = 1500, time.Hour, rec
	whole := mustOpen(t, oc)
	cs.writeTo(t, whole)
	if err := whole.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	n, want := int(rec.samples["retain.chunks"][0]), dataBytes(t, whole)
	if n < 6 {
		t.Fatalf("%d chunks", n)
	}

	fs := vfs.NewMem()
	o := chunkOptions(fs, nil)
	o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, 2
	s, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	first := readMarker(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	o.retainStopAfter, o.resumeStopAfter = 0, 2
	if _, err := Open("db", o); !errors.Is(err, errInjected) {
		t.Fatalf("Open = %v, want the injected failure", err)
	}
	// The database is closed again, so a third Open can take it, and the marker moved
	// on, under the same generation.
	o.resumeStopAfter = 0
	s, err = Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if first == nil || first.generation != 1 || readMarker(t, s) != nil {
		t.Fatalf("the first marker was %+v and the last is %+v", first, readMarker(t, s))
	}
	if !slices.Equal(dataBytes(t, s), want) {
		t.Fatal("a retention resumed twice left other data than an uninterrupted one")
	}
}

// A pass over keys it has already rewritten changes none of them: whatever of it
// was lost, or run again, is harmless. This is what lets a chunk be committed with
// no sync, and a retention that stopped after its chunk but before its marker moved
// be run from the chunk before.
func TestARepeatedPassChangesNoKey(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	for name, ck := range map[string]*CheckpointOptions{"checkpoints on": policy(stress(0)), "checkpoints off": off()} {
		for hname, horizon := range map[string]time.Time{"inside the range": cs.h, "after the range": store.MaxEventTime.Add(time.Hour)} {
			t.Run(name+", "+hname, func(t *testing.T) {
				t.Parallel()
				fs := vfs.NewMem()
				o := chunkOptions(fs, ck)
				o.retainBatchBytes, o.retainChunkTime = 1500, time.Hour
				s, err := Open("db", o)
				if err != nil {
					t.Fatal(err)
				}
				cs.writeTo(t, s)
				last := s.LastSeq()
				if err := s.Retain(bg, horizon); err != nil {
					t.Fatal(err)
				}
				want := dataBytes(t, s)
				lo, _ := dataBounds()
				for i := range 2 {
					// Put the first marker back, as if nothing had been done.
					m := retainMarker{generation: 7 + uint64(i), resume: lo, phase: phaseRewrite}
					for l := range layers {
						m.layers = append(m.layers, retainLayer{layer: catalog.L0 + catalog.Layer(l), horizon: horizon.UTC(), last: last})
					}
					raw, err := appendRetainMarker(nil, m)
					if err != nil {
						t.Fatal(err)
					}
					if err := s.kv.Set(metaKey(metaRetain), raw, s.kv.WriteOptions()); err != nil {
						t.Fatal(err)
					}
					if err := s.Close(); err != nil {
						t.Fatal(err)
					}
					if s, err = Open("db", o); err != nil {
						t.Fatal(err)
					}
					if readMarker(t, s) != nil {
						t.Fatal("the marker is still there after Open")
					}
					if !slices.Equal(dataBytes(t, s), want) {
						t.Fatalf("pass %d over the rewritten keys changed some", i+2)
					}
				}
				_ = s.Close()
			})
		}
	}
}

// ---------------------------------------------------------------------------
// The marker and Open.

// A marker is the retention of the generation that wrote it, and only that
// generation deletes it.
func TestOnlyTheGenerationThatWroteTheMarkerDeletesIt(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	write := func(gen uint64) {
		m := retainMarker{generation: gen, layers: []retainLayer{{layer: catalog.L1, horizon: t0, last: 1}}, resume: []byte{1}, phase: phaseRewrite}
		raw, err := appendRetainMarker(nil, m)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.kv.Set(metaKey(metaRetain), raw, s.kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.releaseMarker(1); err != nil {
		t.Fatalf("releasing an absent marker: %v", err)
	}
	write(2)
	for _, gen := range []uint64{1, 3} {
		if err := s.releaseMarker(gen); err != nil {
			t.Fatal(err)
		}
		if m := readMarker(t, s); m == nil || m.generation != 2 {
			t.Fatalf("generation %d deleted the marker of generation 2: %+v", gen, m)
		}
	}
	if err := s.releaseMarker(2); err != nil {
		t.Fatal(err)
	}
	if m := readMarker(t, s); m != nil {
		t.Fatalf("generation 2 left its own marker: %+v", m)
	}
}

// A Retain with a later horizon after one that stopped starts over from the first
// key under the next generation, and the generation that stopped cannot delete its
// marker; a Retain with the same horizon does nothing, the marker or not.
func TestALaterRetentionAfterAStoppedOneStartsAgainUnderANewGeneration(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	later := cs.h.Add(20 * time.Second)

	o := chunkOptions(vfs.NewMem(), nil)
	o.retainBatchBytes, o.retainChunkTime = 1, time.Hour
	twin := mustOpen(t, o)
	cs.writeTo(t, twin)
	if err := twin.Retain(bg, later); err != nil {
		t.Fatal(err)
	}
	want := dataBytes(t, twin)

	o = chunkOptions(vfs.NewMem(), nil)
	o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1, time.Hour, 3 // a prefix to a chunk
	s := mustOpen(t, o)
	ref := newRef(t)
	cs.writeTo(t, s, ref)
	if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	stopped := dataBytes(t, s)
	first := readMarker(t, s)
	if first == nil || first.generation != 1 || len(first.resume) != prefixLen {
		t.Fatalf("after three chunks the marker is %+v", first)
	}
	if err := s.Retain(bg, cs.h); err != nil || !slices.Equal(dataBytes(t, s), stopped) {
		t.Fatalf("a second Retain with the same horizon = %v, and it changed the data: %v", err, !slices.Equal(dataBytes(t, s), stopped))
	}

	s.stopAfter = 0
	calls := 0
	s.afterRetainCommit = func() {
		calls++
		m := readMarker(t, s)
		if m == nil || m.generation != 2 {
			t.Errorf("after commit %d of the later retention the marker is %+v, want generation 2", calls, m)
			return
		}
		// The retention that stopped finishes late, or is retried: it must leave this
		// marker alone.
		if err := s.releaseMarker(1); err != nil {
			t.Error(err)
		}
		if again := readMarker(t, s); again == nil || again.generation != 2 {
			t.Errorf("generation 1 deleted the marker of generation 2: %+v", again)
		}
		if calls == 1 && bytes.Compare(m.resume, first.resume) >= 0 {
			// Started from the first key: the first chunk is the first prefix, which is
			// before where the earlier retention stopped, not the one after it.
			t.Errorf("the later retention's first chunk reached %x, not before the first retention's stop at %x", m.resume, first.resume)
		}
	}
	if err := s.Retain(bg, later); err != nil {
		t.Fatal(err)
	}
	if calls < 3 {
		t.Fatalf("the later retention made %d commits", calls)
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
	if !slices.Equal(dataBytes(t, s), want) {
		t.Fatal("a retention that began again after a stopped one left other data than one retention to the later horizon")
	}
	if err := ref.Retain(bg, later); err != nil {
		t.Fatal(err)
	}
	compareToReference(t, s, ref, cs.entities[:6], later, s.LastSeq(), store.Latest)
}

// A marker that disagrees with what is stored beside it is refused, whatever the
// reason it does.
func TestOpenRefusesAMarkerThatDoesNotFit(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	cases := map[string]func(t *testing.T, s *Store, m retainMarker){
		"a horizon stored later": func(t *testing.T, s *Store, m retainMarker) {
			raw, _ := pebblekv.EncodeLayerHorizon(cs.h.Add(time.Second), m.layers[0].last)
			set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L2)), raw)
		},
		"a sequence number stored higher": func(t *testing.T, s *Store, m retainMarker) {
			raw, _ := pebblekv.EncodeLayerHorizon(cs.h, m.layers[0].last+1)
			set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L0)), raw)
		},
		"a horizon stored earlier in every layer": func(t *testing.T, s *Store, m retainMarker) {
			for l := range layers {
				raw, _ := pebblekv.EncodeLayerHorizon(cs.h.Add(-time.Second), m.layers[0].last)
				set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(l))), raw)
			}
		},
		"horizons stored later in some layers and earlier in others": func(t *testing.T, s *Store, m retainMarker) {
			for l := range layers {
				at := cs.h.Add(time.Second)
				if l%2 == 1 {
					at = cs.h.Add(-time.Second)
				}
				raw, _ := pebblekv.EncodeLayerHorizon(at, m.layers[0].last)
				set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(l))), raw)
			}
		},
		"horizons stored later in every layer, with a lower sequence number": func(t *testing.T, s *Store, m retainMarker) {
			for l := range layers {
				raw, _ := pebblekv.EncodeLayerHorizon(cs.h.Add(time.Second), m.layers[0].last-1)
				set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(l))), raw)
			}
		},
		"horizons stored later in every layer but one": func(t *testing.T, s *Store, m retainMarker) {
			for l := 1; l < layers; l++ {
				raw, _ := pebblekv.EncodeLayerHorizon(cs.h.Add(time.Second), m.layers[0].last)
				set(t, s, metaKey(pebblekv.HorizonMetaName(catalog.L0+catalog.Layer(l))), raw)
			}
		},
		"a horizon never stored": func(t *testing.T, s *Store, m retainMarker) {
			if err := s.kv.Delete(metaKey(pebblekv.HorizonMetaName(catalog.L1)), s.kv.WriteOptions()); err != nil {
				t.Fatal(err)
			}
		},
		"a marker that is not one": func(t *testing.T, s *Store, m retainMarker) {
			raw, _ := appendRetainMarker(nil, m)
			set(t, s, metaKey(metaRetain), append(raw, 0))
		},
		"a resume key past the data": func(t *testing.T, s *Store, m retainMarker) {
			m.resume = []byte{0x7F}
			setMarker(t, s, m)
		},
		"an empty resume key while rewriting": func(t *testing.T, s *Store, m retainMarker) {
			m.resume = nil
			setMarker(t, s, m)
		},
		"a resume key longer than a prefix": func(t *testing.T, s *Store, m retainMarker) {
			m.resume = bytes.Repeat([]byte{2}, prefixLen+1)
			setMarker(t, s, m)
		},
		"a horizon that rewrites nothing": func(t *testing.T, s *Store, m retainMarker) {
			for i := range m.layers {
				m.layers[i].horizon = store.MinEventTime
				raw, _ := pebblekv.EncodeLayerHorizon(store.MinEventTime, m.layers[i].last)
				set(t, s, metaKey(pebblekv.HorizonMetaName(m.layers[i].layer)), raw)
			}
			setMarker(t, s, m)
		},
	}
	for name, corrupt := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewMem()
			o := chunkOptions(fs, nil)
			o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, 2
			s, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			cs.writeTo(t, s)
			if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
				t.Fatal(err)
			}
			m := *readMarker(t, s)
			m.layers = slices.Clone(m.layers)
			corrupt(t, s, m)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			o.retainStopAfter = 0
			if s, err := Open("db", o); !errors.Is(err, store.ErrInvalid) {
				if err == nil {
					_ = s.Close()
				}
				t.Fatalf("Open = %v, want a refusal wrapping ErrInvalid", err)
			}
			// The refusal released the database: it can be opened again with the fault
			// mended. (The marker is left for the owner to look at, not deleted.)
		})
	}
}

// A database opened read-only cannot finish anything, and does not try: its reads
// are the retained store's, and the marker waits for a store that can write.
func TestAReadOnlyOpenLeavesTheMarkerAlone(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	fs := vfs.NewMem()
	o := chunkOptions(fs, nil)
	o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, 2
	s, err := Open("db", o)
	if err != nil {
		t.Fatal(err)
	}
	cs.writeTo(t, s)
	if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
		t.Fatal(err)
	}
	stopped, marker := dataBytes(t, s), readMarker(t, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	rec := newMemRecorder()
	ro := chunkOptions(fs, nil)
	ro.ReadOnly, ro.Recorder = true, rec
	r, err := Open("db", ro)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	if got := readMarker(t, r); got == nil || got.generation != marker.generation || !bytes.Equal(got.resume, marker.resume) || !slices.Equal(dataBytes(t, r), stopped) {
		t.Errorf("a read-only Open changed the database: the marker is %+v, was %+v", got, marker)
	}
	if _, resumed := rec.counters["retain.resumed"]; resumed {
		t.Error("a read-only Open counted a resumed retention")
	}
	if got := r.Horizon(); !got.Time.Equal(cs.h) {
		t.Errorf("the horizon is %v, want %v", got, cs.h)
	}
}

func set(t *testing.T, s *Store, key, val []byte) {
	t.Helper()
	if err := s.kv.Set(key, val, s.kv.WriteOptions()); err != nil {
		t.Fatal(err)
	}
}

func setMarker(t *testing.T, s *Store, m retainMarker) {
	t.Helper()
	raw, err := appendRetainMarker(nil, m)
	if err != nil {
		t.Fatal(err)
	}
	set(t, s, metaKey(metaRetain), raw)
}

// Each chunk reads the database as it is when the chunk begins. A prefix that
// appears while the pass is under way (here, committed by the hook between two
// chunks) is rewritten when the pass reaches it, whole, and not left or half done
// as an iterator opened earlier would leave it.
func TestEachChunkReadsWhatTheChunksBeforeItCommitted(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	o := chunkOptions(vfs.NewMem(), nil)
	o.retainBatchBytes, o.retainChunkTime = 1, time.Hour
	s := mustOpen(t, o)
	cs.writeTo(t, s)
	// A prefix that sorts after every other, with a record from before the horizon.
	fp := fingerprintOf(catalog.K8sPod, 0xEE)
	prefix, ok := s.prefixOf(catalog.L3, fp, dirEntity)
	if !ok {
		t.Fatal("no prefix")
	}
	if prefix[0] != pebblekv.LayerByte(catalog.L3) {
		t.Fatal("not in the last layer")
	}
	ref := appendRef(nil, nil, nil, "late-arrival")
	ns := cs.h.Add(-time.Second).UnixNano()
	seq := s.LastSeq() + 1000
	val := appendRecordValue(nil, ref, pebblekv.Value{Seq: seq, Kind: lifecycle.Observe, Payload: []byte("x")})
	inserted := false
	s.afterRetainCommit = func() {
		if inserted {
			return
		}
		inserted = true
		set(t, s, recordKey(prefix, ns, seq), val)
	}
	if err := s.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	if !inserted {
		t.Fatal("the hook never ran")
	}
	var keys []stored
	for _, k := range dump(t, s) {
		if k.dir == dirEntity && k.ns <= cs.h.UnixNano() && k.entries == "late-arrival" || k.kind == kindBaseline && k.entries == fmt.Sprintf("late-arrival@%d", ns) {
			keys = append(keys, k)
		}
	}
	if len(keys) != 1 || keys[0].kind != kindBaseline {
		t.Fatalf("the prefix that appeared between chunks was not rewritten into a baseline: %v", keys)
	}
}

// A retention with nothing to rewrite is one chunk that says so, and leaves no marker.
func TestAnEmptyStoreRetainsInOneChunkAndLeavesNoMarker(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec})
	if err := s.Retain(bg, t0); err != nil {
		t.Fatal(err)
	}
	if got := rec.samples["retain.chunks"]; len(got) != 1 || got[0] != 1 {
		t.Errorf("retain.chunks = %v, want one chunk to say there is nothing to do", got)
	}
	if m := readMarker(t, s); m != nil {
		t.Errorf("the marker %+v is left", m)
	}
}

// A binary from before the marker, which shares the format tag, can move the
// horizons and leave the marker where it was. A marker that every stored horizon
// has moved past is overtaken: Open removes it and touches no data, instead of
// refusing the database for good.
func TestAMarkerThatEveryStoredHorizonHasMovedPastIsRemoved(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	stop := func(t *testing.T) (vfs.FS, Options, [][2]string) {
		fs := vfs.NewMem()
		o := chunkOptions(fs, nil)
		o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, 2
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		cs.writeTo(t, s)
		if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
			t.Fatal(err)
		}
		m := *readMarker(t, s)
		for _, l := range m.layers {
			raw, _ := pebblekv.EncodeLayerHorizon(cs.h.Add(time.Minute), l.last+5)
			set(t, s, metaKey(pebblekv.HorizonMetaName(l.layer)), raw)
		}
		data := dataBytes(t, s)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		o.retainStopAfter = 0
		return fs, o, data
	}

	t.Run("a store that can write", func(t *testing.T) {
		t.Parallel()
		fs, o, data := stop(t)
		rec := newMemRecorder()
		o.Recorder = rec
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		if m := readMarker(t, s); m != nil {
			t.Errorf("the marker %+v is left", m)
		}
		if rec.Counter("retain.superseded") != 1 {
			t.Errorf("retain.superseded = %d, want 1", rec.Counter("retain.superseded"))
		}
		if _, resumed := rec.counters["retain.resumed"]; resumed || len(rec.samples["retain.chunks"]) != 0 {
			t.Error("a retention was run for an overtaken marker")
		}
		if !slices.Equal(dataBytes(t, s), data) {
			t.Error("removing an overtaken marker changed the data")
		}
		if got := s.Horizon(); !got.Time.Equal(cs.h.Add(time.Minute)) {
			t.Errorf("the horizon is %v", got)
		}
		_ = fs
		// And the next retention is the first of a new run of generations.
		if err := s.Retain(bg, cs.h.Add(2*time.Minute)); err != nil || readMarker(t, s) != nil {
			t.Errorf("a later Retain = %v, marker %+v", err, readMarker(t, s))
		}
	})
	t.Run("a store opened read-only", func(t *testing.T) {
		t.Parallel()
		_, o, data := stop(t)
		o.ReadOnly = true
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = s.Close() }()
		if readMarker(t, s) == nil || !slices.Equal(dataBytes(t, s), data) {
			t.Error("a read-only Open changed the database")
		}
	})
}

// A crash loses the commits that were not synced. The chunk commits are not, so a
// crash keeps the synced prefix of the log: the horizons and the marker, and the
// chunks up to the last sync that followed them. Here a synced write at chunk j
// makes chunks 1 to j survive, the retention stops after chunk k and the disk is
// what a crash then leaves; opening it must resume after chunk j (and not before
// the marker's resume key, nor past it), finish the pass, and leave the bytes of
// an uninterrupted retention and no marker. j = 0 syncs nothing, so everything
// after the horizons is lost.
func TestACrashLosesOnlyTheChunksAfterTheLastSync(t *testing.T) {
	t.Parallel()
	cs := newChunkStream(t, 90*time.Second)
	build := func(fs vfs.FS, stop int) *Store {
		o := chunkOptions(fs, nil)
		o.Sync = true
		o.retainBatchBytes, o.retainChunkTime, o.retainStopAfter = 1500, time.Hour, stop
		s, err := Open("db", o)
		if err != nil {
			t.Fatal(err)
		}
		cs.writeTo(t, s)
		return s
	}
	rec := newMemRecorder()
	whole := build(vfs.NewMem(), 0)
	whole.rec = rec
	if err := whole.Retain(bg, cs.h); err != nil {
		t.Fatal(err)
	}
	n, want := int(rec.samples["retain.chunks"][0]), dataBytes(t, whole)
	_ = whole.Close()
	if n < 8 {
		t.Fatalf("%d chunks", n)
	}

	pairs := [][2]int{{0, 2}, {0, n / 2}, {0, n}, {1, 2}, {1, n}, {n / 2, n/2 + 1}, {n - 1, n}}
	if raceEnabled {
		pairs = [][2]int{{0, n / 2}, {1, n}, {n - 1, n}}
	}
	for _, pair := range pairs {
		j, k := pair[0], pair[1]
		t.Run(fmt.Sprintf("synced at chunk %d, stopped after chunk %d of %d", j, k, n), func(t *testing.T) {
			t.Parallel()
			fs := vfs.NewCrashableMem()
			s := build(fs, k)
			var resumeJ []byte
			calls := 0
			s.afterRetainCommit = func() {
				calls++
				if calls == j {
					resumeJ = slices.Clone(readMarker(t, s).resume)
					if err := s.kv.LogData(nil, pebble.Sync); err != nil {
						t.Error(err)
					}
				}
			}
			if err := s.Retain(bg, cs.h); !errors.Is(err, errInjected) {
				t.Fatalf("Retain = %v", err)
			}
			// What the disk holds after a crash at this moment: only what was synced.
			crashed := fs.CrashClone(vfs.CrashCloneCfg{})
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			// The marker the crash left: after chunk j's prefixes if chunk j was synced,
			// so that the test is about a partial loss and not all or nothing.
			peek := chunkOptions(crashed.CrashClone(vfs.CrashCloneCfg{}), nil)
			peek.ReadOnly = true
			q, err := Open("db", peek)
			if err != nil {
				t.Fatal(err)
			}
			left := readMarker(t, q)
			_ = q.Close()
			// (Pebble may sync more than was asked, when a memtable is flushed and its log
			// is closed, so the marker may be further on than the sync made certain.)
			switch {
			case left == nil:
				t.Fatal("the crash left no marker, and the retention had not finished")
			case j > 0 && (left.phase == phaseRewrite && bytes.Compare(left.resume, resumeJ) < 0):
				t.Fatalf("chunk %d was synced and the marker the crash left is %+v, before %x", j, left, resumeJ)
			}

			o := chunkOptions(crashed, nil)
			o.Sync = true
			o.retainBatchBytes, o.retainChunkTime = 1500, time.Hour
			ro := newMemRecorder()
			o.Recorder = ro
			r, err := Open("db", o)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = r.Close() }()
			if m := readMarker(t, r); m != nil {
				t.Errorf("the marker %+v is left", m)
			}
			if got := r.Horizon(); !got.Time.Equal(cs.h) {
				t.Fatalf("the horizon was lost: %v", got)
			}
			if !slices.Equal(dataBytes(t, r), want) {
				t.Fatal("after the crash and the reopening the data is not what an uninterrupted retention leaves")
			}
			// Only what the crash lost is done again: with chunks 1 to j kept, no more than
			// the n-j chunks after them (fewer if a background sync kept more).
			if got := ro.samples["retain.chunks"]; len(got) != 1 || int(got[0]) > n-j {
				t.Errorf("the resumed retention ran %v chunks; chunks 1 to %d were synced, so at most %d remain", got, j, n-j)
			}
		})
	}
}
