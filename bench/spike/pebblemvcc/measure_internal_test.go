package pebblemvcc

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

func writeTiny(t *testing.T, e *Engine) []engine.Record {
	t.Helper()
	cfg := workload.Tiny()
	cfg.Duration = 15 * time.Minute
	cfg.CoalesceRuns = true
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.3, 3*time.Minute
	g, err := workload.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	recs := g.All()
	for i := 0; i < len(recs); i += 50 {
		if err := e.Write(recs[i:min(i+50, len(recs))]); err != nil {
			t.Fatal(err)
		}
	}
	return recs
}

// The parts of Breakdown add up to the bytes of every data key and value,
// counted straight off the database: nothing is counted twice and nothing is
// left out.
func TestBreakdownAddsUpToTheBytesOnDisk(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	recs := writeTiny(t, e)
	if err := e.Retain(recs[len(recs)/2].EventTime); err != nil {
		t.Fatal(err)
	}
	if err := e.Settle(); err != nil {
		t.Fatal(err)
	}

	lo, hi := dataBounds()
	it, err := e.kv.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		t.Fatal(err)
	}
	var raw int64
	for ok := it.First(); ok; ok = it.Next() {
		raw += int64(len(it.Key()) + len(it.Value()))
	}
	_ = it.Close()

	parts, err := e.Breakdown()
	if err != nil {
		t.Fatal(err)
	}
	var sum int64
	kinds := map[string]bool{}
	for p, n := range parts {
		sum += n
		kinds[p.Kind] = true
	}
	if sum != raw || raw == 0 {
		t.Errorf("the parts add up to %d bytes, the keys and values to %d", sum, raw)
	}
	for _, k := range []string{pebblekv.PartObserve, pebblekv.PartDelete, pebblekv.PartExtension} {
		if !kinds[k] {
			t.Errorf("no %s bytes after a retention: %v", k, kinds)
		}
	}
	for _, k := range []string{pebblekv.PartCheckpoint, pebblekv.PartBaseline} {
		if kinds[k] {
			t.Errorf("layout M has %s bytes, which it has no such thing as", k)
		}
	}
}

// Every iterator statistic is recorded, under the read that made it, for each
// kind of read. A read that forgot to report would show up as a missing name.
func TestEveryKindOfReadReportsItsIteratorStatistics(t *testing.T) {
	t.Parallel()
	rec := &engine.MemRecorder{}
	e := openMem(t, Options{Recorder: rec})
	recs := writeTiny(t, e)
	if err := e.Settle(); err != nil {
		t.Fatal(err)
	}
	rec.Reset()
	at := recs[len(recs)-1].EventTime
	r := recs[len(recs)-1]
	fp := r.Subject.A
	scope := engine.Current(r.Layer)
	if _, err := e.Neighbors(fp, engine.Forward, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := e.NeighborsBatch([]identity.Fingerprint{fp, fp}, engine.Forward, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Alive(fp, at, scope); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Window(fp, engine.Forward, at.Add(-time.Hour), at.Add(time.Nanosecond), scope); err != nil {
		t.Fatal(err)
	}
	for _, op := range []string{"neighbors", "batch", "alive", "window"} {
		if rec.Counter("read."+op+".reads") != 1 {
			t.Errorf("read.%s.reads = %d, want 1", op, rec.Counter("read."+op+".reads"))
		}
		for _, name := range []string{"block_bytes", "points", "seeks", "steps", "internal_seeks", "internal_steps"} {
			if len(rec.Samples("read."+op+"."+name)) != 1 {
				t.Errorf("read.%s.%s was not sampled once", op, name)
			}
		}
		if got := rec.Samples("read." + op + ".block_bytes"); len(got) == 1 && got[0] == 0 && op != "alive" {
			t.Errorf("read.%s loaded no blocks from a settled database", op)
		}
	}
}

// Stats says what storage did, in numbers that move with the engine, and the
// engine can be brought to rest and compacted.
func TestStatsQuiesceAndCompactAll(t *testing.T) {
	t.Parallel()
	e := openMem(t, Options{})
	before := e.Stats()
	writeTiny(t, e)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if err := e.Quiesce(ctx); err != nil {
		t.Fatal(err)
	}
	mid := e.Stats()
	if mid["flushes"] <= before["flushes"] || mid["live_table_bytes"] == 0 {
		t.Errorf("stats did not move with the writes: %v then %v", before, mid)
	}
	if err := e.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if e.Stats()["tables_l0"] != 0 {
		t.Errorf("tables left in L0 after CompactAll: %d", e.Stats()["tables_l0"])
	}
}
