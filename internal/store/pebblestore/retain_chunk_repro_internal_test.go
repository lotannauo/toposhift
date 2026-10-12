package pebblestore

import (
	"context"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// The repro of a pass that slows to one prefix per chunk, in real time, on the real
// file system and the default options in the background mode. It is not a gate: it
// runs only with TOPOSHIFT_RETAIN_CHUNK_REPRO=1, and logs what it measured.
//
//   - TOPOSHIFT_RETAIN_CHUNK_REPRO_PODS is how many pods the history names (75,000 by
//     default): each has an edge to its own node before the horizon, so a pass rewrites
//     two prefixes per pod;
//   - TOPOSHIFT_RETAIN_CHUNK_REPRO_CASE is "fresh" (a pass started by Retain),
//     "resumed" (a pass stopped by Close after its first chunk and resumed by the next
//     Open) or "both" (the default);
//   - TOPOSHIFT_RETAIN_CHUNK_REPRO_OLD=1 turns off the flushes of the pass, and nothing
//     else: the chunk clock is still the capped one. The store as it was before the
//     fix also needs the clock to charge the whole opening (workStart = begin), which
//     only an edit of chunk() gives;
//   - TOPOSHIFT_RETAIN_CHUNK_REPRO_LIMIT is how long a pass may take before the repro
//     stops waiting and reports it as stalled (60s by default).
func TestRetainChunkRepro(t *testing.T) {
	if os.Getenv("TOPOSHIFT_RETAIN_CHUNK_REPRO") != "1" {
		t.Skip("set TOPOSHIFT_RETAIN_CHUNK_REPRO=1 to run the repro")
	}
	pods := reproInt(t, "TOPOSHIFT_RETAIN_CHUNK_REPRO_PODS", 75_000)
	limit := 60 * time.Second
	if v := os.Getenv("TOPOSHIFT_RETAIN_CHUNK_REPRO_LIMIT"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			t.Fatal(err)
		}
		limit = d
	}
	old := os.Getenv("TOPOSHIFT_RETAIN_CHUNK_REPRO_OLD") == "1"
	which := os.Getenv("TOPOSHIFT_RETAIN_CHUNK_REPRO_CASE")
	for _, resumed := range []bool{false, true} {
		name := map[bool]string{false: "fresh", true: "resumed"}[resumed]
		if which != "" && which != "both" && which != name {
			continue
		}
		t.Run(name, func(t *testing.T) { reproPass(t, pods, resumed, old, limit) })
	}
}

func reproInt(t *testing.T, name string, def int) int {
	t.Helper()
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		t.Fatalf("%s=%q is not a positive number", name, v)
	}
	return n
}

func reproOptions(rec Recorder, old bool) Options {
	o := DefaultOptions()
	o.Retention = Background
	o.Recorder = rec
	if old {
		o.retainFlushDels = -1
	}
	return o
}

func reproPass(t *testing.T, pods int, resumed, old bool, limit time.Duration) {
	dir := t.TempDir()
	o := reproOptions(nil, old)
	stopped := make(chan struct{})
	if resumed {
		var once bool
		o.afterRetainCommit = func() {
			if !once {
				once = true
				close(stopped)
			}
		}
	}
	s, err := Open(dir, o)
	if err != nil {
		t.Fatal(err)
	}
	began := time.Now()
	var batch []store.Record
	for i := range pods {
		batch = append(batch, edgeAt(catalog.L0, boundPod(i), boundNode(i), asyncAt(10), uint64(i)+1))
		if len(batch) == 5000 || i == pods-1 {
			if err := s.Write(bg, batch); err != nil {
				t.Fatal(err)
			}
			batch = nil
		}
	}
	t.Logf("%d pods written in %s", pods, time.Since(began).Round(time.Millisecond))
	rec := newProbeRecorder()
	var start time.Time // the start of the pass this process runs to its end
	if resumed {
		if err := s.Retain(bg, asyncAt(30)); err != nil {
			t.Fatal(err)
		}
		<-stopped
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		if s, err = Open(dir, reproOptions(rec, old)); err != nil {
			t.Fatal(err)
		}
	} else {
		_ = s.Close()
		if s, err = Open(dir, reproOptions(rec, old)); err != nil {
			t.Fatal(err)
		}
		start = time.Now()
		if err := s.Retain(bg, asyncAt(30)); err != nil {
			t.Fatal(err)
		}
	}
	defer func() { _ = s.Close() }()
	ctx, cancel := context.WithTimeout(bg, limit)
	defer cancel()
	werr := s.Instrument().WaitRetained(ctx)
	took := time.Since(start)
	prefixes := rec.sampled("retain.chunk_prefixes")
	opens := rec.sampled("retain.chunk_open_ns")
	holds := rec.sampled("retain.chunk_hold_ns")
	n := min(len(prefixes), len(opens), len(holds))
	if werr == nil && n > 1 {
		n-- // the last chunk of the pass is as short as what was left
	}
	if os.Getenv("TOPOSHIFT_RETAIN_CHUNK_REPRO_VERBOSE") == "1" {
		t.Logf("prefixes %v", prefixes)
		t.Logf("opens %v", opens)
		t.Logf("holds %v", holds)
		t.Logf("unflushed %v", rec.sampled("retain.unflushed_range_deletes"))
	}
	var done int64
	for _, p := range prefixes {
		done += p
	}
	state := "finished"
	if werr != nil {
		state = "stalled (" + werr.Error() + ")"
	}
	t.Logf("%s after %s: %d chunks, %d prefixes rewritten, %d flushes; chunk_prefixes min %d p50 %d; chunk_open_ns p50 %s p99 %s; chunk_hold_ns p99 %s (the last chunk left out)",
		state, took.Round(time.Millisecond), len(prefixes), done, rec.count("retain.flushes"),
		reproQ(prefixes[:n], 0), reproQ(prefixes[:n], 0.5),
		time.Duration(reproQ(opens[:n], 0.5)), time.Duration(reproQ(opens[:n], 0.99)), time.Duration(reproQ(holds[:n], 0.99)))
}

// reproQ is the q-quantile of the values, by rank (q = 0 is the least), or 0 for
// none.
func reproQ(vs []int64, q float64) int64 {
	if len(vs) == 0 {
		return 0
	}
	s := slices.Clone(vs)
	slices.Sort(s)
	return s[min(int(q*float64(len(s))), len(s)-1)]
}
