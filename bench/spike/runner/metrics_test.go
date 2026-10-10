package runner_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// metricsLine is one line of a metrics file, decoded strictly: a field the build
// does not write, or one it has dropped, is found.
type metricsLine struct {
	ElapsedMS  *int64           `json:"elapsed_ms"`
	Phase      *string          `json:"phase"`
	Batches    *int64           `json:"batches"`
	Retentions *int64           `json:"retentions"`
	Stats      map[string]int64 `json:"stats"`
	Go         map[string]int64 `json:"go"`
}

func readMetrics(t *testing.T, path string) []metricsLine {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var lines []metricsLine
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for n := 1; sc.Scan(); n++ {
		var l metricsLine
		dec := json.NewDecoder(strings.NewReader(sc.Text()))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&l); err != nil {
			t.Fatalf("%s line %d: %v: %s", path, n, err, sc.Text())
		}
		lines = append(lines, l)
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return lines
}

// buildAndSample builds L/off on the tiny plan with the options, sampling every 5 ms,
// and checks the metrics file it wrote: the lines have the fields the file promises,
// the time and the batches never go back, and the phases are those wanted, in order:
// the writes, then for each retention of the stream the phases in perRetention and the
// writes again, then those in after.
func buildAndSample(t *testing.T, opts runner.BuildOptions, perRetention, after []string) {
	t.Helper()
	plan := mustPlan(t, tinySpec())
	v := lookup(t, "L/off")

	dir := runner.CandidateDir(t.TempDir(), v.Name)
	opts.MetricsEvery = 5 * time.Millisecond
	m, err := runner.BuildWith(context.Background(), plan, v, dir, clean, opts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Describe[runner.MetricsKey]; got != "5ms" {
		t.Errorf("manifest %s is %q, want \"5ms\"", runner.MetricsKey, got)
	}
	if loaded, err := runner.LoadManifest(dir); err != nil || loaded.Describe[runner.MetricsKey] != "5ms" {
		t.Errorf("the manifest on disk: %v, %v, want %s \"5ms\"", loaded, err, runner.MetricsKey)
	}
	lines := readMetrics(t, filepath.Join(dir, runner.MetricsFile))
	if len(lines) < 3 {
		t.Fatalf("%d lines, want at least the first, one per phase and the last", len(lines))
	}
	var phases []string
	var last int64 = -1
	var lastBatches int64
	for i, l := range lines {
		if l.ElapsedMS == nil || l.Phase == nil || l.Batches == nil || l.Retentions == nil || l.Stats == nil || l.Go == nil {
			t.Fatalf("line %d lacks a field: %+v", i+1, l)
		}
		if *l.ElapsedMS < last {
			t.Errorf("line %d: elapsed_ms %d after %d: it went back", i+1, *l.ElapsedMS, last)
		}
		last = *l.ElapsedMS
		if *l.Batches < lastBatches {
			t.Errorf("line %d: batches %d after %d: it went back", i+1, *l.Batches, lastBatches)
		}
		lastBatches = *l.Batches
		for _, k := range []string{"live_table_bytes", "compaction_debt", "memtable_count", "pending_stats_tables"} {
			if _, ok := l.Stats[k]; !ok {
				t.Errorf("line %d: stats has no %q: %v", i+1, k, l.Stats)
			}
		}
		keys := make([]string, 0, len(l.Go))
		for k := range l.Go {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if want := []string{"heap_goal", "heap_live", "heap_objects", "heap_released", "total"}; !slices.Equal(keys, want) {
			t.Errorf("line %d: go has %v, want %v", i+1, keys, want)
		}
		if l.Go["total"] <= 0 {
			t.Errorf("line %d: go.total is %d, want above 0", i+1, l.Go["total"])
		}
		if len(phases) == 0 || phases[len(phases)-1] != *l.Phase {
			phases = append(phases, *l.Phase)
		}
	}
	// Written at each change of phase, not only when the timer fires: every phase of
	// this build is there, in order, and the last line is the "done" of the stop.
	wantPhases := []string{"write"}
	for range m.Stream.Retentions {
		wantPhases = append(wantPhases, perRetention...)
		wantPhases = append(wantPhases, "write")
	}
	wantPhases = append(wantPhases, after...)
	if !slices.Equal(phases, wantPhases) {
		t.Errorf("phases %v, want %v (%d retentions)", phases, wantPhases, len(m.Stream.Retentions))
	}
	if first := lines[0]; *first.Batches != 0 || *first.Retentions != 0 || *first.Phase != "write" {
		t.Errorf("the first line is %s after %d batches and %d retentions, want the start of the writes", *first.Phase, *first.Batches, *first.Retentions)
	}
	end := lines[len(lines)-1]
	if *end.Phase != "done" || *end.Batches == 0 || *end.Retentions != int64(len(m.Stream.Retentions)) || len(m.Stream.Retentions) == 0 {
		t.Errorf("the last line is %s after %d batches and %d retentions, want done after the stream's %d", *end.Phase, *end.Batches, *end.Retentions, len(m.Stream.Retentions))
	}
}

// A build told to sample its metrics writes, to a file in the candidate's
// directory, a line when it starts, one every interval, one at each change of phase
// and one when it stops, each with the fields the file promises; and records the
// interval in its manifest. It has a goroutine, so it runs in the race tier too: the
// statistics are read while the writer commits.
func TestABuildSamplesItsMetrics(t *testing.T) {
	t.Parallel()
	buildAndSample(t, runner.BuildOptions{}, []string{"retain"}, []string{"uncompacted", "compact", "done"})
}

// With a rest after each retention and the canonical layout, the phases of those are
// in the file as well. It waits for the database to be at rest, so the full tier runs it.
func TestTheSamplingNamesTheRestAndTheCanonicalRewrite(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)
	buildAndSample(t, runner.BuildOptions{RestAfterRetention: true, CanonicalLayout: true},
		[]string{"retain", "rest"}, []string{"uncompacted", "compact", "canonical", "done"})
}

// closeWatch is an engine that counts the statistics asked of it after it was
// closed: a sampler that outlives the database it samples.
type closeWatch struct {
	fullEngine
	closed   atomic.Bool
	staleMu  sync.Mutex
	stale    int
	failFrom int // the write that fails and every one after, counting from 1; 0 for none
	writes   int
}

func (c *closeWatch) Stats() map[string]int64 {
	if c.closed.Load() {
		c.staleMu.Lock()
		c.stale++
		c.staleMu.Unlock()
		return map[string]int64{}
	}
	return c.fullEngine.Stats()
}

func (c *closeWatch) Close() error {
	c.closed.Store(true)
	return c.fullEngine.Close()
}

func (c *closeWatch) Write(batch []engine.Record) error {
	c.writes++
	if c.failFrom > 0 && c.writes >= c.failFrom {
		return errors.New("injected write failure")
	}
	return c.fullEngine.Write(batch)
}

func (c *closeWatch) staleReads() int {
	c.staleMu.Lock()
	defer c.staleMu.Unlock()
	return c.stale
}

// The sampler is stopped, and its last line written, before the database is closed,
// whether the build ends well (the full tier) or fails part-way; the build that fails writes no
// manifest, and the file keeps the lines it had.
func TestTheSamplerStopsBeforeTheDatabaseCloses(t *testing.T) {
	t.Parallel()

	plan := mustPlan(t, tinySpec())
	for _, c := range []struct {
		name     string
		failFrom int
		wantErr  string
	}{
		{"a build that succeeds", 0, ""},
		{"a build whose third write fails", 3, "injected write failure"},
	} {
		if c.wantErr == "" && conformance.Trimmed() {
			continue // a whole build, slow under the race detector: the full tier runs it
		}
		var watch *closeWatch
		v := candidates.Wrap(lookup(t, "L/off"), "L/watch", func(e engine.Engine, o candidates.Options) (engine.Engine, error) {
			if o.ReadOnly {
				return e, nil
			}
			watch = &closeWatch{fullEngine: e.(fullEngine), failFrom: c.failFrom}
			return watch, nil
		})
		dir := runner.CandidateDir(t.TempDir(), v.Name)
		_, err := runner.BuildWith(context.Background(), plan, v, dir, clean, runner.BuildOptions{MetricsEvery: time.Millisecond}, nil)
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: %v, want %q", c.name, err, c.wantErr)
		}
		if n := watch.staleReads(); n != 0 {
			t.Errorf("%s: the statistics were read %d times after the database was closed", c.name, n)
		}
		lines := readMetrics(t, filepath.Join(dir, runner.MetricsFile))
		if len(lines) < 2 {
			t.Errorf("%s: %d lines, want at least the first and the last", c.name, len(lines))
		}
		_, merr := os.Stat(filepath.Join(dir, runner.ManifestFile))
		if (c.wantErr == "") != (merr == nil) {
			t.Errorf("%s: manifest (%v) after a build that returned %v", c.name, merr, err)
		}
	}
}
