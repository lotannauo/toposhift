package runner

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// A sampler keeps the first error of writing its file in the line that failed, and
// stop returns it, and returns the same one again; one whose file was written without
// trouble returns nil, as does one that was never started.
func TestASamplerKeepsTheFirstErrorOfItsFile(t *testing.T) {
	t.Parallel()

	stats := func() map[string]int64 { return map[string]int64{"live_table_bytes": 1} }

	var none *sampler
	if err := none.stop(); err != nil {
		t.Errorf("stop of no sampler: %v", err)
	}

	ok, err := startSampler(filepath.Join(t.TempDir(), MetricsFile), time.Hour, newBuildProgress(), stats)
	if err != nil {
		t.Fatal(err)
	}
	if err := ok.stop(); err != nil {
		t.Errorf("stop of a sampler whose file was written: %v", err)
	}
	if err := ok.stop(); err != nil {
		t.Errorf("a second stop: %v", err)
	}

	s, err := startSampler(filepath.Join(t.TempDir(), MetricsFile), time.Hour, newBuildProgress(), stats)
	if err != nil {
		t.Fatal(err)
	}
	// The interval is an hour, so nothing else touches the file while it is closed
	// underneath the sampler: the next line cannot be written.
	if err := s.f.Close(); err != nil {
		t.Fatal(err)
	}
	s.line()
	s.mu.Lock()
	kept := s.err
	s.mu.Unlock()
	if !errors.Is(kept, os.ErrClosed) {
		t.Fatalf("the error of the line that could not be written was not kept: %v", kept)
	}
	first := s.stop()
	if !errors.Is(first, os.ErrClosed) {
		t.Errorf("stop returned %v, want the error of the file", first)
	}
	if second := s.stop(); !errors.Is(second, first) {
		t.Errorf("a second stop returned %v, want the first's %v", second, first)
	}
}

// A build whose metrics could not be written fails, naming the file, and writes no
// manifest: its timings were taken while the sampler failed, and nothing records that.
func TestABuildWhoseMetricsCannotBeWrittenWritesNoManifest(t *testing.T) {
	// not parallel: it replaces the way the file is opened, and the parallel tests wait
	// for the serial ones to finish
	conformance.SkipWhenTrimmed(t)

	old := openMetricsFile
	t.Cleanup(func() { openMetricsFile = old })
	openMetricsFile = func(path string) (*os.File, error) {
		f, err := old(path)
		if err != nil {
			return nil, err
		}
		return f, f.Close() // closed, so that every write to it fails
	}

	spec := DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.Retentions = []Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	spec.MinNonEmpty = 0
	plan, err := MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	v, err := candidates.Lookup("L/off")
	if err != nil {
		t.Fatal(err)
	}
	clean := Guards{Info: BuildInfo{GoVersion: "test", GOOS: "test", GOARCH: "test", Revision: "r"}}
	dir := CandidateDir(t.TempDir(), v.Name)
	_, err = BuildWith(context.Background(), plan, v, dir, clean, BuildOptions{MetricsEvery: time.Second}, nil)
	if err == nil || !strings.Contains(err.Error(), MetricsFile) || !errors.Is(err, os.ErrClosed) {
		t.Errorf("a build whose metrics file cannot be written: %v, want an error naming %s", err, MetricsFile)
	}
	if _, err := os.Stat(filepath.Join(dir, ManifestFile)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the build wrote a manifest (%v)", err)
	}
}
