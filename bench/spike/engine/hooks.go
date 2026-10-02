package engine

import (
	"slices"
	"sync"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// The optional parts of an engine. None is required to pass conformance; they
// exist so the measurements of the next stage can read what they need without
// changing an engine, and so the harness can reach the paths a small test never
// would.

// Settler is implemented by an engine that can be told to finish its deferred
// work: flush what is in memory to its files and wait for background
// compaction. The conformance test calls it between rounds of comparisons, so
// reads are checked against files as well as memory, and the benchmarks call it
// before measuring size.
type Settler interface {
	Settle() error
}

// Recorder receives what an engine counts and samples as it works: block bytes
// read, blocks skipped by a filter, how many records a read replayed, checkpoint
// hits and misses, versions stepped over, bytes written by kind. An engine is
// given one when it is built and calls it on its read and write paths, so the
// numbers are available without changing the engine; the default is
// [NopRecorder]. Names are the engine's own and are documented by it. A Recorder
// must be safe for concurrent use.
type Recorder interface {
	// Count adds n to the named counter.
	Count(name string, n int64)
	// Sample records one observation of the named quantity, for a distribution
	// (replay length per read, say).
	Sample(name string, v int64)
}

// NopRecorder discards everything.
type NopRecorder struct{}

// Count implements [Recorder].
func (NopRecorder) Count(string, int64) {}

// Sample implements [Recorder].
func (NopRecorder) Sample(string, int64) {}

// MemRecorder keeps everything it is given, for tests and for a benchmark that
// reads the result at the end. It is safe for concurrent use.
type MemRecorder struct {
	mu      sync.Mutex
	counts  map[string]int64
	samples map[string][]int64
}

var _ Recorder = (*MemRecorder)(nil)

// Count implements [Recorder].
func (m *MemRecorder) Count(name string, n int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.counts == nil {
		m.counts = make(map[string]int64)
	}
	m.counts[name] += n
}

// Sample implements [Recorder].
func (m *MemRecorder) Sample(name string, v int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.samples == nil {
		m.samples = make(map[string][]int64)
	}
	m.samples[name] = append(m.samples[name], v)
}

// Counter returns the named counter, zero if it was never counted.
func (m *MemRecorder) Counter(name string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts[name]
}

// Samples returns a copy of the observations of the named quantity, in the
// order they were made.
func (m *MemRecorder) Samples(name string) []int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.samples[name])
}

// Reset forgets everything, so a measurement can start from nothing after a
// warm-up.
func (m *MemRecorder) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.counts, m.samples = nil, nil
}

// LayerSizer is implemented by an engine that can say how many bytes it holds
// for each layer, so the cost of churn can be attributed to the layer that
// causes it.
type LayerSizer interface {
	SizeByLayer() (map[catalog.Layer]int64, error)
}

// Part is a share of what an engine stores: a layer, and a kind of record the
// engine defines and documents ("record", "checkpoint", "baseline", "version").
type Part struct {
	Layer catalog.Layer
	Kind  string
}

// Breakdowner is implemented by an engine that can say, by scanning what it
// holds, how many logical (uncompressed key and value) bytes each [Part] takes.
// It is a scan, not an estimate, so it is for measurement and not for use in a
// hot path.
type Breakdowner interface {
	Breakdown() (map[Part]int64, error)
}
