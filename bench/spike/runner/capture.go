package runner

import (
	"maps"
	"strings"
	"sync"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// Capture is an [engine.Recorder] that adds up everything it is given by name:
// a count adds its number, a sample adds its value, so a read that makes several
// iterators (a batch) reports their sum. [Capture.Take] hands over what has
// been added since the last call, which is how one query's counters are read off
// a recorder shared by all of them.
type Capture struct {
	mu   sync.Mutex
	sums map[string]int64
	all  map[string]int64
}

var _ engine.Recorder = (*Capture)(nil)

// NewCapture returns an empty recorder.
func NewCapture() *Capture {
	return &Capture{sums: map[string]int64{}, all: map[string]int64{}}
}

// Count implements [engine.Recorder].
func (c *Capture) Count(name string, n int64) { c.add(name, n) }

// Sample implements [engine.Recorder].
func (c *Capture) Sample(name string, v int64) { c.add(name, v) }

func (c *Capture) add(name string, n int64) {
	c.mu.Lock()
	c.sums[name] += n
	c.all[name] += n
	c.mu.Unlock()
}

// Take returns what has been added since the last Take and starts again.
func (c *Capture) Take() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.sums
	c.sums = map[string]int64{}
	return out
}

// Totals returns everything added since the beginning.
func (c *Capture) Totals() map[string]int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.all)
}

// volatile says whether a counter may differ between two passes over the same
// queries on the same database, so that a difference in it is not a sign that
// something else changed. Three are: the bytes served from the block cache, which
// depend on what the first pass left in it; the time spent fetching blocks; and,
// for the batched reads, the number of points Pebble reports with a value in a
// value block, which has been seen to differ by a few from one pass to the next
// for layout M (an observed fact, not explained; the steps, points and block
// bytes of the same reads are identical). It is narrowed to that read: the same
// counter drifting anywhere else is a difference.
func volatile(name string) bool {
	return strings.HasSuffix(name, ".block_bytes_cached") || strings.HasSuffix(name, ".block_read_ns") ||
		name == "read.batch.separated_values"
}
