package runner

import (
	"encoding/json"
	"fmt"
	"os"
	rtmetrics "runtime/metrics"
	"sync"
	"sync/atomic"
	"time"
)

// The phases of a build, as a line of its metrics file names them.
const (
	phaseWrite int32 = iota
	phaseRetain
	phaseRest
	phaseUncompacted
	phaseCompact
	phaseCanonical
	phaseDone
)

var phaseNames = [...]string{
	phaseWrite: "write", phaseRetain: "retain", phaseRest: "rest", phaseUncompacted: "uncompacted",
	phaseCompact: "compact", phaseCanonical: "canonical", phaseDone: "done",
}

// buildProgress is what a build tells its sampler: the phase it is in and how many
// batches and retentions it has finished. Every method accepts a nil receiver, so a
// sink made without one reports nothing.
type buildProgress struct {
	phase               atomic.Int32
	batches, retentions atomic.Int64
	// onChange, if set before the build starts, is called after each change of phase,
	// by the goroutine that made it: the sampler's line for the change.
	onChange func()
}

func newBuildProgress() *buildProgress { return &buildProgress{} }

// setPhase records the phase, and has a line written for it.
func (p *buildProgress) setPhase(ph int32) {
	if p == nil {
		return
	}
	p.phase.Store(ph)
	if p.onChange != nil {
		p.onChange()
	}
}

func (p *buildProgress) batch() {
	if p != nil {
		p.batches.Add(1)
	}
}

func (p *buildProgress) retention() {
	if p != nil {
		p.retentions.Add(1)
	}
}

// metricLine is one line of a metrics file.
type metricLine struct {
	ElapsedMS  int64            `json:"elapsed_ms"`
	Phase      string           `json:"phase"`
	Batches    int64            `json:"batches"`
	Retentions int64            `json:"retentions"`
	Stats      map[string]int64 `json:"stats"`
	Go         goMemory         `json:"go"`
}

// goMemory is the Go runtime's memory, read without stopping the world.
type goMemory struct {
	Total        int64 `json:"total"`
	HeapObjects  int64 `json:"heap_objects"`
	HeapReleased int64 `json:"heap_released"`
	HeapGoal     int64 `json:"heap_goal"`
	// HeapLive is the heap found live by the last collection that finished, the figure a
	// store's memory is judged by; it is 0 before the first collection and for a runtime
	// that does not report it.
	HeapLive int64 `json:"heap_live"`
}

// goMemoryNow reads the runtime's memory from runtime/metrics: unlike
// runtime.ReadMemStats it does not stop the world, which would show in the timings
// of the build it measures.
func goMemoryNow() goMemory {
	s := []rtmetrics.Sample{
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/gc/heap/goal:bytes"},
		{Name: "/gc/heap/live:bytes"},
	}
	rtmetrics.Read(s)
	v := func(i int) int64 {
		if s[i].Value.Kind() != rtmetrics.KindUint64 {
			return 0
		}
		return int64(s[i].Value.Uint64())
	}
	return goMemory{Total: v(0), HeapObjects: v(1), HeapReleased: v(2), HeapGoal: v(3), HeapLive: v(4)}
}

// sampler appends a line of the database's statistics and the runtime's memory to a
// file when it starts, every interval, at each change of phase, and when it stops.
// Each line is written whole as it is made, so a build that is killed keeps every
// line so far. The first error writing is kept and returned by [sampler.stop].
type sampler struct {
	every time.Duration
	prog  *buildProgress
	stats func() map[string]int64
	start time.Time

	stopCh chan struct{}
	done   chan struct{}
	once   sync.Once

	// mu serializes the lines (the ticker's and those of a change of phase), so that
	// they are written in the order of their times, and guards what follows.
	mu     sync.Mutex
	f      *os.File
	closed bool
	err    error
}

// openMetricsFile creates the file of a sampler, which must not exist. A test replaces
// it with one that cannot be written, to see what a build does about a file that fails.
var openMetricsFile = func(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
}

// startSampler opens the file (which must not exist) and starts sampling every
// interval. The caller stops it before the database that stats reads is closed.
func startSampler(path string, every time.Duration, prog *buildProgress, stats func() map[string]int64) (*sampler, error) {
	f, err := openMetricsFile(path)
	if err != nil {
		return nil, err
	}
	s := &sampler{f: f, every: every, prog: prog, stats: stats, start: time.Now(), stopCh: make(chan struct{}), done: make(chan struct{})}
	s.line()
	prog.onChange = s.line
	go s.run()
	return s, nil
}

func (s *sampler) run() {
	defer close(s.done)
	tick := time.NewTicker(s.every)
	defer tick.Stop()
	for {
		select {
		case <-s.stopCh:
			return
		case <-tick.C:
		}
		s.line()
	}
}

// line writes one line, keeping the first error; after stop it writes nothing.
func (s *sampler) line() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writeLocked()
}

func (s *sampler) writeLocked() {
	if s.closed {
		return
	}
	l := metricLine{
		ElapsedMS: time.Since(s.start).Milliseconds(),
		Phase:     phaseNames[s.prog.phase.Load()],
		Batches:   s.prog.batches.Load(), Retentions: s.prog.retentions.Load(),
		Stats: s.stats(), Go: goMemoryNow(),
	}
	b, err := json.Marshal(l)
	if err == nil {
		_, err = s.f.Write(append(b, '\n'))
	}
	if err != nil && s.err == nil {
		s.err = err
	}
}

// stop ends the sampling, waits for a line in progress, writes the last line, closes
// the file, and returns the first error of all that. It is safe to call twice, and
// on a nil sampler; the later calls return the same error. The database must still
// be open: stop reads its statistics.
func (s *sampler) stop() error {
	if s == nil {
		return nil
	}
	s.once.Do(func() {
		close(s.stopCh)
		<-s.done
		s.mu.Lock()
		defer s.mu.Unlock()
		s.writeLocked()
		s.closed = true
		if err := s.f.Close(); err != nil && s.err == nil {
			s.err = err
		}
	})
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// failure stops the sampler and returns its first error, naming the candidate.
func (s *sampler) failure(name string) error {
	if err := s.stop(); err != nil {
		return fmt.Errorf("runner: %s: writing %s: %w", name, MetricsFile, err)
	}
	return nil
}

// describedOr is what a manifest's description says of key, or def for a manifest
// that does not have it (one made before the build recorded it).
func describedOr(m *Manifest, key, def string) string {
	if v := m.Describe[key]; v != "" {
		return v
	}
	return def
}
