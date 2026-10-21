package runner

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The phase log of a build with [BuildOptions.PhaseLog] is PhaseLogFile in the
// candidate's directory: one JSON object (a [PhaseRecord]) per batch the build
// wrote, in order, giving the batch's wall time on the build's clock and where the
// store says the time of its Write went: validation, the state reads, the
// invalidation, the record commit, the checkpoint build and the second commit.
//
// The store already samples those phases for every Write of a store that has a
// recorder ("write.phase_ns.*", which a build's recorder keeps as sums); this log
// reads the same samples per batch and adds nothing to the store. Nothing is read
// from a build that does not ask for the log: its recorder is not wrapped, no file
// is made and the manifest says nothing of it. The log is a diagnostic of where the
// time of a batch went, and its timings are local and decide nothing (see
// AGENTS.md); [AnalyzePhases] reads it.
const (
	PhaseLogFile = "phases.jsonl"
	// PhaseLogKey is the key of a manifest's Describe that says the build wrote the
	// phase log ("true"); a build that did not has no such key.
	PhaseLogKey = "phase_log"
)

// PhaseRecord is one batch of the phase log. Times are nanoseconds. StartNs is the
// instant the batch's Write began, since the build's clock epoch (the gate's, which
// the windows of the manifest's Gate are on), and WallNs how long the engine's Write
// took, which is what the gate counts.
//
// Validate to CkptCommit are the store's own samples of that Write. They are spans
// that do not overlap, so their sum is at most WallNs; the rest, [PhaseRecord.Other],
// is the wait for the store's lock (a chunk of a retention that holds it), the time
// to make the batch's keys, the release of the Write's states and the engine's
// conversion of the records: it is not sampled by the store, and the lock wait is
// not separable from it without a change to the store.
//
// CkptBuild is the whole span before the second commit: the decision of which
// prefixes are due, the reads that learn a prefix's checkpoints (which are state
// reads too) and the walks that build the checkpoints. Walk is the walks alone, from
// the store's counter "checkpoint.build_ns", so CkptBuild less Walk is the decision
// and those reads. The other counters of the Write are the number of checkpoints it
// wrote, the prefix states it loaded from the database (cold or not), the records
// its walks read and the iterators it opened. StateLoads also counts the states a
// retention's own goroutine loads while it rewrites a prefix in the background
// ("checkpoint.loads" is one counter), so in a background build it is an upper
// bound of the Write's loads in a batch that a window overlaps.
type PhaseRecord struct {
	Index   int   `json:"i"`
	StartNs int64 `json:"start_ns"`
	WallNs  int64 `json:"wall_ns"`
	Records int   `json:"records"`

	Validate     int64 `json:"validate_ns"`
	State        int64 `json:"state_ns"`
	Invalidate   int64 `json:"invalidate_ns"`
	RecordCommit int64 `json:"record_commit_ns"`
	CkptBuild    int64 `json:"ckpt_build_ns"`
	CkptCommit   int64 `json:"ckpt_commit_ns"`

	Walk          int64 `json:"walk_ns"`
	CkptWritten   int64 `json:"ckpt_written"`
	StateLoads    int64 `json:"state_loads"`
	WalkedRecords int64 `json:"walked_records"`
	Iterators     int64 `json:"iterators"`
}

// Sampled is the sum of the store's six sampled phases.
func (r PhaseRecord) Sampled() int64 {
	return r.Validate + r.State + r.Invalidate + r.RecordCommit + r.CkptBuild + r.CkptCommit
}

// Other is the part of the Write the store does not sample (see [PhaseRecord]); it
// is never negative, since the sampled spans lie inside the Write.
func (r PhaseRecord) Other() int64 { return max(r.WallNs-r.Sampled(), 0) }

// phaseRecorder passes everything to the recorder it wraps and keeps what the
// current Write samples and counts, for [phaseLog.batch] to take once the Write
// returns. Writes are serialized by the build, but a retention in the background
// samples and counts from its own goroutine under other names, so the mutex stays.
type phaseRecorder struct {
	engine.Recorder
	mu  sync.Mutex
	cur PhaseRecord
}

// samplePrefix begins the names of the store's per-Write phase samples.
const samplePrefix = "write.phase_ns."

func (p *phaseRecorder) Sample(name string, v int64) {
	p.mu.Lock()
	switch name {
	case samplePrefix + "validate":
		p.cur.Validate += v
	case samplePrefix + "state":
		p.cur.State += v
	case samplePrefix + "invalidate":
		p.cur.Invalidate += v
	case samplePrefix + "record_commit":
		p.cur.RecordCommit += v
	case samplePrefix + "ckpt_build":
		p.cur.CkptBuild += v
	case samplePrefix + "ckpt_commit":
		p.cur.CkptCommit += v
	}
	p.mu.Unlock()
	p.Recorder.Sample(name, v)
}

func (p *phaseRecorder) Count(name string, n int64) {
	p.mu.Lock()
	switch name {
	case "checkpoint.build_ns":
		p.cur.Walk += n
	case "checkpoint.written":
		p.cur.CkptWritten += n
	case "checkpoint.loads", "checkpoint.loads_cold", "writer_map.false_positive_reads":
		p.cur.StateLoads += n
	case "checkpoint.build_records_walked":
		p.cur.WalkedRecords += n
	case "write.iterators":
		p.cur.Iterators += n
	}
	p.mu.Unlock()
	p.Recorder.Count(name, n)
}

// take returns what the Write since the last take sampled and counted, and starts
// afresh.
func (p *phaseRecorder) take() PhaseRecord {
	p.mu.Lock()
	r := p.cur
	p.cur = PhaseRecord{}
	p.mu.Unlock()
	return r
}

// phaseLog writes the phase log of a build. A nil *phaseLog does nothing, so the
// build calls it unconditionally where it need not know whether the log is on.
type phaseLog struct {
	rec  *phaseRecorder
	f    *os.File
	w    *bufio.Writer
	enc  *json.Encoder
	n    int
	err  error
	done bool
}

// newPhaseLog opens the log in dir for a build whose recorder was wrapped as rec.
func newPhaseLog(dir string, rec *phaseRecorder) (*phaseLog, error) {
	f, err := os.Create(filepath.Join(dir, PhaseLogFile))
	if err != nil {
		return nil, err
	}
	w := bufio.NewWriterSize(f, 1<<20)
	return &phaseLog{rec: rec, f: f, w: w, enc: json.NewEncoder(w)}, nil
}

// discard drops what the recorder holds, so a first batch does not carry the open or
// the writes of a database opened before it.
func (l *phaseLog) discard() {
	if l != nil {
		l.rec.take()
	}
}

// batch logs the batch just written: it began at the build's offset startNs, took
// wall and had records records.
func (l *phaseLog) batch(startNs int64, wall time.Duration, records int) {
	if l == nil || l.done {
		return
	}
	r := l.rec.take()
	r.Index, r.StartNs, r.WallNs, r.Records = l.n, startNs, wall.Nanoseconds(), records
	l.n++
	if err := l.enc.Encode(&r); err != nil && l.err == nil {
		l.err = err
	}
}

// close flushes and closes the log; it is safe to call twice.
func (l *phaseLog) close() error {
	if l == nil || l.done {
		return nil
	}
	l.done = true
	err := l.err
	if e := l.w.Flush(); err == nil {
		err = e
	}
	if e := l.f.Close(); err == nil {
		err = e
	}
	return err
}

// checkPhaseLog refuses the option for a build that has no gate clock to place its
// batches on: one not through the root store (the store's phases are the root
// store's).
func checkPhaseLog(v candidates.Variant, opts BuildOptions) error {
	if opts.PhaseLog && !v.Root() {
		return fmt.Errorf("runner: %s is not built through the root store: only a build through the root store has the store's write phases to log", v.Name)
	}
	return nil
}

// LoadPhaseLog reads the phase log of the build in dir.
func LoadPhaseLog(dir string) ([]PhaseRecord, error) {
	f, err := os.Open(filepath.Join(dir, PhaseLogFile))
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	var out []PhaseRecord
	dec := json.NewDecoder(bufio.NewReaderSize(f, 1<<20))
	for dec.More() {
		var r PhaseRecord
		if err := dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("runner: %s: %w", PhaseLogFile, err)
		}
		out = append(out, r)
	}
	return out, nil
}
