package xtnode

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// The registered settle protocol: a quiet window of two minutes, in a wait of at most half
// an hour.
const (
	RegisteredQuiet = 120 * time.Second
	RegisteredMax   = 30 * time.Minute
)

// The names of the node's metrics that the protocol reads (Prometheus text, on the health
// port's /metrics).
const (
	MetricLag           = "node_tx_lag_MsgId"
	MetricJobsAvailable = "compactor_jobs_available"
	MetricJobsDone      = "compactor_job_timer_seconds_count"
	MetricProcessed     = "node_tx_latestProcessedMsgId"
	MetricSubmitted     = "node_tx_latestSubmittedMsgId"
)

// ErrIngestionStopped is wrapped by the error a probe returns when the node says its
// ingestion has stopped: an indexer that failed on a statement. Nothing the node is given
// afterwards is indexed, so no measurement may go on.
var ErrIngestionStopped = errors.New("xtnode: the node's ingestion has stopped")

// Sample is what the node reported at one poll.
type Sample struct {
	// Lag is the node's log messages not yet processed; BlockLag the blocks of its storage
	// the node has not caught up with.
	Lag, BlockLag float64
	// JobsAvailable are the compaction jobs the compactor sees; JobsDone the jobs it has
	// finished.
	JobsAvailable, JobsDone float64
	// Submitted and Processed are the newest message id appended to the node's log and the
	// newest the node has processed.
	Submitted, Processed float64
}

// Quiet reports whether the sample has no work in it: nothing to index, nothing to
// compact.
func (s Sample) Quiet() bool { return s.quiet(0) }

func (s Sample) quiet(m mutation) bool {
	if m == mutIgnoreJobs {
		return s.Lag == 0 && s.BlockLag == 0
	}
	return s.Lag == 0 && s.BlockLag == 0 && s.JobsAvailable == 0
}

// Prober reads a sample of the node.
type Prober interface {
	Sample(ctx context.Context) (Sample, error)
}

// ParseMetrics reads Prometheus text and returns the value of each metric, summed over the
// labels it has (the node has one label set per metric). Comment lines are skipped; a line
// that is not a sample is an error.
func ParseMetrics(r io.Reader) (map[string]float64, error) {
	out := make(map[string]float64)
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		var name string
		if i := strings.IndexByte(line, '{'); i >= 0 {
			j := strings.LastIndexByte(line, '}')
			if j < i {
				return nil, fmt.Errorf("xtnode: metrics line %q has an open brace", line)
			}
			name, line = line[:i], strings.TrimSpace(line[j+1:])
		} else {
			f := strings.Fields(line)
			if len(f) < 2 {
				return nil, fmt.Errorf("xtnode: metrics line %q is not a sample", line)
			}
			name, line = f[0], f[1]
		}
		f := strings.Fields(line)
		if len(f) == 0 {
			return nil, fmt.Errorf("xtnode: metrics line for %s has no value", name)
		}
		v, err := strconv.ParseFloat(f[0], 64)
		if err != nil {
			return nil, fmt.Errorf("xtnode: metric %s: %w", name, err)
		}
		out[name] += v
	}
	return out, sc.Err()
}

// SampleOf reads the metrics a sample needs from parsed metrics. A metric the node does
// not report is an error, not a zero: a node that has renamed one would otherwise settle
// at once.
func SampleOf(m map[string]float64, blockLag float64) (Sample, error) {
	var s Sample
	s.BlockLag = blockLag
	for _, f := range []struct {
		name string
		to   *float64
	}{{MetricLag, &s.Lag}, {MetricJobsAvailable, &s.JobsAvailable}, {MetricJobsDone, &s.JobsDone}, {MetricProcessed, &s.Processed}, {MetricSubmitted, &s.Submitted}} {
		v, ok := m[f.name]
		if !ok {
			return Sample{}, fmt.Errorf("xtnode: the node reports no %s", f.name)
		}
		*f.to = v
	}
	return s, nil
}

// HTTPProber reads the node's health port.
type HTTPProber struct {
	// Base is the address of the health port, such as http://127.0.0.1:8080.
	Base   string
	Client *http.Client
}

func (p HTTPProber) client() *http.Client {
	if p.Client != nil {
		return p.Client
	}
	return &http.Client{Timeout: 15 * time.Second}
}

func (p HTTPProber) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(p.Base, "/")+path, nil)
	if err != nil {
		return nil, err
	}
	return p.client().Do(req)
}

// Sample implements [Prober].
func (p HTTPProber) Sample(ctx context.Context) (Sample, error) {
	alive, err := p.get(ctx, "/healthz/alive")
	if err != nil {
		return Sample{}, fmt.Errorf("xtnode: %w", err)
	}
	body, _ := io.ReadAll(io.LimitReader(alive.Body, 4096))
	_ = alive.Body.Close()
	if alive.StatusCode == http.StatusServiceUnavailable && strings.Contains(string(body), "Ingestion error") {
		return Sample{}, fmt.Errorf("%w: %s", ErrIngestionStopped, strings.TrimSpace(string(body)))
	}
	// A 503 for a block lag over the node's own threshold is a node that has work to do, not
	// a failure: the header says how much.
	var blockLag float64
	if h := alive.Header.Get("X-XTDB-Block-Lag"); h != "" {
		if blockLag, err = strconv.ParseFloat(h, 64); err != nil {
			return Sample{}, fmt.Errorf("xtnode: X-XTDB-Block-Lag %q: %w", h, err)
		}
	} else if alive.StatusCode != http.StatusOK {
		return Sample{}, fmt.Errorf("xtnode: /healthz/alive answered %d: %s", alive.StatusCode, strings.TrimSpace(string(body)))
	}
	res, err := p.get(ctx, "/metrics")
	if err != nil {
		return Sample{}, fmt.Errorf("xtnode: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return Sample{}, fmt.Errorf("xtnode: /metrics answered %d", res.StatusCode)
	}
	m, err := ParseMetrics(res.Body)
	if err != nil {
		return Sample{}, err
	}
	return SampleOf(m, blockLag)
}

// Admin is a node's health port: what the settle protocol reads, and the one thing it asks.
type Admin interface {
	Prober
	// FinishBlock asks the node to write the block it is filling to its storage.
	FinishBlock(ctx context.Context) error
}

// FinishBlock implements [Admin]: POST /system/finish-block, which appends a flush-block
// message to the node's log. The route is POST only (a GET is answered 405) and answers 200 with
// "Block flush message sent successfully."; the message id it puts in the X-XTDB-Message-Id
// header of XTDB 2.1.0 is the text of a Java object and is not read.
func (p HTTPProber) FinishBlock(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimSuffix(p.Base, "/")+"/system/finish-block", nil)
	if err != nil {
		return err
	}
	res, err := p.client().Do(req)
	if err != nil {
		return fmt.Errorf("xtnode: %w", err)
	}
	defer res.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("xtnode: finish-block answered %d: %s", res.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// Config is the settle protocol's constants and its clock.
type Config struct {
	// Quiet is how long the node must be quiet, in a row; Max is how long Settle waits at
	// most; Poll is the time between samples.
	Quiet, Max, Poll time.Duration
	// Now and Sleep are the clock. Zero values are the system's. Sleep returns early with
	// the context's error if it ends.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) error
	// Failures is how many failed samples in a row are tolerated (each begins a new window);
	// the next is the error of the settle. The default is 3.
	Failures int

	// mut is a deliberate fault, for the tests.
	mut mutation
}

// mutation names a deliberate fault. The tests switch one on to prove that the checks catch
// it; nothing outside the tests sets one, and the zero value is the correct behaviour.
type mutation uint8

const (
	_ mutation = iota
	// mutIgnoreJobs does not count pending compaction jobs as work.
	mutIgnoreJobs
	// mutIgnoreJobsDone does not begin a new window when the count of finished jobs moved.
	mutIgnoreJobsDone
	// mutKeepWindow does not end a window at a sample that has work in it.
	mutKeepWindow
	// mutNoCap waits for ever.
	mutNoCap
	// mutRetryIngestionStopped treats a stopped ingestion as a failed sample.
	mutRetryIngestionStopped
	// mutNoFinish does not ask the node to finish its block.
	mutNoFinish
	// mutNoWaitProcessed begins the quiet window without waiting for the flush message to
	// be processed.
	mutNoWaitProcessed
)

// Registered is the registered protocol.
func Registered() Config {
	return Config{Quiet: RegisteredQuiet, Max: RegisteredMax, Poll: 5 * time.Second}
}

// IsRegistered reports whether the window and the cap are the registered ones.
func (c Config) IsRegistered() bool { return c.Quiet == RegisteredQuiet && c.Max == RegisteredMax }

func (c Config) now() time.Time {
	if c.Now != nil {
		return c.Now()
	}
	return time.Now()
}

func (c Config) sleep(ctx context.Context, d time.Duration) error {
	if c.Sleep != nil {
		return c.Sleep(ctx, d)
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Result is how a wait ended.
type Result struct {
	// Settled is true if the node was quiet for a whole window within the cap.
	Settled bool
	// BlockFinished is true if the node was asked to finish its block and has processed the
	// request (see [FinishAndSettle]); a node that only settled has it false.
	BlockFinished bool
	// Reason says, for a node that did not settle, what it was still doing at the last
	// sample.
	Reason string
}

// Settle polls the node until it has been quiet for a whole window, or the cap has passed.
// It returns an error, and no verdict, if the node cannot be read more times in a row than
// the configuration tolerates, or says its ingestion has stopped.
func Settle(ctx context.Context, p Prober, cfg Config) (Result, error) {
	if cfg.Quiet <= 0 || cfg.Max <= 0 || cfg.Poll <= 0 {
		return Result{}, errors.New("xtnode: the settle protocol needs a window, a cap and a poll interval")
	}
	tolerate := cfg.Failures
	if tolerate <= 0 {
		tolerate = 3
	}
	start := cfg.now()
	var (
		windowStart time.Time
		inWindow    bool
		baseline    float64
		failed      int
		last        Sample
		haveLast    bool
	)
	for {
		s, err := p.Sample(ctx)
		now := cfg.now()
		switch {
		case err == nil:
			failed = 0
		case errors.Is(err, ErrIngestionStopped) && cfg.mut != mutRetryIngestionStopped || ctx.Err() != nil:
			return Result{}, err
		default:
			if failed++; failed > tolerate {
				return Result{}, fmt.Errorf("xtnode: the node could not be read %d times in a row: %w", failed, err)
			}
			inWindow = false
		}
		if err == nil {
			last, haveLast = s, true
			if !s.quiet(cfg.mut) {
				inWindow = inWindow && cfg.mut == mutKeepWindow
			} else if !inWindow || s.JobsDone != baseline && cfg.mut != mutIgnoreJobsDone {
				// A quiet sample begins a window, and a count of finished jobs that moved
				// means a job ran since the window began: the window begins again here.
				inWindow, windowStart, baseline = true, now, s.JobsDone
			}
			if inWindow && now.Sub(windowStart) >= cfg.Quiet {
				return Result{Settled: true}, nil
			}
		}
		if now.Sub(start) >= cfg.Max && cfg.mut != mutNoCap {
			return Result{Reason: reason(last, haveLast)}, nil
		}
		if err := cfg.sleep(ctx, cfg.Poll); err != nil {
			return Result{}, err
		}
	}
}

// FinishAndSettle is [Settle] for a node that has been written to and will be measured: it
// first asks the node to finish the block it is filling, because what is in a block that is
// not finished is in the log and not in storage, so a size taken then misses a tail that varies
// with where the stream ended. It then waits until the node has processed the request (the
// newest message it has processed is at least the newest that had been appended when the
// request returned), and only then begins the quiet window. A node that does not process the
// request within the cap has not finished its block and has not settled. No size is to be
// taken from a result whose BlockFinished is false.
func FinishAndSettle(ctx context.Context, a Admin, cfg Config) (Result, error) {
	if cfg.Quiet <= 0 || cfg.Max <= 0 || cfg.Poll <= 0 {
		return Result{}, errors.New("xtnode: the settle protocol needs a window, a cap and a poll interval")
	}
	if cfg.mut != mutNoFinish {
		if err := a.FinishBlock(ctx); err != nil {
			return Result{}, err
		}
	}
	if cfg.mut != mutNoWaitProcessed {
		done, err := awaitProcessed(ctx, a, cfg)
		if err != nil {
			return Result{}, err
		}
		if !done {
			return Result{Reason: "the node did not process the request to finish its block"}, nil
		}
	}
	res, err := Settle(ctx, a, cfg)
	res.BlockFinished = true
	return res, err
}

// awaitProcessed waits until the node has processed every message that was in its log when the
// request to finish the block returned, or the cap passes.
func awaitProcessed(ctx context.Context, a Admin, cfg Config) (bool, error) {
	tolerate := cfg.Failures
	if tolerate <= 0 {
		tolerate = 3
	}
	start := cfg.now()
	var (
		target float64
		have   bool
		failed int
	)
	for {
		s, err := a.Sample(ctx)
		switch {
		case err == nil:
			failed = 0
			if !have {
				// The request has returned, so its message is in the log: nothing appended
				// before now is newer than what has to be processed.
				target, have = s.Submitted, true
			}
			if s.Processed >= target {
				return true, nil
			}
		case errors.Is(err, ErrIngestionStopped) || ctx.Err() != nil:
			return false, err
		default:
			if failed++; failed > tolerate {
				return false, fmt.Errorf("xtnode: the node could not be read %d times in a row: %w", failed, err)
			}
		}
		if cfg.now().Sub(start) >= cfg.Max {
			return false, nil
		}
		if err := cfg.sleep(ctx, cfg.Poll); err != nil {
			return false, err
		}
	}
}

func reason(s Sample, have bool) string {
	if !have {
		return "the node was never read"
	}
	var r []string
	if s.Lag != 0 {
		r = append(r, fmt.Sprintf("%.0f log messages not yet processed", s.Lag))
	}
	if s.BlockLag != 0 {
		r = append(r, fmt.Sprintf("%.0f blocks behind", s.BlockLag))
	}
	if s.JobsAvailable != 0 {
		r = append(r, fmt.Sprintf("%.0f compaction jobs available", s.JobsAvailable))
	}
	if len(r) == 0 {
		return "compaction jobs finished within the last window"
	}
	return strings.Join(r, ", ")
}
