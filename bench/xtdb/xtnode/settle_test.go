package xtnode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeNode is a node whose samples are a function of the time since the wait began, on a
// clock the wait itself moves.
type fakeNode struct {
	t      time.Duration
	sample func(t time.Duration) (Sample, error)
	polls  int
}

func (f *fakeNode) Sample(context.Context) (Sample, error) {
	f.polls++
	return f.sample(f.t)
}

func (f *fakeNode) config(quiet, max time.Duration) Config {
	origin := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return Config{
		Quiet: quiet, Max: max, Poll: 5 * time.Second,
		Now: func() time.Time { return origin.Add(f.t) },
		Sleep: func(ctx context.Context, d time.Duration) error {
			f.t += d
			if f.t > 100*time.Hour {
				return errors.New("the test's clock ran away: the wait does not end")
			}
			return ctx.Err()
		},
	}
}

func idle(done float64) Sample { return Sample{JobsDone: done} }

func settle(t *testing.T, f *fakeNode, cfg Config) (Result, error) {
	t.Helper()
	return Settle(context.Background(), f, cfg)
}

func TestANodeThatIsQuietSettlesAfterExactlyTheWindow(t *testing.T) {
	f := &fakeNode{sample: func(time.Duration) (Sample, error) { return idle(3), nil }}
	r, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute))
	if err != nil || !r.Settled {
		t.Fatalf("%+v, %v", r, err)
	}
	if f.t != 2*time.Minute {
		t.Errorf("settled after %s, not after the window", f.t)
	}
}

func TestTheWindowBeginsWhenTheLastWorkEnds(t *testing.T) {
	cases := map[string]struct {
		sample func(t time.Duration) Sample
		at     time.Duration
	}{
		"indexing lag until 5 minutes": {func(t time.Duration) Sample {
			if t < 5*time.Minute {
				return Sample{Lag: 12}
			}
			return idle(0)
		}, 7 * time.Minute},
		"block lag until 3 minutes": {func(t time.Duration) Sample {
			if t < 3*time.Minute {
				return Sample{BlockLag: 2}
			}
			return idle(0)
		}, 5 * time.Minute},
		"compaction jobs pending until 200 s": {func(t time.Duration) Sample {
			if t < 200*time.Second {
				return Sample{JobsAvailable: 4}
			}
			return idle(0)
		}, 200*time.Second + 2*time.Minute},
		"a job finishing at 100 s (the count moves, none is pending)": {func(t time.Duration) Sample {
			if t >= 100*time.Second {
				return idle(5)
			}
			return idle(4)
		}, 100*time.Second + 2*time.Minute},
		"work again at 90 s, in the first window": {func(t time.Duration) Sample {
			if t == 90*time.Second {
				return Sample{Lag: 1}
			}
			return idle(0)
		}, 95*time.Second + 2*time.Minute},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			f := &fakeNode{sample: func(at time.Duration) (Sample, error) { return c.sample(at), nil }}
			r, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute))
			if err != nil || !r.Settled {
				t.Fatalf("%+v, %v", r, err)
			}
			if f.t != c.at {
				t.Errorf("settled at %s, want %s", f.t, c.at)
			}
		})
	}
}

func TestANodeThatStaysBusyDoesNotSettleWithinTheCap(t *testing.T) {
	f := &fakeNode{sample: func(time.Duration) (Sample, error) { return Sample{Lag: 7, JobsAvailable: 2}, nil }}
	r, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute))
	if err != nil || r.Settled {
		t.Fatalf("%+v, %v", r, err)
	}
	if f.t < 30*time.Minute || f.t > 31*time.Minute {
		t.Errorf("gave up at %s", f.t)
	}
	if !strings.Contains(r.Reason, "7 log messages") || !strings.Contains(r.Reason, "2 compaction jobs") {
		t.Errorf("reason %q", r.Reason)
	}
	// A node whose compaction never stops finishing jobs is told apart from a busy one.
	n := 0.0
	f = &fakeNode{sample: func(time.Duration) (Sample, error) { n++; return idle(n), nil }}
	if r, err := settle(t, f, f.config(2*time.Minute, 10*time.Minute)); err != nil || r.Settled || !strings.Contains(r.Reason, "finished") {
		t.Fatalf("a node that keeps finishing jobs: %+v, %v", r, err)
	}
}

func TestAStoppedIngestionIsAnErrorNotAWait(t *testing.T) {
	f := &fakeNode{sample: func(at time.Duration) (Sample, error) {
		if at >= time.Minute {
			return Sample{}, fmt.Errorf("%w: stopped", ErrIngestionStopped)
		}
		return Sample{Lag: 1}, nil
	}}
	if _, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute)); !errors.Is(err, ErrIngestionStopped) {
		t.Fatalf("%v", err)
	}
	if f.t > time.Minute+5*time.Second {
		t.Errorf("kept polling until %s", f.t)
	}
}

func TestASampleThatFailsBeginsANewWindowAndTooManyFailuresAreAnError(t *testing.T) {
	boom := errors.New("connection refused")
	f := &fakeNode{sample: func(at time.Duration) (Sample, error) {
		if at == 60*time.Second || at == 65*time.Second {
			return Sample{}, boom
		}
		return idle(0), nil
	}}
	r, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute))
	if err != nil || !r.Settled || f.t != 70*time.Second+2*time.Minute {
		t.Fatalf("%+v, %v, at %s", r, err, f.t)
	}
	f = &fakeNode{sample: func(time.Duration) (Sample, error) { return Sample{}, boom }}
	if _, err := settle(t, f, f.config(2*time.Minute, 30*time.Minute)); !errors.Is(err, boom) || f.polls != 4 {
		t.Fatalf("%v after %d polls", err, f.polls)
	}
}

func TestTheProtocolNeedsAWindowACapAndAPoll(t *testing.T) {
	f := &fakeNode{sample: func(time.Duration) (Sample, error) { return idle(0), nil }}
	for _, c := range []Config{{Max: time.Hour, Poll: time.Second}, {Quiet: time.Hour, Poll: time.Second}, {Quiet: time.Hour, Max: time.Hour}} {
		if _, err := Settle(context.Background(), f, c); err == nil {
			t.Errorf("%+v was accepted", c)
		}
	}
	if !Registered().IsRegistered() || (Config{Quiet: time.Minute, Max: RegisteredMax}).IsRegistered() || (Config{Quiet: RegisteredQuiet, Max: time.Hour}).IsRegistered() {
		t.Error("IsRegistered")
	}
	if RegisteredQuiet != 120*time.Second || RegisteredMax != 30*time.Minute {
		t.Error("the registered protocol is a 120 s window in at most 30 minutes")
	}
}

// TestSettleMutants: each fault the protocol could have, switched on, is caught by the
// checks above.
func TestSettleMutants(t *testing.T) {
	type check struct {
		name string
		run  func(t *testing.T, mut mutation) error
	}
	expect := func(f *fakeNode, mut mutation, quiet, max time.Duration, wantSettled bool, wantAt time.Duration) error {
		cfg := f.config(quiet, max)
		cfg.mut = mut
		r, err := Settle(context.Background(), f, cfg)
		switch {
		case err != nil:
			return err
		case r.Settled != wantSettled:
			return fmt.Errorf("settled %v at %s", r.Settled, f.t)
		case wantAt != 0 && f.t != wantAt:
			return fmt.Errorf("ended at %s, want %s", f.t, wantAt)
		}
		return nil
	}
	pending := func() *fakeNode {
		return &fakeNode{sample: func(at time.Duration) (Sample, error) {
			if at < 200*time.Second {
				return Sample{JobsAvailable: 4}, nil
			}
			return idle(0), nil
		}}
	}
	finishing := func() *fakeNode {
		return &fakeNode{sample: func(at time.Duration) (Sample, error) {
			if at >= 100*time.Second {
				return idle(5), nil
			}
			return idle(4), nil
		}}
	}
	blip := func() *fakeNode {
		return &fakeNode{sample: func(at time.Duration) (Sample, error) {
			if at == 90*time.Second {
				return Sample{Lag: 1}, nil
			}
			return idle(0), nil
		}}
	}
	busy := func() *fakeNode {
		return &fakeNode{sample: func(time.Duration) (Sample, error) { return Sample{Lag: 1}, nil }}
	}
	stopped := func() *fakeNode {
		return &fakeNode{sample: func(time.Duration) (Sample, error) { return Sample{}, ErrIngestionStopped }}
	}
	mutants := map[mutation]check{
		mutIgnoreJobs: {"pending jobs are not work", func(t *testing.T, m mutation) error {
			return expect(pending(), m, 2*time.Minute, 30*time.Minute, true, 200*time.Second+2*time.Minute)
		}},
		mutIgnoreJobsDone: {"a finished job does not begin a window", func(t *testing.T, m mutation) error {
			return expect(finishing(), m, 2*time.Minute, 30*time.Minute, true, 100*time.Second+2*time.Minute)
		}},
		mutKeepWindow: {"work does not end a window", func(t *testing.T, m mutation) error {
			return expect(blip(), m, 2*time.Minute, 30*time.Minute, true, 95*time.Second+2*time.Minute)
		}},
		mutNoCap: {"no cap", func(t *testing.T, m mutation) error {
			return expect(busy(), m, 2*time.Minute, 30*time.Minute, false, 0)
		}},
		mutRetryIngestionStopped: {"a stopped ingestion is retried", func(t *testing.T, m mutation) error {
			f := stopped()
			cfg := f.config(2*time.Minute, 30*time.Minute)
			cfg.mut = m
			_, err := Settle(context.Background(), f, cfg)
			if !errors.Is(err, ErrIngestionStopped) {
				return fmt.Errorf("not the stopped ingestion: %w", err)
			}
			if f.polls != 1 {
				return fmt.Errorf("polled %d times after the ingestion stopped", f.polls)
			}
			return nil
		}},
	}
	for m, c := range mutants {
		if err := c.run(t, 0); err != nil {
			t.Errorf("%s: the correct protocol fails its own check: %v", c.name, err)
		}
		if err := c.run(t, m); err == nil {
			t.Errorf("%s: the mutant was not caught", c.name)
		}
	}
}

const metricsText = `# HELP node_tx_lag_MsgId
# TYPE node_tx_lag_MsgId gauge
node_tx_lag_MsgId{node_id="e4e4c3a3"} 3.0
# TYPE compactor_jobs_available gauge
compactor_jobs_available{node_id="e4e4c3a3"} 2.0
compactor_job_timer_seconds{node_id="e4e4c3a3",quantile="0.75"} 0.5
compactor_job_timer_seconds_count{node_id="e4e4c3a3"} 17
node_tx_latestProcessedMsgId{node_id="e4e4c3a3"} 90.0
node_tx_latestSubmittedMsgId{node_id="e4e4c3a3"} 93.0
plain_metric 1e3
`

func TestMetricsAreParsedAndAMissingOneIsAnError(t *testing.T) {
	m, err := ParseMetrics(strings.NewReader(metricsText))
	if err != nil {
		t.Fatal(err)
	}
	if m[MetricLag] != 3 || m[MetricJobsAvailable] != 2 || m[MetricJobsDone] != 17 || m["plain_metric"] != 1000 {
		t.Fatalf("%v", m)
	}
	s, err := SampleOf(m, 1)
	if err != nil || s != (Sample{Lag: 3, BlockLag: 1, JobsAvailable: 2, JobsDone: 17, Submitted: 93, Processed: 90}) {
		t.Fatalf("%+v, %v", s, err)
	}
	delete(m, MetricJobsAvailable)
	if _, err := SampleOf(m, 0); err == nil {
		t.Error("a node that reports no pending jobs was read as having none")
	}
	for _, bad := range []string{"name{label=\"x\" 1\n", "name\n", "name notanumber\n", "name{a=\"b\"}\n"} {
		if _, err := ParseMetrics(strings.NewReader(bad)); err == nil {
			t.Errorf("%q was parsed", bad)
		}
	}
}

// health is a node's health port.
func health(alive func(w http.ResponseWriter), metrics string) *httptest.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz/alive", func(w http.ResponseWriter, r *http.Request) { alive(w) })
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(metrics)) })
	return httptest.NewServer(mux)
}

func TestTheHTTPProberReadsTheHealthPort(t *testing.T) {
	srv := health(func(w http.ResponseWriter) {
		w.Header().Set("X-XTDB-Block-Lag", "6")
		w.Header().Set("X-XTDB-Block-Lag-Healthy", "false")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Unhealthy - see headers for more info."))
	}, metricsText)
	defer srv.Close()
	s, err := HTTPProber{Base: srv.URL + "/"}.Sample(context.Background())
	if err != nil || s != (Sample{Lag: 3, BlockLag: 6, JobsAvailable: 2, JobsDone: 17, Submitted: 93, Processed: 90}) {
		t.Fatalf("a node behind by blocks: %+v, %v", s, err)
	}

	stopped := health(func(w http.ResponseWriter) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("Ingestion error - clojure.lang.ExceptionInfo"))
	}, metricsText)
	defer stopped.Close()
	if _, err := (HTTPProber{Base: stopped.URL}).Sample(context.Background()); !errors.Is(err, ErrIngestionStopped) {
		t.Fatalf("a node whose ingestion stopped: %v", err)
	}

	broken := health(func(w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }, metricsText)
	defer broken.Close()
	if _, err := (HTTPProber{Base: broken.URL}).Sample(context.Background()); err == nil || errors.Is(err, ErrIngestionStopped) {
		t.Fatalf("a node that answers 500: %v", err)
	}
	if _, err := (HTTPProber{Base: "http://127.0.0.1:1"}).Sample(context.Background()); err == nil {
		t.Fatal("a port nothing listens on was read")
	}
}
