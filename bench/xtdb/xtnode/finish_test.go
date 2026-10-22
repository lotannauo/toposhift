package xtnode

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeAdmin is a node that can be asked to finish its block: the request appends a message,
// and the node has processed it processAfter later (never, if negative). Until the request the
// node has processed everything it was given.
type fakeAdmin struct {
	fakeNode
	finishedAt          time.Duration
	processAfter        time.Duration
	finishes            int
	finishErr           error
	requested           bool
	sampledBeforeFinish int
}

func (f *fakeAdmin) FinishBlock(context.Context) error {
	if f.finishErr != nil {
		return f.finishErr
	}
	f.finishes++
	f.requested, f.finishedAt = true, f.t
	return nil
}

func (f *fakeAdmin) Sample(ctx context.Context) (Sample, error) {
	if !f.requested {
		f.sampledBeforeFinish++
	}
	s, err := f.fakeNode.Sample(ctx)
	if err != nil {
		return s, err
	}
	s.Submitted, s.Processed = 100, 100
	if f.requested {
		s.Submitted = 101
		if f.processAfter < 0 || f.t < f.finishedAt+f.processAfter {
			s.Processed = 100
		} else {
			s.Processed = 101
		}
	}
	return s, nil
}

func newAdmin(processAfter time.Duration) *fakeAdmin {
	a := &fakeAdmin{processAfter: processAfter}
	a.sample = func(time.Duration) (Sample, error) { return idle(0), nil }
	return a
}

func TestTheQuietWindowBeginsOnlyOnceTheBlockIsFinished(t *testing.T) {
	a := newAdmin(30 * time.Second)
	cfg := a.config(2*time.Minute, 30*time.Minute)
	r, err := FinishAndSettle(context.Background(), a, cfg)
	if err != nil || !r.Settled || !r.BlockFinished {
		t.Fatalf("%+v, %v", r, err)
	}
	if a.finishes != 1 || a.sampledBeforeFinish != 0 {
		t.Errorf("%d requests, %d samples before the request", a.finishes, a.sampledBeforeFinish)
	}
	// The request was processed at 30 s, and the window is a whole one from there.
	if a.t != 30*time.Second+2*time.Minute {
		t.Errorf("settled at %s", a.t)
	}
}

func TestANodeThatDoesNotProcessTheRequestHasNotFinishedItsBlock(t *testing.T) {
	a := newAdmin(-1)
	r, err := FinishAndSettle(context.Background(), a, a.config(2*time.Minute, 10*time.Minute))
	if err != nil || r.Settled || r.BlockFinished || r.Reason == "" {
		t.Fatalf("%+v, %v", r, err)
	}
	if a.t < 10*time.Minute {
		t.Errorf("gave up at %s", a.t)
	}
}

func TestFinishAndSettleErrors(t *testing.T) {
	boom := errors.New("refused")
	a := newAdmin(0)
	a.finishErr = boom
	if _, err := FinishAndSettle(context.Background(), a, a.config(time.Minute, time.Hour)); !errors.Is(err, boom) || a.polls != 0 {
		t.Errorf("a request that fails: %v after %d polls", err, a.polls)
	}
	a = newAdmin(0)
	a.sample = func(time.Duration) (Sample, error) { return Sample{}, ErrIngestionStopped }
	if _, err := FinishAndSettle(context.Background(), a, a.config(time.Minute, time.Hour)); !errors.Is(err, ErrIngestionStopped) {
		t.Errorf("a stopped ingestion: %v", err)
	}
	if _, err := FinishAndSettle(context.Background(), newAdmin(0), Config{}); err == nil {
		t.Error("no protocol")
	}
}

func TestFinishAndSettleMutants(t *testing.T) {
	run := func(mut mutation) (*fakeAdmin, Result, error) {
		a := newAdmin(30 * time.Second)
		cfg := a.config(2*time.Minute, 30*time.Minute)
		cfg.mut = mut
		r, err := FinishAndSettle(context.Background(), a, cfg)
		return a, r, err
	}
	check := func(a *fakeAdmin, r Result, err error) error {
		switch {
		case err != nil:
			return err
		case a.finishes != 1:
			return fmt.Errorf("%d requests", a.finishes)
		case a.t != 30*time.Second+2*time.Minute:
			return fmt.Errorf("settled at %s", a.t)
		}
		return nil
	}
	if err := check(run(0)); err != nil {
		t.Fatalf("the correct protocol: %v", err)
	}
	for name, m := range map[string]mutation{"no request": mutNoFinish, "no wait for the request to be processed": mutNoWaitProcessed} {
		if err := check(run(m)); err == nil {
			t.Errorf("%s: the mutant was not caught", name)
		}
	}
}

func TestTheHTTPProberAsksForTheBlockWithAPost(t *testing.T) {
	var methods []string
	mux := http.NewServeMux()
	mux.HandleFunc("/system/finish-block", func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("X-XTDB-Message-Id", "xtdb.api.log.Log$MessageMetadata@69fd8f44")
		_, _ = w.Write([]byte("Block flush message sent successfully."))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	if err := (HTTPProber{Base: srv.URL + "/"}).FinishBlock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(methods) != 1 || methods[0] != http.MethodPost {
		t.Errorf("methods %v", methods)
	}
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("Error sending flush block message: x"))
	}))
	defer failing.Close()
	if err := (HTTPProber{Base: failing.URL}).FinishBlock(context.Background()); err == nil {
		t.Error("a node that answered 500 was taken as having finished its block")
	}
	if err := (HTTPProber{Base: "http://127.0.0.1:1"}).FinishBlock(context.Background()); err == nil {
		t.Error("a port nothing listens on")
	}
}
