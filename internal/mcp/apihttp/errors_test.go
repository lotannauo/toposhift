package apihttp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// hookStore calls before at the start of every read of the range tools, and
// fails the read with what it returns.
type hookStore struct {
	store.Store
	before func(ctx context.Context) error
}

func (h hookStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, s store.Scope) (bool, error) {
	if err := h.before(ctx); err != nil {
		return false, err
	}
	return h.Store.Alive(ctx, fp, t, s)
}

func (h hookStore) Neighbors(ctx context.Context, fp identity.Fingerprint, d store.Direction, t time.Time, s store.Scope) ([]store.Neighbor, error) {
	if err := h.before(ctx); err != nil {
		return nil, err
	}
	return h.Store.Neighbors(ctx, fp, d, t, s)
}

func (h hookStore) NeighborsBatch(ctx context.Context, fps []identity.Fingerprint, d store.Direction, t time.Time, s store.Scope) ([][]store.Neighbor, error) {
	if err := h.before(ctx); err != nil {
		return nil, err
	}
	return h.Store.NeighborsBatch(ctx, fps, d, t, s)
}

func (h hookStore) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope) ([]store.Record, error) {
	if err := h.before(ctx); err != nil {
		return nil, err
	}
	return h.Store.Window(ctx, fp, d, from, to, s)
}

func (h hookStore) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, s store.Scope) ([]store.Record, error) {
	if err := h.before(ctx); err != nil {
		return nil, err
	}
	return h.Store.EntityWindow(ctx, fp, from, to, s)
}

func (h hookStore) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, s store.Scope, o store.SinceOptions) ([]store.Since, error) {
	if err := h.before(ctx); err != nil {
		return nil, err
	}
	return h.Store.WindowSince(ctx, fps, d, from, to, s, o)
}

// holding is a hookStore whose reads wait until they are released or their call
// ends, and count the calls that are waiting.
type holding struct {
	hookStore
	entered atomic.Int32
	release chan struct{}
	once    sync.Once
}

func newHolding(st store.Store) *holding {
	h := &holding{release: make(chan struct{})}
	h.Store = st
	h.before = func(ctx context.Context) error {
		h.entered.Add(1)
		defer h.entered.Add(-1)
		select {
		case <-h.release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return h
}

func (h *holding) let() { h.once.Do(func() { close(h.release) }) }

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

type errBody struct {
	Error struct{ Code, Message string } `json:"error"`
}

func wantRefusal(t *testing.T, what string, got reply, status int, code string) errBody {
	t.Helper()
	var e errBody
	if err := json.Unmarshal(got.body, &e); err != nil {
		t.Fatalf("%s: %v in %s", what, err, got.body)
	}
	if got.status != status || e.Error.Code != code {
		t.Errorf("%s: %d %s, want %d %s: %s", what, got.status, e.Error.Code, status, code, got.body)
	}
	return e
}

// The route that is the one the tools' own calls queue behind: with both slots
// held by tool calls on the source, a route answers busy, with Retry-After; when
// they end, it answers.
func TestRoutesShareTheSlotsOfTheSource(t *testing.T) {
	w := newWorld(t)
	h := newHolding(w.st)
	t.Cleanup(h.let)
	src := sourceOn(t, h, mcp.Config{BusyAfter: 100 * time.Millisecond})
	base := serve(t, New(sources{src: src}))

	var wg sync.WaitGroup
	for range mcp.DefaultMaxConcurrent {
		wg.Go(func() {
			_, _ = src.BlastRadius(context.Background(), mcp.BlastRadiusInput{Entity: w.n.String(), From: w10, To: w1030})
		})
	}
	waitFor(t, "both slots to be held", func() bool { return h.entered.Load() == int32(mcp.DefaultMaxConcurrent) })

	got := post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030}))
	e := wantRefusal(t, "both slots held", got, http.StatusServiceUnavailable, "busy")
	if e.Error.Message != "The server is busy; try again shortly." {
		t.Errorf("message %q", e.Error.Message)
	}
	if ra := got.header.Get("Retry-After"); ra != "5" {
		t.Errorf("Retry-After %q, want 5", ra)
	}

	h.let()
	wg.Wait()
	if got := post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})); got.status != http.StatusOK {
		t.Errorf("after the slots were released: %d %s", got.status, got.body)
	}
}

// A slow sender holds a goroutine and not a slot: two requests that have sent a
// head and half a body do not keep a third from being served.
func TestASlowSenderHoldsNoSlot(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{BusyAfter: 50 * time.Millisecond})
	srv := httptest.NewServer(func() http.Handler {
		mux := http.NewServeMux()
		mux.Handle(PathScope, api.Handler())
		return mux
	}())
	t.Cleanup(srv.Close)

	for range mcp.DefaultMaxConcurrent + 1 {
		conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", srv.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_, err = fmt.Fprintf(conn, "POST %s HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 200\r\n\r\n{\"scope\":", PathScope)
		if err != nil {
			t.Fatal(err)
		}
	}
	got := post(t, srv.URL, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030}))
	if got.status != http.StatusOK {
		t.Errorf("with %d half-sent bodies: %d %s", mcp.DefaultMaxConcurrent+1, got.status, got.body)
	}
}

// writeBlocker is a response writer whose Write waits until it is let go.
type writeBlocker struct {
	*httptest.ResponseRecorder
	writing chan struct{}
	let     chan struct{}
}

func (w *writeBlocker) Write(b []byte) (int, error) {
	close(w.writing)
	<-w.let
	return w.ResponseRecorder.Write(b)
}

// A slow reader holds no slot: while the answer to one request is being written,
// and the write has not returned, a source with one slot serves another.
func TestASlowReaderHoldsNoSlot(t *testing.T) {
	w := newWorld(t)
	api, _ := apiOn(t, w.st, mcp.Config{MaxConcurrent: 1, BusyAfter: 50 * time.Millisecond})
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	request := func() *http.Request {
		req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "http://127.0.0.1:7070"+PathScope, strings.NewReader(good))
		req.Header.Set("Content-Type", "application/json")
		return req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7070}))
	}
	slow := &writeBlocker{ResponseRecorder: httptest.NewRecorder(), writing: make(chan struct{}), let: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		defer close(done)
		api.Handler().ServeHTTP(slow, request())
	}()
	t.Cleanup(func() {
		select {
		case <-slow.let:
		default:
			close(slow.let)
		}
		<-done
	})
	select {
	case <-slow.writing:
	case <-time.After(10 * time.Second):
		t.Fatal("the first answer was never written")
	}
	rec := httptest.NewRecorder()
	api.Handler().ServeHTTP(rec, request())
	if rec.Code != http.StatusOK {
		t.Errorf("while a reader is slow: %d %s", rec.Code, rec.Body)
	}
}

// What a source's failure becomes, by its cause.
func TestFailuresOfTheSource(t *testing.T) {
	w := newWorld(t)
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})

	t.Run("no store attached yet", func(t *testing.T) {
		api := New(sources{err: &mcp.Error{Message: "The store is not available yet."}})
		e := wantRefusal(t, "unattached", post(t, serve(t, api), PathScope, good), 503, "unavailable")
		if e.Error.Message != "The store is not available yet." {
			t.Errorf("message %q", e.Error.Message)
		}
	})
	t.Run("drained before a call", func(t *testing.T) {
		api := New(sources{err: &mcp.Error{Message: "The server is shutting down.", Cause: mcp.ErrShuttingDown}})
		wantRefusal(t, "drained", post(t, serve(t, api), PathScope, good), 503, "shutting_down")
	})
	t.Run("a source that has been drained", func(t *testing.T) {
		api, src := apiOn(t, w.st, mcp.Config{})
		if err := src.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		e := wantRefusal(t, "drained source", post(t, serve(t, api), PathScope, good), 503, "shutting_down")
		if e.Error.Message != "The server is shutting down." {
			t.Errorf("message %q", e.Error.Message)
		}
	})
	t.Run("a call ended by the drain", func(t *testing.T) {
		h := newHolding(w.st)
		t.Cleanup(h.let)
		api, src := apiOn(t, h, mcp.Config{})
		base := serve(t, api)
		got := make(chan reply, 1)
		go func() { got <- post(t, base, PathScope, good) }()
		waitFor(t, "the call to read", func() bool { return h.entered.Load() > 0 })
		if err := src.Drain(context.Background()); err != nil {
			t.Fatal(err)
		}
		wantRefusal(t, "ended by the drain", <-got, 503, "shutting_down")
	})
	t.Run("the deadline", func(t *testing.T) {
		h := newHolding(w.st)
		t.Cleanup(h.let)
		api, _ := apiOn(t, h, mcp.Config{CallTimeout: 50 * time.Millisecond})
		e := wantRefusal(t, "deadline", post(t, serve(t, api), PathScope, good), 504, "deadline")
		if !strings.Contains(e.Error.Message, "took longer than") {
			t.Errorf("message %q", e.Error.Message)
		}
	})
	t.Run("a store without range reads", func(t *testing.T) {
		src, err := mcp.NewSource(mcp.Config{Store: w.st, Rank: rank})
		if err != nil {
			t.Fatal(err)
		}
		e := wantRefusal(t, "no range reads", post(t, serve(t, New(sources{src: src})), PathScope, good), 501, "no_range_reads")
		if e.Error.Message != "Range answers are not available on this store." {
			t.Errorf("message %q", e.Error.Message)
		}
	})
	t.Run("a token ahead of the store", func(t *testing.T) {
		api, _ := apiOn(t, w.st, mcp.Config{})
		base := serve(t, api)
		ahead := fmt.Sprint(w.st.LastSeq() + 1)
		e := wantRefusal(t, "future token", post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030, "asof": ahead})), 409, "future_token")
		if want := fmt.Sprintf("Token %s is ahead of the store (at %d); it is never clamped here.", ahead, w.st.LastSeq()); e.Error.Message != want {
			t.Errorf("message %q, want %q", e.Error.Message, want)
		}
		if got := post(t, base, PathScope, body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030, "asof": fmt.Sprint(w.st.LastSeq())})); got.status != 200 {
			t.Errorf("the store's own token: %d %s", got.status, got.body)
		}
	})
	t.Run("before the horizon", func(t *testing.T) {
		h := hookStore{Store: w.st, before: func(context.Context) error {
			return fmt.Errorf("read: %w", store.ErrBeforeHorizon)
		}}
		api, _ := apiOn(t, h, mcp.Config{})
		e := wantRefusal(t, "before the horizon", post(t, serve(t, api), PathScope, good), 410, "before_horizon")
		if !strings.Contains(e.Error.Message, "no longer keeps") {
			t.Errorf("message %q", e.Error.Message)
		}
	})
	t.Run("invalid arguments", func(t *testing.T) {
		api, _ := apiOn(t, w.st, mcp.Config{})
		base := serve(t, api)
		for name, b := range map[string]string{
			"no scope":               body(t, map[string]any{"scope": []string{}, "from": w10, "to": w1030}),
			"an id that is no id":    body(t, map[string]any{"scope": []string{"nope"}, "from": w10, "to": w1030}),
			"an unknown type":        body(t, map[string]any{"scope": []string{"spaceship:" + strings.Repeat("0", 32)}, "from": w10, "to": w1030}),
			"from after to":          body(t, map[string]any{"scope": strs(w.p), "from": w1030, "to": w10}),
			"a time with no zone":    body(t, map[string]any{"scope": strs(w.p), "from": "2026-01-01T10:00:00", "to": w1030}),
			"asof that is no number": body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030, "asof": "latest"}),
		} {
			wantRefusal(t, name, post(t, base, PathScope, b), 400, "invalid_argument")
		}
		over := make([]string, 257)
		for i := range over {
			over[i] = w.p.String()
		}
		wantRefusal(t, "257 ids", post(t, base, PathScope, body(t, map[string]any{"scope": over, "from": w10, "to": w1030})), 400, "invalid_argument")
		wantRefusal(t, "depth 7", post(t, base, PathBlastRadius, body(t, map[string]any{"entity": w.n.String(), "from": w10, "to": w1030, "depth": 7})), 400, "invalid_argument")
		wantRefusal(t, "a pod that is a node", post(t, base, PathServiceContext, body(t, map[string]any{"service": w.p.String(), "from": w10, "to": w1030})), 400, "invalid_argument")
	})
	t.Run("an internal error holding an endpoint id", func(t *testing.T) {
		var mu sync.Mutex
		var logs []string
		cfg := mcp.Config{Logf: func(format string, args ...any) {
			mu.Lock()
			defer mu.Unlock()
			logs = append(logs, fmt.Sprintf(format, args...))
		}}
		h := hookStore{Store: w.st, before: func(context.Context) error {
			return fmt.Errorf("disk read of %s failed: %s", w.e1, w.e2)
		}}
		api, _ := apiOn(t, h, cfg)
		got := post(t, serve(t, api), PathScope, good)
		e := wantRefusal(t, "internal", got, 500, "internal")
		if !strings.HasPrefix(e.Error.Message, "Internal error") {
			t.Errorf("message %q", e.Error.Message)
		}
		mu.Lock()
		defer mu.Unlock()
		if len(logs) == 0 {
			t.Error("the internal error was not logged")
		}
		for _, text := range append([]string{got.text()}, logs...) {
			if endpointID.MatchString(text) {
				t.Errorf("an endpoint id is printed: %s", text)
			}
		}
	})
}

// A client that went away is answered with nothing: the call is cancelled, and
// no header is written for a reader that is not there.
func TestAClientThatWentAwayGetsNothing(t *testing.T) {
	w := newWorld(t)
	h := newHolding(w.st)
	t.Cleanup(h.let)
	api, _ := apiOn(t, h, mcp.Config{})
	good := body(t, map[string]any{"scope": strs(w.p), "from": w10, "to": w1030})
	ctx, cancel := context.WithCancel(t.Context())
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://127.0.0.1:7070"+PathScope, strings.NewReader(good))
	req.Header.Set("Content-Type", "application/json")
	req = req.WithContext(context.WithValue(req.Context(), http.LocalAddrContextKey, &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 7070}))
	rec := &headerCounter{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() {
		defer close(done)
		api.Handler().ServeHTTP(rec, req)
	}()
	waitFor(t, "the call to read", func() bool { return h.entered.Load() > 0 })
	cancel()
	<-done
	if rec.headers != 0 || rec.Body.Len() != 0 {
		t.Errorf("a client that went away was answered: %d headers, %q", rec.headers, rec.Body)
	}
}

type headerCounter struct {
	*httptest.ResponseRecorder
	headers int
}

func (h *headerCounter) WriteHeader(code int) {
	h.headers++
	h.ResponseRecorder.WriteHeader(code)
}

// The status and the code of a cause, for every cause the table names, and the
// message is the sentence and never the cause.
func TestClassify(t *testing.T) {
	secret := "topo.endpoint:" + strings.Repeat("a", 32)
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"busy", mcp.ErrBusy, 503, "busy"},
		{"no range reads", mcp.ErrNoRangeReads, 501, "no_range_reads"},
		{"cancelled", context.Canceled, 503, "shutting_down"},
		{"deadline", context.DeadlineExceeded, 504, "deadline"},
		{"shutting down", mcp.ErrShuttingDown, 503, "shutting_down"},
		{"closed", fmt.Errorf("read: %w", store.ErrClosed), 503, "shutting_down"},
		{"before the horizon", fmt.Errorf("read: %w", store.ErrBeforeHorizon), 410, "before_horizon"},
		{"before the horizon, as invalid", fmt.Errorf("%w: %w", store.ErrBeforeHorizon, store.ErrInvalid), 410, "before_horizon"},
		{"a description not kept", fmt.Errorf("read: %w", interval.ErrRetained), 410, "before_horizon"},
		{"a future token", mcp.ErrFutureToken, 409, "future_token"},
		{"an answer over the cap", mcp.ErrAnswerTooLarge, 422, "answer_too_large"},
		{"invalid", fmt.Errorf("read: %w", store.ErrInvalid), 400, "invalid_argument"},
		{"a bad fingerprint", fmt.Errorf("read: %w", identity.ErrFingerprint), 400, "invalid_argument"},
		{"an attribute error", &identity.AttrError{}, 400, "invalid_argument"},
		{"anything else", errors.New("boom"), 500, "internal"},
	} {
		status, code := classify(&mcp.Error{Message: "A sentence.", Cause: tc.err})
		if status != tc.status || code != tc.code {
			t.Errorf("%s: %d %s, want %d %s", tc.name, status, code, tc.status, tc.code)
		}
	}
	rec := httptest.NewRecorder()
	writeFailure(rec, &mcp.Error{Message: "A sentence.", Cause: errors.New("the cause holds " + secret)})
	if strings.Contains(rec.Body.String(), "topo.endpoint") || !strings.Contains(rec.Body.String(), "A sentence.") {
		t.Errorf("a failure is written with its cause: %s", rec.Body)
	}
	rec = httptest.NewRecorder()
	writeFailure(rec, errors.New("not a tool error: "+secret))
	if strings.Contains(rec.Body.String(), "topo.endpoint") || rec.Code != 500 {
		t.Errorf("an error that is not the tools': %d %s", rec.Code, rec.Body)
	}
}
