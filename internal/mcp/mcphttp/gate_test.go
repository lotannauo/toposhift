package mcphttp

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

func neighborsArgs(w *world) map[string]any {
	return map[string]any{"entity": w.n1.String(), "at": "2026-01-01T10:08:00Z"}
}

// While there is no store, every tool that reads one answers with the sentence the
// caller gave, as a tool error; the tools that read none answer as always.
func TestGateBeforeTheStore(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := newServer(t, rankObjs)
	cs := connect(t, s)

	reads := []toolCall{
		{name: "blast_radius", args: map[string]any{"entity": w.h1.String(), "from": w10, "to": w1030}},
		{name: "what_changed", args: map[string]any{"scope": []string{w.h1.String()}, "from": w10, "to": w1030}},
		{name: "co_residents", args: map[string]any{"pod": w.p1.String(), "from": w10, "to": w1030}},
		{name: "neighbors_at", args: neighborsArgs(w)},
		{name: "entity_history", args: map[string]any{"entity": w.p1.String(), "from": w10, "to": w1030}},
		{name: "find_entities", args: map[string]any{"type": "k8s.pod", "at": w10}},
		{name: "resolve_entity", args: map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}, "at": w10}},
	}
	const starting = "ingest is starting: the store is opening; send the request again later"
	const noDir = "ingest is off: set a data directory"
	const failed = "ingest is off: the store could not be opened: injected; see the server's log"

	check := func(sentence string) {
		t.Helper()
		for _, c := range reads {
			res := callTool(t, cs, c.name, c.args)
			if !res.IsError || text(t, res) != sentence {
				t.Errorf("%s: error %v, %q; want the sentence %q", c.name, res.IsError, text(t, res), sentence)
			}
		}
		// These read no store.
		answer(t, "describe_schema", callTool(t, cs, "describe_schema", map[string]any{}))
		m := answer(t, "resolve_entity", callTool(t, cs, "resolve_entity",
			map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}}))
		if m["token"] != nil {
			t.Errorf("resolve_entity with no instant read the store: token %v", m["token"])
		}
		if p := m["policy"].(map[string]any); p["rank"] == "" || p["rank"] == nil {
			t.Errorf("resolve_entity before the store: policy %v has no rank", p)
		}
	}

	// Nothing said yet: the default sentence.
	for _, c := range reads {
		res := callTool(t, cs, c.name, c.args)
		if !res.IsError || text(t, res) != sentenceUnavailable {
			t.Errorf("%s: %q, want the default sentence", c.name, text(t, res))
		}
	}
	s.Unavailable(starting)
	check(starting)
	s.Unavailable(noDir)
	check(noDir)
	s.Unavailable(failed)
	check(failed)

	// The same digest before and after the store arrives.
	before := answer(t, "resolve_entity", callTool(t, cs, "resolve_entity",
		map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}}))["policy"].(map[string]any)["rank"]
	if err := s.Attach(w.st); err != nil {
		t.Fatal(err)
	}
	after := answer(t, "resolve_entity", callTool(t, cs, "resolve_entity",
		map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}}))["policy"].(map[string]any)["rank"]
	if before != after {
		t.Errorf("the rank digest is %v before the store and %v after", before, after)
	}
	// Now the reads answer, and the sentence no longer matters.
	answer(t, "neighbors_at", callTool(t, cs, "neighbors_at", neighborsArgs(w)))
	s.Unavailable("too late")
	answer(t, "neighbors_at", callTool(t, cs, "neighbors_at", neighborsArgs(w)))
	if err := s.Attach(w.st); err == nil {
		t.Error("a second store was attached")
	}
}

// blockedStore holds every read until released, and checks after it that the
// store underneath is still open.
type blockedStore struct {
	store.Store
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
	begun    atomic.Int32
	finished atomic.Int32
}

// open lets every held read go; it may be called more than once.
func (b *blockedStore) open() { b.once.Do(func() { close(b.release) }) }

func newBlocked(st store.Store) *blockedStore {
	return &blockedStore{Store: st, entered: make(chan struct{}, 16), release: make(chan struct{})}
}

func (b *blockedStore) Alive(ctx context.Context, fp identity.Fingerprint, at time.Time, sc store.Scope) (bool, error) {
	b.begun.Add(1)
	b.entered <- struct{}{}
	<-b.release
	// The read goes on after the call's context ended, as a read that is already in
	// the store does.
	alive, err := b.Store.Alive(context.WithoutCancel(ctx), fp, at, sc)
	b.finished.Add(1)
	return alive, err
}

func (b *blockedStore) waitEntered(t *testing.T, n int) {
	t.Helper()
	for range n {
		select {
		case <-b.entered:
		case <-time.After(10 * time.Second):
			t.Fatal("a call did not reach the store")
		}
	}
}

func resolveAt(w *world) map[string]any {
	return map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}, "at": w10}
}

// Drain waits for the calls that took the store, running or queued at the gate,
// and the store may be closed only after it returns.
func TestDrainWaitsForRunningAndQueuedCalls(t *testing.T) {
	w := newWorld(t, rankObjs)
	b := newBlocked(w.st)
	s := attached(t, b, rankObjs)
	cs := connect(t, s)
	t.Cleanup(b.open) // a failed test must not leave the calls held

	// The gate takes two at once: two calls read, the third waits for a slot.
	var wg sync.WaitGroup
	results := make([]string, 3)
	for i := range results {
		wg.Add(1)
		go func() {
			defer wg.Done()
			res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: "resolve_entity", Arguments: resolveAt(w)})
			switch {
			case err != nil:
				results[i] = "protocol error: " + err.Error()
			case res.IsError:
				results[i] = text(t, res)
			default:
				results[i] = "answered"
			}
		}()
	}
	b.waitEntered(t, 2)
	time.Sleep(100 * time.Millisecond) // the third has had its chance to reach the gate

	drained := make(chan error, 1)
	go func() { drained <- s.Drain(ctx) }()
	select {
	case err := <-drained:
		t.Fatalf("Drain returned (%v) while two calls were reading", err)
	case <-time.After(200 * time.Millisecond):
	}

	// A call that comes now is refused.
	late := callTool(t, cs, "resolve_entity", resolveAt(w))
	if !late.IsError || text(t, late) != "The server is shutting down." {
		t.Errorf("a call during the drain: %v %q", late.IsError, text(t, late))
	}

	b.open()
	if err := <-drained; err != nil {
		t.Fatalf("Drain: %v", err)
	}
	// By the time Drain returns every read that began has ended: the store may be
	// closed.
	if begun, finished := b.begun.Load(), b.finished.Load(); begun < 2 || begun != finished {
		t.Errorf("when Drain returned, %d reads had begun and %d had ended", begun, finished)
	}
	wg.Wait()
	for i, r := range results {
		if r != "answered" && r != "The server is shutting down." {
			t.Errorf("call %d: %q", i, r)
		}
	}
	if err := s.Attach(w.st); err == nil {
		t.Error("a store was attached after the drain")
	}
}

// A drain that does not finish in time says so, and the store stays the caller's
// to leave open.
func TestDrainTimesOut(t *testing.T) {
	w := newWorld(t, rankObjs)
	b := newBlocked(w.st)
	s := attached(t, b, rankObjs)
	cs := connect(t, s)
	t.Cleanup(b.open) // a failed test must not leave the calls held

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = cs.CallTool(ctx, &sdk.CallToolParams{Name: "resolve_entity", Arguments: resolveAt(w)})
	}()
	b.waitEntered(t, 1)

	dctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := s.Drain(dctx); err == nil {
		t.Fatal("Drain returned nil while a read was running")
	} else if dctx.Err() == nil {
		t.Fatalf("Drain: %v", err)
	}
	b.open()
	<-done
	// Drained once the read has returned.
	if err := s.Drain(ctx); err != nil {
		t.Errorf("Drain after the read returned: %v", err)
	}
}

// Drain with no store attached returns at once, and refuses the reads after it.
func TestDrainWithNoStore(t *testing.T) {
	s := newServer(t, nil)
	cs := connect(t, s)
	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	res := callTool(t, cs, "blast_radius", map[string]any{"entity": "host:00000000000000000000000000000000", "from": w10, "to": w1030})
	if !res.IsError || text(t, res) != "The server is shutting down." {
		t.Errorf("%v %q", res.IsError, text(t, res))
	}
	s.Unavailable("opening")
	res = callTool(t, cs, "blast_radius", map[string]any{"entity": "host:00000000000000000000000000000000", "from": w10, "to": w1030})
	if text(t, res) != "The server is shutting down." {
		t.Errorf("a drained server answers %q after Unavailable", text(t, res))
	}
}

// The rank digest comes from the ranks the server was made with, so it is the
// same before the store is attached and after, in the answers that read no store
// and in those that do, and other ranks give another digest.
func TestRankDigestIsTheSameBeforeAndAfterAttach(t *testing.T) {
	w := newWorld(t, rankObjs)
	digest := func(cs *sdk.ClientSession, withInstant bool) any {
		t.Helper()
		args := map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}}
		if withInstant {
			args["at"] = w10
		}
		m := answer(t, "resolve_entity", callTool(t, cs, "resolve_entity", args))
		return m["policy"].(map[string]any)["rank"]
	}
	s := newServer(t, rankObjs)
	cs := connect(t, s)
	before := digest(cs, false)
	if before == nil || before == "" {
		t.Fatal("no rank digest before the store is attached")
	}
	if err := s.Attach(w.st); err != nil {
		t.Fatal(err)
	}
	if after := digest(cs, false); after != before {
		t.Errorf("without an instant: digest %v after Attach, %v before", after, before)
	}
	if after := digest(cs, true); after != before {
		t.Errorf("with an instant: digest %v after Attach, %v before", after, before)
	}
	if other := digest(connect(t, newServer(t, rankClus)), false); other == before {
		t.Errorf("other ranks give the same digest %v", other)
	}
}
