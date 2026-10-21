package mcphttp

import (
	"errors"
	"testing"

	core "github.com/lotannauo/toposhift/internal/mcp"
)

// The source a surface beside the tools runs on is the tools' own: one source for
// every caller once a store is attached, and before that, and after the drain,
// the sentences the tools give.
func TestSourceIsTheOneTheToolsRunOn(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := newServer(t, rankObjs)

	if src, err := s.Source(false); err != nil || src == nil {
		t.Errorf("before a store, for a call that reads none: %v, %v", src, err)
	}
	var ce *core.Error
	if src, err := s.Source(true); src != nil || !errors.As(err, &ce) || ce.Message != sentenceUnavailable {
		t.Errorf("before a store: %v, %v; want no source and %q", src, err, sentenceUnavailable)
	}
	s.Unavailable("The store is opening.")
	if _, err := s.Source(true); !errors.As(err, &ce) || ce.Message != "The store is opening." {
		t.Errorf("with a sentence set: %v", err)
	}

	if err := s.Attach(w.st); err != nil {
		t.Fatal(err)
	}
	a, err := s.Source(true)
	if err != nil || a == nil {
		t.Fatalf("after Attach: %v, %v", a, err)
	}
	if b, _ := s.Source(true); a != b {
		t.Error("two calls were given two sources: they would not share the gate")
	}
	if b, _ := s.Source(false); a != b {
		t.Error("a call that reads no store was given another source than one that reads it")
	}

	if err := s.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	// A drained server keeps its source, which refuses every call itself.
	if b, err := s.Source(true); err != nil || a != b {
		t.Errorf("after Drain: %v, %v; want the same source", b, err)
	}

	// A server drained before a store was attached has none, and says why.
	d := newServer(t, rankObjs)
	if err := d.Drain(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := d.Source(true); !errors.Is(err, core.ErrShuttingDown) {
		t.Errorf("drained with no store: %v, want a wrap of ErrShuttingDown", err)
	}
}
