package main

import (
	"context"
	"testing"
	"time"
)

// The sequencer's clock is the replay's: sending a line moves it to the line's
// instant, and the instant of a line that arrives late does not move it back.
func TestSendingALineMovesTheClockToIt(t *testing.T) {
	var lines []metricsLine
	if err := readLines([]string{fixtureFile(t)}, func(l metricsLine) error {
		lines = append(lines, l)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(lines) != 20 {
		t.Fatalf("%d lines", len(lines))
	}
	start := lines[0].Time
	p, err := newPipeline(10*time.Minute, start, start, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer p.abort()
	ctx := context.Background()
	// Ten seconds into a flush period: no tick has moved the clock.
	at := start.Add(10 * time.Second)
	if err := p.post(ctx, lines[0].Body, at); err != nil {
		t.Fatal(err)
	}
	if got := p.clk.now(); !got.Equal(at) {
		t.Errorf("the clock is %s, want %s", got.Sub(start), at.Sub(start))
	}
	// A line stamped before the clock does not move it back.
	if err := p.post(ctx, lines[1].Body, start); err != nil {
		t.Fatal(err)
	}
	if got := p.clk.now(); !got.Equal(at) {
		t.Errorf("the clock went back to %s", got.Sub(start))
	}
	// A tick moves it to the tick.
	if err := p.advance(ctx, start.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := p.clk.now(); !got.Equal(start.Add(time.Minute)) {
		t.Errorf("after the tick at a minute the clock is %s", got.Sub(start))
	}
	if len(p.tokens) != 2 {
		t.Errorf("%d samples taken, want 2 (at the start and a minute on)", len(p.tokens))
	}
}

// A line the receiver refuses is an error that names it, and one the sequencer is
// not given leaves the truth as it was.
func TestARefusedLineIsAnError(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	p, err := newPipeline(10*time.Minute, start, start, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer p.abort()
	// Not a protobuf MetricsData.
	err = p.post(context.Background(), []byte{0xff, 0xff, 0xff}, start)
	if err == nil || p.last.called {
		t.Errorf("a body that does not decode: err %v, the sink was called: %v", err, p.last.called)
	}
	if p.truth.n != 0 {
		t.Errorf("the truth holds %d sightings", p.truth.n)
	}
}
