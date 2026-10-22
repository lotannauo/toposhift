package pgengine

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// PhaseWrite is the log the batches of a stream wrote: the log rows, the interval rows,
// the token, and the vacuums the schedule called for after each.
const PhaseWrite = "write"

// settleTime is how long the meter waits before it reads the server's counts of the log.
// A backend reports its counts when it has been idle for a second, and the engine's
// connections are not the meter's, so the counts lag what was written.
const settleTime = 1500 * time.Millisecond

// StreamMeter is a stream sink (runner.Sink) that writes a stream to an engine and counts
// the log the server writes for it. The whole stream is one span, measured both by the
// advance of the log position and by the server's own counts of log records, full-page
// images and record bytes (read once the engine's connections have reported them); the
// batches together and the retentions together (each with the vacuums and the drops it
// calls for) are sums of spans, each bracketed by the position of the log, read on a
// connection of its own, so they are counts of the engine's work and not of the questions
// asked afterwards. The counts are of log bytes and records, never of time.
type StreamMeter struct {
	e      engine.Engine
	conn   *pgx.Conn
	ctx    context.Context
	phases map[string]*Phase
	start  mark
	begun  bool
}

// NewStreamMeter returns a meter of the engine's writes, reading the log position on conn.
func NewStreamMeter(ctx context.Context, e engine.Engine, conn *pgx.Conn) *StreamMeter {
	return &StreamMeter{e: e, conn: conn, ctx: ctx, phases: make(map[string]*Phase)}
}

func (m *StreamMeter) phase(name string) *Phase {
	p := m.phases[name]
	if p == nil {
		p = &Phase{Name: name}
		m.phases[name] = p
	}
	return p
}

// Begin takes a checkpoint, so that what the schema's creation left in the log is not the
// stream's, and marks where the stream begins.
func (m *StreamMeter) Begin() error {
	if _, err := m.conn.Exec(m.ctx, "CHECKPOINT"); err != nil {
		return fmt.Errorf("pgengine: checkpoint: %w", err)
	}
	time.Sleep(settleTime)
	mk, err := readMark(m.ctx, m.conn)
	if err != nil {
		return err
	}
	m.start, m.begun = mk, true
	return nil
}

func (m *StreamMeter) measure(name string, f func() error) error {
	if !m.begun {
		return fmt.Errorf("pgengine: the meter was not begun")
	}
	before, err := readMark(m.ctx, m.conn)
	if err != nil {
		return err
	}
	if err := f(); err != nil {
		return err
	}
	after, err := readMark(m.ctx, m.conn)
	if err != nil {
		return err
	}
	p := m.phase(name)
	// The server's own counts are kept by the backend that wrote the log and reported late,
	// and a batch is written by one of the engine's connections, not the meter's: only the
	// position of the log is exact for a span this short.
	p.LogBytes += after.lsn - before.lsn
	p.CheckpointsInside += after.checkpoints - before.checkpoints
	p.Spans++
	return nil
}

// Write implements the sink of a stream.
func (m *StreamMeter) Write(batch []engine.Record) error {
	return m.measure(PhaseWrite, func() error { return m.e.Write(batch) })
}

// Retain implements the sink of a stream.
func (m *StreamMeter) Retain(horizon time.Time) error {
	return m.measure(PhaseRetain, func() error { return m.e.Retain(horizon) })
}

// Finish marks where the stream ends and returns the phases: the stream, the writes and
// the retentions, in that order.
func (m *StreamMeter) Finish() ([]Phase, error) {
	time.Sleep(settleTime)
	end, err := readMark(m.ctx, m.conn)
	if err != nil {
		return nil, err
	}
	stream := Phase{Name: PhaseStream}
	stream.accumulate(true, m.start, end)
	out := []Phase{stream}
	for _, n := range []string{PhaseWrite, PhaseRetain} {
		if p := m.phases[n]; p != nil {
			out = append(out, *p)
		} else {
			out = append(out, Phase{Name: n})
		}
	}
	return out, nil
}
