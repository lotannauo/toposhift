package sequencer

import (
	"context"

	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// Hooks are the loop's test seams. They are set before Run.
type Hooks struct {
	BeforeWrite             func(batch []store.Record)
	BeforeTranslate         func(cluster string, lr capture.LogRecord)
	AfterTick               func()
	AfterRetainCheck        func()
	AfterWaiter             func()
	BetweenPendingAndWaiter func()
	WaiterExited            func()
}

// SetHooks installs the seams.
func SetHooks(s *Sequencer, h Hooks) {
	s.hooks = hooks{
		beforeWrite: h.BeforeWrite, beforeTranslate: h.BeforeTranslate, afterTick: h.AfterTick,
		afterRetainCheck: h.AfterRetainCheck, afterWaiter: h.AfterWaiter,
		betweenPendingAndWaiter: h.BetweenPendingAndWaiter, waiterExited: h.WaiterExited,
	}
}

// RetainCheck runs a retention check. It is called by the loop's goroutine, from
// a seam.
func RetainCheck(ctx context.Context, s *Sequencer) { s.retainCheck(ctx) }

// Qualifies is the predicate of the agreement data.
var Qualifies = qualifies

// Queued is how many requests wait on the queue.
func Queued(s *Sequencer) int { return len(s.queue) }

// Stopping says whether Stop has been called.
func Stopping(s *Sequencer) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.stopping
}

// NamespaceEvent is the selector of the namespace pass.
var NamespaceEvent = namespaceEvent

// Runs is how many runs the coalescer remembers. It is read once Run has
// returned.
func Runs(s *Sequencer) int { return s.co.Runs() }
