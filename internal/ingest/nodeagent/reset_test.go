package nodeagent_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// (c) A sequencer reset and a process restart in the middle of a stream, with the
// store's checkpoints on (one every few records) and the checkpoint lag at both of
// the values the store is run with: a reset makes the translator and the coalescer
// anew, a restart also closes and opens the store, and neither deletes a checkpoint
// of the node collectors' prefixes after it, because every record after it is
// later than any before it. The answers of the store to reads at every few
// minutes are those of a run without the reset.
func TestScenarioC_ResetAndRestart(t *testing.T) {
	t.Parallel()
	// The cut is in the middle of a run: the report before it started one, and the
	// next would have been absorbed by it.
	const reports, cut = 72, 37
	for _, lag := range []time.Duration{time.Nanosecond, 5 * time.Minute} {
		t.Run(fmt.Sprint("lag ", lag), func(t *testing.T) {
			t.Parallel()
			agents := distinctAgents(t)
			start := instantOf(agents[0].Rec)
			stream := steady(agents, reports, start)
			head, tail := stream[:cut*len(agents)], stream[cut*len(agents):]

			type outcome struct {
				answers      []string
				invalidated  int
				written      int64
				beforeStop   int64
				checkpointed int64
			}
			run := func(t *testing.T, how string) outcome {
				var out outcome
				rec, inv := newCountRecorder(), &invalidations{}
				dir := t.TempDir()
				st := openPebble(t, dir, lag, rec, inv)
				p := newPipe(t, st)
				p.feedAll(head)
				inv.arm()
				switch how {
				case "reset":
					p.reset()
				case "restart":
					if err := st.Close(); err != nil {
						t.Fatal(err)
					}
					st = openPebble(t, dir, lag, rec, inv)
					p.st = st
					p.reset()
				}
				p.feedAll(tail)
				// The records stored before a graceful stop writes what the coalescer
				// absorbed: a reset costs one record per subject here, which the stop's
				// extensions would otherwise hide.
				out.beforeStop = int64(len(p.written))
				p.shutdown()
				t.Cleanup(func() { _ = st.Close() })

				last := instantOf(stream[len(stream)-1].Rec)
				for at := start; !at.After(last.Add(30 * time.Minute)); at = at.Add(7 * time.Minute) {
					for _, a := range agents {
						h, n := hostFP(t, a.resStr(t, "host.id")), nodeFP(t, a.resStr(t, "k8s.node.uid"))
						ok, err := st.Alive(context.Background(), h, at, l1)
						if err != nil {
							t.Fatal(err)
						}
						out.answers = append(out.answers, fmt.Sprint(at.Sub(start), a.Name, ok, runsOn(t, st, n, at), nodesOn(t, st, h, at)))
					}
				}
				inv.mu.Lock()
				out.invalidated = inv.total
				inv.mu.Unlock()
				out.written = int64(len(p.written))
				out.checkpointed = rec.get("checkpoint.written")
				return out
			}

			control := run(t, "control")
			if control.checkpointed == 0 {
				t.Fatal("the store wrote no checkpoint: the scenario proves nothing")
			}
			if control.invalidated != 0 {
				t.Errorf("a run without a reset invalidated %d checkpoints", control.invalidated)
			}
			for _, how := range []string{"reset", "restart"} {
				got := run(t, how)
				t.Logf("lag %s, %s: %d checkpoints written, %d invalidated after the cut, %d records stored (control %d)",
					lag, how, got.checkpointed, got.invalidated, got.written, control.written)
				if got.invalidated != 0 {
					t.Errorf("%s: %d checkpoints invalidated after the cut, want 0", how, got.invalidated)
				}
				if !slices.Equal(got.answers, control.answers) {
					t.Errorf("%s: the answers differ from a run without it", how)
					for i := range got.answers {
						if got.answers[i] != control.answers[i] {
							t.Errorf("  first difference:\n   got  %s\n   want %s", got.answers[i], control.answers[i])
							break
						}
					}
				}
				// The cost of a reset: one more record per subject, the run that the
				// coalescer no longer remembers.
				if extra := got.beforeStop - control.beforeStop; extra != 2*int64(len(agents)) {
					t.Errorf("%s: %d records against the control's %d before the stop, want one more for each of the %d subjects", how, got.beforeStop, control.beforeStop, 2*len(agents))
				}
			}
		})
	}
}

// The count of invalidated checkpoints is not vacuous: an edge record that is earlier
// than the newest of its prefix, written to the same store, deletes checkpoints. (The
// store keeps checkpoints of edges: the node's runs_on prefix and the host's.)
func TestScenarioC_TheCountSeesALateRecord(t *testing.T) {
	t.Parallel()
	agents := distinctAgents(t)
	rec, inv := newCountRecorder(), &invalidations{}
	st := openPebble(t, t.TempDir(), time.Nanosecond, rec, inv)
	t.Cleanup(func() { _ = st.Close() })
	p := newPipe(t, st)
	start := instantOf(agents[0].Rec)
	p.feedAll(steady(agents, 40, start))
	if rec.get("checkpoint.written") == 0 {
		t.Fatal("no checkpoint was written")
	}
	inv.arm()
	host, node := hostFP(t, agents[0].resStr(t, "host.id")), nodeFP(t, agents[0].resStr(t, "k8s.node.uid"))
	// Another producer's edge, long-lived, so that it still holds where a checkpoint
	// summarises the prefix.
	late := store.Record{
		Layer: catalog.L1, Subject: store.EdgeSubject(node, host, catalog.RunsOn), Producer: "late/test", EventTime: start.Add(10 * time.Minute),
		Seq: st.LastSeq() + 1, Kind: lifecycle.Observe, TTL: 24 * time.Hour,
	}
	if err := st.Write(context.Background(), []store.Record{late}); err != nil {
		t.Fatal(err)
	}
	inv.mu.Lock()
	defer inv.mu.Unlock()
	if inv.total == 0 {
		t.Error("a record earlier than the newest of its prefix invalidated no checkpoint: the count measures nothing")
	}
}
