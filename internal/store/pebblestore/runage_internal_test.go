package pebblestore

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// An extension of a run is a record at the run's first instant, so it deletes every
// checkpoint after that instant. When the coalescer bounds a run's age by A, every
// record about a run whose description does not change is written before the run is A
// plus its refresh interval old, so a checkpoint at least A plus the extension
// interval behind the newest record is behind every run that can still be written to,
// and none is deleted; and when A is the extension interval itself no run is ever
// extended, so a checkpoint just behind the newest record survives as well. Without
// the bound, or with a lag short of it, extensions delete them. A run whose
// description changes (a pod made again under its old identity) is closed by a record
// at its first instant whatever A is, so the second rule needs fresh identities.
// Every store answers as the reference store does throughout.
//
// The stream is scripted, like the coalescer's output: four producers refresh the
// pod's placement every minute with a TTL of four, and each producer's run is
// extended at most every two minutes (half the TTL) by a record at its first
// instant, on a prefix that holds many runs begun at different instants (a node's
// reverse prefix holds its pods'); nothing is late, so only the coalescer can put a
// record behind a checkpoint.
func TestALagPastTheRunAgeKeepsCheckpointsThroughExtensions(t *testing.T) {
	t.Parallel()
	const (
		extension = 2 * time.Minute
		refresh   = time.Minute
		ttl       = 4 * time.Minute
	)
	for name, c := range map[string]struct {
		changes        bool // the description changes at each extension
		runMaxAge, lag time.Duration
		retain         bool
		invalidated    bool
	}{
		"no bound, lag 1ns":                           {false, 0, time.Nanosecond, false, true},
		"bound 4m, lag 1ns":                           {false, 4 * time.Minute, time.Nanosecond, false, true},
		"bound 4m, lag 4m + 2m":                       {false, 4 * time.Minute, 4*time.Minute + extension, true, false},
		"bound 2m (the extension), lag 1ns":           {false, extension, time.Nanosecond, false, false},
		"bound 2m, lag 1ns, descriptions that change": {true, extension, time.Nanosecond, false, true},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newMemRecorder()
			p := newPair(t, Options{Checkpoints: policy(CheckpointOptions{On: true, KMin: 4, Alpha: 1, Lag: c.lag}), Recorder: rec})
			producers := []lifecycle.Producer{"a", "b", "c", "d"}
			type run struct {
				start, lastExtended time.Duration
				open                bool
			}
			runs := make([]run, len(producers))
			var seq uint64
			at := func(d time.Duration) time.Time { return t0.Add(d) }
			var horizon time.Time
			for step := range 40 {
				now := time.Duration(step) * refresh
				var batch []store.Record
				for i, prod := range producers {
					r := &runs[i]
					if !r.open && now < time.Duration(i)*refresh {
						continue // the producers come up one refresh apart
					}
					seq++
					age := now - r.start
					switch {
					case !r.open:
						// The first run, at the instant of this refresh.
						*r = run{start: now, lastExtended: now, open: true}
						batch = append(batch, edgeRecord(seq, prod, at(now), lifecycle.Observe, ttl))
					case c.changes && age >= extension:
						// The description changes: the run is closed by a record at its first
						// instant, carried to now, and a new one begins.
						closing := edgeRecord(seq, prod, at(r.start), lifecycle.Observe, ttl)
						closing.Through = at(now)
						seq++
						*r = run{start: now, lastExtended: now, open: true}
						batch = append(batch, closing, edgeRecord(seq, prod, at(now), lifecycle.Observe, ttl))
					case c.runMaxAge > 0 && age >= c.runMaxAge:
						// The run is as old as the bound allows: a new one begins.
						*r = run{start: now, lastExtended: now, open: true}
						batch = append(batch, edgeRecord(seq, prod, at(now), lifecycle.Observe, ttl))
					case now-r.lastExtended >= extension:
						// The extension: the run's first instant, carried to now.
						ext := edgeRecord(seq, prod, at(r.start), lifecycle.Observe, ttl)
						ext.Through = at(now)
						batch = append(batch, ext)
						r.lastExtended = now
					default:
						seq-- // nothing to write: the refresh is coalesced
					}
				}
				if len(batch) > 0 {
					p.write(batch...)
				}
				if c.retain && step == 20 {
					horizon = at(now)
					p.retain(horizon)
					// Nothing older than the horizon can be written, so the runs that
					// began before it are over: the next refresh begins new ones.
					for i := range runs {
						if runs[i].start < now {
							runs[i].open = false
						}
					}
				}
				if len(batch) > 0 {
					var times []time.Time
					for _, d := range []time.Duration{-5 * time.Minute, -90 * time.Second, 0, time.Minute} {
						if tm := at(now + d); !tm.Before(horizon) { // nothing is read before the horizon
							times = append(times, tm)
						}
					}
					tokens := []uint64{seq, store.Latest}
					if !c.retain {
						tokens = append(tokens, seq/2)
					}
					p.same([]identity.Fingerprint{podFP, nodeFP}, times, tokens)
				}
			}
			written, hits := rec.Counter("checkpoint.written"), rec.Counter("read.checkpoint_hits")
			if written == 0 || hits == 0 {
				t.Fatalf("written %d, used %d: the stream does not exercise checkpoints", written, hits)
			}
			if got := rec.Counter("checkpoint.invalidated"); (got > 0) != c.invalidated {
				t.Errorf("%d checkpoints deleted of %d written; want some deleted: %v", got, written, c.invalidated)
			}
			t.Logf("%d written, %d deleted, %d used by reads", written, rec.Counter("checkpoint.invalidated"), hits)
		})
	}
}
