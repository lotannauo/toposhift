package pebblelog

import (
	"errors"
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestRandomCheckpointHistories compares the engine with the oracle on random
// histories whose event times are nanoseconds on a 40 ns grid, so checkpoints,
// records, reads and horizons land on one another and a nanosecond either side,
// which the whole-second workloads never do. Each history mixes entity and edge
// records, several records per prefix in one batch, checkpoints written by the
// hook at random instants, a random lag and spacing, reopenings that switch
// checkpoints on and off, and retentions committed in random pieces that
// sometimes stop halfway, and is read at sampled instants and tokens (the
// retention's last Seq and up) after every step.
func TestRandomCheckpointHistories(t *testing.T) {
	t.Parallel()
	for seed := int64(0); seed < 10; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewSource(seed))
			lags := []time.Duration{0, 1, 3, 7}
			ck := CheckpointOptions{On: rng.Intn(4) != 0, KMin: 1 + rng.Intn(3), Lag: lags[rng.Intn(len(lags))]}
			cfg := pebblekv.Config{Tuning: pebblekv.TinyTuning(), FS: vfs.NewMem()}
			open := func() *Engine {
				o := ck
				if rng.Intn(3) == 0 {
					o = CheckpointOptions{}
				}
				e, err := Open("db", Options{Config: cfg, Checkpoints: o, retainBatchBytes: 1 + rng.Intn(2000), retainStopAfter: rng.Intn(3)})
				if err != nil {
					t.Fatal(err)
				}
				return e
			}
			e := open()
			defer func() { _ = e.Close() }()
			ora := oracle.New()
			ps := peers("a", "b", "c")
			names := []string{"a", "b", "c"}
			at := func(n int) time.Time { return t0.Add(time.Duration(n)) }
			var seq, last uint64
			horizon, retained := 0, false
			var log []string
			for step := range 40 {
				switch op := rng.Intn(10); {
				case op < 6:
					var batch []engine.Record
					for range 1 + rng.Intn(3) {
						seq++
						ns := horizon + rng.Intn(40)
						kind, ttl := lifecycle.Observe, time.Duration(0)
						switch {
						case rng.Intn(3) == 0:
							kind = lifecycle.Delete
						case rng.Intn(3) == 0:
							ttl = time.Duration(1 + rng.Intn(10))
						}
						prod := lifecycle.Producer([]string{"p", "q"}[rng.Intn(2)])
						var r engine.Record
						if rng.Intn(4) == 0 {
							r = engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(podFP), Producer: prod, EventTime: at(ns), Seq: seq, Kind: kind, TTL: ttl}
							if kind == lifecycle.Observe {
								r.Payload = []byte("x")
							}
						} else {
							r = edgeTo(seq, ps[names[rng.Intn(3)]], prod, at(ns), kind, ttl)
						}
						batch = append(batch, r)
						log = append(log, fmt.Sprintf("write seq %d %s@%d %v ttl %d", seq, prod, ns, kind, ttl))
					}
					for _, x := range []engine.Engine{e, ora} {
						if err := x.Write(cloneAll(batch)); err != nil {
							t.Fatalf("%v\n%v", err, log)
						}
					}
				case op < 8:
					c := horizon - 2 + rng.Intn(45)
					var err error
					switch rng.Intn(3) {
					case 0:
						err = e.CheckpointEdges(catalog.L2, podFP, engine.Forward, at(c))
					case 1:
						err = e.CheckpointEdges(catalog.L2, ps[names[rng.Intn(3)]], engine.Reverse, at(c))
					default:
						err = e.CheckpointEntity(catalog.L2, podFP, at(c))
					}
					if err != nil && !errors.Is(err, engine.ErrInvalid) {
						t.Fatal(err)
					}
					log = append(log, fmt.Sprintf("checkpoint at %d, refused %v", c, err != nil))
				case op < 9:
					if nh := horizon + rng.Intn(15); rng.Intn(2) == 0 && (!retained || nh > horizon) {
						horizon, retained, last = nh, true, seq
						for _, x := range []engine.Engine{e, ora} {
							if err := x.Retain(at(horizon)); err != nil && !errors.Is(err, errInjected) {
								t.Fatal(err)
							}
						}
						log = append(log, fmt.Sprintf("retain %d (last seq %d)", horizon, last))
					}
				default:
					if err := e.Close(); err != nil {
						t.Fatal(err)
					}
					e = open()
					log = append(log, "reopen")
				}
				tokens := []uint64{engine.Latest, last, seq}
				for range 3 {
					tokens = append(tokens, last+uint64(rng.Intn(int(seq-last)+1)))
				}
				times := []time.Time{at(horizon), at(horizon + 1), at(horizon + 49)}
				for range 9 {
					times = append(times, at(horizon+rng.Intn(50)))
				}
				for _, fp := range []identity.Fingerprint{podFP, ps["a"], ps["b"], ps["c"]} {
					for _, tm := range times {
						for _, tok := range tokens {
							sc := engine.Scope{Layer: catalog.L2, AsOf: tok}
							for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
								want, _ := ora.Neighbors(fp, dir, tm, sc)
								got, err := e.Neighbors(fp, dir, tm, sc)
								if err != nil || !slices.Equal(got, want) {
									t.Fatalf("step %d: Neighbors(%s, %s, %d, asOf %d) = %v, %v; oracle says %v\nhistory:\n%v", step, fp, dir, tm.Sub(t0), tok, got, err, want, log)
								}
							}
							wantAlive, _ := ora.Alive(fp, tm, sc)
							if gotAlive, err := e.Alive(fp, tm, sc); err != nil || gotAlive != wantAlive {
								t.Fatalf("step %d: Alive(%s, %d, asOf %d) = %v, %v; oracle says %v\nhistory:\n%v", step, fp, tm.Sub(t0), tok, gotAlive, err, wantAlive, log)
							}
						}
					}
				}
			}
		})
	}
}
