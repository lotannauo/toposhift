package conformance

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrTorn is wrapped by the error CheckConcurrentReads returns when a read that
// ran alongside writes matched no state the engine had been in.
var ErrTorn = errors.New("a concurrent read matched no committed state")

// CheckConcurrentReads checks the engine's consistency rule: while one goroutine
// writes batches and retains, others read as of the latest token, and every
// answer must be exactly what the oracle says at the token of some batch
// boundary between the engine's token before the read and its token after it
// (or the next boundary, since a token may lag the batch that raised it by an
// instant). An answer that mixes the world before a batch with the world after
// it, or one that sees a batch the token had not reached, matches none. Reads
// asked about an instant before a retention that began during them are not
// held to anything, and an engine may refuse them with [engine.ErrBeforeHorizon]
// instead; a refusal of an instant at or after the horizon announced so far, or any
// other error, fails the check. Use a fresh engine.
func CheckConcurrentReads(cand engine.Engine) error {
	cfg := workload.Tiny()
	cfg.Seed = 77
	cfg.LateProbability, cfg.LateMeanDelay = 0.2, 3*time.Minute
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.5, 3*time.Minute
	cfg.CoalesceRuns = true
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	ora := oracle.New()
	entities := g.Entities()
	retainAt := []time.Time{cfg.Start.Add(7 * time.Minute), cfg.Start.Add(13 * time.Minute)}

	var (
		mu         sync.Mutex
		boundaries = []uint64{0} // the tokens at which a whole committed world exists
		horizon    atomic.Int64  // the horizon, announced before Retain begins
		stop       atomic.Bool
		failure    error
		// In the second phase one writer keeps moving a few pods between nodes, a
		// delete and an observe in every batch, and the readers watch them.
		hot   atomic.Pointer[[]identity.Fingerprint]
		clock atomic.Int64 // the event time the second phase has reached
	)
	fail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if failure == nil {
			failure = err
		}
		stop.Store(true)
	}

	// worlds are the boundaries a read made between two readings of the token
	// can have seen: those from before up to after, and the next one.
	worlds := func(before, after uint64) []uint64 {
		mu.Lock()
		defer mu.Unlock()
		var out []uint64
		for _, b := range boundaries {
			switch {
			case b < before:
			case b <= after:
				out = append(out, b)
			default:
				return append(out, b)
			}
		}
		return out
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the writer: the one goroutine that writes and retains
		defer wg.Done()
		defer stop.Store(true)
		rng := rand.New(rand.NewPCG(cfg.Seed, 1))
		next := 0
		for !stop.Load() {
			batch := g.Batch(1 + rng.IntN(8))
			if len(batch) == 0 {
				fail(pingPong(cand, ora, cfg, entities, &boundaries, &mu, &horizon, &hot, &clock, &stop))
				return
			}
			if h := time.Unix(0, horizon.Load()); horizon.Load() != 0 {
				batch = slices.DeleteFunc(batch, func(r engine.Record) bool { return r.EventTime.Before(h) })
			}
			if len(batch) > 0 {
				last := batch[len(batch)-1].Seq
				if err := ora.Write(cloneRecords(batch)); err != nil {
					fail(fmt.Errorf("oracle Write: %w", err))
					return
				}
				// The oracle has the batch before the boundary is announced, and the
				// engine after, so a reader that finds a boundary can always ask
				// the oracle about it.
				mu.Lock()
				boundaries = append(boundaries, last)
				mu.Unlock()
				if err := cand.Write(cloneRecords(batch)); err != nil {
					fail(fmt.Errorf("candidate Write: %w", err))
					return
				}
				if next < len(retainAt) && !batch[len(batch)-1].EventTime.Before(retainAt[next]) {
					h := retainAt[next]
					next++
					horizon.Store(h.UnixNano())
					g.SetHorizon(h)
					if err := ora.Retain(h); err != nil {
						fail(err)
						return
					}
					if err := cand.Retain(h); err != nil {
						fail(fmt.Errorf("candidate Retain: %w", err))
						return
					}
				}
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	const readers = 4
	for r := range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(cfg.Seed+uint64(r)+1, 2))
			for !stop.Load() {
				if err := concurrentRead(cand, ora, rng, cfg, entities, &horizon, &hot, &clock, worlds); err != nil {
					fail(err)
					return
				}
				// Readers that never rest starve the writer on a machine with few
				// cores, and the check then takes minutes instead of seconds. A short
				// rest keeps the writer moving and costs no detection: what matters is
				// that reads overlap batches, not how many there are.
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	mu.Lock()
	defer mu.Unlock()
	return failure
}

// concurrentRead asks one question of the candidate while writes go on, and
// checks its answer against the oracle at the boundaries it could have seen.
func concurrentRead(cand engine.Engine, ora *oracle.Oracle, rng *rand.Rand, cfg workload.Config, entities []identity.Fingerprint,
	horizon *atomic.Int64, hot *atomic.Pointer[[]identity.Fingerprint], clock *atomic.Int64, worlds func(before, after uint64) []uint64,
) error {
	// Most reads are about workload placement (L2), where the churn is, and about
	// the entities one batch changes together: a reschedule deletes a pod's old
	// edge and observes its new one in one batch, which changes the pod's forward
	// neighbors and two nodes' reverse neighbors at once.
	layer, dir := catalog.L2, engine.Forward
	var pods, nodes []identity.Fingerprint
	for _, fp := range entities {
		switch fp.Type() {
		case catalog.K8sPod:
			pods = append(pods, fp)
		case catalog.K8sNode:
			nodes = append(nodes, fp)
		}
	}
	// Kind 0, a pod's placement, is the read a torn batch breaks most often, so it
	// is the commonest, and weighted to the busiest pods, which are the first
	// (the workload's churn is skewed toward them).
	kind := []int{0, 0, 0, 1, 2, 3, 4}[rng.IntN(7)]
	fp := pods[min(rng.IntN(len(pods)), rng.IntN(len(pods)))]
	// Once the pods that are being moved constantly exist, kind 0 reads those
	// at the instant the writer has reached.
	moving := hot.Load()
	if moving != nil && kind == 0 {
		fp = (*moving)[rng.IntN(len(*moving))]
	}
	var fps []identity.Fingerprint
	switch kind {
	case 1: // existence of anything
		fp = entities[rng.IntN(len(entities))]
	case 2: // the changes to a node's pods
		fp, dir = nodes[rng.IntN(len(nodes))], engine.Reverse
	case 3: // every node's pods in one batched read
		dir = engine.Reverse
		fps = slices.Clone(nodes)
		rng.Shuffle(len(fps), func(i, j int) { fps[i], fps[j] = fps[j], fps[i] })
	case 4: // anything, in any layer and direction
		layer = []catalog.Layer{catalog.L1, catalog.L2, catalog.L3}[rng.IntN(3)]
		fp = entities[rng.IntN(len(entities))]
		if rng.IntN(2) == 0 {
			dir = engine.Reverse
		}
	}
	t := cfg.Start.Add(time.Duration(rng.IntN(int(cfg.Duration/time.Second)+600)) * time.Second)
	if now := clock.Load(); moving != nil && now != 0 {
		t = time.Unix(0, now)
	}

	// ask puts the question to an engine as of a token.
	type answer struct {
		neighbors []engine.Neighbor
		batch     [][]engine.Neighbor
		alive     bool
		records   []engine.Record
	}
	ask := func(e engine.Engine, asOf uint64) (a answer, err error) {
		sc := engine.Scope{Layer: layer, AsOf: asOf}
		switch kind {
		case 0:
			a.neighbors, err = e.Neighbors(fp, dir, t, sc)
		case 1:
			a.alive, err = e.Alive(fp, t, sc)
		case 2:
			a.records, err = e.Window(fp, dir, t, t.Add(30*time.Minute), sc)
		case 3:
			a.batch, err = e.NeighborsBatch(fps, dir, t, sc)
		default:
			a.neighbors, err = e.Neighbors(fp, dir, t, sc)
		}
		return a, err
	}
	same := func(x, y answer) bool {
		if x.alive != y.alive || !slices.Equal(x.neighbors, y.neighbors) || len(x.batch) != len(y.batch) {
			return false
		}
		for i := range x.batch {
			if !slices.Equal(x.batch[i], y.batch[i]) {
				return false
			}
		}
		return diffRecords(x.records, y.records) == ""
	}

	before := cand.LastSeq()
	got, err := ask(cand, engine.Latest)
	after := cand.LastSeq()
	if err != nil {
		// An engine may refuse a read of an instant a retention may have taken, instead of
		// answering it. The horizon is announced before Retain begins, so the horizon the
		// engine has published is never later than the one announced (read here, after
		// the read, because a horizon loaded before the read could be earlier than the one the
		// engine had published by then): a refusal of an instant before it is legal, and one
		// at or after it, or any other error, is not. The check asks only with engine.Latest,
		// so ErrBeforeHorizon here always means the instant, never a snapshot token.
		if errors.Is(err, engine.ErrBeforeHorizon) {
			if h := horizon.Load(); h != 0 && t.Before(time.Unix(0, h)) {
				return nil
			}
			return fmt.Errorf("concurrent read: refused as before the horizon, though %s is not before the horizon announced so far: %w", t.Format(time.RFC3339), err)
		}
		return fmt.Errorf("concurrent read: %w", err)
	}
	if after < before {
		return fmt.Errorf("LastSeq went backward from %d to %d", before, after)
	}
	if h := horizon.Load(); h != 0 && t.Before(time.Unix(0, h)) {
		return nil // asked about an instant a retention that overlapped the read may have taken
	}
	candidates := worlds(before, after)
	for _, x := range candidates {
		want, err := ask(ora, x)
		if err != nil {
			return err
		}
		if same(got, want) {
			return nil
		}
	}
	return fmt.Errorf("%w: kind %d of %s in %s at %s, between tokens %d and %d (boundaries %v), got %+v",
		ErrTorn, kind, fp, layer, t.Format(time.RFC3339), before, after, candidates, got)
}

// pingPong is the second phase of CheckConcurrentReads: three pods are moved
// from node to node, every batch deleting each pod's edge to the node it was on
// and observing one to the next, so a read of a pod's placement can only be
// consistent by seeing one batch whole, and the horizon follows the writer, so
// reads overlap retentions. It announces each batch's boundary after the oracle
// has it and before the engine does, as the first phase does.
func pingPong(cand engine.Engine, ora *oracle.Oracle, cfg workload.Config, entities []identity.Fingerprint,
	boundaries *[]uint64, mu *sync.Mutex, horizon *atomic.Int64, hot *atomic.Pointer[[]identity.Fingerprint], clock *atomic.Int64, stop *atomic.Bool,
) error {
	var nodes []identity.Fingerprint
	for _, fp := range entities {
		if fp.Type() == catalog.K8sNode {
			nodes = append(nodes, fp)
		}
	}
	res := identity.NewResolver(catalog.Default())
	pods := make([]identity.Fingerprint, 3)
	for i := range pods {
		id, err := res.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: fmt.Sprintf("pingpong-%d", i)}})
		if err != nil {
			return err
		}
		pods[i] = id.Fingerprint()
	}
	hot.Store(&pods)

	seq := ora.LastSeq()
	base := cfg.Start.Add(cfg.Duration + 30*time.Minute) // after every record and horizon of the first phase
	edge := func(p, n int, at time.Time, kind lifecycle.Kind) engine.Record {
		seq++
		r := engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pods[p], nodes[n%len(nodes)], catalog.ScheduledOn), Producer: "pingpong",
			EventTime: at, Seq: seq, Kind: kind,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte{byte(seq)}
		}
		return r
	}
	for i := 0; i < 600 && !stop.Load(); i++ {
		at := base.Add(time.Duration(i) * time.Second)
		var batch []engine.Record
		for p := range pods {
			if i > 0 {
				batch = append(batch, edge(p, i-1+p, at, lifecycle.Delete))
			}
			batch = append(batch, edge(p, i+p, at, lifecycle.Observe))
		}
		if err := ora.Write(cloneRecords(batch)); err != nil {
			return fmt.Errorf("oracle Write: %w", err)
		}
		mu.Lock()
		*boundaries = append(*boundaries, seq)
		mu.Unlock()
		clock.Store(at.UnixNano())
		if err := cand.Write(cloneRecords(batch)); err != nil {
			return fmt.Errorf("candidate Write: %w", err)
		}
		// Every few batches the horizon moves up behind the writer, so reads of the
		// latest instants overlap a retention again and again: the pods' current
		// edges, and the earlier phase's, must stay visible throughout, because a
		// retention that deletes before it has written its baseline shows a gap.
		if i > 0 && i%10 == 0 {
			h := at.Add(-5 * time.Second)
			horizon.Store(h.UnixNano())
			if err := ora.Retain(h); err != nil {
				return err
			}
			if err := cand.Retain(h); err != nil {
				return fmt.Errorf("candidate Retain: %w", err)
			}
		}
		time.Sleep(200 * time.Microsecond)
	}
	return nil
}
