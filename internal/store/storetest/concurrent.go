package storetest

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// ErrTorn is wrapped by the error CheckConcurrentReads returns when a read that
// ran alongside writes matched no committed state.
var ErrTorn = errors.New("a concurrent read matched no committed state")

// concurrent is the state the goroutines of CheckConcurrentReads share.
type concurrent struct {
	cand store.Store
	ref  *memstore.Store // never retained, so it can answer for any instant and token
	cfg  Config

	entities, pods, nodes []identity.Fingerprint

	mu         sync.Mutex
	boundaries []uint64 // the tokens at which a whole committed world exists
	failure    error

	horizon   atomic.Int64 // the horizon, announced before Retain begins; 0 before the first
	announced atomic.Int64 // how many retentions have been announced
	stop      atomic.Bool
	// In the second phase one writer keeps moving a few pods between nodes, a
	// delete and an observe in every batch, and the readers watch them.
	hot   atomic.Pointer[[]identity.Fingerprint]
	clock atomic.Int64 // the event time the second phase has reached
}

func (c *concurrent) fail(err error) {
	if err == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failure == nil {
		c.failure = err
	}
	c.stop.Store(true)
}

// announce publishes a horizon the way a caller does before it calls Retain.
func (c *concurrent) announce(h time.Time) {
	c.horizon.Store(h.UnixNano())
	c.announced.Add(1)
}

// before reports whether t is before the horizon announced so far.
func (c *concurrent) before(t time.Time) bool {
	h := c.horizon.Load()
	return h != 0 && t.Before(time.Unix(0, h))
}

// worlds are the boundaries a read made between two readings of the token can
// have seen: those from before up to after, and the next one.
func (c *concurrent) worlds(before, after uint64) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []uint64
	for _, b := range c.boundaries {
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

func (c *concurrent) addBoundary(seq uint64) {
	c.mu.Lock()
	c.boundaries = append(c.boundaries, seq)
	c.mu.Unlock()
}

// CheckConcurrentReads checks the store's consistency rule: while one goroutine
// writes batches and retains, others read, and every answer must be exactly what
// the reference says at the token of some batch boundary between the store's token
// before the read and its token after it (or the next boundary, since a token may
// lag the batch that raised it by an instant). An answer that mixes the world
// before a batch with the world after it, or one that sees a batch the token had
// not reached, matches none. A reader also pins a read to LastSeq, and the answer
// must be the reference's at exactly that token: a store whose LastSeq runs ahead
// of what its reads see fails this. A read of an instant before the horizon
// announced so far may be refused with store.ErrBeforeHorizon or answered, and an
// answer must be right; a refusal of anything at or after it is a failure, and so
// is any other error. Use a fresh store opened with the zero policy.
//
// The second phase, which moves three pods between nodes while the horizon follows
// the writer, writes 600 batches and retains after every tenth; in the trimmed tier
// (see [Trimmed]) it writes 200, so the check is cheaper under the race detector.
func CheckConcurrentReads(s store.Store) error {
	cfg := Tiny()
	cfg.Seed = 77
	cfg.LateProbability, cfg.LateMax = 0.2, 3*time.Minute
	cfg.ConfirmProbability, cfg.ConfirmTTL = 0.5, 3*time.Minute
	cfg.Runs = true
	g, err := NewGenerator(cfg)
	if err != nil {
		return err
	}
	ref, err := memstore.Open(memstore.Options{})
	if err != nil {
		return err
	}
	defer func() { _ = ref.Close() }()
	c := &concurrent{cand: s, ref: ref, cfg: cfg, entities: g.Entities(), boundaries: []uint64{0}}
	for _, fp := range c.entities {
		switch fp.Type() {
		case catalog.K8sPod:
			c.pods = append(c.pods, fp)
		case catalog.K8sNode:
			c.nodes = append(c.nodes, fp)
		}
	}
	retainAt := []time.Time{cfg.Start.Add(7 * time.Minute), cfg.Start.Add(13 * time.Minute)}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { // the writer: the one goroutine that writes and retains
		defer wg.Done()
		defer c.stop.Store(true)
		rng := rand.New(rand.NewPCG(cfg.Seed, 1))
		next := 0
		for !c.stop.Load() {
			batch := g.Batch(1 + rng.IntN(8))
			if len(batch) == 0 {
				c.fail(c.pingPong())
				return
			}
			if h := c.horizon.Load(); h != 0 {
				batch = slices.DeleteFunc(batch, func(r store.Record) bool { return r.EventTime.Before(time.Unix(0, h)) })
			}
			if len(batch) > 0 {
				last := batch[len(batch)-1].Seq
				if err := ref.Write(bg, cloneRecords(batch)); err != nil {
					c.fail(fmt.Errorf("reference Write: %w", err))
					return
				}
				// The reference has the batch before the boundary is announced, and
				// the store after, so a reader that finds a boundary can always ask
				// the reference about it.
				c.addBoundary(last)
				if err := s.Write(bg, cloneRecords(batch)); err != nil {
					c.fail(fmt.Errorf("the store: Write: %w", err))
					return
				}
				if next < len(retainAt) && !batch[len(batch)-1].EventTime.Before(retainAt[next]) {
					h := retainAt[next]
					next++
					c.announce(h)
					g.SetHorizon(h)
					if err := s.Retain(bg, h); err != nil {
						c.fail(fmt.Errorf("the store: Retain: %w", err))
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
			for !c.stop.Load() {
				if err := c.read(rng); err != nil {
					c.fail(err)
					return
				}
				// Readers that never rest starve the writer on a machine with few
				// cores, and the check then takes minutes instead of seconds. A short
				// rest keeps the writer moving and costs no detection: what matters
				// is that reads overlap batches, not how many there are.
				time.Sleep(50 * time.Microsecond)
			}
		}()
	}
	wg.Wait()
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

// read asks one question of the store while writes go on, and checks its answer
// against the reference at the boundaries it could have seen, or at the token it
// was pinned to.
func (c *concurrent) read(rng *rand.Rand) error {
	// Most reads are about workload placement (L2), where the churn is, and about
	// the entities one batch changes together: a reschedule deletes a pod's old
	// edge and observes its new one in one batch, which changes the pod's forward
	// neighbors and two nodes' reverse neighbors at once.
	a := readArgs{ctx: bg, dir: store.Forward, sc: store.Current(catalog.L2)}
	// Kind 0, a pod's placement, is the read a torn batch breaks most often, so it
	// is the commonest, and weighted to the busiest pods, which are the first (the
	// workload's churn is skewed toward them).
	kind := []int{0, 0, 0, 1, 2, 3, 4, 5}[rng.IntN(8)]
	a.fp = c.pods[min(rng.IntN(len(c.pods)), rng.IntN(len(c.pods)))]
	// Once the pods that are being moved constantly exist, kind 0 reads those at
	// the instant the writer has reached.
	moving := c.hot.Load()
	if moving != nil && (kind == 0 || kind == 5) {
		a.fp = (*moving)[rng.IntN(len(*moving))]
	}
	name := "Neighbors"
	switch kind {
	case 1: // existence of anything
		name, a.fp = "Alive", c.entities[rng.IntN(len(c.entities))]
	case 2: // the changes to a node's pods
		name, a.fp, a.dir = "Window", c.nodes[rng.IntN(len(c.nodes))], store.Reverse
	case 3: // every node's pods in one batched read
		name, a.dir = "NeighborsBatch", store.Reverse
		// A frontier as large as a traversal asks for: several hundred, repeated.
		for len(a.fps) < 300 {
			a.fps = append(a.fps, c.nodes...)
			a.fps = append(a.fps, c.pods[:min(len(c.pods), 8)]...)
		}
		rng.Shuffle(len(a.fps), func(i, j int) { a.fps[i], a.fps[j] = a.fps[j], a.fps[i] })
	case 4: // anything, in any layer and direction
		a.sc.Layer = []catalog.Layer{catalog.L1, catalog.L2, catalog.L3}[rng.IntN(3)]
		a.fp = c.entities[rng.IntN(len(c.entities))]
		if rng.IntN(2) == 0 {
			a.dir = store.Reverse
		}
	case 5: // a pod's own records
		name = "EntityWindow"
	}
	a.t = c.cfg.Start.Add(time.Duration(rng.IntN(int(c.cfg.Duration/time.Second)+600)) * time.Second)
	if now := c.clock.Load(); moving != nil && now != 0 {
		a.t = time.Unix(0, now)
	}
	a.to = a.t.Add(30 * time.Minute)

	if rng.IntN(4) == 0 {
		return c.readPinned(name, a)
	}
	if rng.IntN(5) == 0 {
		// A token above every record reads as the latest.
		a.sc.AsOf = store.Latest - 1
	}
	before := c.cand.LastSeq()
	got, err := ask(c.cand, name, a)
	after := c.cand.LastSeq()
	if err != nil {
		if errors.Is(err, store.ErrBeforeHorizon) {
			if c.before(a.t) {
				return nil // a retention may have taken what was asked about
			}
			return fmt.Errorf("concurrent %s of %s at %s was refused with ErrBeforeHorizon, though it is not before the horizon announced so far: %w",
				name, a.fp, a.t.Format(time.RFC3339), err)
		}
		return fmt.Errorf("concurrent read: %w", err)
	}
	if after < before {
		return fmt.Errorf("LastSeq went backward from %d to %d", before, after)
	}
	candidates := c.worlds(before, after)
	for _, x := range candidates {
		w := a
		w.sc.AsOf = x
		want, err := ask(c.ref, name, w)
		if err != nil {
			return fmt.Errorf("reference: %w", err)
		}
		if got.diff(want) == "" {
			return nil
		}
	}
	return fmt.Errorf("%w: %s of %s in %s at %s, between tokens %d and %d (boundaries %v), got %+v",
		ErrTorn, name, a.fp, a.sc.Layer, a.t.Format(time.RFC3339), before, after, candidates, got)
}

// readPinned takes LastSeq and reads at that token, and the answer must be the
// reference's at exactly that token. The read may be refused only if the instant
// is before the horizon announced so far, or a retention was announced after the
// token was taken: the retention's Seq can be above the token.
func (c *concurrent) readPinned(name string, a readArgs) error {
	announced := c.announced.Load()
	a.sc.AsOf = c.cand.LastSeq()
	got, err := ask(c.cand, name, a)
	if err != nil {
		if errors.Is(err, store.ErrBeforeHorizon) {
			if c.before(a.t) || c.announced.Load() > announced {
				return nil
			}
			return fmt.Errorf("concurrent %s of %s pinned to token %d was refused with ErrBeforeHorizon, though neither its instant %s nor its token is below a horizon announced after the token was taken: %w",
				name, a.fp, a.sc.AsOf, a.t.Format(time.RFC3339), err)
		}
		return fmt.Errorf("concurrent pinned read: %w", err)
	}
	want, err := ask(c.ref, name, a)
	if err != nil {
		return fmt.Errorf("reference: %w", err)
	}
	if msg := got.diff(want); msg != "" {
		return fmt.Errorf("%w: %s of %s in %s at %s pinned to token %d: %s", ErrTorn, name, a.fp, a.sc.Layer, a.t.Format(time.RFC3339), a.sc.AsOf, msg)
	}
	return nil
}

// pingPong is the second phase of CheckConcurrentReads: three pods are moved from
// node to node, every batch deleting each pod's edge to the node it was on and
// observing one to the next, so a read of a pod's placement can only be consistent
// by seeing one batch whole, and the horizon follows the writer, so reads overlap
// retentions. It announces each batch's boundary after the reference has it and
// before the store does, as the first phase does.
func (c *concurrent) pingPong() error {
	res := identity.NewResolver(catalog.Default())
	pods := make([]identity.Fingerprint, 3)
	for i := range pods {
		id, err := res.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: fmt.Sprintf("pingpong-%d", i)}})
		if err != nil {
			return err
		}
		pods[i] = id.Fingerprint()
	}
	c.hot.Store(&pods)

	seq := c.ref.LastSeq()
	base := c.cfg.Start.Add(c.cfg.Duration + 30*time.Minute) // after every record and horizon of the first phase
	edge := func(p, n int, at time.Time, kind lifecycle.Kind) store.Record {
		seq++
		r := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pods[p], c.nodes[n%len(c.nodes)], catalog.ScheduledOn), Producer: "pingpong",
			EventTime: at, Seq: seq, Kind: kind,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte{byte(seq)}
		}
		return r
	}
	// The trimmed tier has fewer batches, and so fewer retentions. What the check
	// accepts does not depend on how many there are, or on timing, only how likely a
	// fault is to be met.
	batches := 600
	if Trimmed() {
		batches = 200
	}
	for i := 0; i < batches && !c.stop.Load(); i++ {
		at := base.Add(time.Duration(i) * time.Second)
		var batch []store.Record
		for p := range pods {
			if i > 0 {
				batch = append(batch, edge(p, i-1+p, at, lifecycle.Delete))
			}
			batch = append(batch, edge(p, i+p, at, lifecycle.Observe))
		}
		if err := c.ref.Write(bg, cloneRecords(batch)); err != nil {
			return fmt.Errorf("reference Write: %w", err)
		}
		c.addBoundary(seq)
		c.clock.Store(at.UnixNano())
		if err := c.cand.Write(bg, cloneRecords(batch)); err != nil {
			return fmt.Errorf("the store: Write: %w", err)
		}
		// Every few batches the horizon moves up behind the writer, so reads of the
		// latest instants overlap a retention again and again: the pods' current
		// edges, and the earlier phase's, must stay visible throughout, because a
		// retention that deletes before it has written its baseline shows a gap.
		if i > 0 && i%10 == 0 {
			h := at.Add(-5 * time.Second)
			c.announce(h)
			if err := c.cand.Retain(bg, h); err != nil {
				return fmt.Errorf("the store: Retain: %w", err)
			}
		}
		time.Sleep(200 * time.Microsecond)
	}
	return nil
}
