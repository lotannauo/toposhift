package subgraph_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/subgraph"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// counting counts the point reads an adapter makes, on top of the spy's record
// of the tokens it read at.
type counting struct {
	*spy
	cmu                        sync.Mutex
	alive, entityWindows, wins int
}

func (c *counting) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	c.cmu.Lock()
	c.alive++
	c.cmu.Unlock()
	return c.spy.Alive(ctx, fp, t, sc)
}

func (c *counting) EntityWindow(ctx context.Context, fp identity.Fingerprint, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	c.cmu.Lock()
	c.entityWindows++
	c.cmu.Unlock()
	return c.spy.EntityWindow(ctx, fp, from, to, sc)
}

func (c *counting) Window(ctx context.Context, fp identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope) ([]store.Record, error) {
	c.cmu.Lock()
	c.wins++
	c.cmu.Unlock()
	return c.spy.Window(ctx, fp, d, from, to, sc)
}

// WindowSince counts the Windows or EntityWindows it stands for, one per
// fingerprint.
func (c *counting) WindowSince(ctx context.Context, fps []identity.Fingerprint, d store.Direction, from, to time.Time, sc store.Scope, o store.SinceOptions) ([]store.Since, error) {
	c.cmu.Lock()
	if d == 0 {
		c.entityWindows += len(fps)
	} else {
		c.wins += len(fps)
	}
	c.cmu.Unlock()
	return c.spy.WindowSince(ctx, fps, d, from, to, sc, o)
}

func readEntities(sg subgraph.Subgraph) (n int) {
	for _, e := range sg.Entities {
		if !e.Boundary {
			n++
		}
	}
	return n
}

// The existence read is one call at the result's token. A write made after the
// result, or between the result and the read, is not seen: the subgraph is the
// one the result's own world gives, and every read names that token.
func TestAdaptersPinTheResultsToken(t *testing.T) {
	inner, f := coWorld(t, memstore.Options{})
	res := findCo(t, inner, podP1, coresidency.Options{})
	want, err := subgraph.FromCoResidency(ctx, inner, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	// After the result: the seed's peers and the node are deleted, and a peer is
	// added.
	f.write(del(ent(podP2), 3), del(ent(podP3), 4), del(ent(nodeN), 6), obs(ent(pod(40)), 1), obs(sched(pod(40), nodeN), 2))
	late := newFeed(t, inner)
	st := &counting{spy: &spy{Store: inner, onLast: func(n int) {
		if n == 1 {
			late.write(del(ent(podP1), 7), del(ent(nodeN2), 8))
		}
	}}}
	before := inner.LastSeq()
	got, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if inner.LastSeq() != before+2 {
		t.Fatal("the hook did not write")
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Errorf("a write after the result changed the subgraph:\n%+v\n%+v", got, want)
	}
	if got.Token != res.Token || res.Token >= before {
		t.Errorf("token %d, result's %d, store's %d", got.Token, res.Token, before)
	}
	if len(st.tokens) == 0 {
		t.Fatal("no read")
	}
	for _, tok := range st.tokens {
		if tok != res.Token {
			t.Fatalf("a read at token %d, want the result's %d: %v", tok, res.Token, st.tokens)
		}
	}
	if st.writes != 0 {
		t.Errorf("%d writes through the reader's store", st.writes)
	}
	// One existence read per entity read, and no edge read: the legs are the
	// result's.
	if st.alive != readEntities(got) || st.entityWindows != st.alive || st.wins != 0 {
		t.Errorf("reads: %d Alive, %d EntityWindow, %d Window for %d entities", st.alive, st.entityWindows, st.wins, readEntities(got))
	}
}

// The same for an extraction and a blast radius.
func TestExtractionAndBlastRadiusPinTheResultsToken(t *testing.T) {
	inner, _ := extractWorld(t)
	res, err := extract.NewExtractor(inner).Extract(ctx, extract.Exploratory{Service: svc(1)}, win30(), extract.Options{})
	if err != nil {
		t.Fatal(err)
	}
	want, err := subgraph.FromExtraction(ctx, inner, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	late := newFeed(t, inner)
	late.write(del(ent(inst(1)), 3), del(ent(svc(1)), 4), del(ent(pod(1)), 5))
	st := &counting{spy: &spy{Store: inner}}
	got, err := subgraph.FromExtraction(ctx, st, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", got) != fmt.Sprintf("%+v", want) {
		t.Error("a write after the result changed the extraction's subgraph")
	}
	for _, tok := range st.tokens {
		if tok != res.Token {
			t.Fatalf("a read at token %d, want %d", tok, res.Token)
		}
	}
	if st.alive != readEntities(got) || st.entityWindows != st.alive || st.wins != 0 {
		t.Errorf("reads: %d Alive, %d EntityWindow, %d Window for %d entities", st.alive, st.entityWindows, st.wins, readEntities(got))
	}

	b, _ := blastWorld(t)
	br, err := blastradius.NewWalker(b).Impact(ctx, hostH, win30(), blastradius.Options{})
	if err != nil {
		t.Fatal(err)
	}
	wantB, err := subgraph.FromBlastRadius(ctx, b, br, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	newFeed(t, b).write(del(ent(nodeN), 3), del(ent(podP1), 4))
	bs := &counting{spy: &spy{Store: b}}
	gotB, err := subgraph.FromBlastRadius(ctx, bs, br, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%+v", gotB) != fmt.Sprintf("%+v", wantB) {
		t.Error("a write after the result changed the blast radius's subgraph")
	}
	for _, tok := range bs.tokens {
		if tok != br.Token {
			t.Fatalf("a read at token %d, want %d", tok, br.Token)
		}
	}
	if bs.alive != readEntities(gotB) || bs.entityWindows != bs.alive || bs.wins != 0 {
		t.Errorf("reads: %d Alive, %d EntityWindow, %d Window for %d entities", bs.alive, bs.entityWindows, bs.wins, readEntities(gotB))
	}
}

// The reference cell of the co-residency package: BudgetReferencePods live pods
// on one node of one host. The read names the seed, a peer for each other pod
// and the node, within the budget (the host is named only by a path through
// it), and is one pass: one existence read per entity.
func TestCoResidencyReferenceCellIsWithinTheReadBudget(t *testing.T) {
	st, f := openStore(t, memstore.Options{})
	f.write(obs(ent(nodeN), -100), obs(ent(hostH), -100), obs(runsOn(nodeN, hostH), -100))
	for i := 1; i <= coresidency.BudgetReferencePods; i++ {
		f.write(obs(ent(pod(i)), -100), obs(sched(pod(i), nodeN), -100))
	}
	res := findCo(t, st, pod(1), coresidency.Options{})
	if len(res.CoResidents) != coresidency.BudgetReferencePods-1 {
		t.Fatalf("%d co-residents", len(res.CoResidents))
	}
	cs := &counting{spy: &spy{Store: st}}
	sg, err := subgraph.FromCoResidency(ctx, cs, res, subgraph.ReadPolicy{})
	if err != nil {
		t.Fatal(err)
	}
	n := coresidency.BudgetReferencePods + 1
	if cs.alive != n || cs.entityWindows != n || cs.wins != 0 || readEntities(sg) != n {
		t.Errorf("%d Alive, %d EntityWindow, %d Window, %d entities, want %d", cs.alive, cs.entityWindows, cs.wins, readEntities(sg), n)
	}
	if cs.alive > subgraph.BudgetCoResidencyEntities {
		t.Errorf("%d existence reads, over the budget of %d", cs.alive, subgraph.BudgetCoResidencyEntities)
	}
	checkDerived(t, st, sg, coWant(res))
}

func TestBudgetConstants(t *testing.T) {
	if subgraph.BudgetDate != "2026-10-15" || subgraph.BudgetCalls != 1 || subgraph.BudgetCoResidencyEntities != 128 ||
		subgraph.BudgetAnchoredEntities != 1024 || subgraph.BudgetExploratoryEntities != 9000 || subgraph.BudgetBlastEntities != 256 {
		t.Error("the budget constants changed: add a dated comment beside them instead")
	}
	// The default cap is its own number: above every budget, and not one of them.
	p := subgraph.DefaultReadPolicy()
	if p.MaxEntities != 16384 {
		t.Errorf("default read policy %+v", p)
	}
	for _, b := range []int{subgraph.BudgetCoResidencyEntities, subgraph.BudgetAnchoredEntities, subgraph.BudgetExploratoryEntities, subgraph.BudgetBlastEntities} {
		if p.MaxEntities <= b {
			t.Errorf("the default cap %d is not above the budget %d", p.MaxEntities, b)
		}
	}
}

// A result that names more entities than the cap is refused, not truncated, and
// before the store is read.
func TestAdaptersRefuseWhatTheyCannotReadWhole(t *testing.T) {
	st, _ := coWorld(t, memstore.Options{})
	res := findCo(t, st, podP1, coresidency.Options{})
	cs := &counting{spy: &spy{Store: st}}
	if _, err := subgraph.FromCoResidency(ctx, cs, res, subgraph.ReadPolicy{MaxEntities: 3}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a cap of 3: %v", err)
	}
	if _, err := subgraph.FromCoResidency(ctx, cs, res, subgraph.ReadPolicy{MaxEntities: -1}); !errors.Is(err, store.ErrInvalid) {
		t.Errorf("a negative cap: %v", err)
	}
	if cs.alive+cs.entityWindows+cs.calls != 0 {
		t.Errorf("the store was read before the arguments were refused: %d %d %d", cs.alive, cs.entityWindows, cs.calls)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := subgraph.FromCoResidency(cancelled, cs, res, subgraph.ReadPolicy{}); !errors.Is(err, context.Canceled) || cs.alive != 0 {
		t.Errorf("a cancelled context: %v, %d reads", err, cs.alive)
	}
	bad := res
	bad.To = bad.From
	if _, err := subgraph.FromCoResidency(ctx, cs, bad, subgraph.ReadPolicy{}); !errors.Is(err, store.ErrInvalid) || cs.alive != 0 {
		t.Errorf("an empty window: %v", err)
	}
	bad = res
	bad.Seed = identity.Fingerprint{}
	if _, err := subgraph.FromCoResidency(ctx, cs, bad, subgraph.ReadPolicy{}); !errors.Is(err, store.ErrInvalid) || cs.alive != 0 {
		t.Errorf("a result without a seed: %v", err)
	}
}

// A retention whose Seq is above the result's token fails the whole read, with
// no partial subgraph: the token is below the horizon's, which the store refuses.
func TestAdaptersFailWholeOnRetention(t *testing.T) {
	st, f := coWorld(t, memstore.Options{})
	res := findCo(t, st, podP1, coresidency.Options{})
	f.write(obs(ent(pod(77)), 1))
	if err := st.Retain(ctx, at(-50)); err != nil {
		t.Skipf("memstore cannot retain: %v", err)
	}
	sg, err := subgraph.FromCoResidency(ctx, st, res, subgraph.ReadPolicy{})
	if !errors.Is(err, store.ErrBeforeHorizon) {
		t.Fatalf("after a retention: %v, want ErrBeforeHorizon", err)
	}
	if len(sg.Elements) != 0 || len(sg.Entities) != 0 {
		t.Error("a partial subgraph with the error")
	}
}
