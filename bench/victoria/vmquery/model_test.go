package vmquery_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// storeOf converts records with the registry and returns the sealed store and the last
// sequence number.
func storeOf(t *testing.T, recs []engine.Record) (*vmmodel.Store, uint64) {
	t.Helper()
	st := vmmodel.NewStore()
	var c *vmmodel.Converter
	c, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Emit: func(ss []vmmodel.Sample) error { return st.Sink(c)(ss) }})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write(recs); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := st.Seal(); err != nil {
		t.Fatal(err)
	}
	return st, recs[len(recs)-1].Seq
}

func placement(seq uint64, producer lifecycle.Producer, kind lifecycle.Kind, ttl time.Duration, at time.Duration) engine.Record {
	r := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(podA, node, catalog.ScheduledOn),
		Producer: producer, EventTime: t0.Add(at), Seq: seq, Kind: kind, TTL: ttl,
	}
	return r
}

func atWindow(t *testing.T, st *vmmodel.Store, sem vmmodel.Semantics, at time.Duration) int {
	t.Helper()
	p := mustRewrite(t, q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0.Add(at), node))
	rs, err := vmquery.Evaluate(st, sem, p.Expr, p.At)
	if err != nil {
		t.Fatal(err)
	}
	return len(rs)
}

// The window of a term is (t-W, t]: a sample exactly W before t is out, one a millisecond
// later is in, one at t is in, one a millisecond after t is not seen. [U] (j)
func TestEvaluateReadsTheWindowAsLeftOpenRightClosed(t *testing.T) {
	const w = 3600 * time.Second
	st, _ := storeOf(t, []engine.Record{placement(1, "k8s-cluster", lifecycle.Observe, w, 0)})
	sem := vmmodel.Semantics{}
	for _, c := range []struct {
		name string
		at   time.Duration
		want int
	}{
		{"just before the sample", -time.Millisecond, 0},
		{"at the sample", 0, 1},
		{"inside the window", w - time.Millisecond, 1},
		{"exactly W after: the left end is out", w, 0},
		{"a millisecond before W", w - time.Millisecond, 1},
	} {
		if got := atWindow(t, st, sem, c.at); got != c.want {
			t.Errorf("%s: %d series, want %d", c.name, got, c.want)
		}
	}
	// The time is truncated to the millisecond before it is compared.
	p := mustRewrite(t, q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0.Add(w+999_999*time.Nanosecond), node))
	rs, _ := vmquery.Evaluate(st, sem, p.Expr, p.At)
	if len(rs) != 0 {
		t.Errorf("a time 0.999999 ms past the window's end is read as before it")
	}
	p = mustRewrite(t, q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0.Add(w-time.Nanosecond), node))
	if rs, _ = vmquery.Evaluate(st, sem, p.Expr, p.At); len(rs) != 1 {
		t.Errorf("a time a nanosecond before the window's end is read as after it")
	}
}

// Each producer is read through its own window: the pod collector's sample is still read
// 59 minutes later, the kubelet's is not read after five minutes.
func TestEvaluateUsesEachProducersWindow(t *testing.T) {
	st, _ := storeOf(t, []engine.Record{
		placement(1, "k8s-cluster", lifecycle.Observe, 3600*time.Second, 0),
		placement(2, "kubelet", lifecycle.Observe, 300*time.Second, 0),
	})
	for _, c := range []struct {
		at   time.Duration
		want int
	}{{time.Second, 2}, {299 * time.Second, 2}, {300 * time.Second, 1}, {59 * time.Minute, 1}, {60 * time.Minute, 0}} {
		if got := atWindow(t, st, vmmodel.Semantics{}, c.at); got != c.want {
			t.Errorf("at %s: %d series, want %d", c.at, got, c.want)
		}
	}
}

// A Delete is a marker. Under the assumption that a window skips markers [U] (k) the
// series reads as present until its window ends; under the other, the last sample in the
// window being a marker ends the answer.
func TestEvaluateAndTheMarker(t *testing.T) {
	st, _ := storeOf(t, []engine.Record{
		placement(1, "kubelet", lifecycle.Observe, 300*time.Second, 0),
		placement(2, "kubelet", lifecycle.Delete, 0, 100*time.Second),
	})
	if got := atWindow(t, st, vmmodel.Semantics{}, 150*time.Second); got != 1 {
		t.Errorf("markers skipped: %d series after the delete, want 1", got)
	}
	if got := atWindow(t, st, vmmodel.Semantics{}, 301*time.Second); got != 0 {
		t.Errorf("markers skipped: %d series after the window, want 0", got)
	}
	if got := atWindow(t, st, vmmodel.Semantics{StaleVisible: true}, 150*time.Second); got != 0 {
		t.Errorf("markers visible: %d series after the delete, want 0", got)
	}
	if got := atWindow(t, st, vmmodel.Semantics{StaleVisible: true}, 50*time.Second); got != 1 {
		t.Errorf("markers visible: %d series before the delete, want 1", got)
	}
}

// Terms select by layer, producer, type and the entity on the end named: nothing else is
// returned, and a series two terms both give is given once.
func TestEvaluateSelectsOnEveryLabel(t *testing.T) {
	other := engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(podA, host, catalog.ScheduledOn), // a different target
		Producer: "kubelet", EventTime: t0, Seq: 3, Kind: lifecycle.Observe, TTL: 300 * time.Second,
	}
	recs := []engine.Record{
		placement(1, "k8s-cluster", lifecycle.Observe, 3600*time.Second, 0),
		placement(2, "kubelet", lifecycle.Observe, 300*time.Second, 0),
		other,
	}
	st, _ := storeOf(t, recs)
	p := mustRewrite(t, q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0.Add(time.Second), node))
	rs, err := vmquery.Evaluate(st, vmmodel.Semantics{}, p.Expr, p.At)
	if err != nil || len(rs) != 2 {
		t.Fatalf("%v, %v", rs, err)
	}
	for _, r := range rs {
		if r.Labels["dst"] != node.String() || r.Value != "1" {
			t.Errorf("a result for another entity or with another value: %+v", r)
		}
	}
	// The same term twice gives each series once.
	twice := append(vmquery.Expr{}, p.Expr...)
	twice = append(twice, p.Expr...)
	rs, _ = vmquery.Evaluate(st, vmmodel.Semantics{}, twice, p.At)
	if len(rs) != 2 {
		t.Errorf("a series given twice by a union: %d results", len(rs))
	}
	// Another layer or another producer selects nothing.
	wrong := append(vmquery.Expr{}, p.Expr...)
	wrong[0].Layer = catalog.L3
	wrong[1].Producer = "traces"
	if rs, _ = vmquery.Evaluate(st, vmmodel.Semantics{}, wrong, p.At); len(rs) != 0 {
		t.Errorf("%d results for a layer and a producer nothing was written with", len(rs))
	}
	// An entity term needs the type to match.
	ent := vmquery.Term{Metric: vmmodel.MetricEntity, Layer: catalog.L2, Type: catalog.K8sPod, Label: vmmodel.LabelFP, Values: []string{vmmodel.FPLabel(node)}, Producer: "node-collector", Window: time.Hour}
	if rs, _ = vmquery.Evaluate(st, vmmodel.Semantics{}, vmquery.Expr{ent}, p.At); len(rs) != 0 {
		t.Errorf("an entity term of the wrong type found %d series", len(rs))
	}
}

func TestModelEngineRefusesWhatTheStoreCannotAnswer(t *testing.T) {
	// The store answers "which pods are on the node" non-empty at the latest state, so an
	// engine that answered an old-token or a window read from it would be caught here.
	st, last := storeOf(t, []engine.Record{
		placement(1, "k8s-cluster", lifecycle.Observe, 3600*time.Second, 0),
		placement(2, "k8s-cluster", lifecycle.Observe, 3600*time.Second, 10*time.Second),
	})
	eng := vmquery.NewModelEngine(registry(t), st, vmmodel.Semantics{}, last)
	at := t0.Add(20 * time.Second)

	sc := engine.Current(catalog.L2)
	ns, err := eng.Neighbors(node, engine.Reverse, at, sc)
	if err != nil || len(ns) != 1 {
		t.Fatalf("the latest state: %v, %v", ns, err)
	}
	if got, err := eng.NeighborsBatch([]identity.Fingerprint{node, node}, engine.Reverse, at, sc); err != nil || len(got) != 2 || len(got[0]) != 1 {
		t.Errorf("batch: %v, %v", got, err)
	}
	if ok, err := eng.Alive(node, at, sc); err != nil || ok {
		t.Errorf("an entity nothing was written for is not alive: %v, %v", ok, err)
	}
	if ok, err := eng.Alive(podA, at, sc); ok || !errors.Is(err, vmquery.ErrNotModelled) || errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("a pod's existence has only a producer that asserts once, so it is not modelled yet: %v, %v", ok, err)
	}
	// The snapshot the store was written through is the latest.
	if got, err := eng.Neighbors(node, engine.Reverse, at, engine.Scope{Layer: catalog.L2, AsOf: last}); err != nil || len(got) != 1 {
		t.Errorf("as of the last token: %v, %v", got, err)
	}

	var ne *vmquery.NotExpressibleError
	old := engine.Scope{Layer: catalog.L2, AsOf: last - 1}
	if got, err := eng.Neighbors(node, engine.Reverse, at, old); got != nil || !errors.As(err, &ne) || ne.Capability != vmmodel.CapAsKnownAt {
		t.Errorf("an old token was answered: %v, %v", got, err)
	}
	if got, err := eng.NeighborsBatch([]identity.Fingerprint{node}, engine.Reverse, at, old); got != nil || !errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("an old token in a batch was answered: %v, %v", got, err)
	}
	if ok, err := eng.Alive(node, at, old); ok || !errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("an old token in an existence read was answered: %v, %v", ok, err)
	}
	if got, err := eng.Window(node, engine.Reverse, t0, at, sc); got != nil || !errors.As(err, &ne) || ne.Capability != vmmodel.CapWindow {
		t.Errorf("a window was answered: %v, %v", got, err)
	}

	if err := eng.Write([]engine.Record{placement(9, "kubelet", lifecycle.Observe, time.Second, 0)}); !errors.Is(err, vmquery.ErrReadOnly) {
		t.Errorf("Write = %v", err)
	}
	if eng.LastSeq() != last {
		t.Errorf("LastSeq %d", eng.LastSeq())
	}
	if err := eng.Retain(t0); err != nil {
		t.Error(err)
	}
	if _, err := eng.Size(); err == nil {
		t.Error("a model reports a size")
	}
	if err := eng.Close(); err != nil {
		t.Error(err)
	}
	// runner.Ask puts the refusal through unchanged, for the reader to record as a row.
	if _, err := runner.Ask(eng, runner.Query{Op: runner.OpWindow, Layer: catalog.L2, Dir: engine.Reverse, Fps: []identity.Fingerprint{node}, From: t0, To: at, AsOf: engine.Latest}); !errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("Ask of a window: %v", err)
	}
}
