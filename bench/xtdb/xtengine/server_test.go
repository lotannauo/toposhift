package xtengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The tests in this file need an XTDB node, as the probes do (see probe_server_test.go for
// how to start one and what the node orders). Each opens engines under a table prefix of
// its own and with a base of the clock, so that it is later than the probes and than every
// test that ran before it. The node keeps the tables; a node is started for a run and
// stopped after it.

// prefixOf is the table prefix of the engines of one directory: a letter and 12 hex digits.
func prefixOf(dir string) string {
	sum := sha256.Sum256([]byte(dir))
	return "x" + hex.EncodeToString(sum[:6])
}

// serverConfig is the configuration of an engine over the live node for a directory.
func serverConfig(t testing.TB, dir string, base time.Time) Config {
	t.Helper()
	return Config{DSN: serverDSN(t), Prefix: prefixOf(dir), Base: base}
}

func clockBase() time.Time { return time.Now().UTC().Truncate(time.Microsecond) }

// openServerEngine opens an engine over the node, in a fresh set of tables.
func openServerEngine(t testing.TB, mode QueryMode) *Engine {
	t.Helper()
	cfg := serverConfig(t, t.TempDir(), clockBase())
	cfg.Mode = mode
	e, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

// serverFactory opens the tables a directory names, with the base the first open chose, so
// that an engine closed and opened again over the same directory finds what it left.
func serverFactory(t *testing.T) conformance.Factory {
	var mu sync.Mutex
	bases := make(map[string]time.Time)
	return func(dir string) (engine.Engine, error) {
		mu.Lock()
		base, ok := bases[dir]
		if !ok {
			base = clockBase()
			bases[dir] = base
		}
		mu.Unlock()
		return Open(context.Background(), serverConfig(t, dir, base))
	}
}

// TestServerConformance is the engine contract on the node: the engine is checked
// against the reference engine on a fixed workload with mid-stream retentions and on the
// scripted checks, less the one check that writes records off a whole microsecond
// (runChecks).
func TestServerConformance(t *testing.T) {
	serverDSN(t)
	// The trimmed tier (a workload with two retentions and the scripted checks): the node
	// takes milliseconds to write a record and to answer a read, so the full tier is
	// hours. Reopen and concurrent reads are TestServerReopen and TestServerConcurrentReads.
	runChecks(t, serverFactory(t), false)
}

// TestServerEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid is the first gate on the
// node, for the plans the model passes.
func TestServerEveryQuestionOfAPlanIsAnsweredAsTheReferenceEngineDid(t *testing.T) {
	serverDSN(t)
	for name, spec := range map[string]runner.Spec{"no retention": tinySpec(), "a retention": tinyRetainingSpec()} {
		t.Run(name, func(t *testing.T) {
			checkPlan(t, makePlan(t, spec), openServerEngine(t, ""))
		})
	}
}

// TestServerSystemTimesAreTheNodesOwn: the system time the node gives a batch is the one
// the engine asked for, to the microsecond, and a read as of a batch's system time sees
// that batch and not the next.
func TestServerSystemTimesAreTheNodesOwn(t *testing.T) {
	cfg := serverConfig(t, t.TempDir(), clockBase())
	e, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	for i, batch := range [][]engine.Record{
		{observeA(1, "a", 10, 0), podRec(2, 10)},
		{observeA(3, "b", 20, 5)},
		{edgeRec(4, edgeB, "c", 30, 0)},
	} {
		if err := e.Write(batch); err != nil {
			t.Fatalf("batch %d: %v", i+1, err)
		}
	}
	rows, err := queryAll(connect(t, pgxMain), "SELECT _id, _system_from FROM "+e.names.of(xtmodel.Meta)+" FOR VALID_TIME ALL ORDER BY _id")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("%d meta documents: %s", len(rows), show(rows))
	}
	for i, r := range rows {
		sys, ok := e.SystemTimeOf([]uint64{2, 3, 4}[i])
		if !ok {
			t.Fatalf("no system time for batch %d", i+1)
		}
		want := time.Unix(0, int64(sys)).UTC()
		got, err := time.Parse("2006-01-02 15:04:05.999999 -0700 MST", r[1])
		if err != nil || !got.Equal(want) {
			t.Errorf("batch %d: the node says %s (%v), the engine asked for %s", i+1, r[1], err, want.Format(time.RFC3339Nano))
		}
	}
	// The newest batch is the third; as of the second's system time, the third's edge is not there.
	reverse := func(asOf uint64) int {
		ns, err := e.Neighbors(node1, engine.Reverse, sec(100), engine.Scope{Layer: catalog.L2, AsOf: asOf})
		if err != nil {
			t.Fatal(err)
		}
		return len(ns)
	}
	if got := []int{reverse(2), reverse(3), reverse(4), reverse(engine.Latest)}; fmt.Sprint(got) != "[1 1 2 2]" {
		t.Errorf("neighbors of the node as of tokens 2, 3, 4 and the newest: %v", got)
	}
}

// extremesStream is the stream of conformance.CheckExtremes with its last instant moved to
// a whole microsecond, which is the closest XTDB can come: the instants, the deadlines that
// come from a TTL and from a Through, and a delete at the last instant.
func extremesStream() []engine.Record {
	hi := time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()
	lo := engine.MinEventTime
	var pods [4]identity.Fingerprint
	for i := range pods {
		pods[i] = fpOf("k8s.pod", byte(100+i))
	}
	edge := func(pod identity.Fingerprint, seq uint64, at time.Time, kind lifecycle.Kind, ttl time.Duration, through time.Time) engine.Record {
		r := engine.Record{
			Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node1, catalog.ScheduledOn), Producer: "extremes",
			EventTime: at, Seq: seq, Kind: kind, TTL: ttl, Through: through,
		}
		if kind == lifecycle.Observe {
			r.Payload = []byte{byte(seq)}
		}
		return r
	}
	podRecord := func(pod identity.Fingerprint, seq uint64, at time.Time) engine.Record {
		return engine.Record{
			Layer: catalog.L2, Subject: engine.EntitySubject(pod), Producer: "extremes", EventTime: at, Seq: seq,
			Kind: lifecycle.Observe, Payload: []byte{byte(seq)},
		}
	}
	return []engine.Record{
		edge(pods[0], 1, lo, lifecycle.Observe, 0, time.Time{}),
		podRecord(pods[0], 2, lo),
		edge(pods[1], 3, hi.Add(-time.Minute), lifecycle.Observe, time.Minute, time.Time{}),
		edge(pods[2], 4, hi.Add(-time.Hour), lifecycle.Observe, time.Minute, hi.Add(-time.Minute)),
		edge(pods[3], 5, hi, lifecycle.Observe, 0, time.Time{}),
		edge(pods[0], 6, hi, lifecycle.Delete, 0, time.Time{}),
		podRecord(pods[3], 7, hi),
	}
}

// TestServerExtremesAgreeWithTheModel: the ends of the representable range, where a layout
// that encodes time in a narrow field can go wrong, are answered as the model answers them
// (which the spike's own check holds to the reference engine): a record at the first instant,
// at the last whole microsecond, deadlines exactly at it, a delete at it, and reads
// either side of both ends and far outside them.
func TestServerExtremesAgreeWithTheModel(t *testing.T) {
	checkExtremes(t, openServerEngine(t, ""))
}

// checkExtremes is that check, for an engine over any node.
func checkExtremes(t testing.TB, e *Engine) {
	t.Helper()
	m := xtmodel.New()
	for _, r := range extremesStream() {
		if err := e.Write([]engine.Record{r}); err != nil {
			t.Fatalf("seq %d: %v", r.Seq, err)
		}
		if err := m.Write([]engine.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	hi := time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()
	lo := engine.MinEventTime
	times := []time.Time{
		{},
		time.Unix(-1<<40, 0), lo.Add(-time.Nanosecond), lo, lo.Add(time.Nanosecond), lo.Add(time.Microsecond), lo.Add(time.Second),
		hi.Add(-time.Hour), hi.Add(-time.Minute - time.Microsecond), hi.Add(-time.Minute), hi.Add(-time.Microsecond), hi.Add(-time.Nanosecond), hi,
		hi.Add(time.Nanosecond), engine.MaxEventTime, engine.MaxEventTime.Add(time.Nanosecond), time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC),
	}
	fps := []identity.Fingerprint{node1}
	for i := range 4 {
		fps = append(fps, fpOf("k8s.pod", byte(100+i)))
	}
	for _, asOf := range []uint64{engine.Latest, 7, 6, 5, 3, 2, 0} {
		for _, layer := range []catalog.Layer{catalog.L1, catalog.L2} {
			sc := engine.Scope{Layer: layer, AsOf: asOf}
			for _, at := range times {
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					got, err1 := e.NeighborsBatch(fps, dir, at, sc)
					want, err2 := m.NeighborsBatch(fps, dir, at, sc)
					if err1 != nil || err2 != nil || fmt.Sprint(got) != fmt.Sprint(want) {
						t.Fatalf("NeighborsBatch at %s, %v, %+v: %v (%v), the model %v (%v)", at.Format(time.RFC3339Nano), dir, sc, got, err1, want, err2)
					}
					for _, fp := range fps {
						g, err1 := e.Window(fp, dir, at, at.Add(time.Hour), sc)
						w, err2 := m.Window(fp, dir, at, at.Add(time.Hour), sc)
						if err1 != nil || err2 != nil || fmt.Sprint(g) != fmt.Sprint(w) {
							t.Fatalf("Window from %s, %v, %+v: %d records (%v), the model %d (%v)", at.Format(time.RFC3339Nano), dir, sc, len(g), err1, len(w), err2)
						}
					}
				}
				for _, fp := range fps {
					g, err1 := e.Alive(fp, at, sc)
					w, err2 := m.Alive(fp, at, sc)
					if err1 != nil || err2 != nil || g != w {
						t.Fatalf("Alive at %s, %+v: %v (%v), the model %v (%v)", at.Format(time.RFC3339Nano), sc, g, err1, w, err2)
					}
				}
			}
		}
	}
}

// TestServerRefusesWhatXTDBCannotHold: a record at the last representable nanosecond is
// refused with ErrInvalid, counted, and leaves nothing on the node; one on the last whole
// microsecond is written.
func TestServerRefusesWhatXTDBCannotHold(t *testing.T) {
	checkRefusesLastNanosecond(t, openServerEngine(t, ""))
}

// checkRefusesLastNanosecond is that check, for an engine over any node.
func checkRefusesLastNanosecond(t testing.TB, e *Engine) {
	t.Helper()
	r := observeA(1, "a", 10, 0)
	r.EventTime = engine.MaxEventTime
	if err := e.Write([]engine.Record{r}); !errors.Is(err, engine.ErrInvalid) {
		t.Fatalf("a record at the last nanosecond: %v", orNil(err))
	}
	if e.Refused().OffMicrosecond != 1 || e.LastSeq() != 0 || e.Counts().Batches != 0 {
		t.Errorf("refused %+v, last seq %d, %d batches", e.Refused(), e.LastSeq(), e.Counts().Batches)
	}
	r.EventTime = time.UnixMicro(engine.MaxEventTime.UnixMicro()).UTC()
	if err := e.Write([]engine.Record{r}); err != nil {
		t.Fatalf("a record at the last whole microsecond: %v", err)
	}
	if ok, err := e.Alive(pod1, r.EventTime, engine.Current(catalog.L2)); err != nil || ok {
		t.Errorf("alive: %v, %v", ok, err)
	}
	if ns, err := e.Neighbors(pod1, engine.Forward, engine.MaxEventTime, engine.Current(catalog.L2)); err != nil || len(ns) != 1 {
		t.Errorf("neighbors at the last nanosecond: %v, %v", ns, err)
	}
}

// TestServerReopen: the tiny workload through an engine that is closed and opened again three
// times, with two retentions between, comparing every answer to the reference engine's after
// each reopening (the spike's reopen check, on one workload and fewer probes, because the node
// takes milliseconds to write a record and to answer a read). A retention is followed by
// batches before every reopening, so the horizon is found as stored (see Engine.Retain).
func TestServerReopen(t *testing.T) {
	dir := t.TempDir()
	base := clockBase()
	open := func() (engine.Engine, error) { return Open(context.Background(), serverConfig(t, dir, base)) }
	e, err := open()
	if err != nil {
		t.Fatal(err)
	}
	current := &e
	t.Cleanup(func() { _ = (*current).Close() })
	err = conformance.Check(e, workload.Tiny(), conformance.Options{
		RetainAt: []float64{0.45, 0.8}, ReopenAt: []float64{0.2, 0.5, 0.85}, CheckEvery: 40, Entities: 4, Probes: 3,
		Reopen: func(old engine.Engine) (engine.Engine, error) {
			if err := old.Close(); err != nil {
				return nil, err
			}
			next, err := open()
			if err != nil {
				return nil, err
			}
			*current = next
			return next, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestServerConcurrentReads: reads that run beside a writer see whole batches (the spike's
// check, on the live node).
func TestServerConcurrentReads(t *testing.T) {
	if err := conformance.CheckConcurrentReads(openServerEngine(t, "")); err != nil {
		t.Fatal(err)
	}
}

// TestServerARetainThatNoBatchFollowsSurvivesAReopen: a Retain is a transaction of its own,
// so an engine opened afterwards refuses what is before the horizon and carries on at the
// next ordinal.
func TestServerARetainThatNoBatchFollowsSurvivesAReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := serverConfig(t, dir, clockBase())
	e, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Write([]engine.Record{observeA(1, "a", 10, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := e.Retain(sec(8)); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	if err := e.Write([]engine.Record{observeA(2, "a", 7, 0)}); !errors.Is(err, engine.ErrBeforeHorizon) {
		t.Fatalf("a record before the horizon, after a reopen: %v", orNil(err))
	}
	if err := e.Write([]engine.Record{observeA(2, "a", 12, 0)}); err != nil {
		t.Fatal(err)
	}
	if sys, ok := e.SystemTimeOf(2); !ok || int64(sys) != cfg.Base.Add(3*time.Microsecond).UnixNano() {
		t.Errorf("the token of the second batch is at %d (%v), want ordinal 3", sys, ok)
	}
	if ns, err := e.Neighbors(pod1, engine.Forward, sec(15), engine.Scope{Layer: catalog.L2, AsOf: 1}); err != nil || len(ns) != 1 {
		t.Errorf("a read at the token the retention named: %v, %v", ns, err)
	}
}
