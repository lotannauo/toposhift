package vmgate_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmgate"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func TestFamily(t *testing.T) {
	for group, want := range map[string]string{
		"node<- hot-records neighbors":   "node<-",
		"service<-L3 median neighbors":   "service<-L3",
		"service<- hot-extensions batch": "service<-",
		"pod-> median neighbors":         "pod->",
		"node->L1 hot-records neighbors": "node->L1",
		"host<- median neighbors":        "host<-",
		"node exists hot-records alive":  "node exists",
		"something":                      "something",
		"":                               "",
	} {
		if got := vmgate.Family(group); got != want {
			t.Errorf("Family(%q) = %q, want %q", group, got, want)
		}
	}
}

// stub is an engine that answers from a function, and refuses what the store cannot ask.
type stub struct {
	engine.Engine
	neighbors func(fp identity.Fingerprint, at time.Time) ([]engine.Neighbor, error)
	alive     func(at time.Time) bool
}

func (s stub) Neighbors(fp identity.Fingerprint, _ engine.Direction, at time.Time, _ engine.Scope) ([]engine.Neighbor, error) {
	return s.neighbors(fp, at)
}

func (s stub) NeighborsBatch(fps []identity.Fingerprint, _ engine.Direction, at time.Time, _ engine.Scope) ([][]engine.Neighbor, error) {
	var out [][]engine.Neighbor
	for _, fp := range fps {
		ns, err := s.neighbors(fp, at)
		if err != nil {
			return nil, err
		}
		out = append(out, ns)
	}
	return out, nil
}

func (s stub) Alive(_ identity.Fingerprint, at time.Time, _ engine.Scope) (bool, error) {
	return s.alive(at), nil
}

func (s stub) Window(identity.Fingerprint, engine.Direction, time.Time, time.Time, engine.Scope) ([]engine.Record, error) {
	return nil, &vmquery.NotExpressibleError{Capability: vmmodel.CapWindow, Question: "w", Why: "no"}
}

var (
	node = fixture.FP(catalog.K8sNode, 1)
	podA = fixture.FP(catalog.K8sPod, 11)
	podB = fixture.FP(catalog.K8sPod, 12)
)

func edge(pod identity.Fingerprint, p lifecycle.Producer, kind lifecycle.Kind, ttl, at time.Duration) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: engine.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: p, EventTime: t0.Add(at), Kind: kind, TTL: ttl}
}

func world(t *testing.T, recs ...engine.Record) *vmmodel.World {
	t.Helper()
	w := vmmodel.NewWorld()
	for i := range recs {
		recs[i].Seq = uint64(i + 1)
	}
	if err := w.Write(recs); err != nil {
		t.Fatal(err)
	}
	return w
}

func query(group string, op runner.Op, rank int, at time.Duration, expect engine.Engine) runner.Query {
	q := runner.Query{Group: group, Rank: rank, Op: op, Layer: catalog.L2, Dir: engine.Reverse, Fps: []identity.Fingerprint{node}, Age: "x", At: t0.Add(at), AsOf: engine.Latest}
	if op == runner.OpWindow {
		return q // the reference's answer is not the question here: the store refuses it
	}
	a, err := runner.Ask(expect, q)
	if err != nil {
		panic(err)
	}
	q.Expect, q.Size = a.Digest, a.Size
	return q
}

// The gate sorts each question into exactly one of refused, boundary, reads a deleted
// fact and compared, and compares only the last: a wrong answer is counted and listed, an
// error that is not a refusal is a wrong answer, and a family where most questions are
// not compared is named.
func TestGateSortsTheQuestions(t *testing.T) {
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	hour := time.Hour
	// Pod A is confirmed by the kubelet at 0 (300 s) and withdrawn at 100 s; pod B is
	// refreshed by the pod collector for good measure.
	w := world(t,
		edge(podA, "kubelet", lifecycle.Observe, 300*time.Second, 0),
		edge(podA, "kubelet", lifecycle.Delete, 0, 100*time.Second),
		edge(podB, "k8s-cluster", lifecycle.Observe, hour, 0),
	)
	only := func(ps ...identity.Fingerprint) func(identity.Fingerprint, time.Time) ([]engine.Neighbor, error) {
		return func(identity.Fingerprint, time.Time) ([]engine.Neighbor, error) {
			var out []engine.Neighbor
			for _, p := range ps {
				out = append(out, engine.Neighbor{Peer: p, Relation: catalog.ScheduledOn})
			}
			engine.SortNeighbors(out)
			return out, nil
		}
	}
	// The reference's answers: pod B alone at every instant of the questions.
	ref := stub{neighbors: only(podB)}
	qs := []runner.Query{
		query("node<- g neighbors", runner.OpNeighbors, 0, 30*time.Minute, ref),  // compared, equal
		query("node<- g neighbors", runner.OpNeighbors, 1, 40*time.Minute, ref),  // compared, wrong below
		query("node<- g neighbors", runner.OpNeighbors, 2, 150*time.Second, ref), // reads a deleted fact
		query("node<- g neighbors", runner.OpNeighbors, 3, 100*time.Second, ref), // boundary: the delete
		query("node<- g window", runner.OpWindow, 4, 30*time.Minute, ref),        // refused
	}
	qs[4].From, qs[4].To = t0, t0.Add(time.Hour)
	eng := stub{neighbors: func(fp identity.Fingerprint, at time.Time) ([]engine.Neighbor, error) {
		switch at.Sub(t0) {
		case 40 * time.Minute:
			return only(podB, podA)(fp, at) // an edge nothing explains
		case 150 * time.Second, 100 * time.Second:
			return only(podB, podA)(fp, at) // the withdrawn edge, within its window
		}
		return only(podB)(fp, at)
	}}
	g := &vmgate.Gate{Reg: reg, Ref: w, Sem: vmmodel.Semantics{}}
	rep, err := g.Run(qs, eng)
	if err != nil {
		t.Fatal(err)
	}
	got := *rep.ByFamily["node<-"]
	want := vmgate.Tally{Asked: 5, Refused: 1, Boundary: 1, DeletedFact: 1, Compared: 2, Equal: 1}
	if got != want || rep.Wrong != 1 || len(rep.Mismatches) != 1 || !strings.Contains(rep.Mismatches[0].Query, "#1") {
		t.Errorf("tally %+v, wrong %d %+v", got, rep.Wrong, rep.Mismatches)
	}
	if rep.Rows[vmmodel.CapWindow] != 1 || rep.OK() {
		t.Errorf("rows %v, OK %v", rep.Rows, rep.OK())
	}
	if err := rep.Accounting(); err != nil {
		t.Error(err)
	}
	// Two of the three questions the store was asked and that were not refused were not
	// compared at the right instants, but the family is not over half: 2 of 4.
	if bad := rep.NotCompared(); len(bad) != 0 {
		t.Errorf("not compared: %v", bad)
	}
	if s := rep.String(); !strings.Contains(s, "node<-") || !strings.Contains(s, `capability row "window": 1 refused`) || !strings.Contains(s, "total") {
		t.Errorf("report:\n%s", s)
	}

	// Under semantics that make a marker end a window the withdrawn edge is not explained.
	g.Sem = vmmodel.Semantics{StaleVisible: true}
	rep, err = g.Run(qs, eng)
	if err != nil {
		t.Fatal(err)
	}
	if rep.ByFamily["node<-"].DeletedFact != 0 || rep.Wrong != 2 {
		t.Errorf("stale visible: %+v, wrong %d", *rep.ByFamily["node<-"], rep.Wrong)
	}

	// An error that is not a refusal is a compared answer that is wrong.
	boom := stub{neighbors: func(identity.Fingerprint, time.Time) ([]engine.Neighbor, error) { return nil, errors.New("boom") }}
	rep, err = g.Run(qs[:1], boom)
	if err != nil || rep.Wrong != 1 || rep.Mismatches[0].AskError != "boom" || rep.ByFamily["node<-"].Compared != 1 {
		t.Errorf("an error: %+v, %v", rep, err)
	}

	// A question that cannot be classified is the gate's own failure.
	old := qs[0]
	old.AsOf = 3
	old.Op = runner.OpNeighbors
	if _, err := g.Run([]runner.Query{old}, stub{neighbors: only(podB)}); err == nil {
		t.Error("an old token that the engine answered was not refused by the gate")
	}
}

func TestNotComparedNamesTheFamilyWhereMostAreNot(t *testing.T) {
	r := &vmgate.Report{
		Families: []string{"a", "b", "c", "d", "e"},
		ByFamily: map[string]*vmgate.Tally{
			"a": {Asked: 10, Refused: 0, Boundary: 5, Compared: 5, Equal: 5},     // exactly half: compared
			"b": {Asked: 10, Refused: 0, Boundary: 6, Compared: 4, Equal: 4},     // more than half
			"c": {Asked: 10, Refused: 4, DeletedFact: 4, Compared: 2, Equal: 2},  // 4 of the 6 that could be asked
			"d": {Asked: 10, Refused: 10},                                        // nothing could be asked
			"e": {Asked: 10, Boundary: 3, DeletedFact: 3, Compared: 4, Equal: 4}, // 6 of 10
		},
		Rows: map[string]int{"window": 14},
	}
	got := r.NotCompared()
	if strings.Join(got, ",") != "b,c,e" {
		t.Errorf("not compared: %v", got)
	}
	if err := r.Accounting(); err != nil {
		t.Error(err)
	}
	r.ByFamily["a"].Asked++
	if r.Accounting() == nil {
		t.Error("a question in no sort")
	}
	r.ByFamily["a"].Asked--
	r.Rows["window"]++
	if r.Accounting() == nil {
		t.Error("a refusal in no row")
	}
	r.Rows["window"]--
	r.Wrong = 1
	if r.Accounting() == nil || r.OK() {
		t.Error("a wrong answer that is not among the compared")
	}
}
