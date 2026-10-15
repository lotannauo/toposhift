package blastscore

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func counts(s CaseScore) [3]int { return [3]int{s.N, s.Hits, s.T} }

func win(asOf uint64) interval.Window {
	return interval.Window{From: w30From, To: w30To, AsOf: asOf}
}

// S1: a chain A depends on B, B depends on S, S depends on D, and S fails. A
// failure travels from a callee to its callers, so the walk reaches B and then
// A and never D. With the truth {B, D}, precision and recall are both a half,
// and D is a miss against the direction of the walk.
func TestS1ChainAndTheDirectionOfAFailure(t *testing.T) {
	A, B, S, D := svc(1), svc(2), svc(3), svc(4)
	st, f := world(t, memstore.Options{}, A, B, S, D)
	f.write(obs(dependsOn(A, B), hm(9, 0)), obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(S, D), hm(9, 0)))
	c := svcCase("Scenario-1", S, []identity.Fingerprint{A, B, S, D}, B, D)

	res, err := blastradius.NewWalker(st).Impact(ctx, S, win(st.LastSeq()), blastradius.Options{})
	if err != nil {
		t.Fatal(err)
	}
	list := membersOf(res, catalog.Service)
	if got, want := fpsOf(list), []identity.Fingerprint{B, A}; !slices.Equal(got, want) {
		t.Fatalf("W = %v, want [B A]", got)
	}
	if list[0].Hop != 1 || list[1].Hop != 2 {
		t.Errorf("hops %d, %d, want 1, 2", list[0].Hop, list[1].Hop)
	}

	run := runCase(t, st, c, Config{})
	if run.Outcome != OutcomeScored {
		t.Fatalf("outcome %s (%s)", run.Outcome, run.Reason)
	}
	w := scoreFor(t, run, LevelService, PredictorW)
	if counts(w) != [3]int{2, 1, 2} || w.Precision() != 0.5 || w.Recall() != 0.5 {
		t.Errorf("W n,hits,truth = %v, P %v, R %v; want [2 1 2], 0.5, 0.5", counts(w), w.Precision(), w.Recall())
	}
	if w.HopN[1] != 1 || w.HopN[2] != 1 || w.HopHits[1] != 1 || w.HopHits[2] != 0 {
		t.Errorf("by hop: n %v, hits %v", w.HopN, w.HopHits)
	}
	if run.Diag.AgainstDirection != 1 {
		t.Errorf("against_direction = %d, want 1", run.Diag.AgainstDirection)
	}
	// B_all names the three others, B_1hop the two neighbours, and B_union what W does.
	if got := counts(scoreFor(t, run, LevelService, PredictorBAll)); got != [3]int{3, 2, 2} {
		t.Errorf("B_all = %v, want [3 2 2]", got)
	}
	if got := counts(scoreFor(t, run, LevelService, PredictorB1Hop)); got != [3]int{2, 2, 2} {
		t.Errorf("B_1hop = %v, want [2 2 2]", got)
	}
	if got := counts(scoreFor(t, run, LevelService, PredictorBUnion)); got != [3]int{2, 1, 2} {
		t.Errorf("B_union = %v, want [2 1 2]", got)
	}
}

// S1 again, with the seed in the truth: it is removed from the truth and never
// counted as a miss.
func TestS1TheSeedIsNeverTruth(t *testing.T) {
	A, B, S := svc(1), svc(2), svc(3)
	st, f := world(t, memstore.Options{}, A, B, S)
	f.write(obs(dependsOn(A, B), hm(9, 0)), obs(dependsOn(B, S), hm(9, 0)))
	run := runCase(t, st, svcCase("Scenario-1", S, []identity.Fingerprint{A, B, S}, S, B, A), Config{})
	if w := scoreFor(t, run, LevelService, PredictorW); counts(w) != [3]int{2, 2, 2} {
		t.Errorf("W = %v, want [2 2 2]: the seed is not a member of the truth", counts(w))
	}
}

// S2: A depends on B while [10:00, 10:10), and B depends on S while [10:20,
// 10:30). Every leg of a path must be live at once, so the walk from S reaches B
// and not A; the union graph, which ignores when, reaches both.
func TestS2LegsMustBeLiveAtOnce(t *testing.T) {
	A, B, S := svc(1), svc(2), svc(3)
	st, f := world(t, memstore.Options{}, A, B, S)
	f.write(live(dependsOn(A, B), hm(10, 0), hm(10, 10))...)
	f.write(live(dependsOn(B, S), hm(10, 20), hm(10, 30))...)
	run := runCase(t, st, svcCase("Scenario-2", S, []identity.Fingerprint{A, B, S}, A, B), Config{})
	if got := counts(scoreFor(t, run, LevelService, PredictorW)); got != [3]int{1, 1, 2} {
		t.Errorf("W = %v, want [1 1 2]: B only", got)
	}
	if got := counts(scoreFor(t, run, LevelService, PredictorBUnion)); got != [3]int{2, 2, 2} {
		t.Errorf("B_union = %v, want [2 2 2]: A and B", got)
	}
}

// S3: two nodes. The seed service's one pod shares node 1 with p2 for 30
// seconds, with p4 for half a second, which is under the minimum, and never
// with p3 on node 2. Co-residency names p2; B_all names p2, p3 and p4; the short
// overlap is counted as suppressed and is not a prediction.
func TestS3CoResidencyAndTheMinimumOverlap(t *testing.T) {
	S, X := svc(1), svc(2)
	p1, p2, p3, p4 := pod(1), pod(2), pod(3), pod(4)
	n1, n2 := node(1), node(2)
	build := func(t *testing.T) (*memstore.Store, Case) {
		st, f := world(t, memstore.Options{}, S, X, p1, p2, p3, p4, n1, n2)
		f.write(obs(dependsOn(S, X), hm(9, 0)))
		f.write(obs(sched(p1, n1), hm(9, 0)), obs(sched(p3, n2), hm(9, 0)))
		f.write(live(sched(p2, n1), hm(10, 10), hm(10, 10).Add(30*time.Second))...)
		f.write(live(sched(p4, n1), hm(10, 20), hm(10, 20).Add(500*time.Millisecond))...)
		c := svcCase("Scenario-3", S, []identity.Fingerprint{S, X}, X)
		c.Class = ClassCoResidency
		c.SeedPods = []identity.Fingerprint{p1}
		c.Pods = []identity.Fingerprint{p1, p2, p3, p4}
		c.Pod = Truth{Entities: []identity.Fingerprint{p2, p3}}
		c.Nodes = 2
		return st, c
	}

	st, c := build(t)
	run := runCase(t, st, c, Config{})
	if got := counts(scoreFor(t, run, LevelPod, PredictorC)); got != [3]int{1, 1, 2} {
		t.Errorf("C = %v, want [1 1 2]: p2 only", got)
	}
	// B_all names the pods that existed, the seed pod apart.
	if got := counts(scoreFor(t, run, LevelPod, PredictorBAll)); got != [3]int{3, 2, 2} {
		t.Errorf("B_all = %v, want [3 2 2]: p2, p3 and p4", got)
	}
	if run.Diag.SuppressedBelowMin != 1 {
		t.Errorf("suppressed_below_min = %d, want 1", run.Diag.SuppressedBelowMin)
	}

	t.Run("a cluster of one node is not scored for co-residency", func(t *testing.T) {
		st, c := build(t)
		c.Nodes = 1
		run := runCase(t, st, c, Config{})
		if _, ok := run.scoreOf(LevelPod, PredictorC); ok {
			t.Error("C was scored on a one-node cluster")
		}
		for _, p := range []Predictor{PredictorCSvc, PredictorWC} {
			if _, ok := run.scoreOf(LevelService, p); ok {
				t.Errorf("%s was scored on a one-node cluster", p)
			}
		}
		want := []Skip{
			{LevelService, PredictorCSvc, ReasonSingleNodeCase, 1},
			{LevelService, PredictorWC, ReasonSingleNodeCase, 1},
			{LevelPod, PredictorC, ReasonSingleNodeCase, 2},
		}
		if !slices.Equal(run.Skips, want) {
			t.Errorf("skips %v, want %v", run.Skips, want)
		}
		// The walk and the baselines still are.
		scoreFor(t, run, LevelService, PredictorW)
		scoreFor(t, run, LevelService, PredictorB1Hop)
	})

	t.Run("a snapshot outside the window cannot say about pods", func(t *testing.T) {
		st, c := build(t)
		c.SnapshotTime = hm(11, 0)
		run := runCase(t, st, c, Config{})
		if run.Diag.SnapshotInWindow || !run.Diag.SnapshotKnown {
			t.Errorf("snapshot known %t, in window %t", run.Diag.SnapshotKnown, run.Diag.SnapshotInWindow)
		}
		if _, ok := run.scoreOf(LevelPod, PredictorC); ok {
			t.Error("C was scored with a snapshot outside the window")
		}
		scoreFor(t, run, LevelService, PredictorW)
		c.SnapshotTime = hm(10, 15)
		run = runCase(t, st, c, Config{})
		scoreFor(t, run, LevelPod, PredictorC)
	})

	t.Run("no seed pod leaves the pod level unscored", func(t *testing.T) {
		st, c := build(t)
		c.SeedPods = nil
		run := runCase(t, st, c, Config{})
		if _, ok := run.scoreOf(LevelPod, PredictorC); ok {
			t.Error("C was scored with no seed pod")
		}
		if len(run.Skips) == 0 || run.Skips[len(run.Skips)-1].Reason != ReasonNoSeedPods {
			t.Errorf("skips %v", run.Skips)
		}
	})
}

// S4: a cap of one hop. B depends on S, and A depends on B. B is reached at the
// cap and is named as truncated, so it is listed and can be a hit; A is never
// reached, so it is a miss. The case is truncated and both pooled variants are
// printed.
func TestS4TruncationAtTheDepthCap(t *testing.T) {
	A, B, S := svc(1), svc(2), svc(3)
	st, f := world(t, memstore.Options{}, A, B, S)
	f.write(obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(A, B), hm(9, 0)))
	c := svcCase("Scenario-4", S, []identity.Fingerprint{A, B, S}, A, B)
	cfg := Config{Walk: blastradius.Options{Policy: blastradius.Policy{MaxDepth: 1, MaxFrontier: 1024, MaxMembers: 4096}}}

	res, err := blastradius.NewWalker(st).Impact(ctx, S, win(st.LastSeq()), cfg.Walk)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(res.Truncated.Depth, []identity.Fingerprint{B}) {
		t.Fatalf("Truncated.Depth = %v, want [B]", res.Truncated.Depth)
	}

	run := runCase(t, st, c, cfg)
	if w := scoreFor(t, run, LevelService, PredictorW); counts(w) != [3]int{1, 1, 2} || !w.Truncated {
		t.Errorf("W = %v, truncated %t; want [1 1 2], true", counts(w), w.Truncated)
	}
	if !run.Diag.Truncated {
		t.Error("the case is not marked truncated")
	}

	rep, err := Evaluate([]CaseRun{run}, Meta{Dataset: "synthetic", Loader: "test"})
	if err != nil {
		t.Fatal(err)
	}
	var w Stratum
	for _, s := range rep.Strata {
		if s.Level == LevelService && s.Predictor == PredictorW {
			w = s
		}
	}
	if w.All.Cases != 1 || w.NoTruncated.Cases != 0 || w.All.Hits != 1 {
		t.Errorf("pooled with %d cases, without truncated %d, hits %d", w.All.Cases, w.NoTruncated.Cases, w.All.Hits)
	}
	if text := rep.Text(); !strings.Contains(text, "without truncated cases") {
		t.Error("the text report does not print the variant without truncated cases")
	}

	// At the default policy nothing is cut.
	if run := runCase(t, st, c, Config{}); run.Diag.Truncated {
		t.Error("truncated at the default policy")
	}
}

// S5: a quarantined seed reaches nothing, and the case cannot say. Only a host
// can be quarantined, by two boots at once.
func TestS5QuarantinedSeed(t *testing.T) {
	H := host(1)
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, node(1))
	boot := func(id string, at time.Time) store.Record {
		r := obs(ent(H), at)
		r.TTL = 15 * time.Minute
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)))
	c := Case{ID: "Scenario-5", Class: ClassCoResidency, From: w30From, To: w30To, Seed: H}
	run := runCase(t, st, c, Config{})
	if run.Outcome != OutcomeCannotSay || run.Reason != ReasonQuarantinedSeed || run.LoadFailed || len(run.Scores) != 0 {
		t.Errorf("outcome %s, reason %s, load failed %t, %d scores", run.Outcome, run.Reason, run.LoadFailed, len(run.Scores))
	}
}

// A quarantined truth entity is a miss and is counted.
func TestQuarantinedTruthIsAMissAndCounted(t *testing.T) {
	S, X, H := svc(1), svc(2), host(1)
	st, f := world(t, memstore.Options{Policy: storetest.QuarantinePolicy()}, S, X)
	boot := func(id string, at time.Time) store.Record {
		r := obs(ent(H), at)
		r.TTL = 15 * time.Minute
		r.Boot = id
		return r
	}
	f.write(boot("b1", hm(9, 0)), boot("b2", hm(9, 5)), boot("b1", hm(9, 10)), obs(dependsOn(X, S), hm(9, 0)))
	run := runCase(t, st, svcCase("Scenario-5", S, []identity.Fingerprint{S, X}, X, H), Config{})
	if run.Diag.QuarantinedMiss != 1 {
		t.Errorf("quarantined_miss = %d, want 1", run.Diag.QuarantinedMiss)
	}
	// X is a hit, H stays in the truth as a miss, and is not "uninstrumented".
	if got := counts(scoreFor(t, run, LevelService, PredictorW)); got != [3]int{1, 1, 2} {
		t.Errorf("W = %v, want [1 1 2]", got)
	}
	if run.Diag.Uninstrumented != 0 {
		t.Errorf("uninstrumented = %d, want 0: a quarantined entity is not an uninstrumented one", run.Diag.Uninstrumented)
	}
}

// extractWorld writes services with a pod each on the given nodes, from 09:00.
type extractWorld struct {
	f *feed
	n byte // next pod and instance number
}

func (w *extractWorld) pod(s identity.Fingerprint, n identity.Fingerprint) identity.Fingerprint {
	w.n++
	p, i := pod(w.n), inst(w.n)
	w.f.write(obs(ent(p), hm(9, 0)), obs(ent(i), hm(9, 0)))
	place(w.f, s, i, p)
	w.f.write(obs(sched(p, n), hm(9, 0)))
	return p
}

// S6: the seed service S has a pod on each of five nodes, D on four of them, and
// R on one. A peer on 4/5 of a frontier of five nodes is ubiquitous: D is
// suppressed and counted, never predicted, and R is.
func TestS6UbiquitousPeerIsSuppressed(t *testing.T) {
	S, D, R, Z := svc(1), svc(2), svc(3), svc(4)
	var ns []identity.Fingerprint
	for k := byte(1); k <= 5; k++ {
		ns = append(ns, node(k))
	}
	st, f := world(t, memstore.Options{}, append([]identity.Fingerprint{S, D, R, Z}, ns...)...)
	f.write(obs(dependsOn(S, Z), hm(9, 0)))
	w := &extractWorld{f: f}
	for k, n := range ns {
		w.pod(S, n)
		if k < 4 {
			w.pod(D, n)
		}
	}
	w.pod(R, ns[0])
	c := svcCase("Scenario-6", S, []identity.Fingerprint{S, D, R, Z}, D, R)
	c.Class = ClassCoResidency
	c.Nodes = 5

	run := runCase(t, st, c, Config{})
	if run.Diag.SuppressedUbiquitous != 1 {
		t.Errorf("suppressed_ubiquitous = %d, want 1", run.Diag.SuppressedUbiquitous)
	}
	if got := counts(scoreFor(t, run, LevelService, PredictorCSvc)); got != [3]int{1, 1, 2} {
		t.Errorf("C->svc = %v, want [1 1 2]: R only, D is ubiquitous", got)
	}
}

// S7: S is the Peer of a pair whose Member, the caller X, sorts before it, and
// the pair is in the answer. A caller K and a callee L that share a node with
// each other, and not with S, are a pair of two other members: not about S.
func TestS7SeedAsThePeerOfAPair(t *testing.T) {
	X, K, L, S, Z := svc(1), svc(2), svc(3), svc(5), svc(9)
	n1, n2 := node(1), node(2)
	st, f := world(t, memstore.Options{}, X, K, L, S, Z, n1, n2)
	f.write(obs(dependsOn(X, S), hm(9, 0)), obs(dependsOn(K, S), hm(9, 0)), obs(dependsOn(S, L), hm(9, 0)))
	w := &extractWorld{f: f}
	w.pod(S, n1)
	w.pod(X, n1)
	w.pod(K, n2)
	w.pod(L, n2)
	c := svcCase("Scenario-7", S, []identity.Fingerprint{X, K, L, S, Z}, X, K)
	c.Class = ClassCoResidency
	c.Nodes = 2
	run := runCase(t, st, c, Config{})
	if got := counts(scoreFor(t, run, LevelService, PredictorCSvc)); got != [3]int{1, 1, 2} {
		t.Errorf("C->svc = %v, want [1 1 2]: X only (K and L are a pair of other members)", got)
	}
	if got := counts(scoreFor(t, run, LevelService, PredictorW)); got != [3]int{2, 2, 2} {
		t.Errorf("W = %v, want [2 2 2]: X and K call S", got)
	}
}

// W+C ranks by hop and then puts the walk first.
func TestMergeWCRanksByHopThenWalkFirst(t *testing.T) {
	a, b, c, d := svc(1), svc(2), svc(3), svc(4)
	w := []Prediction{{b, 1}, {a, 2}}
	cl := []Prediction{{c, 1}, {a, 1}, {d, 1}}
	got := fpsOf(mergeWC(w, cl))
	want := []identity.Fingerprint{b, c, a, d, a}
	if !slices.Equal(got, want) {
		t.Errorf("merge = %v, want %v (de-duplication is the scorer's)", got, want)
	}
	s := scoreList(mergeWC(w, cl), map[identity.Fingerprint]struct{}{a: {}}, nil, Truth{}, 0)
	if s.N != 4 {
		t.Errorf("after de-duplication the list has %d entities, want 4", s.N)
	}
}

func TestLoadChecks(t *testing.T) {
	S, X := svc(1), svc(2)
	t.Run("the seed is absent", func(t *testing.T) {
		st, _ := world(t, memstore.Options{}, X)
		run := runCase(t, st, svcCase("Scenario-8", S, []identity.Fingerprint{S, X}, X), Config{})
		if run.Outcome != OutcomeCannotSay || run.Reason != ReasonSeedAbsent || !run.LoadFailed {
			t.Errorf("outcome %s, reason %s, load failed %t", run.Outcome, run.Reason, run.LoadFailed)
		}
	})
	t.Run("no depends_on in the window", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, S, X)
		f.write(live(dependsOn(X, S), hm(8, 0), hm(9, 30))...)
		run := runCase(t, st, svcCase("Scenario-8", S, []identity.Fingerprint{S, X}, X), Config{})
		if run.Outcome != OutcomeCannotSay || run.Reason != ReasonNoDependsOn || !run.LoadFailed {
			t.Errorf("outcome %s, reason %s, load failed %t", run.Outcome, run.Reason, run.LoadFailed)
		}
	})
	t.Run("an uninstrumented seed is excluded and is not a load failure", func(t *testing.T) {
		st, _ := world(t, memstore.Options{})
		c := svcCase("Scenario-8", S, nil)
		c.Exclusion = ExcludedUninstrumentedSeed
		run := runCase(t, st, c, Config{})
		if run.Outcome != OutcomeExcluded || run.LoadFailed {
			t.Errorf("outcome %s, load failed %t", run.Outcome, run.LoadFailed)
		}
		for _, ex := range []Exclusion{ExcludedUnmappableRoot, ExcludedNoWindow} {
			c.Exclusion = ex
			if run := runCase(t, st, c, Config{}); run.LoadFailed || run.Outcome != OutcomeExcluded {
				t.Errorf("%s: load failed %t, outcome %s; it is reported and not a load failure", ex, run.LoadFailed, run.Outcome)
			}
		}
	})
	t.Run("a case needs a plain id and a window", func(t *testing.T) {
		st, _ := world(t, memstore.Options{}, S)
		bad := svcCase("frontend", S, nil)
		if _, err := RunCase(ctx, st, bad, Config{}); err == nil {
			t.Error("a case named by an entity was accepted")
		}
		bad = svcCase("Scenario-8", S, nil)
		bad.To = bad.From
		if _, err := RunCase(ctx, st, bad, Config{}); err == nil {
			t.Error("an empty window was accepted")
		}
	})
}

// An uninstrumented truth service has no entity in the facts: it stays in the
// truth as a miss, is counted, and the variant that leaves it out raises recall.
func TestUninstrumentedTruthIsAMissAndCounted(t *testing.T) {
	S, X, U := svc(1), svc(2), svc(3)
	st, f := world(t, memstore.Options{}, S, X)
	f.write(obs(dependsOn(X, S), hm(9, 0)))
	run := runCase(t, st, svcCase("Scenario-9", S, []identity.Fingerprint{S, X, U}, X, U), Config{})
	w := scoreFor(t, run, LevelService, PredictorW)
	if counts(w) != [3]int{1, 1, 2} || w.Uninstrumented != 1 || run.Diag.Uninstrumented != 1 {
		t.Errorf("W = %v, uninstrumented %d (%d)", counts(w), w.Uninstrumented, run.Diag.Uninstrumented)
	}
	if v, _ := VariantNoUninstrumented.apply(w); v.T != 1 || v.Recall() != 1 {
		t.Errorf("without uninstrumented truth: truth %d, recall %v; want 1, 1", v.T, v.Recall())
	}
	if w.Recall() != 0.5 {
		t.Errorf("recall %v, want 0.5", w.Recall())
	}
}

// D8: nothing is written after the facts are loaded.
func TestNoWriteAfterLoad(t *testing.T) {
	S, X := svc(1), svc(2)
	st, f := world(t, memstore.Options{}, S, X)
	f.write(obs(dependsOn(X, S), hm(9, 0)))

	ro := ReadOnly(st)
	rec := obs(ent(svc(9)), hm(9, 0))
	rec.Seq = st.LastSeq() + 1
	if err := ro.Write(ctx, []store.Record{rec}); !errors.Is(err, ErrWriteAfterLoad) {
		t.Errorf("Write through the read-only store: %v", err)
	}
	if err := ro.Retain(ctx, hm(9, 0)); !errors.Is(err, ErrWriteAfterLoad) {
		t.Errorf("Retain through the read-only store: %v", err)
	}
	if got := ro.LastSeq(); got != st.LastSeq() {
		t.Errorf("a read-only store reports token %d, the store %d", got, st.LastSeq())
	}

	before := st.LastSeq()
	run := runCase(t, st, svcCase("Scenario-1", S, []identity.Fingerprint{S, X}, X), Config{})
	if st.LastSeq() != before || run.Token != before {
		t.Errorf("token moved from %d to %d (run read at %d)", before, st.LastSeq(), run.Token)
	}
}
