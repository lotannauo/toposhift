package blastscore

import (
	"slices"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// projection is W->pod's ranked list for a case, as the runner builds it.
func projection(t *testing.T, st store.Store, c Case) []Prediction {
	t.Helper()
	ro := ReadOnly(st)
	w := win(ro.LastSeq())
	x := &runner{ctx: ctx, st: ro, c: c, w: w, r: interval.NewReader(ro, interval.Options{}), unattributed: map[identity.Fingerprint]struct{}{}}
	walked, err := blastradius.NewWalker(ro).Impact(ctx, c.Seed, w, blastradius.Options{})
	if err != nil {
		t.Fatal(err)
	}
	list, err := x.projectPods(walked)
	if err != nil {
		t.Fatal(err)
	}
	return list
}

// podCase is a service-level case over the given services with a pod-level truth.
func podCase(id string, seed identity.Fingerprint, services, pods []identity.Fingerprint, truth ...identity.Fingerprint) Case {
	c := svcCase(id, seed, services)
	c.Pods = pods
	c.Pod = Truth{Entities: truth}
	return c
}

// W->pod: B depends on S and A depends on B, so the walk from S reaches B at
// hop 1 and A at hop 2. B runs in pod pB and A in pA; the seed's own pod pS is
// the seed and is never named. The projection names pB at hop 1 and pA at hop
// 2; with the truth {pA, pX} it has precision 1/2 and recall 1/2.
func TestWPodProjectsServiceMembersOntoPods(t *testing.T) {
	S, B, A := svc(1), svc(2), svc(3)
	iS, iB, iA := inst(1), inst(2), inst(3)
	pS, pB, pA, pX := pod(1), pod(2), pod(3), pod(4)
	st, f := world(t, memstore.Options{}, S, B, A, iS, iB, iA, pS, pB, pA, pX)
	f.write(obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(A, B), hm(9, 0)))
	place(f, S, iS, pS)
	place(f, B, iB, pB)
	place(f, A, iA, pA)
	c := podCase("Scenario-1", S, []identity.Fingerprint{S, B, A}, []identity.Fingerprint{pS, pB, pA, pX}, pA, pX)
	c.SeedPods = []identity.Fingerprint{pS}
	c.Nodes = 2

	run := runCase(t, st, c, Config{})
	w := scoreFor(t, run, LevelPod, PredictorWPod)
	if counts(w) != [3]int{2, 1, 2} || w.Precision() != 0.5 || w.Recall() != 0.5 {
		t.Errorf("W->pod n,hits,truth = %v, P %v, R %v; want [2 1 2], 0.5, 0.5", counts(w), w.Precision(), w.Recall())
	}
	if w.HopN[1] != 1 || w.HopN[2] != 1 || w.HopHits[1] != 0 || w.HopHits[2] != 1 {
		t.Errorf("by hop: n %v, hits %v; want pB at 1 (a miss) and pA at 2 (a hit)", w.HopN, w.HopHits)
	}
	// B_all names the pods that existed, the seed pod apart.
	if got := counts(scoreFor(t, run, LevelPod, PredictorBAll)); got != [3]int{3, 2, 2} {
		t.Errorf("B_all = %v, want [3 2 2]", got)
	}
}

// The projection's order is hop, then the time it holds, then fingerprint, and
// the list names each pod once.
func TestWPodRanksByHopThenDuration(t *testing.T) {
	S, B, A := svc(1), svc(2), svc(3)
	iB1, iB2, iA := inst(1), inst(2), inst(3)
	p1, p2, p3 := pod(1), pod(2), pod(3)
	st, f := world(t, memstore.Options{}, S, B, A, iB1, iB2, iA, p1, p2, p3)
	f.write(obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(A, S), hm(9, 0)))
	f.write(obs(edge(iB1, B, catalog.InstanceOf), hm(9, 0)), obs(edge(iB2, B, catalog.InstanceOf), hm(9, 0)))
	f.write(obs(edge(iA, A, catalog.InstanceOf), hm(9, 0)))
	// p1 holds the projection for 10 minutes, p2 for the whole window, p3 (A, also
	// hop 1) for 5 minutes.
	f.write(live(edge(iB1, p1, catalog.PartOf), hm(10, 0), hm(10, 10))...)
	f.write(obs(edge(iB2, p2, catalog.PartOf), hm(9, 0)))
	f.write(live(edge(iA, p3, catalog.PartOf), hm(10, 5), hm(10, 10))...)
	c := podCase("Scenario-1", S, []identity.Fingerprint{S, B, A}, []identity.Fingerprint{p1, p2, p3})

	run := runCase(t, st, c, Config{})
	got := scoreFor(t, run, LevelPod, PredictorWPod)
	if got.N != 3 {
		t.Fatalf("W->pod n = %d, want 3", got.N)
	}
	list := projection(t, st, c)
	if want := []identity.Fingerprint{p2, p1, p3}; !slices.Equal(fpsOf(list), want) {
		t.Errorf("W->pod = %v, want [p2 p1 p3]: longest first at one hop", fpsOf(list))
	}
}

// An instance part of two pods at once names neither over the overlap. Here
// the instance is in p1 and p2 from 10:00 to 10:10, and in p1 alone afterwards:
// p2 is never named, p1 is, and the truth p2 is counted as unattributed.
func TestWPodSeveralPodsNameNeither(t *testing.T) {
	S, B := svc(1), svc(2)
	iB := inst(1)
	p1, p2 := pod(1), pod(2)
	st, f := world(t, memstore.Options{}, S, B, iB, p1, p2)
	f.write(obs(dependsOn(B, S), hm(9, 0)), obs(edge(iB, B, catalog.InstanceOf), hm(9, 0)))
	f.write(obs(edge(iB, p1, catalog.PartOf), hm(9, 0)))
	f.write(live(edge(iB, p2, catalog.PartOf), hm(10, 0), hm(10, 10))...)
	c := podCase("Scenario-1", S, []identity.Fingerprint{S, B}, []identity.Fingerprint{p1, p2}, p2)

	run := runCase(t, st, c, Config{})
	w := scoreFor(t, run, LevelPod, PredictorWPod)
	if counts(w) != [3]int{1, 0, 1} {
		t.Errorf("W->pod = %v, want [1 0 1]: p1 only, from 10:10", counts(w))
	}
	if run.Diag.UnattributedMiss != 1 {
		t.Errorf("unattributed_miss = %d, want 1", run.Diag.UnattributedMiss)
	}

	t.Run("over the whole window neither is named", func(t *testing.T) {
		st, f := world(t, memstore.Options{}, S, B, iB, p1, p2)
		f.write(obs(dependsOn(B, S), hm(9, 0)), obs(edge(iB, B, catalog.InstanceOf), hm(9, 0)))
		f.write(obs(edge(iB, p1, catalog.PartOf), hm(9, 0)), obs(edge(iB, p2, catalog.PartOf), hm(9, 0)))
		run := runCase(t, st, podCase("Scenario-1", S, []identity.Fingerprint{S, B}, []identity.Fingerprint{p1, p2}, p1, p2), Config{})
		if w := scoreFor(t, run, LevelPod, PredictorWPod); w.N != 0 || w.T != 2 {
			t.Errorf("W->pod n = %d, truth = %d, want 0, 2", w.N, w.T)
		}
		if run.Diag.UnattributedMiss != 2 {
			t.Errorf("unattributed_miss = %d, want 2", run.Diag.UnattributedMiss)
		}
	})
}

// An instance of two services at once names neither over the overlap, so its
// pod is named through neither service.
func TestWPodSeveralServicesNameNeither(t *testing.T) {
	S, B, A := svc(1), svc(2), svc(3)
	i := inst(1)
	p := pod(1)
	st, f := world(t, memstore.Options{}, S, B, A, i, p)
	f.write(obs(dependsOn(B, S), hm(9, 0)), obs(dependsOn(A, S), hm(9, 0)))
	f.write(obs(edge(i, B, catalog.InstanceOf), hm(9, 0)), obs(edge(i, A, catalog.InstanceOf), hm(9, 0)))
	f.write(obs(edge(i, p, catalog.PartOf), hm(9, 0)))
	run := runCase(t, st, podCase("Scenario-1", S, []identity.Fingerprint{S, B, A}, []identity.Fingerprint{p}, p), Config{})
	if w := scoreFor(t, run, LevelPod, PredictorWPod); w.N != 0 {
		t.Errorf("W->pod n = %d, want 0: the instance maps to two services", w.N)
	}
}

// Every leg of the path must be live at once. B reaches S only in the first ten
// minutes, and the instance is in its pod only in the last ten: the pod is named
// by no instant.
func TestWPodLegsMustBeLiveAtOnce(t *testing.T) {
	S, B := svc(1), svc(2)
	i := inst(1)
	p := pod(1)
	st, f := world(t, memstore.Options{}, S, B, i, p)
	f.write(live(dependsOn(B, S), hm(10, 0), hm(10, 10))...)
	f.write(obs(edge(i, B, catalog.InstanceOf), hm(9, 0)))
	f.write(live(edge(i, p, catalog.PartOf), hm(10, 20), hm(10, 30))...)
	run := runCase(t, st, podCase("Scenario-1", S, []identity.Fingerprint{S, B}, []identity.Fingerprint{p}, p), Config{})
	if w := scoreFor(t, run, LevelPod, PredictorWPod); w.N != 0 {
		t.Errorf("W->pod n = %d, want 0", w.N)
	}
}

// With no instance there is nothing to project through: the list is empty and
// its precision is undefined, not zero.
func TestWPodWithNoInstancesIsEmpty(t *testing.T) {
	S, B := svc(1), svc(2)
	p := pod(1)
	st, f := world(t, memstore.Options{}, S, B, p)
	f.write(obs(dependsOn(B, S), hm(9, 0)))
	run := runCase(t, st, podCase("Scenario-1", S, []identity.Fingerprint{S, B}, []identity.Fingerprint{p}, p), Config{})
	w := scoreFor(t, run, LevelPod, PredictorWPod)
	if w.N != 0 || w.T != 1 {
		t.Errorf("W->pod n = %d, truth = %d, want 0, 1", w.N, w.T)
	}
	if p := w.Precision(); p == p {
		t.Errorf("precision = %v, want undefined", p)
	}
}

// W+C at pod level ranks W->pod and the co-residents of the seed's pods by hop,
// the projection first among equals.
func TestPodLevelWCCombinesProjectionAndCoResidency(t *testing.T) {
	S, B := svc(1), svc(2)
	iS, iB := inst(1), inst(2)
	pS, pB, pCo := pod(1), pod(2), pod(3)
	n1 := node(1)
	st, f := world(t, memstore.Options{}, S, B, iS, iB, pS, pB, pCo, n1)
	f.write(obs(dependsOn(B, S), hm(9, 0)))
	place(f, S, iS, pS)
	place(f, B, iB, pB)
	f.write(obs(sched(pS, n1), hm(9, 0)), obs(sched(pCo, n1), hm(9, 0)), obs(sched(pB, node(2)), hm(9, 0)))
	c := podCase("Scenario-1", S, []identity.Fingerprint{S, B}, []identity.Fingerprint{pS, pB, pCo}, pB, pCo)
	c.SeedPods = []identity.Fingerprint{pS}
	c.Nodes = 2

	run := runCase(t, st, c, Config{})
	if got := counts(scoreFor(t, run, LevelPod, PredictorWPod)); got != [3]int{1, 1, 2} {
		t.Errorf("W->pod = %v, want [1 1 2]", got)
	}
	if got := counts(scoreFor(t, run, LevelPod, PredictorC)); got != [3]int{1, 1, 2} {
		t.Errorf("C = %v, want [1 1 2]", got)
	}
	if got := counts(scoreFor(t, run, LevelPod, PredictorWC)); got != [3]int{2, 2, 2} {
		t.Errorf("W+C = %v, want [2 2 2]", got)
	}
}
