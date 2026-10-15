package blastscore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/blastradius"
	"github.com/lotannauo/toposhift/internal/query/coresidency"
	"github.com/lotannauo/toposhift/internal/query/extract"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// UnionDepth is how many hops the union-graph comparator and the direction
// diagnostic follow. It is the walk's default depth.
const UnionDepth = 6

// ErrWriteAfterLoad is what a store handed to the predictors answers to a Write
// or a Retain: nothing derived is ever written back to the facts.
var ErrWriteAfterLoad = errors.New("blastscore: a write or a retention after the facts were loaded")

// ReadOnly wraps a store so that it refuses every Write and Retain with
// [ErrWriteAfterLoad] and passes every read through.
func ReadOnly(st store.Store) store.Store { return readOnly{st} }

type readOnly struct{ store.Store }

func (readOnly) Write(context.Context, []store.Record) error { return ErrWriteAfterLoad }
func (readOnly) Retain(context.Context, time.Time) error     { return ErrWriteAfterLoad }

// Config configures a run. The zero Config runs every product query at its
// default policy, which is the only configuration a published score may use.
type Config struct {
	// Walk is the blast-radius walk's options. A scenario that needs a smaller
	// cap, to show a truncation, sets it here.
	Walk blastradius.Options
}

// Outcome says what became of a case.
type Outcome string

const (
	// OutcomeScored is a case with scores.
	OutcomeScored Outcome = "scored"
	// OutcomeExcluded is a case left out before any fact was read.
	OutcomeExcluded Outcome = "excluded"
	// OutcomeCannotSay is a case whose facts do not let it be scored.
	OutcomeCannotSay Outcome = "cannot_say"
)

// Skip is a predictor not scored on a case at a level, and why. T is the size of
// the truth it would have been scored against, so that a rule counting the cases
// a rule removed can tell which of them had a truth.
type Skip struct {
	Level     Level     `json:"level"`
	Predictor Predictor `json:"predictor"`
	Reason    Reason    `json:"reason"`
	T         int       `json:"truth"`
}

// Diagnostics are the counters of one case. The first block is computed from the
// facts and the predictions; the rest is what the loader reported.
type Diagnostics struct {
	// AgainstDirection counts truth services the walk missed that are reachable
	// callee-ward from the seed, which the failure direction of depends_on
	// excludes by design.
	AgainstDirection int `json:"against_direction"`
	// QuarantinedMiss counts truth entities the store quarantined, and
	// UnattributedMiss those it refused to place. Both are misses.
	QuarantinedMiss  int `json:"quarantined_miss"`
	UnattributedMiss int `json:"unattributed_miss"`
	// Uninstrumented counts truth services with no existence in the window.
	Uninstrumented int `json:"uninstrumented"`
	// SuppressedUbiquitous counts the service pairs, and SuppressedBelowMin the
	// co-resident pods, suppressed by their rules. None is a prediction.
	SuppressedUbiquitous int `json:"suppressed_ubiquitous"`
	SuppressedBelowMin   int `json:"suppressed_below_min"`
	// Truncated says some cap cut some answer of the case.
	Truncated bool `json:"truncated"`
	// Unmappable and Unplaced are the truth no rule could map or place, at both
	// levels.
	Unmappable int `json:"unmappable"`
	Unplaced   int `json:"unplaced"`
	// SnapshotKnown says the facts are Kubernetes pulls, and SnapshotInWindow that the
	// span of the pulls overlaps the window. The times themselves are not a result.
	SnapshotKnown    bool `json:"snapshot_known"`
	SnapshotInWindow bool `json:"snapshot_in_window"`
	// Nodes is the number of nodes of the cluster, 0 if unknown.
	Nodes int `json:"nodes_in_cluster"`
	Provenance
}

// CaseRun is the result of running every predictor on one case.
type CaseRun struct {
	ID    string `json:"-"`
	Class Class  `json:"class"`
	// Outcome says whether the case was scored. Exclusion is set for an excluded
	// case, Reason for one that cannot say.
	Outcome   Outcome   `json:"outcome"`
	Exclusion Exclusion `json:"exclusion,omitempty"`
	Reason    Reason    `json:"reason,omitempty"`
	// LoadFailed says the case fails the load check the dataset rule counts, and
	// only these two things do: its seed is absent from the store in the window,
	// or the facts hold no depends_on piece in the window. A quarantined seed, an
	// unmappable root and a missing window are reported and are not load failures.
	LoadFailed bool `json:"load_failed"`
	// Token is the snapshot token every read of the case used.
	Token uint64 `json:"token"`
	// Scores holds one score per level and predictor that ran, and Skips the ones
	// that did not.
	Scores []CaseScore `json:"scores,omitempty"`
	Skips  []Skip      `json:"skips,omitempty"`
	Diag   Diagnostics `json:"diagnostics"`
}

// RunCase runs every predictor and baseline on a case against a store holding
// its facts, at one snapshot token, through a store that refuses writes. The
// store is only read: if its last sequence number moved, RunCase fails.
func RunCase(ctx context.Context, st store.Store, c Case, cfg Config) (CaseRun, error) {
	if err := c.validate(); err != nil {
		return CaseRun{}, err
	}
	run := CaseRun{ID: c.ID, Class: c.Class, Outcome: OutcomeScored, Exclusion: c.Exclusion}
	run.Diag.Provenance = c.Provenance
	run.Diag.Nodes = c.Nodes
	if c.Exclusion != NotExcluded {
		run.Outcome = OutcomeExcluded
		return run, nil
	}

	ro := ReadOnly(st)
	run.Token = ro.LastSeq()
	w := interval.Window{From: c.From, To: c.To, AsOf: run.Token}
	x := &runner{ctx: ctx, st: ro, c: c, cfg: cfg, w: w, r: interval.NewReader(ro, interval.Options{}), run: &run}
	if err := x.run0(); err != nil {
		return CaseRun{}, fmt.Errorf("blastscore: case %s: %w", c.ID, err)
	}
	if got := st.LastSeq(); got != run.Token {
		return CaseRun{}, fmt.Errorf("blastscore: case %s: the store moved from token %d to %d while it was read", c.ID, run.Token, got)
	}
	return run, nil
}

type runner struct {
	ctx context.Context
	st  store.Store
	c   Case
	cfg Config
	w   interval.Window
	r   *interval.Reader
	run *CaseRun

	truncated bool
	// subjects are the entities the queries refused to place, for the counter.
	unattributed map[identity.Fingerprint]struct{}
}

func (x *runner) cannotSay(r Reason, loadFailed bool) {
	x.run.Outcome, x.run.Reason, x.run.LoadFailed = OutcomeCannotSay, r, loadFailed
}

func (x *runner) skip(l Level, p Predictor, r Reason, t Truth, seed identity.Fingerprint) {
	x.run.Skips = append(x.run.Skips, Skip{Level: l, Predictor: p, Reason: r, T: len(truthSet(t, seed)) + t.Unmappable + t.Unplaced})
}

func (x *runner) run0() error {
	c := x.c
	x.unattributed = map[identity.Fingerprint]struct{}{}

	// The load check: the seed exists in the window, is not quarantined, and for
	// a service seed the facts hold a depends_on piece.
	res, err := x.r.Entity(x.ctx, c.Seed, x.w)
	if err != nil {
		return err
	}
	el := res.Elements[0]
	switch {
	case el.Quarantine != nil:
		x.cannotSay(ReasonQuarantinedSeed, false)
		return nil
	case len(el.Pieces) == 0:
		x.cannotSay(ReasonSeedAbsent, true)
		return nil
	}
	service := c.Seed.Type() == catalog.Service
	if service {
		ok, err := x.anyDependsOn()
		if err != nil {
			return err
		}
		if !ok {
			x.cannotSay(ReasonNoDependsOn, true)
			return nil
		}
	}

	snapshotOK := true
	if !c.SnapshotTime.IsZero() {
		x.run.Diag.SnapshotKnown = true
		// The pulls overlap the window: the first is before its end and the last is not
		// before its start. For one pull, it is in the window.
		last := c.SnapshotLast
		if last.Before(c.SnapshotTime) {
			last = c.SnapshotTime
		}
		x.run.Diag.SnapshotInWindow = c.SnapshotTime.Before(c.To) && !last.Before(c.From)
		snapshotOK = x.run.Diag.SnapshotInWindow
	}
	singleNode := c.Nodes == 1

	// The walk is run once: it gives the service level for a service seed and the
	// pod level for a node seed.
	walked, err := blastradius.NewWalker(x.st).Impact(x.ctx, c.Seed, x.w, x.cfg.Walk)
	if err != nil {
		return err
	}
	x.truncated = x.truncated || cutWalk(walked.Truncated)
	for _, u := range walked.Unattributed {
		x.unattributed[u.Subject] = struct{}{}
	}

	if service {
		if err := x.serviceLevel(walked, snapshotOK, singleNode); err != nil {
			return err
		}
	}
	if err := x.podLevel(walked, service, snapshotOK, singleNode); err != nil {
		return err
	}

	for i := range x.run.Scores {
		x.run.Scores[i].Truncated = x.truncated
	}
	x.run.Diag.Truncated = x.truncated
	x.run.Diag.Unmappable = c.Service.Unmappable + c.Pod.Unmappable
	x.run.Diag.Unplaced = c.Service.Unplaced + c.Pod.Unplaced
	x.run.Diag.UnattributedMiss = x.countMissing(x.unattributed)
	return nil
}

// countMissing counts the truth entities, at either level and the seed aside,
// that are in set.
func (x *runner) countMissing(set map[identity.Fingerprint]struct{}) int {
	n := 0
	for _, t := range []Truth{x.c.Service, x.c.Pod} {
		for e := range truthSet(t, x.c.Seed) {
			if _, ok := set[e]; ok {
				n++
			}
		}
	}
	return n
}

// anyDependsOn reports whether any service the facts could name has a depends_on
// piece in the window.
func (x *runner) anyDependsOn() (bool, error) {
	fps := append([]identity.Fingerprint{x.c.Seed}, x.c.Services...)
	_, per, err := x.r.EdgesBatch(x.ctx, fps, store.Forward, x.w)
	if err != nil {
		return false, err
	}
	for _, els := range per {
		for _, el := range els {
			if el.Subject.Relation == catalog.DependsOn && len(el.Pieces) > 0 {
				return true, nil
			}
		}
	}
	return false, nil
}

// existence reads which of fps existed in the window and which the store
// quarantined.
func (x *runner) existence(fps []identity.Fingerprint) (present, quarantined map[identity.Fingerprint]struct{}, err error) {
	present, quarantined = map[identity.Fingerprint]struct{}{}, map[identity.Fingerprint]struct{}{}
	if len(fps) == 0 {
		return present, quarantined, nil
	}
	_, els, err := x.r.Entities(x.ctx, fps, x.w)
	if err != nil {
		return nil, nil, err
	}
	for i, el := range els {
		switch {
		case el.Quarantine != nil:
			quarantined[fps[i]] = struct{}{}
		case len(el.Pieces) > 0:
			present[fps[i]] = struct{}{}
		}
	}
	return present, quarantined, nil
}

// add scores a list and records the score.
func (x *runner) add(l Level, p Predictor, list []Prediction, t Truth, uninstrumented int) {
	s := scoreList(list, truthSet(t, x.c.Seed), map[identity.Fingerprint]struct{}{x.c.Seed: {}}, t, uninstrumented)
	s.Case, s.Level, s.Predictor = x.c.ID, l, p
	x.run.Scores = append(x.run.Scores, s)
}

func (x *runner) serviceLevel(walked blastradius.Result, snapshotOK, singleNode bool) error {
	c := x.c
	truth := truthSet(c.Service, c.Seed)
	truthList := make([]identity.Fingerprint, 0, len(truth))
	for e := range truth {
		truthList = append(truthList, e)
	}
	present, quarantined, err := x.existence(truthList)
	if err != nil {
		return err
	}
	uninstrumented := 0
	for e := range truth {
		if _, ok := present[e]; !ok {
			if _, q := quarantined[e]; !q {
				uninstrumented++
			}
		}
	}
	x.run.Diag.Uninstrumented = uninstrumented
	x.run.Diag.QuarantinedMiss += len(quarantined)

	wList := membersOf(walked, catalog.Service)
	x.add(LevelService, PredictorW, wList, c.Service, uninstrumented)

	// Co-residency of services, scored only where it can mean something.
	var cList []Prediction
	switch {
	case singleNode:
		x.skip(LevelService, PredictorCSvc, ReasonSingleNodeCase, c.Service, c.Seed)
		x.skip(LevelService, PredictorWC, ReasonSingleNodeCase, c.Service, c.Seed)
	case !snapshotOK:
		x.skip(LevelService, PredictorCSvc, ReasonSnapshotOutside, c.Service, c.Seed)
		x.skip(LevelService, PredictorWC, ReasonSnapshotOutside, c.Service, c.Seed)
	default:
		var err error
		cList, err = x.coresidentServices()
		if err != nil {
			return err
		}
		x.add(LevelService, PredictorCSvc, cList, c.Service, uninstrumented)
		x.add(LevelService, PredictorWC, mergeWC(wList, cList), c.Service, uninstrumented)
	}

	all, err := x.baseAll(c.Services, catalog.Service)
	if err != nil {
		return err
	}
	x.add(LevelService, PredictorBAll, all, c.Service, uninstrumented)
	one, err := x.baseOneHop()
	if err != nil {
		return err
	}
	x.add(LevelService, PredictorB1Hop, one, c.Service, uninstrumented)
	union, err := x.reach(store.Reverse)
	if err != nil {
		return err
	}
	x.add(LevelService, PredictorBUnion, union, c.Service, uninstrumented)

	// A truth service the walk missed that the seed reaches callee-ward.
	forward, err := x.reach(store.Forward)
	if err != nil {
		return err
	}
	fwd := map[identity.Fingerprint]struct{}{}
	for _, p := range forward {
		fwd[p.Entity] = struct{}{}
	}
	inW := map[identity.Fingerprint]struct{}{}
	for _, p := range wList {
		inW[p.Entity] = struct{}{}
	}
	for e := range truth {
		if _, hit := inW[e]; hit {
			continue
		}
		if _, ok := fwd[e]; ok {
			x.run.Diag.AgainstDirection++
		}
	}
	return nil
}

func (x *runner) podLevel(walked blastradius.Result, service, snapshotOK, singleNode bool) error {
	c := x.c
	truth := truthSet(c.Pod, c.Seed)
	truthList := make([]identity.Fingerprint, 0, len(truth))
	for e := range truth {
		truthList = append(truthList, e)
	}
	_, quarantined, err := x.existence(truthList)
	if err != nil {
		return err
	}
	x.run.Diag.QuarantinedMiss += len(quarantined)

	if !snapshotOK {
		// The Kubernetes pulls do not overlap the window: nothing at the pod level
		// says anything about it.
		if service {
			x.skip(LevelPod, PredictorWPod, ReasonSnapshotOutside, c.Pod, c.Seed)
			x.skip(LevelPod, PredictorC, ReasonSnapshotOutside, c.Pod, c.Seed)
			x.skip(LevelPod, PredictorWC, ReasonSnapshotOutside, c.Pod, c.Seed)
		} else {
			x.skip(LevelPod, PredictorW, ReasonSnapshotOutside, c.Pod, c.Seed)
		}
		return nil
	}

	if !service {
		x.add(LevelPod, PredictorW, membersOf(walked, catalog.K8sPod), c.Pod, 0)
		all, err := x.baseAll(c.Pods, catalog.K8sPod)
		if err != nil {
			return err
		}
		x.add(LevelPod, PredictorBAll, all, c.Pod, 0)
		return nil
	}

	// A service seed: the walk's service members projected onto pods, and the
	// co-residents of the seed's own pods. The seed pods are the seed here, so
	// the baseline does not name them either.
	wpod, err := x.projectPods(walked)
	if err != nil {
		return err
	}
	x.add(LevelPod, PredictorWPod, wpod, c.Pod, 0)
	all, err := x.baseAll(c.Pods, catalog.K8sPod)
	if err != nil {
		return err
	}
	all = slices.DeleteFunc(all, func(p Prediction) bool { return slices.Contains(c.SeedPods, p.Entity) })
	x.add(LevelPod, PredictorBAll, all, c.Pod, 0)

	switch {
	case len(c.SeedPods) == 0:
		x.skip(LevelPod, PredictorC, ReasonNoSeedPods, c.Pod, c.Seed)
		x.skip(LevelPod, PredictorWC, ReasonNoSeedPods, c.Pod, c.Seed)
	case singleNode:
		x.skip(LevelPod, PredictorC, ReasonSingleNodeCase, c.Pod, c.Seed)
		x.skip(LevelPod, PredictorWC, ReasonSingleNodeCase, c.Pod, c.Seed)
	default:
		list, err := x.coresidentPods()
		if err != nil {
			return err
		}
		x.add(LevelPod, PredictorC, list, c.Pod, 0)
		x.add(LevelPod, PredictorWC, mergeWC(wpod, list), c.Pod, 0)
	}
	return nil
}

// membersOf is the walk's members of one entity type, ranked as the walk ranks
// them, with the walk's depth as the hop.
func membersOf(res blastradius.Result, typ catalog.EntityType) []Prediction {
	var out []Prediction
	for _, m := range res.Members {
		if m.Entity.Type() == typ {
			out = append(out, Prediction{Entity: m.Entity, Hop: m.Depth})
		}
	}
	return out
}

// mergeWC ranks the walk's list and then the co-residency list by hop, the walk
// first among equals and each list in its own order.
func mergeWC(w, c []Prediction) []Prediction {
	out := append(slices.Clone(w), c...)
	sort.SliceStable(out, func(i, j int) bool { return out[i].Hop < out[j].Hop })
	return out
}

func cutWalk(t blastradius.Truncation) bool {
	return len(t.Depth) > 0 || len(t.Frontier) > 0 || t.Members > 0
}

func cutFind(t coresidency.Truncation) bool {
	return len(t.Nodes) > 0 || len(t.Hosts) > 0 || len(t.PerVia) > 0 || t.Results > 0 || t.SuppressedDropped > 0
}

func cutExtract(t extract.Truncation) bool {
	return len(t.Members) > 0 || len(t.Instances) > 0 || len(t.SeedPods) > 0 || t.Pairs > 0 || t.SuppressedDropped > 0
}

// coresidentPods unions the co-residents of every seed pod: a pod keeps the
// longest overlap it has with any, its hop is 1, and a seed pod is never a
// prediction. Suppressed entries are counted.
func (x *runner) coresidentPods() ([]Prediction, error) {
	f := coresidency.NewFinder(x.st)
	best := map[identity.Fingerprint]time.Duration{}
	for _, seed := range x.c.SeedPods {
		res, err := f.Find(x.ctx, seed, x.w, coresidency.Options{NodeOnly: true})
		if err != nil {
			return nil, err
		}
		x.truncated = x.truncated || cutFind(res.Truncated)
		x.run.Diag.SuppressedBelowMin += len(res.Suppressed)
		for _, u := range res.Unattributed {
			x.unattributed[u.Subject] = struct{}{}
		}
		for _, cr := range res.CoResidents {
			best[cr.Peer] = max(best[cr.Peer], cr.Duration)
		}
	}
	for _, seed := range x.c.SeedPods {
		delete(best, seed)
	}
	return rankByDuration(best), nil
}

// coresidentServices is the other service of every pair the seed service is in,
// whichever side of the pair it is on. Pairs of two other members are not about
// the seed. Suppressed pairs are counted.
func (x *runner) coresidentServices() ([]Prediction, error) {
	res, err := extract.NewExtractor(x.st).Extract(x.ctx, extract.Exploratory{Service: x.c.Seed}, x.w, extract.Options{NodeOnly: true})
	if err != nil {
		return nil, err
	}
	x.truncated = x.truncated || cutExtract(res.Truncated)
	for _, pr := range res.PodResults {
		x.truncated = x.truncated || cutFind(pr.Truncated)
	}
	for _, u := range res.Unattributed {
		x.unattributed[u.Subject] = struct{}{}
	}
	other := func(p extract.Pair) (identity.Fingerprint, bool) {
		switch x.c.Seed {
		case p.Member:
			return p.Peer, true
		case p.Peer:
			return p.Member, true
		}
		return identity.Fingerprint{}, false
	}
	for _, p := range res.Suppressed {
		if _, ok := other(p); ok {
			x.run.Diag.SuppressedUbiquitous++
		}
	}
	best := map[identity.Fingerprint]time.Duration{}
	for _, p := range res.Pairs {
		if o, ok := other(p); ok {
			best[o] = max(best[o], p.Duration)
		}
	}
	return rankByDuration(best), nil
}

// rankByDuration ranks entities by duration, longest first, then by fingerprint,
// all at hop 1.
func rankByDuration(d map[identity.Fingerprint]time.Duration) []Prediction {
	out := make([]Prediction, 0, len(d))
	for e := range d {
		out = append(out, Prediction{Entity: e, Hop: 1})
	}
	slices.SortFunc(out, func(a, b Prediction) int {
		if c := cmp.Compare(d[b.Entity], d[a.Entity]); c != 0 {
			return c
		}
		return store.CompareFingerprints(a.Entity, b.Entity)
	})
	return out
}

// baseAll is B_all: every entity of the universe that existed in the window, in
// fingerprint order, with no hop.
func (x *runner) baseAll(universe []identity.Fingerprint, typ catalog.EntityType) ([]Prediction, error) {
	fps := slices.DeleteFunc(slices.Clone(universe), func(e identity.Fingerprint) bool { return e.Type() != typ })
	present, _, err := x.existence(fps)
	if err != nil {
		return nil, err
	}
	var out []Prediction
	for e := range present {
		out = append(out, Prediction{Entity: e})
	}
	slices.SortFunc(out, func(a, b Prediction) int { return store.CompareFingerprints(a.Entity, b.Entity) })
	return out, nil
}

// baseOneHop is B_1hop: the services one depends_on edge from the seed, in
// either direction, ranked by the time the edge was live.
func (x *runner) baseOneHop() ([]Prediction, error) {
	best := map[identity.Fingerprint]time.Duration{}
	for _, dir := range []store.Direction{store.Forward, store.Reverse} {
		res, err := x.r.Edges(x.ctx, x.c.Seed, dir, x.w)
		if err != nil {
			return nil, err
		}
		for _, el := range res.Elements {
			if el.Subject.Relation != catalog.DependsOn || len(el.Pieces) == 0 {
				continue
			}
			peer := el.Subject.B
			if dir == store.Reverse {
				peer = el.Subject.A
			}
			if peer.Type() != catalog.Service || peer == x.c.Seed {
				continue
			}
			best[peer] = max(best[peer], interval.Duration(interval.Spans(el)))
		}
	}
	return rankByDuration(best), nil
}

// reach follows depends_on from the seed, up to UnionDepth hops, over every edge
// that has a piece in the window, whenever it was live. Reverse is the callers'
// side, the direction a failure travels: the union-graph comparator. Forward is
// the callee side, against it. The hop is the depth, and the list is in order of
// depth, then fingerprint.
func (x *runner) reach(dir store.Direction) ([]Prediction, error) {
	seen := map[identity.Fingerprint]struct{}{x.c.Seed: {}}
	frontier := []identity.Fingerprint{x.c.Seed}
	var out []Prediction
	for depth := 1; depth <= UnionDepth && len(frontier) > 0; depth++ {
		_, per, err := x.r.EdgesBatch(x.ctx, frontier, dir, x.w)
		if err != nil {
			return nil, err
		}
		var next []identity.Fingerprint
		for _, els := range per {
			for _, el := range els {
				if el.Subject.Relation != catalog.DependsOn || len(el.Pieces) == 0 {
					continue
				}
				peer := el.Subject.A
				if dir == store.Forward {
					peer = el.Subject.B
				}
				if _, ok := seen[peer]; ok {
					continue
				}
				seen[peer] = struct{}{}
				next = append(next, peer)
			}
		}
		slices.SortFunc(next, store.CompareFingerprints)
		for _, e := range next {
			out = append(out, Prediction{Entity: e, Hop: depth})
		}
		frontier = next
	}
	return out, nil
}
