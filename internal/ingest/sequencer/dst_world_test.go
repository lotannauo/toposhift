package sequencer_test

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The simulated cluster of the seeded simulation of ingest (see dst_test.go). All
// times in a dstWorld are the cluster's own clock, which is the ingest clock; the
// producer's clock, when it is offset, shows only in the times written into the
// requests.

const (
	// dstCluster is the cluster name on every resource.
	dstCluster = "c1"
	// dstNodeCount nodes never change; dstPodsAtT0 pods exist at t0 and at most
	// dstMaxPods at once.
	dstNodeCount = 3
	dstPodsAtT0  = 12
	dstMaxPods   = 20
	// dstWarmup is the stretch with no fault, and dstRunLen the length of a run.
	dstWarmup = time.Hour
	dstRunLen = 4 * time.Hour
	// dstStep is the flush tick, dstPullEvery the interval of both receivers.
	dstStep      = 30 * time.Second
	dstPullEvery = 5 * time.Minute
	// dstTTL is the entity TTL: three intervals.
	dstTTL = 3 * dstPullEvery
	// dstGridEvery is the spacing of the instants the answers are compared at,
	// offset from the pulls so that an interval of three minutes or more holds one.
	dstGridEvery = 150 * time.Second
	dstGridFrom  = dstWarmup + 75*time.Second
	// dstPulls is the number of pull instants, from t0 to the end inclusive.
	dstPulls = int(dstRunLen/dstPullEvery) + 1
)

func dstEnd() time.Time { return t0.Add(dstRunLen) }

// dstCeil rounds an instant up to the next flush tick.
func dstCeil(t time.Time) time.Time {
	d := t.Sub(t0)
	if r := d % dstStep; r != 0 {
		d += dstStep - r
	}
	return t0.Add(d)
}

// dstGridInstants are the instants the answers are compared at.
func dstGridInstants() []time.Time {
	var out []time.Time
	for tm := t0.Add(dstGridFrom); !tm.After(dstEnd()); tm = tm.Add(dstGridEvery) {
		out = append(out, tm)
	}
	return out
}

// dstTruth is when a subject existed in the cluster: born, and died (zero for
// never).
type dstTruth struct{ born, died time.Time }

func (tr dstTruth) existsAt(t time.Time) bool {
	return !t.Before(tr.born) && (tr.died.IsZero() || t.Before(tr.died))
}

// dstSub is a subject the cluster ever had that the answers are read for.
type dstSub struct {
	fp   identity.Fingerprint
	kind string // node, pod or ctr
	name string
	dstTruth
}

type dstEdgeKey struct {
	a, b identity.Fingerprint
	rel  catalog.RelationType
}

// dstPod is a pod over its life. The container is created with the pod, and
// starts a minute later, when the pod is Running. The translators time a
// container's first record, and its part_of edge, at its pod's creation, so
// that is when the truth has the container born.
type dstPod struct {
	uid, name, ctr string
	node           int
	created        time.Time
	sched          time.Time
	started        time.Time
	died           time.Time
	condAt         []time.Time
	fp, ctrFP      identity.Fingerprint
}

func (p *dstPod) existsAt(t time.Time) bool {
	return !p.created.After(t) && (p.died.IsZero() || p.died.After(t))
}

// snapshot is the pod as the cluster shows it at t, with the producer's clock
// offset by k.
func (p *dstPod) snapshot(t time.Time, k time.Duration) *rsPod {
	rp := &rsPod{
		uid: p.uid, name: p.name, node: fmt.Sprintf("n%d", p.node),
		created: p.created.Add(k), scheduled: p.sched.Add(k), phase: "Pending",
	}
	if !t.Before(p.started) {
		rp.phase, rp.ctrID, rp.ctrStarted = "Running", p.ctr, p.started.Add(k)
	}
	for i, c := range p.condAt {
		if !t.Before(c) {
			rp.conds = append(rp.conds, map[string]any{
				"type": fmt.Sprintf("Extra%d", i), "status": "True", "lastTransitionTime": ts(c.Add(k)),
			})
		}
	}
	return rp
}

type dstReqKind int

const (
	dstPullReq dstReqKind = iota
	dstReportReq
	dstWatchReq
)

func (k dstReqKind) String() string {
	return [...]string{"pull", "report", "watch"}[k]
}

// dstListed is a pod a pull lists, and its node.
type dstListed struct{ pod, node identity.Fingerprint }

// dstReq is one request of a receiver, at the instant it is first sent.
type dstReq struct {
	id     int
	at     time.Time
	kind   dstReqKind
	pull   int // the pull instant's number, for a pull and a report
	data   capture.LogsData
	listed []dstListed // a pull's
}

// dstWorld is the cluster of a seed, its truth, and the requests its receivers
// send.
type dstWorld struct {
	k       time.Duration
	nodes   []rsNode
	nodeFPs []identity.Fingerprint
	pods    []*dstPod
	reqs    []*dstReq
	subs    []dstSub
	bySub   map[identity.Fingerprint]int
	edges   map[dstEdgeKey]dstTruth
}

func (w *dstWorld) addSub(fp identity.Fingerprint, kind, name string, born, died time.Time) {
	w.bySub[fp] = len(w.subs)
	w.subs = append(w.subs, dstSub{fp: fp, kind: kind, name: name, dstTruth: dstTruth{born, died}})
}

type dstChange struct {
	at      time.Time
	typ     string
	p       *dstPod
	watched bool
}

// newDSTWorld draws the cluster of a seed: three nodes and twelve pods at t0, and
// per pull interval 0-2 pods created, 0-1 deleted, and a new condition on a running
// pod with probability 1/4. k is the producer's clock offset. force lists the
// intervals that create at least one pod that no watch event shows, so that the
// next pull carries a first sighting.
func newDSTWorld(t testing.TB, draw uint64, k time.Duration, force map[int]bool) *dstWorld {
	t.Helper()
	r := rand.New(rand.NewPCG(draw, 0x776f726c64))
	w := &dstWorld{k: k, bySub: map[identity.Fingerprint]int{}, edges: map[dstEdgeKey]dstTruth{}}
	for i := range dstNodeCount {
		n := rsNode{uid: fmt.Sprintf("node-%d", i), name: fmt.Sprintf("n%d", i), created: t0.Add(-48 * time.Hour)}
		w.nodes = append(w.nodes, n)
		fp := nodeFP(t, n.uid)
		w.nodeFPs = append(w.nodeFPs, fp)
		w.addSub(fp, "node", n.name, n.created, time.Time{})
	}
	newPod := func(created time.Time) *dstPod {
		n := len(w.pods)
		p := &dstPod{
			uid: fmt.Sprintf("pod-%03d", n), name: fmt.Sprintf("p%03d", n), ctr: fmt.Sprintf("ctr-%03d", n),
			node:    r.IntN(dstNodeCount),
			created: created, sched: created.Add(30 * time.Second), started: created.Add(time.Minute),
		}
		p.fp, p.ctrFP = podFP(t, p.uid), ctrFP(t, p.ctr)
		w.pods = append(w.pods, p)
		return p
	}
	var changes []dstChange
	for range dstPodsAtT0 {
		newPod(t0.Add(-time.Duration(1+r.IntN(23)) * time.Hour))
	}
	living := func() int {
		n := 0
		for _, p := range w.pods {
			if p.died.IsZero() {
				n++
			}
		}
		return n
	}
	for j := 0; j < dstPulls-1; j++ {
		p0 := t0.Add(time.Duration(j) * dstPullEvery)
		nCreate := r.IntN(3)
		if force[j] && nCreate == 0 {
			nCreate = 1
		}
		for c := range nCreate {
			wa, ws := r.IntN(2) == 0, r.IntN(2) == 0
			if force[j] && c == 0 {
				wa, ws = false, false
			}
			if living() >= dstMaxPods {
				continue
			}
			p := newPod(p0.Add(time.Minute))
			changes = append(changes,
				dstChange{dstCeil(p.created.Add(5 * time.Second)), "ADDED", p, wa},
				dstChange{dstCeil(p.started.Add(5 * time.Second)), "MODIFIED", p, ws})
		}
		var deleted *dstPod
		if r.IntN(2) == 0 {
			var cand []*dstPod
			for _, p := range w.pods {
				if !p.created.After(p0) && p.died.IsZero() {
					cand = append(cand, p)
				}
			}
			if len(cand) > 6 {
				deleted = cand[r.IntN(len(cand))]
				deleted.died = p0.Add(2 * time.Minute)
				changes = append(changes, dstChange{dstCeil(deleted.died.Add(5 * time.Second)), "DELETED", deleted, r.IntN(2) == 0})
			}
		}
		if r.IntN(4) == 0 {
			var cand []*dstPod
			for _, p := range w.pods {
				if !p.started.After(p0) && p.died.IsZero() && p != deleted {
					cand = append(cand, p)
				}
			}
			if len(cand) > 0 {
				p := cand[r.IntN(len(cand))]
				at := p0.Add(3 * time.Minute)
				p.condAt = append(p.condAt, at)
				changes = append(changes, dstChange{dstCeil(at.Add(5 * time.Second)), "MODIFIED", p, r.IntN(2) == 0})
			}
		}
	}
	for _, p := range w.pods {
		w.addSub(p.fp, "pod", p.name, p.created, p.died)
		w.addSub(p.ctrFP, "ctr", p.ctr, p.created, p.died)
		w.edges[dstEdgeKey{p.fp, w.nodeFPs[p.node], catalog.ScheduledOn}] = dstTruth{p.sched, p.died}
		w.edges[dstEdgeKey{p.ctrFP, p.fp, catalog.PartOf}] = dstTruth{p.created, p.died}
	}

	rw := func(tm time.Time, pods []*rsPod) *rsWorld {
		return &rsWorld{nodes: w.nodes, pods: pods, cluster: dstCluster}
	}
	for i := range dstPulls {
		p := t0.Add(time.Duration(i) * dstPullEvery)
		var snaps []*rsPod
		var listed []dstListed
		for _, pod := range w.pods {
			if pod.existsAt(p) {
				snaps = append(snaps, pod.snapshot(p, k))
				listed = append(listed, dstListed{pod.fp, w.nodeFPs[pod.node]})
			}
		}
		world := rw(p, snaps)
		w.reqs = append(w.reqs, &dstReq{at: p, kind: dstPullReq, pull: i, data: world.pull(p), listed: listed})
		report := world.report(p.Add(k))
		recs := &report.ResourceLogs[0].ScopeLogs[0].LogRecords
		for _, pod := range w.pods {
			if !pod.died.IsZero() && pod.died.After(p.Add(-dstPullEvery)) && !pod.died.After(p) {
				*recs = append(*recs, entity("entity.delete", "k8s.pod", "k8s.pod.uid", pod.uid, p.Add(k), nil))
			}
		}
		w.reqs = append(w.reqs, &dstReq{at: p, kind: dstReportReq, pull: i, data: report})
	}
	for _, c := range changes {
		if !c.watched || c.at.After(dstEnd()) {
			continue
		}
		obj := c.p.snapshot(c.at, k).object()
		rec := objRec(c.at, map[string]any{"type": c.typ, "object": obj}, "pods")
		w.reqs = append(w.reqs, &dstReq{at: c.at, kind: dstWatchReq, data: logs(dstCluster, rec)})
	}
	slices.SortStableFunc(w.reqs, func(a, b *dstReq) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return int(a.kind) - int(b.kind)
	})
	for i, rq := range w.reqs {
		rq.id = i
	}
	return w
}

// edgeOf is the truth of an edge.
func (w *dstWorld) edgeOf(a, b identity.Fingerprint, rel catalog.RelationType) (dstTruth, bool) {
	tr, ok := w.edges[dstEdgeKey{a, b, rel}]
	return tr, ok
}

// nameOf is the name of a subject, for a message.
func (w *dstWorld) nameOf(fp identity.Fingerprint) string {
	if i, ok := w.bySub[fp]; ok {
		return w.subs[i].name
	}
	return fp.String()
}
