package demo

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// The script is what happens to the cluster in the six hours, as data on the
// world's pods and nodes. Instants are after the anchor; each pod-level instant
// then takes up to twenty seconds of jitter from the seed. The named instants
// (the node failing, the eviction's delay, the node's recovery) do not.
//
//   - 1h30m: the frontend is rolled out to a new ReplicaSet. One pod at a time is
//     created, runs about twenty seconds later, and then one old pod is deleted.
//   - 0h15m and every hour after: a Job's pod runs for four minutes, succeeds, and
//     is deleted ten minutes later.
//   - 3h00m: node-2's kubelet reports it not ready, because the runtime is not up.
//     The node agent on it keeps reporting.
//   - 3h05m: the not-ready taint's three hundred seconds pass, and node-2's pods
//     are deleted, except those a DaemonSet keeps on every node. Each replacement
//     is created a second after, on another node, and runs about thirty seconds
//     later.
//   - 4h30m: node-2 is ready again. Nothing moves back.
//
// A simplification: an evicted pod is deleted at once. The API keeps it
// terminating until the kubelet confirms, which it can here because the kubelet
// is up.
const (
	// failedNode is node-2, the node that fails.
	failedNode = 1

	// rolledOut is the Deployment of the namespace shop that is rolled out.
	rolledOut = "frontend"

	rolloutAfter = 90 * time.Minute
	rolloutGap   = 90 * time.Second

	jobFirst = 15 * time.Minute
	jobEvery = time.Hour
	jobRuns  = 4 * time.Minute
	jobKept  = 10 * time.Minute
	jobCount = 6

	notReadyAfter   = 3 * time.Hour
	evictAfter      = 5 * time.Minute
	readyAgainAfter = 4*time.Hour + 30*time.Minute

	// jitter bounds the seconds drawn onto a pod-level instant: up to twenty.
	jitter = 21
)

// outage is node-2 not ready, from T3 to its recovery.
func (b *builder) outage() {
	b.w.Outages = append(b.w.Outages, Outage{
		Node: failedNode,
		From: anchor.Add(notReadyAfter),
		To:   anchor.Add(readyAgainAfter),
	})
}

// rollout replaces the frontend's pods one at a time: the new pod first, and the
// old one deleted once it runs.
func (b *builder) rollout() {
	wi := b.w.workloadIndex("shop", rolledOut)
	wl := b.w.Workloads[wi]
	var olds []*Pod
	for _, p := range b.w.Pods {
		if p.Workload == wi {
			olds = append(olds, p)
		}
	}
	for i, old := range olds {
		created := anchor.Add(rolloutAfter + time.Duration(i)*rolloutGap + b.seconds(jitter))
		p := b.newPod(wi, ownerOf(wl, wl.NextReplicaSet), "1.1.0", created, 20*time.Second+b.seconds(3))
		b.place(p, b.pick(created))
		old.Deleted = p.Started.Add(5 * time.Second)
	}
}

// jobs adds the hourly report Job's pods.
func (b *builder) jobs() {
	wi := b.w.workloadIndex("platform", "report")
	for n := range jobCount {
		created := anchor.Add(jobFirst + time.Duration(n)*jobEvery + b.seconds(jitter))
		owner := fmt.Sprintf("report-%d", n+1)
		p := b.newPod(wi, owner, "1.0.0", created, 3*time.Second+b.seconds(5))
		p.Name = b.unique(func() string { return owner + "-" + b.suffix() })
		p.Finished = p.Started.Add(jobRuns)
		p.Deleted = p.Finished.Add(jobKept)
		b.place(p, b.pick(created))
	}
}

// evict deletes the pods of the failed node when the taint's seconds have passed,
// and creates each one's replacement a second after, on a node that is ready. A
// StatefulSet's replacement keeps its name.
func (b *builder) evict() {
	out := b.w.Outages[0]
	at := out.From.Add(evictAfter)
	var victims []*Pod
	for _, p := range b.w.podsOn(out.Node, at) {
		if b.w.evictable(p) {
			victims = append(victims, p)
		}
	}
	for _, v := range victims {
		v.Deleted = at.Add(b.seconds(jitter))
		created := v.Deleted.Add(time.Second)
		p := b.newPod(v.Workload, v.Owner, v.ImageTag, created, 30*time.Second+b.seconds(6))
		if b.w.Workloads[v.Workload].Kind == kindStatefulSet {
			p.Name = v.Name
		}
		v.ReplacedBy = p
		b.place(p, b.pick(created))
	}
}

// resolver turns the uids of the world into the ids the tools print.
var resolver = identity.NewResolver(catalog.Default())

func fingerprint(typ catalog.EntityType, key catalog.AttributeKey, uid string) identity.Fingerprint {
	id, err := resolver.Resolve(typ, []identity.Attr{{Key: key, Value: uid}})
	if err != nil {
		// The keys are strings the catalog accepts; a draw is never empty.
		panic("demo: " + err.Error())
	}
	return id.Fingerprint()
}

// NodeRef names a node: its name, its uid, and the id the tools print for it.
type NodeRef struct {
	Name, UID, ID string
	// Machine indexes World.Machines: nodes with one machine share a host.
	Machine int
}

// PodRef names a pod in the same way, with the instants it is created and
// deleted at (zero for deleted: never in the window).
type PodRef struct {
	Name, UID, ID, Namespace, Workload, Node string
	Created, Deleted                         time.Time
}

// Story is what the script does, by name and by id: the facts the banner, the
// tools' instructions and the tests read. It holds no pod or node that the script
// does not touch, and is derived from the world, never drawn.
type Story struct {
	Seed        uint64
	Anchor, End time.Time
	// NotReady is when node-2 stops being ready (T3), Evict when its pods are
	// deleted, and Ready when it is ready again.
	NotReady, Evict, Ready time.Time
	// Rollout is when the frontend's rollout begins.
	Rollout time.Time
	Nodes   []NodeRef
	// Failed is node-2, the node that stops being ready.
	Failed NodeRef
	// Checkout and LegacyPay are the replicas of the two workloads that run on
	// node-2 at the anchor.
	Checkout, LegacyPay PodRef
	// Evicted are the pods deleted when the eviction's seconds pass, in the order
	// they are deleted; Replacements are their replacements, in the same order.
	Evicted, Replacements []PodRef
}

func (w *World) nodeRef(i int) NodeRef {
	n := w.Nodes[i]
	return NodeRef{Name: n.Name, UID: n.UID, Machine: n.Machine, ID: fingerprint(catalog.K8sNode, catalog.K8sNodeUID, n.UID).String()}
}

func (w *World) podRef(p *Pod) PodRef {
	wl := w.Workloads[p.Workload]
	return PodRef{
		Name: p.Name, UID: p.UID, Namespace: wl.Namespace, Workload: wl.Name, Node: w.Nodes[p.Node].Name,
		ID:      fingerprint(catalog.K8sPod, catalog.K8sPodUID, p.UID).String(),
		Created: p.Created, Deleted: p.Deleted,
	}
}

// Story derives the story of the world.
func (w *World) Story() Story {
	out := w.Outages[0]
	s := Story{
		Seed: w.Seed, Anchor: anchor, End: anchor.Add(Span),
		NotReady: out.From, Evict: out.From.Add(evictAfter), Ready: out.To,
		Rollout: anchor.Add(rolloutAfter),
		Failed:  w.nodeRef(out.Node),
	}
	for i := range w.Nodes {
		s.Nodes = append(s.Nodes, w.nodeRef(i))
	}
	for _, p := range w.podsOn(out.Node, anchor) {
		switch w.Workloads[p.Workload].Name {
		case "checkout":
			if s.Checkout.UID == "" {
				s.Checkout = w.podRef(p)
			}
		case "legacy-pay":
			if s.LegacyPay.UID == "" {
				s.LegacyPay = w.podRef(p)
			}
		}
	}
	for _, p := range w.Pods {
		if p.ReplacedBy != nil {
			s.Evicted = append(s.Evicted, w.podRef(p))
			s.Replacements = append(s.Replacements, w.podRef(p.ReplacedBy))
		}
	}
	return s
}
