package demo

import (
	"fmt"
	"math/rand/v2"
	"time"
)

// The kinds of workload the world has. The kind decides how a pod is named, who
// owns it, and whether the eviction of a node's pods replaces it.
const (
	kindDeployment  = "Deployment"
	kindStatefulSet = "StatefulSet"
	kindDaemonSet   = "DaemonSet"
	kindJob         = "Job"
)

// Machine is a computer that runs nodes. Many nodes may share one machine, as the
// nodes of a kind or k3d cluster do: they share its kernel, so its boot id and
// its host id.
type Machine struct {
	// HostID is the machine id the node agents report as host.id.
	HostID string
	// BootID is the kernel's boot id, one for the whole run.
	BootID string
}

// Node is a Kubernetes node.
type Node struct {
	Name, UID string
	// Machine indexes World.Machines.
	Machine int
	// IP is the node's InternalIP.
	IP      string
	Created time.Time
}

// Namespace is a Kubernetes namespace.
type Namespace struct{ Name, UID string }

// Workload is a controller and the pods it keeps.
type Workload struct {
	Namespace, Name, Kind string
	Replicas              int
	// ReplicaSet is the hash a Deployment's pods carry in their names, and
	// NextReplicaSet the one a rollout moves them to; both are empty for any other
	// kind.
	ReplicaSet, NextReplicaSet string
	// Service is the service the workload implements. It is empty in this world,
	// which has no service layer; a world that has one sets it, and the emitters
	// that name services read it.
	Service string
}

// Pod is a pod over its whole life. Every instant is a whole second, because a
// Kubernetes timestamp is.
type Pod struct {
	UID, Name string
	// Workload indexes World.Workloads.
	Workload int
	// Owner is the name of the controller object that owns the pod: the ReplicaSet
	// of a Deployment's pod, the StatefulSet, DaemonSet or Job itself otherwise.
	Owner string
	// Node indexes World.Nodes; IP is the address the pod holds from Started on.
	Node int
	IP   string
	// ContainerID is the runtime id of the pod's one container, without its
	// "containerd://" prefix.
	ContainerID string
	ImageTag    string
	// Created is when the object was created, Scheduled when it was bound to its
	// node, and Started when its container began to run. A zero Finished is a pod
	// that runs to the end; a Job's pod succeeds at Finished. A zero Deleted is a
	// pod that is never deleted in the window.
	Created, Scheduled, Started, Finished, Deleted time.Time
	// ReplacedBy is the pod an eviction created in this one's place, if it did.
	ReplacedBy *Pod
}

// Outage is a stretch in which a node's kubelet reports it not ready.
type Outage struct {
	Node     int
	From, To time.Time
}

// World is the cluster and everything that happens to it, as data. It is built
// once by [NewWorld] from a seed and is read-only after: the emitters and the
// tests read it, nothing writes it.
type World struct {
	Seed       uint64
	Machines   []Machine
	Nodes      []Node
	Namespaces []Namespace
	Workloads  []Workload
	// Pods holds every pod the window sees, in the order they are emitted: the pods
	// of the anchor first, then those the script creates.
	Pods    []*Pod
	Outages []Outage
}

// The phases of a pod over its life.
type phase int

const (
	phaseGone phase = iota
	phasePending
	phaseRunning
	phaseSucceeded
)

func (p phase) String() string {
	switch p {
	case phasePending:
		return "Pending"
	case phaseRunning:
		return "Running"
	case phaseSucceeded:
		return "Succeeded"
	}
	return "Gone"
}

// phaseAt is the pod's phase at t. It is gone before it is created and from the
// instant it is deleted.
func (p *Pod) phaseAt(t time.Time) phase {
	switch {
	case t.Before(p.Created), !p.Deleted.IsZero() && !t.Before(p.Deleted):
		return phaseGone
	case t.Before(p.Started):
		return phasePending
	case !p.Finished.IsZero() && !t.Before(p.Finished):
		return phaseSucceeded
	}
	return phaseRunning
}

// readyAt says whether the node's kubelet reports it ready at t, and since when
// it has reported that.
func (w *World) readyAt(node int, t time.Time) (ready bool, since time.Time) {
	since = w.Nodes[node].Created
	ready = true
	for _, o := range w.Outages {
		if o.Node != node {
			continue
		}
		switch {
		case !t.Before(o.To):
			since = o.To
		case !t.Before(o.From):
			return false, o.From
		}
	}
	return ready, since
}

// podsOn is the pods alive on the node at t, in World.Pods order.
func (w *World) podsOn(node int, t time.Time) []*Pod {
	var out []*Pod
	for _, p := range w.Pods {
		if p.Node == node && p.phaseAt(t) != phaseGone {
			out = append(out, p)
		}
	}
	return out
}

// builder draws a world. Every draw is made in NewWorld, in one fixed order, so a
// seed gives one world.
type builder struct {
	r    *rand.Rand
	w    *World
	used map[string]bool
	// nextIP is the last host part handed out in each node's pod range. An address
	// is never handed out twice, so none is held by two pods at once.
	nextIP [4]int
}

// NewWorld draws the cluster of a seed: two machines, four nodes, five
// namespaces and forty pods at the anchor, and then the script of script.go. It
// reads no clock and does no I/O.
func NewWorld(seed uint64) *World {
	b := &builder{
		r:    rand.New(rand.NewPCG(seed, 0x64656d6f)),
		w:    &World{Seed: seed},
		used: map[string]bool{},
	}
	for i := range b.nextIP {
		b.nextIP[i] = 9
	}
	b.machines()
	b.nodes()
	b.namespaces()
	b.workloads()
	b.outage()
	b.basePods()
	b.rollout()
	b.jobs()
	b.evict()
	return b.w
}

const (
	hexDigits = "0123456789abcdef"
	// nameAlphabet is the alphabet Kubernetes draws the random part of a name from.
	nameAlphabet = "bcdfghjklmnpqrstvwxz2456789"
)

func (b *builder) hex(n int) string {
	buf := make([]byte, n)
	for i := range buf {
		buf[i] = hexDigits[b.r.IntN(len(hexDigits))]
	}
	return string(buf)
}

func (b *builder) suffix() string {
	buf := make([]byte, 5)
	for i := range buf {
		buf[i] = nameAlphabet[b.r.IntN(len(nameAlphabet))]
	}
	return string(buf)
}

// uuid draws a version 4 UUID, in the shape of a Kubernetes uid.
func (b *builder) uuid() string {
	return fmt.Sprintf("%s-%s-4%s-%c%s-%s", b.hex(8), b.hex(4), b.hex(3), "89ab"[b.r.IntN(4)], b.hex(3), b.hex(12))
}

// unique draws until the value is one no draw has given before.
func (b *builder) unique(draw func() string) string {
	for {
		if s := draw(); !b.used[s] {
			b.used[s] = true
			return s
		}
	}
}

// seconds draws a whole number of seconds in [0, n).
func (b *builder) seconds(n int) time.Duration { return time.Duration(b.r.IntN(n)) * time.Second }

func (b *builder) machines() {
	for range 2 {
		b.w.Machines = append(b.w.Machines, Machine{
			HostID: b.unique(func() string { return b.hex(32) }),
			BootID: b.unique(b.uuid),
		})
	}
}

// The nodes are created two days before the anchor, as the nodes of the test
// worlds are, so their existence before the anchor comes from their creation
// timestamps.
func (b *builder) nodes() {
	for i := range 4 {
		b.w.Nodes = append(b.w.Nodes, Node{
			Name:    fmt.Sprintf("node-%d", i+1),
			UID:     b.unique(b.uuid),
			Machine: i / 2,
			IP:      fmt.Sprintf("10.0.0.%d", 11+i),
			Created: anchor.Add(-48*time.Hour - b.seconds(1800)),
		})
	}
}

// kubeSystemUID is the one fixed uid of the world: the detector of cluster
// identity tells clusters apart by it, so it is a constant and not a draw.
const kubeSystemUID = "6b1f5f6e-3c2d-4c31-9d61-7a1c5e0f0001"

func (b *builder) namespaces() {
	for _, n := range []string{"shop", "payments", "data", "platform", "kube-system"} {
		uid := kubeSystemUID
		if n != "kube-system" {
			uid = b.unique(b.uuid)
		}
		b.w.Namespaces = append(b.w.Namespaces, Namespace{Name: n, UID: uid})
	}
}

// workloads lists the controllers: the replicas of each at the anchor.
func (b *builder) workloads() {
	for _, wl := range []Workload{
		{Namespace: "shop", Name: "frontend", Kind: kindDeployment, Replicas: 3},
		{Namespace: "shop", Name: "checkout", Kind: kindDeployment, Replicas: 3},
		{Namespace: "shop", Name: "cart", Kind: kindDeployment, Replicas: 2},
		{Namespace: "shop", Name: "catalog", Kind: kindDeployment, Replicas: 3},
		{Namespace: "shop", Name: "search", Kind: kindDeployment, Replicas: 3},
		{Namespace: "shop", Name: "recommend", Kind: kindDeployment, Replicas: 3},
		{Namespace: "payments", Name: "payments-api", Kind: kindDeployment, Replicas: 3},
		{Namespace: "payments", Name: "legacy-pay", Kind: kindDeployment, Replicas: 2},
		{Namespace: "payments", Name: "ledger", Kind: kindDeployment, Replicas: 2},
		{Namespace: "data", Name: "db", Kind: kindStatefulSet, Replicas: 2},
		{Namespace: "data", Name: "redis", Kind: kindDeployment, Replicas: 2},
		{Namespace: "platform", Name: "metrics-agent", Kind: kindDaemonSet, Replicas: 4},
		{Namespace: "platform", Name: "ingress", Kind: kindDeployment, Replicas: 2},
		{Namespace: "platform", Name: "report", Kind: kindJob},
		{Namespace: "kube-system", Name: "coredns", Kind: kindDeployment, Replicas: 2},
		{Namespace: "kube-system", Name: "kube-proxy", Kind: kindDaemonSet, Replicas: 4},
	} {
		if wl.Kind == kindDeployment {
			wl.ReplicaSet = b.unique(func() string { return b.hex(10) })
			if wl.Name == rolledOut {
				wl.NextReplicaSet = b.unique(func() string { return b.hex(10) })
			}
		}
		b.w.Workloads = append(b.w.Workloads, wl)
	}
}

// workloadIndex is the index of the workload of the namespace and name.
func (w *World) workloadIndex(namespace, name string) int {
	for i, wl := range w.Workloads {
		if wl.Namespace == namespace && wl.Name == name {
			return i
		}
	}
	panic("demo: no workload " + namespace + "/" + name)
}

// newPod draws a pod of the workload, owned by the controller object of the given
// name, created at the given instant and bound a second or so later. It does not
// place it. The caller names the pod of a StatefulSet and of a Job.
func (b *builder) newPod(workload int, owner, tag string, created time.Time, runAfter time.Duration) *Pod {
	wl := b.w.Workloads[workload]
	p := &Pod{
		Workload:    workload,
		Owner:       owner,
		Node:        -1,
		ImageTag:    tag,
		ContainerID: b.unique(func() string { return b.hex(64) }),
		UID:         b.unique(b.uuid),
		Created:     created,
		Scheduled:   created.Add(time.Second + b.seconds(3)),
	}
	p.Started = p.Scheduled.Add(runAfter)
	switch wl.Kind {
	case kindDeployment:
		p.Name = b.unique(func() string { return owner + "-" + b.suffix() })
	case kindDaemonSet:
		p.Name = b.unique(func() string { return wl.Name + "-" + b.suffix() })
	}
	b.w.Pods = append(b.w.Pods, p)
	return p
}

// ownerOf is the name of the controller object that owns the pods of a workload
// at a ReplicaSet hash: the ReplicaSet of a Deployment, the workload itself
// otherwise.
func ownerOf(wl Workload, replicaSet string) string {
	if wl.Kind == kindDeployment {
		return wl.Name + "-" + replicaSet
	}
	return wl.Name
}

// pick chooses the node a pod created at t is bound to: among the nodes whose
// kubelet reports ready at t, one of those that hold the fewest pods, drawn. It
// is how a scheduler that spreads pods behaves, and it keeps every node busy
// whatever the seed.
func (b *builder) pick(t time.Time) int {
	var best []int
	least := int(^uint(0) >> 1)
	for i := range b.w.Nodes {
		if ready, _ := b.w.readyAt(i, t); !ready {
			continue
		}
		n := len(b.w.podsOn(i, t))
		switch {
		case n < least:
			least, best = n, []int{i}
		case n == least:
			best = append(best, i)
		}
	}
	return best[b.r.IntN(len(best))]
}

// place binds the pod to the node and gives it the next address of the node's
// range.
func (b *builder) place(p *Pod, node int) {
	p.Node = node
	b.nextIP[node]++
	p.IP = fmt.Sprintf("10.244.%d.%d", node+1, b.nextIP[node])
}

// basePods draws the forty pods that exist at the anchor, created one to
// twenty-four hours before it. Two replicas are forced onto node-2, the node that
// fails, so that its blast radius has a checkout and a legacy-pay pod in it; each
// DaemonSet has one pod on every node; the rest are spread.
func (b *builder) basePods() {
	for wi, wl := range b.w.Workloads {
		for i := range wl.Replicas {
			created := anchor.Add(-time.Hour - b.seconds(23*3600))
			p := b.newPod(wi, ownerOf(wl, wl.ReplicaSet), "1.0.0", created, 3*time.Second+b.seconds(20))
			if wl.Kind == kindStatefulSet {
				p.Name = b.unique(func() string { return fmt.Sprintf("%s-%d", wl.Name, i) })
			}
		}
	}
	forced := map[string]bool{"checkout": true, "legacy-pay": true}
	seen := map[int]bool{}
	for _, p := range b.w.Pods {
		wl := b.w.Workloads[p.Workload]
		switch {
		case wl.Kind == kindDaemonSet:
			// One on every node, in node order.
			n := 0
			for _, q := range b.w.Pods {
				if q.Workload == p.Workload && q.Node >= 0 {
					n++
				}
			}
			b.place(p, n)
		case forced[wl.Name] && !seen[p.Workload]:
			seen[p.Workload] = true
			b.place(p, failedNode)
		}
	}
	for _, p := range b.w.Pods {
		if p.Node < 0 {
			b.place(p, b.pick(anchor))
		}
	}
}

// evictable says whether the eviction of a failed node replaces the pod: a pod a
// DaemonSet keeps on the node stays, and a Job's pod is not replaced.
func (w *World) evictable(p *Pod) bool {
	k := w.Workloads[p.Workload].Kind
	return k != kindDaemonSet && k != kindJob
}
