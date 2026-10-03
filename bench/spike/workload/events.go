package workload

import (
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The events that change the cluster over simulated time.

// emitInitial asserts the whole cluster at the start, as an initial list would.
func (g *Generator) emitInitial() {
	at, cl := g.cfg.Start, &g.cl
	for _, r := range cl.racks {
		g.entityRecord(at, ProducerFabric, r, lifecycle.Observe, 0, g.payload())
	}
	for i := range cl.hosts {
		g.entityRecord(at, ProducerFabric, cl.hosts[i], lifecycle.Observe, 0, g.payload())
		g.edgeRecord(at, ProducerFabric, cl.hosts[i], cl.racks[i%len(cl.racks)], catalog.LocatedIn, lifecycle.Observe, 0, g.payload())
		g.entityRecord(at, ProducerNodeCollector, cl.nodes[i], lifecycle.Observe, g.heartbeatTTL(), cl.nodePayload[i])
		g.edgeRecord(at, ProducerNodeCollector, cl.nodes[i], cl.hosts[i], catalog.RunsOn, lifecycle.Observe, g.heartbeatTTL(), cl.nodePayload[i])
	}
	for _, s := range cl.services {
		g.entityRecord(at, ProducerK8sObjects, s, lifecycle.Observe, 0, g.payload())
	}
	for i, d := range cl.depends {
		g.edgeRecord(at, ProducerTraces, cl.services[d[0]], cl.services[d[1]], catalog.DependsOn, lifecycle.Observe, g.rollupTTL(), cl.rollupPayload[i])
	}
	for p := range cl.pods {
		g.createPod(at, p, cl.podNode[p])
	}
}

// createPod asserts a pod, its containers, its service instance and its
// placement. podNode[p] is set to node.
func (g *Generator) createPod(at time.Time, p, node int) {
	cl := &g.cl
	cl.podNode[p] = node
	if g.cfg.PodHeartbeatInterval > 0 {
		cl.podPayload[p] = g.payload()
	}
	g.entityRecord(at, ProducerK8sObjects, cl.pods[p], lifecycle.Observe, 0, g.payload())
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], cl.nodes[node], catalog.ScheduledOn, lifecycle.Observe, 0, g.payload())
	g.confirm(at, p, node)
	g.entityRecord(at, ProducerK8sObjects, cl.instances[p], lifecycle.Observe, 0, g.payload())
	g.edgeRecord(at, ProducerK8sObjects, cl.instances[p], cl.services[p%g.cfg.Services], catalog.InstanceOf, lifecycle.Observe, 0, g.payload())
	for _, c := range cl.containers[p] {
		g.entityRecord(at, ProducerK8sObjects, c, lifecycle.Observe, 0, g.payload())
		g.edgeRecord(at, ProducerK8sObjects, c, cl.pods[p], catalog.PartOf, lifecycle.Observe, 0, g.payload())
	}
}

// deletePod retires a pod and everything that hangs off it.
func (g *Generator) deletePod(at time.Time, p int) {
	cl := &g.cl
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], cl.nodes[cl.podNode[p]], catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	g.unconfirm(at, p, cl.podNode[p])
	g.entityRecord(at, ProducerK8sObjects, cl.pods[p], lifecycle.Delete, 0, nil)
	g.edgeRecord(at, ProducerK8sObjects, cl.instances[p], cl.services[p%g.cfg.Services], catalog.InstanceOf, lifecycle.Delete, 0, nil)
	g.entityRecord(at, ProducerK8sObjects, cl.instances[p], lifecycle.Delete, 0, nil)
	for _, c := range cl.containers[p] {
		g.edgeRecord(at, ProducerK8sObjects, c, cl.pods[p], catalog.PartOf, lifecycle.Delete, 0, nil)
		g.entityRecord(at, ProducerK8sObjects, c, lifecycle.Delete, 0, nil)
	}
	cl.nodeCount[cl.podNode[p]]--
	cl.podNode[p] = -1
}

func (g *Generator) reschedule(at time.Time, p int) {
	cl := &g.cl
	old := cl.podNode[p]
	node := g.drawNode(old)
	if g.cfg.FreshIdentities {
		// Kubernetes does not move a pod: the old one goes and a new one, with
		// new containers and a new service instance, is created on the new node.
		g.deletePod(at, p)
		g.mint(p)
		cl.nodeCount[node]++
		g.createPod(at, p, node)
		return
	}
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], cl.nodes[old], catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	g.unconfirm(at, p, old)
	cl.nodeCount[old]--
	cl.nodeCount[node]++
	cl.podNode[p] = node
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], cl.nodes[node], catalog.ScheduledOn, lifecycle.Observe, 0, g.payload())
	g.confirm(at, p, node)
}

// flap replaces a pod's placement in place within one second: the old edge is
// deleted and the same edge asserted again with a new description. Both records
// share an event time, so which one wins depends on the sequence number.
func (g *Generator) flap(at time.Time, p int) {
	cl := &g.cl
	node := cl.nodes[cl.podNode[p]]
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], node, catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	g.edgeRecord(at, ProducerK8sObjects, cl.pods[p], node, catalog.ScheduledOn, lifecycle.Observe, 0, g.payload())
	g.confirm(at, p, cl.podNode[p])
}

// confirm has the kubelet also assert pod p's placement on node, at the same
// instant as the scheduler, with probability ConfirmProbability. It draws from
// the random stream only when the probability is above zero, so a stream without
// a second producer is the same as it was before there was one.
func (g *Generator) confirm(at time.Time, p, node int) {
	if g.cfg.ConfirmProbability <= 0 || g.rng.Float64() >= g.cfg.ConfirmProbability {
		return
	}
	cl := &g.cl
	g.edgeRecord(at, ProducerKubelet, cl.pods[p], cl.nodes[node], catalog.ScheduledOn, lifecycle.Observe, g.cfg.ConfirmTTL, g.payload())
	cl.confirmed[p] = true
}

// unconfirm ends pod p's placement on node as far as the kubelet is concerned: half
// the time it withdraws its assertion at the instant the scheduler withdrew
// its own, and otherwise leaves it to expire after ConfirmTTL, holding the edge
// up while the scheduler says it is gone.
func (g *Generator) unconfirm(at time.Time, p, node int) {
	cl := &g.cl
	if !cl.confirmed[p] {
		return
	}
	cl.confirmed[p] = false
	if g.rng.IntN(2) == 0 {
		g.edgeRecord(at, ProducerKubelet, cl.pods[p], cl.nodes[node], catalog.ScheduledOn, lifecycle.Delete, 0, nil)
	}
}

// scheduleChurn picks the next churn event time. Gaps are exponential at
// EventsPerSecond and accumulate on a continuous clock; only the event time
// itself is truncated to the second, so several events can share a second (and,
// on one pod, one instant), as Kubernetes timestamps do. Truncating the clock
// itself instead would stall it: a gap under a second would never move it on.
func (g *Generator) scheduleChurn() time.Time {
	gap := time.Duration(g.rng.ExpFloat64() / g.cfg.EventsPerSecond * float64(time.Second))
	g.churnClock = g.churnClock.Add(gap)
	return g.churnClock.Truncate(time.Second)
}

func (g *Generator) churn(at time.Time) {
	p := int(g.podZipf.Uint64())
	switch roll := g.rng.Float64(); {
	case g.cl.podNode[p] < 0:
		node := g.drawNode(-1)
		if g.cfg.FreshIdentities {
			g.mint(p)
		}
		g.cl.nodeCount[node]++
		g.createPod(at, p, node)
	case roll < 0.62:
		g.reschedule(at, p)
	case roll < 0.77:
		g.flap(at, p)
	default:
		g.deletePod(at, p)
	}
}

func (g *Generator) heartbeat(at time.Time) {
	cl := &g.cl
	for i := range cl.nodes {
		if at.Before(cl.nodeOutageUntil[i]) {
			continue // silent: this is what expiry is for
		}
		if g.cfg.OutageProbability > 0 && g.rng.Float64() < g.cfg.OutageProbability {
			cl.nodeOutageUntil[i] = at.Add(g.cfg.OutageLength)
			continue
		}
		g.entityRecord(at, ProducerNodeCollector, cl.nodes[i], lifecycle.Observe, g.heartbeatTTL(), cl.nodePayload[i])
		g.edgeRecord(at, ProducerNodeCollector, cl.nodes[i], cl.hosts[i], catalog.RunsOn, lifecycle.Observe, g.heartbeatTTL(), cl.nodePayload[i])
	}
}

func (g *Generator) rollup(at time.Time) {
	cl := &g.cl
	for i, d := range cl.depends {
		g.edgeRecord(at, ProducerTraces, cl.services[d[0]], cl.services[d[1]], catalog.DependsOn, lifecycle.Observe, g.rollupTTL(), cl.rollupPayload[i])
	}
}

// podHeartbeat has the cluster-level collector re-assert every live pod's
// placement. Nothing withdraws it: when a pod moves or goes, the assertion
// expires after its TTL.
func (g *Generator) podHeartbeat(at time.Time) {
	cl := &g.cl
	ttl := time.Duration(g.cfg.HeartbeatTTLFactor) * g.cfg.PodHeartbeatInterval
	for p := range cl.pods {
		if cl.podNode[p] < 0 {
			continue
		}
		g.edgeRecord(at, ProducerPodHeartbeat, cl.pods[p], cl.nodes[cl.podNode[p]], catalog.ScheduledOn, lifecycle.Observe, ttl, cl.podPayload[p])
	}
}
