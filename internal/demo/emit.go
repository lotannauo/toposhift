package demo

import (
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The emitters write what the collectors of the shipped configuration send: the
// Kubernetes objects receiver in pull mode and in watch mode, the cluster
// receiver, and a collector on every node. Each is a pure function of the world
// and an instant; none reads a clock, draws, or keeps state between calls.

// ReportEvery is the interval of the objects receiver's pulls, of the cluster
// receiver's reports and of the node agents' reports: five minutes, the interval
// the shipped configuration uses and the sequencer's default.
const ReportEvery = 5 * time.Minute

// Emitter names which collector a request comes from. Requests at one instant
// are sent in the order of the emitters.
type Emitter int

const (
	// EmitObjectsPull is the objects receiver listing the pods or the nodes.
	EmitObjectsPull Emitter = iota
	// EmitClusterReport is the cluster receiver's entity events.
	EmitClusterReport
	// EmitNodeAgent is the host event of the collector on one node.
	EmitNodeAgent
	// EmitObjectsWatch is the objects receiver's watch event for one change.
	EmitObjectsWatch
)

func (e Emitter) String() string {
	switch e {
	case EmitObjectsPull:
		return "objects pull"
	case EmitClusterReport:
		return "cluster report"
	case EmitNodeAgent:
		return "node agent"
	case EmitObjectsWatch:
		return "objects watch"
	}
	return fmt.Sprintf("emitter %d", int(e))
}

// Request is one export of a collector, at the instant it is sent.
type Request struct {
	At      time.Time
	Emitter Emitter
	// What says which request it is, for a message.
	What  string
	build func() capture.LogsData
}

// Data builds the request's OTLP logs.
func (r Request) Data() capture.LogsData { return r.build() }

// Requests lists every request of the window, ordered by instant, then emitter,
// then the order they are listed in. anchor to anchor+Span is covered, both ends
// included.
func (w *World) Requests() []Request {
	end := anchor.Add(Span)
	var out []Request
	for p := anchor; !p.After(end); p = p.Add(ReportEvery) {
		out = append(out,
			Request{p, EmitObjectsPull, "pull of nodes", func() capture.LogsData { return w.pullNodes(p) }},
			Request{p, EmitObjectsPull, "pull of pods", func() capture.LogsData { return w.pullPods(p) }},
			Request{p, EmitClusterReport, "report", func() capture.LogsData { return w.report(p) }},
		)
		for n := range w.Nodes {
			out = append(out, Request{p, EmitNodeAgent, "host event of " + w.Nodes[n].Name, func() capture.LogsData { return w.agent(n, p) }})
		}
	}
	for _, c := range w.changes() {
		what := fmt.Sprintf("watch %s of %s", c.typ, c.name(w))
		out = append(out, Request{c.at, EmitObjectsWatch, what, func() capture.LogsData { return w.watch(c) }})
	}
	slices.SortStableFunc(out, func(a, b Request) int {
		if c := a.At.Compare(b.At); c != 0 {
			return c
		}
		return int(a.Emitter) - int(b.Emitter)
	})
	return out
}

// change is one watch event: a pod or a node that changed, a second ago.
type change struct {
	at   time.Time
	typ  string // ADDED, MODIFIED or DELETED
	pod  *Pod
	node int // the node, when pod is nil
}

func (c change) name(w *World) string {
	if c.pod != nil {
		return c.pod.Name
	}
	return w.Nodes[c.node].Name
}

// changes lists the watch events of the window, one second after each change.
func (w *World) changes() []change {
	end := anchor.Add(Span)
	var out []change
	add := func(c change) {
		if c.at.After(anchor) && !c.at.After(end) {
			out = append(out, c)
		}
	}
	for _, p := range w.Pods {
		add(change{at: p.Created.Add(time.Second), typ: "ADDED", pod: p})
		add(change{at: p.Started.Add(time.Second), typ: "MODIFIED", pod: p})
		if !p.Finished.IsZero() {
			add(change{at: p.Finished.Add(time.Second), typ: "MODIFIED", pod: p})
		}
		if !p.Deleted.IsZero() {
			add(change{at: p.Deleted.Add(time.Second), typ: "DELETED", pod: p})
		}
	}
	for _, o := range w.Outages {
		add(change{at: o.From.Add(time.Second), typ: "MODIFIED", node: o.Node})
		add(change{at: o.To.Add(time.Second), typ: "MODIFIED", node: o.Node})
	}
	return out
}

// ---- the objects receiver ----

func (w *World) image(p *Pod) string {
	wl := w.Workloads[p.Workload]
	return "registry.demo.local/" + wl.Namespace + "/" + wl.Name
}

// podObject is the pod as the API shows it at t.
func (w *World) podObject(p *Pod, t time.Time) map[string]any {
	wl := w.Workloads[p.Workload]
	ph := p.phaseAt(t)
	scheduled := !t.Before(p.Scheduled)
	started := ph == phaseRunning || ph == phaseSucceeded

	ownerKind := wl.Kind
	if wl.Kind == kindDeployment {
		ownerKind = "ReplicaSet"
	}
	meta := map[string]any{
		"uid": p.UID, "name": p.Name, "namespace": wl.Namespace,
		"creationTimestamp": stamp(p.Created),
		"labels":            map[string]any{"app": wl.Name},
		"ownerReferences":   []any{map[string]any{"kind": ownerKind, "name": p.Owner, "controller": true}},
	}
	spec := map[string]any{
		"containers": []any{map[string]any{"name": wl.Name, "image": w.image(p) + ":" + p.ImageTag}},
	}
	status := map[string]any{"phase": ph.String()}
	var conds []any
	cond := func(typ, st, reason string, at time.Time) {
		c := map[string]any{"type": typ, "status": st, "lastTransitionTime": stamp(at)}
		if reason != "" {
			c["reason"] = reason
		}
		conds = append(conds, c)
	}
	if scheduled {
		spec["nodeName"] = w.Nodes[p.Node].Name
		cond("PodScheduled", "True", "", p.Scheduled)
	}
	switch ph {
	case phaseRunning:
		cond("Ready", "True", "", p.Started)
		cond("ContainersReady", "True", "", p.Started)
	case phaseSucceeded:
		cond("Ready", "False", "PodCompleted", p.Finished)
		cond("ContainersReady", "False", "PodCompleted", p.Finished)
	}
	if len(conds) > 0 {
		status["conditions"] = conds
	}
	if started {
		status["hostIP"] = w.Nodes[p.Node].IP
		status["podIP"] = p.IP
		status["podIPs"] = []any{map[string]any{"ip": p.IP}}
		state := map[string]any{"running": map[string]any{"startedAt": stamp(p.Started)}}
		if ph == phaseSucceeded {
			state = map[string]any{"terminated": map[string]any{
				"startedAt": stamp(p.Started), "finishedAt": stamp(p.Finished), "exitCode": 0, "reason": "Completed",
			}}
		}
		status["containerStatuses"] = []any{map[string]any{
			"name": wl.Name, "containerID": "containerd://" + p.ContainerID, "restartCount": 0, "state": state,
		}}
	}
	return map[string]any{"kind": "Pod", "apiVersion": "v1", "metadata": meta, "spec": spec, "status": status}
}

// nodeObject is the node as the API shows it at t.
func (w *World) nodeObject(i int, t time.Time) map[string]any {
	n := w.Nodes[i]
	ready, since := w.readyAt(i, t)
	rc := map[string]any{"type": "Ready", "status": "True", "reason": "KubeletReady", "lastTransitionTime": stamp(since)}
	if !ready {
		rc["status"], rc["reason"] = "False", "KubeletNotReady"
	}
	conds := []any{rc}
	for _, typ := range []string{"MemoryPressure", "DiskPressure", "PIDPressure"} {
		conds = append(conds, map[string]any{"type": typ, "status": "False", "reason": "Kubelet" + typ + "Less", "lastTransitionTime": stamp(n.Created)})
	}
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{
			"uid": n.UID, "name": n.Name, "creationTimestamp": stamp(n.Created),
			"labels": map[string]any{"kubernetes.io/hostname": n.Name},
		},
		"status": map[string]any{
			"conditions": conds,
			"addresses": []any{
				map[string]any{"type": "InternalIP", "address": n.IP},
				map[string]any{"type": "Hostname", "address": n.Name},
			},
			"nodeInfo": map[string]any{"bootID": w.Machines[n.Machine].BootID},
		},
	}
}

func (w *World) objectsRequest(recs ...capture.LogRecord) capture.LogsData {
	return request(capture.InstrumentationScope{Name: objectsScope}, []capture.KeyValue{clusterAttr()}, recs...)
}

// pullNodes is the objects receiver's list of the nodes at p.
func (w *World) pullNodes(p time.Time) capture.LogsData {
	var recs []capture.LogRecord
	for i := range w.Nodes {
		recs = append(recs, objectsRecord(p, w.nodeObject(i, p), "nodes"))
	}
	return w.objectsRequest(recs...)
}

// pullPods is the objects receiver's list of the pods at p.
func (w *World) pullPods(p time.Time) capture.LogsData {
	var recs []capture.LogRecord
	for _, pod := range w.Pods {
		if pod.phaseAt(p) != phaseGone {
			recs = append(recs, objectsRecord(p, w.podObject(pod, p), "pods"))
		}
	}
	return w.objectsRequest(recs...)
}

// watch is the objects receiver's event for a change.
func (w *World) watch(c change) capture.LogsData {
	attrs := func(name string) []capture.KeyValue {
		return []capture.KeyValue{
			{Key: "event.domain", Value: sv("k8s")},
			{Key: "event.name", Value: sv(name)},
		}
	}
	if c.pod == nil {
		body := map[string]any{"type": c.typ, "object": w.nodeObject(c.node, c.at)}
		return w.objectsRequest(objectsRecord(c.at, body, "nodes", attrs(w.Nodes[c.node].Name)...))
	}
	// The object of a deletion is the pod as it was when it went.
	state := c.at
	if c.typ == "DELETED" {
		state = c.pod.Deleted.Add(-time.Second)
	}
	body := map[string]any{"type": c.typ, "object": w.podObject(c.pod, state)}
	rec := objectsRecord(c.at, body, "pods", attrs(c.pod.Name)...)
	ns := w.Workloads[c.pod.Workload].Namespace
	return request(capture.InstrumentationScope{Name: objectsScope},
		[]capture.KeyValue{clusterAttr(), {Key: "k8s.namespace.name", Value: sv(ns)}}, rec)
}

// ---- the cluster receiver ----

// podDesc is the cluster receiver's description of a pod: the keys the
// translator keeps.
func (w *World) podDesc(p *Pod, t time.Time) map[string]string {
	wl := w.Workloads[p.Workload]
	d := map[string]string{
		"k8s.pod.name":           p.Name,
		"k8s.namespace.name":     wl.Namespace,
		"k8s.pod.phase":          p.phaseAt(t).String(),
		"pod.creation_timestamp": stamp(p.Created),
		"k8s.workload.kind":      wl.Kind,
		"k8s.workload.name":      wl.Name,
	}
	if !t.Before(p.Scheduled) {
		d["k8s.node.name"] = w.Nodes[p.Node].Name
	}
	switch wl.Kind {
	case kindDeployment:
		d["k8s.deployment.name"] = wl.Name
		d["k8s.replicaset.name"] = p.Owner
	case kindStatefulSet:
		d["k8s.statefulset.name"] = wl.Name
	}
	return d
}

func (w *World) containerDesc(p *Pod, t time.Time) map[string]string {
	wl := w.Workloads[p.Workload]
	d := map[string]string{
		"k8s.container.name":           wl.Name,
		"k8s.pod.name":                 p.Name,
		"k8s.pod.uid":                  p.UID,
		"k8s.namespace.name":           wl.Namespace,
		"k8s.node.name":                w.Nodes[p.Node].Name,
		"container.image.name":         w.image(p),
		"container.image.tag":          p.ImageTag,
		"container.status":             "running",
		"container.creation_timestamp": stamp(p.Started),
	}
	if p.phaseAt(t) == phaseSucceeded {
		d["container.status"], d["container.status.reason"] = "terminated", "Completed"
	}
	return d
}

func (w *World) nodeDesc(i int, t time.Time) map[string]string {
	n := w.Nodes[i]
	ready, _ := w.readyAt(i, t)
	return map[string]string{
		"k8s.node.name":                          n.Name,
		"node.creation_timestamp":                stamp(n.Created),
		"k8s.node.condition_ready":               fmt.Sprint(ready),
		"k8s.node.condition_memory_pressure":     "false",
		"k8s.node.condition_disk_pressure":       "false",
		"k8s.node.condition_pid_pressure":        "false",
		"k8s.node.condition_network_unavailable": "false",
	}
}

// report is the cluster receiver's report at p: the state of every namespace,
// node, pod and container, and the deletion of each pod that was gone since the
// last report. A deletion carries the instant the receiver saw the pod go, as a
// delete event does.
func (w *World) report(p time.Time) capture.LogsData {
	var recs []capture.LogRecord
	for _, ns := range w.Namespaces {
		recs = append(recs, entityEvent("entity.state", "k8s.namespace", "k8s.namespace.uid", ns.UID, p,
			map[string]string{"k8s.namespace.name": ns.Name}))
	}
	for i, n := range w.Nodes {
		recs = append(recs, entityEvent("entity.state", "k8s.node", "k8s.node.uid", n.UID, p, w.nodeDesc(i, p)))
	}
	for _, pod := range w.Pods {
		ph := pod.phaseAt(p)
		if ph == phaseGone {
			continue
		}
		recs = append(recs, entityEvent("entity.state", "k8s.pod", "k8s.pod.uid", pod.UID, p, w.podDesc(pod, p)))
		if ph == phaseRunning || ph == phaseSucceeded {
			recs = append(recs, entityEvent("entity.state", "container", "container.id", "containerd://"+pod.ContainerID, p, w.containerDesc(pod, p)))
		}
	}
	if last := p.Add(-ReportEvery); !last.Before(anchor) {
		for _, pod := range w.Pods {
			seen := pod.Deleted.Add(time.Second)
			if !pod.Deleted.IsZero() && seen.After(last) && !seen.After(p) && pod.phaseAt(last) != phaseGone {
				recs = append(recs, entityEvent("entity.delete", "k8s.pod", "k8s.pod.uid", pod.UID, seen, nil))
			}
		}
	}
	return request(entityScope(), []capture.KeyValue{clusterAttr()}, recs...)
}

// ---- the node agents ----

// agent is the host event of the collector on node i at p. The host is the
// machine the node runs on: nodes that share a machine report one host.id and one
// boot id, with their own node uids.
func (w *World) agent(i int, p time.Time) capture.LogsData {
	n := w.Nodes[i]
	m := w.Machines[n.Machine]
	res := []capture.KeyValue{
		{Key: "os.type", Value: sv("linux")},
		{Key: "host.arch", Value: sv("amd64")},
		{Key: "topo.host.boot.id", Value: sv(m.BootID)},
		clusterAttr(),
		{Key: "host.id", Value: sv(m.HostID)},
		{Key: "k8s.node.uid", Value: sv(n.UID)},
	}
	lr := capture.LogRecord{
		EventName:    "entity.state",
		TimeUnixNano: capture.Uint64(p.UnixNano()),
		Attributes: []capture.KeyValue{
			{Key: "entity.type", Value: sv("host")},
			{Key: "entity.report.interval", Value: iv(int64(ReportEvery / time.Second))},
		},
	}
	return request(entityScope(), res, lr)
}
