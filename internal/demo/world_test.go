package demo

import (
	"reflect"
	"slices"
	"testing"
	"time"
)

// seeds are the seeds the claims about the world's shape are checked for.
var seeds = []uint64{1, 2, 3, 4, 5, 6, 7, 8}

func podsByWorkload(w *World, namespace, name string, at time.Time) []*Pod {
	var out []*Pod
	for _, p := range w.Pods {
		wl := w.Workloads[p.Workload]
		if wl.Namespace == namespace && wl.Name == name && p.phaseAt(at) != phaseGone {
			out = append(out, p)
		}
	}
	return out
}

// The world is a pure function of its seed: two worlds of one seed, and the
// requests they send, are equal, and another seed is another world.
func TestNewWorldIsAFunctionOfItsSeed(t *testing.T) {
	a, b := NewWorld(1), NewWorld(1)
	if !reflect.DeepEqual(a, b) {
		t.Error("two worlds of seed 1 differ")
	}
	ra, rb := a.Requests(), b.Requests()
	if len(ra) != len(rb) {
		t.Fatalf("%d requests and %d", len(ra), len(rb))
	}
	for i := range ra {
		if !ra[i].At.Equal(rb[i].At) || ra[i].What != rb[i].What || !reflect.DeepEqual(ra[i].Data(), rb[i].Data()) {
			t.Fatalf("request %d (%s at %s) differs between two worlds of seed 1", i, ra[i].What, ra[i].At)
		}
	}
	if reflect.DeepEqual(a.Pods[0], NewWorld(2).Pods[0]) {
		t.Error("seeds 1 and 2 give the same first pod")
	}
}

// The shape: two machines, four nodes, five namespaces and forty pods at the
// anchor, with the placements the story needs, for every seed checked.
func TestWorldShapeAtTheAnchor(t *testing.T) {
	counts := map[string]int{"shop": 17, "payments": 7, "data": 4, "platform": 6, "kube-system": 6}
	for _, seed := range seeds {
		w := NewWorld(seed)
		if len(w.Machines) != 2 || len(w.Nodes) != 4 || len(w.Namespaces) != 5 {
			t.Fatalf("seed %d: %d machines, %d nodes, %d namespaces; want 2, 4, 5", seed, len(w.Machines), len(w.Nodes), len(w.Namespaces))
		}
		for i, n := range w.Nodes {
			if want := i / 2; n.Machine != want {
				t.Errorf("seed %d: %s is on machine %d, want %d", seed, n.Name, n.Machine, want)
			}
		}
		if w.Machines[0].BootID == w.Machines[1].BootID || w.Machines[0].HostID == w.Machines[1].HostID {
			t.Errorf("seed %d: the two machines share a boot id or a host id", seed)
		}

		inNamespace := map[string]int{}
		alive := 0
		for _, p := range w.Pods {
			if p.phaseAt(anchor) == phaseGone {
				continue
			}
			alive++
			inNamespace[w.Workloads[p.Workload].Namespace]++
			if age := anchor.Sub(p.Created); age < time.Hour || age > 24*time.Hour {
				t.Errorf("seed %d: %s was created %s before the anchor, want one to twenty-four hours", seed, p.Name, age)
			}
			if p.phaseAt(anchor) != phaseRunning {
				t.Errorf("seed %d: %s is %s at the anchor, want Running", seed, p.Name, p.phaseAt(anchor))
			}
		}
		if alive != 40 || !reflect.DeepEqual(inNamespace, counts) {
			t.Errorf("seed %d: %d pods at the anchor in %v; want 40 in %v", seed, alive, inNamespace, counts)
		}

		// Unique names and ids.
		uids, ctrs, names := map[string]bool{}, map[string]bool{}, map[string]bool{}
		for _, p := range w.Pods {
			for _, set := range []struct {
				m map[string]bool
				v string
			}{{uids, p.UID}, {ctrs, p.ContainerID}} {
				if set.m[set.v] {
					t.Errorf("seed %d: %s is used twice", seed, set.v)
				}
				set.m[set.v] = true
			}
			// A replacement of a StatefulSet's pod takes its name, and its predecessor
			// is gone by then.
			if w.Workloads[p.Workload].Kind != kindStatefulSet {
				if names[p.Name] {
					t.Errorf("seed %d: the pod name %s is used twice", seed, p.Name)
				}
				names[p.Name] = true
			}
		}

		// The placements the story needs, on node-2.
		on := w.podsOn(failedNode, anchor)
		if len(on) < 8 {
			t.Errorf("seed %d: node-2 holds %d pods at the anchor, want at least 8", seed, len(on))
		}
		has := map[string]int{}
		for _, p := range on {
			has[w.Workloads[p.Workload].Name]++
		}
		for _, name := range []string{"checkout", "legacy-pay", "metrics-agent", "kube-proxy"} {
			if has[name] == 0 {
				t.Errorf("seed %d: node-2 holds no %s pod at the anchor", seed, name)
			}
		}
		// A DaemonSet has one pod on every node.
		for _, name := range []string{"metrics-agent", "kube-proxy"} {
			var nodes []int
			for _, p := range w.Pods {
				if w.Workloads[p.Workload].Name == name {
					nodes = append(nodes, p.Node)
				}
			}
			if !slices.Equal(nodes, []int{0, 1, 2, 3}) {
				t.Errorf("seed %d: the %s pods are on nodes %v, want one on each", seed, name, nodes)
			}
		}
	}
}

// No address is held by two pods at one instant, the node range of an address is
// the node's, and the node's own address is not a pod's.
func TestNoAddressIsHeldByTwoPodsAtOnce(t *testing.T) {
	for _, seed := range seeds {
		w := NewWorld(seed)
		end := anchor.Add(Span)
		for i, p := range w.Pods {
			if p.IP == "" {
				t.Fatalf("seed %d: %s has no address", seed, p.Name)
			}
			for _, n := range w.Nodes {
				if p.IP == n.IP {
					t.Errorf("seed %d: %s holds the address of %s", seed, p.Name, n.Name)
				}
			}
			holdsTo := func(q *Pod) time.Time {
				if q.Deleted.IsZero() || q.Deleted.After(end) {
					return end.Add(time.Hour)
				}
				return q.Deleted
			}
			for _, q := range w.Pods[i+1:] {
				if p.IP == q.IP && p.Started.Before(holdsTo(q)) && q.Started.Before(holdsTo(p)) {
					t.Errorf("seed %d: %s and %s hold %s at once", seed, p.Name, q.Name, p.IP)
				}
			}
		}
	}
}

// The script: nothing is bound to node-2 while its kubelet reports it not ready,
// every evicted pod is deleted at once in the seconds after the taint's
// three hundred, and its replacement is created a second after, elsewhere.
func TestScriptOfTheFailedNode(t *testing.T) {
	for _, seed := range seeds {
		w := NewWorld(seed)
		out := w.Outages[0]
		if out.Node != failedNode || out.From != anchor.Add(3*time.Hour) || out.To != anchor.Add(4*time.Hour+30*time.Minute) {
			t.Fatalf("seed %d: outage %+v", seed, out)
		}
		evict := out.From.Add(5 * time.Minute)
		for _, p := range w.Pods {
			if p.Created.Before(out.To) && !p.Created.Before(out.From) && p.Node == failedNode {
				t.Errorf("seed %d: %s was created on node-2 at %s, while it is not ready", seed, p.Name, p.Created)
			}
		}
		evicted := 0
		for _, p := range w.Pods {
			if p.ReplacedBy == nil {
				continue
			}
			evicted++
			if p.Node != failedNode || w.Workloads[p.Workload].Kind == kindDaemonSet {
				t.Errorf("seed %d: %s was evicted from %s", seed, p.Name, w.Nodes[p.Node].Name)
			}
			if d := p.Deleted.Sub(evict); d < 0 || d > 20*time.Second {
				t.Errorf("seed %d: %s was deleted %s after the taint's seconds passed, want up to 20s", seed, p.Name, d)
			}
			r := p.ReplacedBy
			if !r.Created.Equal(p.Deleted.Add(time.Second)) || r.Node == failedNode || r.Workload != p.Workload {
				t.Errorf("seed %d: %s is replaced by %s created at %s on %s", seed, p.Name, r.Name, r.Created, w.Nodes[r.Node].Name)
			}
		}
		if evicted < 6 {
			t.Errorf("seed %d: %d pods were evicted, want at least the 6 that are not a DaemonSet's of the 10", seed, evicted)
		}
		// The DaemonSet pods are never deleted.
		for _, p := range w.podsOn(failedNode, anchor) {
			if w.Workloads[p.Workload].Kind == kindDaemonSet && !p.Deleted.IsZero() {
				t.Errorf("seed %d: the DaemonSet pod %s was deleted", seed, p.Name)
			}
		}
	}
}

// The rollout replaces the frontend's three pods one at a time, the new pod
// running before the old one is deleted, and the Job runs every hour.
func TestScriptOfTheRolloutAndTheJobs(t *testing.T) {
	for _, seed := range seeds {
		w := NewWorld(seed)
		old := podsByWorkload(w, "shop", "frontend", anchor)
		if len(old) != 3 {
			t.Fatalf("seed %d: %d frontend pods at the anchor", seed, len(old))
		}
		var fresh []*Pod
		for _, p := range w.Pods {
			if wl := w.Workloads[p.Workload]; wl.Name == "frontend" && p.Created.After(anchor) && p.Owner == ownerOf(wl, wl.NextReplicaSet) {
				fresh = append(fresh, p)
			}
		}
		if len(fresh) < 3 {
			t.Fatalf("seed %d: %d frontend pods of the new ReplicaSet", seed, len(fresh))
		}
		for i, o := range old {
			// The first three pods of the new ReplicaSet are the rollout's, in order;
			// an eviction adds more after them.
			n := fresh[i]
			start := anchor.Add(90*time.Minute + time.Duration(i)*90*time.Second)
			if d := n.Created.Sub(start); d < 0 || d > 20*time.Second {
				t.Errorf("seed %d: successor %d is created %s after %s, want up to 20s", seed, i, d, start)
			}
			if !n.Started.Before(o.Deleted) {
				t.Errorf("seed %d: old pod %d is deleted at %s but its successor runs only at %s", seed, i, o.Deleted, n.Started)
			}
		}
		jobs := 0
		for _, p := range w.Pods {
			if w.Workloads[p.Workload].Kind != kindJob {
				continue
			}
			jobs++
			want := anchor.Add(15*time.Minute + time.Duration(jobs-1)*time.Hour)
			if d := p.Created.Sub(want); d < 0 || d > 20*time.Second {
				t.Errorf("seed %d: job pod %d created %s after %s, want up to 20s", seed, jobs, d, want)
			}
			if p.Finished.Sub(p.Started) != 4*time.Minute || p.Deleted.Sub(p.Finished) != 10*time.Minute {
				t.Errorf("seed %d: job pod %s runs %s and is kept %s", seed, p.Name, p.Finished.Sub(p.Started), p.Deleted.Sub(p.Finished))
			}
		}
		if jobs != 6 {
			t.Errorf("seed %d: %d job pods, want 6", seed, jobs)
		}
	}
}

// Every request is at a whole second in the window, the requests are ordered by
// instant and then emitter, and a pod is not listed or reported before it is
// created or after it is deleted.
func TestRequestsAreInOrderInTheWindow(t *testing.T) {
	w := NewWorld(1)
	reqs := w.Requests()
	end := anchor.Add(Span)
	counts := map[Emitter]int{}
	for i, r := range reqs {
		counts[r.Emitter]++
		if r.At.Before(anchor) || r.At.After(end) || r.At.Nanosecond() != 0 {
			t.Errorf("%s at %s is outside the window or not a whole second", r.What, r.At)
		}
		if i > 0 {
			prev := reqs[i-1]
			if r.At.Before(prev.At) || (r.At.Equal(prev.At) && r.Emitter < prev.Emitter) {
				t.Errorf("%s at %s comes after %s at %s", r.What, r.At, prev.What, prev.At)
			}
		}
	}
	// 73 instants of the five-minute cadence, from the anchor to the end.
	for e, want := range map[Emitter]int{EmitObjectsPull: 2 * 73, EmitClusterReport: 73, EmitNodeAgent: 4 * 73} {
		if counts[e] != want {
			t.Errorf("%d requests of %s, want %d", counts[e], e, want)
		}
	}
	if counts[EmitObjectsWatch] == 0 {
		t.Error("no watch request")
	}
	// Node-2's agent keeps reporting while the node is not ready.
	during := 0
	for _, r := range reqs {
		if r.Emitter == EmitNodeAgent && r.What == "host event of node-2" && !r.At.Before(w.Outages[0].From) && r.At.Before(w.Outages[0].To) {
			during++
		}
	}
	if during != 18 {
		t.Errorf("node-2's agent reported %d times while it was not ready, want 18", during)
	}
}

// Every report carries the state of the kube-system namespace under the one fixed
// uid, so that the detector of cluster identity has its sighting, and the other
// namespaces under uids of their own.
func TestEveryReportSightsKubeSystem(t *testing.T) {
	w := NewWorld(1)
	reports := 0
	for _, r := range w.Requests() {
		if r.Emitter != EmitClusterReport {
			continue
		}
		reports++
		sighted := 0
		for _, rl := range r.Data().ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					var typ, name string
					for _, kv := range lr.Attributes {
						switch kv.Key {
						case "entity.type":
							typ = *kv.Value.StringValue
						case "entity.description":
							for _, d := range kv.Value.KVListValue.Values {
								if d.Key == "k8s.namespace.name" {
									name = *d.Value.StringValue
								}
							}
						}
					}
					if typ == "k8s.namespace" && name == "kube-system" && lr.EventName == "entity.state" {
						sighted++
					}
				}
			}
		}
		if sighted != 1 {
			t.Fatalf("the report at %s has %d states of kube-system, want 1", r.At, sighted)
		}
	}
	if reports != 73 {
		t.Errorf("%d reports, want 73", reports)
	}
	uids := map[string]bool{}
	for _, ns := range w.Namespaces {
		if uids[ns.UID] {
			t.Errorf("the uid of %s is used twice", ns.Name)
		}
		uids[ns.UID] = true
		if (ns.Name == "kube-system") != (ns.UID == kubeSystemUID) {
			t.Errorf("namespace %s has uid %s", ns.Name, ns.UID)
		}
	}
}
