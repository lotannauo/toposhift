package itbench

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/bench/eval/factload"
)

// ComponentLabel is the pod label the demo's chart sets its service name from. It
// is the only way a Kubernetes object is mapped to a service here.
const ComponentLabel = "app.kubernetes.io/component"

// The columns of the Kubernetes objects export that are read. Each row is one log
// record whose body is the object as JSON.
const (
	colTimestamp     = "Timestamp"
	colBody          = "Body"
	colLogAttributes = "LogAttributes"
)

// ownerRef is an ownerReference of an object.
type ownerRef struct {
	UID  string `json:"uid"`
	Kind string `json:"kind"`
}

// Object is the part of a listed Kubernetes object that the mapping reads.
type Object struct {
	Kind      string
	Namespace string
	Name      string
	UID       string
	Created   time.Time
	// First and Last are the first and the last time the collector listed the
	// object: an export that is a series of pulls lists an object once in each. The
	// fields below are those of the last listing.
	First, Last time.Time
	Labels      map[string]string
	Owners      []ownerRef
	// Selector is a Service's selector.
	Selector map[string]string
	// NodeName is a Pod's spec.nodeName.
	NodeName string
}

// Cluster is a cluster's objects as an export holds them: the kinds the mapping
// reads, and the pods and nodes the loader is given. An export is one pull of the
// collector, or a series of pulls at a fixed interval; each object of a kind and uid
// is one entry however many pulls list it.
type Cluster struct {
	// Objects are the mapped kinds: pods, nodes, and the workloads and services
	// that own or select pods, one for each kind and uid.
	Objects []*Object
	// Nodes are the Node objects.
	Nodes []*Object
	// First and Last are the times of the first and the last row of a pod or a node:
	// the span of the pulls.
	First, Last time.Time
	// Pulls is the number of pulls of pods and nodes: rows whose times are within a
	// minute of each other are one pull. Interval is the median time between the
	// starts of successive pulls, to the second, or zero if there is one pull.
	Pulls    int
	Interval time.Duration
	// Lifetime is how long the loader keeps an object after the last pull that
	// listed it: factload.Lifetime of the interval.
	Lifetime time.Duration
	// Load are the pull-mode records of the pods and nodes, in order of the time they
	// were listed.
	Load []factload.Object
	// Rows counts the rows read, by kind of resource.
	Rows map[string]int

	owned map[string][]*Object
}

// kindOf maps the resource name the export carries to the kinds the mapping reads.
var kindOf = map[string]string{
	"pods": "Pod", "nodes": "Node", "replicasets": "ReplicaSet", "deployments": "Deployment",
	"statefulsets": "StatefulSet", "daemonsets": "DaemonSet", "services": "Service",
}

// ReadCluster reads a Kubernetes objects export. Rows of one kind and uid are one
// object, with its first and last listing. Only the pods and nodes become records
// for the loader; a row of any other resource that
// the mapping does not read is counted and set aside without being parsed.
func ReadCluster(path string) (*Cluster, error) { return readCluster(path, true) }

// readCluster reads the export; with load unset it keeps no record for the loader,
// which table B has no use for.
func readCluster(path string, load bool) (*Cluster, error) {
	c := &Cluster{Rows: map[string]int{}, owned: map[string][]*Object{}}
	type key struct{ kind, uid string }
	seen := map[key]*Object{}
	var times []time.Time
	err := rows(path, []string{colTimestamp, colBody, colLogAttributes}, func(r *row) error {
		attrs, err := parseMap(r.get(colLogAttributes))
		if err != nil {
			return err
		}
		res := attrs["k8s.resource.name"]
		c.Rows[res]++
		kind, ok := kindOf[res]
		if !ok {
			return nil
		}
		observed, err := parseExportTime(r.get(colTimestamp))
		if err != nil {
			return err
		}
		body := r.get(colBody)
		o, err := parseObject([]byte(body))
		if err != nil {
			return err
		}
		if o.Kind != kind {
			return fmt.Errorf("a %s row holds an object of another kind", kind)
		}
		o.First, o.Last = observed, observed
		if prev := seen[key{kind, o.UID}]; prev != nil && o.UID != "" {
			// Another listing of an object already read: one object, listed again. It
			// takes the fields of its latest listing.
			first, last, created := prev.First, prev.Last, prev.Created
			if observed.After(last) {
				*prev = *o
				last = observed
			}
			if observed.Before(first) {
				first = observed
			}
			prev.First, prev.Last = first, last
			if prev.Created.IsZero() {
				prev.Created = created
			}
			o = prev
		} else {
			if o.UID != "" {
				seen[key{kind, o.UID}] = o
			}
			c.Objects = append(c.Objects, o)
			if kind == "Node" {
				c.Nodes = append(c.Nodes, o)
			}
		}
		if kind == "Pod" || kind == "Node" {
			times = append(times, observed)
			if load {
				obj, err := factload.NewObject(res, []byte(body), observed)
				if err != nil {
					return err
				}
				c.Load = append(c.Load, obj)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	c.detectPulls(times)
	if c.Pulls > 1 {
		slices.SortStableFunc(c.Load, func(a, b factload.Object) int {
			return cmp.Compare(a.Record.ObservedTimeUnixNano, b.Record.ObservedTimeUnixNano)
		})
	}
	for _, o := range c.Objects {
		for _, ow := range o.Owners {
			c.owned[ow.UID] = append(c.owned[ow.UID], o)
		}
	}
	return c, nil
}

// pullGap is the gap in the times of rows that starts a new pull: the rows of one
// pull are stamped within a few seconds.
const pullGap = time.Minute

// detectPulls finds the pulls among the times of the pod and node rows, and the
// interval between them.
func (c *Cluster) detectPulls(times []time.Time) {
	c.Lifetime = factload.Lifetime(0)
	if len(times) == 0 {
		return
	}
	slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
	c.First, c.Last = times[0], times[len(times)-1]
	starts := []time.Time{times[0]}
	for i := 1; i < len(times); i++ {
		if times[i].Sub(times[i-1]) > pullGap {
			starts = append(starts, times[i])
		}
	}
	c.Pulls = len(starts)
	if c.Pulls < 2 {
		return
	}
	gaps := make([]time.Duration, 0, len(starts)-1)
	for i := 1; i < len(starts); i++ {
		gaps = append(gaps, starts[i].Sub(starts[i-1]))
	}
	slices.Sort(gaps)
	c.Interval = gaps[len(gaps)/2].Round(time.Second)
	c.Lifetime = factload.Lifetime(c.Interval)
}

func parseObject(raw []byte) (*Object, error) {
	var j struct {
		Kind     string `json:"kind"`
		Metadata struct {
			UID               string            `json:"uid"`
			Name              string            `json:"name"`
			Namespace         string            `json:"namespace"`
			CreationTimestamp string            `json:"creationTimestamp"`
			Labels            map[string]string `json:"labels"`
			OwnerReferences   []ownerRef        `json:"ownerReferences"`
		} `json:"metadata"`
		Spec struct {
			NodeName string `json:"nodeName"`
			Selector any    `json:"selector"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(raw, &j); err != nil {
		return nil, fmt.Errorf("an object body that is not JSON: %w", err)
	}
	o := &Object{
		Kind: j.Kind, Namespace: j.Metadata.Namespace, Name: j.Metadata.Name, UID: j.Metadata.UID,
		Labels: j.Metadata.Labels, Owners: j.Metadata.OwnerReferences, NodeName: j.Spec.NodeName,
	}
	if ts := j.Metadata.CreationTimestamp; ts != "" {
		t, err := time.Parse(time.RFC3339, ts)
		if err != nil {
			return nil, fmt.Errorf("a creation time that is not RFC 3339")
		}
		o.Created = t.UTC()
	}
	// A Service selects by a flat map of labels; the selectors of workloads are
	// not read, since their pods are found by ownership.
	if j.Kind == "Service" {
		if m, ok := j.Spec.Selector.(map[string]any); ok {
			o.Selector = map[string]string{}
			for k, v := range m {
				s, ok := v.(string)
				if !ok {
					o.Selector = nil
					break
				}
				o.Selector[k] = s
			}
		}
	}
	return o, nil
}

// pods lists the pods an object is, owns or selects. A workload owns the pods whose
// chain of owner references, by uid, reaches it; a Service selects the pods of its
// namespace that carry every label of its selector, and a Service without a
// selector selects none. Selectors and owners are matched exactly.
func (c *Cluster) pods(o *Object) []*Object {
	switch o.Kind {
	case "Pod":
		return []*Object{o}
	case "Service":
		if len(o.Selector) == 0 {
			return nil
		}
		var out []*Object
		for _, p := range c.Objects {
			if p.Kind != "Pod" || p.Namespace != o.Namespace {
				continue
			}
			ok := true
			for k, v := range o.Selector {
				if got, has := p.Labels[k]; !has || got != v {
					ok = false
					break
				}
			}
			if ok {
				out = append(out, p)
			}
		}
		return out
	case "Deployment", "ReplicaSet", "StatefulSet", "DaemonSet":
		var out []*Object
		seen := map[string]bool{}
		queue := []*Object{o}
		for len(queue) > 0 {
			cur := queue[0]
			queue = queue[1:]
			for _, ch := range c.owned[cur.UID] {
				if seen[ch.UID] {
					continue
				}
				seen[ch.UID] = true
				if ch.Kind == "Pod" {
					out = append(out, ch)
				} else {
					queue = append(queue, ch)
				}
			}
		}
		return out
	}
	return nil
}

// Components are the distinct service names of the pods of an object: the values
// of ComponentLabel, sorted. A pod without the label contributes none and is
// counted in missing.
func (c *Cluster) Components(o *Object) (names []string, missing int) {
	set := map[string]bool{}
	for _, p := range c.pods(o) {
		v := p.Labels[ComponentLabel]
		if v == "" {
			missing++
			continue
		}
		set[v] = true
	}
	for n := range set {
		names = append(names, n)
	}
	slices.Sort(names)
	return names, missing
}

// Select lists the objects of a kind in a namespace whose name satisfies match, in
// the order they were listed.
func (c *Cluster) Select(kind, namespace string, match func(name string) bool) []*Object {
	var out []*Object
	for _, o := range c.Objects {
		if o.Kind == kind && o.Namespace == namespace && match(o.Name) {
			out = append(out, o)
		}
	}
	return out
}

// Placed reports whether a pod's node is one of the listed nodes: the loader
// places a pod only on a node it was given, by an exact name.
func (c *Cluster) Placed(p *Object) bool {
	if p.NodeName == "" {
		return false
	}
	n := 0
	for _, node := range c.Nodes {
		if node.Name == p.NodeName {
			n++
		}
	}
	return n == 1
}

// aliveAt reports whether the pulls show a pod at t, as the loader asserts it: it
// was created by t, and t is before the last time it was listed plus the loader's
// lifetime of an object.
func (c *Cluster) aliveAt(p *Object, t time.Time) bool {
	return !p.Created.IsZero() && !p.Created.After(t) && t.Before(p.Last.Add(c.Lifetime))
}
