package itbench

import (
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
	// Observed is when the collector listed the object.
	Observed time.Time
	Labels   map[string]string
	Owners   []ownerRef
	// Selector is a Service's selector.
	Selector map[string]string
	// NodeName is a Pod's spec.nodeName.
	NodeName string
}

// Cluster is one snapshot of a cluster's objects, as the export holds them: the
// kinds the mapping reads, and the pods and nodes the loader is given.
type Cluster struct {
	// Objects are the mapped kinds: pods, nodes, and the workloads and services
	// that own or select pods.
	Objects []*Object
	// Nodes are the Node objects, by name.
	Nodes []*Object
	// Observed is the latest time a pod or node was listed: the snapshot's time.
	Observed time.Time
	// Load are the pull-mode records of the pods and nodes, in file order.
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

// ReadCluster reads a Kubernetes objects export. The file is one snapshot. Only the
// pods and nodes become records for the loader; a row of any other resource that
// the mapping does not read is counted and set aside without being parsed.
func ReadCluster(path string) (*Cluster, error) { return readCluster(path, true) }

// readCluster reads the export; with load unset it keeps no record for the loader,
// which table B has no use for.
func readCluster(path string, load bool) (*Cluster, error) {
	c := &Cluster{Rows: map[string]int{}, owned: map[string][]*Object{}}
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
		o.Observed = observed
		c.Objects = append(c.Objects, o)
		if load && (kind == "Pod" || kind == "Node") {
			obj, err := factload.NewObject(res, []byte(body), observed)
			if err != nil {
				return err
			}
			c.Load = append(c.Load, obj)
		}
		if (kind == "Pod" || kind == "Node") && observed.After(c.Observed) {
			c.Observed = observed
		}
		if kind == "Node" {
			c.Nodes = append(c.Nodes, o)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for _, o := range c.Objects {
		for _, ow := range o.Owners {
			c.owned[ow.UID] = append(c.owned[ow.UID], o)
		}
	}
	return c, nil
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

// aliveAt reports whether the snapshot shows a pod at t: it was created by t, and t
// is before the time it was listed plus the loader's TTL.
func aliveAt(p *Object, t time.Time) bool {
	return !p.Created.IsZero() && !p.Created.After(t) && t.Before(p.Observed.Add(factload.ObjectTTL))
}
