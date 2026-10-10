package k8sobjects

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
)

// A description is the fixed set of fields a translator keeps from an object. It
// is a short allow-list, not the object: the fields that change on every
// heartbeat (resourceVersion, managedFields, lastHeartbeatTime, lastProbeTime)
// and the fields that say nothing about what the object is are left out, so
// that a record that changes none of the kept fields is not a change.

// condView is one condition of an object. The heartbeat and probe times, and
// the free-text message, are not kept.
type condView struct {
	Type, Status, Reason, Time string
}

// ctrView is the status of one container of a pod, as the pod reports it.
type ctrView struct {
	Name     string
	Restarts int64
	// State is the kind of the current state: running, waiting or terminated, or
	// empty. The previous state (lastState) is not read.
	State string
	// Started and Finished are the times the current state gives: running and
	// terminated carry startedAt, terminated carries finishedAt.
	Started, Finished string
	// ID is the container's runtime id as the pod reports it ("<runtime>://<id>"),
	// or empty. It is not part of the payload; it only decides which containers
	// are entities.
	ID string
}

type podView struct {
	UID, Namespace, Name string
	Creation             string
	NodeName             string
	Deletion             string
	Conds                []condView
	Ctrs                 []ctrView
	payload              []byte
}

type nodeView struct {
	UID, Name string
	Creation  string
	Conds     []condView
	payload   []byte
}

func conditions(o object) []condView {
	var out []condView
	for _, c := range list(o, "status", "conditions") {
		out = append(out, condView{
			Type:   str(c, "type"),
			Status: str(c, "status"),
			Reason: str(c, "reason"),
			Time:   str(c, "lastTransitionTime"),
		})
	}
	slices.SortStableFunc(out, func(a, b condView) int { return strings.Compare(a.Type, b.Type) })
	return out
}

// parsePod reads a pod. ns is the namespace the resource names, used only if the
// object does not name its own.
func parsePod(o object, ns string) podView {
	v := podView{
		UID:       str(o, "metadata", "uid"),
		Namespace: str(o, "metadata", "namespace"),
		Name:      str(o, "metadata", "name"),
		Creation:  str(o, "metadata", "creationTimestamp"),
		NodeName:  str(o, "spec", "nodeName"),
		Deletion:  str(o, "metadata", "deletionTimestamp"),
		Conds:     conditions(o),
	}
	for _, c := range list(o, "status", "containerStatuses") {
		cv := ctrView{
			Name:     str(c, "name"),
			Restarts: integer(c, "restartCount"),
			ID:       str(c, "containerID"),
		}
		// Only the current state is read. lastState describes an earlier run of the
		// container, and its times are not this object's.
		for _, kind := range []string{"running", "waiting", "terminated"} {
			if s := obj(c, "state", kind); s != nil {
				cv.State = kind
				cv.Started = str(s, "startedAt")
				cv.Finished = str(s, "finishedAt")
				break
			}
		}
		v.Ctrs = append(v.Ctrs, cv)
	}
	slices.SortStableFunc(v.Ctrs, func(a, b ctrView) int { return strings.Compare(a.Name, b.Name) })

	if v.Namespace == "" {
		v.Namespace = ns
	}
	m := object{}
	put(m, "name", v.Name)
	put(m, "namespace", v.Namespace)
	put(m, "node_name", v.NodeName)
	put(m, "phase", str(o, "status", "phase"))
	put(m, "reason", str(o, "status", "reason"))
	put(m, "host_ip", str(o, "status", "hostIP"))
	put(m, "deletion_timestamp", v.Deletion)
	if ips := list(o, "status", "podIPs"); len(ips) > 0 {
		var a []any
		for _, ip := range ips {
			if s := str(ip, "ip"); s != "" {
				a = append(a, s)
			}
		}
		if len(a) > 0 {
			m["pod_ips"] = a
		}
	}
	if ow := owner(o); ow != nil {
		m["owner"] = ow
	}
	if cs := condsJSON(v.Conds); len(cs) > 0 {
		m["conditions"] = cs
	}
	if len(v.Ctrs) > 0 {
		var a []any
		for _, c := range v.Ctrs {
			e := object{"name": c.Name, "restart_count": c.Restarts}
			put(e, "state", c.State)
			put(e, "started_at", c.Started)
			put(e, "finished_at", c.Finished)
			a = append(a, e)
		}
		m["containers"] = a
	}
	v.payload = canonical(m)
	return v
}

func parseNode(o object) nodeView {
	v := nodeView{
		UID:      str(o, "metadata", "uid"),
		Name:     str(o, "metadata", "name"),
		Creation: str(o, "metadata", "creationTimestamp"),
		Conds:    conditions(o),
	}
	m := object{}
	put(m, "name", v.Name)
	labels := object{}
	for _, k := range []string{"kubernetes.io/hostname", "topology.kubernetes.io/region", "topology.kubernetes.io/zone"} {
		put(labels, k, str(o, "metadata", "labels", k))
	}
	if len(labels) > 0 {
		m["labels"] = labels
	}
	if cs := condsJSON(v.Conds); len(cs) > 0 {
		m["conditions"] = cs
	}
	// Descriptive only: never used to identify a host. A providerID, an address
	// or a boot id says what the node reports about itself, and a different
	// source may report the same machine under another one.
	info := object{}
	put(info, "boot_id", str(o, "status", "nodeInfo", "bootID"))
	put(info, "machine_id", str(o, "status", "nodeInfo", "machineID"))
	put(info, "system_uuid", str(o, "status", "nodeInfo", "systemUUID"))
	if len(info) > 0 {
		m["node_info"] = info
	}
	put(m, "provider_id", str(o, "spec", "providerID"))
	var addrs [][2]string
	for _, a := range list(o, "status", "addresses") {
		typ, addr := str(a, "type"), str(a, "address")
		if typ != "" || addr != "" {
			addrs = append(addrs, [2]string{typ, addr})
		}
	}
	slices.SortFunc(addrs, func(a, b [2]string) int { return slices.Compare(a[:], b[:]) })
	if len(addrs) > 0 {
		a := make([]any, len(addrs))
		for i, p := range addrs {
			e := object{}
			put(e, "type", p[0])
			put(e, "address", p[1])
			a[i] = e
		}
		m["addresses"] = a
	}
	v.payload = canonical(m)
	return v
}

// owner is the controller of the pod, or its first owner if none is marked as
// the controller.
func owner(o object) object {
	refs := list(o, "metadata", "ownerReferences")
	if len(refs) == 0 {
		return nil
	}
	pick := refs[0]
	for _, r := range refs {
		if c, _ := r["controller"].(bool); c {
			pick = r
			break
		}
	}
	m := object{}
	put(m, "kind", str(pick, "kind"))
	put(m, "name", str(pick, "name"))
	if len(m) == 0 {
		return nil
	}
	return m
}

func condsJSON(cs []condView) []any {
	var a []any
	for _, c := range cs {
		e := object{}
		put(e, "type", c.Type)
		put(e, "status", c.Status)
		put(e, "reason", c.Reason)
		put(e, "last_transition_time", c.Time)
		a = append(a, e)
	}
	return a
}

// put sets key to s unless s is empty: an absent field and an empty one are the
// same description.
func put(m object, key, s string) {
	if s != "" {
		m[key] = s
	}
}

// canonical encodes m as JSON with sorted keys and no insignificant white space.
// The encoder sorts the keys of every map, so equal descriptions are equal bytes.
func canonical(m object) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(m); err != nil {
		// Only strings, integers, slices and maps of them are put in m.
		panic("k8sobjects: encoding a description: " + err.Error())
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}

// changedTimes returns the object-field times of what changed between two
// descriptions: the lastTransitionTime of each condition that is new or differs,
// and the startedAt and finishedAt of each container whose value is new or
// differs. Empty strings are left out.
func changedTimes(prevConds, conds []condView, prevCtrs, ctrs []ctrView) []string {
	var out []string
	oldConds := make(map[string]condView, len(prevConds))
	for _, c := range prevConds {
		oldConds[c.Type] = c
	}
	for _, c := range conds {
		if o, ok := oldConds[c.Type]; (!ok || o != c) && c.Time != "" {
			out = append(out, c.Time)
		}
	}
	oldCtrs := make(map[string]ctrView, len(prevCtrs))
	for _, c := range prevCtrs {
		oldCtrs[c.Name] = c
	}
	for _, c := range ctrs {
		o, ok := oldCtrs[c.Name]
		if c.Started != "" && (!ok || o.Started != c.Started) {
			out = append(out, c.Started)
		}
		if c.Finished != "" && (!ok || o.Finished != c.Finished) {
			out = append(out, c.Finished)
		}
	}
	return out
}
