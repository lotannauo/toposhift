package k8sobjects

import (
	"encoding/json"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// handle is the translator's handle of a record it marked in [Result.Backdated]:
// the state that times the subject's later records.
type handle struct {
	pod  *podState
	node *nodeState
	// ctr is the name of a container of the pod: its entity if entity is set,
	// else its part_of edge. peer is the container, or the node of a scheduled_on
	// edge.
	ctr    string
	entity bool
	peer   identity.Fingerprint
}

// Resumed is told the final event time and basis of a record this translator
// marked, once the store has resumed it (see [resume.Apply]). It raises the state
// that times the subject's later records to them: a pod's, a node's or a
// container's latest record, the latest assertion of a scheduled_on edge, and that
// of a container's part_of edge. It never lowers any of them, so a later record of the subject is
// never timed before it, and it changes nothing for a handle it did not give, or
// for a subject whose state has moved on (an edge that has since ended). It does
// no I/O.
func (t *Translator) Resumed(h any, at time.Time, basis store.EventTimeBasis) {
	hd, ok := h.(*handle)
	if !ok || hd == nil {
		return
	}
	raise := func(last *time.Time, lastBasis *store.EventTimeBasis) {
		if at.After(*last) {
			*last, *lastBasis = at, basis
		}
	}
	switch {
	case hd.node != nil:
		raise(&hd.node.last, &hd.node.lastBasis)
	case hd.pod == nil:
	case hd.ctr != "":
		c, ok := hd.pod.ctrs[hd.ctr]
		switch {
		case !ok || c.fp != hd.peer:
		case hd.entity:
			raise(&c.last, &c.lastBasis)
		default:
			raise(&c.since, &c.sinceBasis)
		}
		if ok {
			hd.pod.ctrs[hd.ctr] = c
		}
	case !hd.peer.IsZero():
		if e := &hd.pod.edge; e.node == hd.peer {
			raise(&e.since, &e.sinceBasis)
		}
	default:
		raise(&hd.pod.last, &hd.pod.lastBasis)
	}
}

// Comparable is the part of a payload this translator wrote for a subject that
// describes it, which [resume.Apply] compares with the stored payload. A pod's, a
// node's and a container's payload is a description throughout (an allow-list of
// the object's own fields), and so is a part_of edge's. A scheduled_on edge's
// payload also says how its node was resolved, from this translator's own node
// index, which a translator with no state resolves anew: only its node_name
// describes the edge. A payload that cannot be read is compared whole.
func Comparable(sub store.Subject, payload []byte) []byte {
	if sub.Kind != store.SubjectEdge || sub.Relation != catalog.ScheduledOn {
		return payload
	}
	var p struct {
		NodeName *string `json:"node_name"`
	}
	if err := json.Unmarshal(payload, &p); err != nil || p.NodeName == nil {
		return payload
	}
	return canonical(object{"node_name": *p.NodeName})
}
