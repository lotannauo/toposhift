package k8scluster

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
	node *nodeState
	pod  *podState
	ctr  *ctrState
	// peer is the node of a scheduled_on edge, or the pod of a part_of edge.
	peer identity.Fingerprint
}

// Resumed is told the final event time and basis of a record this translator
// marked, once the store has resumed it (see [resume.Apply]). It raises the state
// that times the subject's later records to them: a node's, a pod's or a
// container's latest record, the latest assertion of a pod's scheduled_on edge,
// and that of a container's part_of edge. It never lowers any of them, so a later
// record of the subject is never timed before it, and it changes nothing for a
// handle it did not give, or for an edge that has since ended. It does no I/O.
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
	case hd.pod != nil && hd.peer.IsZero():
		raise(&hd.pod.last, &hd.pod.lastBasis)
	case hd.pod != nil:
		if e := &hd.pod.edge; e.node == hd.peer {
			raise(&e.since, &e.sinceBasis)
		}
	case hd.ctr != nil && hd.peer.IsZero():
		raise(&hd.ctr.last, &hd.ctr.lastBasis)
	case hd.ctr != nil:
		if hd.ctr.podFP == hd.peer {
			raise(&hd.ctr.since, &hd.ctr.sinceBasis)
		}
	}
}

// Comparable is the part of a payload this translator wrote for a subject that
// describes it, which [resume.Apply] compares with the stored payload. A node's, a
// pod's and a container's payload is a description throughout (the allow-list of
// the description's keys, listed in the package documentation), and so is a
// part_of edge's. A scheduled_on edge's payload also says how its node was
// resolved, from this translator's own node index, which a translator with no
// state resolves anew: only its node_name describes the edge. A payload that
// cannot be read is compared whole.
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
	return canonical(map[string]string{"node_name": *p.NodeName})
}
