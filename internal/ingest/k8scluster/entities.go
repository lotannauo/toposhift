package k8scluster

import (
	"bytes"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// node translates a node state or delete.
func (t *Translator) node(b *batch, ev entityEvent, id string, fp identity.Fingerprint) {
	dk := deletedKey(typeNode, id)
	if _, gone := t.deleted[dk]; gone {
		b.skip(SkipAfterDelete)
		return
	}
	st := t.nodes[id]
	if !ev.state {
		at, basis := b.instant, b.basis
		if st != nil {
			at, basis = b.raise(at, basis, st.last, st.lastBasis)
		}
		t.deleteEntity(b, fp, at, basis)
		if st != nil && st.span != nil {
			st.span.to = at
		}
		delete(t.nodes, id)
		t.deleted[dk] = struct{}{}
		return
	}

	d := describe(typeNode, ev.desc)
	t.noteDropped(b, d.dropped)
	first := st == nil
	var (
		at    time.Time
		basis store.EventTimeBasis
	)
	if first {
		at, basis = b.firstTime(stringOf(ev.desc, keyNodeCreated))
		st = &nodeState{fp: fp}
		t.nodes[id] = st
	} else {
		at, basis = b.raise(b.instant, b.basis, st.last, st.lastBasis)
	}
	changed := first || !bytes.Equal(st.payload, d.payload)
	st.payload = d.payload

	asserted := at
	if changed || t.ttl() > 0 {
		t.observeEntity(b, fp, at, basis, d.payload, b.instant)
		if first {
			b.mark(&handle{node: st})
		}
		if at.After(st.last) {
			st.last, st.lastBasis = at, basis
		}
		if at.Before(b.instant) {
			asserted = b.instant
		}
		if st.span != nil && asserted.After(st.span.last) {
			st.span.last = asserted
		}
	}
	if name := stringOf(ev.desc, keyNodeName); st.span == nil && name != "" {
		st.span = &nodeSpan{fp: fp, from: at, last: asserted}
		t.byName[name] = append(t.byName[name], st.span)
		t.resolvePending(b, name)
	}
}

// nodeSpan is the interval one node UID bore a name: from its first observation
// to its delete (zero while open). last is the latest instant an Observe of the
// node asserted it, from which a span that is still open is taken to end a TTL
// later.
type nodeSpan struct {
	fp       identity.Fingerprint
	from, to time.Time
	last     time.Time
}

// end is when the span ends, or the zero time if it does not. A span ends at its
// node's delete. With a TTL, one that is still open ends a TTL after the node was
// last asserted, so that a node whose delete was lost does not hold its name for
// ever. The end is derived here and never stored: a later record of the node
// moves it.
func (sp *nodeSpan) end(ttl time.Duration) time.Time {
	if !sp.to.IsZero() {
		return sp.to
	}
	if ttl > 0 {
		return sp.last.Add(ttl)
	}
	return time.Time{}
}

// lookup resolves a node name at a time: the one node interval that covers it.
// No interval resolves nothing, and neither do two or more, which is counted with
// [SkipAmbiguousNodeName]: the name is never guessed.
func (t *Translator) lookup(b *batch, name string, at time.Time) (*nodeSpan, bool) {
	var hit *nodeSpan
	covering := 0
	ttl := t.ttl()
	for _, sp := range t.byName[name] {
		if at.Before(sp.from) {
			continue
		}
		if end := sp.end(ttl); !end.IsZero() && !at.Before(end) {
			continue
		}
		covering++
		if hit == nil {
			hit = sp
		}
	}
	if covering > 1 {
		b.skip(SkipAmbiguousNodeName)
		return nil, false
	}
	return hit, hit != nil
}

// resolvePending writes the edges of the pending pods that the node name now
// resolves, at each pod record's own event time. An edge that waited for its node
// is asserted through the latest instant of its pod's records, not the node's.
func (t *Translator) resolvePending(b *batch, name string) {
	var ps []PendingEdge
	for _, p := range t.pending {
		if p.NodeName == name {
			ps = append(ps, p)
		}
	}
	sortPending(ps)
	for _, p := range ps {
		sp, ok := t.lookup(b, name, p.Time)
		if !ok {
			continue
		}
		pod := t.pods[p.PodUID]
		t.beginEdge(b, pod, sp, name, p.Time, p.Basis, pod.seen)
		delete(t.pending, p.PodUID)
	}
}

// pod translates a pod state or delete.
func (t *Translator) pod(b *batch, ev entityEvent, id string, fp identity.Fingerprint) {
	dk := deletedKey(typePod, id)
	if _, gone := t.deleted[dk]; gone {
		b.skip(SkipAfterDelete)
		return
	}
	st := t.pods[id]
	if !ev.state {
		t.deletePod(b, st, id, fp)
		t.deleted[dk] = struct{}{}
		return
	}

	d := describe(typePod, ev.desc)
	t.noteDropped(b, d.dropped)
	first := st == nil
	var (
		at    time.Time
		basis store.EventTimeBasis
	)
	if first {
		at, basis = b.firstTime(stringOf(ev.desc, keyPodCreated))
		st = &podState{fp: fp}
		t.pods[id] = st
	} else {
		at, basis = b.raise(b.instant, b.basis, st.last, st.lastBasis)
	}
	changed := first || !bytes.Equal(st.payload, d.payload)
	st.payload = d.payload
	if b.instant.After(st.seen) {
		st.seen = b.instant
	}

	if changed || t.ttl() > 0 {
		t.observeEntity(b, fp, at, basis, d.payload, b.instant)
		if first {
			b.mark(&handle{pod: st})
		}
		if at.After(st.last) {
			st.last, st.lastBasis = at, basis
		}
	}
	t.reconcileEdge(b, st, id, stringOf(ev.desc, keyNodeName), at, basis)
}

// edgePayload says what the edge was resolved from: the raw node name and the
// basis, so that a later correction of ownership can resolve it again.
func edgePayload(name string, sp *nodeSpan) []byte {
	return canonical(map[string]any{
		"node_name": name,
		"resolution": map[string]string{
			"basis":               "node-name-index",
			"node_interval_start": sp.from.Format(time.RFC3339Nano),
		},
	})
}

// reconcileEdge keeps the scheduled_on edge of the pod in step with the node it
// names. A pod is bound once and never moves, but if the name changes the old edge
// ends and a new one begins.
//
// An edge begins at the instant of the first record that names its node, never at
// the event time of the pod (its creation, on the first record): a pod is bound
// after it is created, and the description carries no binding time. An edge that
// is still pending is tried again at each later record of the pod, and begins at
// that record's instant: later than the truth, so the gap is unattributed and not
// guessed.
//
// With a TTL, an edge established by an earlier record is re-asserted with each
// record of the pod, with the payload it was established with, at the record's
// event time or at the edge's own latest assertion if that is later. An edge begun
// by this record is not asserted twice.
func (t *Translator) reconcileEdge(b *batch, st *podState, uid, name string, at time.Time, basis store.EventTimeBasis) {
	if name == st.edge.nodeName {
		switch {
		case name == "":
		case st.edge.node.IsZero():
			if sp, ok := t.lookup(b, name, b.instant); ok {
				t.beginEdge(b, st, sp, name, b.instant, b.basis, b.instant)
				delete(t.pending, uid)
			}
		case t.ttl() > 0:
			e := &st.edge
			e.since, e.sinceBasis = b.raise(at, basis, e.since, e.sinceBasis)
			t.observeEdge(b, st.fp, e.node, catalog.ScheduledOn, e.since, e.sinceBasis, e.payload, b.instant)
		}
		return
	}
	t.dropEdge(b, st, uid, at, basis, SkipPendingDroppedRebound)
	st.edge = podEdge{nodeName: name}
	if name == "" {
		return
	}
	if sp, ok := t.lookup(b, name, b.instant); ok {
		t.beginEdge(b, st, sp, name, b.instant, b.basis, b.instant)
		return
	}
	p := PendingEdge{Pod: st.fp, PodUID: uid, NodeName: name, Time: b.instant, Basis: b.basis}
	t.pending[uid] = p
	b.pend = append(b.pend, p)
}

// beginEdge writes the scheduled_on edge of the pod to the node of sp, and keeps
// what a re-assertion needs. through is the instant the edge is asserted through
// if it is timed before it.
func (t *Translator) beginEdge(b *batch, st *podState, sp *nodeSpan, name string, at time.Time, basis store.EventTimeBasis, through time.Time) {
	payload := edgePayload(name, sp)
	t.observeEdge(b, st.fp, sp.fp, catalog.ScheduledOn, at, basis, payload, through)
	b.mark(&handle{pod: st, peer: sp.fp})
	st.edge.node, st.edge.payload = sp.fp, payload
	st.edge.since, st.edge.sinceBasis = at, basis
}

// dropEdge ends the established edge of the pod, or forgets its pending one.
func (t *Translator) dropEdge(b *batch, st *podState, uid string, at time.Time, basis store.EventTimeBasis, reason string) {
	switch {
	case st.edge.nodeName == "":
	case !st.edge.node.IsZero():
		at, basis = b.raise(at, basis, st.edge.since, st.edge.sinceBasis)
		t.deleteEdge(b, st.fp, st.edge.node, catalog.ScheduledOn, at, basis)
	default:
		if _, ok := t.pending[uid]; ok {
			delete(t.pending, uid)
			b.skip(reason)
		}
	}
	st.edge = podEdge{}
}

// deletePod ends a pod, with the containers this translator knows for it and
// their edges, and its scheduled_on edge. The Delete of a pod this translator has
// not seen is written all the same, and counted: its edge cannot be addressed,
// because the event does not say which node it was on.
func (t *Translator) deletePod(b *batch, st *podState, uid string, fp identity.Fingerprint) {
	at, basis := b.instant, b.basis
	if st != nil {
		at, basis = b.raise(at, basis, st.last, st.lastBasis)
	}
	for _, id := range slices.Sorted(maps.Keys(t.podCtrs[uid])) {
		t.endContainer(b, id, t.ctrs[id])
	}
	if st != nil {
		t.dropEdge(b, st, uid, at, basis, SkipPendingDroppedDeleted)
	} else {
		b.skip(SkipUnseenDelete)
	}
	t.deleteEntity(b, fp, at, basis)
	delete(t.pods, uid)
}

// container translates a container state or delete. id is the runtime id without
// its prefix; podUID is the pod the description names, if it names one.
func (t *Translator) container(b *batch, ev entityEvent, id string, fp identity.Fingerprint, podUID string, podFP identity.Fingerprint) {
	dk := deletedKey(typeContainer, id)
	if _, gone := t.deleted[dk]; gone {
		b.skip(SkipAfterDelete)
		return
	}
	st := t.ctrs[id]
	if !ev.state {
		if st != nil {
			t.endContainer(b, id, st)
		} else {
			b.skip(SkipUnseenDelete)
			t.deleteEntity(b, fp, b.instant, b.basis)
			t.deleted[dk] = struct{}{}
		}
		return
	}

	d := describe(typeContainer, ev.desc)
	t.noteDropped(b, d.dropped)
	first := st == nil
	var (
		at    time.Time
		basis store.EventTimeBasis
	)
	if first {
		at, basis = b.firstTime(stringOf(ev.desc, keyCtrCreated))
		st = &ctrState{fp: fp}
		t.ctrs[id] = st
	} else {
		at, basis = b.raise(b.instant, b.basis, st.last, st.lastBasis)
	}
	changed := first || !bytes.Equal(st.payload, d.payload)
	st.payload = d.payload

	if changed || t.ttl() > 0 {
		t.observeEntity(b, fp, at, basis, d.payload, b.instant)
		if first {
			b.mark(&handle{ctr: st})
		}
		if at.After(st.last) {
			st.last, st.lastBasis = at, basis
		}
	}

	if podUID == "" {
		b.skip(SkipContainerNoPod)
		return
	}
	switch {
	case st.podUID == "":
	case st.podUID != podUID:
		// A container is in one pod for good; if the event says otherwise the old
		// edge ends and a new one begins.
		since, sinceBasis := b.raise(at, basis, st.since, st.sinceBasis)
		t.deleteEdge(b, fp, st.podFP, catalog.PartOf, since, sinceBasis)
		delete(t.podCtrs[st.podUID], id)
		st.podUID = ""
	case t.ttl() > 0:
		st.since, st.sinceBasis = b.raise(at, basis, st.since, st.sinceBasis)
		t.observeEdge(b, fp, st.podFP, catalog.PartOf, st.since, st.sinceBasis, []byte("{}"), b.instant)
		return
	default:
		return
	}
	st.podUID, st.podFP = podUID, podFP
	st.since, st.sinceBasis = at, basis
	t.observeEdge(b, fp, podFP, catalog.PartOf, at, basis, []byte("{}"), b.instant)
	b.mark(&handle{ctr: st, peer: podFP})
	if t.podCtrs[podUID] == nil {
		t.podCtrs[podUID] = make(map[string]struct{})
	}
	t.podCtrs[podUID][id] = struct{}{}
}

// endContainer deletes a known container and its part_of edge, at the instant of
// the record or, if it asserted either later, at that time.
func (t *Translator) endContainer(b *batch, id string, st *ctrState) {
	if st.podUID != "" {
		at, basis := b.raise(b.instant, b.basis, st.since, st.sinceBasis)
		t.deleteEdge(b, st.fp, st.podFP, catalog.PartOf, at, basis)
		delete(t.podCtrs[st.podUID], id)
		if len(t.podCtrs[st.podUID]) == 0 {
			delete(t.podCtrs, st.podUID)
		}
	}
	at, basis := b.raise(b.instant, b.basis, st.last, st.lastBasis)
	t.deleteEntity(b, st.fp, at, basis)
	delete(t.ctrs, id)
	t.deleted[deletedKey(typeContainer, id)] = struct{}{}
}

// namespace translates a namespace event. A namespace is not in the catalog and
// gives no record. A state of kube-system, or the delete of a UID seen as such,
// gives a sighting.
func (t *Translator) namespace(b *batch, ev entityEvent, lr capture.LogRecord, received time.Time) (Result, error) {
	b.skip(SkipTypeNotInCatalog)
	uid := ev.id
	if !ev.hasID || strings.TrimSpace(uid) == "" {
		return b.result(), nil
	}
	if ev.state {
		if stringOf(ev.desc, keyNamespaceName) != KubeSystem {
			return b.result(), nil
		}
		if ev.iv.present && !ev.iv.valid {
			b.skip(SkipBadInterval)
			return b.result(), nil
		}
	} else if _, ok := t.kubeSystem[uid]; !ok {
		return b.result(), nil
	}

	eff, ttl := t.collection, t.ttl()
	if ev.state {
		if ev.iv.present {
			eff = ev.iv.d
		}
		ttl = DefaultTTLMultiple * eff
	}
	var err error
	if b.instant, b.basis, err = t.instant(b, lr, received, ttl); err != nil {
		return Result{}, err
	}
	s := NamespaceSighting{UID: uid, At: b.instant, Deleted: !ev.state}
	if ev.state {
		s.Interval = t.collection
		if ev.iv.present {
			s.Interval = ev.iv.d
		}
		t.kubeSystem[uid] = struct{}{}
	} else {
		delete(t.kubeSystem, uid)
	}
	b.sight = append(b.sight, s)
	return b.result(), nil
}
