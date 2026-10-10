// Package k8sobjects translates the log records of the OpenTelemetry Kubernetes
// objects receiver into store records.
//
// The receiver emits two kinds of record. In watch mode the body is a watch
// event: a type (ADDED, MODIFIED or DELETED) and the object. In pull mode the
// body is one object of a periodic list. Neither sets the log record's own
// timestamp, only the observed time, so a record's event time is taken from the
// object when the object carries a time for what changed, and is the observed
// time otherwise. The record says which of the two it used in its event time
// basis.
//
// # Entities and edges
//
// A pod ([catalog.K8sPod], keyed by its UID) and a node ([catalog.K8sNode], keyed
// by its UID) are entities. The name never identifies anything: a node deleted
// and created again under one name is two entities. A container that reports a
// runtime id is an entity ([catalog.Container], keyed by that id), with a part_of
// edge to its pod; it takes [Config.EntityTTL] and is re-asserted with each record
// of its pod, so that it lapses with the pod. A container with no runtime id (the
// fake containers of a simulated cluster have none) is not an entity, and each time
// one is seen it is counted as a skip. A pod that names a node has a scheduled_on edge to it, stored
// on the pod side.
//
// # Event time
//
//   - The first record of a UID the translator has not seen, whether a watch
//     event or a list item, takes the object's creationTimestamp (basis object
//     field). After a restart every object is new to the translator, so the first
//     list re-asserts each object at its creation time. That is harmless: the
//     producer and the content are the same and the later sequence number wins.
//     A record before the store's retention horizon is for the sequencer's late
//     path, not for this package.
//   - A later watch event whose description changed takes the newest
//     lastTransitionTime among the conditions that changed and the newest
//     startedAt or finishedAt of the container states that changed (basis object
//     field). Only the current state of a container is read, never lastState.
//   - Everything else takes the observed time (basis observed): a list item of
//     a UID already seen, a change that carries no time of its own (a phase or a
//     reason), a refresh, and the removal of an object (watch DELETED).
//   - A time taken from an object is used only if it lies between the object's
//     creationTimestamp and the observed time plus [MaxClockAhead]. Otherwise the
//     record takes the observed time and the translator counts a skip with
//     [SkipImplausibleTime].
//   - A record's event time is never earlier than the previous record's of the
//     same UID: an earlier one is raised to it, with that record's basis, and
//     counted with [SkipClampedTime].
//
// deletionTimestamp says that deletion was requested, not that it happened. It
// is part of the description and never ends anything: a pod and its edges end at
// the pod's own DELETED, never at its node's, and never from the absence of an
// object from a list. A list that omits an object proves nothing, because an
// empty list leaves no trace in the capture, so the translator infers no Delete
// from it.
//
// # Description
//
// A record's payload is canonical JSON (sorted keys) of a fixed allow-list of
// descriptive fields. The fields that change on every heartbeat or write
// (resourceVersion, managedFields, lastHeartbeatTime, lastProbeTime) are not in
// it, and neither is the observed time or any other collector field. A record
// that changes none of the listed fields produces no record when the translator
// is in watch mode ([Config.EntityTTL] zero), because a reference with no TTL
// needs no refresh.
//
// # Nodes by name
//
// A pod names its node, not its UID. The translator keeps an index from node name
// to the intervals of the node UIDs that have borne it, built from the node
// records it has seen, and resolves the name at the time the edge begins. That is
// the transition time of the pod's PodScheduled condition if it is True and
// plausible, and otherwise the time of the pod record that named the node. If
// exactly one interval covers that time, the edge is emitted, and its payload
// keeps the raw name and the basis of the resolution (the node-name index and the
// start of the node's interval) so that a later correction of ownership can
// resolve it again. If none covers it, or more than one does, the edge is pending:
// the translator keeps it, returns it in [Result.Pending], and emits the edge in
// the call that delivers a node whose name and interval cover the pod's time. It
// also tries again at each later record of the pod, and then the edge begins at
// that record's time: later than the truth, so the gap is unattributed and not
// guessed. A pending edge whose pod is deleted first is dropped and counted.
//
// # After a restart
//
// The state is in memory. A pod seen before a restart is unseen after it, and its
// DELETED is addressed from the deleted object itself: the delete of each of its
// containers that has a runtime id, of their part_of edges, and of its scheduled_on
// edge if the node name resolves at the observed time or, failing that, at the
// time the pod was scheduled (otherwise the edge is counted with
// [SkipUnseenDelete]). A Delete of a reference this producer never
// held changes nothing.
//
// A Translator is not safe for concurrent use.
package k8sobjects

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// MaxClockAhead is how far past a record's observed time a time taken from an
// object may lie before the translator distrusts it. The apiserver and the
// collector keep separate clocks.
const MaxClockAhead = 5 * time.Minute

// The reasons a translator gives for a record or part of a record it does not
// turn into a store record.
const (
	// SkipEntityEvent is an entity event of the cluster receiver
	// (entity.state or entity.delete), which is not translated here.
	SkipEntityEvent = "entity event"
	// SkipUnsupportedKind is an object of a kind other than a pod or a node.
	SkipUnsupportedKind = "unsupported kind"
	// SkipUnsupportedBody is a record whose body is not an object or a watch event.
	SkipUnsupportedBody = "unsupported body"
	// SkipUnsupportedWatchType is a watch event that is not ADDED, MODIFIED or
	// DELETED.
	SkipUnsupportedWatchType = "unsupported watch type"
	// SkipMissingUID is an object with no metadata.uid.
	SkipMissingUID = "missing uid"
	// SkipAfterDelete is a record about a UID that a DELETED already ended.
	SkipAfterDelete = "record after delete"
	// SkipContainerNoID is a container whose status has no runtime id. It is
	// counted once per pod record that carries it.
	SkipContainerNoID = "container without runtime id"
	// SkipBadContainerID is a container id that is not "<runtime>://<id>".
	SkipBadContainerID = "container id without runtime prefix"
	// SkipImplausibleTime is a time in an object that the translator did not
	// use, because it is unreadable or lies outside the plausible range.
	SkipImplausibleTime = "implausible field time"
	// SkipMissingCreation is an object with no creationTimestamp.
	SkipMissingCreation = "missing creation timestamp"
	// SkipPendingDroppedDeleted is a pending scheduled_on edge dropped because
	// its pod was deleted before the node was known.
	SkipPendingDroppedDeleted = "pending edge dropped: pod deleted"
	// SkipPendingDroppedRebound is a pending scheduled_on edge dropped because
	// the pod now names another node.
	SkipPendingDroppedRebound = "pending edge dropped: node name changed"
	// SkipUnseenDelete is the DELETED of a pod the translator has not seen,
	// whose scheduled_on edge it cannot address because the node name does not
	// resolve at the observed time.
	SkipUnseenDelete = "delete of an unseen pod: its edge is not addressed"
	// SkipClampedTime is an event time raised to the previous record's of the
	// same UID, because an older time would make the fold show the older
	// description after the newer one.
	SkipClampedTime = "event time raised to the previous record's"
)

// ErrNoObservedTime is returned for a record that sets no observed time, the one
// time every record is expected to carry.
var ErrNoObservedTime = errors.New("k8sobjects: record has no observed time")

// Config configures a [Translator].
type Config struct {
	// Producer names the source of the assertions: a stable logical name from
	// configuration, such as "k8sobjects/default", never a process. Required.
	Producer lifecycle.Producer
	// Resolver turns the identifying attributes into fingerprints. Required;
	// usually identity.NewResolver(catalog.Default()).
	Resolver *identity.Resolver
	// EntityTTL is the TTL of every pod and node observation. Zero is watch
	// semantics: a reference ends only by a Delete, and a record whose
	// description did not change is not emitted. With a TTL above zero, every pod
	// and node record, from a watch or a list and changed or not, is an Observe
	// with that TTL, so each list refreshes it. A container takes the same TTL
	// and is re-asserted with each record of its pod. Edges always have TTL
	// zero: they end by a Delete, or with the existence of the pod.
	EntityTTL time.Duration
}

// Result is what one log record stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced. They
	// have no Seq, no Boot and no Through.
	Records []store.Record
	// Pending are the scheduled_on edges this call could not resolve because the
	// node name is not yet known to the index. The translator keeps them and
	// emits the edge in a later Result once a node covers them.
	Pending []PendingEdge
	// Skipped says what was set aside and why, sorted by reason.
	Skipped []Skip
}

// Skip is a count of things set aside for one reason.
type Skip struct {
	Reason string
	Count  int
}

// PendingEdge is a scheduled_on edge whose node is not yet known.
type PendingEdge struct {
	// Pod is the pod, and PodUID its UID.
	Pod    identity.Fingerprint
	PodUID string
	// NodeName is the raw spec.nodeName.
	NodeName string
	// Time and Basis are the event time and basis the edge will have: those of
	// the pod record that named the node.
	Time  time.Time
	Basis store.EventTimeBasis
}

// Translator turns log records into store records. It keeps the last description
// of every UID it has seen, an index from node name to the node UIDs that bore
// it, and the scheduled_on edges still waiting for a node.
type Translator struct {
	producer lifecycle.Producer
	resolver *identity.Resolver
	ttl      time.Duration

	pods    map[string]*podState
	nodes   map[string]*nodeState
	deleted map[string]struct{}
	byName  map[string][]*nodeSpan
	pending map[string]PendingEdge
}

type podState struct {
	fp   identity.Fingerprint
	view podView
	// last and lastBasis are the event time and basis of the newest record emitted
	// for the UID.
	last      time.Time
	lastBasis store.EventTimeBasis
	edge      podEdge
	ctrs      map[string]ctrState
}

// podEdge is the scheduled_on edge of a pod. With a name and a zero node it is
// pending; with both it is established.
type podEdge struct {
	nodeName string
	node     identity.Fingerprint
}

type ctrState struct {
	id string
	fp identity.Fingerprint
}

type nodeState struct {
	fp        identity.Fingerprint
	view      nodeView
	last      time.Time
	lastBasis store.EventTimeBasis
	span      *nodeSpan
}

// nodeSpan is the interval one node UID bore a name: from its first observation
// to its deletion (zero while open).
type nodeSpan struct {
	fp       identity.Fingerprint
	from, to time.Time
}

// New returns a translator with no state.
func New(c Config) (*Translator, error) {
	switch {
	case c.Producer == "":
		return nil, errors.New("k8sobjects: producer is required")
	case c.Resolver == nil:
		return nil, errors.New("k8sobjects: resolver is required")
	case c.EntityTTL < 0:
		return nil, errors.New("k8sobjects: entity TTL is negative")
	}
	return &Translator{
		producer: c.Producer,
		resolver: c.Resolver,
		ttl:      c.EntityTTL,
		pods:     make(map[string]*podState),
		nodes:    make(map[string]*nodeState),
		deleted:  make(map[string]struct{}),
		byName:   make(map[string][]*nodeSpan),
		pending:  make(map[string]PendingEdge),
	}, nil
}

// Pending returns the scheduled_on edges still waiting for a node, ordered by
// time and then pod UID.
func (t *Translator) Pending() []PendingEdge {
	out := slices.Collect(maps.Values(t.pending))
	sortPending(out)
	return out
}

func sortPending(ps []PendingEdge) {
	slices.SortFunc(ps, func(a, b PendingEdge) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return strings.Compare(a.PodUID, b.PodUID)
	})
}

// batch collects what one call produces.
type batch struct {
	recs  []store.Record
	pend  []PendingEdge
	skips map[string]int
}

func (b *batch) skip(reason string) {
	if b.skips == nil {
		b.skips = make(map[string]int)
	}
	b.skips[reason]++
}

func (b *batch) result() Result {
	r := Result{Records: b.recs, Pending: b.pend}
	for _, reason := range slices.Sorted(maps.Keys(b.skips)) {
		r.Skipped = append(r.Skipped, Skip{Reason: reason, Count: b.skips[reason]})
	}
	return r
}

// clock bounds the object times a record may use.
type clock struct {
	observed time.Time
	// creation is the object's creationTimestamp, or zero if it has none or the
	// translator distrusts it.
	creation time.Time
}

func (c clock) plausible(tm time.Time) bool {
	if tm.Before(store.MinEventTime) || tm.After(c.observed.Add(MaxClockAhead)) {
		return false
	}
	return c.creation.IsZero() || !tm.Before(c.creation)
}

// clockFor reads the object's creation time and counts a creation time it
// cannot use.
func (b *batch) clockFor(creation string, observed time.Time) clock {
	c := clock{observed: observed}
	if creation == "" {
		b.skip(SkipMissingCreation)
		return c
	}
	tm, ok := parseTime(creation)
	if !ok || !c.plausible(tm) {
		b.skip(SkipImplausibleTime)
		return c
	}
	c.creation = tm
	return c
}

// objectTime returns the newest of the given object times if it is plausible,
// and otherwise the observed time. No times at all is the observed time too.
func (b *batch) objectTime(raws []string, c clock) (time.Time, store.EventTimeBasis) {
	var best time.Time
	for _, raw := range raws {
		tm, ok := parseTime(raw)
		if !ok {
			b.skip(SkipImplausibleTime)
			continue
		}
		if tm.After(best) {
			best = tm
		}
	}
	if best.IsZero() {
		return c.observed, store.BasisObserved
	}
	if !c.plausible(best) {
		b.skip(SkipImplausibleTime)
		return c.observed, store.BasisObserved
	}
	return best, store.BasisObjectField
}

// Translate turns one OTLP log record into records without Seq. It is
// deterministic given the sequence of earlier inputs: the translator keeps the
// last description it saw per UID and a node-name index.
//
// An error means the record could not be read at all (it has no observed time, or
// its UID is not a valid key), and leaves the translator unchanged. A record that
// is valid but not translated is not an error: it is in [Result.Skipped].
func (t *Translator) Translate(res capture.Resource, lr capture.LogRecord) (Result, error) {
	var b batch
	if strings.HasPrefix(lr.EventName, "entity.") {
		b.skip(SkipEntityEvent)
		return b.result(), nil
	}
	if lr.ObservedTimeUnixNano == 0 || lr.ObservedTimeUnixNano > 1<<63-1 {
		return Result{}, ErrNoObservedTime
	}
	observed := time.Unix(0, int64(lr.ObservedTimeUnixNano)).UTC()

	var body object
	if lr.Body != nil {
		body, _ = plain(*lr.Body).(object)
	}
	if body == nil {
		b.skip(SkipUnsupportedBody)
		return b.result(), nil
	}
	watchType, o := "", body
	if typ, ok := body["type"].(string); ok && obj(body, "object") != nil {
		watchType, o = typ, obj(body, "object")
		switch typ {
		case "ADDED", "MODIFIED", "DELETED":
		default:
			b.skip(SkipUnsupportedWatchType)
			return b.result(), nil
		}
	}

	kind := str(o, "kind")
	if kind == "" {
		for _, kv := range lr.Attributes {
			if kv.Key == "k8s.resource.name" && kv.Value.StringValue != nil {
				kind = map[string]string{"pods": "Pod", "nodes": "Node"}[*kv.Value.StringValue]
			}
		}
	}
	var err error
	switch kind {
	case "Pod":
		err = t.pod(&b, watchType, o, namespaceOf(res), observed)
	case "Node":
		err = t.node(&b, watchType, o, observed)
	default:
		b.skip(SkipUnsupportedKind)
	}
	if err != nil {
		return Result{}, err
	}
	return b.result(), nil
}

// namespaceOf is the namespace the receiver puts on the resource, used only for
// an object that does not name its own.
func namespaceOf(res capture.Resource) string {
	for _, kv := range res.Attributes {
		if kv.Key == "k8s.namespace.name" && kv.Value.StringValue != nil {
			return *kv.Value.StringValue
		}
	}
	return ""
}

func (t *Translator) fingerprint(typ catalog.EntityType, key catalog.AttributeKey, v string) (identity.Fingerprint, error) {
	id, err := t.resolver.Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("k8sobjects: %s %q: %w", typ, v, err)
	}
	return id.Fingerprint(), nil
}

func layerOf(fp identity.Fingerprint) catalog.Layer {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		// The resolver and the default catalog list the same types.
		panic("k8sobjects: entity type " + string(fp.Type()) + " is not in the default catalog")
	}
	return e.Layer()
}

func (t *Translator) emit(b *batch, subj store.Subject, kind lifecycle.Kind, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte) {
	r := store.Record{
		Layer:          layerOf(subj.A),
		Subject:        subj,
		Producer:       t.producer,
		EventTime:      at,
		Kind:           kind,
		EventTimeBasis: basis,
	}
	if kind == lifecycle.Observe {
		r.TTL = ttl
		r.Payload = bytes.Clone(payload)
	}
	b.recs = append(b.recs, r)
}

func (t *Translator) observeEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Observe, at, basis, ttl, payload)
}

func (t *Translator) deleteEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Delete, at, basis, 0, nil)
}

func (t *Translator) observeEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis, payload []byte) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Observe, at, basis, 0, payload)
}

func (t *Translator) deleteEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Delete, at, basis, 0, nil)
}

// raise holds an event time at the previous record's of the same UID, with its
// basis, and counts it. The fold orders by event time: a full description asserted
// earlier than the previous record would show the older description after the
// newer one.
func (b *batch) raise(at time.Time, basis store.EventTimeBasis, last time.Time, lastBasis store.EventTimeBasis) (time.Time, store.EventTimeBasis) {
	if at.Before(last) {
		b.skip(SkipClampedTime)
		return last, lastBasis
	}
	return at, basis
}

// eventTime picks the event time and basis of a record that is not the first of
// its UID and not a removal: the observed time for a list item or an unchanged
// description, and otherwise the newest time the change itself carries.
func (b *batch) eventTime(pull, changed bool, raws []string, c clock) (time.Time, store.EventTimeBasis) {
	if pull || !changed {
		return c.observed, store.BasisObserved
	}
	return b.objectTime(raws, c)
}

func (t *Translator) pod(b *batch, watchType string, o object, ns string, observed time.Time) error {
	v := parsePod(o, ns)
	if v.UID == "" {
		b.skip(SkipMissingUID)
		return nil
	}
	if _, gone := t.deleted[v.UID]; gone {
		b.skip(SkipAfterDelete)
		return nil
	}
	fp, err := t.fingerprint(catalog.K8sPod, catalog.K8sPodUID, v.UID)
	if err != nil {
		return err
	}
	st := t.pods[v.UID]
	if watchType == "DELETED" {
		t.deletePod(b, st, fp, v, observed)
		return nil
	}

	c := b.clockFor(v.Creation, observed)
	first := st == nil
	changed := first || !bytes.Equal(st.view.payload, v.payload)
	var (
		at    time.Time
		basis store.EventTimeBasis
	)
	switch {
	case first && !c.creation.IsZero():
		at, basis = c.creation, store.BasisObjectField
	case first:
		at, basis = observed, store.BasisObserved
	default:
		at, basis = b.eventTime(watchType == "", changed, changedTimes(st.view.Conds, v.Conds, st.view.Ctrs, v.Ctrs), c)
		at, basis = b.raise(at, basis, st.last, st.lastBasis)
	}
	if first {
		st = &podState{fp: fp, ctrs: make(map[string]ctrState)}
		t.pods[v.UID] = st
	}

	n := len(b.recs)
	if changed || t.ttl > 0 {
		t.observeEntity(b, fp, at, basis, t.ttl, v.payload)
	}
	t.reconcileContainers(b, st, v, at, basis)
	t.reconcileEdge(b, st, v, c, at, basis)
	st.view = v
	if len(b.recs) > n && at.After(st.last) {
		st.last, st.lastBasis = at, basis
	}
	return nil
}

// runtimeID returns the id of a container from its "<runtime>://<id>" status.
func runtimeID(s string) (string, bool) {
	runtime, id, ok := strings.Cut(s, "://")
	if !ok || runtime == "" || strings.TrimSpace(id) == "" {
		return "", false
	}
	return id, true
}

// containersOf returns the containers of the pod that have a runtime id, by name.
// Only those are entities. When count is set, each container set aside is counted.
func (t *Translator) containersOf(b *batch, v podView, count bool) map[string]ctrState {
	want := make(map[string]ctrState)
	for _, c := range v.Ctrs {
		if c.ID == "" {
			if count {
				b.skip(SkipContainerNoID)
			}
			continue
		}
		id, ok := runtimeID(c.ID)
		if !ok {
			if count {
				b.skip(SkipBadContainerID)
			}
			continue
		}
		fp, err := t.fingerprint(catalog.Container, catalog.ContainerID, id)
		if err != nil {
			if count {
				b.skip(SkipBadContainerID)
			}
			continue
		}
		want[c.Name] = ctrState{id: id, fp: fp}
	}
	return want
}

// reconcileContainers ends the containers of the pod that are gone or have a new
// id, and begins those that are new. With a TTL, it also re-asserts the ones that
// stay, so that they lapse when the pod's records stop.
func (t *Translator) reconcileContainers(b *batch, st *podState, v podView, at time.Time, basis store.EventTimeBasis) {
	want := t.containersOf(b, v, true)
	for _, name := range slices.Sorted(maps.Keys(st.ctrs)) {
		old := st.ctrs[name]
		if w, ok := want[name]; ok && w.id == old.id {
			continue
		}
		t.deleteEdge(b, old.fp, st.fp, catalog.PartOf, at, basis)
		t.deleteEntity(b, old.fp, at, basis)
		delete(st.ctrs, name)
	}
	for _, name := range slices.Sorted(maps.Keys(want)) {
		w := want[name]
		payload := canonical(object{"name": name, "pod_uid": v.UID})
		if _, ok := st.ctrs[name]; ok {
			if t.ttl > 0 {
				t.observeEntity(b, w.fp, at, basis, t.ttl, payload)
			}
			continue
		}
		t.observeEntity(b, w.fp, at, basis, t.ttl, payload)
		t.observeEdge(b, w.fp, st.fp, catalog.PartOf, at, basis, []byte("{}"))
		st.ctrs[name] = w
	}
}

// edgePayload says what the edge was resolved from: the raw node name and the
// basis, so that a later correction of ownership can resolve it again.
func edgePayload(name string, sp *nodeSpan) []byte {
	return canonical(object{
		"node_name": name,
		"resolution": object{
			"basis":               "node-name-index",
			"node_interval_start": sp.from.Format(time.RFC3339Nano),
		},
	})
}

// edgeStart is when the scheduled_on edge of the pod begins: the transition time of
// its PodScheduled condition if that is True and plausible (basis object field),
// and otherwise the time and basis of the record that established it. A pod first
// seen in a list was created long before it was bound, and the creation time
// would put the edge before the node it names.
func (b *batch) edgeStart(v podView, c clock, at time.Time, basis store.EventTimeBasis) (time.Time, store.EventTimeBasis) {
	for _, cond := range v.Conds {
		if cond.Type != "PodScheduled" || cond.Status != "True" || cond.Time == "" {
			continue
		}
		tm, ok := parseTime(cond.Time)
		if !ok || !c.plausible(tm) {
			b.skip(SkipImplausibleTime)
			break
		}
		return tm, store.BasisObjectField
	}
	return at, basis
}

// reconcileEdge keeps the scheduled_on edge of the pod in step with the node it
// names. A pod is bound once and never moves, but if the name changes the old
// edge ends and a new one begins. An edge that is still pending is tried again at
// each later record of the pod, and begins at that record's time: later than the
// truth, so the gap is unattributed and not guessed.
func (t *Translator) reconcileEdge(b *batch, st *podState, v podView, c clock, at time.Time, basis store.EventTimeBasis) {
	if v.NodeName == st.edge.nodeName {
		if v.NodeName != "" && st.edge.node.IsZero() {
			if sp, ok := t.lookup(v.NodeName, at); ok {
				t.observeEdge(b, st.fp, sp.fp, catalog.ScheduledOn, at, basis, edgePayload(v.NodeName, sp))
				st.edge.node = sp.fp
				delete(t.pending, v.UID)
			}
		}
		return
	}
	t.dropEdge(b, st, at, basis, SkipPendingDroppedRebound)
	st.edge = podEdge{nodeName: v.NodeName}
	if v.NodeName == "" {
		return
	}
	from, fromBasis := b.edgeStart(v, c, at, basis)
	if sp, ok := t.lookup(v.NodeName, from); ok {
		t.observeEdge(b, st.fp, sp.fp, catalog.ScheduledOn, from, fromBasis, edgePayload(v.NodeName, sp))
		st.edge.node = sp.fp
		return
	}
	p := PendingEdge{Pod: st.fp, PodUID: v.UID, NodeName: v.NodeName, Time: from, Basis: fromBasis}
	t.pending[v.UID] = p
	b.pend = append(b.pend, p)
}

// dropEdge ends the established edge of the pod, or forgets its pending one.
func (t *Translator) dropEdge(b *batch, st *podState, at time.Time, basis store.EventTimeBasis, reason string) {
	switch {
	case st.edge.nodeName == "":
	case !st.edge.node.IsZero():
		t.deleteEdge(b, st.fp, st.edge.node, catalog.ScheduledOn, at, basis)
	default:
		for uid, p := range t.pending {
			if p.Pod == st.fp {
				delete(t.pending, uid)
				b.skip(reason)
			}
		}
	}
	st.edge = podEdge{}
}

// deletePod ends a pod at its own DELETED, at the observed time, with its
// containers and its edge.
//
// A DELETED for a pod the translator has not seen (it was seen before a restart,
// and the state is in memory) is addressed from the deleted object itself: its
// containers that have a runtime id, and its edge if the node name resolves at the
// observed time. A Delete of a reference this producer never held changes nothing.
func (t *Translator) deletePod(b *batch, st *podState, fp identity.Fingerprint, v podView, observed time.Time) {
	t.deleted[v.UID] = struct{}{}
	if st == nil {
		ctrs := t.containersOf(b, v, false)
		for _, name := range slices.Sorted(maps.Keys(ctrs)) {
			t.deleteEdge(b, ctrs[name].fp, fp, catalog.PartOf, observed, store.BasisObserved)
			t.deleteEntity(b, ctrs[name].fp, observed, store.BasisObserved)
		}
		if v.NodeName != "" {
			sp, ok := t.lookup(v.NodeName, observed)
			if !ok {
				// The node may be gone by now (a pod failed by the garbage collector
				// outlives its node): try again when the pod was scheduled.
				c := clock{observed: observed}
				if tm, ok := parseTime(v.Creation); ok && c.plausible(tm) {
					c.creation = tm
				}
				if from, _ := b.edgeStart(v, c, observed, store.BasisObserved); !from.Equal(observed) {
					sp, ok = t.lookup(v.NodeName, from)
				}
			}
			if ok {
				t.deleteEdge(b, fp, sp.fp, catalog.ScheduledOn, observed, store.BasisObserved)
			} else {
				b.skip(SkipUnseenDelete)
			}
		}
		t.deleteEntity(b, fp, observed, store.BasisObserved)
		return
	}
	at, basis := b.raise(observed, store.BasisObserved, st.last, st.lastBasis)
	for _, name := range slices.Sorted(maps.Keys(st.ctrs)) {
		c := st.ctrs[name]
		t.deleteEdge(b, c.fp, st.fp, catalog.PartOf, at, basis)
		t.deleteEntity(b, c.fp, at, basis)
	}
	t.dropEdge(b, st, at, basis, SkipPendingDroppedDeleted)
	t.deleteEntity(b, fp, at, basis)
	delete(t.pods, v.UID)
}

func (t *Translator) node(b *batch, watchType string, o object, observed time.Time) error {
	v := parseNode(o)
	if v.UID == "" {
		b.skip(SkipMissingUID)
		return nil
	}
	if _, gone := t.deleted[v.UID]; gone {
		b.skip(SkipAfterDelete)
		return nil
	}
	fp, err := t.fingerprint(catalog.K8sNode, catalog.K8sNodeUID, v.UID)
	if err != nil {
		return err
	}
	st := t.nodes[v.UID]
	c := b.clockFor(v.Creation, observed)

	if watchType == "DELETED" {
		t.deleted[v.UID] = struct{}{}
		at, basis := observed, store.BasisObserved
		if st != nil {
			at, basis = b.raise(at, basis, st.last, st.lastBasis)
		}
		t.deleteEntity(b, fp, at, basis)
		switch {
		case st != nil && st.span != nil:
			st.span.to = at
		case v.Name != "":
			// A node deleted before it was seen: its interval is known only from
			// the object's own creation time, and is empty without one.
			from := c.creation
			if from.IsZero() || from.After(at) {
				from = at
			}
			t.byName[v.Name] = append(t.byName[v.Name], &nodeSpan{fp: fp, from: from, to: at})
		}
		delete(t.nodes, v.UID)
		return nil
	}

	first := st == nil
	changed := first || !bytes.Equal(st.view.payload, v.payload)
	var (
		at    time.Time
		basis store.EventTimeBasis
	)
	switch {
	case first && !c.creation.IsZero():
		at, basis = c.creation, store.BasisObjectField
	case first:
		at, basis = observed, store.BasisObserved
	default:
		at, basis = b.eventTime(watchType == "", changed, changedTimes(st.view.Conds, v.Conds, nil, nil), c)
		at, basis = b.raise(at, basis, st.last, st.lastBasis)
	}
	if first {
		st = &nodeState{fp: fp}
		t.nodes[v.UID] = st
	}
	emitted := false
	if changed || t.ttl > 0 {
		t.observeEntity(b, fp, at, basis, t.ttl, v.payload)
		emitted = true
	}
	st.view = v
	if emitted && at.After(st.last) {
		st.last, st.lastBasis = at, basis
	}
	if first && v.Name != "" {
		st.span = &nodeSpan{fp: fp, from: at}
		t.byName[v.Name] = append(t.byName[v.Name], st.span)
		t.resolvePending(b, v.Name)
	}
	return nil
}

// lookup resolves a node name at a time: the one node interval that covers it. No
// interval, or more than one, resolves nothing.
func (t *Translator) lookup(name string, at time.Time) (*nodeSpan, bool) {
	var hit *nodeSpan
	for _, sp := range t.byName[name] {
		if at.Before(sp.from) || (!sp.to.IsZero() && !at.Before(sp.to)) {
			continue
		}
		if hit != nil {
			return nil, false
		}
		hit = sp
	}
	return hit, hit != nil
}

// resolvePending emits the edges of the pending pods that the node name now
// resolves, at each pod record's own event time.
func (t *Translator) resolvePending(b *batch, name string) {
	var ps []PendingEdge
	for _, p := range t.pending {
		if p.NodeName == name {
			ps = append(ps, p)
		}
	}
	sortPending(ps)
	for _, p := range ps {
		sp, ok := t.lookup(name, p.Time)
		if !ok {
			continue
		}
		t.observeEdge(b, p.Pod, sp.fp, catalog.ScheduledOn, p.Time, p.Basis, edgePayload(name, sp))
		t.pods[p.PodUID].edge.node = sp.fp
		delete(t.pending, p.PodUID)
	}
}
