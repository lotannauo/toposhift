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
// An edge takes the TTL of the entity it is stored on, and with a TTL it is
// re-asserted with each record of that entity (both part_of and scheduled_on, with
// the payload they were established with), so that it lapses with the entity when
// a Delete is lost: the store has no read-time cascade from an entity to its
// edges. A re-assertion is never timed before the edge's own latest assertion. A
// Delete carries no TTL, and a Delete is still written for the containers, their
// part_of edges and the scheduled_on edge of a pod at the pod's own DELETED.
//
// # Event time
//
//   - The first record of a UID the translator has not seen, whether a watch
//     event or a list item, takes the object's creationTimestamp (basis object
//     field). After a restart every object is new to the translator, so the first
//     list would re-assert each object at its creation time, late on every prefix
//     it touches. So first sightings are marked, and ingest resumes them from the
//     store (see [Result.Backdated] and "After a restart"). A record before the
//     store's retention horizon is for the sequencer's late path, not for this
//     package.
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
//   - An Observe, of an entity or an edge, whose event time is before the
//     record's observed time also carries Through equal to the observed time. A
//     Kubernetes object with a given UID exists continuously from its creation
//     to its deletion, and UIDs are never reused, so a producer that observes it
//     at T and learns it was created at C can assert its existence from C
//     through T. Without it, a pod first seen hours after its creation would show
//     as absent from C plus the TTL until its next record. The event time, the
//     basis and the TTL are unchanged. An Observe timed at or after the observed
//     time (a clock ahead by up to [MaxClockAhead]) has no Through, and a Delete
//     never has one. An edge resolved when its node arrives is asserted through
//     the pod's latest observation, not the node's: the node's arrival says
//     nothing about the pod.
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
// that changes none of the listed fields produces no record only with a zero TTL
// ([Config.EntityTTL] zero, which means no pull interval), because a reference
// with no TTL needs no refresh. With a TTL every pod and node record is an
// Observe, so that each pull refreshes it.
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
// A node name is resolved only within one translator, which is one producer. If
// more than one node UID bears the name at the time, it resolves to none, the edge
// stays pending, and each such lookup is counted with [SkipAmbiguousNodeName]. With
// a TTL, a node whose DELETED was lost does not hold its name for ever: its
// interval, which has no end until its DELETED, ends a TTL after the last
// instant its records asserted it. That end is derived when the name is looked up
// and never stored, so a node that is seen again extends it.
//
// # Pull generations
//
// The pull records of one kind (pods, nodes) from one list share an observed time,
// so a change in the observed time between pull records of a kind starts a new
// generation. With [Config.PullInterval] above zero, a generation whose observed
// time is more than one and a half intervals after the previous one's is counted
// once with [NotePullGap] in [Result.Notes]: the collector is pulling less often
// than the translator was told (an upstream default of an hour is the usual
// cause). Watch records never count, the first generation never counts, and pods
// and nodes are tracked apart. The caller logs notes; this package does not.
//
// # After a restart
//
// The state is in memory. A pod seen before a restart is unseen after it, so its
// next record is a first sighting: timed at its creation, its containers and their
// part_of edges with it, and its scheduled_on edge at its PodScheduled time (or,
// when the pod is listed before its node, at that time once the node arrives).
// Each such record that carries a Through is marked in [Result.Backdated], and
// ingest resumes it from the producer's last stored assertion of the subject (see
// package resume): an unchanged description is raised to it, a changed one takes
// the observed time. [Translator.Resumed] then raises, and never lowers, the state
// that times the subject's later records, so that none of them is timed before
// the resumed one. [Comparable] is what a description is compared on: everything
// but a scheduled_on edge's resolution, which this translator derives from its
// own node index.
//
// A pod's DELETED after a restart is addressed from the deleted object itself: its
// DELETED is addressed from the deleted object itself: the delete of each of its
// containers that has a runtime id, of their part_of edges, and of its scheduled_on
// edge if the node name resolves at the time the pod was scheduled, or at the
// observed time when the object has no scheduling time (otherwise the edge is
// counted with [SkipUnseenDelete]). The observed time is not tried after a
// scheduling time fails: the name may by then belong to a newer node. A Delete of
// a reference this producer never held changes nothing.
//
// A Translator is not safe for concurrent use.
package k8sobjects

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// The multiples of the pull interval that bound the TTL of a translator that is
// given one.
const (
	// DefaultTTLMultiple is the TTL, in pull intervals, of a translator whose
	// [Config.EntityTTL] is zero and whose [Config.PullInterval] is not.
	DefaultTTLMultiple = 3
	// MinTTLMultiple is the least TTL, in pull intervals, that [New] accepts
	// with a pull interval: a TTL under two intervals would let one late pull
	// lapse every object.
	MinTTLMultiple = 2
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
	// SkipAmbiguousNodeName is a lookup of a node name that more than one node UID
	// bears at the time asked. It resolves to none, and is counted once per lookup.
	SkipAmbiguousNodeName = "node name borne by more than one node"
)

// The reasons a translator gives for a note: something the caller should know
// that is not a record set aside.
const (
	// NotePullGap is a new generation of pull records of one kind whose observed
	// time is more than one and a half pull intervals after the previous
	// generation's.
	NotePullGap = "gap between pull generations over 1.5 times the pull interval"
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
	// EntityTTL is the TTL of every Observe the translator writes. Zero is watch
	// semantics: a reference ends only by a Delete, and a record whose
	// description did not change is not emitted. With a TTL above zero, every pod
	// and node record, from a watch or a list and changed or not, is an Observe
	// with that TTL, so each list refreshes it. A container takes the same TTL
	// and is re-asserted with each record of its pod, and so does an edge (part_of
	// with its container's, scheduled_on with its pod's), so that all of them
	// lapse when the pod's records stop and its Delete was lost. A Delete carries
	// no TTL.
	//
	// With a PullInterval above zero, a zero EntityTTL is DefaultTTLMultiple
	// pull intervals, and an EntityTTL under MinTTLMultiple pull intervals is
	// refused by [New]. With a zero PullInterval the TTL is used as given.
	EntityTTL time.Duration
	// PullInterval is the interval at which the collector lists the objects. Zero
	// says nothing about it: the TTL is EntityTTL as given, and no gap between
	// pull generations is reported. A translator has one TTL for all its life, so
	// a changed interval needs a new Translator, built by the caller; the TTL
	// applies from the next record.
	PullInterval time.Duration
}

// Result is what one log record stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced. They
	// have no Seq and no Boot, and no Through except for an Observe timed before
	// its observation, whose Through is the observed time.
	Records []store.Record
	// Pending are the scheduled_on edges this call could not resolve because the
	// node name is not yet known to the index. The translator keeps them and
	// emits the edge in a later Result once a node covers them.
	Pending []PendingEdge
	// Skipped says what was set aside and why, sorted by reason.
	Skipped []Skip
	// Notes say what the caller should know and log, sorted by reason. They do
	// not change the records.
	Notes []Skip
	// Backdated marks the records this call backdated, which [resume.Apply] times
	// from the store: the first record of a pod, a node or a container and of
	// its part_of edge, and every scheduled_on edge begun at a time before its
	// observation, directly or when its node arrived. Only an Observe that carries
	// a Through is marked. The handles are for [Translator.Resumed].
	Backdated []resume.Mark
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
	interval time.Duration

	// pulls is the newest pull generation seen, by kind.
	pulls [numKinds]pullGen

	pods    map[string]*podState
	nodes   map[string]*nodeState
	deleted map[string]struct{}
	byName  map[string][]*nodeSpan
	pending map[string]PendingEdge
}

// The kinds whose pull generations are tracked.
const (
	kindPod = iota
	kindNode
	numKinds
)

// pullGen is the observed time of the newest generation of pull records of one
// kind, if there is one.
type pullGen struct {
	seen     bool
	observed time.Time
}

type podState struct {
	fp   identity.Fingerprint
	view podView
	// last and lastBasis are the event time and basis of the newest record emitted
	// for the UID.
	last      time.Time
	lastBasis store.EventTimeBasis
	// observed is the latest observed time of the pod's records that are not its
	// DELETED: the last instant the producer saw the pod.
	observed time.Time
	edge     podEdge
	ctrs     map[string]ctrState
}

// podEdge is the scheduled_on edge of a pod. With a name and a zero node it is
// pending; with both it is established.
type podEdge struct {
	nodeName string
	node     identity.Fingerprint
	// payload is what the edge was established with, kept so that a re-assertion
	// repeats it exactly and not what the name resolves to now. since and
	// sinceBasis are the time and basis of the edge's latest Observe, which a
	// re-assertion is never timed before.
	payload    []byte
	since      time.Time
	sinceBasis store.EventTimeBasis
}

type ctrState struct {
	id string
	fp identity.Fingerprint
	// last and lastBasis are the time and basis of the container's latest record.
	// They follow its pod's, except where a resume raised them (see
	// [Translator.Resumed]).
	last      time.Time
	lastBasis store.EventTimeBasis
	// since and sinceBasis are the time and basis of the latest Observe of the
	// container's part_of edge.
	since      time.Time
	sinceBasis store.EventTimeBasis
}

type nodeState struct {
	fp        identity.Fingerprint
	view      nodeView
	last      time.Time
	lastBasis store.EventTimeBasis
	span      *nodeSpan
}

// nodeSpan is the interval one node UID bore a name: from its first observation
// to its deletion (zero while open). last is the latest instant an Observe of
// the node asserted it (its Through, or its event time), from which a span that
// is still open is taken to end a TTL later.
type nodeSpan struct {
	fp       identity.Fingerprint
	from, to time.Time
	last     time.Time
}

// end is when the span ends, or the zero time if it does not. A span ends at its
// node's DELETED. With a TTL, one that is still open ends a TTL after the node was
// last asserted, so that a node whose DELETED was lost does not hold its name for
// ever. The end is derived here and never stored: a later record of the node moves
// it.
func (sp *nodeSpan) end(ttl time.Duration) time.Time {
	if !sp.to.IsZero() {
		return sp.to
	}
	if ttl > 0 {
		return sp.last.Add(ttl)
	}
	return time.Time{}
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
	case c.PullInterval < 0:
		return nil, errors.New("k8sobjects: pull interval is negative")
	}
	ttl := c.EntityTTL
	if c.PullInterval > 0 {
		if c.PullInterval > math.MaxInt64/DefaultTTLMultiple {
			return nil, fmt.Errorf("k8sobjects: pull interval %s is too long", c.PullInterval)
		}
		switch {
		case ttl == 0:
			ttl = DefaultTTLMultiple * c.PullInterval
		case ttl < MinTTLMultiple*c.PullInterval:
			return nil, fmt.Errorf("k8sobjects: entity TTL %s is under %d times the pull interval %s", ttl, MinTTLMultiple, c.PullInterval)
		}
	}
	return &Translator{
		producer: c.Producer,
		resolver: c.Resolver,
		ttl:      ttl,
		interval: c.PullInterval,
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
	notes map[string]int
	marks []resume.Mark
	// observed is the observed time of the record being translated, which an
	// Observe timed before it takes as its Through.
	observed time.Time
}

// mark marks the record just emitted as backdated, with the handle h, if it is an
// Observe with a Through.
func (b *batch) mark(h *handle) {
	i := len(b.recs) - 1
	if i < 0 || b.recs[i].Kind != lifecycle.Observe || !b.recs[i].Through.After(b.recs[i].EventTime) {
		return
	}
	var hv any
	if h != nil {
		hv = h
	}
	b.marks = append(b.marks, resume.Mark{Index: i, Handle: hv, ObservedBasis: store.BasisObserved})
}

func (b *batch) note(reason string) {
	if b.notes == nil {
		b.notes = make(map[string]int)
	}
	b.notes[reason]++
}

func (b *batch) skip(reason string) {
	if b.skips == nil {
		b.skips = make(map[string]int)
	}
	b.skips[reason]++
}

func (b *batch) result() Result {
	r := Result{Records: b.recs, Pending: b.pend, Backdated: b.marks}
	for _, reason := range slices.Sorted(maps.Keys(b.skips)) {
		r.Skipped = append(r.Skipped, Skip{Reason: reason, Count: b.skips[reason]})
	}
	for _, reason := range slices.Sorted(maps.Keys(b.notes)) {
		r.Notes = append(r.Notes, Skip{Reason: reason, Count: b.notes[reason]})
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
	b.observed = observed

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
		if err = t.pod(&b, watchType, o, namespaceOf(res), observed); err == nil && watchType == "" {
			t.pullSeen(&b, kindPod, observed)
		}
	case "Node":
		if err = t.node(&b, watchType, o, observed); err == nil && watchType == "" {
			t.pullSeen(&b, kindNode, observed)
		}
	default:
		b.skip(SkipUnsupportedKind)
	}
	if err != nil {
		return Result{}, err
	}
	return b.result(), nil
}

// pullSeen takes a pull record of a kind. A change in the observed time between
// the pull records of a kind starts a new generation; one that begins more than one
// and a half pull intervals after the previous one is a gap, counted once. A record
// observed before the newest generation is not a new one and changes nothing.
func (t *Translator) pullSeen(b *batch, kind int, observed time.Time) {
	g := &t.pulls[kind]
	switch {
	case !g.seen:
		g.seen, g.observed = true, observed
	case observed.After(g.observed):
		if d := observed.Sub(g.observed); t.interval > 0 && d > t.interval+t.interval/2 {
			b.note(NotePullGap)
		}
		g.observed = observed
	}
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

func (t *Translator) emit(b *batch, subj store.Subject, kind lifecycle.Kind, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte, observed time.Time) {
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
		if at.Before(observed) {
			// The object is known to have existed continuously from its event time,
			// and was observed at the given time.
			r.Through = observed
		}
	}
	b.recs = append(b.recs, r)
}

func (t *Translator) observeEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte, observed time.Time) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Observe, at, basis, ttl, payload, observed)
}

func (t *Translator) deleteEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Delete, at, basis, 0, nil, b.observed)
}

func (t *Translator) observeEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis, ttl time.Duration, payload []byte, observed time.Time) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Observe, at, basis, ttl, payload, observed)
}

func (t *Translator) deleteEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Delete, at, basis, 0, nil, b.observed)
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
	if observed.After(st.observed) {
		st.observed = observed
	}

	n := len(b.recs)
	if changed || t.ttl > 0 {
		t.observeEntity(b, fp, at, basis, t.ttl, v.payload, b.observed)
		if first {
			b.mark(&handle{pod: st})
		}
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
// stay, each followed by its part_of edge, so that they lapse when the pod's records
// stop. The edge is not re-asserted before its own latest assertion.
func (t *Translator) reconcileContainers(b *batch, st *podState, v podView, at time.Time, basis store.EventTimeBasis) {
	want := t.containersOf(b, v, true)
	for _, name := range slices.Sorted(maps.Keys(st.ctrs)) {
		old := st.ctrs[name]
		if w, ok := want[name]; ok && w.id == old.id {
			continue
		}
		t.deleteContainer(b, st, old, at, basis)
		delete(st.ctrs, name)
	}
	for _, name := range slices.Sorted(maps.Keys(want)) {
		w := want[name]
		payload := canonical(object{"name": name, "pod_uid": v.UID})
		if old, ok := st.ctrs[name]; ok {
			if t.ttl > 0 {
				old.last, old.lastBasis = b.raise(at, basis, old.last, old.lastBasis)
				t.observeEntity(b, w.fp, old.last, old.lastBasis, t.ttl, payload, b.observed)
				old.since, old.sinceBasis = b.raise(at, basis, old.since, old.sinceBasis)
				t.observeEdge(b, w.fp, st.fp, catalog.PartOf, old.since, old.sinceBasis, t.ttl, []byte("{}"), b.observed)
				st.ctrs[name] = old
			}
			continue
		}
		t.observeEntity(b, w.fp, at, basis, t.ttl, payload, b.observed)
		b.mark(&handle{pod: st, ctr: name, peer: w.fp, entity: true})
		t.observeEdge(b, w.fp, st.fp, catalog.PartOf, at, basis, t.ttl, []byte("{}"), b.observed)
		b.mark(&handle{pod: st, ctr: name, peer: w.fp})
		w.last, w.lastBasis = at, basis
		w.since, w.sinceBasis = at, basis
		st.ctrs[name] = w
	}
}

// deleteContainer ends a container of the pod and its part_of edge at the time
// given, or at the latest time either was asserted at if that is later.
func (t *Translator) deleteContainer(b *batch, st *podState, c ctrState, at time.Time, basis store.EventTimeBasis) {
	eAt, eBasis := b.raise(at, basis, c.since, c.sinceBasis)
	t.deleteEdge(b, c.fp, st.fp, catalog.PartOf, eAt, eBasis)
	cAt, cBasis := b.raise(at, basis, c.last, c.lastBasis)
	t.deleteEntity(b, c.fp, cAt, cBasis)
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
//
// With a TTL, an edge established by an earlier record is re-asserted with each
// record of the pod, with the payload it was established with, at the record's
// time or at the edge's own latest assertion if that is later (the edge begins at
// the PodScheduled time, which a clock ahead may place after the next record's).
// An edge begun by this record is not asserted twice.
func (t *Translator) reconcileEdge(b *batch, st *podState, v podView, c clock, at time.Time, basis store.EventTimeBasis) {
	if v.NodeName == st.edge.nodeName {
		switch {
		case v.NodeName == "":
		case st.edge.node.IsZero():
			if sp, ok := t.lookup(b, v.NodeName, at); ok {
				t.beginEdge(b, st, sp, v.NodeName, at, basis, b.observed)
				delete(t.pending, v.UID)
			}
		case t.ttl > 0:
			e := &st.edge
			e.since, e.sinceBasis = b.raise(at, basis, e.since, e.sinceBasis)
			t.observeEdge(b, st.fp, e.node, catalog.ScheduledOn, e.since, e.sinceBasis, t.ttl, e.payload, b.observed)
		}
		return
	}
	t.dropEdge(b, st, at, basis, SkipPendingDroppedRebound)
	st.edge = podEdge{nodeName: v.NodeName}
	if v.NodeName == "" {
		return
	}
	from, fromBasis := b.edgeStart(v, c, at, basis)
	if sp, ok := t.lookup(b, v.NodeName, from); ok {
		t.beginEdge(b, st, sp, v.NodeName, from, fromBasis, b.observed)
		return
	}
	p := PendingEdge{Pod: st.fp, PodUID: v.UID, NodeName: v.NodeName, Time: from, Basis: fromBasis}
	t.pending[v.UID] = p
	b.pend = append(b.pend, p)
}

// beginEdge asserts the scheduled_on edge of the pod to the node of sp, and keeps
// what a re-assertion needs. An edge timed before observed is asserted through it.
func (t *Translator) beginEdge(b *batch, st *podState, sp *nodeSpan, name string, at time.Time, basis store.EventTimeBasis, observed time.Time) {
	payload := edgePayload(name, sp)
	t.observeEdge(b, st.fp, sp.fp, catalog.ScheduledOn, at, basis, t.ttl, payload, observed)
	b.mark(&handle{pod: st, peer: sp.fp})
	st.edge.node, st.edge.payload = sp.fp, payload
	st.edge.since, st.edge.sinceBasis = at, basis
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
// containers and its edge. A Delete carries no TTL and no Through.
//
// A DELETED for a pod the translator has not seen (it was seen before a restart,
// and the state is in memory) is addressed from the deleted object itself: its
// containers that have a runtime id, and its edge if the node name resolves at the
// time the pod was scheduled, or at the observed time only when the object has no
// scheduling time. A Delete of a reference this producer never held changes nothing.
func (t *Translator) deletePod(b *batch, st *podState, fp identity.Fingerprint, v podView, observed time.Time) {
	t.deleted[v.UID] = struct{}{}
	if st == nil {
		ctrs := t.containersOf(b, v, false)
		for _, name := range slices.Sorted(maps.Keys(ctrs)) {
			t.deleteEdge(b, ctrs[name].fp, fp, catalog.PartOf, observed, store.BasisObserved)
			t.deleteEntity(b, ctrs[name].fp, observed, store.BasisObserved)
		}
		if v.NodeName != "" {
			// The node is the one that bore the name when the pod was scheduled. A
			// pod failed by the garbage collector outlives its node, and the name may
			// by now belong to a new one, so the observed time is used only when the
			// object has no scheduling time to go by.
			c := clock{observed: observed}
			if tm, ok := parseTime(v.Creation); ok && c.plausible(tm) {
				c.creation = tm
			}
			when, basis := b.edgeStart(v, c, observed, store.BasisObserved)
			if basis != store.BasisObjectField {
				when = observed
			}
			if sp, ok := t.lookup(b, v.NodeName, when); ok {
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
		t.deleteContainer(b, st, st.ctrs[name], at, basis)
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
	asserted := at // the latest instant this Observe asserts the node
	if changed || t.ttl > 0 {
		t.observeEntity(b, fp, at, basis, t.ttl, v.payload, b.observed)
		if first {
			b.mark(&handle{node: st})
		}
		emitted = true
		if at.Before(observed) {
			asserted = observed
		}
	}
	st.view = v
	if emitted && at.After(st.last) {
		st.last, st.lastBasis = at, basis
	}
	if emitted && st.span != nil && asserted.After(st.span.last) {
		st.span.last = asserted
	}
	if first && v.Name != "" {
		st.span = &nodeSpan{fp: fp, from: at, last: asserted}
		t.byName[v.Name] = append(t.byName[v.Name], st.span)
		t.resolvePending(b, v.Name)
	}
	return nil
}

// lookup resolves a node name at a time: the one node interval that covers it. No
// interval resolves nothing, and neither do two or more, which is counted with
// [SkipAmbiguousNodeName]: the name is never guessed.
func (t *Translator) lookup(b *batch, name string, at time.Time) (*nodeSpan, bool) {
	var hit *nodeSpan
	covering := 0
	for _, sp := range t.byName[name] {
		if at.Before(sp.from) {
			continue
		}
		if end := sp.end(t.ttl); !end.IsZero() && !at.Before(end) {
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

// resolvePending emits the edges of the pending pods that the node name now
// resolves, at each pod record's own event time. An edge resolved when its node
// arrives is asserted through the pod's latest observation, not the node's: the
// node's arrival says nothing about whether the pod still exists. It has no
// Through if that observation is not after the edge's time.
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
		t.beginEdge(b, pod, sp, name, p.Time, p.Basis, pod.observed)
		delete(t.pending, p.PodUID)
	}
}
