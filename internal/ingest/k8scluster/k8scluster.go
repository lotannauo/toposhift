// Package k8scluster translates the entity events of the OpenTelemetry
// Kubernetes cluster receiver into store records.
//
// # Input
//
// The receiver, with its entity events written as log records, emits one log
// record per entity state or deletion. The record's eventName is entity.state or
// entity.delete, its body is empty, and its time is timeUnixNano; the observed
// time is not set. Its attributes are:
//
//   - entity.type, a string such as k8s.pod;
//   - entity.id, a key-value list with the type's identifying key, such as
//     k8s.pod.uid;
//   - on a state event only, entity.description, a flat key-value list of
//     strings, and entity.report.interval, the interval in seconds at which the
//     receiver repeats the state of every entity.
//
// A delete event carries only the type and the id. The older form of the
// events, with attributes named otel.entity.*, is not translated; it is
// counted with [SkipLegacyEntityEvent]. A record whose eventName is not
// entity.state or entity.delete, such as a record of the Kubernetes objects
// receiver in a shared pipeline, is counted with [SkipNotEntityEvent].
//
// # Entities and edges
//
// A node ([catalog.K8sNode], keyed by k8s.node.uid), a pod ([catalog.K8sPod],
// keyed by k8s.pod.uid) and a container ([catalog.Container], keyed by its
// runtime id) are entities. Names never identify anything. The other types the
// receiver reports (namespace, deployment, replicaset, statefulset, service)
// are not in the catalog and give no record; each is counted with
// [SkipTypeNotInCatalog], and any other type with [SkipUnknownEntityType].
//
//   - A node state is an Observe of the node. A node delete is a Delete of it,
//     and ends the interval in which the node bore its name.
//   - A pod state is an Observe of the pod and, once the node it names
//     (k8s.node.name) resolves to a node UID of this producer, a scheduled_on
//     edge from the pod to that node. A scheduled_on edge begins at the
//     instant of the first record that names its node. A pod's creation is not its
//     binding, and the description carries no binding time, so the time before is
//     left unattributed. The Kubernetes objects translator, which reads the
//     PodScheduled time, covers it. A name that does not resolve is pending: it
//     is retried at each later record of the pod and when a node arrives. A pod
//     delete is a Delete of the pod and of its scheduled_on edge, and also of
//     each container this translator knows for the pod and of their part_of
//     edges. A pod this translator has not seen is deleted too, and counted with
//     [SkipUnseenDelete], because its node is not known.
//   - A container state with a runtime id is an Observe of the container and a
//     part_of edge to the pod named by the description's k8s.pod.uid, which is
//     taken as given and not resolved. A container delete is a Delete of the
//     container and of its part_of edge. A runtime id may carry a "<runtime>://"
//     prefix, which is stripped, so that the id is the one the Kubernetes objects
//     translator uses. A container with an empty id is refused and counted with
//     [SkipContainerNoID]: a simulated cluster reports every container so. A
//     container with no pod uid is an entity without an edge, counted with
//     [SkipContainerNoPod].
//
// An edge takes the TTL of the entity it is stored on and is re-asserted with
// each record of that entity, so that it lapses with the entity when a Delete is
// lost. A Delete carries no TTL.
//
// A record after the Delete of its UID is set aside and counted with
// [SkipAfterDelete]: a Kubernetes UID is never reused.
//
// # Description
//
// The payload of a state is the canonical JSON (sorted keys) of an allow-list of
// the description's keys, each with its string value, per type. Every other key,
// above all the labels (private data, with no bound on number or size) and the
// owner uids, is dropped. The first time this translator drops a key name it
// counts it with [NoteDescriptionKeyDropped] in [Result.Notes], so that a
// steady stream of the same labels adds nothing after the first. A key that is
// read but not kept, such as a container's k8s.pod.uid, is not dropped. A state
// whose payload did not change gives no record when the TTL is zero, because a
// reference with no TTL needs no refresh.
//
// # Times
//
// Every time this package writes comes from the record or from the caller. It
// never reads a clock.
//
// The instant of a record is the time the record is taken to describe. By
// default the producer's clock is trusted: the instant is timeUnixNano, with
// basis producer_event. The caller may also pass the time it received the
// record, which is optional (zero means not given):
//
//   - a timeUnixNano later than received plus [MaxClockAhead] is implausible: the
//     instant is received, with basis receipt, counted with [SkipImplausibleTime];
//   - a timeUnixNano earlier than received minus the TTL (when there is a TTL) is
//     kept, and counted with [NoteClockBehind]: late records are the sequencer's
//     business;
//   - a zero timeUnixNano takes received, and without it the record is refused
//     with [ErrNoTime].
//
// With [Config.DistrustProducerClock] the instant is always received, with basis
// receipt, and without it the record is refused with [ErrNoTime]. A refused
// record leaves the translator unchanged.
//
// The first record of a UID takes the creation timestamp of its description
// (basis object field) if it lies between [store.MinEventTime] and the instant
// plus [MaxClockAhead], and otherwise the instant; a creation timestamp that is
// there and unusable is counted with [SkipImplausibleTime]. Such a first record,
// timed before its instant and so carrying a Through, is marked in
// [Result.Backdated] (with the part_of edge of a first container), as is a
// scheduled_on edge that waited for its node and is asserted through its pod's
// latest instant: ingest resumes each from the producer's last stored assertion
// of the subject (see "State"). Every other record
// takes the instant, because a description carries no time of what changed. An
// event time is never earlier than the previous record's of the same UID: an
// earlier one is raised to it, with its basis, and counted with
// [SkipClampedTime].
//
// # Through
//
// An Observe, of an entity or an edge, whose event time is before the instant
// of the record carries Through equal to that instant. A Kubernetes UID exists
// continuously from its creation to its deletion, so a producer that sees an
// object at one instant and learns it was created earlier can assert it from the
// creation through that instant. The event time, basis and TTL are unchanged.
// An Observe at or after the instant (a clock ahead by up to [MaxClockAhead])
// has no Through, and a Delete never has one. A scheduled_on edge that waited for
// its node is asserted through the latest instant of its pod's records, not the
// node's: the arrival of a node says nothing about the pod. A re-assertion of an
// edge is never timed before the edge's latest assertion.
//
// # TTL
//
// The TTL of an Observe is [DefaultTTLMultiple] times the entity.report.interval
// of the event, or of [Config.CollectionInterval] if the event has none. An
// interval of zero means the receiver sends no periodic state, and gives TTL
// zero. The value must be an integer number of seconds (the capture package reads
// the decimal string that is an int64 on the wire): a value of another kind, a
// negative one and one over a year give no record and are counted with
// [SkipBadInterval].
//
// A translator has one TTL at a time, so that every Observe of a producer
// carries the same one. Every node, pod or container state event whose interval
// has been read (one set aside earlier, for a missing or empty id, an unusable
// interval or no usable time, has not) switches it from that record on if its
// interval differs, and is counted once with [NoteReportIntervalChanged]; this
// holds for a state set aside with [SkipAfterDelete] and for an unchanged state
// at TTL zero too. The first interval seen is not a change. A namespace state
// does not switch it, and neither does a delete event.
//
// # Nodes by name
//
// A pod names its node, not its UID. The translator keeps, for each node name, the
// intervals in which node UIDs bore it, built from the nodes it has seen, and
// resolves the name at the instant of the pod record. The index is of one
// translator, which is one producer: a name is never resolved against another
// producer's nodes. An interval ends at the delete of its node, or, if the delete
// was lost, a TTL after the last instant the node was asserted, derived when the
// name is looked up. If exactly one interval covers the time the edge is
// written; if none covers it the edge is pending ([Result.Pending]); if more than
// one does, the name resolves to none, is counted with [SkipAmbiguousNodeName],
// and the edge stays pending.
//
// # The kube-system namespace
//
// A later detector of cluster identity needs the UID of each cluster's
// kube-system namespace and how long it is vouched for. This package keeps no
// detector and refuses nothing for it: [Result.Namespaces] holds a
// [NamespaceSighting] for each state event of a namespace named kube-system and
// for each delete of a UID seen as such. A sighting carries the interval of its
// event, or [Config.CollectionInterval] if the event has none, as for a TTL, so
// that it vouches for its UID for three intervals and no longer. The producer,
// which the caller knows, names the cluster.
//
// # Producers
//
// Both this package and the Kubernetes objects translator write the scheduled_on
// edge of a pod, each from its own reference. A Delete of the objects translator
// leaves the edge of this one alive for up to one TTL if this producer's delete
// was lost. While a node is re-created the two producers may briefly resolve
// different node UIDs, so a second, short-lived edge is possible in the union,
// bounded by the TTL. Where both describe an entity, the store's policy ranks
// the objects translator's payload above this one's; this package writes
// ordinary records and knows nothing of the ranking.
//
// # State
//
// The state is in memory and is lost with the process: after a restart every UID
// is new to the translator, and the first record of each is timed at its creation
// again, which would be late on every prefix it touches. Those records are marked,
// and ingest resumes them from the store (see package resume): an unchanged
// description is raised to the producer's last stored assertion of the subject, a
// changed one takes the record's instant. [Translator.Resumed] then raises, and
// never lowers, the state that times the subject's later records. [Comparable] is
// what a description is compared on: the allow-listed keys of the payload, listed
// under "Description" above (a node: k8s.node.name, node.creation_timestamp and
// the five k8s.node.condition_* keys; a pod: k8s.pod.name, k8s.namespace.name,
// k8s.node.name, k8s.pod.phase, k8s.pod.status_reason, the workload, deployment,
// replicaset and statefulset names and pod.creation_timestamp; a container:
// k8s.container.name, k8s.pod.name, k8s.namespace.name, k8s.node.name, the image
// name and tag, container.status, container.status.reason and
// container.creation_timestamp), which the receiver reports alike for an
// unchanged object; and of a scheduled_on edge only its node_name, not its
// resolution, which this translator derives from its own node index. A pending
// edge is lost with the state, and is asserted again when its pod is next seen
// once its node is known. The translator remembers every UID it has seen
// deleted, so that a stale state cannot bring one back; a long-lived process
// holds those UIDs, and the names of at most 4096 dropped description keys.
//
// A Translator is not safe for concurrent use.
package k8scluster

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

// DefaultTTLMultiple is the TTL of an Observe, in report intervals.
const DefaultTTLMultiple = 3

// MaxClockAhead is how far past the time a record was received a time of the
// producer may lie before the translator distrusts it. The collector and this
// process keep separate clocks.
const MaxClockAhead = 5 * time.Minute

// KubeSystem is the name of the namespace whose UID tells clusters apart.
const KubeSystem = "kube-system"

// maxInterval is the longest report interval accepted. It keeps a TTL of
// [DefaultTTLMultiple] intervals far from the end of the representable times.
const maxInterval = 366 * 24 * time.Hour

// maxNotedKeys bounds the set of description key names already counted as
// dropped, which a producer sending ever new label names would otherwise grow
// for ever.
const maxNotedKeys = 4096

// The reasons a translator gives for a record or part of a record it does not
// turn into a store record.
const (
	// SkipNotEntityEvent is a record whose eventName is not entity.state or
	// entity.delete, such as a record of the Kubernetes objects receiver.
	SkipNotEntityEvent = "not an entity event"
	// SkipLegacyEntityEvent is an entity event in the older attribute form
	// (otel.entity.*), which is not translated.
	SkipLegacyEntityEvent = "entity event in the legacy form"
	// SkipUnknownEntityType is an entity event of a type this package does not
	// know, or with no type.
	SkipUnknownEntityType = "unknown entity type"
	// SkipTypeNotInCatalog is an entity event of a type the receiver reports and
	// the catalog has no entity for (namespace, deployment, replicaset,
	// statefulset, service).
	SkipTypeNotInCatalog = "entity type not in the catalog"
	// SkipMissingID is an event with no value for its type's identifying key, or a
	// blank one.
	SkipMissingID = "missing entity id"
	// SkipContainerNoID is a container event whose runtime id is empty.
	SkipContainerNoID = "container without runtime id"
	// SkipContainerNoPod is a container state with no k8s.pod.uid, which gets no
	// part_of edge.
	SkipContainerNoPod = "container without pod uid"
	// SkipBadInterval is a state event whose entity.report.interval is not a
	// whole number of seconds that can be used.
	SkipBadInterval = "unusable report interval"
	// SkipImplausibleTime is a time of the record that was not used, because it is
	// unreadable or lies outside the plausible range.
	SkipImplausibleTime = "implausible time"
	// SkipClampedTime is an event time raised to the previous record's of the
	// same subject, because an older time would make the fold show the older
	// description after the newer one.
	SkipClampedTime = "event time raised to the previous record's"
	// SkipAfterDelete is a record about a UID that a delete already ended.
	SkipAfterDelete = "record after delete"
	// SkipUnseenDelete is the delete of an entity this translator has not seen,
	// whose edge it cannot address.
	SkipUnseenDelete = "delete of an unseen entity: its edge is not addressed"
	// SkipAmbiguousNodeName is a lookup of a node name that more than one node UID
	// bears at the time asked. It resolves to none.
	SkipAmbiguousNodeName = "node name borne by more than one node"
	// SkipPendingDroppedDeleted is a pending scheduled_on edge dropped because its
	// pod was deleted before the node was known.
	SkipPendingDroppedDeleted = "pending edge dropped: pod deleted"
	// SkipPendingDroppedRebound is a pending scheduled_on edge dropped because the
	// pod now names another node.
	SkipPendingDroppedRebound = "pending edge dropped: node name changed"
)

// The reasons a translator gives for a note: something the caller should know
// that is not a record set aside.
const (
	// NoteDescriptionKeyDropped is a description key name dropped for the first
	// time; the note's detail is the name.
	NoteDescriptionKeyDropped = "description key dropped"
	// NoteReportIntervalChanged is a state event whose report interval differs
	// from the translator's current one; the detail names both.
	NoteReportIntervalChanged = "report interval changed"
	// NoteClockBehind is a record whose time is earlier than the time it was
	// received by more than the TTL. Its time is kept.
	NoteClockBehind = "producer time behind receipt by more than the TTL"
	// NoteDroppedKeysFull is the first description key name dropped after the
	// translator stopped remembering names; from then on new names are not noted.
	NoteDroppedKeysFull = "dropped key names no longer tracked"
)

// ErrNoTime is returned for a record with no time that can be used: with the
// producer's clock distrusted and no receipt time, or with neither a usable
// timeUnixNano nor a receipt time.
var ErrNoTime = errors.New("k8scluster: record has no usable time")

// Config configures a [Translator].
type Config struct {
	// Producer names the source of the assertions: "k8scluster/<cluster>". The
	// caller builds it; this package does not check its format. Required.
	Producer lifecycle.Producer
	// Resolver turns the identifying attributes into fingerprints. Required;
	// usually identity.NewResolver(catalog.Default()).
	Resolver *identity.Resolver
	// CollectionInterval is the report interval of the receiver as configured,
	// used for an event that carries none. Zero is no interval: the TTL of such
	// an event is zero. It must not be negative or over a year.
	CollectionInterval time.Duration
	// DistrustProducerClock says that the time in a record is not to be used: the
	// instant of every record is the time it was received, which the caller then
	// has to give. The zero value trusts the producer.
	DistrustProducerClock bool
}

// Result is what one log record stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced. They
	// have no Seq and no Boot, and no Through except for an Observe timed before
	// the record's instant, whose Through is that instant (or, for an edge that
	// waited for its node, the latest instant of its pod).
	Records []store.Record
	// Pending are the scheduled_on edges this call could not resolve because the
	// node name is not yet known to the index. The translator keeps them and
	// writes the edge in a later Result once a node covers them.
	Pending []PendingEdge
	// Skipped says what was set aside and why, sorted by reason.
	Skipped []Skip
	// Notes say what the caller should know and log, sorted by reason and detail.
	// They do not change the records.
	Notes []Note
	// Namespaces are the sightings of the kube-system namespace in this record.
	Namespaces []NamespaceSighting
	// Backdated marks the records this call backdated, which [resume.Apply] times
	// from the store: the first record of a node, a pod or a container and of a
	// container's part_of edge, and every scheduled_on edge begun at a time before
	// the instant it is asserted through (one that waited for its node). Only an
	// Observe that carries a Through is marked. The handles are for
	// [Translator.Resumed].
	Backdated []resume.Mark
}

// Skip is a count of things set aside for one reason.
type Skip struct {
	Reason string
	Count  int
}

// Note is a count of one thing the caller should know.
type Note struct {
	Reason string
	// Detail says which, for a reason that has more than one: the key name of a
	// dropped key, or the two intervals of a change.
	Detail string
	Count  int
}

// PendingEdge is a scheduled_on edge whose node is not yet known.
type PendingEdge struct {
	// Pod is the pod, and PodUID its UID.
	Pod    identity.Fingerprint
	PodUID string
	// NodeName is the node name the pod reports.
	NodeName string
	// Time and Basis are the event time and basis the edge will have: those of
	// the pod record that named the node.
	Time  time.Time
	Basis store.EventTimeBasis
}

// Translator turns log records into store records. It keeps the last description
// of every UID it has seen, the index from node name to the node UIDs that bore
// it, the scheduled_on edges still waiting for a node, the containers of each
// pod and the UIDs seen as kube-system.
type Translator struct {
	producer   lifecycle.Producer
	resolver   *identity.Resolver
	collection time.Duration
	distrust   bool

	// interval is the report interval of the Observes written last, once there
	// is one.
	interval    time.Duration
	intervalSet bool

	nodes   map[string]*nodeState
	pods    map[string]*podState
	ctrs    map[string]*ctrState
	podCtrs map[string]map[string]struct{}
	deleted map[string]struct{}
	byName  map[string][]*nodeSpan
	pending map[string]PendingEdge

	kubeSystem map[string]struct{}
	noted      map[string]struct{}
	notedFull  bool
}

type nodeState struct {
	fp      identity.Fingerprint
	payload []byte
	// last and lastBasis are the event time and basis of the newest record
	// written for the UID.
	last      time.Time
	lastBasis store.EventTimeBasis
	span      *nodeSpan
}

type podState struct {
	fp        identity.Fingerprint
	payload   []byte
	last      time.Time
	lastBasis store.EventTimeBasis
	// seen is the latest instant of the pod's records.
	seen time.Time
	edge podEdge
}

// podEdge is the scheduled_on edge of a pod. With a name and a zero node it is
// pending; with both it is established.
type podEdge struct {
	nodeName string
	node     identity.Fingerprint
	// payload is what the edge was established with, which a re-assertion
	// repeats. since and sinceBasis are the time and basis of the edge's latest
	// Observe, which a re-assertion is never timed before.
	payload    []byte
	since      time.Time
	sinceBasis store.EventTimeBasis
}

type ctrState struct {
	fp        identity.Fingerprint
	payload   []byte
	last      time.Time
	lastBasis store.EventTimeBasis
	// podUID and podFP are the pod of the part_of edge, if there is one;
	// since and sinceBasis are the time and basis of the edge's latest Observe.
	podUID     string
	podFP      identity.Fingerprint
	since      time.Time
	sinceBasis store.EventTimeBasis
}

// New returns a translator with no state.
func New(c Config) (*Translator, error) {
	switch {
	case c.Producer == "":
		return nil, errors.New("k8scluster: producer is required")
	case c.Resolver == nil:
		return nil, errors.New("k8scluster: resolver is required")
	case c.CollectionInterval < 0:
		return nil, errors.New("k8scluster: collection interval is negative")
	case c.CollectionInterval > maxInterval:
		return nil, fmt.Errorf("k8scluster: collection interval %s is over %s", c.CollectionInterval, maxInterval)
	}
	return &Translator{
		producer:   c.Producer,
		resolver:   c.Resolver,
		collection: c.CollectionInterval,
		distrust:   c.DistrustProducerClock,
		nodes:      make(map[string]*nodeState),
		pods:       make(map[string]*podState),
		ctrs:       make(map[string]*ctrState),
		podCtrs:    make(map[string]map[string]struct{}),
		deleted:    make(map[string]struct{}),
		byName:     make(map[string][]*nodeSpan),
		pending:    make(map[string]PendingEdge),
		kubeSystem: make(map[string]struct{}),
		noted:      make(map[string]struct{}),
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

// ttl is the TTL of the Observes written now.
func (t *Translator) ttl() time.Duration {
	if t.intervalSet {
		return DefaultTTLMultiple * t.interval
	}
	return DefaultTTLMultiple * t.collection
}

// batch collects what one call produces.
type batch struct {
	recs  []store.Record
	pend  []PendingEdge
	skips map[string]int
	notes map[[2]string]int
	sight []NamespaceSighting
	marks []resume.Mark
	// instant and basis are the instant of the record being translated.
	instant time.Time
	basis   store.EventTimeBasis
}

// mark marks the record just written as backdated, with the handle h, if it is an
// Observe with a Through. Its Through is the record's instant (or its pod's latest,
// on the same clock), so it takes the instant's basis when re-timed to it.
func (b *batch) mark(h *handle) {
	i := len(b.recs) - 1
	if i < 0 || b.recs[i].Kind != lifecycle.Observe || !b.recs[i].Through.After(b.recs[i].EventTime) {
		return
	}
	b.marks = append(b.marks, resume.Mark{Index: i, Handle: h, ObservedBasis: b.basis})
}

func (b *batch) skip(reason string) {
	if b.skips == nil {
		b.skips = make(map[string]int)
	}
	b.skips[reason]++
}

func (b *batch) note(reason, detail string) {
	if b.notes == nil {
		b.notes = make(map[[2]string]int)
	}
	b.notes[[2]string{reason, detail}]++
}

func (b *batch) result() Result {
	r := Result{Records: b.recs, Pending: b.pend, Namespaces: b.sight, Backdated: b.marks}
	for _, reason := range slices.Sorted(maps.Keys(b.skips)) {
		r.Skipped = append(r.Skipped, Skip{Reason: reason, Count: b.skips[reason]})
	}
	keys := slices.SortedFunc(maps.Keys(b.notes), func(x, y [2]string) int {
		if c := strings.Compare(x[0], y[0]); c != 0 {
			return c
		}
		return strings.Compare(x[1], y[1])
	})
	for _, k := range keys {
		r.Notes = append(r.Notes, Note{Reason: k[0], Detail: k[1], Count: b.notes[k]})
	}
	return r
}

// Translate turns one OTLP log record into records without Seq. It is
// deterministic given the sequence of earlier inputs. received is the time the
// caller received the record; it may be zero, and see the package documentation
// for how it is used.
//
// An error means the record could not be read at all (it has no usable time, or
// its identifier is not a valid key), and leaves the translator unchanged. A
// record that is valid but not translated is not an error: it is in
// [Result.Skipped].
func (t *Translator) Translate(_ capture.Resource, lr capture.LogRecord, received time.Time) (Result, error) {
	var b batch
	ev, reason := decode(lr)
	if reason != "" {
		b.skip(reason)
		return b.result(), nil
	}
	switch ev.typ {
	case typeNode, typePod, typeContainer:
	case typeNamespace:
		return t.namespace(&b, ev, lr, received)
	case typeDeployment, typeReplicaSet, typeStatefulSet, typeService:
		b.skip(SkipTypeNotInCatalog)
		return b.result(), nil
	default:
		b.skip(SkipUnknownEntityType)
		return b.result(), nil
	}

	id := ev.id
	if ev.typ == typeContainer {
		id = runtimeID(id)
	}
	switch {
	case !ev.hasID:
		b.skip(SkipMissingID)
		return b.result(), nil
	case strings.TrimSpace(id) == "":
		if ev.typ == typeContainer {
			b.skip(SkipContainerNoID)
		} else {
			b.skip(SkipMissingID)
		}
		return b.result(), nil
	}
	if ev.state && ev.iv.present && !ev.iv.valid {
		b.skip(SkipBadInterval)
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
	if b.instant, b.basis, err = t.instant(&b, lr, received, ttl); err != nil {
		return Result{}, err
	}

	// Everything that can fail is done before anything is kept.
	var fp, podFP identity.Fingerprint
	var podUID string
	switch ev.typ {
	case typeNode:
		fp, err = t.fingerprint(catalog.K8sNode, catalog.K8sNodeUID, id)
	case typePod:
		fp, err = t.fingerprint(catalog.K8sPod, catalog.K8sPodUID, id)
	case typeContainer:
		fp, err = t.fingerprint(catalog.Container, catalog.ContainerID, id)
		if err == nil && ev.state {
			if podUID = stringOf(ev.desc, keyPodUID); strings.TrimSpace(podUID) == "" {
				podUID = ""
			} else {
				podFP, err = t.fingerprint(catalog.K8sPod, catalog.K8sPodUID, podUID)
			}
		}
	}
	if err != nil {
		return Result{}, err
	}

	if ev.state {
		t.setInterval(&b, eff)
	}
	switch ev.typ {
	case typeNode:
		t.node(&b, ev, id, fp)
	case typePod:
		t.pod(&b, ev, id, fp)
	case typeContainer:
		t.container(&b, ev, id, fp, podUID, podFP)
	}
	return b.result(), nil
}

// setInterval makes the interval of a state event the current one. The first
// one the translator sees is not a change.
func (t *Translator) setInterval(b *batch, eff time.Duration) {
	switch {
	case !t.intervalSet:
		t.interval, t.intervalSet = eff, true
	case eff != t.interval:
		b.note(NoteReportIntervalChanged, t.interval.String()+" to "+eff.String())
		t.interval = eff
	}
}

// instant returns the time a record describes and where it came from. It counts
// what it sets aside or notes in b and changes nothing else.
func (t *Translator) instant(b *batch, lr capture.LogRecord, received time.Time, ttl time.Duration) (time.Time, store.EventTimeBasis, error) {
	if !received.IsZero() {
		received = received.UTC()
		if received.Before(store.MinEventTime) || received.After(store.MaxEventTime) {
			received = time.Time{}
		}
	}
	if t.distrust {
		if received.IsZero() {
			return time.Time{}, 0, ErrNoTime
		}
		return received, store.BasisReceipt, nil
	}
	n := uint64(lr.TimeUnixNano)
	switch {
	case n == 0:
		if received.IsZero() {
			return time.Time{}, 0, ErrNoTime
		}
		return received, store.BasisReceipt, nil
	case n > math.MaxInt64:
		if received.IsZero() {
			return time.Time{}, 0, ErrNoTime
		}
		b.skip(SkipImplausibleTime)
		return received, store.BasisReceipt, nil
	}
	tm := time.Unix(0, int64(n)).UTC()
	if !received.IsZero() {
		if tm.After(received.Add(MaxClockAhead)) {
			b.skip(SkipImplausibleTime)
			return received, store.BasisReceipt, nil
		}
		if ttl > 0 && tm.Before(received.Add(-ttl)) {
			b.note(NoteClockBehind, "")
		}
	}
	return tm, store.BasisProducerEvent, nil
}

func (t *Translator) fingerprint(typ catalog.EntityType, key catalog.AttributeKey, v string) (identity.Fingerprint, error) {
	id, err := t.resolver.Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("k8scluster: %s %q: %w", typ, v, err)
	}
	return id.Fingerprint(), nil
}

func layerOf(fp identity.Fingerprint) catalog.Layer {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		// The resolver and the default catalog list the same types.
		panic("k8scluster: entity type " + string(fp.Type()) + " is not in the default catalog")
	}
	return e.Layer()
}

// recordLayer is the layer a record about the subject is stored in: an entity
// takes its type's layer, and an edge takes the layer its relation is stored
// in, which the catalog records.
func recordLayer(subj store.Subject) catalog.Layer {
	if subj.Kind != store.SubjectEdge {
		return layerOf(subj.A)
	}
	r, ok := catalog.Default().Relation(subj.Relation)
	if !ok || r.Derived() {
		// A translator emits only stored relations the default catalog lists.
		panic("k8scluster: relation " + string(subj.Relation) + " is not a stored relation of the default catalog")
	}
	return r.Layer()
}

// emit writes a record. An Observe takes the current TTL, and carries the given
// instant as its Through if its event time is before it. A Delete carries
// neither.
func (t *Translator) emit(b *batch, subj store.Subject, kind lifecycle.Kind, at time.Time, basis store.EventTimeBasis, payload []byte, through time.Time) {
	r := store.Record{
		Layer:          recordLayer(subj),
		Subject:        subj,
		Producer:       t.producer,
		EventTime:      at,
		Kind:           kind,
		EventTimeBasis: basis,
	}
	if kind == lifecycle.Observe {
		r.TTL = t.ttl()
		r.Payload = bytes.Clone(payload)
		if at.Before(through) {
			r.Through = through
		}
	}
	b.recs = append(b.recs, r)
}

func (t *Translator) observeEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis, payload []byte, through time.Time) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Observe, at, basis, payload, through)
}

func (t *Translator) deleteEntity(b *batch, fp identity.Fingerprint, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EntitySubject(fp), lifecycle.Delete, at, basis, nil, time.Time{})
}

func (t *Translator) observeEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis, payload []byte, through time.Time) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Observe, at, basis, payload, through)
}

func (t *Translator) deleteEdge(b *batch, from, to identity.Fingerprint, rel catalog.RelationType, at time.Time, basis store.EventTimeBasis) {
	t.emit(b, store.EdgeSubject(from, to, rel), lifecycle.Delete, at, basis, nil, time.Time{})
}

// raise holds an event time at the previous record's of the same subject, with
// its basis, and counts it. The fold orders by event time: a description
// asserted earlier than the previous record would show the older description
// after the newer one, and a Delete earlier than an Observe would not end it.
func (b *batch) raise(at time.Time, basis store.EventTimeBasis, last time.Time, lastBasis store.EventTimeBasis) (time.Time, store.EventTimeBasis) {
	if at.Before(last) {
		b.skip(SkipClampedTime)
		return last, lastBasis
	}
	return at, basis
}

// firstTime is the event time of the first record of a UID: the creation
// timestamp of its description if there is one and it is plausible, and
// otherwise the instant.
func (b *batch) firstTime(creation string) (time.Time, store.EventTimeBasis) {
	if creation == "" {
		return b.instant, b.basis
	}
	tm, ok := parseTime(creation)
	if !ok || tm.Before(store.MinEventTime) || tm.After(b.instant.Add(MaxClockAhead)) {
		b.skip(SkipImplausibleTime)
		return b.instant, b.basis
	}
	return tm, store.BasisObjectField
}

// noteDropped counts the description keys dropped by a state, each name the first time
// only.
func (t *Translator) noteDropped(b *batch, dropped []string) {
	for _, k := range dropped {
		if _, ok := t.noted[k]; ok {
			continue
		}
		if len(t.noted) >= maxNotedKeys {
			if !t.notedFull {
				t.notedFull = true
				b.note(NoteDroppedKeysFull, "")
			}
			return
		}
		t.noted[k] = struct{}{}
		b.note(NoteDescriptionKeyDropped, k)
	}
}

func deletedKey(typ, id string) string { return typ + "\x00" + id }
