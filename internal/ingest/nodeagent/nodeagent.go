// Package nodeagent translates the host entity events of node-level OpenTelemetry
// collectors into store records of hosts and of the placement of Kubernetes
// nodes on them.
//
// # Input
//
// A collector that runs on each node, with the host metrics receiver and a
// resource detection processor in a logs pipeline, emits one entity event per
// report interval, and reaches toposhift only through the cluster collector.
// The record's eventName is entity.state, its body is empty, and its time is
// timeUnixNano, the node collector's clock. Its attributes are entity.type, which
// is host, and entity.report.interval, an integer number of seconds. The record
// carries no entity.id and no entity.description: what identifies and describes
// the host is in the resource attributes of the record's resource:
//
//   - host.id, the identity of the host;
//   - k8s.node.uid, the identity of the Kubernetes node the collector runs on;
//   - topo.host.boot.id, the boot id of the host's kernel, set by the collector's
//     own configuration;
//   - the description keys listed under "Payload";
//   - cloud.provider, cloud.region and cloud.availability_zone, which name the
//     zone when [Config.Zones] is on.
//
// A record whose eventName is not entity.state or entity.delete is counted with
// [SkipNotEntityEvent], and one of another entity type with [SkipNotHost]: the
// caller that gives this translator its records routes by entity.type, and this
// package does not depend on how. The older form of the events, with attributes
// named otel.entity.*, is not translated and is counted with
// [SkipLegacyEntityEvent]. A delete event is counted with [SkipHostDelete] and
// writes nothing: see "What is asserted".
//
// # Identity
//
// The host is the entity of type host, identified by host.id. A node is the
// entity of type k8s.node, identified by k8s.node.uid. Both are taken exactly as
// received: nothing is trimmed, case-folded or normalised, and a value is
// identity only if it is a string on the resource, there once. This package does
// not assume that a host.id is the raw machine id or a hash of it: whichever the
// collector's configuration produces is the identity, and matching against
// another source of the same host is only as good as that source's value being
// the same bytes. A host.id that is absent, not a string, empty, white space only,
// or given twice with different values writes nothing, and is counted with
// [SkipNoHostID]: the placement stops at the node, and what runs on the node is
// unattributed to a host. The same holds for k8s.node.uid and the edge, counted
// with [RefusedNoNodeUID]; the host is still asserted.
//
// Nothing else identifies anything. host.name, the node's name, the node's
// providerID, the kubelet's machine id and system uuid, and every address are
// never identity and never join anything. The translator resolves no name to a
// uid; many nodes may run on one host (a machine that runs several node
// containers), and then they report one host.id with distinct node uids: one
// host, many runs_on edges, and no collision. A node uid is never the host's.
//
// The identity encoding is the identity package's. An exact byte-for-byte host.id
// is part of a fingerprint's canonical bytes, so it is held in the store's data
// and in any capture of the events. The value is meant to be kept confidential
// where it is a machine id.
//
// # What is asserted
//
// The producer is "nodeagent/<cluster>", given in [Config.Producer]. Every node
// collector of a cluster shares it, and it writes only its own prefixes: the
// host entity, the edge from the node to the host, and with zones the zone entity
// and the edge from the host to the zone. It never asserts the node entity, which
// the Kubernetes translators do, and never asserts anything about a pod.
//
//   - host: an Observe of the host, with its payload and boot.
//   - runs_on: an Observe of the edge from the k8s.node to the host, re-asserted
//     with each host record and with its TTL, so that it lapses with the record
//     that carries it. Its layer is the one the catalog records for the relation
//     (L1: binding a workload to its host is host placement), not the layer of the
//     node.
//   - located_in: only with [Config.Zones] on, and only when cloud.provider,
//     cloud.region and cloud.availability_zone are all present and non-empty: an
//     Observe of the topo.zone entity whose id is
//     "<provider>/<region>/<availability zone>", and of the edge from the host to
//     it. The id format is part of identity and cannot change once written.
//
// The edge payloads, and the zone's, are empty.
//
// This translator writes Observes only. It never writes a Delete, and never
// writes a record with a Through: no time is backdated, because a report says only
// that the host was seen at the instant of the report. A Delete by the shared
// producer could release a reference that another node collector still refreshes
// (two nodes on one host; one zone). A node reported on a new host.id (a detector
// change, a node re-created on a machine of another identity) gets a new edge, and
// its old edge lapses by TTL; for up to one TTL the node has two live runs_on
// edges, and a pod's host over that overlap is unattributed, not the newer one.
//
// # A pod's host
//
// A pod's host is not written. It is derived at read time, and labeled as
// derived: a pod is on host H over the overlap of its scheduled_on edge to node N
// and N's runs_on edge to H. Where no runs_on edge covers the pod's time, its host
// is unattributed, and co-residency is of the node only ("shared node", not
// "shared host"). Where N has two live runs_on edges to different hosts, the pod's
// host is unattributed over the overlap, whichever edge is newer.
//
// A runs_on edge is stored in a longer-lived layer than its node (an edge in L1,
// a node in L2), so an edge can outlive the retention of its node's record. The
// read side's range semantics end an edge at its node's existence; this is by
// design, and this package does nothing about it.
//
// # Addresses
//
// host.ip and host.mac are not in the payload and join nothing. Who owns an
// address over time is decided where addresses are modelled, with this rule: an
// address claimed by two hosts at once is owned by neither.
//
// # Payload
//
// The payload of the host is the canonical JSON (sorted keys) of an allow-list of
// the resource's attributes, each with its string value: host.arch, host.type,
// os.type, host.cpu.model.name, cloud.provider, cloud.platform, cloud.region,
// cloud.availability_zone and cloud.resource_id. A key whose value is an empty
// string is left out, so that a detector that sometimes reports one empty does not
// make a changing payload, which would end the run of refreshes. os.version and
// os.description are never kept, because the detector reads them from the
// collector's own container, not from the host: the payload would state the
// collector image's userland as the host's. Every other attribute (host.name, the
// node's name and every address among them) is dropped. The first time this
// translator drops a key name it counts it with [NoteDescriptionKeyDropped] in
// [Result.Notes], so that a steady stream adds nothing after the first. A key that
// is read but not kept (host.id, k8s.node.uid, topo.host.boot.id and
// k8s.cluster.name) is not dropped.
//
// # Boot
//
// [store.Record.Boot] of the host is topo.host.boot.id exactly as received: this
// package never trims or edits it, because the store compares boots as given and
// two values that differ in white space are two boots. The collector's
// configuration is what strips the newline of the kernel's file. A boot id that
// is absent, or blank, gives a host record with no boot, counted with
// [SubstitutedNoBoot]; one that is not a string, is not valid UTF-8 or is longer than
// [store.MaxBootLen] does the same, counted with [SubstitutedBadBoot]. Two machines that
// report one host.id with distinct boot ids are a clone collision, which the store
// reports, loudly, by quarantining the host. Many nodes on one machine share the
// kernel's boot id, which is one host and no collision. Known limits: a clone made
// from a snapshot shares its boot id and is missed, and a machine restored from an
// older snapshot reads as a collision.
//
// # Times
//
// Every time this package writes comes from the record or from the caller. It
// never reads a clock.
//
// The instant of a record is the time it is taken to describe. By default the
// producer's clock is trusted: the instant is timeUnixNano, with basis
// producer_event. The caller may also pass the time it received the record, which
// is optional (zero means not given):
//
//   - a timeUnixNano later than received plus [MaxClockAhead] is implausible: the
//     instant is received, with basis receipt, counted with [SubstitutedReceiptImplausibleTime];
//   - a timeUnixNano earlier than received minus the TTL is kept, and counted with
//     [NoteClockBehind]: late records are the sequencer's business;
//   - a zero timeUnixNano takes received, counted with [SubstitutedReceiptNoTime], and without
//     received the record is refused with [ErrNoTime].
//
// With [Config.DistrustProducerClock] the instant is always received, with basis
// receipt, and without it the record is refused with [ErrNoTime]. A refused record
// leaves the translator unchanged.
//
// The time of a record is never corrected against another record's. Several node
// collectors on one host write the host's prefix in arrival order, with their own
// clocks, and a record that is earlier than one before it is the store's late
// path, not this package's.
//
// # TTL
//
// The TTL of every Observe is [DefaultTTLMultiple] times [Config.Interval], the
// report interval the node collectors are configured with, for the life of the
// translator. It is never taken from an event: every collector of a cluster shares
// one producer, every Observe of one producer must carry one TTL at a time, and
// events of two intervals (a rollout, or one misconfigured collector) would
// otherwise change it with every record and end every run of refreshes. An event
// whose entity.report.interval is not [Config.Interval], or is missing or unusable,
// is translated as usual, counted with [SubstitutedConfiguredInterval], and noted once for
// each distinct value with [NoteIntervalMismatch]. Changing the interval takes
// effect with a new translator.
//
// # Counters
//
// [Result.Skipped] holds counts of three kinds, told apart by the name of the reason:
// Skip, a record set aside that writes nothing; Refused, a part refused while the host
// is still written; and Substituted, a record still written with something put in
// place of what it gave (the receipt time, the configured interval, no boot). A caller
// that exposes them as counters names them for that, not as dropped records.
//
// # State
//
// The translator keeps only the set of description key names it has already
// counted as dropped, and the set of report intervals it has already noted, each
// bounded. Nothing it writes depends on any earlier record: after a restart, every
// record is as it would have been. No record is backdated, so none is marked for
// resuming, [Result.Backdated] is always empty, and [Translator.Resumed] is a
// no-op that exists so that the translator can stand where a [resume.Resumer] is
// asked for. [Comparable] is the identity function for the same reason: a payload
// is a description throughout.
//
// A Translator is not safe for concurrent use.
package nodeagent

import (
	"bytes"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// Source is the source name of the producer: the producer of a cluster is
// "nodeagent/<cluster>". It is never renamed.
const Source = "nodeagent"

// DefaultTTLMultiple is the TTL of an Observe, in report intervals.
const DefaultTTLMultiple = 3

// MaxClockAhead is how far past the time a record was received a time of the
// producer may lie before the translator distrusts it. The node collector and this
// process keep separate clocks.
const MaxClockAhead = 5 * time.Minute

// maxInterval is the longest report interval accepted. It keeps a TTL of
// [DefaultTTLMultiple] intervals far from the end of the representable times.
const maxInterval = 366 * 24 * time.Hour

// maxNoted bounds each of the sets of names and values already counted, which a
// stream of ever new ones would otherwise grow for ever.
const maxNoted = 4096

// The reasons a translator gives for a record or part of a record it does not turn
// into a store record, or a part of one it does not use.
//
// A reason named Skip is a record set aside, and writes nothing. One named Refused is
// a part of a record that is refused while the host is still written. One named
// Substituted counts a record that is still written, with something put in place of
// what the record gave (the receipt time, the configured interval, no boot). A caller
// that exposes these as counters must name them for what they are, not as dropped
// records.
const (
	// SkipNotEntityEvent is a record whose eventName is not entity.state or
	// entity.delete.
	SkipNotEntityEvent = "not an entity event"
	// SkipLegacyEntityEvent is an entity event in the older attribute form
	// (otel.entity.*), which is not translated.
	SkipLegacyEntityEvent = "entity event in the legacy form"
	// SkipNotHost is an entity event of a type other than host, or with no type.
	SkipNotHost = "entity type is not host"
	// SkipHostDelete is the delete of a host. The node collectors assert no
	// Delete, and a delete event is translated to nothing.
	SkipHostDelete = "host delete: not translated"
	// SkipNoHostID is an event whose resource has no usable host.id: it is
	// absent, not a string, blank, or given twice with different values. Nothing is
	// written for it.
	SkipNoHostID = "no host id"
	// RefusedNoNodeUID is an event whose resource has no usable k8s.node.uid. The host
	// is written and the runs_on edge is not.
	RefusedNoNodeUID = "no node uid: edge refused, host written"
	// RefusedZoneID is an event, with zones on, whose cloud.provider, cloud.region or
	// cloud.availability_zone contains "/": the joined id would be ambiguous, so
	// no zone and no located_in edge is written. The host is.
	RefusedZoneID = "zone id part contains a slash: zone refused, host written"
	// SubstitutedNoBoot is an event with no topo.host.boot.id, or a blank one. The host is
	// written with no boot.
	SubstitutedNoBoot = "no boot id: host written without one"
	// SubstitutedBadBoot is an event whose topo.host.boot.id is present and not usable: it
	// is not a string, is not valid UTF-8, or is longer than the store keeps. The
	// host is written with no boot.
	SubstitutedBadBoot = "unusable boot id: host written without one"
	// SubstitutedConfiguredInterval is an event whose entity.report.interval is not the
	// translator's interval, or is missing or unusable. It is translated as usual.
	SubstitutedConfiguredInterval = "report interval differs: configured one used"
	// SubstitutedReceiptNoTime is an event with no timeUnixNano: the time it was received is
	// taken.
	SubstitutedReceiptNoTime = "no event time: receipt substituted"
	// SubstitutedReceiptImplausibleTime is a time of the record that was not used, because it is
	// unreadable or lies too far ahead of the receipt.
	SubstitutedReceiptImplausibleTime = "implausible time: receipt substituted"
)

// The reasons a translator gives for a note: something the caller should know that
// is not a record set aside.
const (
	// NoteDescriptionKeyDropped is a resource key name dropped for the first time;
	// the note's detail is the name.
	NoteDescriptionKeyDropped = "description key dropped"
	// NoteIntervalMismatch is a report interval, noted the first time it differs
	// from the translator's; the detail is the value, "missing" or "unusable".
	NoteIntervalMismatch = "report interval differs from the configured one"
	// NoteClockBehind is a record whose time is earlier than the time it was
	// received by more than the TTL. Its time is kept.
	NoteClockBehind = "producer time behind receipt by more than the TTL"
	// NoteNotedFull is the first name or value counted after the translator
	// stopped remembering them; from then on new ones are not noted.
	NoteNotedFull = "dropped key names and intervals no longer tracked"
)

// ErrNoTime is returned for a record with no time that can be used: with the
// producer's clock distrusted and no receipt time, or with neither a usable
// timeUnixNano nor a receipt time.
var ErrNoTime = errors.New("nodeagent: record has no usable time")

// Config configures a [Translator].
type Config struct {
	// Producer names the source of the assertions: "nodeagent/<cluster>". The
	// caller builds it; this package does not check its format. Required.
	Producer lifecycle.Producer
	// Resolver turns the identifying attributes into fingerprints. Required;
	// usually identity.NewResolver(catalog.Default()).
	Resolver *identity.Resolver
	// Interval is the report interval the node collectors are configured with. The
	// TTL of every Observe is [DefaultTTLMultiple] times it. Required: it must be
	// above zero and at most a year.
	Interval time.Duration
	// DistrustProducerClock says that the time in a record is not to be used: the
	// instant of every record is the time it was received, which the caller then
	// has to give. The zero value trusts the producer.
	DistrustProducerClock bool
	// Zones turns on the zone entity and the located_in edge from the host to it.
	// The zero value writes neither.
	Zones bool
}

// Result is what one log record stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced: the
	// host, its runs_on edge, and with zones the zone and its located_in edge. They
	// have no Seq, no Through and no Delete. Only the host carries a Boot.
	Records []store.Record
	// Backdated is always empty: no record of this translator is backdated, so none
	// is marked for [resume.Apply].
	Backdated []resume.Mark
	// Skipped says what was set aside or not used, and why, sorted by reason.
	Skipped []Skip
	// Notes say what the caller should know and log, sorted by reason and detail.
	// They do not change the records.
	Notes []Note
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
	// dropped key, or the interval of a mismatch.
	Detail string
	Count  int
}

// Translator turns log records into store records.
type Translator struct {
	producer lifecycle.Producer
	resolver *identity.Resolver
	interval time.Duration
	ttl      time.Duration
	distrust bool
	zones    bool

	noted     map[string]struct{}
	intervals map[string]struct{}
	notedFull bool
}

// New returns a translator with no state.
func New(c Config) (*Translator, error) {
	switch {
	case c.Producer == "":
		return nil, errors.New("nodeagent: producer is required")
	case c.Resolver == nil:
		return nil, errors.New("nodeagent: resolver is required")
	case c.Interval <= 0:
		return nil, errors.New("nodeagent: interval is required and must be above zero")
	case c.Interval > maxInterval:
		return nil, fmt.Errorf("nodeagent: interval %s is over %s", c.Interval, maxInterval)
	}
	return &Translator{
		producer:  c.Producer,
		resolver:  c.Resolver,
		interval:  c.Interval,
		ttl:       DefaultTTLMultiple * c.Interval,
		distrust:  c.DistrustProducerClock,
		zones:     c.Zones,
		noted:     make(map[string]struct{}),
		intervals: make(map[string]struct{}),
	}, nil
}

// TTL is the TTL of every Observe this translator writes.
func (t *Translator) TTL() time.Duration { return t.ttl }

// batch collects what one call produces.
type batch struct {
	recs  []store.Record
	skips map[string]int
	notes map[[2]string]int
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
	r := Result{Records: b.recs}
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

// Translate turns one OTLP log record, with the resource it came with, into
// records without Seq. It is deterministic given the sequence of earlier inputs.
// received is the time the caller received the record; it may be zero, and see the
// package documentation for how it is used.
//
// An error means the record could not be read at all (it has no usable time, or
// an identifier is not a valid key), and leaves the translator unchanged. A record
// that is valid but not translated is not an error: it is in [Result.Skipped].
func (t *Translator) Translate(res capture.Resource, lr capture.LogRecord, received time.Time) (Result, error) {
	var b batch
	ev, reason := decode(lr)
	if reason != "" {
		b.skip(reason)
		return b.result(), nil
	}
	attrs := collect(res.Attributes)

	hostID, ok := attrs.text(string(catalog.HostID))
	if !ok || strings.TrimSpace(hostID) == "" {
		b.skip(SkipNoHostID)
		return b.result(), nil
	}
	nodeUID, hasNode := attrs.text(string(catalog.K8sNodeUID))
	if hasNode && strings.TrimSpace(nodeUID) == "" {
		hasNode = false
	}
	if !hasNode {
		b.skip(RefusedNoNodeUID)
	}

	at, basis, err := t.instant(&b, lr, received)
	if err != nil {
		return Result{}, err
	}

	// Everything that can fail is done before anything is kept.
	hostFP, err := t.fingerprint(catalog.Host, catalog.HostID, hostID)
	if err != nil {
		return Result{}, err
	}
	var nodeFP, zoneFP identity.Fingerprint
	if hasNode {
		if nodeFP, err = t.fingerprint(catalog.K8sNode, catalog.K8sNodeUID, nodeUID); err != nil {
			return Result{}, err
		}
	}
	zoned := false
	if t.zones {
		var zoneID string
		var refused bool
		zoneID, zoned, refused = zoneOf(attrs)
		if refused {
			b.skip(RefusedZoneID)
		}
		if zoned {
			if zoneFP, err = t.fingerprint(catalog.Zone, catalog.ZoneID, zoneID); err != nil {
				return Result{}, err
			}
		}
	}
	boot := bootOf(&b, attrs)
	d := describe(attrs)

	t.noteDropped(&b, d.dropped)
	t.checkInterval(&b, ev.iv)

	t.observe(&b, store.EntitySubject(hostFP), at, basis, d.payload, boot)
	if hasNode {
		t.observe(&b, store.EdgeSubject(nodeFP, hostFP, catalog.RunsOn), at, basis, nil, "")
	}
	if zoned {
		t.observe(&b, store.EntitySubject(zoneFP), at, basis, nil, "")
		t.observe(&b, store.EdgeSubject(hostFP, zoneFP, catalog.LocatedIn), at, basis, nil, "")
	}
	return b.result(), nil
}

// instant returns the time a record describes and where it came from. It counts
// what it sets aside or notes in b and changes nothing else.
func (t *Translator) instant(b *batch, lr capture.LogRecord, received time.Time) (time.Time, store.EventTimeBasis, error) {
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
		b.skip(SubstitutedReceiptNoTime)
		return received, store.BasisReceipt, nil
	case n > math.MaxInt64:
		if received.IsZero() {
			return time.Time{}, 0, ErrNoTime
		}
		b.skip(SubstitutedReceiptImplausibleTime)
		return received, store.BasisReceipt, nil
	}
	tm := time.Unix(0, int64(n)).UTC()
	if !received.IsZero() {
		if tm.After(received.Add(MaxClockAhead)) {
			b.skip(SubstitutedReceiptImplausibleTime)
			return received, store.BasisReceipt, nil
		}
		if tm.Before(received.Add(-t.ttl)) {
			b.note(NoteClockBehind, "")
		}
	}
	return tm, store.BasisProducerEvent, nil
}

func (t *Translator) fingerprint(typ catalog.EntityType, key catalog.AttributeKey, v string) (identity.Fingerprint, error) {
	id, err := t.resolver.Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("nodeagent: %s %q: %w", typ, v, err)
	}
	return id.Fingerprint(), nil
}

// bootOf is the boot of the host record: the value as received, if there is a
// usable one.
func bootOf(b *batch, attrs attributes) string {
	a, ok := attrs[keyBoot]
	switch {
	case !ok:
		b.skip(SubstitutedNoBoot)
		return ""
	case a.text == nil || a.conflict:
		b.skip(SubstitutedBadBoot)
		return ""
	}
	s := *a.text
	switch {
	case strings.TrimSpace(s) == "":
		b.skip(SubstitutedNoBoot)
		return ""
	case len(s) > store.MaxBootLen || !utf8.ValidString(s):
		b.skip(SubstitutedBadBoot)
		return ""
	}
	return s
}

// zoneOf is the zone id of the host and whether the three cloud keys that make it
// are all there, as non-empty strings.
//
// A part that contains "/" would make the ids of two different zones collide
// ("a/b" and "c" against "a" and "b/c"), so it gives no zone: refused is true, and
// the caller counts it.
func zoneOf(attrs attributes) (id string, ok, refused bool) {
	var parts [3]string
	for i, k := range [...]string{keyCloudProvider, keyCloudRegion, keyCloudZone} {
		s, present := attrs.text(k)
		if !present || s == "" {
			return "", false, false
		}
		parts[i] = s
	}
	for _, p := range parts {
		if strings.Contains(p, "/") {
			return "", false, true
		}
	}
	return strings.Join(parts[:], "/"), true, false
}

// checkInterval counts an event whose report interval is not the translator's.
func (t *Translator) checkInterval(b *batch, iv interval) {
	var detail string
	switch {
	case !iv.present:
		detail = "missing"
	case !iv.valid:
		detail = "unusable"
	case iv.d == t.interval:
		return
	default:
		detail = iv.d.String()
	}
	b.skip(SubstitutedConfiguredInterval)
	if _, ok := t.intervals[detail]; ok {
		return
	}
	if len(t.intervals) >= maxNoted {
		t.noteFull(b)
		return
	}
	t.intervals[detail] = struct{}{}
	b.note(NoteIntervalMismatch, detail)
}

// noteDropped counts the keys dropped by an event, each name the first time only.
func (t *Translator) noteDropped(b *batch, dropped []string) {
	for _, k := range dropped {
		if _, ok := t.noted[k]; ok {
			continue
		}
		if len(t.noted) >= maxNoted {
			t.noteFull(b)
			return
		}
		t.noted[k] = struct{}{}
		b.note(NoteDescriptionKeyDropped, k)
	}
}

func (t *Translator) noteFull(b *batch) {
	if !t.notedFull {
		t.notedFull = true
		b.note(NoteNotedFull, "")
	}
}

func layerOf(fp identity.Fingerprint) catalog.Layer {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		// The resolver and the default catalog list the same types.
		panic("nodeagent: entity type " + string(fp.Type()) + " is not in the default catalog")
	}
	return e.Layer()
}

// recordLayer is the layer a record about the subject is stored in: an entity takes
// its type's layer, and an edge takes the layer its relation is stored in, which
// the catalog records.
func recordLayer(subj store.Subject) catalog.Layer {
	if subj.Kind != store.SubjectEdge {
		return layerOf(subj.A)
	}
	r, ok := catalog.Default().Relation(subj.Relation)
	if !ok || r.Derived() {
		// A translator emits only stored relations the default catalog lists.
		panic("nodeagent: relation " + string(subj.Relation) + " is not a stored relation of the default catalog")
	}
	return r.Layer()
}

// observe writes an Observe with the translator's TTL. It is the only way this
// package writes a record: there is no Delete and no Through.
func (t *Translator) observe(b *batch, subj store.Subject, at time.Time, basis store.EventTimeBasis, payload []byte, boot string) {
	b.recs = append(b.recs, store.Record{
		Layer:          recordLayer(subj),
		Subject:        subj,
		Producer:       t.producer,
		EventTime:      at,
		Kind:           lifecycle.Observe,
		TTL:            t.ttl,
		Payload:        bytes.Clone(payload),
		Boot:           boot,
		EventTimeBasis: basis,
	})
}
