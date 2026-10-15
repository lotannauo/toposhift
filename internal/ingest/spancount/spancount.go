// Package spancount translates the span counter of the OpenTelemetry count
// connector into service instances, the services they are instances of, and the
// pods they run in.
//
// # Input
//
// The count connector copies the attributes of a span's resource onto its output,
// and counts the spans in a delta sum: trace.span.count. A point's times are the
// earliest and the latest time of the spans it counts, on the application's clock.
// The connector emits nothing for a resource with no spans, so a point with a value
// above zero is evidence that a span of the resource was seen at the point's time.
//
// What the resource says is what identifies things:
//
//   - service.name and service.namespace, the service;
//   - service.instance.id, with the two above, the service instance;
//   - k8s.pod.uid, the pod the instance runs in, when the application's SDK sets it
//     from the downward API.
//
// # A point is a sighting
//
// Every delta point with a value above zero is a sighting of the instance, at the
// point's time. A resource with several such points is one sighting, at the latest of
// their times. A cumulative sum, a sum with no temporality, a point with a value of
// zero and a metric of another name are counted and set aside.
//
// # Identity
//
// An instance is (service.namespace or none, service.name, service.instance.id), a
// service is (service.namespace or none, service.name), and a pod is k8s.pod.uid. A
// value is used as received if it is a string, given once, and not blank; an empty
// namespace is none. A resource with no usable service.name, service.instance.id or
// namespace writes nothing, and is counted. Nothing else identifies anything: a pod
// IP, a pod name, a host name and a network address never identify or join anything.
// The services are the ones package svcgraph writes, with the same identity.
//
// # The bridge
//
// An instance is part of the pod it runs in. The edge is written only from a
// resource that carries k8s.pod.uid, and names the pod by that uid. A resource with
// no k8s.pod.uid, including one that carries only k8s.pod.ip, writes the instance
// without a bridge and is counted with [UnplacedNoPodUID]; so does one whose uid is
// not usable ([UnplacedBadPodUID]). The pod is never asserted: the Kubernetes
// translators do that.
//
// That the uid was not matched by an address is a rule of the deployment and not
// something this package can check: a processor that associates a resource with a
// pod by the connection's address stamps a uid that cannot be told from one the
// application's SDK set. The collectors in front of toposhift must not do so.
//
// # What is asserted
//
// The producer is "spancount/<cluster>", given in [Config.Producer], and it is never
// renamed. A sighting is written as Observes with the TTL of [Config], at the point's
// time, with basis producer_event (the application's clock):
//
//   - the service, in layer L3, with no payload;
//   - the service instance, in layer L2, whose payload is the canonical JSON of its
//     service.version if it has a usable one, and none otherwise, so that a new
//     version ends the instance's run: that is the deploy signal;
//   - instance_of from the instance to the service, in layer L2;
//   - part_of from the instance to the pod, in layer L2, when there is a bridge.
//
// The edges have no payload. This package never writes a Delete, never a Through, and
// never a count: the evidence is the sighting, and the instance ends at the last
// sighting plus the TTL. It is not state: nothing a call returns depends on an
// earlier call.
//
// # Times
//
// This package never reads a clock. Every time it returns is a point's time, and the
// one comparison with the present uses the time the caller received the request: a
// point later than that by more than [otlpmetrics.MaxClockAhead] is skipped and
// counted, never rejected and never replaced by the receipt time.
//
// A Translator is not safe for concurrent use, but it keeps no state.
package spancount

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Source is the source name of the producer: the producer of a cluster is
// "spancount/<cluster>". It is never renamed.
const Source = "spancount"

// Metric is the name of the count connector's span counter.
const Metric = "trace.span.count"

// KeyServiceVersion is the resource attribute that describes an instance.
const KeyServiceVersion = "service.version"

// maxTTL keeps the TTL far from the end of the representable times.
const maxTTL = 366 * 24 * time.Hour

// maxVersionLen bounds the version kept in an instance's payload, in bytes.
const maxVersionLen = 256

// The reasons a translator gives for a point or a resource it does not turn into
// records, or part of one it does not use. A caller that exposes them as counters
// names them for what they are, not as dropped data.
const (
	// SkipNotCounter is a point of another metric.
	SkipNotCounter = "not the span counter"
	// SkipNotDelta is a point of a sum that is not a delta sum.
	SkipNotDelta = "span count is not a delta sum"
	// SkipNoSpans is a point whose value is zero.
	SkipNoSpans = "point counts no spans"
	// SkipNoServiceName is a resource with a sighting and no usable service.name:
	// nothing is written for it.
	SkipNoServiceName = "no service name"
	// SkipNoInstanceID is a resource with a sighting and no usable
	// service.instance.id: nothing is written for it.
	SkipNoInstanceID = "no service instance id"
	// SkipBadNamespace is a resource with a sighting whose service.namespace is not
	// a string, is given twice with different values, or is blank: nothing is
	// written for it.
	SkipBadNamespace = "service namespace unusable"
	// SkipBadIdentity is a resource whose identity the identity encoding refused.
	SkipBadIdentity = "identity refused by the identity encoding"
	// UnplacedNoPodUID is a resource with no k8s.pod.uid: the instance is written
	// and the bridge is not.
	UnplacedNoPodUID = "no pod uid: instance written without a bridge"
	// UnplacedBadPodUID is a resource whose k8s.pod.uid is not a string, is given
	// twice with different values, or is blank: the instance is written and the
	// bridge is not.
	UnplacedBadPodUID = "unusable pod uid: instance written without a bridge"
	// RefusedVersion is a resource whose service.version is present and not usable
	// (not a string, given twice with different values, not valid UTF-8, or longer
	// than 256 bytes): the instance is written with no payload.
	RefusedVersion = "unusable service version: instance written without one"
)

// Config configures a [Translator].
type Config struct {
	// Producer names the source of the assertions: "spancount/<cluster>". The
	// caller builds it; this package does not check its format. Required.
	Producer lifecycle.Producer
	// Resolver turns the identifying attributes into fingerprints. Required;
	// usually identity.NewResolver(catalog.Default()).
	Resolver *identity.Resolver
	// TTL is the TTL of every Observe, and so how long an instance outlives its last
	// sighting. Required: it must be above zero and at most a year.
	TTL time.Duration
}

// Result is what one resource stands for.
type Result struct {
	// Records are the store records, in the order they should be sequenced: the
	// service, the instance, instance_of, and the bridge if there is one. They have
	// no Seq, no Through and no Delete.
	Records []store.Record
	// Skipped says what was set aside or not used, and why, sorted by reason.
	Skipped []otlpmetrics.Count
}

// Translator turns the points of the span counter into store records.
type Translator struct {
	producer lifecycle.Producer
	fp       otlpmetrics.Fingerprints
	ttl      time.Duration
}

// New returns a translator.
func New(c Config) (*Translator, error) {
	switch {
	case c.Producer == "":
		return nil, errors.New("spancount: producer is required")
	case c.Resolver == nil:
		return nil, errors.New("spancount: resolver is required")
	case c.TTL <= 0:
		return nil, errors.New("spancount: ttl is required and must be above zero")
	case c.TTL > maxTTL:
		return nil, fmt.Errorf("spancount: ttl %s is over %s", c.TTL, maxTTL)
	}
	return &Translator{producer: c.Producer, fp: otlpmetrics.NewFingerprints(c.Resolver), ttl: c.TTL}, nil
}

// TTL is the TTL of every Observe this translator writes.
func (t *Translator) TTL() time.Duration { return t.ttl }

// Translate turns the points of one resource into records. received is the time the
// caller received the request; it may be zero, and then no point is judged to be
// ahead of it.
func (t *Translator) Translate(res otlpmetrics.Resource, received time.Time) Result {
	var tally otlpmetrics.Tally
	otlpmetrics.Dropped(&tally, res)
	var at time.Time
	sightings := 0
	for _, p := range res.Points {
		if p.Metric != Metric {
			tally.Add(SkipNotCounter)
			continue
		}
		tm, reason := otlpmetrics.Check(p, received)
		if reason != "" {
			tally.Add(reason)
			continue
		}
		switch {
		case p.Temporality != otlpmetrics.Delta:
			tally.Add(SkipNotDelta)
		case p.Value <= 0:
			tally.Add(SkipNoSpans)
		default:
			sightings++
			if tm.After(at) {
				at = tm
			}
		}
	}
	if sightings == 0 {
		return Result{Skipped: tally.Counts()}
	}
	recs := t.sighting(&tally, otlpmetrics.Collect(res.Attributes), at)
	return Result{Records: recs, Skipped: tally.Counts()}
}

// sighting is the records of the resource's instance, or nothing if it cannot be
// identified.
func (t *Translator) sighting(tally *otlpmetrics.Tally, attrs otlpmetrics.Attributes, at time.Time) []store.Record {
	name, st := attrs.Get(otlpmetrics.KeyServiceName)
	if st != otlpmetrics.StatusOK || !otlpmetrics.UsableID(name) {
		tally.Add(SkipNoServiceName)
		return nil
	}
	instanceID, st := attrs.Get(otlpmetrics.KeyServiceInstanceID)
	if st != otlpmetrics.StatusOK || !otlpmetrics.UsableID(instanceID) {
		tally.Add(SkipNoInstanceID)
		return nil
	}
	ns, st := attrs.Get(otlpmetrics.KeyServiceNamespace)
	if st == otlpmetrics.StatusBad || !otlpmetrics.UsableNamespace(ns) {
		tally.Add(SkipBadNamespace)
		return nil
	}
	service, err := t.fp.Service(ns, name)
	if err != nil {
		tally.Add(SkipBadIdentity)
		return nil
	}
	instance, err := t.fp.Instance(ns, name, instanceID)
	if err != nil {
		tally.Add(SkipBadIdentity)
		return nil
	}
	var pod identity.Fingerprint
	switch uid, st := attrs.Get(otlpmetrics.KeyPodUID); {
	case st == otlpmetrics.StatusAbsent:
		tally.Add(UnplacedNoPodUID)
	case st == otlpmetrics.StatusBad || !otlpmetrics.UsableID(uid):
		tally.Add(UnplacedBadPodUID)
	default:
		if pod, err = t.fp.Pod(uid); err != nil {
			tally.Add(UnplacedBadPodUID)
		}
	}
	obs := func(s store.Subject, payload []byte) store.Record {
		return otlpmetrics.Observe(t.producer, s, at, store.BasisProducerEvent, t.ttl, payload)
	}
	recs := []store.Record{
		obs(store.EntitySubject(service), nil),
		obs(store.EntitySubject(instance), t.description(tally, attrs)),
		obs(store.EdgeSubject(instance, service, catalog.InstanceOf), nil),
	}
	if !pod.IsZero() {
		recs = append(recs, obs(store.EdgeSubject(instance, pod, catalog.PartOf), nil))
	}
	return recs
}

// description is the payload of an instance: the canonical JSON of its version, or
// nothing.
func (t *Translator) description(tally *otlpmetrics.Tally, attrs otlpmetrics.Attributes) []byte {
	v, st := attrs.Get(KeyServiceVersion)
	switch {
	case st == otlpmetrics.StatusAbsent:
		return nil
	case st == otlpmetrics.StatusBad || !utf8.ValidString(v) || len(v) > maxVersionLen:
		tally.Add(RefusedVersion)
		return nil
	case v == "":
		return nil
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(map[string]string{KeyServiceVersion: v}); err != nil {
		// Only a map of strings is encoded.
		panic("spancount: encoding a description: " + err.Error())
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}
