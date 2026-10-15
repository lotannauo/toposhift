package factload

import (
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// SpanKind is the kind of a span.
type SpanKind uint8

// The span kinds the pairing reads. Anything else is internal.
const (
	KindInternal SpanKind = iota
	KindClient
	KindServer
	KindProducer
	KindConsumer
)

// Span is the part of a span the writer reads. The identity of its service and
// instance are read from the resource that produced it, never from an address or
// a host: Namespace is service.namespace (empty if the resource has none),
// Service is service.name, InstanceID is service.instance.id and PodUID is
// k8s.pod.uid. Nothing else of the resource is carried.
type Span struct {
	Start    time.Time
	TraceID  string
	SpanID   string
	ParentID string
	Kind     SpanKind

	Service    string
	Namespace  string
	InstanceID string
	PodUID     string
}

// MessagingPayload is the payload of a depends_on observed from a producer span
// and its consumer child. A failure does not travel that way through depends_on
// (it reaches the producer from a failing consumer and never the reverse), so a
// loader marks them.
const MessagingPayload = `{"connection":"messaging"}`

// AddressPayload is the payload of an instance whose id is the pod uid, and of
// every bridge from an instance to a pod. In the files this loader was written
// against the collector put the pod uid on the span by matching an address, which
// a writer of the product does not assert; the label says so, and the strata that
// depend on it are read with that in mind. Source names the dataset and revision
// the label came from.
func AddressPayload(source string) []byte {
	b, err := json.Marshal(struct {
		Basis  string `json:"basis"`
		Source string `json:"source"`
	}{"address-associated", source})
	if err != nil {
		panic(err)
	}
	return b
}

// Bucket is the width of the minute buckets that group spans.
const Bucket = time.Minute

// ServiceKey names a service by its namespace and name.
type ServiceKey struct{ Namespace, Name string }

// PairKey names the services of one dependency: the caller and the callee.
type PairKey struct{ From, To ServiceKey }

type payloadClass uint8

const (
	payloadNone payloadClass = iota
	payloadAddress
	payloadMessaging
)

// slot is one record to write: a subject observed in a minute bucket, with the
// earliest span start in it.
type slot struct {
	sub    store.Subject
	bucket int64
	class  payloadClass
}

// spanFacts is what the span writer found, as the records to write and the
// counts to report.
type spanFacts struct {
	recs []store.Record

	spans, namespaceSplit        int
	pairs, selfPairs, noChild    int
	services, instances, bridges int
	dependsOn, dependsOnRuns     int
	messaging                    map[PairKey]struct{}
	instrumented                 map[ServiceKey]struct{}
	calls                        map[PairKey]struct{}
}

type resolverCache struct {
	r    *identity.Resolver
	svc  map[ServiceKey]identity.Fingerprint
	inst map[[3]string]identity.Fingerprint
	pod  map[string]identity.Fingerprint
}

func newResolverCache(r *identity.Resolver) *resolverCache {
	return &resolverCache{r: r, svc: map[ServiceKey]identity.Fingerprint{}, inst: map[[3]string]identity.Fingerprint{}, pod: map[string]identity.Fingerprint{}}
}

func (c *resolverCache) service(k ServiceKey) (identity.Fingerprint, error) {
	if fp, ok := c.svc[k]; ok {
		return fp, nil
	}
	id, err := c.r.Resolve(catalog.Service, []identity.Attr{
		{Key: catalog.ServiceNamespace, Value: k.Namespace}, {Key: catalog.ServiceName, Value: k.Name},
	})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("factload: service %q: %w", k.Name, err)
	}
	c.svc[k] = id.Fingerprint()
	return c.svc[k], nil
}

func (c *resolverCache) instance(k ServiceKey, instance string) (identity.Fingerprint, error) {
	key := [3]string{k.Namespace, k.Name, instance}
	if fp, ok := c.inst[key]; ok {
		return fp, nil
	}
	id, err := c.r.Resolve(catalog.ServiceInstance, []identity.Attr{
		{Key: catalog.ServiceNamespace, Value: k.Namespace},
		{Key: catalog.ServiceName, Value: k.Name},
		{Key: catalog.ServiceInstanceID, Value: instance},
	})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("factload: instance of %q: %w", k.Name, err)
	}
	c.inst[key] = id.Fingerprint()
	return c.inst[key], nil
}

func (c *resolverCache) podOf(uid string) (identity.Fingerprint, error) {
	if fp, ok := c.pod[uid]; ok {
		return fp, nil
	}
	id, err := c.r.Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: uid}})
	if err != nil {
		return identity.Fingerprint{}, fmt.Errorf("factload: pod uid %q: %w", uid, err)
	}
	c.pod[uid] = id.Fingerprint()
	return c.pod[uid], nil
}

func layerOfSubject(s store.Subject) catalog.Layer {
	c := catalog.Default()
	if s.Kind == store.SubjectEntity {
		e, ok := c.Entity(s.A.Type())
		if !ok {
			panic("factload: entity type " + string(s.A.Type()) + " is not in the default catalog")
		}
		return e.Layer()
	}
	r, ok := c.Relation(s.Relation)
	if !ok {
		panic("factload: relation " + string(s.Relation) + " is not in the default catalog")
	}
	return r.Layer()
}

type spanRef struct{ trace, span string }

// writeSpans turns spans into the service writer's records, in event time order.
func writeSpans(spans []Span, source string) (spanFacts, error) {
	var f spanFacts
	f.messaging = map[PairKey]struct{}{}
	f.instrumented = map[ServiceKey]struct{}{}
	f.calls = map[PairKey]struct{}{}
	rc := newResolverCache(identity.NewResolver(catalog.Default()))
	address := AddressPayload(source)
	first := map[slot]time.Time{}
	note := func(s slot, at time.Time) {
		if t, ok := first[s]; !ok || at.Before(t) {
			first[s] = at
		}
	}
	bucketOf := func(t time.Time) int64 { return t.Truncate(Bucket).UnixNano() }

	// A span with no namespace is the service (none, name) only if the scenario has
	// no span of that name with a namespace. Otherwise it is a second identity of one
	// service: it is counted and written to nothing (no service, no pair, no
	// instance), so that it neither adds an entity to the base rate nor appears as a
	// member of its own.
	named := map[string]bool{}
	for _, s := range spans {
		if s.Namespace != "" && s.Service != "" {
			named[s.Service] = true
		}
	}
	dropped := func(s Span) bool { return s.Service == "" || (s.Namespace == "" && named[s.Service]) }

	// The parents a pairing can name: client and producer spans.
	parents := map[spanRef]int{}
	hadChild := map[int]bool{}
	f.spans = len(spans)
	for i, s := range spans {
		if dropped(s) {
			if s.Service != "" {
				f.namespaceSplit++
			}
			continue
		}
		if s.Kind == KindClient || s.Kind == KindProducer {
			parents[spanRef{s.TraceID, s.SpanID}] = i
		}
	}

	for _, s := range spans {
		if dropped(s) {
			continue
		}
		key := ServiceKey{s.Namespace, s.Service}
		fp, err := rc.service(key)
		if err != nil {
			return spanFacts{}, err
		}
		f.instrumented[key] = struct{}{}
		b := bucketOf(s.Start)
		note(slot{sub: store.EntitySubject(fp), bucket: b}, s.Start)

		if s.InstanceID != "" {
			in, err := rc.instance(key, s.InstanceID)
			if err != nil {
				return spanFacts{}, err
			}
			class := payloadNone
			if s.PodUID != "" && s.InstanceID == s.PodUID {
				class = payloadAddress
			}
			note(slot{sub: store.EntitySubject(in), bucket: b, class: class}, s.Start)
			note(slot{sub: store.EdgeSubject(in, fp, catalog.InstanceOf), bucket: b}, s.Start)
			if s.PodUID != "" {
				pod, err := rc.podOf(s.PodUID)
				if err != nil {
					return spanFacts{}, err
				}
				note(slot{sub: store.EdgeSubject(in, pod, catalog.PartOf), bucket: b, class: payloadAddress}, s.Start)
			}
		}

		// A server span with a client parent, or a consumer span with a producer
		// parent, in one trace.
		if (s.Kind != KindServer && s.Kind != KindConsumer) || s.ParentID == "" {
			continue
		}
		pi, ok := parents[spanRef{s.TraceID, s.ParentID}]
		if !ok {
			continue
		}
		p := spans[pi]
		messaging := false
		switch {
		case p.Kind == KindClient && s.Kind == KindServer:
		case p.Kind == KindProducer && s.Kind == KindConsumer:
			messaging = true
		default:
			continue
		}
		hadChild[pi] = true
		pk := PairKey{From: ServiceKey{p.Namespace, p.Service}, To: key}
		if pk.From == pk.To {
			f.selfPairs++
			continue
		}
		f.pairs++
		f.calls[pk] = struct{}{}
		from, err := rc.service(pk.From)
		if err != nil {
			return spanFacts{}, err
		}
		class := payloadNone
		if messaging {
			class = payloadMessaging
			f.messaging[pk] = struct{}{}
		}
		note(slot{sub: store.EdgeSubject(from, fp, catalog.DependsOn), bucket: bucketOf(p.Start), class: class}, p.Start)
	}
	for _, i := range parents {
		if !hadChild[i] {
			f.noChild++
		}
	}

	recs := make([]store.Record, 0, len(first))
	for s, at := range first {
		r := store.Record{
			Layer: layerOfSubject(s.sub), Subject: s.sub, Producer: ProducerTraces,
			EventTime: at, Kind: lifecycle.Observe, TTL: ServiceTTL, EventTimeBasis: store.BasisProducerEvent,
		}
		switch s.class {
		case payloadAddress:
			r.Payload = address
		case payloadMessaging:
			r.Payload = []byte(MessagingPayload)
		}
		recs = append(recs, r)
		switch {
		case s.sub.Kind == store.SubjectEntity && s.sub.A.Type() == catalog.Service:
			f.services++
		case s.sub.Kind == store.SubjectEntity:
			f.instances++
		case s.sub.Relation == catalog.DependsOn:
			f.dependsOn++
		case s.sub.Relation == catalog.PartOf:
			f.bridges++
		}
	}
	slices.SortFunc(recs, compareRecords)
	f.recs = recs
	f.dependsOnRuns = runsOf(recs, catalog.DependsOn)
	return f, nil
}

// compareRecords orders records by event time, then by subject, so that a run is
// the same whatever order the spans came in. A record of one subject at one
// event time has one payload class only in a bucket, so there is no further tie.
func compareRecords(a, b store.Record) int {
	if c := a.EventTime.Compare(b.EventTime); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Subject.Kind, b.Subject.Kind); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.Subject.A, b.Subject.A); c != 0 {
		return c
	}
	if c := store.CompareFingerprints(a.Subject.B, b.Subject.B); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Subject.Relation, b.Subject.Relation); c != 0 {
		return c
	}
	return cmp.Compare(string(a.Payload), string(b.Payload))
}

// runsOf counts the maximal runs of existence that the records of one relation
// give, ignoring its endpoints: a run goes on while the next observation is no
// later than the last one's deadline.
func runsOf(recs []store.Record, rel catalog.RelationType) int {
	last := map[store.Subject]time.Time{}
	runs := 0
	for _, r := range recs {
		if r.Subject.Kind != store.SubjectEdge || r.Subject.Relation != rel {
			continue
		}
		d, ok := last[r.Subject]
		if !ok || r.EventTime.After(d) {
			runs++
		}
		last[r.Subject] = r.EventTime.Add(r.TTL)
	}
	return runs
}
