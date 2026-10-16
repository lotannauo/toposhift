package spancount_test

import (
	"bytes"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/spancount"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

const (
	testProducer = lifecycle.Producer("spancount/test")
	// ttl is the instance TTL in these tests, six hours.
	ttl = 6 * time.Hour
)

var t0 = time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

func newTranslator(t testing.TB) *spancount.Translator {
	t.Helper()
	tr, err := spancount.New(spancount.Config{Producer: testProducer, Resolver: resolver(), TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func kv(k, v string) capture.KeyValue { return capture.KeyValue{Key: k, Value: sv(v)} }

// attrs are the resource attributes of an instance of a service in Kubernetes, as the
// application's SDK gives them: namespace, name, instance id, version and the pod's uid.
func attrs(ns, name, id, version, podUID string) []capture.KeyValue {
	var out []capture.KeyValue
	if ns != "" {
		out = append(out, kv("service.namespace", ns))
	}
	out = append(out, kv("service.name", name), kv("service.instance.id", id))
	if version != "" {
		out = append(out, kv("service.version", version))
	}
	if podUID != "" {
		out = append(out, kv("k8s.pod.uid", podUID))
	}
	return out
}

// count is a delta point of the span counter that counts n spans between the two
// times, as the connector makes it.
func count(from, to time.Time, n float64) otlpmetrics.SumPoint {
	return otlpmetrics.SumPoint{
		Scope: "countconnector", Metric: spancount.Metric,
		Temporality: otlpmetrics.Delta, Monotonic: true,
		StartTimeUnixNano: uint64(from.UnixNano()), TimeUnixNano: uint64(to.UnixNano()), Value: n,
	}
}

func resource(kvs []capture.KeyValue, points ...otlpmetrics.SumPoint) otlpmetrics.Resource {
	return otlpmetrics.Resource{Attributes: kvs, Points: points}
}

func skipped(r spancount.Result, reason string) int {
	for _, s := range r.Skipped {
		if s.Reason == reason {
			return s.Count
		}
	}
	return 0
}

func fp(t testing.TB, typ catalog.EntityType, attrs ...identity.Attr) identity.Fingerprint {
	t.Helper()
	id, err := resolver().Resolve(typ, attrs)
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func serviceFP(t testing.TB, ns, name string) identity.Fingerprint {
	a := []identity.Attr{{Key: catalog.ServiceName, Value: name}}
	if ns != "" {
		a = append([]identity.Attr{{Key: catalog.ServiceNamespace, Value: ns}}, a...)
	}
	return fp(t, catalog.Service, a...)
}

func instanceFP(t testing.TB, ns, name, id string) identity.Fingerprint {
	a := []identity.Attr{{Key: catalog.ServiceName, Value: name}, {Key: catalog.ServiceInstanceID, Value: id}}
	if ns != "" {
		a = append([]identity.Attr{{Key: catalog.ServiceNamespace, Value: ns}}, a...)
	}
	return fp(t, catalog.ServiceInstance, a...)
}

func podFP(t testing.TB, uid string) identity.Fingerprint {
	return fp(t, catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: uid})
}

func describe(recs []store.Record) string {
	s := ""
	for _, r := range recs {
		s += "\n  " + r.Kind.String() + " " + r.Layer.String() + " " + r.EventTime.Format("15:04:05")
		if r.Subject.Kind == store.SubjectEdge {
			s += " edge " + string(r.Subject.Relation)
		} else {
			s += " entity " + string(r.Subject.A.Type())
		}
	}
	return s
}

// wantRecords checks the records of one sighting against the subjects expected, in order,
// and that each is an Observe of this producer at tm with the TTL.
func wantRecords(t *testing.T, got []store.Record, tm time.Time, subjects ...store.Subject) {
	t.Helper()
	if len(got) != len(subjects) {
		t.Fatalf("%d records, want %d:%s", len(got), len(subjects), describe(got))
	}
	for i, r := range got {
		switch {
		case r.Subject != subjects[i]:
			t.Errorf("record %d: subject %+v, want %+v", i, r.Subject, subjects[i])
		case !r.EventTime.Equal(tm):
			t.Errorf("record %d: at %s, want %s", i, r.EventTime, tm)
		case r.Kind != lifecycle.Observe || r.TTL != ttl || r.Producer != testProducer || r.EventTimeBasis != store.BasisProducerEvent:
			t.Errorf("record %d: %v ttl %s producer %s basis %s", i, r.Kind, r.TTL, r.Producer, r.EventTimeBasis)
		case !r.Through.IsZero() || r.Seq != 0 || r.Boot != "":
			t.Errorf("record %d: through %v seq %d boot %q", i, r.Through, r.Seq, r.Boot)
		}
	}
}

// subjects of an instance with a bridge: its service, itself, instance_of and part_of.
func instanceSubjects(t *testing.T, ns, name, id, pod string) []store.Subject {
	inst, svc := instanceFP(t, ns, name, id), serviceFP(t, ns, name)
	out := []store.Subject{
		store.EntitySubject(svc), store.EntitySubject(inst),
		store.EdgeSubject(inst, svc, catalog.InstanceOf),
	}
	if pod != "" {
		out = append(out, store.EdgeSubject(inst, podFP(t, pod), catalog.PartOf))
	}
	return out
}

// An instance of a service in a pod: its service, the instance, instance_of, and the
// bridge to the pod by its uid, at the latest time of its points, in the layers the
// catalog records.
func TestAnInstanceInAPod(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	r := tr.Translate(resource(attrs("shop", "cart", "i-1", "2.1.0", "uid-1"),
		count(t0, t0.Add(time.Second), 4), count(t0.Add(time.Minute), t0.Add(time.Minute+2*time.Second), 9)), t0.Add(2*time.Minute))
	at := t0.Add(time.Minute + 2*time.Second)
	wantRecords(t, r.Records, at, instanceSubjects(t, "shop", "cart", "i-1", "uid-1")...)
	if len(r.Skipped) != 0 {
		t.Errorf("skipped %+v", r.Skipped)
	}
	layers := []catalog.Layer{catalog.L3, catalog.L2, catalog.L2, catalog.L2}
	for i, rec := range r.Records {
		if rec.Layer != layers[i] {
			t.Errorf("record %d in %s, want %s", i, rec.Layer, layers[i])
		}
		if err := rec.Validate(); err != nil {
			t.Error(err)
		}
	}
	// The service carries its description and the instance its version; the edges
	// carry nothing.
	for i, rec := range r.Records {
		want := ""
		switch i {
		case 0:
			want = `{"name":"cart","namespace":"shop"}`
		case 1:
			want = `{"service.version":"2.1.0"}`
		}
		if string(rec.Payload) != want {
			t.Errorf("record %d payload %q, want %q", i, rec.Payload, want)
		}
	}
	// A pod is never asserted.
	for _, rec := range r.Records {
		if rec.Subject.Kind == store.SubjectEntity && rec.Subject.A.Type() == catalog.K8sPod {
			t.Error("the pod is asserted")
		}
	}
}

// The instance ends at its last sighting plus the TTL, and says why.
func TestAnInstanceEndsByLivenessExpiry(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	var as []lifecycle.Assertion
	inst := instanceFP(t, "", "cart", "i-1")
	for i := 0; i < 3; i++ {
		tm := t0.Add(time.Duration(i) * time.Hour)
		for _, rec := range tr.Translate(resource(attrs("", "cart", "i-1", "", ""), count(tm, tm, 1)), tm).Records {
			if rec.Subject == store.EntitySubject(inst) {
				rec.Seq = uint64(len(as) + 1)
				as = append(as, rec.Assertion())
			}
		}
	}
	tl, err := lifecycle.Fold(as, lifecycle.Policy{})
	if err != nil {
		t.Fatal(err)
	}
	iv := tl.Existence()
	last := t0.Add(2 * time.Hour)
	if len(iv) != 1 || !iv[0].Start.Equal(t0) || !iv[0].End.Equal(last.Add(ttl)) || iv[0].EndSource != lifecycle.EndLivenessExpiry {
		t.Errorf("existence %+v, want [%s, %s) by liveness_expiry", iv, t0, last.Add(ttl))
	}
}

// A resource with no k8s.pod.uid writes the instance without a bridge, and says it is
// unplaced. A pod's IP or name is not a uid and joins nothing.
func TestNoPodUIDNoBridge(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	for name, extra := range map[string][]capture.KeyValue{
		"none":        nil,
		"pod ip only": {kv("k8s.pod.ip", "10.0.0.7")},
		"pod name":    {kv("k8s.pod.name", "cart-7d9f"), kv("host.name", "cart-7d9f")},
		"all but uid": {kv("k8s.pod.ip", "10.0.0.7"), kv("k8s.pod.name", "cart-7d9f"), kv("k8s.node.name", "n1"), kv("container.id", "abc")},
	} {
		res := resource(append(attrs("", "cart", "i-1", "", ""), extra...), count(t0, t0, 1))
		r := tr.Translate(res, t0)
		wantRecords(t, r.Records, t0, instanceSubjects(t, "", "cart", "i-1", "")...)
		if skipped(r, spancount.UnplacedNoPodUID) != 1 {
			t.Errorf("%s: skips %+v, want the instance counted as unplaced", name, r.Skipped)
		}
		for _, rec := range r.Records {
			if rec.Subject.Relation == catalog.PartOf || rec.Subject.A.Type() == catalog.K8sPod || rec.Subject.B.Type() == catalog.K8sPod {
				t.Errorf("%s: a record about a pod: %+v", name, rec.Subject)
			}
		}
	}
}

// A uid given twice with different values, a uid that is not a string and a blank one
// give no bridge. A uid given twice with the same value is that value.
func TestAnUnusablePodUIDGivesNoBridge(t *testing.T) {
	t.Parallel()
	one := int64(1)
	n := capture.Int64(one)
	base := attrs("", "cart", "i-1", "", "")
	for name, extra := range map[string][]capture.KeyValue{
		"twice":    {kv("k8s.pod.uid", "a"), kv("k8s.pod.uid", "b")},
		"not text": {{Key: "k8s.pod.uid", Value: capture.AnyValue{IntValue: &n}}},
		"blank":    {kv("k8s.pod.uid", "  ")},
		"empty":    {kv("k8s.pod.uid", "")},
	} {
		tr := newTranslator(t)
		r := tr.Translate(resource(append(slicesClone(base), extra...), count(t0, t0, 1)), t0)
		wantRecords(t, r.Records, t0, instanceSubjects(t, "", "cart", "i-1", "")...)
		if skipped(r, spancount.UnplacedBadPodUID) != 1 {
			t.Errorf("%s: skips %+v", name, r.Skipped)
		}
	}
	tr := newTranslator(t)
	r := tr.Translate(resource(append(slicesClone(base), kv("k8s.pod.uid", "a"), kv("k8s.pod.uid", "a")), count(t0, t0, 1)), t0)
	wantRecords(t, r.Records, t0, instanceSubjects(t, "", "cart", "i-1", "a")...)
}

func slicesClone(kvs []capture.KeyValue) []capture.KeyValue {
	return append([]capture.KeyValue(nil), kvs...)
}

// A StatefulSet's pod is created again with the same name and a new uid, and the instance
// id, which carries the pod's name, is the same: one instance, two bridges, each to its pod.
func TestAStatefulSetUIDChangeGivesTwoBridges(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	first := tr.Translate(resource(attrs("db", "pg", "pg-0", "", "uid-old"), count(t0, t0, 1)), t0)
	second := tr.Translate(resource(attrs("db", "pg", "pg-0", "", "uid-new"), count(t0.Add(time.Hour), t0.Add(time.Hour), 1)), t0.Add(time.Hour))
	inst := instanceFP(t, "db", "pg", "pg-0")
	bridges := map[identity.Fingerprint]bool{}
	for _, r := range append(first.Records, second.Records...) {
		if r.Subject.Relation == catalog.PartOf {
			if r.Subject.A != inst {
				t.Errorf("a bridge from another instance")
			}
			bridges[r.Subject.B] = true
		}
	}
	if len(bridges) != 2 || !bridges[podFP(t, "uid-old")] || !bridges[podFP(t, "uid-new")] {
		t.Errorf("bridges %v, want one to each pod", bridges)
	}
}

// A sidecar that is its own service instance, in the pod of another service: two
// instances, two bridges, to the one pod.
func TestASidecarInstanceSharesThePod(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	app := tr.Translate(resource(attrs("shop", "cart", "c-1", "", "uid-1"), count(t0, t0, 1)), t0)
	side := tr.Translate(resource(attrs("shop", "envoy", "e-1", "", "uid-1"), count(t0, t0, 1)), t0)
	var to []identity.Fingerprint
	var from []identity.Fingerprint
	for _, r := range append(app.Records, side.Records...) {
		if r.Subject.Relation == catalog.PartOf {
			from, to = append(from, r.Subject.A), append(to, r.Subject.B)
		}
	}
	if len(to) != 2 || to[0] != to[1] || to[0] != podFP(t, "uid-1") || from[0] == from[1] {
		t.Errorf("bridges from %v to %v, want two instances of one pod", from, to)
	}
}

// Identity is exact and from the resource only: a namespace is part of the service and
// of the instance; an empty namespace is none; names and ids are not trimmed.
func TestIdentityIsExact(t *testing.T) {
	t.Parallel()
	tr := newTranslator(t)
	subj := func(kvs []capture.KeyValue) (service, instance identity.Fingerprint) {
		r := tr.Translate(resource(kvs, count(t0, t0, 1)), t0)
		if len(r.Records) < 2 {
			t.Fatalf("no records for %v: %+v", kvs, r.Skipped)
		}
		return r.Records[0].Subject.A, r.Records[1].Subject.A
	}
	sNone, iNone := subj(attrs("", "cart", "i-1", "", ""))
	sEmpty, iEmpty := subj(append(attrs("", "cart", "i-1", "", ""), kv("service.namespace", "")))
	sNS, iNS := subj(attrs("shop", "cart", "i-1", "", ""))
	sID, iID := subj(attrs("", "cart", "i-2", "", ""))
	sPad, iPad := subj(attrs("", "cart ", "i-1", "", ""))
	if sNone != sEmpty || iNone != iEmpty {
		t.Error("an empty namespace is not none")
	}
	if sNone == sNS || iNone == iNS {
		t.Error("a namespace is not part of identity")
	}
	if sNone != sID || iNone == iID {
		t.Error("an instance id belongs to the instance and not the service")
	}
	if sNone == sPad || iNone == iPad {
		t.Error("a name was trimmed")
	}
	if sNone != serviceFP(t, "", "cart") || iNone != instanceFP(t, "", "cart", "i-1") {
		t.Error("identity differs from the catalog's")
	}
}

// What cannot be identified writes nothing and is counted: no service name, no instance
// id (the service alone would be a guess at an instance), an unusable namespace.
func TestWhatCannotBeIdentifiedWritesNothing(t *testing.T) {
	t.Parallel()
	one := capture.Int64(1)
	for name, tc := range map[string]struct {
		kvs    []capture.KeyValue
		reason string
	}{
		"no name":          {[]capture.KeyValue{kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
		"blank name":       {[]capture.KeyValue{kv("service.name", " "), kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
		"two names":        {[]capture.KeyValue{kv("service.name", "a"), kv("service.name", "b"), kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
		"name not text":    {[]capture.KeyValue{{Key: "service.name", Value: capture.AnyValue{IntValue: &one}}, kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
		"no instance id":   {[]capture.KeyValue{kv("service.name", "a")}, spancount.SkipNoInstanceID},
		"empty id":         {[]capture.KeyValue{kv("service.name", "a"), kv("service.instance.id", "")}, spancount.SkipNoInstanceID},
		"two ids":          {[]capture.KeyValue{kv("service.name", "a"), kv("service.instance.id", "i"), kv("service.instance.id", "j")}, spancount.SkipNoInstanceID},
		"blank namespace":  {[]capture.KeyValue{kv("service.name", "a"), kv("service.instance.id", "i"), kv("service.namespace", " ")}, spancount.SkipBadNamespace},
		"two namespaces":   {[]capture.KeyValue{kv("service.name", "a"), kv("service.instance.id", "i"), kv("service.namespace", "x"), kv("service.namespace", "y")}, spancount.SkipBadNamespace},
		"invalid utf-8":    {[]capture.KeyValue{kv("service.name", "a\xff"), kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
		"no attributes":    {nil, spancount.SkipNoServiceName},
		"an enormous name": {[]capture.KeyValue{kv("service.name", strings.Repeat("a", 5000)), kv("service.instance.id", "i")}, spancount.SkipNoServiceName},
	} {
		r := newTranslator(t).Translate(resource(tc.kvs, count(t0, t0, 1)), t0)
		if len(r.Records) != 0 || skipped(r, tc.reason) != 1 {
			t.Errorf("%s: %d records, skips %+v, want none and one %q", name, len(r.Records), r.Skipped, tc.reason)
		}
	}
}

// The same service has the same identity whichever writer asserts it: the service
// entity of the span counter is the one the service graph writes for a call to or from
// it. Both writers assert it, because an edge is reported only while both its endpoints
// exist.
func TestBothWritersAssertTheSameServiceEntity(t *testing.T) {
	t.Parallel()
	sc := newTranslator(t)
	spans := sc.Translate(resource(attrs("shop", "cart", "i-1", "", "u"), count(t0, t0, 1)), t0)
	g, err := svcgraph.New(svcgraph.Config{Producer: "svcgraph/test", Resolver: resolver(), Expiry: ttl})
	if err != nil {
		t.Fatal(err)
	}
	p := otlpmetrics.SumPoint{
		Scope: svcgraph.Scope, Metric: svcgraph.Metric, Temporality: otlpmetrics.Delta, Monotonic: true,
		StartTimeUnixNano: uint64(t0.UnixNano()), TimeUnixNano: uint64(t0.UnixNano()), Value: 1,
		Attributes: []capture.KeyValue{
			kv("client", "cart"), kv("server", "db"), kv("connection_type", ""), kv("failed", "false"),
			kv("client_service.namespace", "shop"),
		},
	}
	graph := g.Translate(resource(nil, p), t0)
	want := store.EntitySubject(serviceFP(t, "shop", "cart"))
	has := func(recs []store.Record) bool {
		for _, r := range recs {
			if r.Subject == want && r.Layer == catalog.L3 && r.Kind == lifecycle.Observe {
				return true
			}
		}
		return false
	}
	if !has(spans.Records) || !has(graph.Records) {
		t.Errorf("the service entity is asserted by the span counter: %v, by the service graph: %v", has(spans.Records), has(graph.Records))
	}
	// And with the same description, byte for byte.
	payloadOf := func(recs []store.Record) []byte {
		for _, r := range recs {
			if r.Subject == want {
				return r.Payload
			}
		}
		return nil
	}
	sp, gp := payloadOf(spans.Records), payloadOf(graph.Records)
	if !bytes.Equal(sp, gp) || !bytes.Equal(sp, otlpmetrics.ServiceDescription("shop", "cart")) || string(sp) != `{"name":"cart","namespace":"shop"}` {
		t.Errorf("the span counter describes the service as %q, the service graph as %q", sp, gp)
	}
	// The producers differ, because the two cadences differ and a producer has one TTL.
	if spans.Records[0].Producer == graph.Records[0].Producer {
		t.Error("the two writers share a producer")
	}
}

// Points that are not a delta count of spans are counted, and write nothing.
func TestWhatIsNotASpanCountIsCounted(t *testing.T) {
	t.Parallel()
	good := count(t0, t0, 1)
	cumulative, unspecified, zero, other, nonMono := good, good, good, good, good
	cumulative.Temporality = otlpmetrics.Cumulative
	unspecified.Temporality = otlpmetrics.TemporalityUnspecified
	zero.Value = 0
	other.Metric = "trace.span.errors"
	nonMono.Monotonic = false
	res := resource(attrs("", "cart", "i-1", "", ""), cumulative, unspecified, zero, other, nonMono)
	res.DroppedNotSum, res.DroppedNonMonotonic = 3, 2
	r := newTranslator(t).Translate(res, t0)
	if len(r.Records) != 0 {
		t.Errorf("%d records:%s", len(r.Records), describe(r.Records))
	}
	for reason, n := range map[string]int{
		spancount.SkipNotDelta: 2, spancount.SkipNoSpans: 1, spancount.SkipNotCounter: 1,
		otlpmetrics.SkipNonMonotonic: 3, otlpmetrics.SkipNotSum: 3,
	} {
		if skipped(r, reason) != n {
			t.Errorf("%q: %d, want %d (all %+v)", reason, skipped(r, reason), n, r.Skipped)
		}
	}
	// Among points that are counted, the one that is a count is used.
	r = newTranslator(t).Translate(resource(attrs("", "cart", "i-1", "", ""), cumulative, count(t0, t0.Add(time.Minute), 2), zero), t0.Add(time.Hour))
	wantRecords(t, r.Records, t0.Add(time.Minute), instanceSubjects(t, "", "cart", "i-1", "")...)
}

// A point more than five minutes ahead of the time its request was received is skipped
// and counted, never replaced by the receipt time; the other points are used.
func TestAPointAheadOfReceiptIsSkipped(t *testing.T) {
	t.Parallel()
	received := t0.Add(time.Minute)
	ahead := received.Add(otlpmetrics.MaxClockAhead + time.Nanosecond)
	edge := received.Add(otlpmetrics.MaxClockAhead)
	kvs := attrs("", "cart", "i-1", "", "")
	r := newTranslator(t).Translate(resource(kvs, count(t0, ahead, 1)), received)
	if len(r.Records) != 0 || skipped(r, otlpmetrics.SkipClockAhead) != 1 {
		t.Errorf("a point ahead: %d records, skips %+v", len(r.Records), r.Skipped)
	}
	r = newTranslator(t).Translate(resource(kvs, count(t0, ahead, 1), count(t0, t0, 1)), received)
	wantRecords(t, r.Records, t0, instanceSubjects(t, "", "cart", "i-1", "")...)
	if skipped(r, otlpmetrics.SkipClockAhead) != 1 {
		t.Errorf("skips %+v", r.Skipped)
	}
	r = newTranslator(t).Translate(resource(kvs, count(t0, edge, 1)), received)
	wantRecords(t, r.Records, edge, instanceSubjects(t, "", "cart", "i-1", "")...)
	// With no receipt time given, none is ahead.
	r = newTranslator(t).Translate(resource(kvs, count(t0, t0.Add(100*time.Hour), 1)), time.Time{})
	if len(r.Records) == 0 {
		t.Error("a point was judged ahead of a receipt time that was not given")
	}
	// A time of zero or outside the range is no time.
	zero := count(t0, t0, 1)
	zero.TimeUnixNano = 0
	huge := count(t0, t0, 1)
	huge.TimeUnixNano = 1 << 63
	r = newTranslator(t).Translate(resource(kvs, zero, huge), received)
	if len(r.Records) != 0 || skipped(r, otlpmetrics.SkipNoTime) != 2 {
		t.Errorf("points with no time: %d records, skips %+v", len(r.Records), r.Skipped)
	}
}

// A version is the instance's description: a new version is a new payload, and so a new
// run of the instance, which is the deploy signal. An absent or empty version is no
// payload, and an unusable one is counted.
func TestTheVersionIsTheInstancesPayload(t *testing.T) {
	t.Parallel()
	payload := func(version string) []byte {
		tr := newTranslator(t)
		return tr.Translate(resource(attrs("", "cart", "i-1", version, ""), count(t0, t0, 1)), t0).Records[1].Payload
	}
	if got := string(payload("1.0")); got != `{"service.version":"1.0"}` {
		t.Errorf("payload %q", got)
	}
	if string(payload("1.0")) == string(payload("1.1")) {
		t.Error("two versions, one payload")
	}
	if got := payload(""); len(got) != 0 {
		t.Errorf("an absent version gave %q", got)
	}
	if got := string(payload("<&>")); got != `{"service.version":"<&>"}` {
		t.Errorf("the payload escapes: %q", got)
	}
	one := capture.Int64(1)
	for name, v := range map[string][]capture.KeyValue{
		"empty":    {kv("service.version", "")},
		"not text": {{Key: "service.version", Value: capture.AnyValue{IntValue: &one}}},
		"twice":    {kv("service.version", "1"), kv("service.version", "2")},
		"long":     {kv("service.version", strings.Repeat("v", 300))},
		"bad utf8": {kv("service.version", "\xff")},
	} {
		r := newTranslator(t).Translate(resource(append(attrs("", "cart", "i-1", "", ""), v...), count(t0, t0, 1)), t0)
		wantRecords(t, r.Records, t0, instanceSubjects(t, "", "cart", "i-1", "")...)
		if len(r.Records[1].Payload) != 0 {
			t.Errorf("%s: payload %q", name, r.Records[1].Payload)
		}
		if want := name != "empty"; (skipped(r, spancount.RefusedVersion) == 1) != want {
			t.Errorf("%s: skips %+v", name, r.Skipped)
		}
	}
}

// Nothing a call returns depends on an earlier call.
func TestTheTranslatorKeepsNoState(t *testing.T) {
	t.Parallel()
	a, b := newTranslator(t), newTranslator(t)
	res := resource(attrs("shop", "cart", "i-1", "2", "u1"), count(t0, t0, 1))
	for range 3 {
		a.Translate(resource(attrs("x", "y", "z", "", ""), count(t0, t0, 5)), t0)
	}
	if ra, rb := a.Translate(res, t0), b.Translate(res, t0); !reflect.DeepEqual(ra, rb) {
		t.Error("two translators gave different results")
	}
	// The same call twice is the same sighting twice: it is the coalescer that folds them.
	if r1, r2 := a.Translate(res, t0), a.Translate(res, t0); !reflect.DeepEqual(r1, r2) {
		t.Error("a repeated call gave a different result")
	}
}

func TestConfigIsChecked(t *testing.T) {
	t.Parallel()
	good := spancount.Config{Producer: testProducer, Resolver: resolver(), TTL: time.Hour}
	if _, err := spancount.New(good); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*spancount.Config){
		"no producer": func(c *spancount.Config) { c.Producer = "" },
		"no resolver": func(c *spancount.Config) { c.Resolver = nil },
		"no ttl":      func(c *spancount.Config) { c.TTL = 0 },
		"negative":    func(c *spancount.Config) { c.TTL = -time.Hour },
		"huge":        func(c *spancount.Config) { c.TTL = 400 * 24 * time.Hour },
	} {
		c := good
		mut(&c)
		if _, err := spancount.New(c); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	tr, _ := spancount.New(good)
	if tr.TTL() != time.Hour {
		t.Errorf("ttl %s", tr.TTL())
	}
}

// The producer of a cluster whose name has the longest length the store allows is a name
// the store writes, and the source is never renamed.
func TestTheLongestProducerIsWritable(t *testing.T) {
	t.Parallel()
	p := lifecycle.Producer(spancount.Source + "/" + strings.Repeat("c", store.MaxClusterNameLen))
	if err := store.WritableProducer(p); err != nil {
		t.Error(err)
	}
	if spancount.Source != "spancount" {
		t.Errorf("the source is %q: it is never renamed", spancount.Source)
	}
}

// The translator never reads a clock: no non-test file of the package names a function
// of package time that reads or waits on one.
func TestTheTranslatorNeverReadsAClock(t *testing.T) {
	t.Parallel()
	banned := map[string]bool{
		"Now": true, "Since": true, "Until": true, "Sleep": true, "After": true, "Tick": true,
		"NewTimer": true, "NewTicker": true, "AfterFunc": true,
	}
	files, err := filepath.Glob("*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v, %d files", err, len(files))
	}
	checked := 0
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		checked++
		f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == "time" && banned[sel.Sel.Name] {
				t.Errorf("%s uses time.%s", name, sel.Sel.Name)
			}
			return true
		})
	}
	if checked < 1 {
		t.Errorf("only %d source files were checked", checked)
	}
}

// Every record of a grid of resources is a record a store accepts, in the layer the
// catalog records for its subject: the service in L3, and the instance and its two edges
// in L2.
func TestEveryRecordIsWhatTheCatalogAndTheStoreExpect(t *testing.T) {
	t.Parallel()
	cat := catalog.Default()
	for _, ns := range []string{"", "shop"} {
		for _, pod := range []string{"", "uid-1"} {
			for _, version := range []string{"", "3.1"} {
				r := newTranslator(t).Translate(resource(attrs(ns, "cart", "i-1", version, pod), count(t0, t0, 1)), t0)
				for _, rec := range r.Records {
					if err := rec.Validate(); err != nil {
						t.Fatal(err)
					}
					if rec.Subject.Kind == store.SubjectEdge {
						rl, ok := cat.Relation(rec.Subject.Relation)
						if !ok || rec.Layer != rl.Layer() || !rl.Allows(rec.Subject.A.Type(), rec.Subject.B.Type()) {
							t.Errorf("edge %s %s -> %s in %s", rec.Subject.Relation, rec.Subject.A.Type(), rec.Subject.B.Type(), rec.Layer)
						}
						if rec.Subject.Relation == catalog.PartOf && rec.Subject.B.Type() != catalog.K8sPod {
							t.Errorf("a part_of to a %s", rec.Subject.B.Type())
						}
						if len(rec.Payload) != 0 {
							t.Errorf("edge %s has payload %q", rec.Subject.Relation, rec.Payload)
						}
						continue
					}
					e, ok := cat.Entity(rec.Subject.A.Type())
					if !ok || rec.Layer != e.Layer() {
						t.Errorf("entity %s in %s", rec.Subject.A.Type(), rec.Layer)
					}
					// A service carries its description and an instance its version, if
					// it has one.
					var want []byte
					switch rec.Subject.A.Type() {
					case catalog.Service:
						want = otlpmetrics.ServiceDescription(ns, "cart")
					case catalog.ServiceInstance:
						if version != "" {
							want = []byte(`{"service.version":"` + version + `"}`)
						}
					}
					if !bytes.Equal(rec.Payload, want) {
						t.Errorf("%s (namespace %q, version %q): payload %q, want %q", rec.Subject.A.Type(), ns, version, rec.Payload, want)
					}
				}
			}
		}
	}
}
