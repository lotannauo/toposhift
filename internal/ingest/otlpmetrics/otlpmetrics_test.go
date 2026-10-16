package otlpmetrics_test

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

var t0 = time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func kv(k, v string) capture.KeyValue { return capture.KeyValue{Key: k, Value: sv(v)} }

// The clock-ahead bound is the one the log translators use.
func TestMaxClockAheadIsTheLogTranslators(t *testing.T) {
	t.Parallel()
	for name, d := range map[string]time.Duration{"k8scluster": k8scluster.MaxClockAhead, "nodeagent": nodeagent.MaxClockAhead} {
		if d != otlpmetrics.MaxClockAhead {
			t.Errorf("%s: %s, otlpmetrics %s", name, d, otlpmetrics.MaxClockAhead)
		}
	}
}

func TestTime(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		n  uint64
		ok bool
	}{
		{0, false},
		{1, true},
		{uint64(t0.UnixNano()), true},
		{math.MaxInt64, true},
		{math.MaxInt64 + 1, false},
		{math.MaxUint64, false},
	} {
		got, ok := otlpmetrics.Time(tc.n)
		if ok != tc.ok {
			t.Errorf("%d: ok %v, want %v", tc.n, ok, tc.ok)
		}
		if ok && got.UnixNano() != int64(tc.n) {
			t.Errorf("%d: %v", tc.n, got)
		}
		if ok && got.Location() != time.UTC {
			t.Errorf("%d: not UTC", tc.n)
		}
	}
}

func TestTemporalityNames(t *testing.T) {
	t.Parallel()
	for tm, want := range map[otlpmetrics.Temporality]string{
		otlpmetrics.TemporalityUnspecified: "unspecified", otlpmetrics.Delta: "delta", otlpmetrics.Cumulative: "cumulative", 9: "invalid",
	} {
		if tm.String() != want {
			t.Errorf("%d: %q, want %q", tm, tm.String(), want)
		}
	}
	// The values are OTLP's.
	if otlpmetrics.Delta != 1 || otlpmetrics.Cumulative != 2 {
		t.Error("temporality values differ from OTLP's")
	}
}

// A value is identity only if it is a string, given once.
func TestCollectAndGet(t *testing.T) {
	t.Parallel()
	n := capture.Int64(7)
	b := true
	a := otlpmetrics.Collect([]capture.KeyValue{
		kv("one", "x"),
		kv("same", "y"), kv("same", "y"),
		kv("differ", "a"), kv("differ", "b"),
		{Key: "int", Value: capture.AnyValue{IntValue: &n}},
		{Key: "mixed", Value: sv("s")},
		{Key: "mixed", Value: capture.AnyValue{IntValue: &n}},
		{Key: "mixed2", Value: capture.AnyValue{IntValue: &n}},
		{Key: "mixed2", Value: sv("s")},
		{Key: "bool", Value: capture.AnyValue{BoolValue: &b}},
		{Key: "empty", Value: capture.AnyValue{}},
		kv("blank", ""),
	})
	for key, want := range map[string]struct {
		v  string
		st otlpmetrics.Status
	}{
		"one": {"x", otlpmetrics.StatusOK}, "same": {"y", otlpmetrics.StatusOK},
		"differ": {"", otlpmetrics.StatusBad}, "int": {"", otlpmetrics.StatusBad},
		"mixed": {"", otlpmetrics.StatusBad}, "mixed2": {"", otlpmetrics.StatusBad},
		"bool": {"", otlpmetrics.StatusBad}, "empty": {"", otlpmetrics.StatusBad},
		"blank": {"", otlpmetrics.StatusOK}, "missing": {"", otlpmetrics.StatusAbsent},
	} {
		v, st := a.Get(key)
		if v != want.v || st != want.st {
			t.Errorf("%s: %q, %d; want %q, %d", key, v, st, want.v, want.st)
		}
	}
	// A value is exactly as received.
	if v, _ := otlpmetrics.Collect([]capture.KeyValue{kv("k", " V ")}).Get("k"); v != " V " {
		t.Errorf("value %q was changed", v)
	}
	if otlpmetrics.Collect(nil) == nil {
		t.Error("Collect(nil) is nil")
	}
}

func point(at time.Time, v float64) otlpmetrics.SumPoint {
	return otlpmetrics.SumPoint{Monotonic: true, TimeUnixNano: uint64(at.UnixNano()), Value: v}
}

// Check applies, in order, monotonic, a usable time, a usable value, and the clock.
func TestCheck(t *testing.T) {
	t.Parallel()
	received := t0.Add(time.Minute)
	bad := func(p otlpmetrics.SumPoint, mut func(*otlpmetrics.SumPoint)) otlpmetrics.SumPoint { mut(&p); return p }
	for name, tc := range map[string]struct {
		p      otlpmetrics.SumPoint
		recv   time.Time
		reason string
	}{
		"fine":               {point(t0, 1), received, ""},
		"zero value":         {point(t0, 0), received, ""},
		"not monotonic":      {bad(point(t0, 1), func(p *otlpmetrics.SumPoint) { p.Monotonic = false }), received, otlpmetrics.SkipNonMonotonic},
		"no time":            {bad(point(t0, 1), func(p *otlpmetrics.SumPoint) { p.TimeUnixNano = 0 }), received, otlpmetrics.SkipNoTime},
		"time out of range":  {bad(point(t0, 1), func(p *otlpmetrics.SumPoint) { p.TimeUnixNano = 1 << 63 }), received, otlpmetrics.SkipNoTime},
		"negative":           {point(t0, -1), received, otlpmetrics.SkipBadValue},
		"nan":                {point(t0, math.NaN()), received, otlpmetrics.SkipBadValue},
		"inf":                {point(t0, math.Inf(1)), received, otlpmetrics.SkipBadValue},
		"at the bound":       {point(received.Add(otlpmetrics.MaxClockAhead), 1), received, ""},
		"past the bound":     {point(received.Add(otlpmetrics.MaxClockAhead+time.Nanosecond), 1), received, otlpmetrics.SkipClockAhead},
		"no receipt":         {point(t0.Add(1000*time.Hour), 1), time.Time{}, ""},
		"receipt too early":  {point(t0, 1), time.Unix(-5, 0), ""},
		"receipt too late":   {point(t0, 1), time.Unix(0, math.MaxInt64).Add(time.Hour), ""},
		"monotonic first":    {bad(point(t0, math.NaN()), func(p *otlpmetrics.SumPoint) { p.Monotonic = false }), received, otlpmetrics.SkipNonMonotonic},
		"time before value":  {bad(point(t0, -1), func(p *otlpmetrics.SumPoint) { p.TimeUnixNano = 0 }), received, otlpmetrics.SkipNoTime},
		"value before clock": {point(received.Add(time.Hour), -1), received, otlpmetrics.SkipBadValue},
	} {
		at, reason := otlpmetrics.Check(tc.p, tc.recv)
		if reason != tc.reason {
			t.Errorf("%s: reason %q, want %q", name, reason, tc.reason)
		}
		if reason == "" && at.UnixNano() != int64(tc.p.TimeUnixNano) {
			t.Errorf("%s: time %v", name, at)
		}
	}
}

func TestTallyAndDropped(t *testing.T) {
	t.Parallel()
	var tl otlpmetrics.Tally
	if tl.Counts() != nil {
		t.Error("an empty tally has counts")
	}
	tl.Add("b")
	tl.AddN("a", 3)
	tl.AddN("c", 0)
	tl.AddN("d", -2)
	tl.Add("b")
	want := []otlpmetrics.Count{{Reason: "a", Count: 3}, {Reason: "b", Count: 2}}
	if got := tl.Counts(); !reflect.DeepEqual(got, want) {
		t.Errorf("counts %+v, want %+v", got, want)
	}
	var d otlpmetrics.Tally
	otlpmetrics.Dropped(&d, otlpmetrics.Resource{DroppedNotSum: 2, DroppedNonMonotonic: 5})
	if got := d.Counts(); len(got) != 2 {
		t.Errorf("dropped %+v", got)
	}
	var none otlpmetrics.Tally
	otlpmetrics.Dropped(&none, otlpmetrics.Resource{})
	if none.Counts() != nil {
		t.Error("a resource that dropped nothing counted something")
	}
}

func TestUsableID(t *testing.T) {
	t.Parallel()
	for s, want := range map[string]bool{
		"x": true, " x ": true, "": false, " ": false, "\t\n": false, "\xff": false,
		strings.Repeat("a", identity.MaxValueLen): true, strings.Repeat("a", identity.MaxValueLen+1): false,
	} {
		if got := otlpmetrics.UsableID(s); got != want {
			t.Errorf("%q: %v", s[:min(len(s), 12)], got)
		}
	}
	if !otlpmetrics.UsableNamespace("") || otlpmetrics.UsableNamespace(" ") || !otlpmetrics.UsableNamespace("ns") {
		t.Error("UsableNamespace")
	}
}

// Fingerprints are the catalog's: a service is (namespace or none, name), an instance
// adds its id, and a pod is its uid.
func TestFingerprints(t *testing.T) {
	t.Parallel()
	r := identity.NewResolver(catalog.Default())
	f := otlpmetrics.NewFingerprints(r)
	resolve := func(typ catalog.EntityType, attrs ...identity.Attr) identity.Fingerprint {
		id, err := r.Resolve(typ, attrs)
		if err != nil {
			t.Fatal(err)
		}
		return id.Fingerprint()
	}
	got, err := f.Service("", "cart")
	if err != nil || got != resolve(catalog.Service, identity.Attr{Key: catalog.ServiceName, Value: "cart"}) {
		t.Errorf("service with no namespace: %v, %v", got, err)
	}
	got, _ = f.Service("shop", "cart")
	if got != resolve(catalog.Service, identity.Attr{Key: catalog.ServiceNamespace, Value: "shop"}, identity.Attr{Key: catalog.ServiceName, Value: "cart"}) {
		t.Error("service with a namespace")
	}
	got, _ = f.Instance("shop", "cart", "i-1")
	if got != resolve(catalog.ServiceInstance, identity.Attr{Key: catalog.ServiceNamespace, Value: "shop"},
		identity.Attr{Key: catalog.ServiceName, Value: "cart"}, identity.Attr{Key: catalog.ServiceInstanceID, Value: "i-1"}) {
		t.Error("instance")
	}
	got, _ = f.Pod("u")
	if got != resolve(catalog.K8sPod, identity.Attr{Key: catalog.K8sPodUID, Value: "u"}) {
		t.Error("pod")
	}
	if _, err := f.Service("", " "); err == nil {
		t.Error("a blank service name resolved")
	}
	if _, err := f.Pod(""); err == nil {
		t.Error("an empty pod uid resolved")
	}
	if otlpmetrics.KeyServiceName != "service.name" || otlpmetrics.KeyServiceNamespace != "service.namespace" ||
		otlpmetrics.KeyServiceInstanceID != "service.instance.id" || otlpmetrics.KeyPodUID != "k8s.pod.uid" {
		t.Error("an attribute key")
	}
}

// A service's description is its name and, when it has one, its namespace, as the
// canonical JSON a payload is: keys in that order, no HTML escaping, no trailing
// newline. Decoding it gives back the values its fingerprint is made of.
func TestServiceDescription(t *testing.T) {
	t.Parallel()
	f := otlpmetrics.NewFingerprints(identity.NewResolver(catalog.Default()))
	for _, tc := range []struct{ ns, name, want string }{
		{"", "web", `{"name":"web"}`},
		{"shop", "web", `{"name":"web","namespace":"shop"}`},
		{"", "<&>", `{"name":"<&>"}`},
		{"a>b", "<&>", `{"name":"<&>","namespace":"a>b"}`},
		{"", "a\"b\\c\x01", `{"name":"a\"b\\c\u0001"}`},
		{"n\n", "\u00e9 \u2028", "{\"name\":\"\u00e9 \\u2028\",\"namespace\":\"n\\n\"}"},
	} {
		got := otlpmetrics.ServiceDescription(tc.ns, tc.name)
		if string(got) != tc.want {
			t.Errorf("(%q, %q): %q, want %q", tc.ns, tc.name, got, tc.want)
		}
		var d struct{ Name, Namespace string }
		if err := json.Unmarshal(got, &d); err != nil {
			t.Errorf("(%q, %q): %v", tc.ns, tc.name, err)
			continue
		}
		if d.Name != tc.name || d.Namespace != tc.ns {
			t.Errorf("(%q, %q): decodes to %+v", tc.ns, tc.name, d)
		}
		want, err := f.Service(tc.ns, tc.name)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := f.Service(d.Namespace, d.Name); err != nil || got != want {
			t.Errorf("(%q, %q): the decoded values give another fingerprint (%v)", tc.ns, tc.name, err)
		}
	}
	// A description is a pure function of its values, and a different service has a
	// different one.
	if string(otlpmetrics.ServiceDescription("shop", "web")) == string(otlpmetrics.ServiceDescription("", "web")) {
		t.Error("a namespace changed nothing")
	}
}

// The layer of a record is the catalog's: an entity's type's, an edge's relation's.
func TestLayerAndObserve(t *testing.T) {
	t.Parallel()
	r := identity.NewResolver(catalog.Default())
	f := otlpmetrics.NewFingerprints(r)
	svc, _ := f.Service("", "a")
	svc2, _ := f.Service("", "b")
	inst, _ := f.Instance("", "a", "i")
	pod, _ := f.Pod("u")
	cat := catalog.Default()
	for _, tc := range []struct {
		subj store.Subject
		want catalog.Layer
	}{
		{store.EntitySubject(svc), catalog.L3},
		{store.EntitySubject(inst), catalog.L2},
		{store.EntitySubject(pod), catalog.L2},
		{store.EdgeSubject(svc, svc2, catalog.DependsOn), catalog.L3},
		{store.EdgeSubject(inst, svc, catalog.InstanceOf), catalog.L2},
		{store.EdgeSubject(inst, pod, catalog.PartOf), catalog.L2},
	} {
		got := otlpmetrics.Layer(tc.subj)
		if got != tc.want {
			t.Errorf("%+v: %s, want %s", tc.subj, got, tc.want)
		}
		if tc.subj.Kind == store.SubjectEdge {
			rel, _ := cat.Relation(tc.subj.Relation)
			if got != rel.Layer() {
				t.Errorf("%s: %s, the catalog says %s", tc.subj.Relation, got, rel.Layer())
			}
		}
	}
	payload := []byte(`{"a":"b"}`)
	rec := otlpmetrics.Observe("svcgraph/x", store.EntitySubject(svc), t0, store.BasisObserved, time.Hour, payload)
	payload[2] = 'z'
	if string(rec.Payload) != `{"a":"b"}` {
		t.Error("the payload was not copied")
	}
	if rec.Kind != lifecycle.Observe || rec.Layer != catalog.L3 || rec.TTL != time.Hour || !rec.EventTime.Equal(t0) ||
		rec.EventTimeBasis != store.BasisObserved || rec.Producer != "svcgraph/x" || !rec.Through.IsZero() {
		t.Errorf("%+v", rec)
	}
	if err := rec.Validate(); err != nil {
		t.Error(err)
	}
	if rec := otlpmetrics.Observe("p/x", store.EntitySubject(svc), t0, store.BasisObserved, time.Hour, nil); rec.Payload != nil {
		t.Errorf("an empty payload is %q", rec.Payload)
	}
}

// A relation that is derived is not stored, and a writer must not ask for its layer.
func TestLayerOfADerivedRelationPanics(t *testing.T) {
	t.Parallel()
	f := otlpmetrics.NewFingerprints(identity.NewResolver(catalog.Default()))
	a, _ := f.Service("", "a")
	b, _ := f.Service("", "b")
	defer func() {
		if recover() == nil {
			t.Error("no panic")
		}
	}()
	otlpmetrics.Layer(store.EdgeSubject(a, b, catalog.SameAs))
}
