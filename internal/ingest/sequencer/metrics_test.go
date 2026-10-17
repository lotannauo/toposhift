package sequencer_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// metricsHarness is a sequencer over a memstore, with its own TTLs, so that the
// two producers' are told apart.
func metricsHarness(t *testing.T, opts ...option) (*harness, *fakeStore) {
	t.Helper()
	st := &fakeStore{inner: newMem(t)}
	opts = append([]option{withConfig(func(c *sequencer.Config) {
		c.DependencyExpiry, c.InstanceTTL = 3*time.Hour, 2*time.Hour
	})}, opts...)
	return newHarness(t, st, opts...), st
}

// A series seen through its warm-up and then increased is a sighting: the
// service of each side, then depends_on, of the producer svcgraph/<cluster>,
// with the dependency expiry as TTL, observed, at the point's time, in the layers
// the catalog says. A span count resource gives the service, the instance and the
// edges of the instance, of the producer spancount/<cluster>, with the instance
// TTL, producer_event.
func TestMetricsRecordsOfASightingAndOfAnInstance(t *testing.T) {
	h, st := metricsHarness(t)
	h.start()
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	if n := len(st.records()); n != 0 {
		t.Fatalf("the first point of a series wrote %d records", n)
	}
	h.setNow(time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(time.Minute), 2)))

	a, b := svcFP(t, "shop", "a"), svcFP(t, "shop", "b")
	graph := fromProducer(st.records(), "svcgraph/c")
	if len(graph) != 3 {
		t.Fatalf("%d service graph records, want 3: %v", len(graph), showAll(graph))
	}
	wantSubjects := []store.Subject{store.EntitySubject(a), store.EntitySubject(b), store.EdgeSubject(a, b, catalog.DependsOn)}
	for i, r := range graph {
		wantPayload := ""
		if i < 2 {
			wantPayload = string(otlpmetrics.ServiceDescription("shop", []string{"a", "b"}[i]))
		}
		switch {
		case r.Subject != wantSubjects[i]:
			t.Errorf("record %d: subject %v, want %v", i, r.Subject, wantSubjects[i])
		case r.Producer != "svcgraph/c" || r.Kind != lifecycle.Observe:
			t.Errorf("record %d: %s", i, show(r))
		case r.TTL != 3*time.Hour:
			t.Errorf("record %d: TTL %s, want the dependency expiry 3h", i, r.TTL)
		case r.EventTimeBasis != store.BasisObserved:
			t.Errorf("record %d: basis %s", i, r.EventTimeBasis)
		case !r.EventTime.Equal(at(time.Minute)):
			t.Errorf("record %d: at %s, want the point's time", i, r.EventTime.Sub(t0))
		case r.Layer != catalog.L3:
			t.Errorf("record %d: layer %s, want L3", i, r.Layer)
		case string(r.Payload) != wantPayload:
			t.Errorf("record %d: payload %q, want %q", i, r.Payload, wantPayload)
		}
	}

	h.setNow(2 * time.Minute)
	h.sendMOK(otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		spanRes("c", "shop", "a", "a-1", "pod-uid-1", spanPoint(at(2*time.Minute), 3)),
	}})
	spans := fromProducer(st.records(), "spancount/c")
	pod := podFP(t, "pod-uid-1")
	inst := instFP(t, "shop", "a", "a-1")
	want := []struct {
		sub   store.Subject
		layer catalog.Layer
		desc  string
	}{
		{store.EntitySubject(a), catalog.L3, string(otlpmetrics.ServiceDescription("shop", "a"))},
		{store.EntitySubject(inst), catalog.L2, ""},
		{store.EdgeSubject(inst, a, catalog.InstanceOf), catalog.L2, ""},
		{store.EdgeSubject(inst, pod, catalog.PartOf), catalog.L2, ""},
	}
	if len(spans) != len(want) {
		t.Fatalf("%d span count records, want %d: %v", len(spans), len(want), showAll(spans))
	}
	for i, r := range spans {
		switch {
		case r.Subject != want[i].sub || r.Layer != want[i].layer:
			t.Errorf("record %d: %s in %s, want %v in %s", i, show(r), r.Layer, want[i].sub, want[i].layer)
		case r.Producer != "spancount/c" || r.TTL != 2*time.Hour:
			t.Errorf("record %d: producer %s TTL %s, want spancount/c and the instance TTL 2h", i, r.Producer, r.TTL)
		case r.EventTimeBasis != store.BasisProducerEvent || !r.EventTime.Equal(at(2*time.Minute)):
			t.Errorf("record %d: basis %s at %s", i, r.EventTimeBasis, r.EventTime.Sub(t0))
		case string(r.Payload) != want[i].desc:
			t.Errorf("record %d: payload %q, want %q", i, r.Payload, want[i].desc)
		}
	}
}

// firstWith is a fake store's injection that fails the first write holding a
// depends_on record. With stored, the batch is written first, so the write
// returns an error although its batch is stored.
func firstWith(stored bool) (func(int, []store.Record) (bool, error), *atomic.Bool) {
	var fired atomic.Bool
	return func(_ int, batch []store.Record) (bool, error) {
		if len(dependsOn(batch)) > 0 && fired.CompareAndSwap(false, true) {
			return stored, errInjected
		}
		return true, nil
	}, &fired
}

// A group whose write failed and was not stored is answered 503, and the
// collector's retry of the same request is sighted: the service graph translator
// is rolled back to what the last good group left, neither made anew (the retry
// would be a first point and be swallowed by the warm-up) nor left with the
// failed group's changes (the retry would be a duplicate).
func TestMetricsRetryAfterAFailedWrite(t *testing.T) {
	h, st := metricsHarness(t)
	inject, fired := firstWith(false)
	st.inject = inject
	h.start()
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	h.setNow(time.Minute)
	req := mreq("c", ab.cum(at(time.Minute), 2))
	if _, err := h.sendM(req); !errors.Is(err, otlphttp.ErrBusy) || !fired.Load() {
		t.Fatalf("the failed write: %v, want 503", err)
	}
	if n := len(dependsOn(st.records())); n != 0 {
		t.Fatalf("%d depends_on records after a write that was not stored", n)
	}
	if got := counter(h, "svcgraph.sightings"); got != 0 {
		t.Errorf("sightings %d counted for a group that failed", got)
	}
	h.sendMOK(req)
	if n := len(dependsOn(st.records())); n != 1 {
		t.Fatalf("%d depends_on records after the retry, want its sighting", n)
	}
}

// A write that returns an error but whose batch was stored is a success: the
// requests get 200, the translator keeps what the group did, so the next point of
// the same value is not a sighting, and the group's counts are noted.
func TestMetricsWriteErrorWithTheBatchStored(t *testing.T) {
	h, st := metricsHarness(t)
	inject, fired := firstWith(true)
	st.inject = inject
	h.start()
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	h.setNow(time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(time.Minute), 2)))
	if !fired.Load() || len(dependsOn(st.records())) != 1 {
		t.Fatalf("fired %v, %d depends_on records", fired.Load(), len(dependsOn(st.records())))
	}
	h.setNow(2 * time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(2*time.Minute), 2)))
	if n := len(dependsOn(st.records())); n != 1 {
		t.Errorf("%d depends_on records after a point of an unchanged value, want the one", n)
	}
	for name, want := range map[string]int64{"metrics.points": 3, "svcgraph.points": 3, "svcgraph.sightings": 1} {
		if got := counter(h, name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
}

// A group whose write the store refused as invalid is written one request at
// a time after a reset: the translator is rolled back first, and each request is
// translated against what the one before it committed, so each increase is
// sighted exactly once.
func TestMetricsSplitAfterAnInvalidWrite(t *testing.T) {
	h, st := metricsHarness(t)
	g := newGate(t)
	var refused atomic.Int32
	st.inject = func(i int, batch []store.Record) (bool, error) {
		if i == 0 {
			g.hold()
		}
		if len(dependsOn(batch)) == 2 && refused.Add(1) == 1 {
			return false, fmt.Errorf("fake: batch[0]: a value the codec cannot encode: %w", store.ErrInvalid)
		}
		return true, nil
	}
	h.start()
	h.setNow(0)
	warm := h.sendMAsync(otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		mres(clusterAttrs("c"), ab.cum(at(0), 1), cd.cum(at(0), 1)),
	}})
	<-g.entered
	h.setNow(time.Minute)
	first := h.sendMAsync(mreq("c", ab.cum(at(time.Minute), 2)))
	second := h.sendMAsync(mreq("c", cd.cum(at(time.Minute), 2)))
	h.waitQueued(2)
	g.open()
	for i, ch := range []<-chan answer{warm, first, second} {
		if got := recv(t, ch); got.err != nil || got.p.Rejected != 0 {
			t.Fatalf("request %d: %+v", i, got)
		}
	}
	if refused.Load() != 1 {
		t.Fatalf("the invalid refusal fired %d times, want once", refused.Load())
	}
	for _, e := range []edge{ab, cd} {
		if n := len(edgeRecords(t, st.records(), e)); n != 1 {
			t.Errorf("%d depends_on records of %s>%s, want its one sighting", n, e.client, e.server)
		}
	}
}

// A write refused for the horizon is rerun once against the translator's
// committed state: the sighting of a request that is not before the horizon is
// written once, and the one that is before it is rejected into the partial
// success.
func TestMetricsHorizonRerun(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	var move atomic.Bool
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) {
		if move.CompareAndSwap(true, false) {
			if err := mem.Retain(context.Background(), at(-time.Hour)); err != nil {
				t.Error(err)
			}
		}
	}})).start()
	h.setNow(0)
	h.sendMOK(mreq("c", cd.cum(at(0), 1)))
	move.Store(true)
	h.setNow(time.Minute)
	p, err := h.sendM(mreq("c", ab.delta(at(-2*time.Hour), 1), cd.cum(at(time.Minute), 2)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Rejected != 1 || !strings.HasPrefix(p.Message, "1 data point rejected") || !strings.Contains(p.Message, "retention horizon") {
		t.Errorf("partial %+v, want the old sighting rejected for the horizon", p)
	}
	if n := len(edgeRecords(t, st.records(), cd)); n != 1 {
		t.Errorf("%d depends_on records of the current edge, want its one sighting", n)
	}
	if n := len(edgeRecords(t, st.records(), ab)); n != 0 {
		t.Errorf("%d records of the old edge, want none", n)
	}
}

// A panic while translating a metrics request refuses it (400) and makes the
// translators of its cluster name anew, the service graph's included: with no
// state, the next point of a series seen before is its first, set aside by the
// warm-up.
func TestMetricsPanicMakesTheServiceGraphAnew(t *testing.T) {
	h, st := metricsHarness(t, withHooks(sequencer.Hooks{BeforeTranslateMetrics: func(_ string, res otlpmetrics.Resource) {
		for _, a := range res.Attributes {
			if a.Key == "boom" {
				panic("boom")
			}
		}
	}}))
	h.start()
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	h.setNow(time.Minute)
	boom := otlpmetrics.Data{Resources: []otlpmetrics.Resource{mres(clusterAttrs("c", kv("boom", "1")), cd.delta(at(time.Minute), 1))}}
	_, err := h.sendM(boom)
	var refused *otlphttp.RefusedError
	if !errors.As(err, &refused) || !strings.Contains(refused.Message, "boom") {
		t.Fatalf("the panicking request: %v, want a 400 naming the panic", err)
	}
	h.setNow(2 * time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(2*time.Minute), 2)))
	if n := len(dependsOn(st.records())); n != 0 {
		t.Errorf("%d depends_on records, want the increase set aside by a new warm-up", n)
	}
	if got := counter(h, "svcgraph.skipped_warm_up"); got != 2 {
		t.Errorf("warm-up skips %d, want 2: the first point and the one after the panic", got)
	}
}

// A logs request and a metrics request in one group are one write; each is
// answered with its own noun.
func TestMetricsAndLogsInOneGroup(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st)
	if err := mem.Retain(context.Background(), at(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	old := pod{"old-1", "old-1", "", t0.Add(-3 * time.Hour), time.Time{}, "Running"}
	current := pod{"cur-1", "cur-1", "", t0.Add(-3 * time.Hour), time.Time{}, "Running"}
	l := h.sendAsync(context.Background(), logs("", watchPod(at(-2*time.Hour), "ADDED", old), pullPod(t0, current)))
	m := h.sendMAsync(otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		mres(clusterAttrs(""), ab.delta(at(-2*time.Hour), 1), cd.delta(at(0), 1)),
	}})
	h.waitQueued(2)
	h.start()
	lg, mg := recv(t, l), recv(t, m)
	if lg.err != nil || lg.p.Rejected != 1 || !strings.HasPrefix(lg.p.Message, "1 log record rejected") {
		t.Errorf("the logs request: %+v", lg)
	}
	if mg.err != nil || mg.p.Rejected != 1 || !strings.HasPrefix(mg.p.Message, "1 data point rejected") {
		t.Errorf("the metrics request: %+v", mg)
	}
	if n := st.writeCount(); n != 1 {
		t.Errorf("%d writes, want 1 for the group", n)
	}
	if len(edgeRecords(t, st.records(), cd)) != 1 || len(fromProducer(st.records(), "k8sobjects/")) == 0 {
		t.Errorf("the accepted records of both requests were not written: %v", showAll(st.records()))
	}
}

// A span count resource whose record is before the horizon is rejected with
// the weight of its points, and the other resource of the request is written.
func TestMetricsSpanCountsRejectedByTheirPoints(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st).start()
	if err := mem.Retain(context.Background(), at(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	// The sequencer reads the horizons at each group.
	old := spanRes("c", "shop", "a", "a-1", "", spanPoint(at(-2*time.Hour-2*time.Minute), 1), spanPoint(at(-2*time.Hour-time.Minute), 2), spanPoint(at(-2*time.Hour), 4))
	cur := spanRes("c", "shop", "b", "b-1", "", spanPoint(at(0), 1))
	p, err := h.sendM(otlpmetrics.Data{Resources: []otlpmetrics.Resource{old, cur}})
	if err != nil {
		t.Fatal(err)
	}
	if p.Rejected != 3 || !strings.HasPrefix(p.Message, "3 data points rejected, which a retry would not change") {
		t.Errorf("partial %+v, want the three points of the old resource", p)
	}
	b := instFP(t, "shop", "b", "b-1")
	wrote := false
	for _, r := range st.records() {
		wrote = wrote || r.Subject == store.EntitySubject(b)
	}
	if !wrote {
		t.Errorf("the current resource's instance was not written: %v", showAll(st.records()))
	}
	p, err = h.sendM(otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		spanRes("c", "shop", "a", "a-1", "", spanPoint(at(-2*time.Hour), 1)),
	}})
	if err != nil || p.Rejected != 1 || !strings.HasPrefix(p.Message, "1 data point rejected") {
		t.Errorf("partial %+v, error %v, want one data point", p, err)
	}
}

// A request refused whole, by a configured cluster name that a resource
// differs from or by the detector, is a 400 that changes no state: a point of the
// refused request is not taken for seen, and nothing is counted.
func TestMetricsRefusedRequestsChangeNothing(t *testing.T) {
	t.Run("a configured cluster name", func(t *testing.T) {
		h, st := metricsHarness(t, withConfig(func(c *sequencer.Config) { c.ClusterName, c.ClusterNameSet = "prod", true }))
		h.start()
		h.setNow(0)
		h.sendMOK(mreq("", ab.cum(at(0), 1)))
		h.setNow(time.Minute)
		_, err := h.sendM(otlpmetrics.Data{Resources: []otlpmetrics.Resource{
			mres(nil, ab.cum(at(time.Minute), 2)),
			mres(clusterAttrs("other"), cd.delta(at(time.Minute), 1)),
		}})
		var refused *otlphttp.RefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Message, "differs from the configured cluster name") {
			t.Fatalf("the request: %v, want a 400 for the cluster name", err)
		}
		if got := counter(h, "metrics.points"); got != 1 {
			t.Errorf("metrics.points %d, want the first request's one", got)
		}
		h.sendMOK(mreq("", ab.cum(at(time.Minute), 2)))
		if n := len(edgeRecords(t, st.records(), ab)); n != 1 {
			t.Errorf("%d sightings of the edge, want the one: the refused request left no trace", n)
		}
		if n := len(fromProducer(st.records(), "svcgraph/prod")); n != 3 {
			t.Errorf("%d records of svcgraph/prod, want 3", n)
		}
	})
	t.Run("the detector", func(t *testing.T) {
		h, st := metricsHarness(t)
		h.start()
		h.setNow(time.Minute)
		h.sendOK(logs("", nsState("ks-1", "kube-system", at(time.Minute)), pullPod(at(time.Minute), p1)))
		h.sendMOK(mreq("", ab.cum(at(time.Minute), 1)))
		h.setNow(6 * time.Minute)
		if _, err := h.send(logs("", nsState("ks-2", "kube-system", at(6*time.Minute)))); err == nil {
			t.Fatal("the second kube-system UID was not refused")
		}
		_, err := h.sendM(mreq("", ab.cum(at(6*time.Minute), 2)))
		var refused *otlphttp.RefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Message, `"ks-1", "ks-2"`) {
			t.Fatalf("the metrics request: %v, want a 400 naming both UIDs", err)
		}
		if got := counter(h, "metrics.points"); got != 1 {
			t.Errorf("metrics.points %d, want the first request's one", got)
		}
		if n := len(dependsOn(st.records())); n != 0 {
			t.Errorf("%d depends_on records from a refused request", n)
		}
	})
}

// A cluster with service graph points that has none for more than three
// connector flushes is silent as of the next tick, warned about once for each
// silence, and a point clears it.
func TestMetricsSilentCluster(t *testing.T) {
	var log syncBuffer
	h, _ := metricsHarness(t, withLog(&log))
	h.start()
	const warning = "no service graph point for more than three flushes"
	silent := func() bool { return h.seq.MetricsStatus().Clusters["c"].Silent }
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	h.setNow(3 * time.Minute)
	h.tick()
	if silent() || log.count(warning) != 0 {
		t.Fatalf("silent after three flushes exactly: %v, %d warnings", silent(), log.count(warning))
	}
	h.setNow(4 * time.Minute)
	h.tick()
	h.setNow(5 * time.Minute)
	h.tick()
	if !silent() || log.count(warning) != 1 {
		t.Fatalf("after more than three flushes: silent %v, %d warnings, want silent and one warning", silent(), log.count(warning))
	}
	if got := h.seq.MetricsStatus().Clusters["c"].LastServiceGraphPoint; !got.Equal(at(0)) {
		t.Errorf("last point %s, want the ingest instant of the group", got.Sub(t0))
	}
	h.setNow(6 * time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(6*time.Minute), 1)))
	if silent() {
		t.Fatal("a point did not clear the silence")
	}
	h.setNow(8 * time.Minute)
	h.tick()
	if silent() || log.count(warning) != 1 {
		t.Errorf("two minutes after a point: silent %v, %d warnings", silent(), log.count(warning))
	}
	h.setNow(10 * time.Minute)
	h.tick()
	if !silent() || log.count(warning) != 2 {
		t.Errorf("after a second silence: silent %v, %d warnings, want a second warning", silent(), log.count(warning))
	}
}

// A translator whose series table is full counts the series it sets aside,
// and the sequencer warns at most once a minute for each cluster.
func TestMetricsSeriesOverflow(t *testing.T) {
	var log syncBuffer
	h, _ := metricsHarness(t, withLog(&log), withConfig(func(c *sequencer.Config) { c.ServiceGraphMaxSeries = 2 }))
	h.start()
	const warning = "series table is full"
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1), cd.cum(at(0), 1), ef.cum(at(0), 1)))
	if got := counter(h, "svcgraph.skipped_series_overflow"); got != 1 {
		t.Fatalf("overflow count %d, want 1", got)
	}
	if st := h.seq.MetricsStatus().Clusters["c"]; st.Series != 2 {
		t.Errorf("series %d, want the 2 the table holds", st.Series)
	}
	h.setNow(10 * time.Second)
	h.sendMOK(mreq("c", ef.cum(at(10*time.Second), 1)))
	if got := counter(h, "svcgraph.skipped_series_overflow"); got != 2 || log.count(warning) != 1 {
		t.Errorf("overflow %d, %d warnings, want 2 and one within the minute", got, log.count(warning))
	}
	h.setNow(70 * time.Second)
	h.sendMOK(mreq("c", ef.cum(at(70*time.Second), 1)))
	if log.count(warning) != 2 {
		t.Errorf("%d warnings a minute later, want 2", log.count(warning))
	}
}

// The counters count the groups answered without error only, and a resource
// with span counts and no service.instance.id writes nothing, is counted, and is
// warned about.
func TestMetricsCountersAndTheMissingInstanceID(t *testing.T) {
	var log syncBuffer
	h, st := metricsHarness(t, withLog(&log))
	inject, _ := firstWith(false)
	st.inject = inject
	h.start()
	h.setNow(0)
	req := otlpmetrics.Data{Resources: []otlpmetrics.Resource{
		mres(clusterAttrs("c"), ab.delta(at(0), 1)),
		spanRes("c", "shop", "noid", "", "", spanPoint(at(0), 2)),
	}}
	if _, err := h.sendM(req); !errors.Is(err, otlphttp.ErrBusy) {
		t.Fatalf("the failed write: %v", err)
	}
	if got := h.seq.MetricsStatus(); len(got.Counters) != 0 || len(got.Clusters) != 0 || log.count("service.instance.id") != 0 {
		t.Fatalf("a group that failed was counted: %+v, %d warnings", got, log.count("service.instance.id"))
	}
	h.sendMOK(req)
	for name, want := range map[string]int64{
		"metrics.points": 2, "svcgraph.points": 1, "svcgraph.sightings": 1,
		"spancount.points": 1, "spancount.skipped_no_instance_id": 1, "spancount.sightings": 0,
	} {
		if got := counter(h, name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	if n := log.count("service.instance.id"); n != 1 {
		t.Errorf("%d warnings of the missing instance id, want 1", n)
	}
	if recs := fromProducer(st.records(), "spancount/"); len(recs) != 0 {
		t.Errorf("a resource with no instance id wrote %v", showAll(recs))
	}
	status := h.seq.MetricsStatus()
	if status.DependencyExpiry != 3*time.Hour || status.InstanceTTL != 2*time.Hour {
		t.Errorf("status TTLs %s and %s", status.DependencyExpiry, status.InstanceTTL)
	}
	c := status.Clusters["c"]
	if !c.LastServiceGraphPoint.Equal(at(0)) || !c.LastSpanCountPoint.Equal(at(0)) || c.Silent || c.Series != 0 {
		t.Errorf("cluster status %+v", c)
	}
}

// Other points are counted by the sequencer and given to nobody.
func TestMetricsOtherPointsAreCounted(t *testing.T) {
	h, st := metricsHarness(t)
	h.start()
	h.setNow(0)
	other := otlpmetrics.SumPoint{Scope: "x", Metric: "other_total", Temporality: otlpmetrics.Delta, Monotonic: true, TimeUnixNano: nano(at(0)), Value: 1}
	res := mres(clusterAttrs("c"), other, other)
	res.DroppedNotSum, res.DroppedNonMonotonic = 3, 4
	h.sendMOK(otlpmetrics.Data{Resources: []otlpmetrics.Resource{res}})
	for name, want := range map[string]int64{
		"metrics.points": 9, "metrics.skipped_other_metric": 2, "metrics.dropped_not_sum": 3, "metrics.dropped_non_monotonic": 4,
	} {
		if got := counter(h, name); got != want {
			t.Errorf("%s = %d, want %d", name, got, want)
		}
	}
	for name := range h.seq.MetricsStatus().Counters {
		if strings.HasPrefix(name, "svcgraph.") || strings.HasPrefix(name, "spancount.") {
			t.Errorf("counter %s: the points were given to a translator", name)
		}
	}
	if len(st.records()) != 0 {
		t.Errorf("records written: %v", showAll(st.records()))
	}
}

// MaxGroupRecords counts the data points of a metrics request: a request of
// as many points as the bound fills a group, and the log record behind it waits for
// the next.
func TestMetricsGroupBoundCountsPoints(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st, withConfig(func(c *sequencer.Config) { c.MaxGroupRecords = 3 }))
	m := h.sendMAsync(mreq("c", ab.delta(at(0), 1), cd.delta(at(0), 1), ef.delta(at(0), 1)))
	h.waitQueued(1)
	l := h.sendAsync(context.Background(), logs("", pullPod(t0, p1)))
	h.waitQueued(2)
	h.start()
	for i, ch := range []<-chan answer{m, l} {
		if got := recv(t, ch); got.err != nil || got.p.Rejected != 0 {
			t.Fatalf("request %d: %+v", i, got)
		}
	}
	if n := st.writeCount(); n != 2 {
		t.Errorf("%d writes, want 2: the points fill the first group", n)
	}
}

// A group of logs requests only, whose write failed and was not stored, does
// not make the service graph anew: the next point of a series seen before is
// sighted.
func TestMetricsLogsGroupFailureKeepsTheServiceGraph(t *testing.T) {
	h, st := metricsHarness(t)
	var failed atomic.Bool
	st.inject = func(_ int, batch []store.Record) (bool, error) {
		if len(batch) > 0 && len(dependsOn(batch)) == 0 && failed.CompareAndSwap(false, true) {
			return false, errInjected
		}
		return true, nil
	}
	h.start()
	h.setNow(0)
	h.sendMOK(mreq("c", ab.cum(at(0), 1)))
	if _, err := h.send(logs("c", pullPod(t0, p1))); !errors.Is(err, otlphttp.ErrBusy) || !failed.Load() {
		t.Fatalf("the logs request: %v, want a failed write", err)
	}
	h.setNow(time.Minute)
	h.sendMOK(mreq("c", ab.cum(at(time.Minute), 2)))
	if n := len(dependsOn(st.records())); n != 1 {
		t.Errorf("%d depends_on records, want the increase sighted", n)
	}
}

// One resource of delta points of three edges, one timed before the horizon:
// only that sighting is rejected, with the weight of one data point, and the other
// two are written, services and edges.
func TestMetricsOneSightingRejectedNotTheResource(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st).start()
	if err := mem.Retain(context.Background(), at(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	p, err := h.sendM(mreq("c", ab.delta(at(0), 1), cd.delta(at(-2*time.Hour), 1), ef.delta(at(0), 1)))
	if err != nil {
		t.Fatal(err)
	}
	if p.Rejected != 1 || !strings.HasPrefix(p.Message, "1 data point rejected, which a retry would not change; the first: ") {
		t.Fatalf("partial %+v, want one data point", p)
	}
	for _, c := range []struct {
		e    edge
		want int
	}{{ab, 1}, {cd, 0}, {ef, 1}} {
		if n := len(edgeRecords(t, st.records(), c.e)); n != c.want {
			t.Errorf("%d depends_on records of %s>%s, want %d", n, c.e.client, c.e.server, c.want)
		}
	}
	services := 0
	for _, r := range st.records() {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Service {
			services++
		}
	}
	if services != 4 {
		t.Errorf("%d service records, want the 4 of the two sightings written", services)
	}
}

// The configuration of the metrics path is checked at New.
func TestMetricsConfig(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	for name, c := range map[string]sequencer.Config{
		"negative expiry":       {Store: st, DependencyExpiry: -time.Second},
		"negative ttl":          {Store: st, InstanceTTL: -time.Second},
		"expiry over a year":    {Store: st, DependencyExpiry: 400 * 24 * time.Hour},
		"ttl over a year":       {Store: st, InstanceTTL: 400 * 24 * time.Hour},
		"negative series bound": {Store: st, ServiceGraphMaxSeries: -1},
	} {
		if _, err := sequencer.New(c); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	s, err := sequencer.New(sequencer.Config{Store: st})
	if err != nil {
		t.Fatal(err)
	}
	if got := s.MetricsStatus(); got.DependencyExpiry != sequencer.DefaultDependencyExpiry || got.InstanceTTL != sequencer.DefaultInstanceTTL ||
		sequencer.DefaultDependencyExpiry != 6*time.Hour || sequencer.DefaultInstanceTTL != 6*time.Hour {
		t.Errorf("defaults %s and %s", got.DependencyExpiry, got.InstanceTTL)
	}
}

// The service graph's records feed retention's clock agreement (the
// collector's clock), and the span counts' do not (many applications' clocks, any
// one of which behind would pause retention), whether they were written or
// refused as before a horizon.
func TestMetricsClockAgreement(t *testing.T) {
	old := trueNow.Add(-2 * day)
	graph := func(tm time.Time) otlpmetrics.Data { return mreq("", ab.delta(tm, 1)) }
	spans := func(tm time.Time) otlpmetrics.Data {
		return otlpmetrics.Data{Resources: []otlpmetrics.Resource{spanRes("", "shop", "a", "a-1", "", spanPoint(tm, 1))}}
	}
	for _, c := range []struct {
		name    string
		horizon time.Time // retained to before the loop starts, none if zero
		steps   []otlpmetrics.Data
		want    sequencer.Agreement
	}{
		{"service graph two days off", time.Time{}, []otlpmetrics.Data{graph(old)}, sequencer.Disagree},
		{"service graph now", time.Time{}, []otlpmetrics.Data{graph(trueNow)}, sequencer.Agree},
		{"span counts two days off", time.Time{}, []otlpmetrics.Data{spans(old)}, sequencer.AgreementUnknown},
		{"span counts after a good batch", time.Time{}, []otlpmetrics.Data{graph(trueNow), spans(old)}, sequencer.Agree},
		{"service graph refused for the horizon", trueNow.Add(-day), []otlpmetrics.Data{graph(old)}, sequencer.Disagree},
		{"span counts refused for the horizon", trueNow.Add(-day), []otlpmetrics.Data{spans(old)}, sequencer.AgreementUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := newRetainHarness(t, trueNow, sequencer.Hooks{})
			h.ret.noFwd = true
			if !c.horizon.IsZero() {
				h.retainTo(c.horizon)
			}
			h.begin()
			for _, d := range c.steps {
				if _, err := h.sendM(d); err != nil {
					t.Fatal(err)
				}
			}
			h.check()
			if got := h.status().Agreement; got != c.want {
				t.Errorf("agreement %s, want %s", got, c.want)
			}
		})
	}
}
