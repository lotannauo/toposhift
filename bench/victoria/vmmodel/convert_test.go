package vmmodel_test

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func ms(d time.Duration) int64 { return t0.Add(d).UnixMilli() }

// fataler is what a test and a rapid property both are.
type fataler interface {
	Helper()
	Fatal(args ...any)
}

func registry(t fataler) *vmmodel.Registry {
	t.Helper()
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

// run converts a stream, as one batch per record when each is true and as one batch
// otherwise, and returns the samples (with their series' keys) and the final counts and
// digest.
type outcome struct {
	samples []keyed
	counts  vmmodel.Counts
	digest  string
	series  []vmmodel.Series
}

type keyed struct {
	Key   string
	Ms    int64
	Stale bool
}

// until is the end of the period the converter is given unless a test says otherwise.
var until = t0.Add(1000 * time.Second)

func convert(t fataler, recs []engine.Record, batch int, opts ...func(*vmmodel.Options)) outcome {
	t.Helper()
	var out outcome
	o := vmmodel.Options{Registry: registry(t), Until: until}
	for _, f := range opts {
		f(&o)
	}
	var c *vmmodel.Converter
	o.Emit = func(ss []vmmodel.Sample) error {
		for _, s := range ss {
			out.samples = append(out.samples, keyed{c.Series(s.Series).Key(), s.TimestampMs, s.Stale})
		}
		return nil
	}
	c, err := vmmodel.NewConverter(o)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < len(recs); i += batch {
		if err := c.Write(recs[i:min(i+batch, len(recs))]); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	out.counts, out.digest = c.Counts(), c.Digest()
	for i := range out.counts.Series {
		out.series = append(out.series, c.Series(uint32(i)))
	}
	return out
}

// The fixture of the golden test. Node collector: an entity observed at 0, 60 and 120 s
// and then silent (its series just ends, no marker), and a late sample for 30 s that comes
// after a record at 950 s. Pod 1's placement: the Kubernetes watch's one assertion at 10 s
// (a watch-mode producer: the scraper writes it again every minute until the delete), the
// pod collector's refresh at 900 s, and the watch's delete at 950 s (a marker). Pod 2: an
// assertion and a delete 500 ns apart, truncated to one millisecond, so the later by
// sequence number (the delete) is kept. Pod 3: a delete and an assertion in one
// millisecond, so the assertion is kept, as a flap leaves the pod placed; it is scraped
// until the end of the period, 1,000 s.
func fixtureStream() []engine.Record {
	node := fixture.FP(catalog.K8sNode, 1)
	pod1, pod2, pod3 := fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12), fixture.FP(catalog.K8sPod, 13)
	at := func(d time.Duration) time.Time { return t0.Add(d) }
	nodeRec := func(seq uint64, d time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: at(d), Seq: seq, Kind: lifecycle.Observe, TTL: 240 * time.Second, Payload: []byte{1}}
	}
	pods := map[int]engine.Subject{
		1: engine.EdgeSubject(pod1, node, catalog.ScheduledOn),
		2: engine.EdgeSubject(pod2, node, catalog.ScheduledOn),
		3: engine.EdgeSubject(pod3, node, catalog.ScheduledOn),
	}
	placement := func(seq uint64, pod int, producer lifecycle.Producer, kind lifecycle.Kind, ttl time.Duration, d time.Duration) engine.Record {
		r := engine.Record{Layer: catalog.L2, Subject: pods[pod], Producer: producer, EventTime: at(d), Seq: seq, Kind: kind, TTL: ttl}
		if kind == lifecycle.Observe {
			r.Payload = []byte{2}
		}
		return r
	}
	return []engine.Record{
		nodeRec(1, 0),
		nodeRec(2, 60*time.Second),
		nodeRec(3, 120*time.Second),
		placement(4, 1, "k8sobjects", lifecycle.Observe, 0, 10*time.Second),
		placement(5, 1, "k8s-cluster", lifecycle.Observe, 3600*time.Second, 900*time.Second),
		placement(6, 1, "k8sobjects", lifecycle.Delete, 0, 950*time.Second),
		placement(7, 2, "k8sobjects", lifecycle.Observe, 0, 30*time.Second+999_999*time.Nanosecond),
		placement(8, 2, "k8sobjects", lifecycle.Delete, 0, 30*time.Second+500*time.Nanosecond),
		placement(9, 3, "k8sobjects", lifecycle.Delete, 0, 40*time.Second),
		placement(10, 3, "k8sobjects", lifecycle.Observe, 0, 40*time.Second),
		nodeRec(11, 30*time.Second),
	}
}

// TestConversionGolden pins what the fixture becomes: every sample and marker, in the
// order written, the series, and the digest of both. The order is that of the samples
// that wait for a collision to be ruled out (the records of the refreshing producers, and
// the first sample and the marker of a watch-mode series), after the scrapes of a
// watch-mode series, which are written as soon as the series is closed (and, for one
// still open, at the end of the period).
//
// It catches a marker written on expiry (the node's series must end with its last
// sample), no marker on a Delete, nanoseconds passed as milliseconds, a collapse that
// keeps the first record of a millisecond instead of the last by sequence number, a
// scrape at the delete's own millisecond, one missing at the end of the period, and an
// open series that is not scraped.
func TestConversionGolden(t *testing.T) {
	out := convert(t, fixtureStream(), 4)

	node := fixture.FP(catalog.K8sNode, 1)
	pod1, pod2, pod3 := fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12), fixture.FP(catalog.K8sPod, 13)
	nodeS := vmmodel.EntitySeries(catalog.L2, node, "node-collector").Key()
	p1o := vmmodel.EdgeSeries(catalog.L2, pod1, node, catalog.ScheduledOn, "k8sobjects").Key()
	p1c := vmmodel.EdgeSeries(catalog.L2, pod1, node, catalog.ScheduledOn, "k8s-cluster").Key()
	p2o := vmmodel.EdgeSeries(catalog.L2, pod2, node, catalog.ScheduledOn, "k8sobjects").Key()
	p3o := vmmodel.EdgeSeries(catalog.L2, pod3, node, catalog.ScheduledOn, "k8sobjects").Key()
	var want []keyed
	// Pod 1 is scraped from its assertion at 10 s, every 60 s, strictly before its delete
	// at 950 s: 70 s up to 910 s.
	for at := 70 * time.Second; at < 950*time.Second; at += time.Minute {
		want = append(want, keyed{p1o, ms(at), false})
	}
	// Pod 3 is scraped from 40 s up to and including the end of the period.
	for at := 100 * time.Second; at <= 1000*time.Second; at += time.Minute {
		want = append(want, keyed{p3o, ms(at), false})
	}
	want = append(want,
		keyed{nodeS, ms(0), false},
		keyed{nodeS, ms(60 * time.Second), false},
		keyed{nodeS, ms(120 * time.Second), false},
		keyed{p1c, ms(900 * time.Second), false},
		keyed{nodeS, ms(30 * time.Second), false},
		keyed{p1o, ms(10 * time.Second), false}, // the watch's assertion, the first sample of its series
		keyed{p2o, ms(30 * time.Second), true},  // the delete, which came later than the observe in the same millisecond
		keyed{p3o, ms(40 * time.Second), false}, // the observe, which came later than the delete
		keyed{p1o, ms(950 * time.Second), true}, // the delete is a marker
	)
	if !slices.Equal(out.samples, want) {
		t.Fatalf("samples\n got %v\nwant %v", out.samples, want)
	}

	c := out.counts
	if c.Records != 11 || c.Observes != 8 || c.Deletes != 3 {
		t.Errorf("read %d records, %d observes, %d deletes", c.Records, c.Observes, c.Deletes)
	}
	// 15 scrapes for pod 1 and 16 for pod 3.
	if c.Scraped != 31 || c.Reasserted != 0 {
		t.Errorf("scraped %d, reasserted %d", c.Scraped, c.Reasserted)
	}
	// The first and the last timestamp written: the node's first sample at 0, and the
	// scrapes of pod 3 through the end of the period.
	if c.FirstMs != ms(0) || c.LastMs != ms(1000*time.Second) {
		t.Errorf("first %d and last %d ms, want %d and %d", c.FirstMs, c.LastMs, ms(0), ms(1000*time.Second))
	}
	if c.Samples != 38 || c.Markers != 2 || c.CollapsedObserves != 1 || c.CollapsedDeletes != 1 || c.Collapses() != 2 {
		t.Errorf("wrote %d samples and %d markers, collapsed %d observes and %d deletes", c.Samples, c.Markers, c.CollapsedObserves, c.CollapsedDeletes)
	}
	if c.Series != 5 || c.EntitySeries != 1 || c.EdgeSeries != 4 {
		t.Errorf("series %d (%d entity, %d edge)", c.Series, c.EntitySeries, c.EdgeSeries)
	}
	// Every series' earliest sample is in the first hour, but the late sample for the node
	// at 30 s does not change that; the churn is by the earliest sample.
	if len(c.Churn) != 1 || c.Churn[0].HourMs != ms(0) || c.Churn[0].Created != 5 {
		t.Errorf("churn %+v", c.Churn)
	}
	if c.Samples+c.Markers+c.Collapses()+c.Reasserted != c.Records+c.Scraped {
		t.Error("records are not accounted for")
	}

	// The digest is recomputed here from the documented encoding of the list above (the
	// index of each series in order of first appearance, the millisecond, the kind) and of
	// the series keys, and is pinned.
	idx := map[string]uint32{nodeS: 0, p1o: 1, p1c: 2, p2o: 3, p3o: 4}
	samples := sha256.New()
	for _, s := range want {
		var b [13]byte
		binary.BigEndian.PutUint32(b[0:], idx[s.Key])
		binary.BigEndian.PutUint64(b[4:], uint64(s.Ms))
		if s.Stale {
			b[12] = 1
		}
		samples.Write(b[:])
	}
	series := sha256.New()
	for _, k := range []string{nodeS, p1o, p1c, p2o, p3o} {
		fmt.Fprintf(series, "%d:%s\n", len(k), k)
	}
	final := sha256.New()
	final.Write(samples.Sum(nil))
	final.Write(series.Sum(nil))
	if got := hex.EncodeToString(final.Sum(nil)); got != out.digest {
		t.Errorf("digest %s, recomputed %s", out.digest, got)
	}
	const pinned = "09312fa9cf1c49f1b5042117f01a23ccf5e515e3e71f70f2da8f9c42d195a638"
	if out.digest != pinned {
		t.Errorf("digest %s is not the pinned %s", out.digest, pinned)
	}
}

// The batch size the stream is written in changes nothing: samples, counts and digest are
// the same for a batch of one record, of three, and of all of them.
func TestConversionDoesNotDependOnTheBatches(t *testing.T) {
	recs := fixtureStream()
	a, b, c := convert(t, recs, 1), convert(t, recs, 3), convert(t, recs, len(recs))
	for name, o := range map[string]outcome{"3": b, "all": c} {
		if !slices.Equal(a.samples, o.samples) || a.digest != o.digest {
			t.Errorf("batch %s differs from batch 1", name)
		}
	}
}

// The reorder window is what lets a collision be told: a record a whole window behind the
// newest is refused, one just inside it is kept and collapses with its twin.
func TestReorderWindow(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	obs := func(seq uint64, d time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0.Add(d), Seq: seq, Kind: lifecycle.Observe, TTL: 240 * time.Second}
	}
	window := func(o *vmmodel.Options) { o.ReorderWindow = time.Hour }

	// Just inside the window: twin of the first record, 3,599.999 s behind.
	out := convert(t, []engine.Record{obs(1, 0), obs(2, time.Hour-time.Millisecond), obs(3, 0)}, 1, window)
	if out.counts.CollapsedObserves != 1 || out.counts.Samples != 2 {
		t.Errorf("inside the window: %+v", out.counts)
	}

	// Exactly the window behind: refused, because the twin may already be written.
	c, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until, ReorderWindow: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	err = c.Write([]engine.Record{obs(1, 0), obs(2, time.Hour), obs(3, 0)})
	if !errors.Is(err, vmmodel.ErrTooLate) {
		t.Errorf("a record a whole window behind: err = %v", err)
	}
	if err := c.Write(nil); !errors.Is(err, vmmodel.ErrClosed) {
		t.Errorf("after an error: %v", err)
	}
}

func TestConverterRefusals(t *testing.T) {
	node, pod := fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.K8sPod, 2)
	good := engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0, Seq: 5, Kind: lifecycle.Observe, TTL: 240 * time.Second}
	with := func(f func(*engine.Record)) engine.Record {
		r := good
		f(&r)
		return r
	}
	cases := []struct {
		name string
		prev []engine.Record
		rec  engine.Record
		want error
	}{
		{"a run of refreshes", nil, with(func(r *engine.Record) { r.Through = t0.Add(time.Hour) }), vmmodel.ErrCoalesced},
		{"a producer that does not hold the subject", nil, with(func(r *engine.Record) { r.Producer = "traces" }), vmmodel.ErrUnregistered},
		{"an unknown producer", nil, with(func(r *engine.Record) { r.Producer = "other" }), vmmodel.ErrUnregistered},
		{"a subject the registry does not hold", nil, with(func(r *engine.Record) {
			r.Subject = engine.EdgeSubject(pod, node, catalog.HasAddress)
		}), vmmodel.ErrUnregistered},
		{"a TTL other than the window", nil, with(func(r *engine.Record) { r.TTL = 60 * time.Second }), vmmodel.ErrWindow},
		{"no TTL where the window is not zero", nil, with(func(r *engine.Record) { r.TTL = 0 }), vmmodel.ErrWindow},
		{"a TTL where the producer has none", nil, with(func(r *engine.Record) {
			r.Subject, r.Producer, r.TTL = engine.EntitySubject(pod), "k8sobjects", time.Minute
		}), vmmodel.ErrWindow},
		{"a sequence number that does not rise", []engine.Record{good}, with(func(r *engine.Record) { r.EventTime = t0.Add(time.Second) }), vmmodel.ErrOrder},
		{"sequence number zero", nil, with(func(r *engine.Record) { r.Seq = 0 }), vmmodel.ErrOrder},
		{"a time before 1970", nil, with(func(r *engine.Record) { r.EventTime = time.Unix(-1, 0) }), vmmodel.ErrTime},
		{"a record kind that is not one", nil, with(func(r *engine.Record) { r.Kind = 0 }), nil},
		{"a subject of no kind", nil, with(func(r *engine.Record) { r.Subject = engine.Subject{} }), vmmodel.ErrUnregistered},
	}
	for _, c := range cases {
		cv, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until})
		if err != nil {
			t.Fatal(err)
		}
		if len(c.prev) > 0 {
			if err := cv.Write(c.prev); err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
		}
		err = cv.Write([]engine.Record{c.rec})
		switch {
		case err == nil:
			t.Errorf("%s: accepted", c.name)
		case c.want != nil && !errors.Is(err, c.want):
			t.Errorf("%s: err = %v, want %v", c.name, err, c.want)
		}
		if err := cv.Write(nil); !errors.Is(err, vmmodel.ErrClosed) {
			t.Errorf("%s: a later write is not refused: %v", c.name, err)
		}
		if err := cv.Close(); err == nil {
			t.Errorf("%s: Close does not report the failure", c.name)
		}
	}
}

func TestConverterOptionsAndEmitFailure(t *testing.T) {
	if _, err := vmmodel.NewConverter(vmmodel.Options{}); err == nil {
		t.Error("a converter with no registry")
	}
	for _, w := range []time.Duration{-time.Second, time.Microsecond} {
		if _, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until, ReorderWindow: w}); err == nil {
			t.Errorf("reorder window %s accepted", w)
		}
	}
	// A failing sink closes the converter with its error.
	boom := errors.New("boom")
	c, _ := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until, ReorderWindow: time.Second, Emit: func([]vmmodel.Sample) error { return boom }})
	node := fixture.FP(catalog.K8sNode, 1)
	rec := func(seq uint64, d time.Duration) engine.Record {
		return engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0.Add(d), Seq: seq, Kind: lifecycle.Observe, TTL: 240 * time.Second}
	}
	if err := c.Write([]engine.Record{rec(1, 0), rec(2, 2*time.Second)}); !errors.Is(err, boom) {
		t.Errorf("Write = %v, want the sink's error", err)
	}
	if err := c.Write([]engine.Record{rec(3, 3*time.Second)}); !errors.Is(err, vmmodel.ErrClosed) || !errors.Is(err, boom) {
		t.Errorf("after the failure: %v", err)
	}
	// Retain changes nothing and Close twice is fine.
	d, _ := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until})
	if err := d.Retain(t0); err != nil {
		t.Error(err)
	}
	if d.Digest() != "" {
		t.Error("a digest before Close")
	}
	if err := d.Close(); err != nil {
		t.Error(err)
	}
	if err := d.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
	if d.Digest() == "" {
		t.Error("no digest after Close")
	}
}

// ---- properties ----

var (
	propNode, propHost = fixture.FP(catalog.K8sNode, 1), fixture.FP(catalog.Host, 2)
	propPods           = []engine.Subject{
		engine.EdgeSubject(fixture.FP(catalog.K8sPod, 11), propNode, catalog.ScheduledOn),
		engine.EdgeSubject(fixture.FP(catalog.K8sPod, 12), propNode, catalog.ScheduledOn),
	}
	propSubjects = []engine.Subject{
		engine.EntitySubject(propNode),
		engine.EdgeSubject(propNode, propHost, catalog.RunsOn),
		propPods[0], propPods[1],
	}
)

// genStream draws a stream of records over a few subjects, producers drawn from those the
// registry says hold the subject and TTLs from their windows, at millisecond resolution
// in a span so short that many fall in one millisecond, in ascending sequence order, some
// of them late. The producers in watch mode are left out unless watch is set, in which
// case the span is minutes (on a grid of half a scrape interval, with jitter of a few
// milliseconds, so that scrapes, asserts and deletes meet) and their records carry no TTL.
func genStream(t *rapid.T, reg *vmmodel.Registry, watch bool) []engine.Record {
	n := rapid.IntRange(1, 60).Draw(t, "records")
	recs := make([]engine.Record, n)
	for i := range recs {
		s := rapid.SampledFrom(propSubjects).Draw(t, "subject")
		var hs []vmmodel.Holder
		if s.Kind == engine.SubjectEntity {
			hs = reg.EntityHolders(s.A.Type())
		} else {
			hs = reg.EdgeHolders(s.Relation)
		}
		if !watch {
			hs = slices.DeleteFunc(slices.Clone(hs), func(h vmmodel.Holder) bool { return h.Watch() })
		}
		h := rapid.SampledFrom(hs).Draw(t, "holder")
		layer := catalog.L2
		if s.Relation == catalog.RunsOn {
			layer = catalog.L1
		}
		at := time.Duration(rapid.IntRange(0, 25).Draw(t, "ms"))*time.Millisecond + time.Duration(rapid.IntRange(0, 999_999).Draw(t, "ns"))
		if watch {
			at = time.Duration(rapid.IntRange(0, 12).Draw(t, "half scrapes"))*(vmmodel.WatchScrape/2) + time.Duration(rapid.IntRange(0, 3).Draw(t, "jitter"))*time.Millisecond
		}
		r := engine.Record{Layer: layer, Subject: s, Producer: h.Producer, Seq: uint64(i + 1), EventTime: t0.Add(at)}
		if rapid.IntRange(0, 3).Draw(t, "delete") == 0 {
			r.Kind = lifecycle.Delete
		} else {
			r.Kind, r.TTL = lifecycle.Observe, h.Window
			if h.Watch() {
				r.TTL = 0
			}
		}
		recs[i] = r
	}
	return recs
}

// bruteForce keeps, for each series and millisecond, the record with the highest
// sequence number, and lists them in the order of those records.
func bruteForce(recs []engine.Record) (winners []keyed, observes, deletes, collapsedObs, collapsedDel uint64) {
	type k struct {
		key string
		ms  int64
	}
	last := map[k]int{}
	for i, r := range recs {
		s, err := vmmodel.SeriesOf(r)
		if err != nil {
			panic(err)
		}
		key := k{s.Key(), r.EventTime.UnixMilli()}
		if j, ok := last[key]; ok {
			if recs[j].Kind == lifecycle.Delete {
				collapsedDel++
			} else {
				collapsedObs++
			}
		}
		last[key] = i
		if r.Kind == lifecycle.Delete {
			deletes++
		} else {
			observes++
		}
	}
	for i, r := range recs {
		s, _ := vmmodel.SeriesOf(r)
		if last[k{s.Key(), r.EventTime.UnixMilli()}] == i {
			winners = append(winners, keyed{s.Key(), r.EventTime.UnixMilli(), r.Kind == lifecycle.Delete})
		}
	}
	return
}

// A conversion is what a brute force says, whatever the stream and the batching: one
// sample for each Observe and one marker for each Delete less the collapsed ones, in the
// order of the records that won, the series are the distinct identities, the same stream
// gives the same digest twice, and no dropped or invented record shifts a count.
func TestConversionProperties(t *testing.T) {
	reg := registry(t)
	rapid.Check(t, func(rt *rapid.T) {
		recs := genStream(rt, reg, false)
		batch := rapid.IntRange(1, len(recs)).Draw(rt, "batch")
		a := convert(rt, recs, batch)
		b := convert(rt, recs, len(recs))

		winners, obs, del, cobs, cdel := bruteForce(recs)
		if !slices.Equal(a.samples, winners) {
			rt.Fatalf("samples\n got %v\nwant %v", a.samples, winners)
		}
		c := a.counts
		if c.Observes != obs || c.Deletes != del || c.CollapsedObserves != cobs || c.CollapsedDeletes != cdel {
			rt.Fatalf("counts %+v, want observes %d deletes %d collapsed %d/%d", c, obs, del, cobs, cdel)
		}
		if c.Samples != obs-cobs || c.Markers != del-cdel {
			rt.Fatalf("%d samples and %d markers, want %d and %d", c.Samples, c.Markers, obs-cobs, del-cdel)
		}
		distinct := map[string]bool{}
		for _, r := range recs {
			s, _ := vmmodel.SeriesOf(r)
			distinct[s.Key()] = true
		}
		if c.Series != len(distinct) || c.EntitySeries+c.EdgeSeries != c.Series {
			rt.Fatalf("%d series, want %d", c.Series, len(distinct))
		}
		if a.digest != b.digest || !slices.Equal(a.samples, b.samples) || a.digest != convert(rt, recs, batch).digest {
			rt.Fatal("the conversion is not deterministic or depends on the batches")
		}
	})
}

// With no two records in one millisecond the properties are the plain ones: a sample for
// every Observe, a marker for every Delete.
func TestWithoutCollisionsSamplesAreObservesAndMarkersAreDeletes(t *testing.T) {
	reg := registry(t)
	rapid.Check(t, func(rt *rapid.T) {
		recs := genStream(rt, reg, false)
		for i := range recs {
			recs[i].EventTime = t0.Add(time.Duration(i) * time.Millisecond) // distinct milliseconds
		}
		a := convert(rt, recs, len(recs))
		var obs, del uint64
		for _, r := range recs {
			if r.Kind == lifecycle.Delete {
				del++
			} else {
				obs++
			}
		}
		if a.counts.Samples != obs || a.counts.Markers != del || a.counts.Collapses() != 0 {
			rt.Fatalf("%+v for %d observes and %d deletes", a.counts, obs, del)
		}
	})
}

// ---- producers in watch mode ----

type candidate struct {
	key    string
	ms     int64
	stale  bool
	seq    uint64
	scrape bool
}

// bruteForceAll says what a stream that includes producers in watch mode becomes, by
// folding each series' records in event-time order one at a time: the samples as a set
// (in no order) and the counts. A series of a producer in watch mode is open from an
// Observe until a Delete, scraped from the opening every scrape interval, strictly
// before the Delete, or through untilMs if it is still open.
func bruteForceAll(recs []engine.Record, reg *vmmodel.Registry, untilMs int64) ([]keyed, vmmodel.Counts) {
	var c vmmodel.Counts
	by := map[string][]engine.Record{}
	var order []string
	for _, r := range recs {
		s, err := vmmodel.SeriesOf(r)
		if err != nil {
			panic(err)
		}
		if _, ok := by[s.Key()]; !ok {
			order = append(order, s.Key())
		}
		by[s.Key()] = append(by[s.Key()], r)
		c.Records++
		if r.Kind == lifecycle.Delete {
			c.Deletes++
		} else {
			c.Observes++
		}
	}
	var cands []candidate
	for _, key := range order {
		rs := by[key]
		var hs []vmmodel.Holder
		if rs[0].Subject.Kind == engine.SubjectEntity {
			hs = reg.EntityHolders(rs[0].Subject.A.Type())
		} else {
			hs = reg.EdgeHolders(rs[0].Subject.Relation)
		}
		var h vmmodel.Holder
		for _, x := range hs {
			if x.Producer == rs[0].Producer {
				h = x
			}
		}
		if !h.Watch() {
			for _, r := range rs {
				cands = append(cands, candidate{key: key, ms: r.EventTime.UnixMilli(), stale: r.Kind == lifecycle.Delete, seq: r.Seq})
			}
			continue
		}
		slices.SortFunc(rs, func(a, b engine.Record) int {
			if x := cmp.Compare(a.EventTime.UnixMilli(), b.EventTime.UnixMilli()); x != 0 {
				return x
			}
			return cmp.Compare(a.Seq, b.Seq)
		})
		step := h.Scrape.Milliseconds()
		open, start := false, int64(0)
		for _, r := range rs {
			ms := r.EventTime.UnixMilli()
			if r.Kind == lifecycle.Observe {
				if open {
					c.Reasserted++
					continue
				}
				open, start = true, ms
				cands = append(cands, candidate{key: key, ms: ms, seq: r.Seq})
				continue
			}
			if open {
				for at := start + step; at < ms; at += step {
					cands = append(cands, candidate{key: key, ms: at, scrape: true})
				}
				open = false
			}
			cands = append(cands, candidate{key: key, ms: ms, stale: true, seq: r.Seq})
		}
		if open {
			for at := start + step; at <= untilMs; at += step {
				cands = append(cands, candidate{key: key, ms: at, scrape: true})
			}
		}
	}
	// Two candidates of one series in one millisecond leave the later by sequence number.
	type k struct {
		key string
		ms  int64
	}
	winner := map[k]int{}
	for i, x := range cands {
		if x.scrape {
			continue
		}
		kk := k{x.key, x.ms}
		j, ok := winner[kk]
		if !ok {
			winner[kk] = i
			continue
		}
		loser := i
		if x.seq > cands[j].seq {
			loser, winner[kk] = j, i
		}
		if cands[loser].stale {
			c.CollapsedDeletes++
		} else {
			c.CollapsedObserves++
		}
		cands[loser].key = "" // dropped
	}
	var out []keyed
	for _, x := range cands {
		if x.key == "" {
			continue
		}
		out = append(out, keyed{x.key, x.ms, x.stale})
		switch {
		case x.stale:
			c.Markers++
		default:
			c.Samples++
			if x.scrape {
				c.Scraped++
			}
		}
	}
	return out, c
}

func sortKeyed(ks []keyed) []keyed {
	out := slices.Clone(ks)
	slices.SortFunc(out, func(a, b keyed) int {
		if x := cmp.Compare(a.Key, b.Key); x != 0 {
			return x
		}
		if x := cmp.Compare(a.Ms, b.Ms); x != 0 {
			return x
		}
		if a.Stale != b.Stale {
			if a.Stale {
				return 1
			}
			return -1
		}
		return 0
	})
	return out
}

// With producers in watch mode in the stream, in any batching, a conversion is what a
// brute force says: the samples as a set, and every count, the scrapes and the Observes
// of an open series among them. The accounting identity holds.
func TestWatchConversionProperties(t *testing.T) {
	reg := registry(t)
	rapid.Check(t, func(rt *rapid.T) {
		recs := genStream(rt, reg, true)
		batch := rapid.IntRange(1, len(recs)).Draw(rt, "batch")
		a := convert(rt, recs, batch)
		want, wc := bruteForceAll(recs, reg, until.UnixMilli())
		if got := sortKeyed(a.samples); !slices.Equal(got, sortKeyed(want)) {
			rt.Fatalf("samples\n got %v\nwant %v", got, sortKeyed(want))
		}
		c := a.counts
		if c.Records != wc.Records || c.Observes != wc.Observes || c.Deletes != wc.Deletes ||
			c.Samples != wc.Samples || c.Markers != wc.Markers || c.Scraped != wc.Scraped || c.Reasserted != wc.Reasserted ||
			c.CollapsedObserves != wc.CollapsedObserves || c.CollapsedDeletes != wc.CollapsedDeletes {
			rt.Fatalf("counts %+v, want %+v", c, wc)
		}
		if c.Samples+c.Markers+c.Collapses()+c.Reasserted != c.Records+c.Scraped {
			rt.Fatalf("records are not accounted for: %+v", c)
		}
		if b := convert(rt, recs, len(recs)); a.digest != b.digest {
			rt.Fatal("the digest depends on the batches")
		}
	})
}

var watchEdge = engine.EdgeSubject(fixture.FP(catalog.K8sPod, 21), fixture.FP(catalog.K8sNode, 1), catalog.ScheduledOn)

func watchRec(seq uint64, kind lifecycle.Kind, at time.Duration) engine.Record {
	return engine.Record{Layer: catalog.L2, Subject: watchEdge, Producer: "k8sobjects", EventTime: t0.Add(at), Seq: seq, Kind: kind}
}

func scrapedAt(series string, ats ...time.Duration) []keyed {
	var out []keyed
	for _, at := range ats {
		out = append(out, keyed{series, ms(at), false})
	}
	return out
}

// A fact held by a producer in watch mode is a sample at its assertion and one at every
// scrape after it, strictly before its delete, and a marker at the delete.
func TestAWatchFactIsScrapedBetweenItsAssertionAndItsDelete(t *testing.T) {
	series := vmmodel.EdgeSeries(catalog.L2, fixture.FP(catalog.K8sPod, 21), fixture.FP(catalog.K8sNode, 1), catalog.ScheduledOn, "k8sobjects").Key()
	cases := []struct {
		name string
		del  time.Duration
		want []time.Duration // the samples, besides the assertion at 10 s
	}{
		{"deleted before the first scrape", 50 * time.Second, nil},
		{"deleted exactly at the first scrape: no sample, the marker takes the millisecond", 70 * time.Second, nil},
		{"deleted a millisecond after the first scrape", 70*time.Second + time.Millisecond, []time.Duration{70 * time.Second}},
		{"deleted after three scrapes", 200 * time.Second, []time.Duration{70 * time.Second, 130 * time.Second, 190 * time.Second}},
	}
	for _, c := range cases {
		out := convert(t, []engine.Record{watchRec(1, lifecycle.Observe, 10*time.Second), watchRec(2, lifecycle.Delete, c.del)}, 1)
		want := scrapedAt(series, 10*time.Second)
		want = append(want, scrapedAt(series, c.want...)...)
		want = append(want, keyed{series, ms(c.del), true})
		if got := sortKeyed(out.samples); !slices.Equal(got, sortKeyed(want)) {
			t.Errorf("%s\n got %v\nwant %v", c.name, got, want)
		}
		if out.counts.Scraped != uint64(len(c.want)) || out.counts.Markers != 1 {
			t.Errorf("%s: %+v", c.name, out.counts)
		}
	}
}

// What is not deleted by the end of the period is scraped through it, the last instant
// included.
func TestAnOpenWatchFactIsScrapedThroughTheEndOfThePeriod(t *testing.T) {
	series := vmmodel.EdgeSeries(catalog.L2, fixture.FP(catalog.K8sPod, 21), fixture.FP(catalog.K8sNode, 1), catalog.ScheduledOn, "k8sobjects").Key()
	for _, c := range []struct {
		name  string
		until time.Duration
		want  []time.Duration
	}{
		{"the end is on the grid", 130 * time.Second, []time.Duration{10 * time.Second, 70 * time.Second, 130 * time.Second}},
		{"the end is a millisecond before the grid", 130*time.Second - time.Millisecond, []time.Duration{10 * time.Second, 70 * time.Second}},
		{"the end is before the assertion's first scrape", 60 * time.Second, []time.Duration{10 * time.Second}},
	} {
		out := convert(t, []engine.Record{watchRec(1, lifecycle.Observe, 10*time.Second)}, 1, func(o *vmmodel.Options) { o.Until = t0.Add(c.until) })
		if want := scrapedAt(series, c.want...); !slices.Equal(sortKeyed(out.samples), want) {
			t.Errorf("%s\n got %v\nwant %v", c.name, out.samples, want)
		}
	}
}

// A delete that arrives before the assertion it ends (the stream's arrival is out of
// order) still ends it, because the records of a watch-mode producer are handled in
// event-time order; and a second assertion of a series that is being scraped writes
// nothing.
func TestWatchRecordsAreHandledInEventTimeOrder(t *testing.T) {
	series := vmmodel.EdgeSeries(catalog.L2, fixture.FP(catalog.K8sPod, 21), fixture.FP(catalog.K8sNode, 1), catalog.ScheduledOn, "k8sobjects").Key()
	recs := []engine.Record{
		watchRec(1, lifecycle.Delete, 100*time.Second),
		watchRec(2, lifecycle.Observe, 10*time.Second),
		watchRec(3, lifecycle.Observe, 40*time.Second), // the scraper was writing it already
	}
	out := convert(t, recs, 1)
	want := append(scrapedAt(series, 10*time.Second, 70*time.Second), keyed{series, ms(100 * time.Second), true})
	if got := sortKeyed(out.samples); !slices.Equal(got, sortKeyed(want)) {
		t.Errorf("\n got %v\nwant %v", got, want)
	}
	if out.counts.Reasserted != 1 || out.counts.Scraped != 1 {
		t.Errorf("%+v", out.counts)
	}
	// The same records in the order of their event times are the same samples.
	again := convert(t, []engine.Record{
		watchRec(1, lifecycle.Observe, 10*time.Second),
		watchRec(2, lifecycle.Observe, 40*time.Second),
		watchRec(3, lifecycle.Delete, 100*time.Second),
	}, 1)
	if got := sortKeyed(again.samples); !slices.Equal(got, sortKeyed(want)) {
		t.Errorf("in event-time order\n got %v\nwant %v", got, want)
	}
}

// A fact deleted and asserted again is two intervals, each scraped from its own assertion.
func TestAWatchFactAssertedAgainIsScrapedFromTheNewAssertion(t *testing.T) {
	series := vmmodel.EdgeSeries(catalog.L2, fixture.FP(catalog.K8sPod, 21), fixture.FP(catalog.K8sNode, 1), catalog.ScheduledOn, "k8sobjects").Key()
	out := convert(t, []engine.Record{
		watchRec(1, lifecycle.Observe, 0),
		watchRec(2, lifecycle.Delete, 100*time.Second),
		watchRec(3, lifecycle.Observe, 130*time.Second),
	}, 1, func(o *vmmodel.Options) { o.Until = t0.Add(260 * time.Second) })
	want := scrapedAt(series, 0, 60*time.Second)
	want = append(want, keyed{series, ms(100 * time.Second), true})
	want = append(want, scrapedAt(series, 130*time.Second, 190*time.Second, 250*time.Second)...)
	if got := sortKeyed(out.samples); !slices.Equal(got, sortKeyed(want)) {
		t.Errorf("\n got %v\nwant %v", got, want)
	}
}

// A registry with a producer in watch mode needs the end of the period, and a watch-mode
// record carries no TTL.
func TestWatchModeRefusals(t *testing.T) {
	if _, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t)}); err == nil {
		t.Error("a converter for producers in watch mode with no end of the period")
	}
	r := watchRec(1, lifecycle.Observe, 0)
	r.TTL = time.Minute
	c, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: until})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write([]engine.Record{r}); !errors.Is(err, vmmodel.ErrWindow) {
		t.Errorf("a TTL on a watch-mode record: %v", err)
	}
	// A registry with refreshing producers only needs none.
	reg, err := vmmodel.NewRegistry(map[catalog.RelationType][]vmmodel.Holder{catalog.RunsOn: {{Producer: "p", Window: time.Minute}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vmmodel.NewConverter(vmmodel.Options{Registry: reg}); err != nil {
		t.Errorf("refreshing producers only: %v", err)
	}
}

// A long-lived fact is written out in chunks, so that a period of months for many facts
// is not held in memory: no call to Emit gets more than the chunk, and every sample is
// emitted once.
func TestScrapesAreEmittedInChunks(t *testing.T) {
	var calls, total, biggest int
	end := 120 * 24 * time.Hour
	c, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), Until: t0.Add(end), Emit: func(ss []vmmodel.Sample) error {
		calls++
		total += len(ss)
		biggest = max(biggest, len(ss))
		return nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Write([]engine.Record{watchRec(1, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	want := int(end/vmmodel.WatchScrape) + 1
	if total != want || c.Counts().Samples != uint64(want) || c.Counts().Scraped != uint64(want-1) {
		t.Errorf("emitted %d samples, counted %+v, want %d", total, c.Counts(), want)
	}
	if calls < 2 || biggest > 1<<16 {
		t.Errorf("%d calls, the biggest %d", calls, biggest)
	}
}
