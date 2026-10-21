package vmmodel_test

import (
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

func convert(t fataler, recs []engine.Record, batch int, opts ...func(*vmmodel.Options)) outcome {
	t.Helper()
	var out outcome
	o := vmmodel.Options{Registry: registry(t)}
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
// after a record at 950 s. Pod 1's placement: the scheduler's one assertion at 10 s, the
// pod collector's refresh at 900 s, and the scheduler's delete at 950 s (a marker). Pod 2:
// an assertion and a delete 500 ns apart, truncated to one millisecond, so the later by
// sequence number (the delete) is kept. Pod 3: a delete and an assertion in one
// millisecond, so the assertion is kept, as a flap leaves the pod placed.
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
// order of the records that made them, the series, and the digest of both.
//
// It catches a marker written on expiry (the node's series must end with its last sample),
// no marker on a Delete, nanoseconds passed as milliseconds, and a collapse that keeps the
// first record of a millisecond instead of the last by sequence number.
func TestConversionGolden(t *testing.T) {
	out := convert(t, fixtureStream(), 4)

	node := fixture.FP(catalog.K8sNode, 1)
	pod1, pod2, pod3 := fixture.FP(catalog.K8sPod, 11), fixture.FP(catalog.K8sPod, 12), fixture.FP(catalog.K8sPod, 13)
	nodeS := vmmodel.EntitySeries(catalog.L2, node, "node-collector").Key()
	p1o := vmmodel.EdgeSeries(catalog.L2, pod1, node, catalog.ScheduledOn, "k8sobjects").Key()
	p1c := vmmodel.EdgeSeries(catalog.L2, pod1, node, catalog.ScheduledOn, "k8s-cluster").Key()
	p2o := vmmodel.EdgeSeries(catalog.L2, pod2, node, catalog.ScheduledOn, "k8sobjects").Key()
	p3o := vmmodel.EdgeSeries(catalog.L2, pod3, node, catalog.ScheduledOn, "k8sobjects").Key()
	want := []keyed{
		{nodeS, ms(0), false},
		{nodeS, ms(60 * time.Second), false},
		{nodeS, ms(120 * time.Second), false},
		{p1o, ms(10 * time.Second), false},
		{p1c, ms(900 * time.Second), false},
		{p1o, ms(950 * time.Second), true}, // the delete is a marker
		{p2o, ms(30 * time.Second), true},  // the delete, which came later than the observe in the same millisecond
		{p3o, ms(40 * time.Second), false}, // the observe, which came later than the delete
		{nodeS, ms(30 * time.Second), false},
	}
	if !slices.Equal(out.samples, want) {
		t.Fatalf("samples\n got %v\nwant %v", out.samples, want)
	}

	c := out.counts
	if c.Records != 11 || c.Observes != 8 || c.Deletes != 3 {
		t.Errorf("read %d records, %d observes, %d deletes", c.Records, c.Observes, c.Deletes)
	}
	if c.Samples != 7 || c.Markers != 2 || c.CollapsedObserves != 1 || c.CollapsedDeletes != 1 || c.Collapses() != 2 {
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
	if c.Samples+c.Markers+c.Collapses() != c.Records {
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
	const pinned = "0c6520805ec56c5c19175608f38cfb0265d928392d18057ce65ceb0da9715d14"
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
	c, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), ReorderWindow: time.Hour})
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
		cv, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t)})
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
		if _, err := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), ReorderWindow: w}); err == nil {
			t.Errorf("reorder window %s accepted", w)
		}
	}
	// A failing sink closes the converter with its error.
	boom := errors.New("boom")
	c, _ := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t), ReorderWindow: time.Second, Emit: func([]vmmodel.Sample) error { return boom }})
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
	d, _ := vmmodel.NewConverter(vmmodel.Options{Registry: registry(t)})
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
// of them late.
func genStream(t *rapid.T, reg *vmmodel.Registry) []engine.Record {
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
		h := rapid.SampledFrom(hs).Draw(t, "holder")
		layer := catalog.L2
		if s.Relation == catalog.RunsOn {
			layer = catalog.L1
		}
		r := engine.Record{
			Layer: layer, Subject: s, Producer: h.Producer, Seq: uint64(i + 1),
			EventTime: t0.Add(time.Duration(rapid.IntRange(0, 25).Draw(t, "ms"))*time.Millisecond + time.Duration(rapid.IntRange(0, 999_999).Draw(t, "ns"))),
		}
		if rapid.IntRange(0, 3).Draw(t, "delete") == 0 {
			r.Kind = lifecycle.Delete
		} else {
			r.Kind, r.TTL = lifecycle.Observe, h.Window
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
		recs := genStream(rt, reg)
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
		recs := genStream(rt, reg)
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
