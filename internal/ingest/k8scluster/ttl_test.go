package k8scluster_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// wantTTL fails the test unless every Observe in rs has the TTL and every Delete
// has none. It returns the number of Observes it saw.
func wantTTL(t testing.TB, what string, rs []store.Record, ttl time.Duration) int {
	t.Helper()
	n := 0
	for _, r := range rs {
		switch r.Kind {
		case lifecycle.Observe:
			n++
			if r.TTL != ttl {
				t.Errorf("%s: Observe of %v has TTL %s, want %s", what, r.Subject, r.TTL, ttl)
			}
		case lifecycle.Delete:
			if r.TTL != 0 {
				t.Errorf("%s: Delete of %v has TTL %s, want 0", what, r.Subject, r.TTL)
			}
		}
	}
	return n
}

// cluster runs a node, a pod on it and a container of the pod through tr, with
// the given interval options on each event, and returns all the records.
func cluster(t testing.TB, tr *k8scluster.Translator, at time.Time, opts func(ent) ent) []store.Record {
	t.Helper()
	var out []store.Record
	for _, e := range []ent{
		nodeEv("n-1", "node-a", at, time.Time{}),
		podEv("p-1", "p", "node-a", at, time.Time{}),
		ctrEv("containerd://c1", "p-1", "one", at, time.Time{}),
	} {
		out = append(out, translate(t, tr, opts(e)).Records...)
	}
	return out
}

func keep(e ent) ent { return e }

func TestTTLFromReportInterval(t *testing.T) {
	t.Run("a report interval of 60 seconds", func(t *testing.T) {
		tr := newTranslator(t, collection(5*time.Minute))
		rs := cluster(t, tr, t0, keep)
		// node, pod, scheduled_on, container, part_of
		if n := wantTTL(t, "states", rs, 3*time.Minute); n != 5 {
			t.Errorf("%d Observes, want 5: %s", n, describeRecords(rs))
		}
		var dels []store.Record
		for _, e := range []ent{
			deleteEv("container", "c1", t0.Add(time.Minute)),
			deleteEv("k8s.pod", "p-1", t0.Add(time.Minute)),
			deleteEv("k8s.node", "n-1", t0.Add(time.Minute)),
		} {
			dels = append(dels, translate(t, tr, e).Records...)
		}
		if len(dels) == 0 {
			t.Fatal("the deletes gave no records")
		}
		wantTTL(t, "deletes", dels, 0)
	})

	t.Run("an event with no interval takes the collection interval", func(t *testing.T) {
		for _, c := range []struct {
			collection, want time.Duration
		}{{5 * time.Minute, 15 * time.Minute}, {30 * time.Second, 90 * time.Second}, {0, 0}} {
			tr := newTranslator(t, collection(c.collection))
			rs := cluster(t, tr, t0, func(e ent) ent { return e.noInterval() })
			if n := wantTTL(t, fmt.Sprintf("collection interval %s", c.collection), rs, c.want); n != 5 {
				t.Errorf("%d Observes, want 5", n)
			}
		}
	})

	t.Run("an interval of zero is no TTL", func(t *testing.T) {
		tr := newTranslator(t, collection(5*time.Minute))
		rs := cluster(t, tr, t0, func(e ent) ent { return e.interval(0) })
		if n := wantTTL(t, "interval 0", rs, 0); n != 5 {
			t.Errorf("%d Observes, want 5", n)
		}
	})

	t.Run("a changed interval applies from that record", func(t *testing.T) {
		tr := newTranslator(t)
		step := func(i int64, at time.Time) k8scluster.Result {
			return translate(t, tr, nodeEv("n-1", "a", at, time.Time{}).interval(i))
		}
		r := step(60, t0)
		if got := only(t, r).TTL; got != 3*time.Minute || len(r.Notes) != 0 {
			t.Errorf("first record: TTL %s notes %v, want 3m and none: the first interval is not a change", got, r.Notes)
		}
		r = step(60, t0.Add(time.Minute))
		if got := only(t, r).TTL; got != 3*time.Minute || len(r.Notes) != 0 {
			t.Errorf("same interval: TTL %s notes %v", got, r.Notes)
		}
		r = step(120, t0.Add(2*time.Minute))
		if got := only(t, r).TTL; got != 6*time.Minute {
			t.Errorf("record with a changed interval: TTL %s, want 6m", got)
		}
		if len(r.Notes) != 1 || r.Notes[0].Reason != k8scluster.NoteReportIntervalChanged || r.Notes[0].Count != 1 {
			t.Errorf("notes = %+v, want one %q", r.Notes, k8scluster.NoteReportIntervalChanged)
		}
		// The delete has no interval and leaves the current one alone.
		translate(t, tr, deleteEv("k8s.pod", "p-9", t0.Add(3*time.Minute)))
		r = step(120, t0.Add(4*time.Minute))
		if got := only(t, r).TTL; got != 6*time.Minute || len(r.Notes) != 0 {
			t.Errorf("after a delete: TTL %s notes %v, want 6m and none", got, r.Notes)
		}
		// Edges follow: they are written at the TTL of their entity.
		translate(t, tr, nodeEv("n-2", "b", t0.Add(5*time.Minute), time.Time{}).interval(120))
		rs := translate(t, tr, podEv("p-1", "p", "b", t0.Add(6*time.Minute), time.Time{}).interval(120)).Records
		if n := wantTTL(t, "pod and edge at 120s", rs, 6*time.Minute); n != 2 {
			t.Errorf("%d Observes, want the pod and its edge", n)
		}
		// An event with no interval is an interval of the collection interval.
		r = translate(t, tr, nodeEv("n-1", "a", t0.Add(7*time.Minute), time.Time{}).noInterval().with("k8s.node.condition_ready", "false"))
		if got := only(t, r).TTL; got != 0 || noteCount(r, k8scluster.NoteReportIntervalChanged) != 1 {
			t.Errorf("no interval: TTL %s notes %v, want 0 and a change noted", got, r.Notes)
		}
	})
}

func TestThroughRule(t *testing.T) {
	created := t0.Add(-2 * time.Hour)

	t.Run("a first sighting 2h after creation", func(t *testing.T) {
		tr := newTranslator(t)
		translate(t, tr, nodeEv("n-1", "a", t0, created.Add(-time.Hour)))
		rs := translate(t, tr, podEv("p-1", "p", "a", t0, created)).Records
		if len(rs) != 2 {
			t.Fatalf("gave %s", describeRecords(rs))
		}
		wantTimes(t, "pod", rs[0], created, store.BasisObjectField, t0)
		// The edge is not about the pod's creation: it begins at the instant.
		wantTimes(t, "edge", rs[1], t0, store.BasisProducerEvent, time.Time{})
		rs = translate(t, tr, ctrEv("containerd://c1", "p-1", "one", t0, created)).Records
		if len(rs) != 2 {
			t.Fatalf("gave %s", describeRecords(rs))
		}
		for _, r := range rs {
			wantTimes(t, "container or edge", r, created, store.BasisObjectField, t0)
		}
	})

	t.Run("a clock ahead", func(t *testing.T) {
		tr := newTranslator(t)
		ahead := t0.Add(2 * time.Minute)
		r := only(t, translate(t, tr, nodeEv("n-1", "a", t0, ahead)))
		wantTimes(t, "creation 2 minutes after the instant", r, ahead, store.BasisObjectField, time.Time{})

		// Exactly MaxClockAhead is still plausible; a second more is not.
		r = only(t, translate(t, tr, nodeEv("n-2", "b", t0, t0.Add(k8scluster.MaxClockAhead))))
		wantTimes(t, "creation MaxClockAhead after the instant", r, t0.Add(k8scluster.MaxClockAhead), store.BasisObjectField, time.Time{})
		res := translate(t, tr, nodeEv("n-3", "c", t0, t0.Add(k8scluster.MaxClockAhead+time.Second)))
		wantTimes(t, "creation too far ahead", only(t, res), t0, store.BasisProducerEvent, time.Time{})
		if skipCount(res, k8scluster.SkipImplausibleTime) != 1 {
			t.Errorf("skips = %v, want %q", res.Skipped, k8scluster.SkipImplausibleTime)
		}

		// The next record of n-1 is at its instant, which is before the previous
		// record's time: it is raised to it, and is then after its own instant.
		res = translate(t, tr, nodeEv("n-1", "a", t0.Add(time.Minute), ahead))
		wantTimes(t, "raised", only(t, res), ahead, store.BasisObjectField, time.Time{})
		if skipCount(res, k8scluster.SkipClampedTime) != 1 {
			t.Errorf("skips = %v, want %q", res.Skipped, k8scluster.SkipClampedTime)
		}
	})

	t.Run("a timely record and a delete", func(t *testing.T) {
		tr := newTranslator(t)
		r := only(t, translate(t, tr, nodeEv("n-1", "a", t0, time.Time{})))
		wantTimes(t, "state at its instant", r, t0, store.BasisProducerEvent, time.Time{})
		d := only(t, translate(t, tr, deleteEv("k8s.node", "n-1", t0.Add(time.Hour))))
		wantTimes(t, "delete", d, t0.Add(time.Hour), store.BasisProducerEvent, time.Time{})
		// Nor does a delete that is raised to the time of the record before it.
		translate(t, tr, nodeEv("n-2", "b", t0, t0.Add(4*time.Minute)))
		d = only(t, translate(t, tr, deleteEv("k8s.node", "n-2", t0)))
		wantTimes(t, "raised delete", d, t0.Add(4*time.Minute), store.BasisObjectField, time.Time{})
	})
}

func TestTrustedClockPlausibility(t *testing.T) {
	rcv := t0
	state := func(tn time.Time) ent { return nodeEv("n-1", "a", tn, time.Time{}) }

	type result struct {
		r   k8scluster.Result
		err error
	}
	run := func(e ent, received time.Time) result {
		r, err := newTranslator(t).Translate(noResource, e.rec(), received)
		return result{r, err}
	}
	check := func(name string, got result, at time.Time, basis store.EventTimeBasis, implausible, behind int) {
		t.Helper()
		if got.err != nil {
			t.Errorf("%s: %v", name, got.err)
			return
		}
		rec := only(t, got.r)
		wantTimes(t, name, rec, at, basis, time.Time{})
		if n := skipCount(got.r, k8scluster.SkipImplausibleTime); n != implausible {
			t.Errorf("%s: %d implausible-time skips, want %d (%v)", name, n, implausible, got.r.Skipped)
		}
		if n := noteCount(got.r, k8scluster.NoteClockBehind); n != behind {
			t.Errorf("%s: %d clock-behind notes, want %d (%v)", name, n, behind, got.r.Notes)
		}
	}

	check("received not given", run(state(t0), time.Time{}), t0, store.BasisProducerEvent, 0, 0)
	check("received equal", run(state(rcv), rcv), rcv, store.BasisProducerEvent, 0, 0)

	// Ahead of receipt.
	check("MaxClockAhead + 1ns ahead", run(state(rcv.Add(k8scluster.MaxClockAhead+1)), rcv), rcv, store.BasisReceipt, 1, 0)
	check("exactly MaxClockAhead ahead", run(state(rcv.Add(k8scluster.MaxClockAhead)), rcv), rcv.Add(k8scluster.MaxClockAhead), store.BasisProducerEvent, 0, 0)

	// Behind receipt: the time is kept, and the caller is told.
	behind := rcv.Add(-ttl60 - 1)
	check("a TTL + 1ns behind", run(state(behind), rcv), behind, store.BasisProducerEvent, 0, 1)
	check("exactly a TTL behind", run(state(rcv.Add(-ttl60)), rcv), rcv.Add(-ttl60), store.BasisProducerEvent, 0, 0)
	check("long behind", run(state(rcv.Add(-24*time.Hour)), rcv), rcv.Add(-24*time.Hour), store.BasisProducerEvent, 0, 1)
	// With no TTL there is no lateness to note.
	check("long behind, no TTL", run(state(rcv.Add(-24*time.Hour)).interval(0), rcv), rcv.Add(-24*time.Hour), store.BasisProducerEvent, 0, 0)

	// A zero time.
	check("no time, received given", run(state(time.Time{}), rcv), rcv, store.BasisReceipt, 0, 0)
	if got := run(state(time.Time{}), time.Time{}); !errors.Is(got.err, k8scluster.ErrNoTime) {
		t.Errorf("no time and no receipt: error = %v, want ErrNoTime", got.err)
	}

	// A time that is not a time of day.
	huge := state(time.Time{})
	lr := huge.rec()
	lr.TimeUnixNano = capture.Uint64(1 << 63)
	if r, err := newTranslator(t).Translate(noResource, lr, rcv); err != nil {
		t.Errorf("a time past the end of time: %v", err)
	} else {
		wantTimes(t, "a time past the end of time", only(t, r), rcv, store.BasisReceipt, time.Time{})
		if skipCount(r, k8scluster.SkipImplausibleTime) != 1 {
			t.Errorf("skips = %v", r.Skipped)
		}
	}
	if _, err := newTranslator(t).Translate(noResource, lr, time.Time{}); !errors.Is(err, k8scluster.ErrNoTime) {
		t.Errorf("a time past the end of time and no receipt: error = %v, want ErrNoTime", err)
	}

	// A delete is judged against the TTL the translator is using.
	tr := newTranslator(t)
	translate(t, tr, state(rcv))
	r, err := tr.Translate(noResource, deleteEv("k8s.node", "n-1", rcv.Add(-time.Hour)).rec(), rcv)
	if err != nil {
		t.Fatal(err)
	}
	if noteCount(r, k8scluster.NoteClockBehind) != 1 {
		t.Errorf("a delete an hour behind: notes = %v", r.Notes)
	}
	wantTimes(t, "delete", only(t, r), rcv, store.BasisProducerEvent, time.Time{})
}

// lineWithInterval is a node state as the collector writes it in OTLP/JSON, with
// the given JSON for the value of entity.report.interval.
func lineWithInterval(value string) string {
	return `{"resourceLogs":[{"resource":{},"scopeLogs":[{"scope":{},"logRecords":[{"timeUnixNano":"1791563574304684247","body":{},"attributes":[` +
		`{"key":"entity.id","value":{"kvlistValue":{"values":[{"key":"k8s.node.uid","value":{"stringValue":"n-1"}}]}}},` +
		`{"key":"entity.type","value":{"stringValue":"k8s.node"}},` +
		`{"key":"entity.report.interval","value":` + value + `},` +
		`{"key":"entity.description","value":{"kvlistValue":{"values":[{"key":"k8s.node.name","value":{"stringValue":"a"}}]}}}` +
		`],"eventName":"entity.state"}]}]}]}`
}

func decodeLine(t testing.TB, line string) (capture.LogRecord, error) {
	t.Helper()
	d, err := capture.DecodeLogs(json.RawMessage(line))
	if err != nil {
		return capture.LogRecord{}, err
	}
	return d.ResourceLogs[0].ScopeLogs[0].LogRecords[0], nil
}

func TestReportIntervalAsString(t *testing.T) {
	// On the wire a 64-bit integer is a decimal string; a number is read too.
	for _, value := range []string{`{"intValue":"60"}`, `{"intValue":60}`, `{"intValue":"6e1"}`} {
		lr, err := decodeLine(t, lineWithInterval(value))
		if err != nil {
			t.Fatalf("%s: %v", value, err)
		}
		r, err := newTranslator(t).Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatalf("%s: %v", value, err)
		}
		if got := only(t, r).TTL; got != 3*time.Minute {
			t.Errorf("interval %s: TTL = %s, want 3m", value, got)
		}
		if hasSkip(r, k8scluster.SkipBadInterval) {
			t.Errorf("interval %s counted as bad", value)
		}
	}

	// A decimal with a fraction is not an integer: the capture package refuses the
	// whole record, so it never reaches the translator.
	if _, err := decodeLine(t, lineWithInterval(`{"intValue":"60.5"}`)); !errors.Is(err, capture.ErrFormat) {
		t.Errorf("an intValue of 60.5: error = %v, want one wrapping capture.ErrFormat", err)
	}

	// Every other kind of value, and the integers that give no usable TTL, are
	// refused for the record.
	for _, value := range []string{
		`{"stringValue":"60"}`, `{"stringValue":"60.5"}`, `{"doubleValue":60}`, `{"doubleValue":60.5}`,
		`{"boolValue":true}`, `{"intValue":"-1"}`, `{"intValue":-1}`, `{}`,
		`{"intValue":"99999999999"}`, `{"intValue":"9223372036854775807"}`,
	} {
		lr, err := decodeLine(t, lineWithInterval(value))
		if err != nil {
			t.Fatalf("%s: %v", value, err)
		}
		tr := newTranslator(t)
		r, err := tr.Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatalf("%s: %v", value, err)
		}
		if len(r.Records) != 0 || skipCount(r, k8scluster.SkipBadInterval) != 1 {
			t.Errorf("interval %s: %d records and skips %v, want none and one %q", value, len(r.Records), r.Skipped, k8scluster.SkipBadInterval)
		}
		// Nothing was kept: the same node is still new to the translator.
		good, err := decodeLine(t, lineWithInterval(`{"intValue":"60"}`))
		if err != nil {
			t.Fatal(err)
		}
		r, err = tr.Translate(noResource, good, time.Time{})
		if err != nil || len(r.Records) != 1 {
			t.Errorf("after interval %s: %v, %d records", value, err, len(r.Records))
		}
		if n := noteCount(r, k8scluster.NoteReportIntervalChanged); n != 0 {
			t.Errorf("after interval %s: %d interval changes noted", value, n)
		}
	}

	// A year is the longest interval taken, and a TTL of three of them is far from
	// the end of the representable times.
	for _, secs := range []int64{366 * 24 * 3600, 366*24*3600 + 1} {
		lr, err := decodeLine(t, lineWithInterval(fmt.Sprintf(`{"intValue":"%d"}`, secs)))
		if err != nil {
			t.Fatal(err)
		}
		r, err := newTranslator(t).Translate(noResource, lr, time.Time{})
		if err != nil {
			t.Fatal(err)
		}
		ok := secs == 366*24*3600
		if ok != (len(r.Records) == 1) || ok == hasSkip(r, k8scluster.SkipBadInterval) {
			t.Errorf("interval of %d s: %d records, skips %v, want taken = %v", secs, len(r.Records), r.Skipped, ok)
		}
		if ok {
			rec := only(t, r)
			rec.Seq = 1
			if err := rec.Validate(); err != nil {
				t.Errorf("record with the longest interval does not validate: %v", err)
			}
		}
	}
}
