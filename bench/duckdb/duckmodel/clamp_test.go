package duckmodel

import (
	"bytes"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func TestClampInstant(t *testing.T) {
	for _, c := range []struct {
		name string
		t    time.Time
		ns   int64
		ok   bool
	}{
		{"far before the range", time.Unix(-1<<40, 0), 0, false},
		{"a nanosecond before the range", engine.MinEventTime.Add(-time.Nanosecond), 0, false},
		{"the first instant", engine.MinEventTime, 0, true},
		{"after the first instant", engine.MinEventTime.Add(time.Nanosecond), 1, true},
		{"before the last instant", engine.MaxEventTime.Add(-time.Nanosecond), math.MaxInt64 - 1, true},
		{"the last instant", engine.MaxEventTime, math.MaxInt64, true},
		{"a nanosecond after the range", engine.MaxEventTime.Add(time.Nanosecond), math.MaxInt64, true},
		{"far after the range", time.Unix(1<<40, 0), math.MaxInt64, true},
	} {
		if ns, ok := ClampInstant(c.t); ns != c.ns || ok != c.ok {
			t.Errorf("%s: ClampInstant = %d, %v; want %d, %v", c.name, ns, ok, c.ns, c.ok)
		}
	}
}

func TestClampWindow(t *testing.T) {
	min, max := engine.MinEventTime, engine.MaxEventTime
	for _, c := range []struct {
		name     string
		from, to time.Time
		want     Bounds
	}{
		{"an empty interval", time.Unix(5, 0), time.Unix(5, 0), Bounds{Empty: true}},
		{"a reversed interval", time.Unix(6, 0), time.Unix(5, 0), Bounds{Empty: true}},
		{"all before the range", time.Unix(-1<<40, 0), min, Bounds{Empty: true}},
		{"from after the range", max.Add(time.Nanosecond), time.Unix(1<<40, 0), Bounds{Empty: true}},
		{"inside", time.Unix(5, 0), time.Unix(6, 0), Bounds{Lo: 5e9, Hi: 6e9}},
		{"from before the range", time.Unix(-1<<40, 0), time.Unix(6, 0), Bounds{Lo: 0, Hi: 6e9}},
		{"to the last instant, which is excluded", time.Unix(5, 0), max, Bounds{Lo: 5e9, Hi: math.MaxInt64}},
		{"to after the last instant, which includes it", time.Unix(5, 0), max.Add(time.Nanosecond), Bounds{Lo: 5e9, Hi: math.MaxInt64, Unbounded: true}},
		{"from the last instant", max, time.Unix(1<<40, 0), Bounds{Lo: math.MaxInt64, Hi: math.MaxInt64, Unbounded: true}},
	} {
		if got := ClampWindow(c.from, c.to); got != c.want {
			t.Errorf("%s: ClampWindow = %+v, want %+v", c.name, got, c.want)
		}
	}
	b := ClampWindow(time.Unix(5, 0), time.Unix(6, 0))
	for ns, want := range map[int64]bool{5e9 - 1: false, 5e9: true, 6e9 - 1: true, 6e9: false} {
		if b.Contains(ns) != want {
			t.Errorf("a window [5s, 6s) contains %d: %v", ns, !want)
		}
	}
	if !ClampWindow(time.Unix(5, 0), max.Add(time.Second)).Contains(math.MaxInt64) {
		t.Error("a window past the range does not contain the last instant")
	}
	if ClampWindow(time.Unix(5, 0), time.Unix(5, 0)).Contains(5e9) {
		t.Error("an empty window contains an instant")
	}
}

func sameRecords(a, b []engine.Record) bool {
	return slices.EqualFunc(a, b, func(x, y engine.Record) bool {
		return x.Layer == y.Layer && x.Subject == y.Subject && x.Producer == y.Producer && x.EventTime.Equal(y.EventTime) &&
			x.Seq == y.Seq && x.Kind == y.Kind && x.TTL == y.TTL && x.Through.Equal(y.Through) && bytes.Equal(x.Payload, y.Payload)
	})
}

// extremesAgree writes records at the first and last instants the range holds to a model
// and to the reference engine, and asks both about instants and windows around and far
// outside the range. It returns the first difference.
func extremesAgree(t *testing.T, m engine.Engine) (diff string) {
	t.Helper()
	ora := oracle.New()
	a, b := fp(t, "k8s.pod", 1), fp(t, "k8s.node", 2)
	subjects := []engine.Subject{engine.EdgeSubject(a, b, catalog.ScheduledOn), engine.EdgeSubject(b, a, catalog.ScheduledOn)}
	recs := []engine.Record{
		{Layer: catalog.L2, Subject: subjects[0], Producer: "p", EventTime: engine.MinEventTime, Seq: 1, Kind: lifecycle.Observe, Payload: []byte{1}},
		{Layer: catalog.L2, Subject: subjects[0], Producer: "p", EventTime: engine.MinEventTime.Add(time.Nanosecond), Seq: 2, Kind: lifecycle.Delete},
		{Layer: catalog.L2, Subject: subjects[1], Producer: "p", EventTime: engine.MaxEventTime.Add(-time.Nanosecond), Seq: 3, Kind: lifecycle.Observe, Payload: []byte{3}},
		{Layer: catalog.L2, Subject: subjects[1], Producer: "q", EventTime: engine.MaxEventTime, Seq: 4, Kind: lifecycle.Observe, Payload: []byte{4}},
	}
	for _, e := range []engine.Engine{m, ora} {
		if err := e.Write(slices.Clone(recs)); err != nil {
			t.Fatal(err)
		}
	}
	instants := []time.Time{
		time.Unix(-1<<40, 0), engine.MinEventTime.Add(-time.Nanosecond), engine.MinEventTime, engine.MinEventTime.Add(time.Nanosecond),
		time.Unix(5, 0), engine.MaxEventTime.Add(-2 * time.Nanosecond), engine.MaxEventTime.Add(-time.Nanosecond), engine.MaxEventTime,
		engine.MaxEventTime.Add(time.Nanosecond), time.Unix(1<<40, 0),
	}
	sc := engine.Current(catalog.L2)
	for _, x := range instants {
		for _, ent := range []identity.Fingerprint{a, b} {
			for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
				got, err1 := m.Neighbors(ent, dir, x, sc)
				want, err2 := ora.Neighbors(ent, dir, x, sc)
				if err1 != nil || err2 != nil || !slices.Equal(got, want) {
					return "Neighbors " + x.Format(time.RFC3339Nano) + " " + dir.String()
				}
			}
		}
	}
	for _, from := range instants {
		for _, to := range instants {
			for _, ent := range []identity.Fingerprint{a, b} {
				for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
					got, err1 := m.Window(ent, dir, from, to, sc)
					want, err2 := ora.Window(ent, dir, from, to, sc)
					if err1 != nil || err2 != nil || !sameRecords(got, want) {
						return "Window [" + from.Format(time.RFC3339Nano) + ", " + to.Format(time.RFC3339Nano) + ") " + dir.String()
					}
				}
			}
		}
	}
	return ""
}

// TestReadsAtTheExtremesAgreeWithTheReferenceEngine asks about every instant around
// the ends of the range, and far outside it, and has two faults the clamps exist for
// fail: an instant converted without clamping overflows, and a window whose upper bound
// is inclusive reads the record at that bound.
func TestReadsAtTheExtremesAgreeWithTheReferenceEngine(t *testing.T) {
	for _, arm := range arms {
		if d := extremesAgree(t, New(arm)); d != "" {
			t.Errorf("%s: the model differs from the reference engine at %s", arm, d)
		}
		for name, mut := range map[string]mutant{"no clamp": mutNoClamp, "inclusive window bound": mutWindowInclusive} {
			if d := extremesAgree(t, newModel(arm, mut)); d == "" {
				t.Errorf("%s: the mutant %q was not caught", arm, name)
			}
		}
	}
}
