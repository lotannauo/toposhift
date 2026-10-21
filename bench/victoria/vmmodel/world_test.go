package vmmodel_test

import (
	"reflect"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var (
	wPod1, wPod2, wNode = fixture.FP(catalog.K8sPod, 1), fixture.FP(catalog.K8sPod, 2), fixture.FP(catalog.K8sNode, 3)
	wSubjects           = []engine.Subject{
		engine.EntitySubject(wNode),
		engine.EdgeSubject(wPod1, wNode, catalog.ScheduledOn),
		engine.EdgeSubject(wPod2, wNode, catalog.ScheduledOn),
		engine.EdgeSubject(wPod1, wNode, catalog.PartOf),
	}
)

func genWorldRecords(t *rapid.T) []engine.Record {
	n := rapid.IntRange(1, 40).Draw(t, "records")
	recs := make([]engine.Record, n)
	for i := range recs {
		s := rapid.SampledFrom(wSubjects).Draw(t, "subject")
		r := engine.Record{
			Layer: catalog.L2, Subject: s, Seq: uint64(i + 1),
			Producer:  rapid.SampledFrom([]lifecycle.Producer{"a", "b", "c"}).Draw(t, "producer"),
			EventTime: time.Unix(1+rapid.Int64Range(0, 60).Draw(t, "at"), 0).UTC(),
		}
		if rapid.IntRange(0, 3).Draw(t, "delete") == 0 {
			r.Kind = lifecycle.Delete
		} else {
			r.Kind = lifecycle.Observe
			r.Payload = []byte{byte(rapid.IntRange(0, 2).Draw(t, "payload"))}
			if rapid.Bool().Draw(t, "ttl") {
				r.TTL = time.Duration(rapid.Int64Range(1, 8).Draw(t, "ttl s")) * time.Second
			}
			if rapid.IntRange(0, 3).Draw(t, "run") == 0 {
				r.Through = r.EventTime.Add(time.Duration(rapid.Int64Range(0, 5).Draw(t, "through")) * time.Second)
			}
		}
		recs[i] = r
	}
	return recs
}

// The world folds with the reference's rule, so its answers are the reference engine's:
// the edges alive at an instant, in both directions, and the existence of an entity, for
// random streams with several producers, runs, deletes and shared instants.
func TestWorldAgreesWithTheReferenceEngine(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		recs := genWorldRecords(rt)
		o := oracle.New()
		w := vmmodel.NewWorld()
		for _, chunk := range [][]engine.Record{recs[:len(recs)/2], recs[len(recs)/2:]} {
			if len(chunk) == 0 {
				continue
			}
			if err := o.Write(chunk); err != nil {
				rt.Fatal(err)
			}
			if err := w.Write(chunk); err != nil {
				rt.Fatal(err)
			}
		}
		sc := engine.Current(catalog.L2)
		for at := int64(0); at <= 75; at++ {
			x := time.Unix(at, 0).UTC()
			for _, fp := range []identity.Fingerprint{wPod1, wPod2, wNode} {
				for _, d := range []engine.Direction{engine.Forward, engine.Reverse} {
					want, err := o.Neighbors(fp, d, x, sc)
					if err != nil {
						rt.Fatal(err)
					}
					got, err := w.Neighbors(fp, d, catalog.L2, x)
					if err != nil {
						rt.Fatal(err)
					}
					if len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
						rt.Fatalf("neighbors of %s %s at %d: got %v, want %v", fp, d, at, got, want)
					}
				}
			}
			wantAlive, err := o.Alive(wNode, x, sc)
			if err != nil {
				rt.Fatal(err)
			}
			gotAlive, err := w.AliveAt(engine.EntitySubject(wNode), x)
			if err != nil || gotAlive != wantAlive {
				rt.Fatalf("alive at %d: got %v (%v), want %v", at, gotAlive, err, wantAlive)
			}
		}
	})
}

func TestWorldKeepsOnlyWhatTouchesItsEntities(t *testing.T) {
	other := fixture.FP(catalog.K8sNode, 99)
	w := vmmodel.NewWorld(wPod1)
	recs := []engine.Record{
		{Layer: catalog.L2, Subject: engine.EdgeSubject(wPod1, wNode, catalog.ScheduledOn), Producer: "a", EventTime: time.Unix(1, 0), Seq: 1, Kind: lifecycle.Observe, TTL: time.Minute},
		{Layer: catalog.L2, Subject: engine.EdgeSubject(wPod2, other, catalog.ScheduledOn), Producer: "a", EventTime: time.Unix(1, 0), Seq: 2, Kind: lifecycle.Observe, TTL: time.Minute},
		{Layer: catalog.L2, Subject: engine.EntitySubject(other), Producer: "a", EventTime: time.Unix(1, 0), Seq: 3, Kind: lifecycle.Observe, TTL: time.Minute},
	}
	if err := w.Write(recs); err != nil {
		t.Fatal(err)
	}
	if got := w.Incident(wPod1, engine.Forward, catalog.L2); len(got) != 1 {
		t.Errorf("incident of the pinned pod: %v", got)
	}
	if got := w.Incident(wPod2, engine.Forward, catalog.L2); len(got) != 0 {
		t.Errorf("a record that touches no pinned entity was kept: %v", got)
	}
	if ivs, _ := w.Existence(engine.EntitySubject(other)); len(ivs) != 0 {
		t.Errorf("an entity nothing pinned touches has intervals: %v", ivs)
	}
	// An end of an edge is a touch: the node is pinned by being the target.
	w2 := vmmodel.NewWorld(wNode)
	if err := w2.Write(recs[:1]); err != nil {
		t.Fatal(err)
	}
	if got := w2.Incident(wNode, engine.Reverse, catalog.L2); len(got) != 1 {
		t.Errorf("reverse incident of the target: %v", got)
	}
}

func TestWorldRefusals(t *testing.T) {
	w := vmmodel.NewWorld()
	r := engine.Record{Layer: catalog.L2, Subject: wSubjects[1], Producer: "a", EventTime: time.Unix(1, 0), Seq: 2, Kind: lifecycle.Observe, TTL: time.Minute}
	if err := w.Write([]engine.Record{r}); err != nil {
		t.Fatal(err)
	}
	if err := w.Write([]engine.Record{r}); err == nil {
		t.Error("a sequence number that does not rise was accepted")
	}
	r.Seq, r.Layer = 3, catalog.L3
	if err := w.Write([]engine.Record{r}); err == nil {
		t.Error("a subject in two layers was accepted")
	}
	if err := w.Retain(time.Unix(5, 0)); err != nil {
		t.Error(err)
	}
	if got := w.Incident(wPod1, engine.Forward, catalog.Layer(9)); got != nil {
		t.Errorf("incident in a layer that is not one: %v", got)
	}
}
