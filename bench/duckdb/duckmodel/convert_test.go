package duckmodel

import (
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// held maps each field of an engine record to the field of a store record that holds it,
// and storeOnly lists the fields a store record has that an engine record does not.
var (
	held = map[string]string{
		"Layer": "Layer", "Subject": "Subject", "Producer": "Producer", "EventTime": "EventTime", "Seq": "Seq",
		"Kind": "Kind", "TTL": "TTL", "Through": "Through", "Payload": "Payload",
	}
	storeOnly = []string{"Boot", "EventTimeBasis"}
)

func fieldNames(v any) []string {
	rt := reflect.TypeOf(v)
	out := make([]string, rt.NumField())
	for i := range out {
		out[i] = rt.Field(i).Name
	}
	slices.Sort(out)
	return out
}

// TestEveryFieldOfBothRecordsIsAccountedFor fails when either record type gains a field:
// the conversion must then say where it goes or why it is not held.
func TestEveryFieldOfBothRecordsIsAccountedFor(t *testing.T) {
	var want []string
	for k := range held {
		want = append(want, k)
	}
	slices.Sort(want)
	if got := fieldNames(engine.Record{}); !slices.Equal(got, want) {
		t.Errorf("the fields of engine.Record are %v; the conversion accounts for %v", got, want)
	}
	var wantStore []string
	for _, v := range held {
		wantStore = append(wantStore, v)
	}
	wantStore = append(wantStore, storeOnly...)
	slices.Sort(wantStore)
	if got := fieldNames(store.Record{}); !slices.Equal(got, wantStore) {
		t.Errorf("the fields of store.Record are %v; the conversion accounts for %v", got, wantStore)
	}
}

// full is an engine record with every field set to a value other than its zero.
func full(t testingT) engine.Record {
	t.Helper()
	return engine.Record{
		Layer: catalog.L2, Subject: engine.EdgeSubject(fp(t, "k8s.pod", 1), fp(t, "k8s.node", 2), catalog.ScheduledOn),
		Producer: "p/1", EventTime: time.Unix(100, 7).UTC(), Seq: 42, Kind: lifecycle.Observe, TTL: 90 * time.Second,
		Through: time.Unix(160, 9).UTC(), Payload: []byte("payload"),
	}
}

// conversionErr returns what a conversion gets wrong about the fields of an engine record
// that a store record holds, or "".
func conversionErr(t testingT, to func(engine.Record) (store.Record, error)) string {
	t.Helper()
	r := full(t)
	rv := reflect.ValueOf(r)
	for i := range rv.NumField() {
		if rv.Field(i).IsZero() {
			return "the sample record leaves " + rv.Type().Field(i).Name + " zero"
		}
	}
	got, err := to(r)
	if err != nil {
		t.Fatal(err)
	}
	gv := reflect.ValueOf(got)
	for from, into := range held {
		a, b := rv.FieldByName(from), gv.FieldByName(into)
		if b.IsZero() {
			return into + " is zero in the store record"
		}
		if from == "Subject" {
			s := got.Subject
			if s.A != r.Subject.A || s.B != r.Subject.B || s.Relation != r.Subject.Relation || s.Kind != store.SubjectEdge {
				return "Subject"
			}
			continue
		}
		if !reflect.DeepEqual(a.Interface(), b.Interface()) {
			return from
		}
	}
	return ""
}

func TestConversionCarriesEveryField(t *testing.T) {
	if d := conversionErr(t, StoreRecord); d != "" {
		t.Fatalf("StoreRecord: %s", d)
	}
	if d := conversionErr(t, func(r engine.Record) (store.Record, error) { return storeRecord(r, mutConvertDropsThrough) }); d == "" {
		t.Fatal("a conversion that drops the through time was not caught")
	}
}

// TestConversionRoundTrips converts every record of a workload to a store record, which
// must be valid, and back.
func TestConversionRoundTrips(t *testing.T) {
	g, err := workload.New(conformance.Configs()[5])
	if err != nil {
		t.Fatal(err)
	}
	n, entities, deletes, runs := 0, 0, 0, 0
	for _, r := range g.All() {
		s, err := StoreRecord(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Validate(); err != nil {
			t.Fatalf("seq %d: the store record is invalid: %v", r.Seq, err)
		}
		back, err := EngineRecord(s, Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(back, r) {
			t.Fatalf("seq %d: %+v came back as %+v", r.Seq, r, back)
		}
		n++
		if r.Subject.Kind == engine.SubjectEntity {
			entities++
		}
		if r.Kind == lifecycle.Delete {
			deletes++
		}
		if !r.Through.IsZero() {
			runs++
		}
	}
	if n == 0 || entities == 0 || deletes == 0 || runs == 0 {
		t.Fatalf("the workload has %d records, %d of entities, %d deletes, %d runs: not every shape was converted", n, entities, deletes, runs)
	}
}

func TestEngineRecordRefusesWhatItCannotHold(t *testing.T) {
	r := full(t)
	s, err := StoreRecord(r)
	if err != nil {
		t.Fatal(err)
	}
	if s.Boot != "" || s.EventTimeBasis != store.BasisUnknown {
		t.Fatalf("the two fields only a store record holds are %q and %s", s.Boot, s.EventTimeBasis)
	}
	withBoot, withBasis := s, s
	withBoot.Boot = "boot-1"
	withBasis.EventTimeBasis = store.BasisProducerEvent
	for name, c := range map[string]struct {
		rec  store.Record
		with Options
	}{
		"a boot id":           {withBoot, Options{DropBoot: true}},
		"an event time basis": {withBasis, Options{DropBasis: true}},
	} {
		if _, err := EngineRecord(c.rec, Options{}); err == nil {
			t.Errorf("%s was dropped without being asked to", name)
		}
		if got, err := EngineRecord(c.rec, c.with); err != nil || !reflect.DeepEqual(got, r) {
			t.Errorf("%s with the option: %v, %v", name, got, err)
		}
	}
	bad := s
	bad.Subject.Kind = 0
	if _, err := EngineRecord(bad, Options{}); err == nil {
		t.Error("a store record with no subject kind was converted")
	}
	r.Subject.Kind = 0
	if _, err := StoreRecord(r); err == nil {
		t.Error("an engine record with no subject kind was converted")
	}
}
