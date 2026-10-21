package pginput_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pginput"
	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

func fp(typ catalog.EntityType, b byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = b
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod, node = fp(catalog.K8sPod, 1), fp(catalog.K8sNode, 2)
	base      = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

// records is a small stream: a pod and a node observed, the pod placed on the node,
// refreshed, and the placement deleted and made again.
func records() []store.Record {
	at := func(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }
	edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
	return []store.Record{
		{Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: at(0), Seq: 1, Kind: lifecycle.Observe, TTL: time.Minute, Payload: []byte("pod")},
		{Layer: catalog.L2, Subject: store.EntitySubject(node), Producer: "k8s", EventTime: at(0), Seq: 2, Kind: lifecycle.Observe, Payload: []byte("node")},
		{Layer: catalog.L2, Subject: edge, Producer: "k8s", EventTime: at(1), Seq: 3, Kind: lifecycle.Observe, TTL: 30 * time.Second, Payload: []byte("e1")},
		{Layer: catalog.L2, Subject: edge, Producer: "k8s", EventTime: at(10), Seq: 4, Kind: lifecycle.Observe, TTL: 30 * time.Second, Through: at(20), Payload: []byte("e2")},
		{Layer: catalog.L2, Subject: edge, Producer: "k8s", EventTime: at(50), Seq: 5, Kind: lifecycle.Delete},
		{Layer: catalog.L2, Subject: edge, Producer: "k8s", EventTime: at(60), Seq: 6, Kind: lifecycle.Observe, Payload: nil},
	}
}

func writeFile(t *testing.T, rs []store.Record) string {
	t.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 4})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "x.parquet")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestForEachReturnsTheFilesRecordsAsEngineRecordsInBatches(t *testing.T) {
	rs := records()
	path := writeFile(t, rs)
	var got []engine.Record
	var sizes []int
	info, err := pginput.ForEach(path, 4, pginput.Options{}, func(b []engine.Record) error {
		sizes = append(sizes, len(b))
		got = append(got, b...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Records != int64(len(rs)) || info.Digest == "" || !slices.Equal(sizes, []int{4, 2}) {
		t.Fatalf("info %+v, batch sizes %v", info, sizes)
	}
	for i, r := range rs {
		want, err := pginput.Convert(r, pginput.Options{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[i], want) {
			t.Errorf("record %d: %+v, want %+v", i, got[i], want)
		}
		if got[i].Seq != r.Seq || got[i].Subject.A != r.Subject.A || !got[i].EventTime.Equal(r.EventTime) {
			t.Errorf("record %d does not carry the stored record's identity: %+v", i, got[i])
		}
	}
}

func TestForEachStopsWhenAskedAndPassesErrorsOn(t *testing.T) {
	path := writeFile(t, records())
	calls := 0
	if _, err := pginput.ForEach(path, 2, pginput.Options{}, func([]engine.Record) error { calls++; return pginput.ErrStop }); err != nil || calls != 1 {
		t.Fatalf("stop: %d calls, %v", calls, err)
	}
	boom := errors.New("boom")
	if _, err := pginput.ForEach(path, 2, pginput.Options{}, func([]engine.Record) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("error: %v", err)
	}
	if _, err := pginput.ForEach(path, 0, pginput.Options{}, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a batch of 0 was accepted")
	}
	if _, err := pginput.ForEach(filepath.Join(t.TempDir(), "missing"), 2, pginput.Options{}, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a missing file was read")
	}
}

// TestLoadMakesTheIntervalsTheReferenceFolds loads the file into a model and into the
// reference engine, and compares the intervals the planner derived with the reference
// fold of the same records, with reads at the instants that matter.
func TestLoadMakesTheIntervalsTheReferenceFolds(t *testing.T) {
	path := writeFile(t, records())
	m, ora := pgmodel.New(), oracle.New()
	info, n, err := pginput.Load(m, path, 2, 0, pginput.Options{})
	if err != nil || n != 6 || info.Records != 6 {
		t.Fatalf("load: %d records, %+v, %v", n, info, err)
	}
	if _, _, err := pginput.Load(ora, path, 5, 0, pginput.Options{}); err != nil {
		t.Fatal(err)
	}
	if m.LastSeq() != 6 {
		t.Fatalf("last seq %d", m.LastSeq())
	}
	// The placement is alive over [1, 50) (a refresh at 10 through 20 with a 30 s TTL
	// holds it to 50, and the delete at 50 ends it) and from 60 on.
	tb := m.Tables()
	if len(tb.Edges) != 2 || len(tb.Entities) != 2 {
		t.Fatalf("tables: %d edge rows, %d entity rows: %+v", len(tb.Edges), len(tb.Entities), tb)
	}
	sc := engine.Current(catalog.L2)
	for _, s := range []int{-1, 0, 1, 49, 50, 59, 60, 1000} {
		at := base.Add(time.Duration(s) * time.Second)
		a, err := m.Neighbors(pod, engine.Forward, at, sc)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ora.Neighbors(pod, engine.Forward, at, sc)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(a, b) {
			t.Errorf("at %d s: %v, the reference engine %v", s, a, b)
		}
		if want := s >= 1 && s < 50 || s >= 60; (len(a) == 1) != want {
			t.Errorf("at %d s: %d neighbors, alive should be %v", s, len(a), want)
		}
	}
}

func TestLoadStopsAtTheLimit(t *testing.T) {
	path := writeFile(t, records())
	m := pgmodel.New()
	_, n, err := pginput.Load(m, path, 2, 3, pginput.Options{})
	if err != nil || n != 4 || m.LastSeq() != 4 {
		t.Fatalf("load to a limit of 3: %d records, seq %d, %v", n, m.LastSeq(), err)
	}
}

func TestConvertRefusesWhatAnEngineRecordCannotHold(t *testing.T) {
	r := records()[0]
	r.Boot = "boot-1"
	if _, err := pginput.Convert(r, pginput.Options{}); err == nil {
		t.Error("a boot id was dropped silently")
	}
	if _, err := pginput.Convert(r, pginput.Options{DropBasis: true}); err == nil {
		t.Error("DropBasis accepted a boot id")
	}
	if _, err := pginput.Convert(r, pginput.Options{DropBoot: true}); err != nil {
		t.Errorf("a boot id with DropBoot: %v", err)
	}
	r = records()[0]
	r.EventTimeBasis = store.BasisObserved
	if _, err := pginput.Convert(r, pginput.Options{}); err == nil {
		t.Error("an event time basis was dropped silently")
	}
	if _, err := pginput.Convert(r, pginput.Options{DropBoot: true}); err == nil {
		t.Error("DropBoot accepted an event time basis")
	}
	if _, err := pginput.Convert(r, pginput.Options{DropBasis: true}); err != nil {
		t.Errorf("a basis with DropBasis: %v", err)
	}
	r = records()[0]
	r.Subject.Kind = 9
	if _, err := pginput.Convert(r, pginput.Options{DropBasis: true, DropBoot: true}); err == nil {
		t.Error("an unknown subject kind was converted")
	}
}

// TestEveryFieldOfAStoreRecordIsMapped holds the conversion to the two record types: a
// field that a store record has must be either a field of the engine record with the
// same name and a value that arrives, or one of the two the converter names and refuses.
// A field added to either type fails this test until the conversion decides what to do
// with it.
func TestEveryFieldOfAStoreRecordIsMapped(t *testing.T) {
	refused := map[string]bool{"Boot": true, "EventTimeBasis": true}
	st, en := reflect.TypeFor[store.Record](), reflect.TypeFor[engine.Record]()
	for i := range st.NumField() {
		name := st.Field(i).Name
		if refused[name] {
			if _, ok := en.FieldByName(name); ok {
				t.Errorf("%s is refused by the converter but the engine record has it", name)
			}
			continue
		}
		if _, ok := en.FieldByName(name); !ok {
			t.Errorf("store.Record.%s has no engine.Record field and is not refused", name)
		}
	}
	for i := range en.NumField() {
		if _, ok := st.FieldByName(en.Field(i).Name); !ok {
			t.Errorf("engine.Record.%s has no store.Record field", en.Field(i).Name)
		}
	}

	// And each mapped field arrives: set every one to a distinct non-zero value.
	in := store.Record{
		Layer: catalog.L3, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p",
		EventTime: base, Seq: 7, Kind: lifecycle.Observe, TTL: 3 * time.Second, Through: base.Add(time.Hour), Payload: []byte("x"),
	}
	out, err := pginput.Convert(in, pginput.Options{})
	if err != nil {
		t.Fatal(err)
	}
	ov, iv := reflect.ValueOf(out), reflect.ValueOf(in)
	for i := range st.NumField() {
		name := st.Field(i).Name
		if refused[name] || name == "Subject" {
			continue
		}
		if iv.Field(i).IsZero() {
			t.Fatalf("the test leaves %s zero, so it would not notice a field dropped", name)
		}
		if got := ov.FieldByName(name); !reflect.DeepEqual(got.Interface(), iv.Field(i).Interface()) {
			t.Errorf("%s: %v became %v", name, iv.Field(i).Interface(), got.Interface())
		}
	}
	if out.Subject.A != pod || out.Subject.B != node || out.Subject.Relation != catalog.ScheduledOn || out.Subject.Kind != engine.SubjectEdge {
		t.Errorf("subject: %+v", out.Subject)
	}
}
