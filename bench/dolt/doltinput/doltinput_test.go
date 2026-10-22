package doltinput_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
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
	info, err := doltinput.ForEach(path, 4, doltinput.Options{}, func(b []engine.Record) error {
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
		want, err := doltinput.Convert(r, doltinput.Options{})
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
	if _, err := doltinput.ForEach(path, 2, doltinput.Options{}, func([]engine.Record) error { calls++; return doltinput.ErrStop }); err != nil || calls != 1 {
		t.Fatalf("stop: %d calls, %v", calls, err)
	}
	boom := errors.New("boom")
	if _, err := doltinput.ForEach(path, 2, doltinput.Options{}, func([]engine.Record) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("error: %v", err)
	}
	if _, err := doltinput.ForEach(path, 0, doltinput.Options{}, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a batch of 0 was accepted")
	}
	if _, err := doltinput.ForEach(filepath.Join(t.TempDir(), "missing"), 2, doltinput.Options{}, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a missing file was read")
	}
}

// feed writes an activity file to an engine in batches of batch records, stopping after
// about limit records if limit is positive, and returns the records written.
func feed(t *testing.T, e engine.Engine, path string, batch int, limit int64) int64 {
	t.Helper()
	var n int64
	_, err := doltinput.ForEach(path, batch, doltinput.Options{}, doltinput.StopAfter(limit, func(rs []engine.Record) error {
		n += int64(len(rs))
		return e.Write(rs)
	}))
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestAFileFedToTheModelIsReadAsTheReferenceEngineReadsIt loads the file into the model of
// the design and into the reference engine, and compares the reads at the instants that
// matter.
func TestAFileFedToTheModelIsReadAsTheReferenceEngineReadsIt(t *testing.T) {
	path := writeFile(t, records())
	m, ora := doltmodel.New(), oracle.New()
	if n := feed(t, m, path, 2, 0); n != 6 {
		t.Fatalf("%d records", n)
	}
	feed(t, ora, path, 5, 0)
	if m.LastSeq() != 6 {
		t.Fatalf("last seq %d", m.LastSeq())
	}
	if c := m.Counts(); c.Commits != 3 || c.Records != 6 {
		t.Fatalf("three batches of two make three commits: %+v", c)
	}
	// The placement is alive over [1, 50) (a refresh at 10 through 20 with a 30 s TTL
	// holds it to 50, and the delete at 50 ends it) and from 60 on.
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

func TestStopAfterEndsTheReadAtTheLimit(t *testing.T) {
	path := writeFile(t, records())
	m := doltmodel.New()
	if n := feed(t, m, path, 2, 3); n != 4 || m.LastSeq() != 4 {
		t.Fatalf("a limit of 3: %d records, seq %d", n, m.LastSeq())
	}
}

func TestForEachCountCountsEachDroppedFieldOnItsOwn(t *testing.T) {
	rs := records()
	host := store.EntitySubject(fp(catalog.Host, 9))
	rs[0].Boot, rs[0].Subject, rs[0].Layer = "b1", host, catalog.L1
	rs[1].Boot, rs[1].EventTimeBasis, rs[1].Subject, rs[1].Layer = "b1", store.BasisObserved, host, catalog.L1
	rs[2].EventTimeBasis = store.BasisObserved
	rs[3].EventTimeBasis = store.BasisObserved
	path := writeFile(t, rs)
	_, d, err := doltinput.ForEachCount(path, 4, doltinput.Options{DropBasis: true, DropBoot: true}, func([]engine.Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if d != (doltinput.Dropped{Basis: 3, Boot: 2}) {
		t.Fatalf("dropped %+v, want 3 with a basis and 2 with a boot id (one record has both)", d)
	}
	// A prefix counts only what it read.
	_, d, err = doltinput.ForEachCount(path, 1, doltinput.Options{DropBasis: true, DropBoot: true}, doltinput.StopAfter(2, func([]engine.Record) error { return nil }))
	if err != nil || d != (doltinput.Dropped{Basis: 1, Boot: 2}) {
		t.Fatalf("a prefix of two: %+v, %v", d, err)
	}
	// Without the options the file is refused, and nothing is counted as dropped.
	if _, _, err := doltinput.ForEachCount(path, 4, doltinput.Options{}, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("records with a boot id and a basis were read without the options")
	}
}

func TestConvertRefusesWhatAnEngineRecordCannotHold(t *testing.T) {
	r := records()[0]
	r.Boot = "boot-1"
	if _, err := doltinput.Convert(r, doltinput.Options{}); err == nil {
		t.Error("a boot id was dropped silently")
	}
	if _, err := doltinput.Convert(r, doltinput.Options{DropBasis: true}); err == nil {
		t.Error("DropBasis accepted a boot id")
	}
	if _, err := doltinput.Convert(r, doltinput.Options{DropBoot: true}); err != nil {
		t.Errorf("a boot id with DropBoot: %v", err)
	}
	r = records()[0]
	r.EventTimeBasis = store.BasisObserved
	if _, err := doltinput.Convert(r, doltinput.Options{}); err == nil {
		t.Error("an event time basis was dropped silently")
	}
	if _, err := doltinput.Convert(r, doltinput.Options{DropBoot: true}); err == nil {
		t.Error("DropBoot accepted an event time basis")
	}
	if _, err := doltinput.Convert(r, doltinput.Options{DropBasis: true}); err != nil {
		t.Errorf("a basis with DropBasis: %v", err)
	}
	r = records()[0]
	r.Subject.Kind = 9
	if _, err := doltinput.Convert(r, doltinput.Options{DropBasis: true, DropBoot: true}); err == nil {
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
	out, err := doltinput.Convert(in, doltinput.Options{})
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
