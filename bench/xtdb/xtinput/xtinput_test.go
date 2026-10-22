package xtinput_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

func fpN(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n>>8), byte(n)
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

var (
	pod, node = fpN(catalog.K8sPod, 1), fpN(catalog.K8sNode, 2)
	base      = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
)

func at(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }

// records is a small stream: a pod and a node observed, the pod placed on the node,
// refreshed, and the placement deleted and made again.
func records() []store.Record {
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
	var st xtinput.Stats
	info, err := xtinput.ForEach(path, 4, xtinput.Options{}, &st, func(b []engine.Record) error {
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
	if st != (xtinput.Stats{Records: 6}) {
		t.Errorf("stats %+v", st)
	}
	for i, r := range rs {
		want, err := xtinput.Convert(r, xtinput.Options{}, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[i], want) || got[i].Seq != r.Seq || !got[i].EventTime.Equal(r.EventTime) {
			t.Errorf("record %d: %+v, want %+v", i, got[i], want)
		}
	}
}

func TestForEachStopsWhenAskedAndPassesErrorsOn(t *testing.T) {
	path := writeFile(t, records())
	calls := 0
	if _, err := xtinput.ForEach(path, 2, xtinput.Options{}, nil, func([]engine.Record) error { calls++; return xtinput.ErrStop }); err != nil || calls != 1 {
		t.Fatalf("stop: %d calls, %v", calls, err)
	}
	boom := errors.New("boom")
	if _, err := xtinput.ForEach(path, 2, xtinput.Options{}, nil, func([]engine.Record) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("error: %v", err)
	}
	if _, err := xtinput.ForEach(path, 0, xtinput.Options{}, nil, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a batch of 0 was accepted")
	}
	if _, err := xtinput.ForEach(filepath.Join(t.TempDir(), "missing"), 2, xtinput.Options{}, nil, func([]engine.Record) error { return nil }); err == nil {
		t.Fatal("a missing file was read")
	}
}

// TestLoadWritesTheFileToAnEngineAsTheReferenceFolds loads the file into the model of the
// baseline's tables and into the reference engine, and compares reads at the instants that
// matter.
func TestLoadWritesTheFileToAnEngineAsTheReferenceFolds(t *testing.T) {
	path := writeFile(t, records())
	m, ora := xtmodel.New(), oracle.New()
	info, st, err := xtinput.Load(m, path, 2, 0, xtinput.Options{})
	if err != nil || st.Records != 6 || info.Records != 6 {
		t.Fatalf("load: %+v, %+v, %v", st, info, err)
	}
	if _, _, err := xtinput.Load(ora, path, 5, 0, xtinput.Options{}); err != nil {
		t.Fatal(err)
	}
	if m.LastSeq() != 6 {
		t.Fatalf("last seq %d", m.LastSeq())
	}
	sc := engine.Current(catalog.L2)
	for _, s := range []int{-1, 0, 1, 49, 50, 59, 60, 1000} {
		a, err := m.Neighbors(pod, engine.Forward, at(s), sc)
		if err != nil {
			t.Fatal(err)
		}
		b, err := ora.Neighbors(pod, engine.Forward, at(s), sc)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(a, b) {
			t.Errorf("at %d s: %v, the reference engine %v", s, a, b)
		}
		// The placement is alive over [1, 50) and from 60 on.
		if want := s >= 1 && s < 50 || s >= 60; (len(a) == 1) != want {
			t.Errorf("at %d s: %d neighbors, alive should be %v", s, len(a), want)
		}
	}
}

func TestLoadStopsAtTheLimit(t *testing.T) {
	path := writeFile(t, records())
	m := xtmodel.New()
	_, st, err := xtinput.Load(m, path, 2, 3, xtinput.Options{})
	if err != nil || st.Records != 4 || m.LastSeq() != 4 {
		t.Fatalf("load to a limit of 3: %+v, seq %d, %v", st, m.LastSeq(), err)
	}
}

func TestConvertRefusesWhatAnEngineRecordCannotHoldAndCountsWhatItDrops(t *testing.T) {
	r := records()[0]
	r.Boot = "boot-1"
	if _, err := xtinput.Convert(r, xtinput.Options{}, nil); err == nil {
		t.Error("a boot id was dropped silently")
	}
	if _, err := xtinput.Convert(r, xtinput.Options{DropBasis: true}, nil); err == nil {
		t.Error("DropBasis accepted a boot id")
	}
	var st xtinput.Stats
	if _, err := xtinput.Convert(r, xtinput.Options{DropBoot: true}, &st); err != nil || st != (xtinput.Stats{Records: 1, BootDropped: 1}) {
		t.Errorf("a boot id with DropBoot: %+v, %v", st, err)
	}
	r = records()[0]
	r.EventTimeBasis = store.BasisObserved
	if _, err := xtinput.Convert(r, xtinput.Options{}, nil); err == nil {
		t.Error("an event time basis was dropped silently")
	}
	if _, err := xtinput.Convert(r, xtinput.Options{DropBoot: true}, nil); err == nil {
		t.Error("DropBoot accepted an event time basis")
	}
	st = xtinput.Stats{}
	if _, err := xtinput.Convert(r, xtinput.Options{DropBasis: true}, &st); err != nil || st != (xtinput.Stats{Records: 1, BasisDropped: 1}) {
		t.Errorf("a basis with DropBasis: %+v, %v", st, err)
	}
	r = records()[0]
	r.Subject.Kind = 9
	if _, err := xtinput.Convert(r, xtinput.Options{DropBasis: true, DropBoot: true}, nil); err == nil {
		t.Error("an unknown subject kind was converted")
	}
}

// TestRecordsOffAWholeMicrosecondAreCounted: the counting pass finds the records an XTDB
// engine would refuse, by each of the boundaries it checks, as the planner counts them.
func TestRecordsOffAWholeMicrosecondAreCounted(t *testing.T) {
	odd := func(f func(*store.Record)) store.Record {
		r := records()[0]
		f(&r)
		return r
	}
	for name, c := range map[string]struct {
		r    store.Record
		want uint64
	}{
		"whole seconds":       {records()[0], 0},
		"a whole microsecond": {odd(func(r *store.Record) { r.EventTime = r.EventTime.Add(7 * time.Microsecond) }), 0},
		"an event time":       {odd(func(r *store.Record) { r.EventTime = r.EventTime.Add(1) }), 1},
		"a deadline":          {odd(func(r *store.Record) { r.TTL += 1 }), 1},
		"a through time":      {odd(func(r *store.Record) { r.Through = r.EventTime.Add(1500) }), 1},
	} {
		var stats xtinput.Stats
		e, err := xtinput.Convert(c.r, xtinput.Options{}, &stats)
		if err != nil {
			t.Fatal(err)
		}
		if stats.OffMicrosecond != c.want {
			t.Errorf("%s: counted %d, want %d", name, stats.OffMicrosecond, c.want)
		}
		m := xtmodel.New()
		if err := m.Write([]engine.Record{e}); err != nil {
			t.Fatal(err)
		}
		if got := m.Counts().OffMicrosecond; got != c.want {
			t.Errorf("%s: the planner counted %d", name, got)
		}
	}
}

// TestTheCountingPassSeesOffMicrosecondRecordsInAFile: the count is made on what a file
// gives back, so a file that holds a boundary finer than a microsecond is counted, and one
// that does not is not.
func TestTheCountingPassSeesOffMicrosecondRecordsInAFile(t *testing.T) {
	rs := records()
	rs[3].EventTime = rs[3].EventTime.Add(1) // 1 ns after a whole second
	var st xtinput.Stats
	if _, err := xtinput.ForEach(writeFile(t, rs), 4, xtinput.Options{}, &st, func([]engine.Record) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if st.Records != 6 || st.OffMicrosecond != 1 {
		t.Errorf("%+v", st)
	}
	st = xtinput.Stats{}
	if _, err := xtinput.ForEach(writeFile(t, records()), 4, xtinput.Options{}, &st, func([]engine.Record) error { return nil }); err != nil || st.OffMicrosecond != 0 {
		t.Errorf("%+v, %v", st, err)
	}
}

// TestEveryFieldOfAStoreRecordIsMapped holds the conversion to the two record types: a
// field that a store record has must be either a field of the engine record with the same
// name and a value that arrives, or one of the two the converter names and counts as
// dropped. A field added to either type fails this test until the conversion decides what to
// do with it.
func TestEveryFieldOfAStoreRecordIsMapped(t *testing.T) {
	dropped := map[string]bool{"Boot": true, "EventTimeBasis": true}
	st, en := reflect.TypeFor[store.Record](), reflect.TypeFor[engine.Record]()
	for i := range st.NumField() {
		name := st.Field(i).Name
		if dropped[name] {
			if _, ok := en.FieldByName(name); ok {
				t.Errorf("%s is dropped by the converter but the engine record has it", name)
			}
			continue
		}
		if _, ok := en.FieldByName(name); !ok {
			t.Errorf("store.Record.%s has no engine.Record field and is not dropped", name)
		}
	}
	for i := range en.NumField() {
		if _, ok := st.FieldByName(en.Field(i).Name); !ok {
			t.Errorf("engine.Record.%s has no store.Record field", en.Field(i).Name)
		}
	}
	in := store.Record{
		Layer: catalog.L3, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "p",
		EventTime: base, Seq: 7, Kind: lifecycle.Observe, TTL: 3 * time.Second, Through: base.Add(time.Hour), Payload: []byte("x"),
	}
	out, err := xtinput.Convert(in, xtinput.Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ov, iv := reflect.ValueOf(out), reflect.ValueOf(in)
	for i := range st.NumField() {
		name := st.Field(i).Name
		if dropped[name] || name == "Subject" {
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
