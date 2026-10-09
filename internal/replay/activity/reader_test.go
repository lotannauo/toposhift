package activity_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// fataler is what the helpers need of a test, so that they serve both a
// testing.T and a rapid.T.
type fataler interface {
	Helper()
	Fatal(args ...any)
	Fatalf(format string, args ...any)
}

// fingerprint resolves one fingerprint, failing the test if the identity does
// not resolve.
func fingerprint(tb fataler, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	tb.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		tb.Fatal(err)
	}
	return id.Fingerprint()
}

var epoch = time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

// sample returns n valid records with ascending Seq: entities and edges,
// observations and deletes, some with a TTL, a Through time or a payload.
func sample(tb fataler, n int) []store.Record {
	tb.Helper()
	pod := fingerprint(tb, catalog.K8sPod, catalog.K8sPodUID, "pod-1")
	node := fingerprint(tb, catalog.K8sNode, catalog.K8sNodeUID, "node-1")
	host := fingerprint(tb, catalog.Host, catalog.HostID, "host-1")
	rack := fingerprint(tb, catalog.Rack, catalog.RackID, "rack-1")
	ctr := fingerprint(tb, catalog.Container, catalog.ContainerID, "ctr-1")

	recs := make([]store.Record, n)
	for i := range recs {
		at := epoch.Add(time.Duration(i) * time.Second)
		r := store.Record{Producer: "collector-a", EventTime: at, Seq: uint64(i+1) * 3, Kind: lifecycle.Observe, EventTimeBasis: store.EventTimeBasis(i % 5)}
		switch i % 5 {
		case 0:
			r.Layer, r.Subject = catalog.L2, store.EntitySubject(pod)
			r.TTL, r.Payload = time.Minute, []byte("pod payload "+string(rune('a'+i%26)))
		case 1:
			r.Layer, r.Subject = catalog.L2, store.EdgeSubject(pod, node, catalog.ScheduledOn)
			r.Payload = []byte{0, 1, 2, byte(i)}
		case 2:
			r.Layer, r.Subject = catalog.L1, store.EdgeSubject(host, rack, catalog.LocatedIn)
			r.Through = at.Add(10 * time.Second)
		case 3:
			r.Layer, r.Subject = catalog.L1, store.EntitySubject(host)
			if i%10 == 3 {
				r.Boot = fmt.Sprintf("boot-%d", i/10%3) // an observation of a host with a boot id
			} else {
				r.Kind = lifecycle.Delete
			}
		case 4:
			r.Layer, r.Subject = catalog.L2, store.EdgeSubject(ctr, pod, catalog.PartOf)
			r.Kind = lifecycle.Delete
		}
		if err := r.Validate(); err != nil {
			tb.Fatalf("sample record %d is not valid: %v", i, err)
		}
		recs[i] = r
	}
	return recs
}

// writeFile writes the records as an activity file.
func writeFile(tb fataler, recs []store.Record, opts activity.WriterOptions) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, opts)
	if err != nil {
		tb.Fatalf("NewWriter: %v", err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			tb.Fatalf("Write(seq %d): %v", r.Seq, err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatalf("Close: %v", err)
	}
	return buf.Bytes()
}

// openFile opens an activity file held in memory.
func openFile(data []byte, opts activity.ReaderOptions) (*activity.Reader, error) {
	return activity.NewReader(bytes.NewReader(data), int64(len(data)), opts)
}

// drain reads records until the end or the first error. The records it
// returns are all that were read before the error.
func drain(r *activity.Reader) ([]store.Record, error) {
	var out []store.Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, rec)
	}
}

// readFile reads a whole activity file held in memory.
func readFile(tb fataler, data []byte, opts activity.ReaderOptions) []store.Record {
	tb.Helper()
	r, err := openFile(data, opts)
	if err != nil {
		tb.Fatalf("NewReader: %v", err)
	}
	got, err := drain(r)
	if err != nil {
		tb.Fatalf("Next after %d records: %v", len(got), err)
	}
	return got
}

// sameRecord compares two records field by field: times by Equal, payloads
// by bytes.Equal, subjects with ==.
func sameRecord(a, b store.Record) bool {
	return a.Layer == b.Layer && a.Subject == b.Subject && a.Producer == b.Producer &&
		a.EventTime.Equal(b.EventTime) && a.Seq == b.Seq && a.Kind == b.Kind && a.TTL == b.TTL &&
		a.Through.Equal(b.Through) && bytes.Equal(a.Payload, b.Payload) && a.Boot == b.Boot && a.EventTimeBasis == b.EventTimeBasis
}

// requireSame fails the test unless got is want.
func requireSame(tb fataler, want, got []store.Record) {
	tb.Helper()
	if len(got) != len(want) {
		tb.Fatalf("read %d records, want %d", len(got), len(want))
	}
	for i := range want {
		if !sameRecord(want[i], got[i]) {
			tb.Fatalf("record %d (seq %d) read back as %+v, want %+v", i, want[i].Seq, got[i], want[i])
		}
	}
}

func TestReadBack(t *testing.T) {
	t.Parallel()

	recs := sample(t, 37)
	for _, tc := range []struct {
		name       string
		write      activity.WriterOptions
		read       activity.ReaderOptions
		wantGroups int
	}{
		{"defaults", activity.WriterOptions{}, activity.ReaderOptions{}, 1},
		{"one row per group, one per batch", activity.WriterOptions{RowGroupRows: 1}, activity.ReaderOptions{BatchRows: 1}, 37},
		{"groups of 7, batches of 3", activity.WriterOptions{RowGroupRows: 7}, activity.ReaderOptions{BatchRows: 3}, 6},
		{"groups of 10, batches of 10", activity.WriterOptions{RowGroupRows: 10}, activity.ReaderOptions{BatchRows: 10}, 4},
		{"groups of 37, batches larger than the file", activity.WriterOptions{RowGroupRows: 37}, activity.ReaderOptions{BatchRows: 1000}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := writeFile(t, recs, tc.write)
			r, err := openFile(data, tc.read)
			if err != nil {
				t.Fatalf("NewReader: %v", err)
			}
			want := activity.Info{Version: 1, Records: 37, MinSeq: 3, MaxSeq: 37 * 3, RowGroups: tc.wantGroups}
			if got := r.Info(); got != want {
				t.Errorf("Info() = %+v, want %+v", got, want)
			}
			got, err := drain(r)
			if err != nil {
				t.Fatalf("Next: %v", err)
			}
			requireSame(t, recs, got)
			for range 2 {
				if _, err := r.Next(); !errors.Is(err, io.EOF) {
					t.Errorf("Next after the end = %v, want io.EOF", err)
				}
			}
			if err := r.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		})
	}
}

// A record read back must not point into the reader's buffers: reading on
// with small batches, over many pages, must not change records already
// returned.
func TestRecordsOwnTheirBytes(t *testing.T) {
	t.Parallel()

	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	var recs []store.Record
	for i := range 300 {
		payload := bytes.Repeat([]byte{byte(i)}, 20_000+i) // several pages per row group
		recs = append(recs, store.Record{
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: lifecycle.Producer("producer-" + string(rune('a'+i%7))),
			EventTime: epoch.Add(time.Duration(i) * time.Second), Seq: uint64(i + 1), Kind: lifecycle.Observe, Payload: payload,
		})
	}
	data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: 120})
	got := readFile(t, data, activity.ReaderOptions{BatchRows: 2})
	requireSame(t, recs, got) // compared only after everything was read
}

func TestFieldsReadBack(t *testing.T) {
	t.Parallel()

	recs := sample(t, 5)
	got := readFile(t, writeFile(t, recs, activity.WriterOptions{}), activity.ReaderOptions{})
	for i, r := range got {
		if r.EventTime.Location() != time.UTC {
			t.Errorf("record %d: event time in %v, want UTC", i, r.EventTime.Location())
		}
		if !r.Through.IsZero() && r.Through.Location() != time.UTC {
			t.Errorf("record %d: through in %v, want UTC", i, r.Through.Location())
		}
		if len(recs[i].Payload) == 0 && r.Payload != nil {
			t.Errorf("record %d: empty payload read back as %#v, want nil", i, r.Payload)
		}
		if recs[i].Through.IsZero() && !r.Through.IsZero() {
			t.Errorf("record %d: a null through read back as %v, want the zero time", i, r.Through)
		}
	}
	// Through at exactly the Unix epoch is a time, not a null.
	pod := fingerprint(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	one := store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "p", EventTime: store.MinEventTime,
		Seq: 1, Kind: lifecycle.Observe, Through: store.MinEventTime,
	}
	back := readFile(t, writeFile(t, []store.Record{one}, activity.WriterOptions{}), activity.ReaderOptions{})
	requireSame(t, []store.Record{one}, back)
	if back[0].Through.IsZero() {
		t.Error("a through time at the epoch read back as the zero time")
	}
}

func TestLargeSeq(t *testing.T) {
	t.Parallel()

	recs := sample(t, 4)
	seqs := []uint64{1, 1 << 63, 1<<63 + 5, math.MaxUint64}
	for i := range recs {
		recs[i].Seq = seqs[i]
	}
	data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: 2})
	r, err := openFile(data, activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got := r.Info(); got.MinSeq != 1 || got.MaxSeq != math.MaxUint64 {
		t.Errorf("Info() = %+v, want MinSeq 1 and MaxSeq %d", got, uint64(math.MaxUint64))
	}
	got, err := drain(r)
	if err != nil {
		t.Fatal(err)
	}
	requireSame(t, recs, got)
}

func TestErrorsAreSticky(t *testing.T) {
	t.Parallel()

	// A digest that disagrees with the rows is found at the end of the file.
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// A digest of the one row group that does not match its rows, with a file
	// digest that is consistent with it.
	bogus := strings.Repeat("0", 64)
	activity.Tamper(w, func(kv map[string]string) {
		kv["toposhift.activity.group_digests"] = bogus
		kv["toposhift.activity.digest"] = specFileDigest([]string{bogus})
	})
	for _, r := range sample(t, 3) {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := openFile(buf.Bytes(), activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got, err := drain(r)
	if !errors.Is(err, activity.ErrFormat) || !strings.Contains(err.Error(), "digest") {
		t.Fatalf("drain = %v after %d records, want an ErrFormat error naming the digest", err, len(got))
	}
	if len(got) != 0 {
		t.Errorf("%d records were returned from a row group whose digest does not match, want none", len(got))
	}
	for range 2 {
		if _, again := r.Next(); !errors.Is(again, err) {
			t.Errorf("Next after the error = %v, want the same error %v", again, err)
		}
	}
}

func TestReaderOptionsRefused(t *testing.T) {
	t.Parallel()

	data := writeFile(t, sample(t, 2), activity.WriterOptions{})
	for _, opts := range []activity.ReaderOptions{{BatchRows: -1}, {MaxRowGroupBytes: -1}} {
		if _, err := openFile(data, opts); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("NewReader(%+v) = %v, want an error wrapping store.ErrInvalid", opts, err)
		}
	}
}

func TestNextAfterClose(t *testing.T) {
	t.Parallel()

	r, err := openFile(writeFile(t, sample(t, 10), activity.WriterOptions{RowGroupRows: 4}), activity.ReaderOptions{BatchRows: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := r.Next(); !errors.Is(err, activity.ErrReaderClosed) {
		t.Errorf("Next after Close = %v, want an error wrapping ErrReaderClosed", err)
	}
	// Also after the end of the file.
	r, err = openFile(writeFile(t, sample(t, 3), activity.WriterOptions{}), activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drain(r); err != nil {
		t.Fatal(err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Next(); !errors.Is(err, activity.ErrReaderClosed) {
		t.Errorf("Next after the end and Close = %v, want an error wrapping ErrReaderClosed", err)
	}
}

func TestBootRoundTrip(t *testing.T) {
	t.Parallel()

	host := fingerprint(t, catalog.Host, catalog.HostID, "h1")
	other := fingerprint(t, catalog.Host, catalog.HostID, "h2")
	rack := fingerprint(t, catalog.Rack, catalog.RackID, "r1")
	longest := strings.Repeat("0123456789abcdef", store.MaxBootLen/16) // exactly store.MaxBootLen bytes
	if len(longest) != store.MaxBootLen {
		t.Fatalf("test boot is %d bytes", len(longest))
	}
	boots := []string{"", "b1", "b1", "b2", " leading and trailing ", "ブート-3", longest, "b1", "", longest}

	var recs []store.Record
	for i, b := range boots {
		h := host
		if i%2 == 1 {
			h = other
		}
		recs = append(recs, store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(h), Producer: "cloud", EventTime: epoch.Add(time.Duration(i) * time.Second),
			Seq: uint64(i + 1), Kind: lifecycle.Observe, Boot: b, Payload: []byte("p"),
		})
	}
	// Records that never carry one, between the others.
	recs = append(recs,
		store.Record{Layer: catalog.L1, Subject: store.EdgeSubject(host, rack, catalog.LocatedIn), Producer: "cloud", EventTime: epoch, Seq: 100, Kind: lifecycle.Observe},
		store.Record{Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "cloud", EventTime: epoch, Seq: 101, Kind: lifecycle.Delete},
	)
	for _, groupRows := range []int{1, 3, 100} {
		data := writeFile(t, recs, activity.WriterOptions{RowGroupRows: groupRows})
		got := readFile(t, data, activity.ReaderOptions{BatchRows: 2})
		requireSame(t, recs, got)
	}
}
