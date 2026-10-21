package vmwrite_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math"
	"slices"
	"testing"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmwrite"
)

// unhex reads a hex string with spaces and line breaks.
func unhex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(string(bytes.Join(bytes.Fields([]byte(s)), nil)))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The bytes of one series with two labels and two samples, written out by hand from the
// message definitions:
//
//	WriteRequest.timeseries (field 1, length-delimited): 0a <len> ..., where the length is
//	15 + 8 + 14 + 14 = 51 = 0x33
//	  TimeSeries.labels (field 1): 0a <len> { name (field 1): 0a 08 "__name__"; value (field 2): 12 01 "m" }
//	  TimeSeries.labels (field 1): 0a <len> { 0a 01 "a"; 12 01 "b" }
//	  TimeSeries.samples (field 2): 12 <len> { value (field 1, fixed64): 09 <8 bytes little endian>; timestamp (field 2, varint): 10 <varint> }
//
// 1.0 is 0x3ff0000000000000, so 00 00 00 00 00 00 f0 3f. 1000 is e8 07. 300 is ac 02.
// The staleness marker is 0x7ff0000000000002, so 02 00 00 00 00 00 f0 7f.
const handChecked = `
0a 33
  0a 0d 0a08 5f5f6e616d655f5f 1201 6d
  0a 06 0a01 61 1201 62
  12 0c 09 000000000000f03f 10 e807
  12 0c 09 020000000000f07f 10 ac02
`

var labels = []vmmodel.Label{{Name: "a", Value: "b"}, {Name: "__name__", Value: "m"}} // out of order on purpose

func TestMarshalIsTheHandCheckedProtobuf(t *testing.T) {
	got := vmwrite.Marshal([]vmwrite.TimeSeries{{Labels: labels, Samples: []vmwrite.Sample{vmwrite.One(1000), vmwrite.Marker(300)}}})
	if want := unhex(t, handChecked); !bytes.Equal(got, want) {
		t.Errorf("message\n got %x\nwant %x", got, want)
	}
	// The labels of the argument are not reordered.
	if labels[0].Name != "a" {
		t.Error("Marshal sorted its argument")
	}
}

// Labels are written sorted by name whatever order they come in: with __name__ first, and
// the rest by byte order, uppercase before lowercase.
func TestLabelsAreSortedByName(t *testing.T) {
	in := []vmmodel.Label{{Name: "producer", Value: "p"}, {Name: "Z", Value: "z"}, {Name: "layer", Value: "L2"}, {Name: "__name__", Value: "m"}}
	msg := vmwrite.Marshal([]vmwrite.TimeSeries{{Labels: in}})
	got := decodeLabels(t, msg)
	want := []string{"Z", "__name__", "layer", "producer"}
	if !slices.Equal(got, want) {
		t.Errorf("label order %v, want %v", got, want)
	}
}

// decodeLabels reads the names of the labels of the first series of a message, in the
// order they are written, with the protobuf wire decoder.
func decodeLabels(t *testing.T, msg []byte) []string {
	t.Helper()
	num, typ, n := protowire.ConsumeTag(msg)
	if n < 0 || num != 1 || typ != protowire.BytesType {
		t.Fatalf("not a timeseries: %v %v %v", num, typ, n)
	}
	ts, m := protowire.ConsumeBytes(msg[n:])
	if m < 0 {
		t.Fatal("bad length")
	}
	var names []string
	for len(ts) > 0 {
		num, typ, n := protowire.ConsumeTag(ts)
		if n < 0 {
			t.Fatal("bad tag")
		}
		ts = ts[n:]
		switch typ {
		case protowire.BytesType:
			b, m := protowire.ConsumeBytes(ts)
			ts = ts[m:]
			if num == 1 {
				fnum, _, k := protowire.ConsumeTag(b)
				if fnum != 1 {
					t.Fatalf("label field %d", fnum)
				}
				name, _ := protowire.ConsumeString(b[k:])
				names = append(names, name)
			}
		default:
			t.Fatalf("unexpected wire type %v", typ)
		}
	}
	return names
}

// The body is snappy's block format. The framed format starts with a stream identifier
// (ff 06 00 00 "sNaPpY") and a block decoder does not take it.
func TestCompressIsTheBlockFormatNotTheFramedOne(t *testing.T) {
	msg := vmwrite.Marshal([]vmwrite.TimeSeries{{Labels: labels, Samples: []vmwrite.Sample{vmwrite.One(1000)}}})
	body := vmwrite.Compress(msg)
	back, err := snappy.Decode(nil, body)
	if err != nil || !bytes.Equal(back, msg) {
		t.Fatalf("a block decoder does not give the message back: %v", err)
	}
	if bytes.HasPrefix(body, []byte{0xff, 0x06, 0x00, 0x00, 's', 'N', 'a', 'P', 'p', 'Y'}) {
		t.Error("the body starts with the framed format's stream identifier")
	}
	// What a framed writer produces is not a block, and the block decoder rejects it.
	var framed bytes.Buffer
	w := snappy.NewBufferedWriter(&framed)
	if _, err := w.Write(msg); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := snappy.Decode(nil, framed.Bytes()); err == nil {
		t.Error("the block decoder took framed data: this test cannot tell the formats apart")
	}
	if !bytes.HasPrefix(framed.Bytes(), []byte{0xff, 0x06, 0x00, 0x00}) {
		t.Error("the framed writer does not write its identifier")
	}
}

func TestMarkerIsStaleNaNExactly(t *testing.T) {
	m := vmwrite.Marker(5)
	if math.Float64bits(m.Value) != 0x7ff0000000000002 || !m.IsMarker() || m.TimestampMs != 5 {
		t.Errorf("marker %x", math.Float64bits(m.Value))
	}
	if !math.IsNaN(m.Value) {
		t.Error("the marker is not a NaN")
	}
	if vmwrite.One(5).IsMarker() || (vmwrite.Sample{Value: math.NaN()}).IsMarker() {
		t.Error("a sample that is not the marker is reported as one")
	}
}

func lab(i uint32) []vmmodel.Label {
	return []vmmodel.Label{{Name: "__name__", Value: "m"}, {Name: "i", Value: string(rune('a' + i))}}
}

// Requests hold exactly size samples but the last, in the order added; one series' samples
// in a request are together under one TimeSeries, series in order of first appearance.
func TestBatcher(t *testing.T) {
	var reqs []vmwrite.Request
	b, err := vmwrite.NewBatcher(3, lab, func(r vmwrite.Request) error { reqs = append(reqs, r); return nil })
	if err != nil {
		t.Fatal(err)
	}
	in := []vmmodel.Sample{
		{Series: 1, TimestampMs: 10}, {Series: 0, TimestampMs: 11}, {Series: 1, TimestampMs: 12}, // request 1: series 1, 0
		{Series: 0, TimestampMs: 13, Stale: true}, {Series: 2, TimestampMs: 14}, // request 2 starts
		{Series: 2, TimestampMs: 15}, {Series: 1, TimestampMs: 16}, // request 2 ends, request 3 starts
	}
	if err := b.Add(in[:2]); err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 0 {
		t.Fatal("a request before size samples")
	}
	if err := b.Add(in[2:]); err != nil {
		t.Fatal(err)
	}
	if len(reqs) != 2 {
		t.Fatalf("%d requests before the flush", len(reqs))
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := b.Flush(); err != nil { // nothing left: no empty request
		t.Fatal(err)
	}
	if len(reqs) != 3 {
		t.Fatalf("%d requests", len(reqs))
	}
	wantSamples, wantSeries := []int{3, 3, 1}, []int{2, 2, 1}
	for i, r := range reqs {
		if r.Samples != wantSamples[i] || r.Series != wantSeries[i] {
			t.Errorf("request %d: %d samples in %d series, want %d in %d", i, r.Samples, r.Series, wantSamples[i], wantSeries[i])
		}
	}
	// Request 1 is series 1 (samples at 10 and 12) then series 0 (at 11), by hand.
	msg, err := snappy.Decode(nil, reqs[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	want := vmwrite.Marshal([]vmwrite.TimeSeries{
		{Labels: lab(1), Samples: []vmwrite.Sample{vmwrite.One(10), vmwrite.One(12)}},
		{Labels: lab(0), Samples: []vmwrite.Sample{vmwrite.One(11)}},
	})
	if !bytes.Equal(msg, want) {
		t.Errorf("request 1\n got %x\nwant %x", msg, want)
	}
	// Request 2 holds the marker.
	msg, _ = snappy.Decode(nil, reqs[1].Body)
	want = vmwrite.Marshal([]vmwrite.TimeSeries{
		{Labels: lab(0), Samples: []vmwrite.Sample{vmwrite.Marker(13)}},
		{Labels: lab(2), Samples: []vmwrite.Sample{vmwrite.One(14), vmwrite.One(15)}},
	})
	if !bytes.Equal(msg, want) {
		t.Errorf("request 2\n got %x\nwant %x", msg, want)
	}
}

func TestBatcherRefusalsAndSendErrors(t *testing.T) {
	if _, err := vmwrite.NewBatcher(0, lab, nil); err == nil {
		t.Error("a request of no samples")
	}
	boom := errors.New("boom")
	b, _ := vmwrite.NewBatcher(1, lab, func(vmwrite.Request) error { return boom })
	if err := b.Add([]vmmodel.Sample{{Series: 0, TimestampMs: 1}}); !errors.Is(err, boom) {
		t.Errorf("Add = %v", err)
	}
	b, _ = vmwrite.NewBatcher(5, lab, func(vmwrite.Request) error { return boom })
	_ = b.Add([]vmmodel.Sample{{Series: 0, TimestampMs: 1}})
	if err := b.Flush(); !errors.Is(err, boom) {
		t.Errorf("Flush = %v", err)
	}
}

func TestEndpointConstants(t *testing.T) {
	if vmwrite.Path != "/api/v1/write" || vmwrite.ContentEncoding != "snappy" || vmwrite.ContentType != "application/x-protobuf" || vmwrite.RemoteWriteVersion != "0.1.0" {
		t.Error("the endpoint or its headers changed")
	}
}
