// Package vmwrite encodes samples as Prometheus remote-write requests, by hand.
//
// A request is a WriteRequest protobuf message, compressed with snappy's block format
// (not its framed stream format), posted to the store's remote-write endpoint (assumption (a), verified).
// Only the fields a store needs are written:
//
//	WriteRequest { repeated TimeSeries timeseries = 1; }
//	TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
//	Label        { string name = 1; string value = 2; }
//	Sample       { double value = 1; int64 timestamp = 2; }   // milliseconds
//
// The labels of a series are written sorted by name, as a remote-write receiver may
// require. A staleness marker is the float whose bits are StaleNaN (0x7ff0000000000002),
// the value Prometheus writes when a series ends (assumption (b), verified).
//
// The package builds the bytes and sends nothing. It knows no server: the address, the
// retry and the pacing are the caller's.
package vmwrite

import (
	"fmt"
	"math"
	"slices"
	"strings"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// Path is the remote-write endpoint (assumption (a), verified), and the headers a request carries.
const (
	Path            = "/api/v1/write"
	ContentType     = "application/x-protobuf"
	ContentEncoding = "snappy"
	// RemoteWriteVersion is the value of the X-Prometheus-Remote-Write-Version header.
	RemoteWriteVersion = "0.1.0"
)

// StaleNaNBits are the bits of the staleness marker value.
const StaleNaNBits uint64 = 0x7ff0000000000002

// The field numbers of the messages.
const (
	fieldTimeSeries protowire.Number = 1
	fieldLabels     protowire.Number = 1
	fieldSamples    protowire.Number = 2
	fieldName       protowire.Number = 1
	fieldValue      protowire.Number = 2
	fieldSampleVal  protowire.Number = 1
	fieldSampleTS   protowire.Number = 2
)

// Sample is one value at a time, in milliseconds.
type Sample struct {
	Value       float64
	TimestampMs int64
}

// Marker is the sample that ends a series at a time.
func Marker(ms int64) Sample { return Sample{math.Float64frombits(StaleNaNBits), ms} }

// One is the sample of value 1 at a time.
func One(ms int64) Sample { return Sample{1, ms} }

// IsMarker reports whether the sample is the staleness marker, by its bits: a NaN that is
// not StaleNaN is not one.
func (s Sample) IsMarker() bool { return math.Float64bits(s.Value) == StaleNaNBits }

// TimeSeries is a series and its samples, in the order they are written.
type TimeSeries struct {
	Labels  []vmmodel.Label
	Samples []Sample
}

// Marshal returns the WriteRequest message for the series, uncompressed. The labels of
// each series are written sorted by name (the argument is not changed), and the series
// and samples in the order given.
func Marshal(series []TimeSeries) []byte {
	var b []byte
	var ts, lab []byte
	for _, s := range series {
		ts = ts[:0]
		labels := slices.Clone(s.Labels)
		slices.SortStableFunc(labels, func(a, c vmmodel.Label) int { return strings.Compare(a.Name, c.Name) })
		for _, l := range labels {
			lab = lab[:0]
			lab = protowire.AppendTag(lab, fieldName, protowire.BytesType)
			lab = protowire.AppendString(lab, l.Name)
			lab = protowire.AppendTag(lab, fieldValue, protowire.BytesType)
			lab = protowire.AppendString(lab, l.Value)
			ts = protowire.AppendTag(ts, fieldLabels, protowire.BytesType)
			ts = protowire.AppendBytes(ts, lab)
		}
		for _, sm := range s.Samples {
			var smp []byte
			smp = protowire.AppendTag(smp, fieldSampleVal, protowire.Fixed64Type)
			smp = protowire.AppendFixed64(smp, math.Float64bits(sm.Value))
			smp = protowire.AppendTag(smp, fieldSampleTS, protowire.VarintType)
			smp = protowire.AppendVarint(smp, uint64(sm.TimestampMs))
			ts = protowire.AppendTag(ts, fieldSamples, protowire.BytesType)
			ts = protowire.AppendBytes(ts, smp)
		}
		b = protowire.AppendTag(b, fieldTimeSeries, protowire.BytesType)
		b = protowire.AppendBytes(b, ts)
	}
	return b
}

// Compress is the body of a request: the message in snappy's block format.
func Compress(message []byte) []byte { return snappy.Encode(nil, message) }

// Request is one remote-write request, ready to post.
type Request struct {
	// Body is the compressed message.
	Body []byte
	// Samples and Series are what the message holds, markers included.
	Samples, Series int
}

// Batcher groups samples into requests of a fixed number of samples, in the order they
// are added, and hands each to send. Within a request the samples of one series are
// written together under one TimeSeries, in the order added; the series appear in order
// of their first sample. Only the last request may be shorter. One goroutine writes, as
// the store is written by one writer.
type Batcher struct {
	size   int
	labels func(uint32) []vmmodel.Label
	send   func(Request) error

	order []uint32
	by    map[uint32]*TimeSeries
	n     int
}

// NewBatcher returns a batcher of requests of size samples. labels gives the labels of a
// series by the index its samples carry.
func NewBatcher(size int, labels func(uint32) []vmmodel.Label, send func(Request) error) (*Batcher, error) {
	if size < 1 {
		return nil, fmt.Errorf("vmwrite: a request of %d samples", size)
	}
	return &Batcher{size: size, labels: labels, send: send, by: map[uint32]*TimeSeries{}}, nil
}

// Add adds samples, sending a request each time size are held.
func (b *Batcher) Add(samples []vmmodel.Sample) error {
	for _, s := range samples {
		ts := b.by[s.Series]
		if ts == nil {
			ts = &TimeSeries{Labels: b.labels(s.Series)}
			b.by[s.Series] = ts
			b.order = append(b.order, s.Series)
		}
		if s.Stale {
			ts.Samples = append(ts.Samples, Marker(s.TimestampMs))
		} else {
			ts.Samples = append(ts.Samples, One(s.TimestampMs))
		}
		b.n++
		if b.n == b.size {
			if err := b.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Flush sends the samples held as a request, if any.
func (b *Batcher) Flush() error {
	if b.n == 0 {
		return nil
	}
	series := make([]TimeSeries, len(b.order))
	for i, idx := range b.order {
		series[i] = *b.by[idx]
	}
	req := Request{Body: Compress(Marshal(series)), Samples: b.n, Series: len(series)}
	b.order, b.by, b.n = b.order[:0], map[uint32]*TimeSeries{}, 0
	return b.send(req)
}
