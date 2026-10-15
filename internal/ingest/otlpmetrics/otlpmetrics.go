// Package otlpmetrics is the narrow model of OpenTelemetry metrics that the
// metric translators read, and the few helpers they share. It decodes nothing:
// a receiver converts a request to a [Data], and the translators of
// package svcgraph and package spancount read it.
//
// # What the model holds
//
// Only sums are modelled, because the two connectors whose output toposhift reads
// (the service graph connector's request counter and the count connector's span
// counter) emit nothing else. A [Resource] holds its attributes, the newest time
// among all its points, its sum points, and the counts of the points the
// converter dropped because they were not sums or were not monotonic, so that a
// translator can count them. A [SumPoint] holds the name of its scope and of its
// metric, its temporality, whether the sum is monotonic, its start and its time,
// its value and its attributes. Histograms, gauges, summaries, exemplars and
// flags are not modelled.
//
// Times are OTLP's: nanoseconds since the Unix epoch in a uint64, where zero is
// unset. A time that does not fit an int64 of nanoseconds is unusable, and
// [Time] says so.
//
// # Attributes
//
// Attributes are the capture package's key-values, and [Collect] reads them the
// way the log translators do: a value is identity only if it is a string, given
// once. A key given twice with different values, or with a string and a value of
// another kind, is [StatusBad] and no value is chosen for it, because choosing
// one would be a guess.
//
// # Counts
//
// [Tally] and [Count] are the shape in which the translators report what they
// set aside: a reason and how many times.
package otlpmetrics

import (
	"maps"
	"math"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// MaxClockAhead is how far past the time a request was received a point's time may
// lie before a translator distrusts it. The collector and this process keep
// separate clocks. It is the same bound the log translators use.
const MaxClockAhead = 5 * time.Minute

// Temporality is the aggregation temporality of a sum. The values are OTLP's.
type Temporality uint8

const (
	// TemporalityUnspecified is a sum that does not say. A translator sets it
	// aside.
	TemporalityUnspecified Temporality = 0
	// Delta is a sum whose points each cover the interval since the previous one.
	Delta Temporality = 1
	// Cumulative is a sum whose points each cover the interval since the start
	// time, which is the same for the series until it is reset.
	Cumulative Temporality = 2
)

// String returns the temporality as OTLP names it, in lower case.
func (t Temporality) String() string {
	switch t {
	case TemporalityUnspecified:
		return "unspecified"
	case Delta:
		return "delta"
	case Cumulative:
		return "cumulative"
	}
	return "invalid"
}

// Data is the metrics of one request.
type Data struct {
	Resources []Resource
}

// Resource is the points of one resource.
type Resource struct {
	// Attributes are the resource's attributes, in the order received.
	Attributes []capture.KeyValue
	// NewestTimeUnixNano is the newest time among all the points of the resource,
	// the dropped ones included, or zero if there is none. The converter sets it;
	// the translators do not read it.
	NewestTimeUnixNano uint64
	// Points are the resource's sum points, in the order received.
	Points []SumPoint
	// DroppedNotSum counts the points the converter dropped because their metric
	// is not a sum, and DroppedNonMonotonic those dropped because it is a sum that
	// is not monotonic. A translator reports both and does nothing else with them.
	DroppedNotSum, DroppedNonMonotonic int
}

// SumPoint is one data point of a sum.
type SumPoint struct {
	// Scope is the name of the instrumentation scope, and Metric the name of the
	// metric.
	Scope, Metric string
	Temporality   Temporality
	// Monotonic says the sum only increases between resets.
	Monotonic bool
	// StartTimeUnixNano and TimeUnixNano are the point's start and time.
	StartTimeUnixNano, TimeUnixNano uint64
	// Value is the point's value, whether it was sent as an integer or as a
	// double. An integer beyond 2^53 is not held exactly.
	Value float64
	// Attributes are the point's attributes, in the order received.
	Attributes []capture.KeyValue
}

// Time converts an OTLP time. It reports false for zero, which is unset, and for
// a time outside the range a store can represent.
func Time(n uint64) (time.Time, bool) {
	if n == 0 || n > uint64(store.MaxEventTime.UnixNano()) {
		return time.Time{}, false
	}
	return time.Unix(0, int64(n)).UTC(), true
}

// Status says what a key of a set of attributes holds.
type Status uint8

const (
	// StatusAbsent is a key that is not there.
	StatusAbsent Status = iota
	// StatusOK is a key that is there as a string, with one value.
	StatusOK
	// StatusBad is a key that is there but is not a string, or is given more than
	// once with values that differ.
	StatusBad
)

type attribute struct {
	text *string
	bad  bool
}

// Attributes are the attributes of a resource or a point by key.
type Attributes map[string]attribute

// Collect reads a list of attributes. A key given twice with the same string value
// is that value; given twice with different values, or with a string and a value of
// another kind, it is bad.
func Collect(kvs []capture.KeyValue) Attributes {
	out := make(Attributes, len(kvs))
	for _, kv := range kvs {
		a, seen := out[kv.Key]
		switch {
		case !seen:
			a = attribute{text: kv.Value.StringValue, bad: kv.Value.StringValue == nil}
		case a.text != nil && kv.Value.StringValue != nil && *a.text == *kv.Value.StringValue:
			// The same value again: not a conflict.
		default:
			a.bad = true
		}
		out[kv.Key] = a
	}
	return out
}

// Get returns the string value of a key exactly as received, and its status. The
// value is empty unless the status is [StatusOK].
func (a Attributes) Get(key string) (string, Status) {
	v, ok := a[key]
	switch {
	case !ok:
		return "", StatusAbsent
	case v.bad || v.text == nil:
		return "", StatusBad
	}
	return *v.text, StatusOK
}

// The reasons the shared checks give for a point they set aside, and for the points
// the converter dropped. A translator reports them in its Skipped counts.
const (
	// SkipNotSum counts a point the converter dropped because its metric is not a
	// sum.
	SkipNotSum = "metric is not a sum: dropped by the converter"
	// SkipNonMonotonic counts a point of a sum that is not monotonic, whether the
	// converter dropped it or it arrived marked so.
	SkipNonMonotonic = "sum is not monotonic"
	// SkipNoTime counts a point whose time is zero or outside the range a store
	// can represent.
	SkipNoTime = "point has no usable time"
	// SkipBadValue counts a point whose value is negative, not a number or
	// infinite.
	SkipBadValue = "point value is negative or not finite"
	// SkipClockAhead counts a point whose time is later than the time the request
	// was received by more than [MaxClockAhead]. It is skipped, never rejected,
	// and never advances any state.
	SkipClockAhead = "point time is too far ahead of receipt"
)

// Dropped counts the points the converter dropped from r, in t.
func Dropped(t *Tally, r Resource) {
	t.AddN(SkipNotSum, r.DroppedNotSum)
	t.AddN(SkipNonMonotonic, r.DroppedNonMonotonic)
}

// Check applies the checks that every sum point must pass before a translator
// looks at what it says: it is monotonic, its time is usable, its value is a
// non-negative number, and its time is not more than [MaxClockAhead] after
// received. A received time that is zero, or that a store could not hold, skips the
// last check. It returns the point's time,
// or the reason the point is set aside.
func Check(p SumPoint, received time.Time) (time.Time, string) {
	if !p.Monotonic {
		return time.Time{}, SkipNonMonotonic
	}
	at, ok := Time(p.TimeUnixNano)
	if !ok {
		return time.Time{}, SkipNoTime
	}
	if math.IsNaN(p.Value) || math.IsInf(p.Value, 0) || p.Value < 0 {
		return time.Time{}, SkipBadValue
	}
	if validReceipt(received) && at.After(received.Add(MaxClockAhead)) {
		return time.Time{}, SkipClockAhead
	}
	return at, ""
}

// validReceipt says whether a receipt time is one a translator can compare with: it
// is given, and a store could hold it. Any other is taken as not given.
func validReceipt(received time.Time) bool {
	return !received.IsZero() && !received.Before(store.MinEventTime) && !received.After(store.MaxEventTime)
}

// Count is a number of things set aside or noted for one reason.
type Count struct {
	Reason string
	Count  int
}

// Tally counts things by reason. The zero value is ready to use.
type Tally map[string]int

// Add counts one thing for a reason, and AddN n things.
func (t *Tally) Add(reason string) { t.AddN(reason, 1) }

// AddN counts n things for a reason. It ignores n of zero or less.
func (t *Tally) AddN(reason string, n int) {
	if n <= 0 {
		return
	}
	if *t == nil {
		*t = make(Tally)
	}
	(*t)[reason] += n
}

// Counts returns the tally sorted by reason, or nil if it is empty.
func (t Tally) Counts() []Count {
	var out []Count
	for _, r := range slices.Sorted(maps.Keys(t)) {
		out = append(out, Count{Reason: r, Count: t[r]})
	}
	return out
}
