package pebblekv

import (
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// The value codec is the root module's. A record the benchmarks hold has no boot
// id and no event-time basis, so its value has both clear, which is byte for
// byte what the spike always wrote.

// Where says how an instant lies against the range a store can represent.
type Where = rootkv.Where

const (
	// Before is an instant earlier than store.MinEventTime.
	Before = rootkv.Before
	// Inside is an instant a store can hold.
	Inside = rootkv.Inside
	// After is an instant later than store.MaxEventTime.
	After = rootkv.After
)

// Locate converts an instant to Unix nanoseconds, or says it lies outside the
// representable range, with the nearest end as the number.
func Locate(t time.Time) (ns int64, where Where) { return rootkv.Locate(t) }

// Time is the instant a number of Unix nanoseconds stands for, in UTC.
func Time(ns int64) time.Time { return rootkv.Time(ns) }

// ValueFormat is the version byte that starts every value.
const ValueFormat = rootkv.ValueFormat

// ErrValue marks bytes that are not a value this package wrote.
var ErrValue = rootkv.ErrValue

// Value is what a layout stores for one version of one subject, apart from what
// its key already says.
type Value = rootkv.Value

// FromRecord is the value of a record.
func FromRecord(r engine.Record) Value {
	v := Value{Seq: r.Seq, Kind: r.Kind, TTL: r.TTL, Payload: r.Payload}
	if !r.Through.IsZero() {
		v.HasThrough, v.Through = true, r.Through.UnixNano()
	}
	return v
}

// DecodeValue reads a value written by [Value.Append]. The payload aliases b.
func DecodeValue(b []byte) (Value, error) { return rootkv.DecodeValue(b) }
