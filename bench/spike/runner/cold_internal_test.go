package runner

import (
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"testing"
)

// gogc is the collector's target as the runtime reports it, "off" as -1.
func gogc() int {
	sample := []rtmetrics.Sample{{Name: "/gc/gogc:percent"}}
	rtmetrics.Read(sample)
	return int(int64(sample[0].Value.Uint64()))
}

// Reads that hold the collector off while they count allocations overlap in tests that
// run beside each other. However they overlap, the target is back to what it was when the
// last of them is done: one that restored what it found, when another held the collector
// off, would leave it off for the rest of the process.
func TestOverlappingHoldsOfTheCollectorLeaveItAsItWas(t *testing.T) {
	// Not parallel: the target is the process's.
	prev := debug.SetGCPercent(100)
	defer debug.SetGCPercent(prev)
	if gogc() != 100 {
		t.Skip("the runtime does not report the GC target")
	}
	first := holdGCOff()
	if gogc() != -1 {
		t.Errorf("target %d with a hold, want off", gogc())
	}
	second := holdGCOff()
	first() // the first to start is the first to end
	if gogc() != -1 {
		t.Errorf("target %d with one hold left, want off", gogc())
	}
	second()
	if got := gogc(); got != 100 {
		t.Errorf("target %d after the last hold, want the 100 it had", got)
	}
	// the last to start may be the first to end
	third, fourth := holdGCOff(), holdGCOff()
	fourth()
	if gogc() != -1 {
		t.Errorf("target %d with one hold left, want off", gogc())
	}
	third()
	if got := gogc(); got != 100 {
		t.Errorf("target %d after the last hold, want the 100 it had", got)
	}
	// and when it was off already, it stays off
	debug.SetGCPercent(-1)
	holdGCOff()()
	if got := gogc(); got != -1 {
		t.Errorf("target %d after a hold of a collector that was off, want off", got)
	}
}

// A hold released twice is released once: the second call must not drive the count
// below zero, which would stop every later hold from turning the collector off.
func TestAHoldReleasedTwiceIsReleasedOnce(t *testing.T) {
	prev := debug.SetGCPercent(100)
	defer debug.SetGCPercent(prev)
	if gogc() != 100 {
		t.Skip("the runtime does not report the GC target")
	}
	a := holdGCOff()
	b := holdGCOff()
	a()
	a() // again: no effect
	if gogc() != -1 {
		t.Errorf("target %d with one hold left after a double release, want off", gogc())
	}
	b()
	if got := gogc(); got != 100 {
		t.Errorf("target %d after the last hold, want the 100 it had", got)
	}
	c := holdGCOff()
	if gogc() != -1 {
		t.Errorf("target %d with a new hold after a double release, want off", gogc())
	}
	c()
}
