package prefixfilter

import (
	"encoding/binary"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

// scaleKey fills key, 20 bytes, with the i-th key of a fixed sequence of
// distinct keys. Its leading bytes repeat every 64 keys, as the prefixes of one
// entity share theirs, and the rest is a bijective scramble of i, so the keys
// are distinct by construction.
func scaleKey(key []byte, i uint64) {
	binary.BigEndian.PutUint32(key[0:4], uint32(i>>6))
	binary.BigEndian.PutUint64(key[4:12], mix64(i+1))
	binary.BigEndian.PutUint64(key[12:20], i)
}

// TestFilterAtThirtyDayScale builds a filter from the number of prefixes a
// 400-node cluster accumulates in 30 days, and logs what that costs. It
// allocates hundreds of megabytes, so it runs only when TOPOSHIFT_FILTER_SCALE=1
// is set, and CI does not set it.
func TestFilterAtThirtyDayScale(t *testing.T) {
	if os.Getenv("TOPOSHIFT_FILTER_SCALE") != "1" {
		t.Skip("set TOPOSHIFT_FILTER_SCALE=1 to build a filter of 16.5 million keys")
	}
	const (
		n       = 16_500_000
		samples = 1_000_000
	)

	keys := func(yield func([]byte) bool) {
		key := make([]byte, 20)
		for i := range uint64(n) {
			scaleKey(key, i)
			if !yield(key) {
				return
			}
		}
	}

	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)

	// Sample the heap while Build runs: after Build returns, the transient hashes
	// and counters are garbage, and only the sampling sees the peak.
	var (
		mu       sync.Mutex
		peak     uint64
		stop     = make(chan struct{})
		sampling sync.WaitGroup
	)
	sampling.Add(1)
	go func() {
		defer sampling.Done()
		var m runtime.MemStats
		for {
			runtime.ReadMemStats(&m)
			mu.Lock()
			peak = max(peak, m.HeapInuse)
			mu.Unlock()
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
		}
	}()

	start := time.Now()
	f, err := Build(keys)
	buildTime := time.Since(start)
	close(stop)
	sampling.Wait()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	runtime.ReadMemStats(&after)
	runtime.GC()
	var settled runtime.MemStats
	runtime.ReadMemStats(&settled)
	runtime.KeepAlive(f)

	t.Logf("build: %v for %d keys (%d distinct)", buildTime, n, f.Len())
	t.Logf("size: %d bytes (%.2f bits per key), seed %#x", f.SizeBytes(), 8*float64(f.SizeBytes())/n, f.seed)
	t.Logf("heap: peak in use %d MB sampled during Build (counts garbage the collector has not yet freed), "+
		"%d MB in use right after, %d MB after a collection, %d MB allocated over the build",
		peak>>20, after.HeapInuse>>20, settled.HeapInuse>>20, (after.TotalAlloc-before.TotalAlloc)>>20)

	if f.SizeBytes() > 21_000_000 {
		t.Errorf("SizeBytes = %d, want at most 21,000,000", f.SizeBytes())
	}

	// Hits: every 16th key, then the same number of keys that were not built in.
	key := make([]byte, 20)
	start = time.Now()
	missed := 0
	for i := range uint64(samples) {
		scaleKey(key, i*(n/samples))
		if !f.Contains(key) {
			missed++
		}
	}
	hitTime := time.Since(start)
	if missed != 0 {
		t.Errorf("%d false negatives in %d keys that were built in", missed, samples)
	}

	start = time.Now()
	falsePositives := 0
	for i := range uint64(samples) {
		scaleKey(key, n+i)
		if f.Contains(key) {
			falsePositives++
		}
	}
	missTime := time.Since(start)

	t.Logf("Contains: %.1f ns per hit, %.1f ns per miss (%d queries each, includes making the key)",
		float64(hitTime.Nanoseconds())/samples, float64(missTime.Nanoseconds())/samples, samples)
	rate := float64(falsePositives) / samples
	t.Logf("false positives: %d in %d (%.3f%%, expected 0.391%%)", falsePositives, samples, 100*rate)
	if rate < 0.0034 || rate > 0.0045 {
		t.Errorf("false-positive rate %.4f outside 0.0034 to 0.0045", rate)
	}
}
