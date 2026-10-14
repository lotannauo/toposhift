package main

import (
	"cmp"
	"maps"
	"slices"
)

// ivState is the open interval of one key.
type ivState struct {
	start, last int32 // minutes from the start of the hour
	samples     int32
	files       int32 // archives the interval has a sample in
	lastFile    int32
	tag         uint32 // what the caller wants to know of the interval's first sample
}

// interval is a closed maximal interval: the key was seen at every sample of
// [start, last] with no gap of more than one TTL, in minutes from the start of
// the hour. Its half-open existence is [start, last + TTL).
type interval struct {
	key         uint64
	start, last int32
	samples     int32
	files       int32
	tag         uint32
}

// ivTracker finds the maximal intervals of keys from their raw samples, by
// the lifecycle's rule: a sample continues an interval if it is at most one
// TTL after the previous sample of the key (a gap of exactly one TTL touches,
// and touching runs are one interval), and starts a new one otherwise.
type ivTracker struct {
	step   int32 // minutes between consecutive samples
	ttl    int32 // the TTL in minutes
	strict bool  // a mutant: a gap of exactly one TTL splits
	state  map[uint64]ivState
	done   []interval
	// onGap, if set, is called with the number of samples missed when a key
	// reappears after a gap that does not split it.
	onGap func(missed int32)
}

func newTracker(step, ttl int32) *ivTracker {
	return &ivTracker{step: step, ttl: ttl, state: make(map[uint64]ivState)}
}

// observe records a sample of key at minute, in file (the archive index).
// Samples of one key must come in ascending minute order, once per minute.
func (t *ivTracker) observe(key uint64, minute int32, tag uint32, file int32) {
	st, ok := t.state[key]
	if ok {
		gap := minute - st.last
		cont := gap <= t.ttl
		if t.strict {
			cont = gap < t.ttl
		}
		if cont {
			if gap > t.step && t.onGap != nil {
				t.onGap(gap/t.step - 1)
			}
			st.last = minute
			st.samples++
			if file != st.lastFile {
				st.files++
				st.lastFile = file
			}
			t.state[key] = st
			return
		}
		t.done = append(t.done, interval{key, st.start, st.last, st.samples, st.files, st.tag})
	}
	t.state[key] = ivState{start: minute, last: minute, samples: 1, files: 1, lastFile: file, tag: tag}
}

// closeAll ends every open interval at its last sample, in key order.
func (t *ivTracker) closeAll() {
	for _, k := range slices.Sorted(maps.Keys(t.state)) {
		st := t.state[k]
		t.done = append(t.done, interval{k, st.start, st.last, st.samples, st.files, st.tag})
	}
	clear(t.state)
}

// finish closes what is open and returns every interval ordered by key, then start.
func (t *ivTracker) finish() []interval {
	t.closeAll()
	slices.SortFunc(t.done, func(a, b interval) int {
		return cmp.Or(cmp.Compare(a.key, b.key), cmp.Compare(a.start, b.start))
	})
	return t.done
}

// open is how many keys have an open interval.
func (t *ivTracker) open() int { return len(t.state) }

// pair packs two ids into one key.
func pair(a, b uint32) uint64 { return uint64(a)<<32 | uint64(b) }

// unpair is the ids of a pair.
func unpair(k uint64) (uint32, uint32) { return uint32(k >> 32), uint32(k) }

// censored is whether an interval touches an end of the hour: it starts at
// the first sample, or its last deadline passes the end.
func censored(iv interval, ttl, hourMinutes int32) (left, right bool) {
	return iv.start == 0, iv.last+ttl > hourMinutes
}
