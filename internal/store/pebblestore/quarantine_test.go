package pebblestore

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

func hostObservation(seq uint64, at time.Time, boot string) store.Record {
	return store.Record{
		Layer: catalog.L1, Subject: store.EntitySubject(fingerprintOf(catalog.Host, 0x60)), Producer: "agent",
		EventTime: at, Seq: seq, Kind: lifecycle.Observe, Boot: boot, Payload: []byte("host"),
	}
}

// A retention keeps the whole prefix of an entity whose discarded records carry a
// boot, so a clone that appears after the horizon is judged against the boot
// history and quarantined, as the reference quarantines it.
func TestABootBearingPrefixIsKeptWholeAndACloneIsQuarantined(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	opts := Options{Policy: storetest.QuarantinePolicy(), Recorder: rec}
	p := newPair(t, opts)
	host := hostObservation(0, t0, "").Subject.A
	p.write(hostObservation(1, t0, "boot-a"), hostObservation(2, t0.Add(time.Minute), "boot-b"))
	before := dump(t, p.s)
	p.retain(t0.Add(10 * time.Minute))
	if rec.counters["retain.prefixes_kept_for_boots"] != 1 || rec.counters["retain.baselines_written"] != 0 || rec.counters["retain.range_deletes"] != 0 {
		t.Errorf("counters after a retention of a host with boots: %v", rec.counters)
	}
	if after := dump(t, p.s); len(after) != len(before) {
		t.Fatalf("the prefix was shortened:\n before %v\n after  %v", before, after)
	}
	// The old boot comes back after the new one: a clone, judged against records
	// the retention left alone.
	p.write(hostObservation(3, t0.Add(20*time.Minute), "boot-a"))
	sc := store.Current(catalog.L1)
	for _, at := range []time.Time{t0.Add(10 * time.Minute), t0.Add(30 * time.Minute), store.MaxEventTime} {
		ok, err := p.s.Alive(bg, host, at, sc)
		var q *store.QuarantineError
		if ok || !errors.As(err, &q) || q.Collision == nil || q.Collision.StaleBoot != "boot-a" || q.Collision.NewerBoot != "boot-b" || q.Entity != host || q.Layer != catalog.L1 {
			t.Errorf("Alive at %v = %v, %v; want a quarantine of boot-a against boot-b", at, ok, err)
		}
		_, want := p.ref.Alive(bg, host, at, sc)
		var wq *store.QuarantineError
		if !errors.As(want, &wq) || *wq.Collision != *q.Collision {
			t.Errorf("the reference says %v, the store %v", want, err)
		}
	}
	// A token before the clone sees no collision.
	ok, err := p.s.Alive(bg, host, t0.Add(30*time.Minute), store.Scope{Layer: catalog.L1, AsOf: 2})
	wantOK, wantErr := p.ref.Alive(bg, host, t0.Add(30*time.Minute), store.Scope{Layer: catalog.L1, AsOf: 2})
	if ok != wantOK || (err == nil) != (wantErr == nil) || !ok {
		t.Errorf("Alive pinned before the clone = %v, %v; the reference says %v, %v", ok, err, wantOK, wantErr)
	}
	// Windows never quarantine.
	if _, err := p.s.EntityWindow(bg, host, t0.Add(10*time.Minute), t0.Add(time.Hour), sc); err != nil {
		t.Errorf("EntityWindow of a quarantined entity = %v", err)
	}
}

// Records without a boot never keep a prefix whole, and a store with no boot key
// rewrites a prefix whatever its records carry.
func TestOnlyABootUnderABootPolicyKeepsAPrefixWhole(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		policy  lifecycle.Policy
		boot    string
		kept    int64
		deletes int64
	}{
		"a boot under a boot key":  {storetest.QuarantinePolicy(), "boot-a", 1, 0},
		"no boot under a boot key": {storetest.QuarantinePolicy(), "", 0, 1},
		"a boot with no boot key":  {lifecycle.Policy{}, "boot-a", 0, 1},
		"no boot and no boot key":  {lifecycle.Policy{}, "", 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := newMemRecorder()
			p := newPair(t, Options{Policy: tc.policy, Recorder: rec})
			p.write(hostObservation(1, t0, tc.boot))
			p.retain(t0.Add(time.Hour))
			if got := rec.counters["retain.prefixes_kept_for_boots"]; got != tc.kept {
				t.Errorf("prefixes kept for boots = %d, want %d", got, tc.kept)
			}
			if got := rec.counters["retain.range_deletes"]; got != tc.deletes {
				t.Errorf("range deletes = %d, want %d", got, tc.deletes)
			}
			host := hostObservation(0, t0, "").Subject.A
			got, err := p.s.Alive(bg, host, t0.Add(2*time.Hour), store.Current(catalog.L1))
			want, _ := p.ref.Alive(bg, host, t0.Add(2*time.Hour), store.Current(catalog.L1))
			if err != nil || got != want {
				t.Errorf("Alive = %v, %v; the reference says %v", got, err, want)
			}
		})
	}
}

// Without a boot key Alive is the walk that stops at the first live reference, and
// a record's boot is kept in the value and returned by a window.
func TestAliveWithoutABootKeyReadsOnlyWhatItNeeds(t *testing.T) {
	t.Parallel()
	rec := newMemRecorder()
	s := openMem(t, Options{Recorder: rec})
	var batch []store.Record
	for i := range 20 {
		r := hostObservation(uint64(i+1), t0.Add(time.Duration(i)*time.Second), "")
		r.Producer = lifecycle.Producer("p" + string(rune('a'+i%2)))
		batch = append(batch, r)
	}
	if err := s.Write(bg, batch); err != nil {
		t.Fatal(err)
	}
	host := batch[0].Subject.A
	if ok, err := s.Alive(bg, host, t0.Add(time.Hour), store.Current(catalog.L1)); err != nil || !ok {
		t.Fatalf("Alive = %v, %v", ok, err)
	}
	if got := rec.counters["read.records_stepped"]; got >= 20 {
		t.Errorf("Alive stepped over %d records; it should stop at the first live reference", got)
	}
}
