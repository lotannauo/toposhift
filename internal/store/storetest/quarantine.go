package storetest

import (
	"errors"
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// QuarantinePolicy is the policy a store is opened with to quarantine hosts:
// entity existence is folded with the boot id of the observations
// ([store.Record].Boot, which only a host observation carries) as the boot key, so
// that an old boot reported again after a newer one appeared is a clone collision.
// A backend must support at least this policy and the zero policy.
func QuarantinePolicy() lifecycle.Policy { return lifecycle.Policy{BootKey: lifecycle.BootID} }

// CheckQuarantine checks that an entity whose boots collided is quarantined: Alive
// answers false with a *store.QuarantineError that names the entity, the layer and
// the collision, whatever the instant, only for tokens that see the collision, and
// still after a Retain that discards the records of the collision. A Retain must
// also keep the boot history a later collision is judged against. Neighbors,
// NeighborsBatch, Window and EntityWindow never return the error. A host
// observation with a blank boot id is refused. Every read is compared with the
// reference's as well as with the values the scenario fixes. s must be a fresh
// store opened with [QuarantinePolicy].
func CheckQuarantine(st store.Store) error {
	host1, err := entityFP(catalog.Host, catalog.HostID, "quarantine-host-1")
	if err != nil {
		return err
	}
	host2, err := entityFP(catalog.Host, catalog.HostID, "quarantine-host-2")
	if err != nil {
		return err
	}
	rack, err := entityFP(catalog.Rack, catalog.RackID, "quarantine-rack")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	hostLayer := entityLayer(catalog.Host)
	observe := func(seq uint64, host identity.Fingerprint, p lifecycle.Producer, boot string, at time.Duration) store.Record {
		return store.Record{
			Layer: hostLayer, Subject: store.EntitySubject(host), Producer: p, EventTime: x.Add(at), Seq: seq,
			Kind: lifecycle.Observe, TTL: 3 * time.Minute, Payload: []byte("host"), Boot: boot,
		}
	}
	s, err := newScenario(st, QuarantinePolicy(), func(t time.Time) string { return t.Sub(x).String() })
	if err != nil {
		return err
	}
	defer s.close()

	// Host 1: boot-a, then boot-b, and then a clone of the first machine reports
	// boot-a again. Host 2 is the same without the clone, yet.
	first := uint64(seqBase)
	for _, r := range []store.Record{
		observe(first+1, host1, "node-collector", "boot-a", 0),
		observe(first+2, host1, "node-collector", "boot-b", time.Minute),
		{
			Layer: hostLayer, Subject: store.EdgeSubject(host1, rack, catalog.LocatedIn), Producer: "fabric", EventTime: x, Seq: first + 3,
			Kind: lifecycle.Observe, Payload: []byte("placement"),
		},
		observe(first+4, host2, "node-collector", "boot-a", 0),
		observe(first+5, host2, "node-collector", "boot-b", time.Minute),
		observe(first+6, host1, "clone", "boot-a", 90*time.Second),
	} {
		if err := s.write(r); err != nil {
			return err
		}
	}
	fps := []identity.Fingerprint{host1, host2, rack}
	layers := []catalog.Layer{hostLayer, catalog.L2}
	times := []time.Time{
		x.Add(-time.Hour), x.Add(-time.Nanosecond), x, x.Add(30 * time.Second), x.Add(time.Minute), x.Add(90 * time.Second),
		x.Add(2 * time.Minute), x.Add(4*time.Minute + 30*time.Second), x.Add(10 * time.Minute), time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	if err := s.check(fps, layers, times, tokensAround(first+1, first+6), store.MinEventTime); err != nil {
		return fmt.Errorf("before the retention: %w", err)
	}
	wantCollision := func(host identity.Fingerprint, observed, newerFirst time.Duration) *lifecycle.CloneCollisionError {
		return &lifecycle.CloneCollisionError{
			StaleBoot: "boot-a", ObservedAt: x.Add(observed), NewerBoot: "boot-b", NewerFirstSeen: x.Add(newerFirst),
		}
	}
	// Host 1 is quarantined at every instant, with the same collision.
	for _, t := range times {
		ok, err := st.Alive(bg, host1, t, store.Current(hostLayer))
		if err := exactQuarantine(fmt.Sprintf("Alive of host 1 at %s", s.rd.label(t)), ok, err, host1, hostLayer, wantCollision(host1, 90*time.Second, time.Minute)); err != nil {
			return err
		}
	}
	// Only a token that sees the clone's record sees the collision.
	for _, tok := range []uint64{0, first + 1, first + 2, first + 5} {
		ok, err := st.Alive(bg, host1, x.Add(10*time.Minute), store.Scope{Layer: hostLayer, AsOf: tok})
		if err != nil {
			return fmt.Errorf("host 1 as of %d, before the clone's record: want no error from Alive, got %w", tok, err)
		}
		_ = ok
	}
	// The same entity in a layer it is not in.
	if ok, err := st.Alive(bg, host1, x.Add(10*time.Minute), store.Current(catalog.L2)); ok || err != nil {
		return fmt.Errorf("host 1 in L2: Alive = %s; want false and no error", describeAlive(ok, err))
	}
	// Host 2 is not quarantined, and nothing else reads the error.
	if ok, err := st.Alive(bg, host2, x.Add(30*time.Second), store.Current(hostLayer)); err != nil || !ok {
		return fmt.Errorf("host 2: Alive = %s; want true and no error", describeAlive(ok, err))
	}
	for _, dir := range bothDirections {
		a := readArgs{ctx: bg, fp: host1, fps: []identity.Fingerprint{host1, host2, rack}, dir: dir, t: x.Add(30 * time.Second), to: x.Add(time.Hour), sc: store.Current(hostLayer)}
		for _, kind := range []string{"Neighbors", "NeighborsBatch", "Window", "EntityWindow"} {
			if _, err := ask(st, kind, a); err != nil {
				return fmt.Errorf("%s of a quarantined host must not return its quarantine: %w", describeArgs(kind, a), err)
			}
		}
	}

	// Host 2 is retained across: its first boot is before the horizon, and the clone
	// that reports it comes after, so the store must have kept the boot history.
	h := x.Add(2 * time.Minute)
	if err := s.retain(h); err != nil {
		return err
	}
	if err := s.write(observe(first+7, host2, "clone", "boot-a", 3*time.Minute)); err != nil {
		return err
	}
	after := []time.Time{h, x.Add(150 * time.Second), x.Add(3 * time.Minute), x.Add(10 * time.Minute), time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)}
	if err := s.check(fps, layers, after, []uint64{first + 6, first + 7, first + 8, 1 << 63, store.Latest - 1, store.Latest}, h); err != nil {
		return fmt.Errorf("after the retention: %w", err)
	}
	for _, t := range after {
		ok, err := st.Alive(bg, host2, t, store.Current(hostLayer))
		if err := exactQuarantine(fmt.Sprintf("Alive of host 2 at %s, after a clone reported a boot older than a boot before the horizon", s.rd.label(t)), ok, err, host2, hostLayer, wantCollision(host2, 3*time.Minute, time.Minute)); err != nil {
			return err
		}
		ok, err = st.Alive(bg, host1, t, store.Current(hostLayer))
		if err := exactQuarantine(fmt.Sprintf("Alive of host 1 at %s, after a retention that discarded the records of its collision", s.rd.label(t)), ok, err, host1, hostLayer, wantCollision(host1, 90*time.Second, time.Minute)); err != nil {
			return err
		}
	}
	// A token before the clone's record sees no collision for host 2.
	if _, err := st.Alive(bg, host2, h, store.Scope{Layer: hostLayer, AsOf: st.Horizon().Seq}); err != nil {
		return fmt.Errorf("host 2 as of the horizon's seq %d, before the clone's record: want no error from Alive, got %w", st.Horizon().Seq, err)
	}

	// What a host observation may carry: a blank boot id is refused, whole.
	seq := s.ref.LastSeq()
	blank := observe(seq+2, host2, "node-collector", " \t", 5*time.Minute)
	if err := wantInvalid("a host observation with a blank boot id", st.Write(bg, []store.Record{observe(seq+1, host1, "node-collector", "boot-b", 5*time.Minute), blank})); err != nil {
		return err
	}
	if got := st.LastSeq(); got != seq {
		return fmt.Errorf("a refused batch moved LastSeq to %d, want %d", got, seq)
	}
	// A delete carries none, and an observation that reports none is still an
	// observation.
	del := store.Record{
		Layer: hostLayer, Subject: store.EntitySubject(host2), Producer: "other", EventTime: x.Add(5 * time.Minute), Seq: seq + 1, Kind: lifecycle.Delete,
	}
	noBoot := observe(seq+2, host1, "other", "", 5*time.Minute)
	if err := s.write(del, noBoot); err != nil {
		return err
	}
	last := uint64(seq + 2)
	return s.check(fps, layers, after, []uint64{last - 1, last, last + 1, store.Latest}, h)
}

// exactQuarantine requires a read of Alive to say that the entity is quarantined
// with exactly the collision given.
func exactQuarantine(what string, ok bool, err error, host identity.Fingerprint, layer catalog.Layer, want *lifecycle.CloneCollisionError) error {
	var qe *store.QuarantineError
	switch {
	case !errors.As(err, &qe):
		return fmt.Errorf("%s = %s; want false and a *store.QuarantineError", what, describeAlive(ok, err))
	case ok:
		return fmt.Errorf("%s = true with the quarantine error %w; want false", what, err)
	case qe.Entity != host || qe.Layer != layer:
		return fmt.Errorf("%s: quarantine names %s in %s; want %s in %s", what, qe.Entity, qe.Layer, host, layer)
	case !errors.Is(err, store.ErrQuarantined) || !errors.Is(err, lifecycle.ErrCloneCollision):
		return fmt.Errorf("%s: %w must match store.ErrQuarantined and lifecycle.ErrCloneCollision", what, err)
	}
	if msg := collisionDiff(qe.Collision, want); msg != "" {
		return fmt.Errorf("%s: %s", what, msg)
	}
	return nil
}
