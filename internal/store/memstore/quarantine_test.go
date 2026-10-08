package memstore_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// bootPolicy is the policy that tells the boots of a host apart by the boot id
// its records carry.
var bootPolicy = lifecycle.Policy{BootKey: lifecycle.BootID}

// bootHost is a host with the records of a clone collision: a node collector
// sees boot-a, then boot-b, and then a clone of the first machine reports boot-a.
type bootHost struct {
	host, rack identity.Fingerprint
}

func newBootHost(t testing.TB, name string) bootHost {
	return bootHost{
		host: fp(t, catalog.Host, catalog.HostID, name),
		rack: fp(t, catalog.Rack, catalog.RackID, "rack-"+name),
	}
}

func (h bootHost) observe(seq uint64, producer lifecycle.Producer, boot string, when time.Duration) store.Record {
	return store.Record{
		Layer: catalog.L1, Subject: store.EntitySubject(h.host), Producer: producer, EventTime: at(when),
		Seq: seq, Kind: lifecycle.Observe, TTL: 3 * time.Minute, Payload: []byte("host"), Boot: boot,
	}
}

func (h bootHost) located(seq uint64, when time.Duration) store.Record {
	return store.Record{
		Layer: catalog.L1, Subject: store.EdgeSubject(h.host, h.rack, catalog.LocatedIn), Producer: "fabric",
		EventTime: at(when), Seq: seq, Kind: lifecycle.Observe, Payload: []byte("placement"),
	}
}

// collide writes the records of a clone collision from seq first on: boot-a at 0,
// boot-b at 1m, then the clone's boot-a at 90s. It returns the seq after them.
func (h bootHost) collide(t testing.TB, s *memstore.Store, first uint64) uint64 {
	write(t, s,
		h.observe(first, "node-collector", "boot-a", 0),
		h.observe(first+1, "node-collector", "boot-b", time.Minute),
		h.observe(first+2, "clone", "boot-a", 90*time.Second),
	)
	return first + 3
}

var l1 = store.Current(catalog.L1)

func wantQuarantine(t *testing.T, what string, err error, h bootHost) {
	t.Helper()
	var qe *store.QuarantineError
	if !errors.As(err, &qe) {
		t.Errorf("%s: err = %v, want a *store.QuarantineError", what, err)
		return
	}
	if qe.Entity != h.host || qe.Layer != catalog.L1 || qe.Collision == nil {
		t.Errorf("%s: quarantine = %+v, want host %s in L1 with a collision", what, qe, h.host)
		return
	}
	c := qe.Collision
	if c.StaleBoot != "boot-a" || c.NewerBoot != "boot-b" || !c.ObservedAt.Equal(at(90*time.Second)) || !c.NewerFirstSeen.Equal(at(time.Minute)) {
		t.Errorf("%s: collision = %+v, want boot-a seen at 90s after boot-b first appeared at 1m", what, c)
	}
	if !errors.Is(err, store.ErrQuarantined) || !errors.Is(err, lifecycle.ErrCloneCollision) {
		t.Errorf("%s: err = %v should match ErrQuarantined and ErrCloneCollision", what, err)
	}
}

func TestACloneCollisionQuarantinesTheEntity(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	s := open(t, memstore.Options{Policy: bootPolicy})
	h.collide(t, s, 1)

	// Whatever the instant is, and the answer is false with the error.
	for name, when := range map[string]time.Time{
		"before the host exists": at(-time.Hour), "during the first boot": at(30 * time.Second),
		"during the collision": at(100 * time.Second), "much later": at(time.Hour), "in 9999": time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC),
	} {
		alive, err := s.Alive(context.Background(), h.host, when, l1)
		if alive {
			t.Errorf("%s: a quarantined host is alive", name)
		}
		wantQuarantine(t, name, err, h)
	}

	// Pinned to a token before the clone record, there is no collision yet.
	for _, tt := range []struct {
		when  time.Duration
		asOf  uint64
		alive bool
	}{
		{-time.Second, 2, false},
		{30 * time.Second, 2, true},
		{2 * time.Minute, 2, true},
		{10 * time.Minute, 2, false},
		{30 * time.Second, 1, true},
		{90 * time.Second, 0, false},
	} {
		alive, err := s.Alive(context.Background(), h.host, at(tt.when), store.Scope{Layer: catalog.L1, AsOf: tt.asOf})
		if err != nil || alive != tt.alive {
			t.Errorf("as of %d at %s: Alive = %v, %v; want %v, nil", tt.asOf, tt.when, alive, err, tt.alive)
		}
	}
	// And at the token that sees it.
	if _, err := s.Alive(context.Background(), h.host, at(30*time.Second), store.Scope{Layer: catalog.L1, AsOf: 3}); err == nil {
		t.Error("the token of the clone record does not see the collision")
	}

	// Another layer does not hold the host, so there is nothing to quarantine.
	if alive, err := s.Alive(context.Background(), h.host, at(30*time.Second), l2); err != nil || alive {
		t.Errorf("Alive in L2 = %v, %v; want false, nil", alive, err)
	}
}

func TestOtherReadsOfAQuarantinedEntityAreAnswered(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	s := open(t, memstore.Options{Policy: bootPolicy})
	write(t, s, h.located(1, 0))
	h.collide(t, s, 2)
	ctx := context.Background()

	if _, err := s.Alive(ctx, h.host, at(30*time.Second), l1); err == nil {
		t.Fatal("the host is not quarantined")
	}
	ns, err := s.Neighbors(ctx, h.host, store.Forward, at(30*time.Second), l1)
	if err != nil || !slices.Equal(ns, []store.Neighbor{{Peer: h.rack, Relation: catalog.LocatedIn}}) {
		t.Errorf("Neighbors = %v, %v; want the rack", ns, err)
	}
	if ns, err := s.Neighbors(ctx, h.rack, store.Reverse, at(30*time.Second), l1); err != nil || len(ns) != 1 {
		t.Errorf("reverse Neighbors = %v, %v; want the host", ns, err)
	}
	batch, err := s.NeighborsBatch(ctx, []identity.Fingerprint{h.host, h.rack}, store.Forward, at(30*time.Second), l1)
	if err != nil || len(batch) != 2 || len(batch[0]) != 1 || len(batch[1]) != 0 {
		t.Errorf("NeighborsBatch = %v, %v", batch, err)
	}
	ws, err := s.Window(ctx, h.host, store.Forward, at(0), at(time.Hour), l1)
	if err != nil || !slices.Equal(seqs(ws), []uint64{1}) {
		t.Errorf("Window = %v, %v; want the placement", seqs(ws), err)
	}
	es, err := s.EntityWindow(ctx, h.host, at(0), at(time.Hour), l1)
	if err != nil || !slices.Equal(seqs(es), []uint64{2, 3, 4}) { // boot-a at 0, boot-b at 1m, the clone's boot-a at 90s
		t.Errorf("EntityWindow = %v, %v; want the three existence records in event order", seqs(es), err)
	}
	var boots []string
	for _, r := range es {
		boots = append(boots, r.Boot)
	}
	if want := []string{"boot-a", "boot-b", "boot-a"}; !slices.Equal(boots, want) {
		t.Errorf("EntityWindow boots = %v, want %v: the records keep their boot ids", boots, want)
	}
}

// TestAQuarantineSurvivesRetention covers the retention boundary. The first host
// collides before the horizon and stays quarantined after it. The second host has
// seen two boots before the horizon, and a stale boot that reappears after the
// Retain still collides with the newer one. This store keeps everything, so both
// hold trivially; they are what a real backend, which may replace the records
// before the horizon with a baseline, has to reproduce: the boot history that a
// later collision is judged against must survive Retain.
func TestAQuarantineSurvivesRetention(t *testing.T) {
	t.Parallel()
	h1, h2 := newBootHost(t, "h1"), newBootHost(t, "h2")
	s := open(t, memstore.Options{Policy: bootPolicy})
	ctx := context.Background()

	// Everything about both hosts before the horizon is written first: after Retain,
	// records before it are refused.
	next := h1.collide(t, s, 1)
	write(t, s,
		h2.observe(next, "node-collector", "boot-a", 0),
		h2.observe(next+1, "node-collector", "boot-b", time.Minute),
	)
	horizonSeq := next + 1
	if err := s.Retain(ctx, at(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if got := s.Horizon(); got.Seq != horizonSeq {
		t.Fatalf("Horizon = %+v, want seq %d", got, horizonSeq)
	}
	// The clone of the second host is seen only after the horizon.
	write(t, s, h2.observe(horizonSeq+1, "clone", "boot-a", 3*time.Minute))

	after := at(2 * time.Minute)
	for _, asOf := range []uint64{horizonSeq, store.Latest} {
		_, err := s.Alive(ctx, h1.host, after, store.Scope{Layer: catalog.L1, AsOf: asOf})
		wantQuarantine(t, "the first host after the horizon", err, h1)
	}

	// The second host's boot history is what the later collision is judged against.
	alive, err := s.Alive(ctx, h2.host, after, l1)
	if alive {
		t.Error("the second host is alive")
	}
	var qe *store.QuarantineError
	if !errors.As(err, &qe) || qe.Collision == nil || qe.Collision.StaleBoot != "boot-a" || qe.Collision.NewerBoot != "boot-b" ||
		!qe.Collision.ObservedAt.Equal(at(3*time.Minute)) || !qe.Collision.NewerFirstSeen.Equal(at(time.Minute)) {
		t.Errorf("second host at Latest: err = %v, want the collision of boot-a at 3m after boot-b at 1m", err)
	}
	// Before its clone record it is not quarantined.
	alive, err = s.Alive(ctx, h2.host, after, store.Scope{Layer: catalog.L1, AsOf: horizonSeq})
	if err != nil || !alive {
		t.Errorf("second host as of the horizon's seq = %v, %v; want true, nil", alive, err)
	}
}

func TestBootIdsAreCheckedWhenRecordsAreWritten(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	ctx := context.Background()

	withBoot := func(mod func(*store.Record)) store.Record {
		r := h.observe(2, "node-collector", "boot-a", 0)
		mod(&r)
		return r
	}
	for name, r := range map[string]store.Record{
		"a blank boot":          withBoot(func(r *store.Record) { r.Boot = " " }),
		"a boot of white space": withBoot(func(r *store.Record) { r.Boot = "\t\n" }),
		"a boot on a delete":    withBoot(func(r *store.Record) { r.Kind, r.TTL, r.Payload = lifecycle.Delete, 0, nil }),
		"a boot on an edge":     func() store.Record { r := h.located(2, 0); r.Boot = "boot-a"; return r }(),
	} {
		for policyName, p := range map[string]lifecycle.Policy{"boot policy": bootPolicy, "zero policy": {}} {
			s := open(t, memstore.Options{Policy: p})
			batch := []store.Record{h.observe(1, "node-collector", "boot-a", 0), r, h.observe(3, "node-collector", "boot-b", time.Minute)}
			if err := s.Write(ctx, batch); !errors.Is(err, store.ErrInvalid) || errors.Is(err, store.ErrBeforeHorizon) {
				t.Errorf("%s under the %s: err = %v, want ErrInvalid", name, policyName, err)
			}
			if got := s.LastSeq(); got != 0 {
				t.Errorf("%s under the %s: LastSeq = %d after a refused batch", name, policyName, got)
			}
			if got := s.Layers(h.host); len(got) != 0 {
				t.Errorf("%s under the %s: the refused batch left the layers %v", name, policyName, got)
			}
		}
	}

	t.Run("what is accepted", func(t *testing.T) {
		t.Parallel()
		s := open(t, memstore.Options{Policy: bootPolicy})
		noBoot := h.observe(2, "fabric", "", 0)
		del := h.observe(3, "node-collector", "", time.Minute)
		del.Kind, del.TTL, del.Payload = lifecycle.Delete, 0, nil
		if err := s.Write(ctx, []store.Record{h.observe(1, "node-collector", "boot-a", 0), noBoot, del, h.located(4, 0)}); err != nil {
			t.Errorf("an observation without a boot, a delete and an edge: %v", err)
		}
	})

	// The one record Validate lets through and a policy can still not fold: a
	// policy whose boot key is the payload, which cannot be blank.
	t.Run("a policy that reads the payload as the boot", func(t *testing.T) {
		t.Parallel()
		s := open(t, memstore.Options{Policy: lifecycle.Policy{BootKey: "payload"}})
		blank := h.observe(2, "node-collector", "", 0)
		blank.Payload = nil
		err := s.Write(ctx, []store.Record{h.observe(1, "node-collector", "", 0), blank})
		if !errors.Is(err, store.ErrInvalid) || s.LastSeq() != 0 {
			t.Errorf("a blank payload under a payload boot key: err = %v, LastSeq = %d; want ErrInvalid and nothing stored", err, s.LastSeq())
		}
		if err := s.Write(ctx, []store.Record{h.observe(1, "node-collector", "", 0)}); err != nil {
			t.Errorf("a payload under a payload boot key: %v", err)
		}
	})
}

func TestWithoutBootTrackingNothingIsQuarantined(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	// A policy without the boot id key ignores the boots the records carry.
	for name, p := range map[string]lifecycle.Policy{
		"zero":                {},
		"another boot key":    {BootKey: "other.key"},
		"ranked, no boot key": {Rank: map[lifecycle.Producer]int{"clone": 1}},
		"skewed enough":       {BootKey: lifecycle.BootID, Skew: time.Hour},
	} {
		s := open(t, memstore.Options{Policy: p})
		h.collide(t, s, 1)
		alive, err := s.Alive(context.Background(), h.host, at(30*time.Second), l1)
		if err != nil || !alive {
			t.Errorf("%s: Alive = %v, %v; want true, nil", name, alive, err)
		}
	}

	// A producer that reports no boot cannot collide with one that does.
	s := open(t, memstore.Options{Policy: bootPolicy})
	write(t, s,
		h.observe(1, "node-collector", "boot-a", 0),
		h.observe(2, "node-collector", "boot-b", time.Minute),
		h.observe(3, "fabric", "", 90*time.Second),
	)
	if alive, err := s.Alive(context.Background(), h.host, at(100*time.Second), l1); err != nil || !alive {
		t.Errorf("a producer without a boot: Alive = %v, %v; want true, nil", alive, err)
	}
}

func TestOpenRefusesAPolicyTheSpecificationRefuses(t *testing.T) {
	t.Parallel()
	for name, p := range map[string]lifecycle.Policy{
		"a negative skew":       {Skew: -time.Second},
		"a boot key not valid":  {BootKey: "Not Valid"},
		"a skew and a boot key": {BootKey: "ok", Skew: -1},
	} {
		s, err := memstore.Open(memstore.Options{Policy: p})
		if !errors.Is(err, store.ErrInvalid) || !errors.Is(err, lifecycle.ErrInvalid) {
			t.Errorf("%s: err = %v, want it to wrap ErrInvalid and lifecycle.ErrInvalid", name, err)
		}
		if s != nil {
			t.Errorf("%s: Open returned a store with its error", name)
		}
	}
	for name, p := range map[string]lifecycle.Policy{
		"the zero policy": {},
		"a boot key":      {BootKey: lifecycle.BootID},
		"a skew":          {BootKey: lifecycle.BootID, Skew: time.Minute},
		"a rank":          {Rank: map[lifecycle.Producer]int{"a": 1}},
	} {
		s, err := memstore.Open(memstore.Options{Policy: p})
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		_ = s.Close()
	}
}

func TestSkewToleratesAnOldBootObservedAgain(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	for _, tt := range []struct {
		skew time.Duration
		want bool // quarantined
	}{{0, true}, {29 * time.Second, true}, {30 * time.Second, false}, {time.Hour, false}} {
		s := open(t, memstore.Options{Policy: lifecycle.Policy{BootKey: lifecycle.BootID, Skew: tt.skew}})
		h.collide(t, s, 1) // boot-a is seen again 30s after boot-b first appeared
		_, err := s.Alive(context.Background(), h.host, at(30*time.Second), l1)
		if quarantined := err != nil; quarantined != tt.want {
			t.Errorf("skew %s: err = %v, want quarantined %v", tt.skew, err, tt.want)
		}
	}
}

func TestAQuarantineErrorIsACopyTheCallerMayChange(t *testing.T) {
	t.Parallel()
	h := newBootHost(t, "h1")
	s := open(t, memstore.Options{Policy: bootPolicy})
	h.collide(t, s, 1)
	ctx := context.Background()

	_, err := s.Alive(ctx, h.host, at(30*time.Second), l1)
	var qe *store.QuarantineError
	if !errors.As(err, &qe) || qe.Collision == nil {
		t.Fatalf("err = %v, want a quarantine", err)
	}
	// The caller changes everything it was given.
	qe.Entity, qe.Layer = identity.Fingerprint{}, catalog.L3
	qe.Collision.StaleBoot, qe.Collision.NewerBoot = "changed", "changed"
	qe.Collision.ObservedAt, qe.Collision.NewerFirstSeen = time.Time{}, time.Time{}

	for _, scope := range []store.Scope{l1, {Layer: catalog.L1, AsOf: 3}} {
		_, err = s.Alive(ctx, h.host, at(time.Hour), scope)
		wantQuarantine(t, "after the caller changed an earlier error", err, h)
	}
}
