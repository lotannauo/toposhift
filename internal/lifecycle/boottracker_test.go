package lifecycle_test

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// foldBootsBefore is how Fold found boots and collisions before the fold was split
// into Sightings and a BootTracker, kept as it was (with the package's unexported
// helpers written out), so that the split is shown to change no answer.
func foldBootsBefore(as []lifecycle.Assertion, p lifecycle.Policy) ([]lifecycle.Boot, error) {
	if p.BootKey == "" {
		return nil, nil
	}
	sorted := slices.Clone(as)
	slices.SortFunc(sorted, func(a, b lifecycle.Assertion) int {
		if c := a.EventTime.Compare(b.EventTime); c != 0 {
			return c
		}
		switch {
		case a.Seq < b.Seq:
			return -1
		case a.Seq > b.Seq:
			return 1
		}
		return 0
	})
	// latestPerInstant, then effective.
	refs := map[lifecycle.Producer][]lifecycle.Assertion{}
	for _, a := range sorted {
		list := refs[a.Producer]
		if n := len(list); n > 0 && list[n-1].EventTime.Equal(a.EventTime) {
			list[n-1] = a
		} else {
			list = append(list, a)
		}
		refs[a.Producer] = list
	}
	var effective []lifecycle.Assertion
	for _, list := range refs {
		effective = append(effective, list...)
	}
	slices.SortFunc(effective, func(a, b lifecycle.Assertion) int {
		if c := a.EventTime.Compare(b.EventTime); c != 0 {
			return c
		}
		return int(a.Seq) - int(b.Seq)
	})
	bootOf := func(a lifecycle.Assertion, key catalog.AttributeKey) string {
		for _, attr := range a.Attrs {
			if attr.Key == key {
				return attr.Value.(string)
			}
		}
		return ""
	}

	var list []lifecycle.Boot
	index := make(map[string]int)
	observe := func(id string, at time.Time) error {
		i, known := index[id]
		if !known {
			index[id] = len(list)
			list = append(list, lifecycle.Boot{ID: id, FirstSeen: at, LastSeen: at})
			return nil
		}
		if i+1 < len(list) {
			newer := list[i+1]
			if at.Sub(newer.FirstSeen) > p.Skew {
				return &lifecycle.CloneCollisionError{
					StaleBoot: id, ObservedAt: at,
					NewerBoot: newer.ID, NewerFirstSeen: newer.FirstSeen,
				}
			}
		}
		list[i].LastSeen = at
		return nil
	}
	type point struct {
		at time.Time
		id string
	}
	var points []point
	for _, a := range effective {
		if a.Kind != lifecycle.Observe {
			continue
		}
		id := bootOf(a, p.BootKey)
		if id == "" {
			continue
		}
		points = append(points, point{a.EventTime, id})
		if a.Through.After(a.EventTime) {
			points = append(points, point{a.Through, id})
		}
	}
	slices.SortFunc(points, func(x, y point) int {
		if c := x.at.Compare(y.at); c != 0 {
			return c
		}
		return strings.Compare(x.id, y.id)
	})
	for _, pt := range points {
		if err := observe(pt.id, pt.at); err != nil {
			return nil, err
		}
	}
	return list, nil
}

func sameCollision(a, b *lifecycle.CloneCollisionError) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.StaleBoot == b.StaleBoot && a.NewerBoot == b.NewerBoot && a.ObservedAt.Equal(b.ObservedAt) && a.NewerFirstSeen.Equal(b.NewerFirstSeen)
}

func collisionOf(err error) *lifecycle.CloneCollisionError {
	var ce *lifecycle.CloneCollisionError
	if errors.As(err, &ce) {
		return ce
	}
	return nil
}

// Fold gives the boots and the collision it gave before it used a BootTracker.
func TestPropertyFoldBootsAreWhatTheyWereBeforeTheTracker(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(true).Draw(t, "assertions")
		pol := genPolicy(true).Draw(t, "policy")
		tl, err := lifecycle.Fold(as, pol)
		want, wantErr := foldBootsBefore(as, pol)
		if (err == nil) != (wantErr == nil) || !sameCollision(collisionOf(err), collisionOf(wantErr)) {
			t.Fatalf("Fold: %v; before the tracker: %v", err, wantErr)
		}
		if err == nil && !slices.Equal(tl.Boots(), want) {
			t.Fatalf("Fold's boots %+v; before the tracker %+v", tl.Boots(), want)
		}
	})
}

// A fold of the sightings cut at any instants, each piece taken by a new tracker
// seeded with the state the last one reached, is the fold of them all: the state
// is all the rest of the fold needs.
func TestPropertyASeededTrackerGoesOnAsIfItNeverStopped(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		as := genAssertions(true).Draw(t, "assertions")
		pol := genPolicy(true).Draw(t, "policy")
		tl, err := lifecycle.Fold(as, pol)
		sights, serr := lifecycle.Sightings(as, pol)
		if serr != nil {
			t.Fatal(serr)
		}
		if !slices.IsSortedFunc(sights, lifecycle.CompareSightings) || len(slices.CompactFunc(slices.Clone(sights), func(x, y lifecycle.Sighting) bool { return lifecycle.CompareSightings(x, y) == 0 })) != len(sights) {
			t.Fatalf("the sightings are not sorted, or repeat: %+v", sights)
		}
		cuts := rapid.SliceOfN(rapid.Int64Range(-1, 80), 0, 4).Draw(t, "cuts")
		slices.Sort(cuts)
		tr := lifecycle.NewBootTracker(pol)
		var got error
		i := 0
		for _, c := range append(cuts, 1000) {
			for ; i < len(sights) && sights[i].At.Before(sec(c)); i++ {
				if e := tr.Observe(sights[i].ID, sights[i].At); e != nil && got == nil {
					got = e
				}
			}
			next := lifecycle.NewBootTracker(pol)
			if e := next.Seed(tr.Boots(), tr.Collision()); e != nil {
				t.Fatalf("seeding with a state a tracker reached: %v", e)
			}
			tr = next
		}
		if got == nil && tr.Collision() != nil {
			t.Fatal("a collision that no sighting returned")
		}
		if (err == nil) != (got == nil) || !sameCollision(collisionOf(err), tr.Collision()) || !sameCollision(collisionOf(err), collisionOf(got)) {
			t.Fatalf("Fold: %v; the pieces: %v, tracker %+v", err, got, tr.Collision())
		}
		if err == nil && !slices.Equal(tl.Boots(), tr.Boots()) {
			t.Fatalf("Fold's boots %+v; the pieces' %+v", tl.Boots(), tr.Boots())
		}
	})
}

func TestBootTracker(t *testing.T) {
	t.Parallel()
	pol := lifecycle.Policy{BootKey: lifecycle.BootID, Skew: time.Second}

	t.Run("a collision is kept and nothing changes after it", func(t *testing.T) {
		t.Parallel()
		tr := lifecycle.NewBootTracker(pol)
		for _, s := range []lifecycle.Sighting{{at(0), "A"}, {at(minute), "B"}} {
			if err := tr.Observe(s.ID, s.At); err != nil {
				t.Fatal(err)
			}
		}
		// Exactly the skew after B first appeared: not a collision.
		if err := tr.Observe("A", at(minute+time.Second)); err != nil {
			t.Fatalf("A at the skew's limit: %v", err)
		}
		err := tr.Observe("A", at(minute+time.Second+1))
		want := &lifecycle.CloneCollisionError{StaleBoot: "A", ObservedAt: at(minute + time.Second + 1), NewerBoot: "B", NewerFirstSeen: at(minute)}
		if !sameCollision(collisionOf(err), want) || !errors.Is(err, lifecycle.ErrCloneCollision) {
			t.Fatalf("A a nanosecond past the skew: %v", err)
		}
		before := tr.Boots()
		if err := tr.Observe("C", at(2*minute)); !sameCollision(collisionOf(err), want) {
			t.Errorf("a sighting after the collision: %v", err)
		}
		if !slices.Equal(tr.Boots(), before) || !sameCollision(tr.Collision(), want) {
			t.Errorf("the state changed after the collision: %+v, %+v", tr.Boots(), tr.Collision())
		}
		// The copies it hands out are copies.
		tr.Collision().StaleBoot = "X"
		tr.Boots()[0].ID = "X"
		if !sameCollision(tr.Collision(), want) || tr.Boots()[0].ID != "A" {
			t.Error("changing a returned value changed the tracker")
		}
	})

	t.Run("a sighting out of order is refused", func(t *testing.T) {
		t.Parallel()
		tr := lifecycle.NewBootTracker(pol)
		if err := tr.Observe("B", at(minute)); err != nil {
			t.Fatal(err)
		}
		for name, s := range map[string]lifecycle.Sighting{
			"an earlier instant":                   {at(0), "A"},
			"an earlier ID at the instant":         {at(minute), "A"},
			"no ID":                                {at(2 * minute), ""},
			"an earlier instant than the seed has": {at(0), "B"},
		} {
			if err := tr.Observe(s.ID, s.At); !errors.Is(err, lifecycle.ErrInvalid) {
				t.Errorf("%s: %v, want ErrInvalid", name, err)
			}
		}
		if err := tr.Observe("B", at(minute)); err != nil {
			t.Errorf("the same sighting again: %v", err)
		}
		if got := tr.Boots(); len(got) != 1 || got[0] != (lifecycle.Boot{ID: "B", FirstSeen: at(minute), LastSeen: at(minute)}) {
			t.Errorf("a refused sighting changed the boots: %+v", got)
		}
		seeded := lifecycle.NewBootTracker(pol)
		if err := seeded.Seed(tr.Boots(), nil); err != nil {
			t.Fatal(err)
		}
		if err := seeded.Observe("Z", at(0)); !errors.Is(err, lifecycle.ErrInvalid) {
			t.Errorf("a sighting before the seed's last instant: %v, want ErrInvalid", err)
		}
	})

	t.Run("a seed no fold gives is refused", func(t *testing.T) {
		t.Parallel()
		a, b := lifecycle.Boot{ID: "A", FirstSeen: at(0), LastSeen: at(minute)}, lifecycle.Boot{ID: "B", FirstSeen: at(30 * time.Second), LastSeen: at(30 * time.Second)}
		hit := &lifecycle.CloneCollisionError{StaleBoot: "A", ObservedAt: at(2 * minute), NewerBoot: "B", NewerFirstSeen: at(30 * time.Second)}
		for name, tc := range map[string]struct {
			boots     []lifecycle.Boot
			collision *lifecycle.CloneCollisionError
		}{
			"no ID":                       {[]lifecycle.Boot{{FirstSeen: at(0), LastSeen: at(0)}}, nil},
			"an ID twice":                 {[]lifecycle.Boot{a, a}, nil},
			"last seen before first":      {[]lifecycle.Boot{{ID: "A", FirstSeen: at(minute), LastSeen: at(0)}}, nil},
			"out of order of appearance":  {[]lifecycle.Boot{b, a}, nil},
			"a collision with no boots":   {nil, hit},
			"a collision with no newer":   {[]lifecycle.Boot{a}, hit},
			"a collision with another":    {[]lifecycle.Boot{a, b}, &lifecycle.CloneCollisionError{StaleBoot: "B", ObservedAt: at(2 * minute), NewerBoot: "A", NewerFirstSeen: at(0)}},
			"a newer first seen elsewise": {[]lifecycle.Boot{a, b}, &lifecycle.CloneCollisionError{StaleBoot: "A", ObservedAt: at(2 * minute), NewerBoot: "B", NewerFirstSeen: at(0)}},
			"observed before the newer":   {[]lifecycle.Boot{a, b}, &lifecycle.CloneCollisionError{StaleBoot: "A", ObservedAt: at(30 * time.Second), NewerBoot: "B", NewerFirstSeen: at(30 * time.Second)}},
		} {
			tr := lifecycle.NewBootTracker(pol)
			if err := tr.Observe("Q", at(time.Hour)); err != nil {
				t.Fatal(err)
			}
			if err := tr.Seed(tc.boots, tc.collision); !errors.Is(err, lifecycle.ErrInvalid) {
				t.Errorf("%s: Seed = %v, want ErrInvalid", name, err)
			}
			if got := tr.Boots(); len(got) != 1 || got[0].ID != "Q" || tr.Collision() != nil {
				t.Errorf("%s: a refused seed changed the tracker: %+v", name, got)
			}
		}
		tr := lifecycle.NewBootTracker(pol)
		if err := tr.Seed([]lifecycle.Boot{a, b}, hit); err != nil {
			t.Fatalf("a seed a fold gives: %v", err)
		}
		if err := tr.Observe("C", at(time.Hour)); !sameCollision(collisionOf(err), hit) {
			t.Errorf("a seeded collision is not kept: %v", err)
		}
	})
}

func TestSightings(t *testing.T) {
	t.Parallel()
	pol := lifecycle.Policy{BootKey: lifecycle.BootID}
	as := []lifecycle.Assertion{
		obs("node", 1, 0, 0, boot("A")),
		run("node", 2, minute, 3*minute, 0, boot("B")),
		obs("agent", 3, minute, 0, boot("A")), // A sorts before B at one instant
		obs("node", 4, 5*minute, 0, boot("C")),
		obs("node", 5, 5*minute, 0, boot("D")), // overwrites C at its instant: C is never seen
		obs("agent", 6, 6*minute, 0),           // no boot
		obs("agent", 7, 0, 0, boot("A")),       // a repeat of (0, A)
	}
	got, err := lifecycle.Sightings(as, pol)
	want := []lifecycle.Sighting{{at(0), "A"}, {at(minute), "A"}, {at(minute), "B"}, {at(3 * minute), "B"}, {at(5 * minute), "D"}}
	if err != nil || !slices.Equal(got, want) {
		t.Errorf("Sightings = %+v, %v\nwant %+v", got, err, want)
	}
	if got, err := lifecycle.Sightings(as, lifecycle.Policy{}); err != nil || got != nil {
		t.Errorf("Sightings with no boot key = %+v, %v; want none", got, err)
	}
	blank := obs("node", 8, 0, 0, boot(" "))
	if _, err := lifecycle.Sightings(append(as, blank), pol); !errors.Is(err, lifecycle.ErrInvalid) {
		t.Errorf("Sightings of a blank boot = %v, want ErrInvalid", err)
	}
	if _, err := lifecycle.Sightings(as, lifecycle.Policy{BootKey: lifecycle.BootID, Skew: -1}); !errors.Is(err, lifecycle.ErrInvalid) {
		t.Errorf("Sightings under a negative skew = %v, want ErrInvalid", err)
	}
}
