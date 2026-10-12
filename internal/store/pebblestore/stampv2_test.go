package pebblestore

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// stampV2File is testdata/stamp_v2.json: see its "about".
type stampV2File struct {
	Vectors []struct {
		Name    string
		Horizon time.Time
		Through uint64
		W       uint64
		Entries []struct {
			Ref, Value string
			EventNs    int64
		}
		Boots []struct {
			ID          string
			First, Last int64
		}
		Carried []struct {
			At int64
			ID string
		}
		Encoded   string
		Collision *struct {
			Stale, Newer         string
			Observed, NewerFirst int64
		}
	}
	Refused []struct{ Name, Encoded string }
}

// The value of a baseline with a boot state is a stored format: if one of these
// changes, the format changed, and that needs a new format byte and a migration,
// never a regenerated expectation. A reader takes formats 1 and 2 and refuses any
// other leading byte, and refuses a format 2 value no retention writes.
func TestStampFormatTwoGoldenVectors(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("testdata/stamp_v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var f stampV2File
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Vectors) == 0 || len(f.Refused) == 0 {
		t.Fatal("no vectors")
	}
	formats := map[byte]bool{}
	for _, v := range f.Vectors {
		s := stamp{kind: kindBaseline, foldVersion: foldVersion, through: v.Through, w: v.W, horizon: v.Horizon.UTC()}
		for _, e := range v.Entries {
			ref, rerr := hex.DecodeString(e.Ref)
			val, verr := hex.DecodeString(e.Value)
			if err := errors.Join(rerr, verr); err != nil {
				t.Fatal(err)
			}
			pv, err := pebblekv.DecodeValue(val)
			if err != nil {
				t.Fatal(err)
			}
			s.entries = append(s.entries, entry{ref: ref, eventNs: e.EventNs, value: pv})
		}
		if len(v.Boots) > 0 {
			s.boots = &bootState{}
			for _, b := range v.Boots {
				s.boots.boots = append(s.boots.boots, lifecycle.Boot{ID: b.ID, FirstSeen: pebblekv.Time(b.First), LastSeen: pebblekv.Time(b.Last)})
			}
			if c := v.Collision; c != nil {
				s.boots.collision = &lifecycle.CloneCollisionError{StaleBoot: c.Stale, ObservedAt: pebblekv.Time(c.Observed), NewerBoot: c.Newer, NewerFirstSeen: pebblekv.Time(c.NewerFirst)}
			}
			for _, c := range v.Carried {
				s.boots.carried = append(s.boots.carried, lifecycle.Sighting{At: pebblekv.Time(c.At), ID: c.ID})
			}
		}
		got, err := appendStamp(nil, s)
		if err != nil || hex.EncodeToString(got) != v.Encoded {
			t.Errorf("%s: encoded %x, %v\nwant %s", v.Name, got, err, v.Encoded)
			continue
		}
		formats[got[0]] = true
		back, err := decodeStamp(got)
		if err != nil {
			t.Errorf("%s: %v", v.Name, err)
			continue
		}
		// What decodes encodes again to the same bytes: the carried sightings are
		// canonical once written.
		again, err := appendStamp(nil, back)
		if err != nil || !slices.Equal(again, got) {
			t.Errorf("%s: decoded and encoded again: %x, %v", v.Name, again, err)
		}
		if (back.boots == nil) != (s.boots == nil) {
			t.Errorf("%s: decoded a boot state %+v", v.Name, back.boots)
			continue
		}
		if b := back.boots; b != nil {
			if !slices.Equal(b.boots, s.boots.boots) || (b.collision == nil) != (s.boots.collision == nil) ||
				(b.collision != nil && *b.collision != *s.boots.collision) || !slices.IsSortedFunc(b.carried, lifecycle.CompareSightings) {
				t.Errorf("%s: decoded %+v", v.Name, b)
			}
		}
	}
	if !formats[stampFormat] || !formats[stampFormatBoots] {
		t.Errorf("the vectors cover the leading bytes %v; want 1 and 2", formats)
	}
	for _, r := range f.Refused {
		b, err := hex.DecodeString(r.Encoded)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := decodeStamp(b); !errors.Is(err, pebblekv.ErrValue) {
			t.Errorf("%s: decodeStamp = %v, want an error wrapping ErrValue", r.Name, err)
		}
	}
}

// A boot state is written only on a baseline, and only with a boot.
func TestABootStateIsWrittenOnlyOnABaselineWithABoot(t *testing.T) {
	t.Parallel()
	h := time.Unix(1_700_000_000, 0).UTC()
	one := []lifecycle.Boot{{ID: "boot-a", FirstSeen: h.Add(-time.Second), LastSeen: h.Add(-time.Second)}}
	for name, s := range map[string]stamp{
		"a checkpoint":        {kind: kindCheckpoint, foldVersion: foldVersion, boots: &bootState{boots: one}},
		"a baseline, no boot": {kind: kindBaseline, foldVersion: foldVersion, horizon: h, boots: &bootState{}},
	} {
		if b, err := appendStamp(nil, s); err == nil {
			t.Errorf("%s: encoded %x", name, b)
		}
	}
}
