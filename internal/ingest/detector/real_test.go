package detector_test

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/detector"
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// realSightings translates the entity events of a fixture of the cluster
// translator, cut from the third kwok capture, with a new translator and returns
// the kube-system sightings in the order of the records, converted by the
// package.
func realSightings(t testing.TB, cluster, fixture string) []detector.Sighting {
	t.Helper()
	tr, err := k8scluster.New(k8scluster.Config{
		Producer: lifecycle.Producer("k8scluster/" + cluster),
		Resolver: identity.NewResolver(catalog.Default()),
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(filepath.Join("..", "k8scluster", "testdata", fixture))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []detector.Sighting
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<24)
	for line := 1; sc.Scan(); line++ {
		d, err := capture.DecodeLogs(json.RawMessage(sc.Bytes()))
		if err != nil {
			t.Fatalf("%s:%d: %v", fixture, line, err)
		}
		for _, rl := range d.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					r, err := tr.Translate(rl.Resource, lr, time.Time{})
					if err != nil {
						t.Fatalf("%s:%d: %v", fixture, line, err)
					}
					for _, n := range r.Namespaces {
						out = append(out, detector.FromK8sCluster(cluster, n))
					}
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestFromK8sCluster(t *testing.T) {
	when := time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)
	got := detector.FromK8sCluster("prod", k8scluster.NamespaceSighting{UID: "u", At: when, Interval: time.Minute})
	want := detector.Sighting{Cluster: "prod", UID: "u", At: when, AliveUntil: when.Add(3 * time.Minute)}
	if got != want {
		t.Errorf("state: %+v, want %+v", got, want)
	}
	got = detector.FromK8sCluster("prod", k8scluster.NamespaceSighting{UID: "u", Deleted: true, At: when})
	want = detector.Sighting{Cluster: "prod", UID: "u", Deleted: true, At: when, AliveUntil: when}
	if got != want {
		t.Errorf("delete: %+v, want %+v", got, want)
	}
	got = detector.FromK8sCluster("prod", k8scluster.NamespaceSighting{UID: "u", At: when})
	if !got.AliveUntil.IsZero() {
		t.Errorf("no interval: %+v, want no end", got)
	}
}

// The namespace sightings of the third kwok capture, under one cluster name,
// are one kube-system UID and refuse nothing; under two names they are one
// reverse case, noted and not refused.
func TestRealCapture(t *testing.T) {
	var all []detector.Sighting
	runs := [][]detector.Sighting{realSightings(t, "x", "run1.jsonl"), realSightings(t, "x", "run2.jsonl")}
	for i, r := range runs {
		if len(r) == 0 {
			t.Fatalf("run%d gave no sighting", i+1)
		}
		all = append(all, r...)
	}
	uid := all[0].UID
	for _, s := range all {
		if s.UID != uid || s.Deleted || s.AliveUntil.IsZero() {
			t.Fatalf("sighting %+v: want one UID, state events with an end", s)
		}
	}
	first, last := all[0].At, all[len(all)-1].AliveUntil
	start := first.Add(-10 * time.Minute)

	rename := func(cluster string, ss []detector.Sighting) []detector.Sighting {
		out := slices.Clone(ss)
		for i := range out {
			out[i].Cluster = cluster
		}
		return out
	}

	t.Run("one name", func(t *testing.T) {
		d := newDetector(t, detector.Config{}, start)
		next := 0
		src := rename("prod", all)
		for now := start; now.Before(last.Add(time.Hour)); now = now.Add(30 * time.Second) {
			for next < len(src) && !src[next].At.After(now) {
				d.Observe(src[next])
				next++
			}
			if v := d.Check("prod", now); v.Refuse {
				t.Fatalf("refused at %s: %+v", now, v)
			}
			if n := d.Notes(now); len(n) != 0 {
				t.Fatalf("noted at %s: %+v", now, n)
			}
		}
		if next != len(src) {
			t.Fatalf("fed %d of %d sightings", next, len(src))
		}
	})

	t.Run("two names", func(t *testing.T) {
		d := newDetector(t, detector.Config{}, start)
		var src []detector.Sighting
		for _, s := range all {
			src = append(src, rename("prod", []detector.Sighting{s})[0], rename("staging", []detector.Sighting{s})[0])
		}
		next, noted := 0, 0
		for now := start; now.Before(last.Add(time.Hour)); now = now.Add(30 * time.Second) {
			for next < len(src) && !src[next].At.After(now) {
				d.Observe(src[next])
				next++
			}
			for _, c := range []string{"prod", "staging"} {
				if v := d.Check(c, now); v.Refuse {
					t.Fatalf("%s refused at %s: %+v", c, now, v)
				}
			}
			for _, n := range d.Notes(now) {
				noted++
				if n.UID != uid || !slices.Equal(n.Clusters, []string{"prod", "staging"}) {
					t.Fatalf("note %+v", n)
				}
			}
		}
		// One note for each stretch in which both names vouch for the UID; the
		// two runs of the capture are separated by a gap.
		if noted != len(runs) {
			t.Fatalf("%d notes, want %d, one for each run", noted, len(runs))
		}
	})

	t.Run("the first run alone", func(t *testing.T) {
		d := newDetector(t, detector.Config{}, start)
		for _, s := range runs[0] {
			d.Observe(rename("prod", []detector.Sighting{s})[0])
			d.Observe(rename("staging", []detector.Sighting{s})[0])
		}
		at0 := runs[0][0].At
		if n := d.Notes(at0); len(n) != 1 || n[0].UID != uid {
			t.Fatalf("Notes = %+v, want one note", n)
		}
		if n := d.Notes(at0.Add(time.Minute)); len(n) != 0 {
			t.Fatalf("repeated: %+v", n)
		}
	})

	t.Run("a rebuilt cluster", func(t *testing.T) {
		// The real UID, and a second one that appears a minute after it: the same
		// cluster name is now two clusters until the first lapses.
		d := newDetector(t, detector.Config{}, start)
		s := rename("prod", runs[0])[0]
		d.Observe(s)
		other := s
		other.UID = "rebuilt-" + s.UID
		other.At = s.At.Add(time.Minute)
		other.AliveUntil = other.At.Add(3 * time.Minute)
		d.Observe(other)
		v := d.Check("prod", other.At)
		if !v.Refuse || !slices.Equal(v.UIDs, []string{s.UID, other.UID}) {
			t.Fatalf("Check = %+v, want a refusal with both UIDs", v)
		}
		if v := d.Check("prod", s.AliveUntil); v.Refuse {
			t.Fatalf("still refused when the real UID lapsed: %+v", v)
		}
	})
}
