package detector

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

var p0 = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)

func pat(m int) time.Time { return p0.Add(time.Duration(m) * time.Minute) }

func (d *Detector) size() int {
	n := 0
	for _, uids := range d.clusters {
		n += len(uids)
	}
	return n
}

func st(cluster, uid string, when time.Time) Sighting {
	return Sighting{Cluster: cluster, UID: uid, At: when, AliveUntil: when.Add(3 * time.Minute)}
}

// After many UIDs lapse, Prune empties the state, and no verdict changes.
func TestPrune(t *testing.T) {
	d, err := New(Config{}, p0)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 500 {
		c := fmt.Sprintf("c%d", i%7)
		d.Observe(st(c, fmt.Sprintf("u%d", i), pat(i%20)))
		d.Observe(st("shared", fmt.Sprintf("u%d", i), pat(i%20))) // a reverse case for each
	}
	d.Observe(Sighting{Cluster: "c0", UID: "gone", Deleted: true, At: pat(3)})
	if d.size() == 0 {
		t.Fatal("nothing observed")
	}
	// Before everything lapsed, a prune keeps what has not ended long enough.
	d.Prune(pat(10))
	if d.size() == 0 {
		t.Fatal("pruned UIDs that had not lapsed")
	}

	// Everything ended by minute 23; DefaultAlive more passes by minute 38.
	now := pat(60)
	probe := func(d *Detector, from time.Time) []string {
		var out []string
		for m := 0; m < 30; m++ {
			at := from.Add(time.Duration(m) * time.Minute)
			for _, c := range []string{"c0", "c3", "shared", "none"} {
				v := d.Check(c, at)
				out = append(out, fmt.Sprint(c, v.Refuse, v.Blind, v.UIDs))
			}
			out = append(out, fmt.Sprint(len(d.Notes(at))))
		}
		return out
	}
	before := probe(d, now)
	d.Prune(now)
	if d.size() != 0 || len(d.clusters) != 0 || len(d.noted) != 0 {
		t.Fatalf("after Prune: %d UIDs, %d clusters, %d noted; want none", d.size(), len(d.clusters), len(d.noted))
	}
	// The same probe on a detector that never pruned must agree.
	ref, _ := New(Config{}, p0)
	for i := range 500 {
		c := fmt.Sprintf("c%d", i%7)
		ref.Observe(st(c, fmt.Sprintf("u%d", i), pat(i%20)))
		ref.Observe(st("shared", fmt.Sprintf("u%d", i), pat(i%20)))
	}
	ref.Observe(Sighting{Cluster: "c0", UID: "gone", Deleted: true, At: pat(3)})
	if want := probe(ref, now); !slices.Equal(before, want) {
		t.Fatal("verdicts differ between a pruned and an unpruned detector")
	}
	if after := probe(d, now); !slices.Equal(after, before) {
		t.Fatal("verdicts after Prune differ from those before")
	}

	// A new conflict after pruning is seen as before.
	d.Observe(st("c0", "n1", pat(70)))
	d.Observe(st("c0", "n2", pat(70)))
	if v := d.Check("c0", pat(71)); !v.Refuse || !slices.Equal(v.UIDs, []string{"n1", "n2"}) {
		t.Fatalf("after Prune: %+v", v)
	}
}

// Prune keeps a UID that ended less than DefaultAlive ago, and one still alive.
func TestPruneMargin(t *testing.T) {
	d, _ := New(Config{DefaultAlive: 10 * time.Minute}, p0)
	d.Observe(Sighting{Cluster: "a", UID: "x", At: pat(0), AliveUntil: pat(5)})
	d.Observe(Sighting{Cluster: "a", UID: "y", At: pat(0), AliveUntil: pat(30)})
	d.Prune(pat(15)) // x ended exactly DefaultAlive ago: kept
	if d.size() != 2 {
		t.Fatalf("size %d, want 2", d.size())
	}
	d.Prune(pat(15).Add(time.Nanosecond))
	if d.size() != 1 {
		t.Fatalf("size %d, want 1", d.size())
	}
	d.Prune(pat(100))
	if d.size() != 0 || len(d.clusters) != 0 {
		t.Fatalf("size %d, %d clusters", d.size(), len(d.clusters))
	}
}

// The package reads no clock and does no I/O.
func TestNoClockNoIO(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	var checked int
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		checked++
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		src := string(b)
		for _, bad := range []string{"time.Now", "time.Since", "time.Until", "time.Sleep", "time.After", "time.Tick", "time.NewTimer", "time.NewTicker", "go func", "\"os\"", "\"net", "\"sync\"", "\"math/rand"} {
			if strings.Contains(stripComments(src), bad) {
				t.Errorf("%s uses %s", f, bad)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no non-test file found")
	}
}

// stripComments removes line comments, so that the documentation can name what
// the package does not do.
func stripComments(src string) string {
	var out []string
	for _, l := range strings.Split(src, "\n") {
		if i := strings.Index(l, "//"); i >= 0 {
			l = l[:i]
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}
