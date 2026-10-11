package detector_test

import (
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/ingest/detector"
)

// t0 is the start of the detector in most tests; at(m) is m minutes after it.
var t0 = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)

func at(m float64) time.Time { return t0.Add(time.Duration(m * float64(time.Minute))) }

// fataler is what newDetector needs of a *testing.T and of a *rapid.T.
type fataler interface {
	Helper()
	Fatal(args ...any)
}

func newDetector(t fataler, c detector.Config, start time.Time) *detector.Detector {
	t.Helper()
	d, err := detector.New(c, start)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// state is a state sighting that vouches for three intervals, as the cluster
// receiver's translator reports one.
func state(cluster, uid string, when time.Time, interval time.Duration) detector.Sighting {
	return detector.Sighting{Cluster: cluster, UID: uid, At: when, AliveUntil: when.Add(3 * interval)}
}

func del(cluster, uid string, when time.Time) detector.Sighting {
	return detector.Sighting{Cluster: cluster, UID: uid, Deleted: true, At: when, AliveUntil: when}
}

func wantRefused(t *testing.T, d *detector.Detector, cluster string, now time.Time, uids ...string) {
	t.Helper()
	v := d.Check(cluster, now)
	if !v.Refuse || v.Blind || !slices.Equal(v.UIDs, uids) {
		t.Fatalf("Check(%s, %s) = %+v, want a refusal with UIDs %v", cluster, now.Sub(t0), v, uids)
	}
	q := make([]string, len(uids))
	for i, u := range uids {
		q[i] = strconv.Quote(u)
	}
	want := "two clusters under cluster name " + strconv.Quote(cluster) + ": set k8s.cluster.name in each collector (kube-system UIDs " + strings.Join(q, ", ") + ")"
	if v.Message != want {
		t.Fatalf("Message = %q, want %q", v.Message, want)
	}
}

func wantAccepted(t *testing.T, d *detector.Detector, cluster string, now time.Time) {
	t.Helper()
	v := d.Check(cluster, now)
	if v.Refuse || v.Blind || len(v.UIDs) != 0 || v.Message != "" {
		t.Fatalf("Check(%s, %s) = %+v, want accepted", cluster, now.Sub(t0), v)
	}
}

func TestNewValidates(t *testing.T) {
	for name, c := range map[string]detector.Config{
		"negative warmup":     {Warmup: -time.Second},
		"negative alive":      {DefaultAlive: -time.Second},
		"warmup over a year":  {Warmup: 400 * 24 * time.Hour},
		"alive over a year":   {DefaultAlive: 400 * 24 * time.Hour},
		"both out of range":   {Warmup: -1, DefaultAlive: -1},
		"huge alive duration": {DefaultAlive: time.Duration(1<<63 - 1)},
	} {
		if _, err := detector.New(c, t0); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
	if _, err := detector.New(detector.Config{}, time.Time{}); err == nil {
		t.Error("zero start: no error")
	}
	if _, err := detector.New(detector.Config{}, t0); err != nil {
		t.Errorf("zero config: %v", err)
	}
}

// Two real clusters under one name are refused for as long as both report,
// and the refusal ends at the last report of the one that stops, plus three
// intervals.
func TestTwoRealClusters(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	const lastB = 20 // minutes
	for m := 0; m <= 30; m++ {
		d.Observe(state("prod", "A", at(float64(m)), time.Minute))
		if m <= lastB {
			d.Observe(state("prod", "B", at(float64(m)), time.Minute))
		}
		now := at(float64(m))
		switch {
		case m < 5:
			if v := d.Check("prod", now); !v.Blind || v.Refuse {
				t.Fatalf("minute %d: %+v, want blind", m, v)
			}
		case m < lastB+3:
			wantRefused(t, d, "prod", now, "A", "B")
		default:
			wantAccepted(t, d, "prod", now)
		}
	}
	// To the nanosecond: B's last report was at minute 20, vouching to 23.
	d2 := newDetector(t, detector.Config{}, t0)
	d2.Observe(state("prod", "A", at(22), time.Minute))
	d2.Observe(state("prod", "B", at(20), time.Minute))
	wantRefused(t, d2, "prod", at(23).Add(-time.Nanosecond), "A", "B")
	wantAccepted(t, d2, "prod", at(23))
}

// A cluster rebuilt under the same name is refused while its old UID still
// vouches, and accepted when that lapses or is deleted.
func TestClusterRebuild(t *testing.T) {
	start := at(-10)
	const interval = 5 * time.Minute
	T0 := t0
	build := func() *detector.Detector {
		d := newDetector(t, detector.Config{}, start)
		d.Observe(state("prod", "A", T0, interval))                  // vouches until T0+15m
		d.Observe(state("prod", "B", T0.Add(time.Minute), interval)) // the rebuilt cluster
		return d
	}

	d := newDetector(t, detector.Config{}, start)
	d.Observe(state("prod", "A", T0, interval))
	wantAccepted(t, d, "prod", T0.Add(time.Minute))
	d.Observe(state("prod", "B", T0.Add(time.Minute), interval))
	for m := 1; m < 15; m++ {
		wantRefused(t, d, "prod", T0.Add(time.Duration(m)*time.Minute), "A", "B")
	}
	wantRefused(t, d, "prod", T0.Add(15*time.Minute-time.Nanosecond), "A", "B")
	wantAccepted(t, d, "prod", T0.Add(15*time.Minute))
	wantAccepted(t, d, "prod", T0.Add(20*time.Minute))

	// A's delete arrives at T0+2m: the refusal ends there.
	d = build()
	d.Observe(del("prod", "A", T0.Add(2*time.Minute)))
	wantRefused(t, d, "prod", T0.Add(time.Minute), "A", "B")
	wantRefused(t, d, "prod", T0.Add(2*time.Minute-time.Nanosecond), "A", "B")
	wantAccepted(t, d, "prod", T0.Add(2*time.Minute))
	wantAccepted(t, d, "prod", T0.Add(10*time.Minute))
}

// A conflict inside the warmup is not refused or noted; the same state
// just after it is.
func TestBlindWindow(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "A", at(3), time.Minute)) // vouches until minute 6
	d.Observe(state("prod", "B", at(3), time.Minute))
	d.Observe(state("a", "C", at(3), time.Minute)) // a reverse case, too
	d.Observe(state("b", "C", at(3), time.Minute))

	for _, m := range []float64{0, 3, 4, 4.999} {
		v := d.Check("prod", at(m))
		if !v.Blind || v.Refuse || len(v.UIDs) != 0 || v.Message != "" {
			t.Errorf("Check at minute %v = %+v, want blind and nothing else", m, v)
		}
		if n := d.Notes(at(m)); len(n) != 0 {
			t.Errorf("Notes at minute %v = %+v, want none", m, n)
		}
	}
	// The blind window ends at start plus Warmup, inclusive; the same state is
	// refused then, and the held-back note comes out.
	wantRefused(t, d, "prod", at(5), "A", "B")
	if n := d.Notes(at(5)); len(n) != 1 || n[0].UID != "C" {
		t.Errorf("Notes at minute 5 = %+v, want the note of C", n)
	}

	// The state reported just after the warmup is refused at once.
	d = newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "A", at(5), time.Minute))
	d.Observe(state("prod", "B", at(5), time.Minute))
	wantRefused(t, d, "prod", at(5), "A", "B")

	// A configured warmup.
	d = newDetector(t, detector.Config{Warmup: time.Minute}, t0)
	d.Observe(state("prod", "A", at(0), 10*time.Minute))
	d.Observe(state("prod", "B", at(0), 10*time.Minute))
	if v := d.Check("prod", at(0.5)); !v.Blind || v.Refuse {
		t.Errorf("inside a 1m warmup: %+v", v)
	}
	wantRefused(t, d, "prod", at(1), "A", "B")
}

// One UID under two cluster names is noted once and refuses neither name.
func TestReverseCase(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "K", at(6), time.Minute))
	d.Observe(state("default", "K", at(6), time.Minute)) // alive until minute 9
	wantAccepted(t, d, "prod", at(6))
	wantAccepted(t, d, "default", at(6))

	n := d.Notes(at(6))
	if len(n) != 1 || n[0].UID != "K" || !slices.Equal(n[0].Clusters, []string{"default", "prod"}) {
		t.Fatalf("Notes = %+v, want one note of K under default and prod", n)
	}
	if want := `kube-system UID "K" is alive under cluster names "default", "prod": one cluster is ingested twice`; n[0].Message != want {
		t.Errorf("Message = %q", n[0].Message)
	}
	// While it lasts it is not repeated, whatever refreshes it.
	d.Observe(state("prod", "K", at(7), time.Minute))
	d.Observe(state("default", "K", at(7), time.Minute))
	for _, m := range []float64{6, 7, 8, 9} {
		if n := d.Notes(at(m)); len(n) != 0 {
			t.Fatalf("Notes at minute %v = %+v, want none", m, n)
		}
		wantAccepted(t, d, "prod", at(m))
		wantAccepted(t, d, "default", at(m))
	}
	// It ends (both lapse at minute 10); a call that sees the end clears it.
	if n := d.Notes(at(11)); len(n) != 0 {
		t.Fatalf("Notes after it ended = %+v", n)
	}
	// And is reported again when it begins anew.
	d.Observe(state("prod", "K", at(12), time.Minute))
	d.Observe(state("default", "K", at(12), time.Minute))
	if n := d.Notes(at(12)); len(n) != 1 || n[0].UID != "K" {
		t.Fatalf("Notes on a new beginning = %+v, want one", n)
	}
	if n := d.Notes(at(12)); len(n) != 0 {
		t.Fatalf("repeated: %+v", n)
	}

	// It ends when only one name still vouches, too, and is then not a case.
	d.Observe(del("default", "K", at(13)))
	if n := d.Notes(at(13)); len(n) != 0 {
		t.Fatalf("Notes with one name left = %+v", n)
	}
	// Three names are one note listing all three, and a second UID is a second
	// note, in the order of the UIDs.
	d = newDetector(t, detector.Config{}, t0)
	for _, c := range []string{"c", "a", "b"} {
		d.Observe(state(c, "Z", at(6), time.Minute))
	}
	d.Observe(state("a", "Y", at(6), time.Minute))
	d.Observe(state("b", "Y", at(6), time.Minute))
	n = d.Notes(at(6))
	if len(n) != 2 || n[0].UID != "Y" || n[1].UID != "Z" || !slices.Equal(n[1].Clusters, []string{"a", "b", "c"}) {
		t.Fatalf("Notes = %+v, want Y under a, b then Z under a, b, c", n)
	}
}

// Sightings keep updating the state while a name is refused, and the refusal
// ends exactly when one UID's newest sighting lapses.
func TestReadsWhileRefusing(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "A", at(5), time.Minute))
	d.Observe(state("prod", "B", at(5), time.Minute)) // both vouch until minute 8
	wantRefused(t, d, "prod", at(6), "A", "B")

	// Refreshing B while refused extends the refusal to B's new end.
	d.Observe(state("prod", "B", at(7), time.Minute)) // until minute 10
	d.Observe(state("prod", "A", at(7), time.Minute))
	wantRefused(t, d, "prod", at(9.5), "A", "B")

	// A third UID joins during the refusal.
	d.Observe(state("prod", "C", at(8), time.Minute)) // until minute 11
	wantRefused(t, d, "prod", at(9.5), "A", "B", "C")

	// A is refreshed just before minute 11 and vouches to minute 14; B lapses at
	// 10 and C at 11. The refusal ends exactly when C lapses.
	d.Observe(state("prod", "A", at(11).Add(-time.Nanosecond), time.Minute))
	wantRefused(t, d, "prod", at(11).Add(-time.Nanosecond), "A", "C")
	wantAccepted(t, d, "prod", at(11))

	// A sighting after the end starts a new conflict.
	d.Observe(state("prod", "B", at(12), time.Minute))
	wantRefused(t, d, "prod", at(12), "A", "B")
}

// The newest At decides, not the last written.
func TestOutOfOrder(t *testing.T) {
	// A state older than a delete does not revive the UID.
	d := newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "B", at(6), time.Minute))
	d.Observe(del("prod", "A", at(7)))
	d.Observe(state("prod", "A", at(6.5), time.Minute)) // older, arrives later
	wantAccepted(t, d, "prod", at(7))
	wantAccepted(t, d, "prod", at(7.5))

	// A delete older than a later state does not end it.
	d = newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "B", at(6), time.Minute))
	d.Observe(state("prod", "A", at(8), time.Minute)) // until minute 11
	d.Observe(del("prod", "A", at(7)))                // older, arrives later
	wantRefused(t, d, "prod", at(8), "A", "B")
	wantRefused(t, d, "prod", at(8.5), "A", "B")

	// The same older state does not shorten the end of a newer one.
	d = newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "A", at(8), time.Minute)) // until minute 11
	d.Observe(state("prod", "B", at(8), time.Minute))
	d.Observe(state("prod", "A", at(6), time.Minute)) // older, until minute 9
	wantRefused(t, d, "prod", at(10), "A", "B")

	// At equal instants the delete wins, in either order.
	for _, deleteFirst := range []bool{true, false} {
		d = newDetector(t, detector.Config{}, t0)
		d.Observe(state("prod", "B", at(6), time.Minute))
		s, x := state("prod", "A", at(7), time.Minute), del("prod", "A", at(7))
		if deleteFirst {
			d.Observe(x)
			d.Observe(s)
		} else {
			d.Observe(s)
			d.Observe(x)
		}
		wantAccepted(t, d, "prod", at(7))
		wantAccepted(t, d, "prod", at(7.5))
	}
}

// The outcome does not depend on the order sightings arrive in, even at equal
// instants.
func TestArrivalOrderDoesNotMatter(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		clusters := []string{"a", "b"}
		uids := []string{"x", "y", "z"}
		sight := rapid.Custom(func(t *rapid.T) detector.Sighting {
			s := detector.Sighting{
				Cluster: rapid.SampledFrom(clusters).Draw(t, "cluster"),
				UID:     rapid.SampledFrom(uids).Draw(t, "uid"),
				Deleted: rapid.Bool().Draw(t, "deleted"),
				At:      at(float64(rapid.IntRange(0, 12).Draw(t, "at"))),
			}
			switch rapid.IntRange(0, 2).Draw(t, "end") {
			case 0:
			case 1:
				s.AliveUntil = s.At.Add(time.Duration(rapid.IntRange(1, 6).Draw(t, "len")) * time.Minute)
			case 2:
				s.AliveUntil = s.At
			}
			if s.Deleted {
				s.AliveUntil = s.At
			}
			return s
		})
		in := rapid.SliceOfN(sight, 0, 14).Draw(t, "sightings")
		perm := rapid.Permutation(in).Draw(t, "permutation")

		a := newDetector(t, detector.Config{}, t0)
		b := newDetector(t, detector.Config{}, t0)
		for _, s := range in {
			a.Observe(s)
		}
		for _, s := range perm {
			b.Observe(s)
		}
		for m := 5.0; m < 40; m += 0.5 {
			for _, c := range clusters {
				if va, vb := a.Check(c, at(m)), b.Check(c, at(m)); !sameVerdict(va, vb) {
					t.Fatalf("Check(%s, %v): %+v, then %+v after a permutation", c, m, va, vb)
				}
			}
			if na, nb := a.Notes(at(m)), b.Notes(at(m)); len(na) != len(nb) {
				t.Fatalf("Notes at %v: %+v, then %+v", m, na, nb)
			}
		}
	})
}

func sameVerdict(a, b detector.Verdict) bool {
	return a.Refuse == b.Refuse && a.Blind == b.Blind && a.Message == b.Message && slices.Equal(a.UIDs, b.UIDs)
}

// A state sighting with no end lasts DefaultAlive.
func TestNoEndLastsDefaultAlive(t *testing.T) {
	noEnd := func(c, u string, when time.Time) detector.Sighting {
		return detector.Sighting{Cluster: c, UID: u, At: when}
	}
	for _, tc := range []struct {
		name string
		cfg  detector.Config
		span time.Duration
	}{
		{"default", detector.Config{}, 15 * time.Minute},
		{"configured", detector.Config{DefaultAlive: 7 * time.Minute}, 7 * time.Minute},
	} {
		d := newDetector(t, tc.cfg, at(-10))
		d.Observe(noEnd("prod", "A", t0))
		d.Observe(noEnd("prod", "B", t0))
		wantRefused(t, d, "prod", t0, "A", "B")
		wantRefused(t, d, "prod", t0.Add(tc.span-time.Nanosecond), "A", "B")
		if v := d.Check("prod", t0.Add(tc.span)); v.Refuse {
			t.Errorf("%s: still refused at the end: %+v", tc.name, v)
		}
	}
	// A delete with no end is still a delete at its At.
	d := newDetector(t, detector.Config{}, at(-10))
	d.Observe(noEnd("prod", "A", t0))
	d.Observe(noEnd("prod", "B", t0))
	d.Observe(detector.Sighting{Cluster: "prod", UID: "A", Deleted: true, At: at(1)})
	wantRefused(t, d, "prod", at(0.5), "A", "B")
	wantAccepted(t, d, "prod", at(1))
}

// A cluster name never sighted is never refused.
func TestNoSource(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	for _, m := range []float64{5, 6, 100} {
		wantAccepted(t, d, "prod", at(m))
	}
	d.Observe(state("other", "A", at(6), time.Minute))
	d.Observe(state("other", "B", at(6), time.Minute))
	wantRefused(t, d, "other", at(6), "A", "B")
	wantAccepted(t, d, "prod", at(6))
	wantAccepted(t, d, "", at(6))
	if n := d.Notes(at(6)); len(n) != 0 {
		t.Errorf("Notes = %+v", n)
	}
	// A sighting that cannot be attributed is no source.
	d.Observe(state("", "A", at(6), time.Minute))
	d.Observe(state("", "B", at(6), time.Minute))
	d.Observe(state("prod", "", at(6), time.Minute))
	wantAccepted(t, d, "", at(6))
	wantAccepted(t, d, "prod", at(6))
}

// The refusal of one name does not touch another.
func TestNamesAreIndependent(t *testing.T) {
	d := newDetector(t, detector.Config{}, t0)
	d.Observe(state("prod", "A", at(6), time.Minute))
	d.Observe(state("prod", "B", at(6), time.Minute))
	d.Observe(state("staging", "C", at(6), time.Minute))
	d.Observe(state("dev", "D", at(6), time.Minute))
	d.Observe(state("dev", "E", at(6), time.Minute))
	wantRefused(t, d, "prod", at(7), "A", "B")
	wantAccepted(t, d, "staging", at(7))
	wantRefused(t, d, "dev", at(7), "D", "E")
	// Repeated sightings of one UID are one UID.
	d.Observe(state("staging", "C", at(6.5), time.Minute))
	wantAccepted(t, d, "staging", at(7))
}

// The same sightings in the same order give the same verdicts.
func TestDeterministic(t *testing.T) {
	run := func() []string {
		d := newDetector(t, detector.Config{}, t0)
		var out []string
		for m := 0; m < 40; m++ {
			now := at(float64(m))
			d.Observe(state("prod", "A", now, time.Minute))
			if m%7 < 4 {
				d.Observe(state("prod", "B", now, time.Minute))
				d.Observe(state("dev", "B", now, time.Minute))
			}
			if m == 20 {
				d.Observe(del("prod", "A", now))
			}
			v := d.Check("prod", now)
			out = append(out, strings.Join(append([]string{v.Message}, v.UIDs...), "|"))
			for _, n := range d.Notes(now) {
				out = append(out, n.Message)
			}
			if m%10 == 9 {
				d.Prune(now)
			}
		}
		return out
	}
	a, b := run(), run()
	if !slices.Equal(a, b) {
		t.Fatalf("two runs differ:\n%q\n%q", a, b)
	}
	if len(a) < 40 {
		t.Fatalf("only %d lines: the scenario did not run", len(a))
	}
}

// The cluster name and the UIDs come from the collector and end up in a message
// that reaches it: they are quoted and bounded, and the verdict keeps them whole.
func TestMessagesQuoteAndBound(t *testing.T) {
	name := strings.Repeat("n", 200)
	var uids []string
	for _, c := range "abcdef" {
		uids = append(uids, strings.Repeat(string(c), 200))
	}
	d := newDetector(t, detector.Config{}, t0)
	for _, u := range uids {
		d.Observe(state(name, u, at(6), time.Minute))
	}
	v := d.Check(name, at(6))
	if !v.Refuse || !slices.Equal(v.UIDs, uids) {
		t.Fatalf("Check = %+v, want a refusal with the six whole UIDs", v)
	}
	if len(v.Message) >= 512 {
		t.Errorf("message is %d bytes: %q", len(v.Message), v.Message)
	}
	cut := func(s string) string { return strconv.Quote(s[:64]) + "..." }
	for _, want := range []string{
		"cluster name " + cut(name) + ":",
		"(kube-system UIDs " + cut(uids[0]) + ", " + cut(uids[1]) + ", " + cut(uids[2]) + ", " + cut(uids[3]) + ", and 2 more)",
	} {
		if !strings.Contains(v.Message, want) {
			t.Errorf("message %q does not contain %q", v.Message, want)
		}
	}
	if strings.Contains(v.Message, name[:65]) || strings.Contains(v.Message, uids[4]) {
		t.Errorf("message %q shows more than it should", v.Message)
	}

	// Exactly four UIDs and a name of exactly 64 bytes are shown whole.
	d = newDetector(t, detector.Config{}, t0)
	short := strings.Repeat("s", 64)
	for _, u := range []string{"a", "b", "c", "d"} {
		d.Observe(state(short, u, at(6), time.Minute))
	}
	if m := d.Check(short, at(6)).Message; strings.Contains(m, "more") || strings.Contains(m, "...") || !strings.Contains(m, strconv.Quote(short)) {
		t.Errorf("message %q", m)
	}

	// A cut never splits a rune, and quoting escapes quotes and control bytes.
	d = newDetector(t, detector.Config{}, t0)
	multi := strings.Repeat("\u20ac", 30) // 90 bytes of three-byte runes: byte 64 is inside a rune
	d.Observe(state(multi, "a\nb\"c", at(6), time.Minute))
	d.Observe(state(multi, "\xff\xfe", at(6), time.Minute))
	m := d.Check(multi, at(6)).Message
	if want := strconv.Quote(strings.Repeat("\u20ac", 21)) + "..."; !strings.Contains(m, want) {
		t.Errorf("message %q does not contain %q", m, want)
	}
	for _, want := range []string{`"a\nb\"c"`, `"\xff\xfe"`} {
		if !strings.Contains(m, want) {
			t.Errorf("message %q does not contain %q", m, want)
		}
	}
	if strings.ContainsAny(m, "\n") || !utf8.ValidString(m) {
		t.Errorf("message %q has a raw newline or is not valid UTF-8", m)
	}

	// A note is quoted and bounded the same way, and keeps the names whole.
	d = newDetector(t, detector.Config{}, t0)
	var names []string
	for _, c := range "abcdef" {
		names = append(names, strings.Repeat(string(c), 100))
		d.Observe(state(names[len(names)-1], uids[0], at(6), time.Minute))
	}
	n := d.Notes(at(6))
	if len(n) != 1 || !slices.Equal(n[0].Clusters, names) {
		t.Fatalf("Notes = %+v, want one note with the six whole names", n)
	}
	if len(n[0].Message) >= 512 {
		t.Errorf("note is %d bytes", len(n[0].Message))
	}
	for _, want := range []string{"UID " + cut(uids[0]), cut(names[0]) + ", " + cut(names[1]) + ", " + cut(names[2]) + ", " + cut(names[3]) + ", and 2 more:"} {
		if !strings.Contains(n[0].Message, want) {
			t.Errorf("note %q does not contain %q", n[0].Message, want)
		}
	}
}
