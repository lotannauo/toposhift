package interval

import (
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var t0 = time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC)

func sec(n int) time.Time { return t0.Add(time.Duration(n) * time.Second) }

func TestClipRefusesAFiniteIntervalWithNoSource(t *testing.T) {
	from, to := sec(0), sec(30)
	if _, err := clip([]ival{{start: sec(5), end: sec(10), src: lifecycle.EndUnknown}}, from, to); err == nil {
		t.Error("a finite interval ending inside the window with no source was clipped into a piece")
	}
	// One that reaches To is clipped to it, and reports no source by rule.
	ps, err := clip([]ival{{start: sec(5), end: sec(40), src: lifecycle.EndUnknown}}, from, to)
	if err != nil || len(ps) != 1 || !ps[0].End.Equal(to) || ps[0].EndSource != lifecycle.EndUnknown {
		t.Errorf("clip = %v, %v", ps, err)
	}
	ps, err = clip([]ival{{start: sec(-5), end: sec(10), src: lifecycle.EndProducer}, {start: sec(20), open: true}}, from, to)
	if err != nil || len(ps) != 2 || !ps[0].Before || !ps[0].Start.Equal(from) || ps[0].EndSource != lifecycle.EndProducer ||
		ps[1].Before || !ps[1].End.Equal(to) || ps[1].EndSource != lifecycle.EndUnknown {
		t.Errorf("clip = %+v, %v", ps, err)
	}
	// Intervals that end at From or start at To are outside the window.
	ps, err = clip([]ival{{start: sec(-5), end: from, src: lifecycle.EndProducer}, {start: to, open: true}}, from, to)
	if err != nil || len(ps) != 0 {
		t.Errorf("clip = %+v, %v", ps, err)
	}
}

func TestCutLabelsEnds(t *testing.T) {
	edge := []ival{{start: sec(0), end: sec(10), src: lifecycle.EndProducer}, {start: sec(20), open: true}}
	end := []ival{{start: sec(5), end: sec(10), src: lifecycle.EndLivenessExpiry}, {start: sec(15), end: sec(25), src: lifecycle.EndLivenessExpiry}}
	got := cut(edge, end)
	want := []ival{
		{start: sec(5), end: sec(10), src: lifecycle.EndProducer}, // a tie: the edge's own source
		{start: sec(20), end: sec(25), src: lifecycle.EndCascade},
	}
	if len(got) != len(want) {
		t.Fatalf("cut = %+v", got)
	}
	for i := range got {
		if !got[i].start.Equal(want[i].start) || !got[i].end.Equal(want[i].end) || got[i].open || got[i].src != want[i].src {
			t.Errorf("cut[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	both := cut([]ival{{start: sec(0), open: true}}, []ival{{start: sec(3), open: true}})
	if len(both) != 1 || !both[0].open || !both[0].start.Equal(sec(3)) {
		t.Errorf("cut of two open intervals = %+v", both)
	}
}

// TestP11ClassIsTotal checks that every list of pieces has exactly one class,
// the one the table gives for (k, a, b), and the transition count.
func TestP11ClassIsTotal(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		from, to := sec(0), sec(20)
		vs := rapid.SliceOfDistinct(rapid.IntRange(0, 20), func(v int) int { return v }).Draw(t, "bounds")
		slices.Sort(vs)
		var ps []Piece
		for i := 0; i+1 < len(vs); i += 2 {
			ps = append(ps, Piece{Span: Span{Start: sec(vs[i]), End: sec(vs[i+1])}})
		}
		class, n := classify(ps, from, to)
		k := len(ps)
		var a, b bool
		if k > 0 {
			a, b = ps[0].Start.Equal(from), ps[k-1].End.Equal(to)
		}
		matches := 0
		for _, row := range []struct {
			ok    bool
			class Class
		}{
			{k == 0, ClassNone},
			{k >= 2, ClassFlapping},
			{k == 1 && a && b, ClassStable},
			{k == 1 && !a && b, ClassAppeared},
			{k == 1 && a && !b, ClassDisappeared},
			{k == 1 && !a && !b, ClassTransient},
		} {
			if row.ok {
				matches++
				if class != row.class {
					t.Fatalf("pieces %v: class %s, want %s", ps, class, row.class)
				}
			}
		}
		if matches != 1 {
			t.Fatalf("%d rows of the table match %v", matches, ps)
		}
		want := 0
		for _, p := range ps {
			if p.Start.After(from) {
				want++
			}
			if p.End.Before(to) {
				want++
			}
		}
		if n != want {
			t.Fatalf("pieces %v: %d transitions, want %d", ps, n, want)
		}
	})
	names := map[string]bool{}
	for c := ClassNone; c <= ClassFlapping; c++ {
		names[c.String()] = true
	}
	if len(names) != 6 || Class(9).String() != "Class(9)" {
		t.Errorf("class names %v", names)
	}
}
