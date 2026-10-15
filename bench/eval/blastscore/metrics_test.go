package blastscore

import (
	"math"
	"testing"

	"github.com/lotannauo/toposhift/internal/identity"
)

func set(fps ...identity.Fingerprint) map[identity.Fingerprint]struct{} {
	m := map[identity.Fingerprint]struct{}{}
	for _, e := range fps {
		m[e] = struct{}{}
	}
	return m
}

func undefined(x float64) bool { return math.IsNaN(x) }

// The expected values are written by hand before the code. Four cases:
//
//	A: list a b c d at hops 1 1 2 3, truth {a, c, x}: n 4, hits 2, truth 3.
//	   P = 1/2, R = 2/3. P@1 = 1, P@3 = 2/3, P@5 = 2/4 (n < k divides by n).
//	   R@1 = 1/3, R@3 = 2/3, R@5 = 2/3.
//	B: list e, truth {e, f}: n 1, hits 1, truth 2. P = 1, R = 1/2.
//	C: an empty list, truth {g}: P undefined, R = 0.
//	D: list h, an empty truth: P = 0, R undefined.
func TestMetricsByHand(t *testing.T) {
	a, b, c, d, x := svc(1), svc(2), svc(3), svc(4), svc(5)
	e, f, g, h := svc(6), svc(7), svc(8), svc(9)
	A := scoreList([]Prediction{{a, 1}, {b, 1}, {c, 2}, {d, 3}}, set(a, c, x), nil, Truth{Entities: []identity.Fingerprint{a, c, x}}, 0)
	B := scoreList([]Prediction{{e, 1}}, set(e, f), nil, Truth{Entities: []identity.Fingerprint{e, f}}, 0)
	C := scoreList(nil, set(g), nil, Truth{Entities: []identity.Fingerprint{g}}, 0)
	D := scoreList([]Prediction{{h, 1}}, set(), nil, Truth{}, 0)

	if A.N != 4 || A.Hits != 2 || A.T != 3 || A.Precision() != 0.5 || !approx(A.Recall(), 2.0/3) {
		t.Errorf("A: n %d hits %d truth %d P %v R %v", A.N, A.Hits, A.T, A.Precision(), A.Recall())
	}
	for i, want := range [][2]float64{{1, 1.0 / 3}, {2.0 / 3, 2.0 / 3}, {0.5, 2.0 / 3}} {
		if got := A.PrecisionAt(i); !approx(got, want[0]) {
			t.Errorf("A P@%d = %v, want %v", Ks[i], got, want[0])
		}
		if got := A.RecallAt(i); !approx(got, want[1]) {
			t.Errorf("A R@%d = %v, want %v", Ks[i], got, want[1])
		}
	}
	if B.Precision() != 1 || B.Recall() != 0.5 {
		t.Errorf("B: P %v R %v", B.Precision(), B.Recall())
	}
	if !undefined(C.Precision()) || !undefined(C.PrecisionAt(0)) || C.Recall() != 0 {
		t.Errorf("C: P %v P@1 %v R %v; want undefined, undefined, 0", C.Precision(), C.PrecisionAt(0), C.Recall())
	}
	if D.Precision() != 0 || !undefined(D.Recall()) {
		t.Errorf("D: P %v R %v; want 0, undefined", D.Precision(), D.Recall())
	}

	// Pooled over all four: n 4+1+0+1 = 6, hits 2+1+0+0 = 3, truth 3+2+1+0 = 6.
	p := pool([]CaseScore{A, B, C, D}, VariantAll)
	if p.N != 6 || p.Hits != 3 || p.T != 6 || p.Cases != 4 || p.Scorable != 3 {
		t.Errorf("pooled n %d hits %d truth %d cases %d scorable %d", p.N, p.Hits, p.T, p.Cases, p.Scorable)
	}
	if p.Precision != 0.5 || p.Recall != 0.5 || p.F1 != 0.5 {
		t.Errorf("micro P %v R %v F1 %v, want 0.5 each", p.Precision, p.Recall, p.F1)
	}
	// Macro precision: A 1/2, B 1, D 0 (C undefined) = 1.5/3. Macro recall: A 2/3, B 1/2, C 0 (D undefined) = (7/6)/3.
	if !approx(float64(p.MacroPrecision), 0.5) || !approx(float64(p.MacroRecall), 7.0/18) {
		t.Errorf("macro P %v R %v, want 0.5 and 7/18", p.MacroPrecision, p.MacroRecall)
	}
	// P@k over A, B, D (C has no list): k=1 {1, 1, 0}; k=3 {2/3, 1, 0}; k=5 {1/2, 1, 0}.
	for i, want := range [][2]float64{{1, 2.0 / 3}, {2.0 / 3, 5.0 / 9}, {0.5, 0.5}} {
		if !approx(float64(p.AtK[i].PrecisionMedian), want[0]) || !approx(float64(p.AtK[i].PrecisionMean), want[1]) {
			t.Errorf("P@%d median %v mean %v, want %v", Ks[i], p.AtK[i].PrecisionMedian, p.AtK[i].PrecisionMean, want)
		}
	}
	// R@1 over A, B, C (D has no truth): {1/3, 1/2, 0}: median 1/3, mean 5/18.
	if !approx(float64(p.AtK[0].RecallMedian), 1.0/3) || !approx(float64(p.AtK[0].RecallMean), 5.0/18) {
		t.Errorf("R@1 median %v mean %v", p.AtK[0].RecallMedian, p.AtK[0].RecallMean)
	}
	// By hop, micro: hop 1 has a, b, e, h (hits a, e); hop 2 has c (hit); hop 3 has d.
	want := []HopPrecision{{1, 4, 2, 0.5}, {2, 1, 1, 1}, {3, 1, 0, 0}}
	for i, w := range want {
		if got := p.ByHop[i]; got.Hop != w.Hop || got.Predicted != w.Predicted || got.Hits != w.Hits || got.Precision != w.Precision {
			t.Errorf("hop %d: %+v, want %+v", w.Hop, got, w)
		}
	}
	if got := p.ByHop[3]; got.Predicted != 0 || !undefined(float64(got.Precision)) {
		t.Errorf("hop 4: %+v, want no entities and an undefined precision", got)
	}
}

// P@k divides by min(k, n), never by k.
func TestPrecisionAtKDividesByTheListWhenItIsShort(t *testing.T) {
	a, b := svc(1), svc(2)
	s := scoreList([]Prediction{{a, 1}, {b, 1}}, set(a, b), nil, Truth{Entities: []identity.Fingerprint{a, b}}, 0)
	if got := s.PrecisionAt(2); got != 1 { // k = 5, n = 2
		t.Errorf("P@5 of two hits in a list of two = %v, want 1", got)
	}
	if got := s.PrecisionAt(1); got != 1 { // k = 3
		t.Errorf("P@3 = %v, want 1", got)
	}
}

// A Deployment and its Pod that map to one service are one truth entity; the
// seed is not truth; what no rule maps or places stays in the truth size.
func TestTruthIsASetWithoutTheSeedAndWithWhatCannotBeMapped(t *testing.T) {
	seed, x, y := svc(1), svc(2), svc(3)
	tr := Truth{Entities: []identity.Fingerprint{x, x, y, seed}, Unmappable: 2, Unplaced: 1}
	set := truthSet(tr, seed)
	if len(set) != 2 {
		t.Fatalf("truth set %d, want 2", len(set))
	}
	s := scoreList([]Prediction{{x, 1}}, set, nil, tr, 0)
	if s.T != 5 || s.Recall() != 0.2 {
		t.Errorf("truth size %d recall %v, want 5 and 0.2", s.T, s.Recall())
	}
	// The seed is never a prediction either.
	s = scoreList([]Prediction{{seed, 1}, {x, 2}, {x, 3}}, set, truthSet(Truth{Entities: []identity.Fingerprint{seed}}, identity.Fingerprint{}), tr, 0)
	if s.N != 1 || s.HopN[2] != 1 {
		t.Errorf("n %d, hop 2 has %d: the seed is dropped and an entity counts once, at its first hop", s.N, s.HopN[2])
	}
}

// F1 is from the micro values, and 0 when both are 0.
func TestF1(t *testing.T) {
	if got := f1(2, 4, 8); !approx(got, 2*0.5*0.25/0.75) {
		t.Errorf("f1 = %v", got)
	}
	if got := f1(0, 4, 8); got != 0 {
		t.Errorf("f1 with no hit = %v, want 0", got)
	}
	if got := f1(0, 0, 8); !undefined(got) {
		t.Errorf("f1 with no list = %v, want undefined", got)
	}
}

func TestMedian(t *testing.T) {
	for _, tc := range []struct {
		in   []float64
		want float64
	}{{[]float64{3, 1, 2}, 2}, {[]float64{4, 1, 2, 3}, 2.5}, {[]float64{7}, 7}} {
		if got := median(tc.in); got != tc.want {
			t.Errorf("median(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
	if !undefined(median(nil)) {
		t.Error("median of nothing is defined")
	}
}
