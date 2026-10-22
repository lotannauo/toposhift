package main

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

func fingerprint(n byte) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0] = n
	fp, err := identity.FingerprintFromHash(catalog.Service, h)
	if err != nil {
		panic(err)
	}
	return fp
}

var (
	epoch    = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	edgeSub  = store.EdgeSubject(fingerprint(1), fingerprint(2), catalog.DependsOn)
	producer = lifecycle.Producer("svcgraph/x")
)

func minutes(n float64) time.Duration { return time.Duration(n * float64(time.Minute)) }

// observe judges a member against the truth: the bound must not exceed the true
// silence, which the sightings the token knows of give, and must fall short of it by
// at most X. Each fault is counted, and nothing else is.
func TestObserveJudgesAMemberAgainstTheTruth(t *testing.T) {
	const expiry = 10 * time.Minute // X = 5 minutes, half-life 75 seconds
	pol := decay.DefaultPolicy()
	h := pol.HalfLife(expiry)
	// Instants are in minutes from the epoch.
	ev := func(last float64, ttl time.Duration) []interval.Evidence {
		at := epoch.Add(minutes(last))
		return []interval.Evidence{{Producer: producer, EventTime: at, TTL: ttl, Latest: at}}
	}
	type sighting struct {
		at    float64
		stamp uint64
	}
	cases := []struct {
		name      string
		evidence  []interval.Evidence
		sightings []sighting
		at        float64
		token     uint64
		want      edgeRow
		bin       int
	}{
		{
			name: "a bound that is the true silence less X is sound and tight",
			// Last record at 0 (last instant 0), last sighting at 1: at 9 the true
			// silence is 8 and the bound 9-0-5 = 4, short by 4.
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {1, 2}}, at: 9, token: 5,
			want: edgeRow{Members: 1, Shown: 1, TrueOverX: 1, MaxBound: minutes(4), MaxTrue: minutes(8), MaxShort: minutes(4)}, bin: 4,
		},
		{
			name:     "no silence at all is not shown",
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {4, 2}}, at: 4, token: 5,
			want: edgeRow{Members: 1, MaxTrue: 0, MaxShort: 0}, bin: 0,
		},
		{
			name: "a silence below X cannot show",
			// The record's last instant is 0, the last sighting 2, at 4: bound 0, true 2.
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {2, 2}}, at: 4, token: 5,
			want: edgeRow{Members: 1, MaxTrue: minutes(2), MaxShort: minutes(2)}, bin: 0,
		},
		{
			name: "a bound above the true silence is unsound",
			// The producer was seen at 8, but the record's last instant is 0: the
			// bound at 9 is 4 and the true silence 1.
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {8, 2}}, at: 9, token: 5,
			want: edgeRow{Members: 1, Shown: 1, Unsound: 1, MaxBound: minutes(4), MaxTrue: minutes(1)}, bin: 4,
		},
		{
			name: "a bound short of the true silence by more than X is loose",
			// The record's last instant is 4, the only sighting at 0: bound at 9 is 0,
			// the true silence 9, short by 9.
			evidence: ev(4, expiry), sightings: []sighting{{0, 1}}, at: 9, token: 5,
			want: edgeRow{Members: 1, TrueOverX: 1, Loose: 1, MaxTrue: minutes(9), MaxShort: minutes(9)}, bin: 0,
		},
		{
			name: "a sighting the token does not know of is not the truth",
			// The sighting at 8 arrived after the token, so the truth is the one at 0:
			// the bound 4 is then sound and tight.
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {8, 9}}, at: 9, token: 5,
			want: edgeRow{Members: 1, Shown: 1, TrueOverX: 1, MaxBound: minutes(4), MaxTrue: minutes(9), MaxShort: minutes(5)}, bin: 4,
		},
		{
			name: "a sighting after the instant is not the truth",
			// The sighting at 8 is after the instant 6: the true silence is from 0.
			evidence: ev(0, expiry), sightings: []sighting{{0, 1}, {8, 2}}, at: 6, token: 5,
			want: edgeRow{Members: 1, Shown: 1, TrueOverX: 1, MaxBound: minutes(1), MaxTrue: minutes(6), MaxShort: minutes(5)}, bin: 1,
		},
		{
			name:     "a producer with no sighting known at the token has no truth",
			evidence: ev(0, expiry), sightings: []sighting{{0, 9}}, at: 9, token: 5,
			want: edgeRow{Members: 1, NoTruth: 1},
		},
		{
			name:     "evidence with another TTL than the expiry is counted",
			evidence: ev(0, 5*time.Minute), sightings: []sighting{{0, 1}}, at: 1, token: 5,
			want: edgeRow{Members: 1, OtherTTL: 1, MaxTrue: minutes(1), MaxShort: minutes(1)}, bin: 0,
		},
		{
			name:     "no TTL means no recency",
			evidence: ev(0, 0), sightings: []sighting{{0, 1}}, at: 1, token: 5,
			want: edgeRow{Members: 1, OtherTTL: 1, NoRecency: 1},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := &pipeline{expiry: expiry, truth: newTruth()}
			for _, s := range c.sightings {
				p.truth.add(tkey{sub: edgeSub, prod: producer}, epoch.Add(minutes(s.at)), s.stamp)
			}
			var row edgeRow
			var out tokenResult
			p.observe(&row, &out, interval.Element{Subject: edgeSub, Last: c.evidence}, epoch.Add(minutes(c.at)), c.token, pol, h)
			if row != c.want {
				t.Errorf("row %+v, want %+v", row, c.want)
			}
			var bins [6]int
			if row.NoRecency == 0 && row.NoTruth == 0 {
				bins[c.bin] = 1
			}
			if out.SilenceBins != bins {
				t.Errorf("bins %v, want %v", out.SilenceBins, bins)
			}
		})
	}
}

// The bins are the bound in half-lives.
func TestSilenceBins(t *testing.T) {
	h := 75 * time.Second
	for _, c := range []struct {
		bound time.Duration
		want  int
	}{
		{0, 0}, {1, 1}, {h - 1, 1}, {h, 2}, {2*h - 1, 2}, {2 * h, 3}, {3 * h, 4}, {4*h - 1, 4}, {4 * h, 5}, {100 * h, 5},
	} {
		if got := silenceBin(c.bound, h); got != c.want {
			t.Errorf("silenceBin(%s) = %d, want %d", c.bound, got, c.want)
		}
	}
	if silenceBin(time.Second, 0) != 5 {
		t.Errorf("with no half-life every positive bound is in the last bin")
	}
}

// Every fault is a failure of the run, and a row's faults are the sum of its kinds.
func TestFaultsAreFailures(t *testing.T) {
	if (edgeRow{Members: 9, Shown: 9, TrueOverX: 9}).faults() != 0 {
		t.Errorf("a row with no fault has faults")
	}
	r := edgeRow{Unsound: 1, Loose: 2, NoRecency: 4, NoTruth: 8, OtherTTL: 16}
	if r.faults() != 31 {
		t.Errorf("faults() = %d, want 31", r.faults())
	}
	var total edgeRow
	total.add(r)
	total.add(edgeRow{Unsound: 1, MaxBound: minutes(3), MaxShort: minutes(2)})
	if total.Unsound != 2 || total.faults() != 32 || total.MaxBound != minutes(3) || total.MaxShort != minutes(2) {
		t.Errorf("total %+v", total)
	}
}
