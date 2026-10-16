package interval_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The refusal of a description carries the seed that decided it, so a caller can
// say from when a window does not need that seed. Four stores, each retained at
// 10:00 in the layer of its pod: a seed backdated by a run, a refreshing seed, a
// producer that asserts once, and two producers under a rank.
func TestRetainedErrorCarriesTheSeed(t *testing.T) {
	pod := mkfp(t, catalog.K8sPod, 1)
	rank := map[lifecycle.Producer]int{k8s: 2, k8sCluster: 1}
	cases := []struct {
		name string
		recs []store.Record
		// refused is the window that must be refused, by the producer, with the seed
		// of eventTime and through, at the instant; deadline is the seed's deadline,
		// with ok false for a seed with no TTL.
		from, to    time.Time
		producer    lifecycle.Producer
		eventTime   time.Time
		through     time.Time
		at          time.Time
		deadline    time.Time
		hasDeadline bool
		answerFrom  time.Time // a window from here to answerTo is answered
		answerTo    time.Time
		// next, when set, is the producer whose own seed refuses the window that
		// starts at the deadline: the other producer's seed wins in its turn.
		next         lifecycle.Producer
		message      string // the exact text, when pinned
		describeRank map[lifecycle.Producer]int
	}{
		{
			name: "backdated", recs: []store.Record{run(ent(pod), k8s, hm(9, 0), 15*time.Minute, hm(11, 0))},
			from: hm(10, 30), to: hm(10, 45), producer: k8s, eventTime: hm(9, 0), through: hm(11, 0), at: hm(10, 30),
			deadline: hm(11, 15), hasDeadline: true, answerFrom: hm(11, 15), answerTo: hm(11, 30),
		},
		{
			name: "refreshing", recs: []store.Record{observe(ent(pod), k8s, hm(9, 55), 15*time.Minute)},
			from: hm(10, 5), to: hm(10, 20), producer: k8s, eventTime: hm(9, 55), at: hm(10, 5),
			deadline: hm(10, 10), hasDeadline: true, answerFrom: hm(10, 10), answerTo: hm(10, 20),
		},
		{
			name: "asserted once", recs: []store.Record{observe(ent(pod), k8s, hm(9, 0), 0)},
			from: hm(12, 0), to: hm(13, 0), producer: k8s, eventTime: hm(9, 0), at: hm(12, 0),
			message: "interval: Entities: the description of entity " + pod.String() + " at 2026-01-01T12:00:00Z by k8sobjects/default was asserted before the retention horizon and no longer kept: unsupported operation",
		},
		{
			name: "two producers", recs: []store.Record{
				observe(ent(pod), k8s, hm(9, 50), 15*time.Minute),
				observe(ent(pod), k8sCluster, hm(9, 58), 30*time.Minute),
			},
			describeRank: rank,
			from:         hm(10, 0), to: hm(10, 5), producer: k8s, eventTime: hm(9, 50), at: hm(10, 0),
			deadline: hm(10, 5), hasDeadline: true, next: k8sCluster,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := openStore(t, memstore.Options{Keep: [4]bool{true, true, false, true}})
			newFeed(t, st).write(tc.recs...)
			if err := st.Retain(ctx, hm(10, 0)); err != nil {
				t.Fatal(err)
			}
			r := interval.NewReader(st, interval.Options{Describe: true, Rank: tc.describeRank})
			_, out, err := r.Entities(ctx, []identity.Fingerprint{pod}, win(tc.from, tc.to))
			if out != nil || !errors.Is(err, interval.ErrRetained) || !errors.Is(err, errors.ErrUnsupported) {
				t.Fatalf("Entities = %v, %v; want ErrRetained", out, err)
			}
			var re *interval.RetainedError
			if !errors.As(err, &re) {
				t.Fatalf("%v is no *RetainedError", err)
			}
			if re.Subject != ent(pod) || !re.At.Equal(tc.at) || re.Producer != tc.producer {
				t.Errorf("RetainedError = %v at %v by %s; want %v at %v by %s", re.Subject, re.At, re.Producer, ent(pod), tc.at, tc.producer)
			}
			if !re.Seed.EventTime.Equal(tc.eventTime) || !re.Seed.Through.Equal(tc.through) || re.Seed.Producer != tc.producer || re.Seed.Payload != nil {
				t.Errorf("Seed = event %v through %v by %s with payload %q; want event %v through %v by %s with none",
					re.Seed.EventTime, re.Seed.Through, re.Seed.Producer, re.Seed.Payload, tc.eventTime, tc.through, tc.producer)
			}
			if d, ok := re.Deadline(); ok != tc.hasDeadline || !d.Equal(tc.deadline) {
				t.Errorf("Deadline = %v, %v; want %v, %v", d, ok, tc.deadline, tc.hasDeadline)
			}
			if tc.message != "" && err.Error() != tc.message {
				t.Errorf("error = %q\nwant      %q", err.Error(), tc.message)
			}
			if tc.hasDeadline {
				// A window that starts at the deadline does not need the seed.
				from, to := tc.deadline, tc.deadline.Add(15*time.Minute)
				if !tc.answerFrom.IsZero() {
					from, to = tc.answerFrom, tc.answerTo
				}
				_, _, err := r.Entities(ctx, []identity.Fingerprint{pod}, win(from, to))
				var again *interval.RetainedError
				switch {
				case tc.next == "" && err != nil:
					t.Errorf("Entities from the deadline %v: %v; want an answer", from, err)
				case tc.next != "" && (!errors.As(err, &again) || again.Producer != tc.next):
					t.Errorf("Entities from the deadline %v: %v; want a refusal by %s", from, err, tc.next)
				}
			}
		})
	}
}
