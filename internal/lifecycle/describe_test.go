package lifecycle_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func describeAt(t *testing.T, tl lifecycle.Timeline, x time.Duration) lifecycle.Description {
	t.Helper()
	d, ok := tl.DescribeAt(at(x))
	if !ok {
		t.Fatalf("subject does not exist at %s", x)
	}
	return d
}

func wantAttr(t *testing.T, d lifecycle.Description, key string, value any, from lifecycle.Producer) {
	t.Helper()
	got, ok := d[catalog.AttributeKey(key)]
	if !ok {
		t.Errorf("attribute %q missing from %v", key, d)
		return
	}
	if got.Value != value || got.Producer != from {
		t.Errorf("attribute %q = %v from %q, want %v from %q", key, got.Value, got.Producer, value, from)
	}
}

func TestDescriptionAuthority(t *testing.T) {
	t.Parallel()

	rank := lifecycle.Policy{Rank: map[lifecycle.Producer]int{"full": 10, "subset": 1}}

	t.Run("rank wins over recency", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, rank,
			obs("full", 1, 0, 0, attr("phase", "Running")),
			obs("subset", 2, 5*minute, 0, attr("phase", "Pending")), // newer, lower authority
		)
		wantAttr(t, describeAt(t, tl, 6*minute), "phase", "Running", "full")
	})

	t.Run("attributes merge across producers", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, rank,
			obs("full", 1, 0, 0, attr("phase", "Running"), attr("node", "n1")),
			obs("subset", 2, 0, 0, attr("phase", "Pending"), attr("ip", "10.0.0.7")),
		)
		d := describeAt(t, tl, minute)
		wantAttr(t, d, "phase", "Running", "full")
		wantAttr(t, d, "node", "n1", "full")
		wantAttr(t, d, "ip", "10.0.0.7", "subset")
	})

	t.Run("a listed producer beats an unlisted one", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, rank,
			obs("aaa-unlisted", 1, 0, 0, attr("x", 1)),
			obs("subset", 2, 0, 0, attr("x", 2)),
		)
		wantAttr(t, describeAt(t, tl, minute), "x", 2, "subset")
	})

	t.Run("equal ranks go to the smaller name, never to recency", func(t *testing.T) {
		t.Parallel()
		// Recency would flip the answer at each refresh, every minute, with
		// nothing changing.
		as := []lifecycle.Assertion{obs("a", 1, 0, 0, attr("x", "from-a")), obs("b", 2, 0, 0, attr("x", "from-b"))}
		for i := range 6 {
			as = append(as, obs(lifecycle.Producer([]string{"a", "b"}[i%2]), uint64(10+i), time.Duration(i+1)*minute, 0,
				attr("x", "refreshed")))
		}
		tl := fold(t, lifecycle.Policy{}, as...)
		for i := range 7 {
			d := describeAt(t, tl, time.Duration(i)*minute+1)
			if got := d["x"].Producer; got != "a" {
				t.Errorf("at minute %d the winner is %q, want a on every refresh", i, got)
			}
		}
	})

	t.Run("a silent high-ranked producer stops overriding", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, rank,
			obs("full", 1, 0, 5*minute, attr("phase", "Running")),
			obs("subset", 2, 0, 0, attr("phase", "Unknown")),
		)
		wantAttr(t, describeAt(t, tl, 4*minute), "phase", "Running", "full")
		wantAttr(t, describeAt(t, tl, 5*minute), "phase", "Unknown", "subset") // full expired
	})

	t.Run("a deleted producer stops contributing", func(t *testing.T) {
		t.Parallel()
		tl := fold(t, rank,
			obs("full", 1, 0, 0, attr("phase", "Running")),
			obs("subset", 2, 0, 0, attr("phase", "Unknown")),
			del("full", 3, 3*minute),
		)
		wantAttr(t, describeAt(t, tl, 2*minute), "phase", "Running", "full")
		wantAttr(t, describeAt(t, tl, 3*minute), "phase", "Unknown", "subset")
	})
}

func TestDescriptionIsTheLatestCompleteObservation(t *testing.T) {
	t.Parallel()

	tl := fold(t, lifecycle.Policy{},
		obs("P", 1, 0, 0, attr("a", 1), attr("b", 2)),
		obs("P", 2, 5*minute, 0, attr("a", 3)), // b is gone: an observation is complete
	)
	before := describeAt(t, tl, 4*minute)
	after := describeAt(t, tl, 6*minute)
	wantAttr(t, before, "a", 1, "P")
	wantAttr(t, before, "b", 2, "P")
	wantAttr(t, after, "a", 3, "P")
	if _, ok := after["b"]; ok {
		t.Errorf("attribute b survived an observation that omitted it: %v", after)
	}
}

func TestDescribeAtAgreesWithExistence(t *testing.T) {
	t.Parallel()

	tl := fold(t, lifecycle.Policy{}, obs("P", 1, 0, 5*minute, attr("a", 1)), obs("Q", 2, 10*minute, 0))

	if d, ok := tl.DescribeAt(at(6 * minute)); ok || d != nil {
		t.Errorf("a description for a subject that does not exist: %v", d)
	}
	// An observation with no attributes is a subject with an empty description.
	if d, ok := tl.DescribeAt(at(11 * minute)); !ok || d == nil || len(d) != 0 {
		t.Errorf("DescribeAt = %v, %v; want an empty, non-nil description", d, ok)
	}
}
