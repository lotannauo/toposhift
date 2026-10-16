package storetest_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// The digest of each of Workloads' streams, by goldenLine, computed before
// Config.Refresh existed. The zero RefreshConfig must leave every one of them as it
// was: a test that compared a stream with itself after the field was added would
// prove nothing, because the zero field is the absent field.
var workloadDigestsBeforeRefresh = map[string]struct {
	records int
	digest  string
}{
	"tiny":                        {2741, "2b70712db1c92c2f33b4536d0d9dea644c444686af6aae5f594cb9facbb6c924"},
	"late and confirmed":          {2844, "a920eff92778f476ae9d0ab5f465c54df0a3ba1dd5d448daa4574f68b9be6c4a"},
	"watch mode, seq across 2^32": {2336, "196f56c9f4a1f1c07e818d98df6d7b2c374d012a4a3b575a47f9f02214640322"},
	"runs and lateness":           {2876, "a07d3194e1e5463e8c965752e77f3f07cfd103331956046b72b1586411386ac3"},
	"busy, seq near 2^63":         {9283, "5f3f82983db29e884ca11bffd9cadf1f6d0a237e413622c22c7e24853e86f9bd"},
	"reboots and clones":          {2777, "443c5ff99bca211ee88b267b87c924f03c499538f96e6490b4f256b0a424ec63"},
}

func TestTheZeroRefreshKeepsEveryWorkloadsStream(t *testing.T) {
	t.Parallel()
	ws := storetest.Workloads()
	if len(ws) != len(workloadDigestsBeforeRefresh) {
		t.Fatalf("%d workloads, %d pinned digests", len(ws), len(workloadDigestsBeforeRefresh))
	}
	for _, w := range ws {
		want, ok := workloadDigestsBeforeRefresh[w.Name]
		if !ok {
			t.Errorf("workload %q has no pinned digest", w.Name)
			continue
		}
		if w.Config.Refresh != (storetest.RefreshConfig{}) {
			t.Errorf("workload %q sets Refresh", w.Name)
		}
		recs := allRecords(t, w.Config)
		if got := digest(recs); len(recs) != want.records || got != want.digest {
			t.Errorf("workload %q: %d records with digest %s, want %d with %s", w.Name, len(recs), got, want.records, want.digest)
		}
	}
}

func TestRefreshConfigValidate(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		mod func(*storetest.Config)
		ok  bool
	}{
		"on": {func(c *storetest.Config) { c.Refresh.On = true }, true},
		"on with runs and a run age": {func(c *storetest.Config) {
			c.Refresh = storetest.RefreshConfig{On: true, RunMaxAge: 90 * time.Second}
			c.Runs = true
		}, true},
		"a run age without refresh":   {func(c *storetest.Config) { c.Refresh.RunMaxAge = time.Minute; c.Runs = true }, true},
		"on in watch mode":            {func(c *storetest.Config) { c.Refresh.On = true; c.HeartbeatInterval, c.TTLFactor = 0, 0 }, false},
		"a run age without runs":      {func(c *storetest.Config) { c.Refresh.RunMaxAge = time.Minute }, true},
		"a negative run age":          {func(c *storetest.Config) { c.Refresh.RunMaxAge = -time.Second; c.Runs = true }, false},
		"a run age off a second":      {func(c *storetest.Config) { c.Refresh.RunMaxAge = 1500 * time.Millisecond; c.Runs = true }, false},
		"on, everything else as well": {func(c *storetest.Config) { *c = everything(); c.Refresh.On = true }, true},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := storetest.Tiny()
			tt.mod(&c)
			err := c.Validate()
			if tt.ok {
				if err != nil {
					t.Fatalf("Validate = %v, want a valid config", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "Refresh") {
				t.Fatalf("Validate = %v, want an error naming Refresh", err)
			}
		})
	}
}

// key is a record without its Seq, and with the TTL of a k8sobjects or fabric
// record dropped, so a stream with Refresh on can be matched against the same
// stream without it.
func refreshKey(r store.Record) string {
	if r.Producer == storetest.ProducerK8sObjects || r.Producer == storetest.ProducerFabric {
		r.TTL = 0
	}
	r.Seq = 0
	return goldenLine(r) + fmt.Sprint(r.EventTimeBasis)
}

// The refreshing mode adds re-assertions and TTLs, and draws nothing: every record
// of the stream without it is in the stream with it (k8sobjects' and fabric's
// TTLs aside), and every other record is a k8sobjects or fabric Observe at a
// heartbeat, with the heartbeat's TTL.
func TestRefreshAddsReassertionsAndDrawsNothing(t *testing.T) {
	t.Parallel()
	for _, mod := range map[string]func(*storetest.Config){
		"tiny":       func(*storetest.Config) {},
		"everything": func(c *storetest.Config) { *c = everything(); c.Runs = false },
		"late":       func(c *storetest.Config) { c.LateProbability, c.ConfirmProbability = 0.5, 0.5 },
	} {
		c := storetest.Tiny()
		mod(&c)
		plain := map[string]int{}
		for _, r := range allRecords(t, c) {
			plain[refreshKey(r)]++
		}
		c.Refresh.On = true
		ttl := time.Duration(c.TTLFactor) * c.HeartbeatInterval
		added := 0
		for _, r := range allRecords(t, c) {
			watched := r.Producer == storetest.ProducerK8sObjects || r.Producer == storetest.ProducerFabric
			if watched && r.Kind == lifecycle.Observe && r.TTL != ttl {
				t.Fatalf("%s Observe with TTL %s, want %s: %+v", r.Producer, r.TTL, ttl, r)
			}
			if k := refreshKey(r); plain[k] > 0 {
				plain[k]--
				continue
			}
			added++
			beat := r.EventTime.Sub(c.Start) % c.HeartbeatInterval
			if !watched || r.Kind != lifecycle.Observe || beat != 0 || r.EventTime.Equal(c.Start) {
				t.Fatalf("a record that is not a heartbeat re-assertion by k8sobjects or fabric: %+v", r)
			}
		}
		for k, n := range plain {
			if n != 0 {
				t.Fatalf("%d records of the stream without Refresh are missing with it: %s", n, k)
			}
		}
		if added == 0 {
			t.Fatal("Refresh.On added no re-assertion")
		}
	}
}

func TestRefreshKeepsRunsEquivalent(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 2; seed++ {
		c := everything()
		c.Seed = seed
		c.Refresh = storetest.RefreshConfig{On: true, RunMaxAge: 90 * time.Second}
		checkRunsEquivalence(t, c, nil)
	}
}

func TestRunMaxAgeBoundsEveryRun(t *testing.T) {
	t.Parallel()
	c := everything()
	c.Refresh.RunMaxAge = 90 * time.Second
	extended := 0
	for _, r := range allRecords(t, c) {
		if r.Through.IsZero() {
			continue
		}
		extended++
		if age := r.Through.Sub(r.EventTime); age >= c.Refresh.RunMaxAge {
			t.Fatalf("a run of age %s with RunMaxAge %s: %+v", age, c.Refresh.RunMaxAge, r)
		}
	}
	if extended == 0 {
		t.Fatal("no run was extended")
	}
	// Without the bound, runs grow past it.
	c.Refresh.RunMaxAge = 0
	longest := time.Duration(0)
	for _, r := range allRecords(t, c) {
		if !r.Through.IsZero() {
			longest = max(longest, r.Through.Sub(r.EventTime))
		}
	}
	if longest < 90*time.Second {
		t.Fatalf("the longest unbounded run is %s, which the bound would not have cut", longest)
	}
}
