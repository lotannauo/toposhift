package runner

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// None of these tests reads a real clock: the clock is a number the test moves, and
// every duration in them is a number the test chose.

// scriptedEngine answers a read of an entity with the neighbors a function gives for
// the number of the call (counted over all entities), and counts the calls.
type scriptedEngine struct {
	engine.Engine
	calls int
	// answer is what call n (from 1) of the read of entity fp returns.
	answer func(fp identity.Fingerprint, n int) ([]engine.Neighbor, error)
	// onCall runs inside the engine's call.
	onCall func(fp identity.Fingerprint, n int)
	perFp  map[identity.Fingerprint]int
}

func (s *scriptedEngine) Neighbors(fp identity.Fingerprint, _ engine.Direction, _ time.Time, _ engine.Scope) ([]engine.Neighbor, error) {
	s.calls++
	if s.perFp == nil {
		s.perFp = map[identity.Fingerprint]int{}
	}
	s.perFp[fp]++
	if s.onCall != nil {
		s.onCall(fp, s.perFp[fp])
	}
	return s.answer(fp, s.perFp[fp])
}

var (
	fpsMu   sync.Mutex
	fpsByID = map[byte]identity.Fingerprint{}
	idsByFp = map[identity.Fingerprint]byte{}
)

// fpOf is the fingerprint of the entity the test calls b.
func fpOf(b byte) identity.Fingerprint {
	fpsMu.Lock()
	defer fpsMu.Unlock()
	if f, ok := fpsByID[b]; ok {
		return f
	}
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: fmt.Sprintf("node-%d", b)}})
	if err != nil {
		panic(err)
	}
	f := id.Fingerprint()
	fpsByID[b], idsByFp[f] = f, b
	return f
}

// idOf is the number the test gave the entity.
func idOf(f identity.Fingerprint) byte {
	fpsMu.Lock()
	defer fpsMu.Unlock()
	return idsByFp[f]
}

func neighborsOf(b byte) []engine.Neighbor {
	return []engine.Neighbor{{Peer: fpOf(b), Relation: "r"}}
}

// scriptedPlan has one query for each of the entities, each expecting the neighbors
// neighborsOf(entity).
func scriptedPlan(t *testing.T, entities ...byte) *Plan {
	t.Helper()
	p := &Plan{}
	for i, b := range entities {
		q := Query{Group: "g", Rank: i, Op: OpNeighbors, Fps: []identity.Fingerprint{fpOf(b)}, Age: AgeNow}
		a, err := fetched{ns: neighborsOf(b)}.answer(q)
		if err != nil {
			t.Fatal(err)
		}
		q.Expect = a.Digest
		p.Queries = append(p.Queries, q)
	}
	return p
}

func rightAnswers(fp identity.Fingerprint, _ int) ([]engine.Neighbor, error) {
	return neighborsOf(idOf(fp)), nil
}

// probeFunc is a probe that reads a counter the test moves.
type probeFunc struct {
	n      *int64
	events *[]string
}

func (p probeFunc) WarmCounter() (string, int64, error) {
	if p.events != nil {
		*p.events = append(*p.events, "probe")
	}
	return "test.counter", *p.n, nil
}

// What is between the clock's two readings is the engine's call and nothing else: not
// the hashing of the answer, which a candidate that gives the same answer pays alike
// and which would dilute a small read; and the warm-up asks are not timed at all.
func TestTimeQueriesTimesOnlyTheEngineCall(t *testing.T) {
	t.Parallel()

	const warmup, reps = 3, 5
	var now, counter int64
	var events []string
	eng := &scriptedEngine{answer: rightAnswers, onCall: func(identity.Fingerprint, int) { events = append(events, "engine") }}
	steps := askSteps{
		fetch: func(e engine.Engine, q Query) (fetched, error) {
			events = append(events, "fetch")
			now += 5000 // what the engine's call costs
			return fetch(e, q)
		},
		answer: func(f fetched, q Query) (Answer, error) {
			events = append(events, "answer")
			now += 777 // what hashing costs
			return f.answer(q)
		},
	}
	o := TimedReadOptions{Warmup: warmup, Reps: reps, Clock: func() int64 { events = append(events, "clock"); return now }}
	plan := scriptedPlan(t, 1, 2)
	got, err := timeQueries(context.Background(), eng, probeFunc{&counter, &events}, plan, o, steps)
	if err != nil {
		t.Fatal(err)
	}

	var want []string
	for range plan.Queries {
		for range warmup {
			want = append(want, "fetch", "engine", "answer")
		}
		want = append(want, "probe")
		for range reps {
			want = append(want, "clock", "fetch", "engine", "clock", "answer")
		}
		want = append(want, "probe")
	}
	if !slices.Equal(events, want) {
		t.Fatalf("the order of events is\n%s\nwant\n%s", strings.Join(events, " "), strings.Join(want, " "))
	}
	for _, q := range got {
		if len(q.Ns) != reps {
			t.Fatalf("%s has %d durations, want %d", q.Name, len(q.Ns), reps)
		}
		for i, d := range q.Ns {
			if d != 5000 {
				t.Errorf("%s ask %d took %d on the test's clock, want the engine call's 5000 alone", q.Name, i, d)
			}
		}
	}
}

// Warmup and Reps asks reach the engine for each query, in the order of the plan, and
// the durations are kept in the order of the asks.
func TestTimeQueriesCountsWarmUpsAndRepsAndKeepsOrder(t *testing.T) {
	t.Parallel()

	var now int64
	var order []identity.Fingerprint
	eng := &scriptedEngine{
		answer: rightAnswers,
		onCall: func(fp identity.Fingerprint, n int) {
			order = append(order, fp)
			// The cost of the n-th ask of an entity is 1000*entity + n.
			now += int64(idOf(fp))*1000 + int64(n)
		},
	}
	var counter int64
	plan := scriptedPlan(t, 7, 3, 5)
	// Zero options are the defaults: 3 and 31.
	got, err := TimeQueries(context.Background(), eng, probeFunc{n: &counter}, plan, TimedReadOptions{Clock: func() int64 { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if want := (3 + 31) * 3; eng.calls != want {
		t.Errorf("%d asks reached the engine, want %d", eng.calls, want)
	}
	// Plan order: all of an entity's asks, then the next entity's.
	var entities []byte
	for _, fp := range order {
		if len(entities) == 0 || entities[len(entities)-1] != idOf(fp) {
			entities = append(entities, idOf(fp))
		}
	}
	if !slices.Equal(entities, []byte{7, 3, 5}) {
		t.Errorf("the entities were asked in the order %v", entities)
	}
	for i, e := range []int64{7, 3, 5} {
		if len(got[i].Ns) != 31 {
			t.Fatalf("query %d has %d durations", i, len(got[i].Ns))
		}
		for rep, d := range got[i].Ns {
			// The timed asks are the 4th to the 34th of the entity.
			if want := e*1000 + int64(3+rep+1); d != want {
				t.Errorf("query %d, timed ask %d took %d, want %d (out of order, or a warm-up timed)", i, rep, d, want)
			}
		}
	}
}

// The probe's counter is read around the timed asks of a query and only those: what
// moved it in the warm-up asks, or between two queries, is not the query's.
func TestAQueryThatMovesTheProbeDuringItsTimedAsksIsNotWarm(t *testing.T) {
	t.Parallel()

	var counter int64
	eng := &scriptedEngine{answer: rightAnswers, onCall: func(fp identity.Fingerprint, n int) {
		switch {
		case idOf(fp) == 1 && n <= 3: // a warm-up ask of the first query loads blocks
			counter += 10
		case idOf(fp) == 2 && n == 5: // a timed ask of the second does
			counter += 4
		}
	}}
	got, err := TimeQueries(context.Background(), eng, probeFunc{n: &counter}, scriptedPlan(t, 1, 2, 3), TimedReadOptions{Clock: func() int64 { return 0 }})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []struct {
		delta int64
		warm  bool
	}{{0, true}, {4, false}, {0, true}} {
		if got[i].WarmDelta != want.delta || got[i].Warm != want.warm {
			t.Errorf("query %d: delta %d, warm %v; want %d, %v", i, got[i].WarmDelta, got[i].Warm, want.delta, want.warm)
		}
	}
}

// An answer that is not the reference engine's, in a warm-up or a timed ask, marks its
// query and no other; an error of the engine ends the timing and is not lost.
func TestTimeQueriesVoidsAWrongAnswer(t *testing.T) {
	t.Parallel()

	var counter int64
	probe := probeFunc{n: &counter}
	o := TimedReadOptions{Warmup: 3, Reps: 7, Clock: func() int64 { return 0 }}
	for _, c := range []struct {
		name string
		ask  int // the ask of the second query that answers wrongly
	}{{"a timed ask", 5}, {"a warm-up ask", 2}} {
		eng := &scriptedEngine{answer: func(fp identity.Fingerprint, n int) ([]engine.Neighbor, error) {
			if idOf(fp) == 2 && n == c.ask {
				return neighborsOf(99), nil
			}
			return neighborsOf(idOf(fp)), nil
		}}
		got, err := TimeQueries(context.Background(), eng, probe, scriptedPlan(t, 1, 2, 3), o)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if got[0].Mismatch || !got[1].Mismatch || got[2].Mismatch {
			t.Errorf("%s: mismatches are %v %v %v, want only the second query's", c.name, got[0].Mismatch, got[1].Mismatch, got[2].Mismatch)
		}
	}

	boom := errors.New("boom")
	eng := &scriptedEngine{answer: func(fp identity.Fingerprint, n int) ([]engine.Neighbor, error) {
		if idOf(fp) == 2 && n == 6 {
			return nil, boom
		}
		return neighborsOf(idOf(fp)), nil
	}}
	if _, err := TimeQueries(context.Background(), eng, probe, scriptedPlan(t, 1, 2, 3), o); !errors.Is(err, boom) {
		t.Errorf("an engine error in a timed ask: %v", err)
	}
}

// Nothing is timed, and nothing is asked, once the context is done; it is looked at
// between queries and not inside an interval.
func TestTimeQueriesStopsBetweenQueries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	var counter int64
	eng := &scriptedEngine{answer: rightAnswers, onCall: func(fp identity.Fingerprint, n int) {
		if idOf(fp) == 1 && n == 2 {
			cancel() // in the middle of the first query's asks
		}
	}}
	_, err := TimeQueries(ctx, eng, probeFunc{n: &counter}, scriptedPlan(t, 1, 2), TimedReadOptions{Warmup: 3, Reps: 5, Clock: func() int64 { return 0 }})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if eng.perFp[fpOf(1)] != 8 || eng.perFp[fpOf(2)] != 0 {
		t.Errorf("asks of the first query %d (want all 8, the interval is not cut), of the second %d (want 0)", eng.perFp[fpOf(1)], eng.perFp[fpOf(2)])
	}
}

// A count that makes no timed read is refused, and the engine is not asked.
func TestTimeQueriesRefusesNegativeCounts(t *testing.T) {
	t.Parallel()

	var counter int64
	eng := &scriptedEngine{answer: rightAnswers}
	for _, o := range []TimedReadOptions{{Warmup: -1}, {Reps: -3}} {
		if _, err := TimeQueries(context.Background(), eng, probeFunc{n: &counter}, scriptedPlan(t, 1), o); err == nil || eng.calls != 0 {
			t.Errorf("%+v: %v after %d asks", o, err, eng.calls)
		}
	}
}

// The recorder is muted for the whole timing and only for that.
func TestTimeQueriesMutesAndUnmutes(t *testing.T) {
	t.Parallel()

	rec := NewCapture()
	var counter int64
	eng := &scriptedEngine{answer: rightAnswers, onCall: func(identity.Fingerprint, int) { rec.Count("x", 1) }}
	_, err := TimeQueries(context.Background(), eng, probeFunc{n: &counter}, scriptedPlan(t, 1, 2), TimedReadOptions{Mute: rec.Mute})
	if err != nil {
		t.Fatal(err)
	}
	if got := rec.Take(); len(got) != 0 {
		t.Errorf("the recorder kept %v while muted", got)
	}
	rec.Count("x", 1)
	if got := rec.Take(); got["x"] != 1 {
		t.Errorf("the recorder is still muted after the timing: %v", got)
	}
	// And unmuted when the timing fails.
	boom := fmt.Errorf("boom")
	bad := &scriptedEngine{answer: func(identity.Fingerprint, int) ([]engine.Neighbor, error) { return nil, boom }}
	if _, err := TimeQueries(context.Background(), bad, probeFunc{n: &counter}, scriptedPlan(t, 1), TimedReadOptions{Mute: rec.Mute}); err == nil {
		t.Fatal("no error")
	}
	rec.Count("x", 1)
	if got := rec.Take(); got["x"] != 1 {
		t.Errorf("the recorder is still muted after a failed timing: %v", got)
	}
}

// The value of a query is the median of its asks: the lower of the two middle ones when
// the count is even, never an average and never the upper one.
func TestQueryValueIsTheLowerMedian(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		ns   []int64
		want int64
	}{
		{[]int64{9, 1, 5}, 5},
		{[]int64{40, 10, 30, 20}, 20},
		{[]int64{4, 3}, 3},
		{[]int64{7}, 7},
	} {
		if got := queryValue(c.ns); got != c.want {
			t.Errorf("queryValue(%v) = %d, want %d", c.ns, got, c.want)
		}
	}
}

// statsOnly is an engine whose statistics are whatever the test says.
type statsOnly struct {
	measurable
	stats map[string]int64
}

func (s statsOnly) Stats() map[string]int64 { return s.stats }

// A statistic that is missing is no zero: a probe that read it as one would find every
// query warm.
func TestTheStatsProbeRefusesAMissingCounter(t *testing.T) {
	t.Parallel()

	if _, n, err := (statsProbe{statsOnly{stats: map[string]int64{"cache_misses": 7}}}).WarmCounter(); err != nil || n != 7 {
		t.Errorf("with the counter: %d, %v", n, err)
	}
	if _, _, err := (statsProbe{statsOnly{stats: map[string]int64{"cache_hits": 7}}}).WarmCounter(); err == nil || !strings.Contains(err.Error(), "cache_misses") {
		t.Errorf("without the counter: %v", err)
	}
}
