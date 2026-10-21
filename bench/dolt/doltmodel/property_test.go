package doltmodel_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The subjects of the random streams: two entities and four edges between them (two of
// them the same pair in the other direction, and two relations), all in layer L2, so that
// they share endpoints and a reader sees several peers.
var (
	randomSubjects = []engine.Subject{
		engine.EntitySubject(pod1),
		engine.EntitySubject(node1),
		engine.EdgeSubject(pod1, node1, "scheduled_on"),
		engine.EdgeSubject(pod2, node1, "scheduled_on"),
		engine.EdgeSubject(node1, pod1, "scheduled_on"),
		engine.EdgeSubject(pod1, node1, "part_of"),
	}
	randomProducers = []lifecycle.Producer{"a", "b", "c"}
)

// genRecord draws a record about a random subject. Event times fall in a minute starting
// ten seconds past the epoch, so that many records share instants and a later batch is often
// before an earlier one; TTLs are zero (watch mode) or a few seconds; a delete carries none.
func genRecord(t *rapid.T, label string, notBefore int64) engine.Record {
	s := rapid.SampledFrom(randomSubjects).Draw(t, label+" subject")
	r := engine.Record{
		Layer: catalog.L2, Subject: s,
		Producer:  rapid.SampledFrom(randomProducers).Draw(t, label+" producer"),
		EventTime: at(max(notBefore, 10+rapid.Int64Range(0, 60).Draw(t, label+" at"))),
	}
	if rapid.IntRange(0, 3).Draw(t, label+" delete") == 0 {
		r.Kind = lifecycle.Delete
		return r
	}
	r.Kind = lifecycle.Observe
	r.Payload = []byte{byte(rapid.IntRange(0, 3).Draw(t, label+" payload"))}
	if rapid.Bool().Draw(t, label+" has ttl") {
		r.TTL = time.Duration(rapid.Int64Range(1, 8).Draw(t, label+" ttl")) * time.Second
	}
	if rapid.IntRange(0, 3).Draw(t, label+" run") == 0 {
		r.Through = r.EventTime.Add(time.Duration(rapid.Int64Range(0, 5).Draw(t, label+" through")) * time.Second)
	}
	return r
}

// TestRandomStreamsKeepStateEqualToLogAndEqualToTheOracle writes random batches, with late
// events, shared instants, overridden records, several producers, runs and deletes, and
// retentions at random points, to both arms and to the reference engine. After every batch
// the state-equals-log property holds at every batch-boundary token, and every read, at
// every second of the minute and beside it, is the reference engine's.
func TestRandomStreamsKeepStateEqualToLogAndEqualToTheOracle(t *testing.T) {
	for _, arm := range []doltmodel.Arm{doltmodel.ArmCommit, doltmodel.ArmRows} {
		t.Run(arm.String(), func(t *testing.T) {
			rapid.Check(t, func(t *rapid.T) {
				m, ora := doltmodel.New(doltmodel.WithArm(arm)), oracle.New()
				var w written
				seq := uint64(rapid.SampledFrom([]uint64{0, 1<<32 - 3, 1<<63 - 3}).Draw(t, "first seq"))
				for b := range rapid.IntRange(1, 8).Draw(t, "batches") {
					n := rapid.IntRange(1, 6).Draw(t, "batch size")
					var batch []engine.Record
					floor := doltmodel.Nanos(w.horizon) / int64(time.Second)
					for i := range n {
						r := genRecord(t, fmt.Sprintf("record %d.%d", b, i), floor)
						seq++
						r.Seq = seq
						batch = append(batch, r)
					}
					if err := m.Write(slices.Clone(batch)); err != nil {
						t.Fatal(err)
					}
					if err := ora.Write(slices.Clone(batch)); err != nil {
						t.Fatal(err)
					}
					w.records = append(w.records, batch...)
					if rapid.IntRange(0, 3).Draw(t, "retain") == 0 {
						h := at(rapid.Int64Range(10, 60).Draw(t, "horizon"))
						if err := m.Retain(h); err != nil {
							t.Fatal(err)
						}
						if err := ora.Retain(h); err != nil {
							t.Fatal(err)
						}
						if h.After(w.horizon) {
							w.horizon, w.floor = h, m.LastSeq()
						}
					}
					if err := checkPaths(m, w); err != nil {
						t.Fatal(err)
					}
					if err := compareToOracle(m, ora, w); err != nil {
						t.Fatal(err)
					}
				}
			})
		})
	}
}

// compareToOracle asks both engines every read of the universe at every second of the
// minute and a nanosecond either side, as of the newest token and every committed token at
// or above the floor.
func compareToOracle(m *doltmodel.Model, ora *oracle.Oracle, w written) error {
	tokens := []uint64{engine.Latest}
	for _, c := range m.Commits() {
		for _, tok := range []uint64{c.Token, c.Token - 1} { // a boundary, and a token inside its batch
			if tok >= w.floor && c.Token > 0 && !slices.Contains(tokens, tok) {
				tokens = append(tokens, tok)
			}
		}
	}
	for _, token := range tokens {
		sc := engine.Scope{Layer: catalog.L2, AsOf: token}
		for s := int64(0); s <= 75; s++ {
			for _, d := range []int64{-1, 0, 1} {
				ts := time.Unix(s, d).UTC()
				if ts.Before(w.horizon) {
					continue
				}
				for _, f := range []identity.Fingerprint{pod1, pod2, node1} {
					a, err := m.Alive(f, ts, sc)
					if err != nil {
						return err
					}
					if want, _ := ora.Alive(f, ts, sc); a != want {
						return fmt.Errorf("alive %s at %s as of %d: %v, the oracle %v", f, ts.Format(time.RFC3339Nano), token, a, want)
					}
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						got, err := m.Neighbors(f, dir, ts, sc)
						if err != nil {
							return err
						}
						want, _ := ora.Neighbors(f, dir, ts, sc)
						if !slices.Equal(got, want) {
							return fmt.Errorf("neighbors %s %s at %s as of %d: %v, the oracle %v", f, dir, ts.Format(time.RFC3339Nano), token, got, want)
						}
					}
				}
			}
		}
	}
	return nil
}
