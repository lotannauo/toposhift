package doltmodel_test

import (
	"fmt"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// workOf is the rows the key ranges of one read hold, counted independently of the model's
// own reads, from its tables as they are now and as the token's commit left the state.
type workOf struct{ state, log uint64 }

// expectNeighbors counts, for a neighbors read of fp in direction dir at t as of token, the
// state rows of the subject's range and, for each whose row is after t, the log rows of
// its key; a token that names no commit reads the whole range of the log.
func expectNeighbors(m *doltmodel.Model, fp identity.Fingerprint, dir engine.Direction, t int64, token uint64) workOf {
	tb := m.Tables()
	enc := string(doltmodel.EncodeFingerprint(fp))
	state, have := stateFor(m, tb, token)
	var w workOf
	if !have {
		for _, l := range tb.Log {
			if l.Layer == int(catalog.L2) && l.Dir == int(dir) && l.A == enc {
				w.log++
			}
		}
		return w
	}
	for _, s := range state {
		if s.Layer != int(catalog.L2) || s.Dir != int(dir) || s.A != enc {
			continue
		}
		w.state++
		if s.EventNs <= t {
			continue
		}
		for _, l := range tb.Log {
			if l.Layer == s.Layer && l.Dir == s.Dir && l.A == s.A && l.Peer == s.Peer && l.Rel == s.Rel && l.Producer == s.Producer {
				w.log++
			}
		}
	}
	return w
}

// expectAlive is the same for an existence read: the entity state and log tables.
func expectAlive(m *doltmodel.Model, fp identity.Fingerprint, t int64, token uint64) workOf {
	tb := m.Tables()
	enc := string(doltmodel.EncodeFingerprint(fp))
	_, ents, have := entStateFor(m, tb, token)
	var w workOf
	if !have {
		for _, l := range tb.EntLog {
			if l.Fp == enc {
				w.log++
			}
		}
		return w
	}
	for _, s := range ents {
		if s.Fp != enc {
			continue
		}
		w.state++
		if s.EventNs <= t {
			continue
		}
		for _, l := range tb.EntLog {
			if l.Fp == s.Fp && l.Producer == s.Producer {
				w.log++
			}
		}
	}
	return w
}

// stateFor is the edge state a read as of the token sees, and whether there is any: the
// current state for a token at or above the newest, the newest commit's at the token in the
// arm with commits.
func stateFor(m *doltmodel.Model, tb doltmodel.Tables, token uint64) ([]doltmodel.StateRow, bool) {
	s, _, ok := stateAndEnt(m, tb, token)
	return s, ok
}

func entStateFor(m *doltmodel.Model, tb doltmodel.Tables, token uint64) ([]doltmodel.StateRow, []doltmodel.EntStateRow, bool) {
	return stateAndEnt(m, tb, token)
}

func stateAndEnt(m *doltmodel.Model, tb doltmodel.Tables, token uint64) ([]doltmodel.StateRow, []doltmodel.EntStateRow, bool) {
	if token >= m.LastSeq() {
		return tb.State, tb.EntState, true
	}
	if m.Arm() != doltmodel.ArmCommit {
		return nil, nil, false
	}
	ord := 0
	for _, c := range m.Commits() {
		if c.Token == token {
			ord = c.Ordinal
		}
	}
	if ord == 0 {
		return nil, nil, false
	}
	s, e, err := m.StateAt(ord)
	if err != nil {
		panic(err)
	}
	return s, e, true
}

// delta is what the counters rose by.
func delta(before, after map[string]uint64) map[string]uint64 {
	out := map[string]uint64{}
	for k, v := range after {
		if d := v - before[k]; d != 0 {
			out[k] = d
		}
	}
	return out
}

// TestWorkIsTheRowsTheKeyRangesHold holds the model's derived counts of the rows a read's
// ranges hold to a count made from the model's tables, for random streams with late
// records, runs and retentions, every kind of read, every instant of the minute, and every
// token: the newest, a batch boundary and a token inside a batch.
func TestWorkIsTheRowsTheKeyRangesHold(t *testing.T) {
	for _, arm := range []doltmodel.Arm{doltmodel.ArmCommit, doltmodel.ArmRows} {
		t.Run(arm.String(), func(t *testing.T) {
			rapid.Check(t, func(rt *rapid.T) {
				m := doltmodel.New(doltmodel.WithArm(arm))
				seq := uint64(0)
				var horizon int64
				for b := range rapid.IntRange(1, 6).Draw(rt, "batches") {
					var batch []engine.Record
					for i := range rapid.IntRange(1, 6).Draw(rt, "batch size") {
						r := genRecord(rt, fmt.Sprintf("record %d.%d", b, i), horizon/int64(time.Second))
						seq++
						r.Seq = seq
						batch = append(batch, r)
					}
					if err := m.Write(batch); err != nil {
						rt.Fatal(err)
					}
					if rapid.IntRange(0, 3).Draw(rt, "retain") == 0 {
						h := rapid.Int64Range(10, 60).Draw(rt, "horizon")
						if err := m.Retain(at(h)); err != nil {
							rt.Fatal(err)
						}
						horizon = max(horizon, h*int64(time.Second))
					}
				}
				tokens := []uint64{engine.Latest}
				for _, c := range m.Commits() {
					tokens = append(tokens, c.Token, c.Token-1)
				}
				for _, token := range tokens {
					sc := engine.Scope{Layer: catalog.L2, AsOf: token}
					for s := int64(5); s <= 75; s += 7 {
						ts := time.Unix(s, 0).UTC()
						if doltmodel.Nanos(ts) < horizon {
							continue
						}
						for _, f := range []identity.Fingerprint{pod1, pod2, node1} {
							for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
								before := m.Work()
								if _, err := m.Neighbors(f, dir, ts, sc); err != nil {
									rt.Fatal(err)
								}
								want := expectNeighbors(m, f, dir, doltmodel.Nanos(ts), token)
								got := delta(before, m.Work())
								if got["neighbors/rows-state"] != want.state || got["neighbors/rows-log"] != want.log {
									rt.Fatalf("neighbors %s %s at %d s as of %d: work %v, counted from the tables %+v", f, dir, s, token, got, want)
								}
							}
							before := m.Work()
							if _, err := m.Alive(f, ts, sc); err != nil {
								rt.Fatal(err)
							}
							want := expectAlive(m, f, doltmodel.Nanos(ts), token)
							got := delta(before, m.Work())
							if got["alive/rows-state"] != want.state || got["alive/rows-log"] != want.log {
								rt.Fatalf("alive %s at %d s as of %d: work %v, counted from the tables %+v", f, s, token, got, want)
							}
						}
					}
				}
			})
		})
	}
}

// TestWorkOfAWindowIsTheLogRowsOfItsRange: the rows of the subject in the window, whatever the
// sequence filter takes out of them, and beside it the rows of the subject's whole range, which
// is what the primary key's prefix holds.
func TestWorkOfAWindowIsTheLogRowsOfItsRange(t *testing.T) {
	m := doltmodel.New()
	var seq uint64
	for i := range 6 {
		seq++
		r := observe(edge1, seq, "a", int64(10+i*10), 0)
		if err := m.Write([]engine.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	// Records at 10, 20, ..., 60 s; the window [20 s, 50 s) holds three, and as of token 3
	// the filter leaves two of them, but the range is three rows.
	if _, err := m.Window(pod1, engine.Forward, at(20), at(50), engine.Scope{Layer: catalog.L2, AsOf: 3}); err != nil {
		t.Fatal(err)
	}
	if got := m.Work(); got["window/rows-log"] != 3 || got["window/rows-prefix"] != 6 || len(got) != 2 {
		t.Fatalf("work %v", got)
	}
	// A window that holds none reads none of the index's range, and all of the prefix's; a
	// window of the other direction reads the other direction's rows (the target's).
	before := m.Work()
	if _, err := m.Window(pod1, engine.Forward, at(1000), at(2000), engine.Scope{Layer: catalog.L2, AsOf: engine.Latest}); err != nil {
		t.Fatal(err)
	}
	if d := delta(before, m.Work()); len(d) != 1 || d["window/rows-prefix"] != 6 {
		t.Fatalf("an empty window: %v", d)
	}
	if _, err := m.Window(node1, engine.Reverse, at(0), at(100), engine.Scope{Layer: catalog.L2, AsOf: engine.Latest}); err != nil {
		t.Fatal(err)
	}
	if got := m.Work(); got["window/rows-log"] != 3+6 || got["window/rows-prefix"] != 6+6+6 {
		t.Fatalf("work %v", got)
	}
	// A window in another layer reads nothing.
	before = m.Work()
	if _, err := m.Window(pod1, engine.Forward, at(0), at(100), engine.Scope{Layer: catalog.L1, AsOf: engine.Latest}); err != nil {
		t.Fatal(err)
	}
	if d := delta(before, m.Work()); len(d) != 0 {
		t.Fatalf("a window in another layer: %v", d)
	}
}
