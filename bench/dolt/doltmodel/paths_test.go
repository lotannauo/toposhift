package doltmodel_test

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// lapseInstants are the instants at which records take effect and lapse, and the instants
// either side of each: where a read's answer can change. They are sorted and distinct.
func lapseInstants(records []engine.Record) []int64 {
	set := map[int64]struct{}{}
	for _, r := range records {
		ev := doltmodel.Nanos(r.EventTime)
		set[ev], set[ev-1], set[ev+1] = struct{}{}, struct{}{}, struct{}{}
		if r.TTL > 0 {
			last := ev
			if !r.Through.IsZero() {
				last = max(last, doltmodel.Nanos(r.Through))
			}
			d := last + int64(r.TTL)
			set[d], set[d-1], set[d+1] = struct{}{}, struct{}{}, struct{}{}
		}
	}
	out := make([]int64, 0, len(set))
	for ns := range set {
		if ns >= 0 {
			out = append(out, ns)
		}
	}
	slices.Sort(out)
	return out
}

// probeInstants are the instants one token is read at: far past every record, around the
// token's frontier and the horizon, and a few of the lapse instants, a different few for
// each token (by its index, so a run reads the same ones every time).
func probeInstants(lapse []int64, horizon, frontier int64, hasFrontier bool, index int) []int64 {
	set := map[int64]struct{}{doltmodel.Nanos(engine.MaxEventTime): {}}
	if horizon > 0 {
		set[horizon], set[horizon+1] = struct{}{}, struct{}{}
	}
	if hasFrontier && frontier != doltmodel.NoFrontier {
		for _, d := range []int64{-int64(time.Second), -1, 0, 1, int64(time.Second), int64(time.Hour)} {
			if frontier+d >= 0 {
				set[frontier+d] = struct{}{}
			}
		}
	}
	if n := len(lapse); n > 0 {
		for k := range 6 {
			set[lapse[(index*37+k*n/6)%n]] = struct{}{}
		}
		// The lapse instants nearest the frontier are where a wrong frontier shows.
		at, _ := slices.BinarySearch(lapse, frontier)
		for _, j := range []int{at - 2, at - 1, at, at + 1, at + 2, at + 3} {
			if j >= 0 && j < n {
				set[lapse[j]] = struct{}{}
			}
		}
	}
	out := make([]int64, 0, len(set))
	for ns := range set {
		out = append(out, ns)
	}
	slices.Sort(out)
	return out
}

// maxEntitiesPerToken bounds the entities one token is read for.
const maxEntitiesPerToken = 250

// checkPaths is the per-key-equals-log property on what a stream has written. For every
// token the specified reads cover (those at or above the sequence number of the last
// retention): every batch boundary, the newest token, and the tokens just inside a batch,
// which name no commit; and for every instant at or after the horizon, including the
// instants just before and at each token's frontier, the rule's answer equals the log path's,
// for every entity, direction and layer. The state table the model kept is also the one the
// log implies.
func checkPaths(m *doltmodel.Model, w written) error {
	layers := entityLayers(w.records)
	fps := sortedEntities(layers)
	tokens := []uint64{engine.Latest}
	for _, c := range m.Commits() {
		for _, tok := range []uint64{c.Token, c.Token - 1} { // a boundary, and a token inside its batch
			if tok >= w.floor && c.Token > 0 && !slices.Contains(tokens, tok) {
				tokens = append(tokens, tok)
			}
		}
	}
	horizon := int64(0)
	if !w.horizon.IsZero() {
		horizon = doltmodel.Nanos(w.horizon)
	}
	lapse := lapseInstants(w.records)
	stride := (len(fps) + maxEntitiesPerToken - 1) / maxEntitiesPerToken
	for i, token := range tokens {
		frontier, haveState := m.Frontier(token)
		for _, ns := range probeInstants(lapse, horizon, frontier, haveState, i) {
			if ns < horizon {
				continue
			}
			t := doltmodel.FromNanos(ns)
			for j, fp := range fps {
				// A stream with many entities is read at a different slice of them at
				// each token, so that every entity is read at many tokens.
				if stride > 1 && (j+i)%stride != 0 {
					continue
				}
				for _, layer := range layers[fp] {
					sc := engine.Scope{Layer: layer, AsOf: token}
					label := func() string {
						return fmt.Sprintf("token %d at %d (frontier %d), %s in %s", token, ns, frontier, fp, layer)
					}
					log, err := m.AliveVia(doltmodel.PathLog, fp, t, sc)
					if err != nil {
						return err
					}
					got, err := m.Alive(fp, t, sc)
					if err != nil {
						return err
					}
					if got != log {
						return fmt.Errorf("alive: the rule says %v, the log %v: %s", got, log, label())
					}
					for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
						log, err := m.NeighborsVia(doltmodel.PathLog, fp, dir, t, sc)
						if err != nil {
							return err
						}
						got, err := m.Neighbors(fp, dir, t, sc)
						if err != nil {
							return err
						}
						if !slices.Equal(got, log) {
							return fmt.Errorf("neighbors %s: the rule says %v, the log %v: %s", dir, got, log, label())
						}
					}
				}
			}
		}
	}
	return checkDerivedState(m, w.floor)
}

// checkDerivedState compares the state tables the model kept with those the logs imply.
func checkDerivedState(m *doltmodel.Model, lastRetainSeq uint64) error {
	t := m.Tables()
	state, ent := doltmodel.DeriveState(t.Log, t.EntLog)
	// Rows retention deleted are not in the model's state: those committed by the last
	// retention whose deadline is at or before the horizon.
	state, ent = doltmodel.WithoutExpired(state, ent, t.Meta.HorizonNs, doltmodel.EncodeSeq(lastRetainSeq))
	got := doltmodel.Tables{Meta: t.Meta, State: t.State, EntState: t.EntState}
	want := doltmodel.Tables{Meta: t.Meta, State: state, EntState: ent}
	if got.Digest() != want.Digest() {
		return fmt.Errorf("the state table is not the one the log implies: %s", got.FirstDifference(want))
	}
	return nil
}

func pathConfigs() []workload.Config {
	cfgs := conformance.Configs()
	// A spread of shapes: heavy lateness, a second producer, coalesced runs with lateness
	// and fresh identities with backlogs.
	return []workload.Config{cfgs[1], cfgs[5], cfgs[6]}
}

// TestPerKeyEqualsLog streams fixed workloads, with late events, shared instants, second
// producers, coalesced runs and retentions, into both arms, and checks the property at
// every batch-boundary token and beside it.
func TestPerKeyEqualsLog(t *testing.T) {
	for i, cfg := range pathConfigs() {
		for _, retain := range [][]float64{nil, {0.4}, {0.3, 0.7}} {
			for _, arm := range []doltmodel.Arm{doltmodel.ArmCommit, doltmodel.ArmRows} {
				t.Run(fmt.Sprintf("config %d retain %v %s", i, retain, arm), func(t *testing.T) {
					conformance.SkipWhenTrimmed(t)
					m := doltmodel.New(doltmodel.WithArm(arm))
					if err := stream(m, cfg, 40, retain, func(w written) error { return checkPaths(m, w) }); err != nil {
						t.Fatal(err)
					}
					if arm == doltmodel.ArmCommit && retain == nil {
						// The rule is not a single path in disguise: the checks settled keys
						// both ways.
						p := m.Paths()
						if p["neighbors/keys-state"] == 0 || p["neighbors/keys-log"] == 0 || p["alive/keys-state"] == 0 || p["alive/keys-log"] == 0 {
							t.Fatalf("the checks did not settle keys both ways: %v", p)
						}
					}
				})
			}
		}
	}
}
