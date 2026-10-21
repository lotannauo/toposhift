package doltmodel_test

import (
	"fmt"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// A check is a function of a way to make a model that returns the first thing it finds
// wrong.
type check struct {
	name string
	run  func(newModel func(...doltmodel.Option) *doltmodel.Model) error
}

var (
	// oracleCheck is the spike's conformance check against the reference engine, on
	// every fixed workload with and without mid-stream retentions.
	oracleCheck = check{"conformance against the reference", func(newModel func(...doltmodel.Option) *doltmodel.Model) error {
		for i, cfg := range conformance.Configs() {
			for _, retain := range [][]float64{nil, {0.3, 0.7}} {
				if err := conformance.Check(newModel(), cfg, conformance.Options{RetainAt: retain}); err != nil {
					return fmt.Errorf("config %d retain %v: %w", i, retain, err)
				}
			}
		}
		return nil
	}}
	// scriptedChecks are the conformance package's scripted scenarios.
	scriptedChecks = check{"scripted conformance checks", func(newModel func(...doltmodel.Option) *doltmodel.Model) error {
		for name, c := range map[string]func(engine.Engine) error{
			"write contract": conformance.CheckWriteContract, "read contract": conformance.CheckReadContract,
			"instant": conformance.CheckInstant, "producers": conformance.CheckProducers,
			"relations": conformance.CheckRelations, "extremes": conformance.CheckExtremes,
		} {
			if err := c(newModel()); err != nil {
				return fmt.Errorf("%s: %w", name, err)
			}
		}
		return nil
	}}
	// refusalCheck is the model's own: the refused batches leave nothing behind.
	refusalCheck = check{"refused batches leave nothing", func(newModel func(...doltmodel.Option) *doltmodel.Model) error {
		return checkRefusals(newModel(doltmodel.WithPayloadCapacity(4)))
	}}
	// settleCheck is about how keys are settled, which no answer shows: a row at the
	// instant settles its key, and the old snapshot's reads settle most keys from history.
	settleCheck = check{"settlement counts", func(newModel func(...doltmodel.Option) *doltmodel.Model) error {
		if err := checkRowAtInstant(newModel()); err != nil {
			return err
		}
		return checkOldTokenSettledFromHistory(newModel(doltmodel.WithPayloadCapacity(doltmodel.RegisteredPayloadCapacity)))
	}}
	// pathsCheck is the state-equals-log property on the fixed workloads.
	pathsCheck = check{"per key equals log", func(newModel func(...doltmodel.Option) *doltmodel.Model) error {
		for i, cfg := range pathConfigs() {
			for _, retain := range [][]float64{nil, {0.3, 0.7}} {
				m := newModel()
				if err := stream(m, cfg, 40, retain, func(w written) error { return checkPaths(m, w) }); err != nil {
					return fmt.Errorf("config %d retain %v: %w", i, retain, err)
				}
			}
		}
		return nil
	}}
)

// TestMutantsAreCaught switches each deliberate defect on, one at a time, and requires the
// checks written for it to fail. The checks are deterministic (fixed workloads and seeds),
// so a defect either is caught or is not; a check listed here that stops catching its
// defect is a check that has lost its teeth.
func TestMutantsAreCaught(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	for _, c := range []struct {
		fault string
		by    []check
	}{
		{doltmodel.FaultDropSeqFilter, []check{oracleCheck, pathsCheck}},
		{doltmodel.FaultTieByEvent, []check{oracleCheck, scriptedChecks, pathsCheck}},
		{doltmodel.FaultStateKeepsLast, []check{oracleCheck, pathsCheck}},
		// A row used for a key whose event is after the instant gives a wrong answer; the
		// same rule written with a strict comparison gives the same answers and settles
		// fewer keys, which only the counts show; a log fallback without the token's filter
		// is found where a later record is before the instant; and the choice made for the
		// whole read instead of each key gives the same answers too.
		{doltmodel.FaultUseRowAfterT, []check{oracleCheck, pathsCheck}},
		{doltmodel.FaultEventBefore, []check{settleCheck}},
		{doltmodel.FaultFallbackNoSeq, []check{oracleCheck, pathsCheck}},
		{doltmodel.FaultPathPerQuery, []check{settleCheck}},
		{doltmodel.FaultCommitAfter, []check{oracleCheck, scriptedChecks, pathsCheck}},
		{doltmodel.FaultWindowDedup, []check{oracleCheck, scriptedChecks}},
		{doltmodel.FaultBatchDropsEmpty, []check{oracleCheck, scriptedChecks}},
		{doltmodel.FaultRetainDropsHolding, []check{oracleCheck, pathsCheck}},
		{doltmodel.FaultRetainDropsOpenState, []check{oracleCheck, pathsCheck}},
		{doltmodel.FaultAcceptBeforeHorizon, []check{oracleCheck, scriptedChecks}},
		{doltmodel.FaultDeadlineInclusive, []check{oracleCheck, scriptedChecks}},
		// What a refused batch must leave behind is the model's own check, as well as the
		// conformance package's, which has no payload capacity.
		{doltmodel.FaultSkipValidate, []check{scriptedChecks, refusalCheck}},
		{doltmodel.FaultAcceptStaleSeq, []check{scriptedChecks, refusalCheck}},
		{doltmodel.FaultAcceptLayerConflict, []check{refusalCheck}},
		{doltmodel.FaultIgnoreCapacity, []check{refusalCheck}},
		{doltmodel.FaultIgnoreProducerLen, []check{refusalCheck}},
	} {
		for _, ck := range c.by {
			t.Run(c.fault+"/"+ck.name, func(t *testing.T) {
				t.Parallel()
				err := ck.run(func(opts ...doltmodel.Option) *doltmodel.Model {
					m := doltmodel.New(opts...)
					m.Break(c.fault)
					return m
				})
				if err == nil {
					t.Fatalf("a model that %s passed the %s", c.fault, ck.name)
				}
			})
		}
	}
}

// TestTheUnbrokenModelPassesTheChecks is the other half: with no defect every check
// passes, so a failure above is the defect's.
func TestTheUnbrokenModelPassesTheChecks(t *testing.T) {
	conformance.SkipWhenTrimmed(t)
	for _, ck := range []check{oracleCheck, scriptedChecks, pathsCheck, refusalCheck, settleCheck} {
		t.Run(ck.name, func(t *testing.T) {
			t.Parallel()
			if err := ck.run(func(opts ...doltmodel.Option) *doltmodel.Model { return doltmodel.New(opts...) }); err != nil {
				t.Fatal(err)
			}
		})
	}
}
