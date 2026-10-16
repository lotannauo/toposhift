package main

import (
	"strings"
	"testing"
)

// TestMutantsAreCaught runs the reduction with a deliberate fault and shows
// that the check of the test named in each case fails on it. The same checks
// pass on the honest reduction in those tests.
func TestMutantsAreCaught(t *testing.T) {
	for _, c := range []struct {
		name string
		// caughtBy is the test whose check must fail.
		caughtBy string
		mut      mutant
		data     dataset
		change   func(*Config)
		// fails reports whether the check sees the fault, given the result or the error.
		fails func(t *testing.T, res *results, cfg Config, err error) bool
	}{
		{
			name: "split at a file boundary", caughtBy: "TestIntervalAcrossFileBoundaryIsOne", mut: mutSplitAtFileBoundary,
			data: dataset{placement: boundaryPod()},
			fails: func(t *testing.T, _ *results, cfg Config, err error) bool {
				return err == nil && checkBoundary(t, cfg) != nil
			},
		},
		{
			name: "a gap of exactly one TTL is a break", caughtBy: "TestGapsSplitOnlyBeyondOneTTL", mut: mutGapAtTTLBreaks,
			data: dataset{placement: gapPods()},
			fails: func(t *testing.T, _ *results, cfg Config, err error) bool {
				return err == nil && checkGaps(t, cfg) != nil
			},
		},
		{
			name: "UNKNOWN is a service", caughtBy: "TestCallsRollupEdgesAndUnknownEndpoints", mut: mutUnknownIsService,
			data: callsData(),
			fails: func(t *testing.T, res *results, _ Config, err error) bool {
				return err == nil && checkUnknownIsNoService(res) != nil
			},
		},
		{
			name: "a Delete is inferred at a disappearance", caughtBy: "TestStreamsAreTheProductsRecords", mut: mutInferDelete,
			data: dataset{placement: gapPods()},
			fails: func(t *testing.T, _ *results, _ Config, err error) bool {
				return err != nil && strings.Contains(err.Error(), "Delete")
			},
		},
		{
			name: "the fixture downsamples rows, not nodes", caughtBy: "TestFixtureKeepsWholeNodes", mut: mutUniformDownsample,
			data:   fixtureData(),
			change: func(c *Config) { c.FixtureNodes = 8 },
			fails: func(t *testing.T, res *results, _ Config, err error) bool {
				return err == nil && checkFixtureNodes(res) != nil
			},
		},
		{
			name: "the CPU utilization is in the payload", caughtBy: "TestMeasurementsAreNotInThePayload", mut: mutCPUInPayload,
			data: dataset{placement: cpuPod()},
			fails: func(t *testing.T, _ *results, cfg Config, err error) bool {
				return err == nil && checkPayloads(t, cfg) != nil
			},
		},
		{
			name: "the service is read off the instance token", caughtBy: "TestServiceIsTheMSNameColumn", mut: mutServiceFromToken,
			data: dataset{placement: serviceOfPod()},
			fails: func(t *testing.T, res *results, _ Config, err error) bool {
				return err == nil && checkServiceColumn(res) != nil
			},
		},
		{
			name: "the every-sample stream goes through the coalescer", caughtBy: "TestEveryAssertionIsInTheSStream", mut: mutCoalescedS,
			data: armsData(t), change: arms(true, false),
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err == nil && checkEveryAssertion(t, res, cfg, "c60") != nil
			},
		},
		{
			name: "the interval stream's Through is the interval's deadline", caughtBy: "TestIntervalStreamIsOneRecordPerInterval", mut: mutIntervalThroughDeadline,
			data: armsData(t), change: arms(false, true),
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err != nil || checkIntervalStream(t, res, cfg, "c60") != nil
			},
		},
		{
			name: "the interval stream splits at a gap of exactly one TTL", caughtBy: "TestIntervalStreamIsOneRecordPerInterval", mut: mutGapAtTTLBreaks,
			data: armsData(t), change: arms(false, true),
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err == nil && checkIGaps(t, res, cfg) != nil
			},
		},
		{
			name: "an arm changes the coalesced stream", caughtBy: "TestArmsLeaveTheCoalescedStreamsAndTheTablesAlone", mut: mutArmFeedsC,
			data: armsData(t), change: arms(true, false),
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err == nil && checkArmsLeaveCAlone(t, res, cfg) != nil
			},
		},
		{
			name: "the TTL factor is ignored", caughtBy: "TestTTLFactorSetsThePlacementTTLOnly", mut: mutFactorIgnored,
			data: ttlData(), change: func(c *Config) { c.TTLFactor = 15 },
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err == nil && checkTTLFactor(t, res, cfg) != nil
			},
		},
		{
			name: "the TTL factor is applied to the calls", caughtBy: "TestTTLFactorSetsThePlacementTTLOnly", mut: mutFactorOnCalls,
			data: ttlData(), change: func(c *Config) { c.TTLFactor = 15 },
			fails: func(t *testing.T, res *results, cfg Config, err error) bool {
				return err == nil && checkTTLFactor(t, res, cfg) != nil
			},
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			cfg := testConfig(t, c.data.withFiller().write(t))
			cfg.mut = c.mut
			if c.change != nil {
				c.change(&cfg)
			}
			res, err := reduce(cfg)
			if !c.fails(t, res, cfg, err) {
				t.Fatalf("%s is not caught by %s (error: %v)", c.name, c.caughtBy, err)
			}
		})
	}
}
