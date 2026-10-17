package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
)

// TestMutantsAreCaught runs the measurement with a deliberate fault and shows that
// the check of the test named in each case fails on it. The same checks pass on the
// honest measurement in those tests.
//
// Skipping CompactAll alone (Canonicalize still runs) is not a case: Canonicalize flushes the memtable and rewrites every
// live key into the bottom level itself, so a store that was not compacted first ends
// in the same tables, and no check can tell the two apart.
func TestMutantsAreCaught(t *testing.T) {
	f := newFixture(t)
	if n := int64(len(f.records)); n%37 == 0 {
		t.Fatalf("the fixture holds %d records, a multiple of the batch: the last batch is not partial", n)
	}
	blobs := func(m *pebble.Metrics) { m.BlobFiles.LiveSize = 1 }
	for _, c := range []struct {
		name string
		// caughtBy is the test whose check must fail.
		caughtBy string
		mut      mutant
		change   func(t *testing.T, c *config)
		// fails reports whether the check sees the fault, given the result or the error.
		fails func(res *result, diag diagnostics, err error) bool
	}{
		{
			name: "the last partial batch is dropped", caughtBy: "TestLoadWritesTheFilesRecords", mut: mutDropLastBatch,
			fails: func(_ *result, _ diagnostics, err error) bool { return err != nil },
		},
		{
			name: "the keys are counted as records", caughtBy: "TestLoadWritesTheFilesRecords", mut: mutKeysAsRecords,
			fails: func(res *result, _ diagnostics, err error) bool { return err == nil && checkRecords(res, f) != nil },
		},
		{
			name: "Canonicalize is skipped", caughtBy: "TestTwoLoadsAgree", mut: mutSkipCanonicalize,
			fails: func(res *result, diag diagnostics, err error) bool {
				return err == nil && checkSettled(res, diag) != nil
			},
		},
		{
			name: "CompactAll and Canonicalize are skipped", caughtBy: "TestTwoLoadsAgree", mut: mutSkipSettle,
			fails: func(res *result, diag diagnostics, err error) bool {
				return err == nil && checkSettled(res, diag) != nil
			},
		},
		{
			name: "Through is stripped", caughtBy: "TestDataDigestIsFrozen", mut: mutStripThrough,
			fails: func(res *result, _ diagnostics, err error) bool { return err == nil && checkFrozenDigest(res) != nil },
		},
		{
			name: "Seq is renumbered", caughtBy: "TestDataDigestIsFrozen", mut: mutRenumberSeq,
			fails: func(res *result, _ diagnostics, err error) bool { return err == nil && checkFrozenDigest(res) != nil },
		},
		{
			// The store itself refuses an entity type in another layer than its own; the
			// digest would show it for a store that did not.
			name: "the records go to the wrong layer", caughtBy: "TestDataDigestIsFrozen", mut: mutWrongLayer,
			fails: func(res *result, _ diagnostics, err error) bool { return err != nil || checkFrozenDigest(res) != nil },
		},
		{
			name: "the checkpoint setting is not passed", caughtBy: "TestCheckpointsLandOnEdgePrefixesOnly", mut: mutCheckpointsIgnored,
			change: func(_ *testing.T, c *config) { c.Checkpoints = checkpointsOff },
			fails:  func(res *result, _ diagnostics, err error) bool { return err == nil && checkNoCheckpoints(res) != nil },
		},
		{
			name: "the options have no boot key", caughtBy: "TestCheckpointsLandOnEdgePrefixesOnly", mut: mutNoBootKey,
			fails: func(res *result, _ diagnostics, err error) bool {
				return err == nil && checkDefaultCheckpoints(res) != nil
			},
		},
		{
			name: "the blob guard is removed", caughtBy: "TestBlobFilesFailTheMeasurementWithNoRow", mut: mutNoBlobGuard,
			change: func(_ *testing.T, c *config) { c.judge = blobs },
			fails:  func(_ *result, _ diagnostics, err error) bool { return err == nil },
		},
		{
			name: "the blob guard is inverted", caughtBy: "TestLoadWritesTheFilesRecords", mut: mutBlobGuardInverted,
			fails: func(_ *result, _ diagnostics, err error) bool { return err != nil },
		},
		{
			name: "-empty writes a record", caughtBy: "TestEmptyStore", mut: mutEmptyWrites,
			change: func(_ *testing.T, c *config) { c.In, c.Empty, c.ExpectRecords, c.Samples = "", true, 0, 0 },
			fails: func(res *result, _ diagnostics, err error) bool {
				return err == nil && (res.Records != 0 || res.Keys != 0)
			},
		},
		{
			name: "-in is accepted beside -empty", caughtBy: "TestEmptyIsRefusedBesideAnInput", mut: mutEmptyAcceptsIn,
			change: func(_ *testing.T, c *config) { c.Empty, c.ExpectRecords, c.Samples = true, 0, 0 },
			fails:  func(_ *result, _ diagnostics, err error) bool { return err == nil },
		},
		{
			name: "the work tree guard is removed", caughtBy: "TestRefusesAnOutputDirectoryInAGitWorkTree", mut: mutNoWorkTreeGuard,
			change: func(t *testing.T, c *config) {
				tree := t.TempDir()
				if err := os.Mkdir(filepath.Join(tree, ".git"), 0o755); err != nil {
					t.Fatal(err)
				}
				c.Out = filepath.Join(tree, "results")
			},
			fails: func(_ *result, _ diagnostics, err error) bool { return err == nil },
		},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			cfg := testConfig(t, f, "T/S")
			cfg.mut = c.mut
			if c.change != nil {
				c.change(t, &cfg)
			}
			res, diag, err := weigh(context.Background(), cfg)
			if !c.fails(res, diag, err) {
				t.Fatalf("%s is not caught by %s (error: %v)", c.name, c.caughtBy, err)
			}
		})
	}
}
