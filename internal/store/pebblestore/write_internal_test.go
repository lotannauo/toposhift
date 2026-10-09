package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

var errCommit = errors.New("injected commit failure")

// An error from a commit makes the store refuse further writes and retentions
// until it is reopened, whether or not the batch landed: Pebble's error does not
// say, and a reopening decides. Reads continue, and LastSeq is what the database
// shows.
func TestAFailedCommitStopsTheStoreWhateverBecameOfTheBatch(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		before, after func() error
		wantSeq       uint64
	}{
		"the batch did not land":        {before: func() error { return errCommit }, wantSeq: 1},
		"the batch landed and was seen": {after: func() error { return errCommit }, wantSeq: 5},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			s := openMem(t, Options{})
			if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
				t.Fatal(err)
			}
			s.beforeRecordApply, s.afterRecordApply = tc.before, tc.after
			err := s.Write(bg, []store.Record{edgeRecord(3, "p", t0.Add(time.Second), lifecycle.Observe, 0), edgeRecord(5, "p", t0.Add(2*time.Second), lifecycle.Observe, 0)})
			if !errors.Is(err, errCommit) {
				t.Fatalf("Write = %v, want an error wrapping the commit's", err)
			}
			if got := s.LastSeq(); got != tc.wantSeq {
				t.Errorf("LastSeq = %d, want %d: what the database shows", got, tc.wantSeq)
			}
			s.beforeRecordApply, s.afterRecordApply = nil, nil
			if err := s.Write(bg, []store.Record{edgeRecord(6, "p", t0.Add(3*time.Second), lifecycle.Observe, 0)}); err == nil {
				t.Error("a Write after a failed commit succeeded")
			}
			if err := s.Retain(bg, t0.Add(time.Hour)); err == nil {
				t.Error("a Retain after a failed commit succeeded")
			}
			recs, err := s.Window(bg, podFP, store.Forward, t0, t0.Add(time.Minute), store.Current(catalog.L2))
			if err != nil || uint64(len(recs)) != (tc.wantSeq+1)/2 {
				t.Errorf("Window = %d records, %v; want what LastSeq %d names", len(recs), err, tc.wantSeq)
			}
		})
	}
}

// If the outcome cannot even be read back, the store stops all the same, and
// LastSeq stays where it was.
func TestAStoreThatLosesTheOutcomeOfACommitStopsWriting(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	s.beforeRecordApply = func() error { return errCommit }
	s.rereadFails = func() error { return errors.New("the read-back failed") }
	if err := s.Write(bg, []store.Record{edgeRecord(2, "p", t0.Add(time.Second), lifecycle.Observe, 0)}); !errors.Is(err, errCommit) {
		t.Fatalf("Write = %v", err)
	}
	s.beforeRecordApply, s.rereadFails = nil, nil
	if err := s.Write(bg, []store.Record{edgeRecord(2, "p", t0.Add(time.Second), lifecycle.Observe, 0)}); err == nil {
		t.Error("a Write after the outcome was lost succeeded")
	}
	if err := s.Retain(bg, t0.Add(time.Hour)); err == nil {
		t.Error("a Retain after the outcome was lost succeeded")
	}
	if err := s.Write(bg, nil); err != nil {
		t.Errorf("an empty batch is a no-op whatever the state; got %v", err)
	}
	if got, err := s.Neighbors(bg, podFP, store.Forward, t0.Add(time.Minute), store.Current(catalog.L2)); err != nil || len(got) != 1 {
		t.Errorf("a read of a store that stopped writing = %v, %v; want the edge", got, err)
	}
	if s.LastSeq() != 1 {
		t.Errorf("LastSeq = %d, want 1", s.LastSeq())
	}
}

// The failure Pebble reports for a log that cannot be synced is fatal to it: the
// logger the store opens it with ends the process with status 70, as it must, and
// what the next start finds is the batch whole or not at all. The test runs the
// failing store in a child process (the test binary again) on real files, and
// reopens them here.
func TestAFailedSyncEndsTheProcessAndLeavesTheBatchWholeOrAbsent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(bg, time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestHelperAStoreWhoseLogCannotBeSynced$")
	cmd.Env = append(os.Environ(), "PEBBLESTORE_SYNC_FAILS_IN="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 70 {
		t.Fatalf("the child ended with %v, want exit status 70\n%s", err, out)
	}
	if !strings.Contains(string(out), "INJECTED-SYNC-FAILURE-REACHED") {
		t.Fatalf("the child did not reach the failing commit:\n%s", out)
	}

	s, err := Open(dir, Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning()}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	recs, err := s.Window(bg, podFP, store.Forward, t0, t0.Add(time.Hour), store.Current(catalog.L2))
	if err != nil {
		t.Fatal(err)
	}
	switch s.LastSeq() {
	case 1: // the first, synced batch alone
		if len(recs) != 1 {
			t.Errorf("LastSeq 1 and %d records", len(recs))
		}
	case 3: // and the batch whose sync failed, whole
		if len(recs) != 3 {
			t.Errorf("LastSeq 3 and %d records: part of a batch", len(recs))
		}
	default:
		t.Errorf("LastSeq = %d after reopening, want 1 or 3", s.LastSeq())
	}
	if _, err := s.Window(bg, nodeFP, store.Reverse, t0, t0.Add(time.Hour), store.Current(catalog.L2)); err != nil {
		t.Fatal(err)
	}
	// And the reopened store is not stuck.
	if err := s.Write(bg, []store.Record{edgeRecord(s.LastSeq()+1, "p", t0.Add(time.Minute), lifecycle.Observe, 0)}); err != nil {
		t.Errorf("a Write after reopening = %v", err)
	}
}

// failingSyncFS fails the sync of the log once armed.
type failingSyncFS struct {
	vfs.FS
	armed atomic.Bool
}

func (f *failingSyncFS) wrap(file vfs.File, err error, name string) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	if strings.HasSuffix(name, ".log") {
		return &failingSyncFile{File: file, fs: f}, nil
	}
	return file, nil
}

func (f *failingSyncFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, category)
	return f.wrap(file, err, name)
}

func (f *failingSyncFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, category)
	return f.wrap(file, err, newname)
}

type failingSyncFile struct {
	vfs.File
	fs *failingSyncFS
}

func (f *failingSyncFile) Sync() error {
	if f.fs.armed.Load() {
		return errors.New("injected sync failure")
	}
	return f.File.Sync()
}

func (f *failingSyncFile) SyncData() error {
	if f.fs.armed.Load() {
		return errors.New("injected sync failure")
	}
	return f.File.SyncData()
}

// TestHelperAStoreWhoseLogCannotBeSynced is the child of the test above, and does
// nothing when run on its own.
func TestHelperAStoreWhoseLogCannotBeSynced(t *testing.T) {
	dir := os.Getenv("PEBBLESTORE_SYNC_FAILS_IN")
	if dir == "" {
		t.Skip("run by TestAFailedSyncEndsTheProcessAndLeavesTheBatchWholeOrAbsent")
	}
	fsys := &failingSyncFS{FS: vfs.Default}
	s, err := Open(dir, Options{Config: pebblekv.Config{Tuning: pebblekv.TinyTuning(), Sync: true, FS: fsys}})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatal(err)
	}
	fsys.armed.Store(true)
	fmt.Println("INJECTED-SYNC-FAILURE-REACHED")
	// Pebble's logger ends the process inside this call.
	_ = s.Write(bg, []store.Record{
		edgeRecord(2, "p", t0.Add(time.Second), lifecycle.Observe, 0), edgeRecord(3, "p", t0.Add(2*time.Second), lifecycle.Observe, 0),
	})
	t.Fatal("the process survived a failed sync of the log")
}

// The context decides before the commit and never after: a Write whose context
// ends while the commit is in flight has stored its batch and says so.
func TestAContextThatEndsDuringTheCommitDoesNotClaimNothingWasStored(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(bg)
	s := openMem(t, Options{afterRecordApply: func() error { cancel(); return nil }})
	if err := s.Write(ctx, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Fatalf("Write = %v: the context ended after the commit, which cannot be a failure", err)
	}
	if s.LastSeq() != 1 {
		t.Errorf("LastSeq = %d, want 1", s.LastSeq())
	}
	// And one that ended before it stores nothing.
	if err := s.Write(ctx, []store.Record{edgeRecord(2, "p", t0, lifecycle.Observe, 0)}); !errors.Is(err, context.Canceled) || s.LastSeq() != 1 {
		t.Errorf("Write with a done context = %v, LastSeq %d; want the context's error and 1", err, s.LastSeq())
	}
}

// No record may carry the Seq that names every record, however the batch is
// built.
func TestSeqLatestIsRefused(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	good := edgeRecord(1, "p", t0, lifecycle.Observe, 0)
	for name, batch := range map[string][]store.Record{
		"alone":              {edgeRecord(store.Latest, "p", t0, lifecycle.Observe, 0)},
		"after a good one":   {good, edgeRecord(store.Latest, "p", t0.Add(time.Second), lifecycle.Observe, 0)},
		"one below it, then": {edgeRecord(store.Latest-1, "p", t0, lifecycle.Observe, 0), edgeRecord(store.Latest, "p", t0.Add(time.Second), lifecycle.Observe, 0)},
	} {
		if err := s.Write(bg, batch); !errors.Is(err, store.ErrInvalid) || errors.Is(err, store.ErrBeforeHorizon) {
			t.Errorf("a batch with Seq Latest %s = %v, want ErrInvalid", name, err)
		}
		if s.LastSeq() != 0 || len(dump(t, s)) != 0 {
			t.Fatalf("a refused batch (%s) left LastSeq %d and %v", name, s.LastSeq(), dump(t, s))
		}
	}
	// The largest Seq a record can carry is the one below Latest.
	if err := s.Write(bg, []store.Record{edgeRecord(store.Latest-1, "p", t0, lifecycle.Observe, 0)}); err != nil {
		t.Errorf("Seq Latest-1 = %v", err)
	}
}

// The checks of Write come in the order of the reference's, so a batch that breaks
// two rules is refused for the same one by both.
func TestWriteChecksInTheReferencesOrder(t *testing.T) {
	t.Parallel()
	p := newPair(t, Options{})
	p.write(edgeRecord(5, "p", t0, lifecycle.Observe, 0))
	p.retain(t0.Add(time.Minute))
	before := t0.Add(-time.Hour)
	invalid := edgeRecord(6, "", t0.Add(time.Hour), lifecycle.Observe, 0) // no producer
	for name, batch := range map[string][]store.Record{
		"invalid before the seq":              {func() store.Record { r := invalid; r.Seq = 1; return r }()},
		"seq before the horizon":              {edgeRecord(4, "p", before, lifecycle.Observe, 0)},
		"seq Latest before the horizon":       {edgeRecord(store.Latest, "p", before, lifecycle.Observe, 0)},
		"horizon after a good record":         {edgeRecord(6, "p", t0.Add(time.Hour), lifecycle.Observe, 0), edgeRecord(7, "p", before, lifecycle.Observe, 0)},
		"a repeated seq after the horizon":    {edgeRecord(6, "p", before, lifecycle.Observe, 0), edgeRecord(6, "p", t0.Add(time.Hour), lifecycle.Observe, 0)},
		"a subject in two layers in a batch":  {edgeRecord(6, "p", t0.Add(time.Hour), lifecycle.Observe, 0), withLayer(edgeRecord(7, "p", t0.Add(time.Hour), lifecycle.Observe, 0), catalog.L3)},
		"a subject in one layer in two batch": {edgeRecord(6, "p", t0.Add(time.Hour), lifecycle.Observe, 0), edgeRecord(7, "q", t0.Add(time.Hour), lifecycle.Observe, 0)},
	} {
		got := p.s.Write(bg, cloneAll(batch))
		want := p.ref.Write(bg, cloneAll(batch))
		if (got == nil) != (want == nil) || errors.Is(got, store.ErrBeforeHorizon) != errors.Is(want, store.ErrBeforeHorizon) || errors.Is(got, store.ErrInvalid) != errors.Is(want, store.ErrInvalid) {
			t.Errorf("%s: the store says %v, the reference %v", name, got, want)
		}
		if p.s.LastSeq() != p.ref.LastSeq() {
			t.Fatalf("%s: LastSeq %d, the reference's %d", name, p.s.LastSeq(), p.ref.LastSeq())
		}
	}
}

func withLayer(r store.Record, l catalog.Layer) store.Record { r.Layer = l; return r }

// A closed store refuses even an empty batch, and a done context is refused before
// an empty batch is.
func TestWriteChecksClosedThenContextThenEmpty(t *testing.T) {
	t.Parallel()
	s := openMem(t, Options{})
	done, cancel := context.WithCancel(bg)
	cancel()
	if err := s.Write(done, nil); !errors.Is(err, context.Canceled) {
		t.Errorf("an empty batch with a done context = %v, want the context's error", err)
	}
	if err := s.Write(bg, nil); err != nil {
		t.Errorf("an empty batch = %v", err)
	}
	_ = s.Close()
	if err := s.Write(done, nil); !errors.Is(err, store.ErrClosed) {
		t.Errorf("an empty batch to a closed store with a done context = %v, want ErrClosed", err)
	}
}

var errValue = errors.New("injected value check failure")

// A record whose value the codec would refuse is refused with ErrInvalid at its own
// turn, whole batch and no Seq consumed; a record before it that breaks another
// rule is refused for that rule first, as the reference store orders its checks.
func TestAValueTheCodecWouldRefuseIsRefusedAtItsTurn(t *testing.T) {
	t.Parallel()
	var seen []uint64
	s := openMem(t, Options{checkValue: func(v pebblekv.Value) error {
		seen = append(seen, v.Seq)
		if v.Seq == 2 {
			return errValue
		}
		return nil
	}})
	good, bad := edgeRecord(1, "p", t0, lifecycle.Observe, 0), edgeRecord(2, "p", t0.Add(time.Second), lifecycle.Observe, 0)
	err := s.Write(bg, []store.Record{good, bad})
	if !errors.Is(err, store.ErrInvalid) || !errors.Is(err, errValue) || errors.Is(err, store.ErrBeforeHorizon) {
		t.Fatalf("Write = %v, want an error wrapping ErrInvalid and the check's", err)
	}
	if s.LastSeq() != 0 || len(dump(t, s)) != 0 {
		t.Errorf("a refused batch left LastSeq %d and %v", s.LastSeq(), dump(t, s))
	}
	if !slices.Equal(seen, []uint64{1, 2}) {
		t.Errorf("the check saw seqs %v, want each record's value", seen)
	}
	// A record that breaks an earlier rule (the Seq) is refused for it first.
	if err := s.Write(bg, []store.Record{good}); err != nil {
		t.Fatal(err)
	}
	err = s.Write(bg, []store.Record{edgeRecord(1, "p", t0, lifecycle.Observe, 0), bad})
	if !errors.Is(err, store.ErrInvalid) || errors.Is(err, errValue) {
		t.Errorf("a repeated Seq before a bad value = %v, want the Seq's error", err)
	}
	// And the real check accepts what Validate accepts.
	ok := openMem(t, Options{})
	if err := ok.Write(bg, []store.Record{good, bad}); err != nil {
		t.Errorf("Write with the real check = %v", err)
	}
}
