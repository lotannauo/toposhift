package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// crashed is what a stand-in for the end of the process panics with: the code after
// the point of the panic does not run, as after a kill.
type crashed struct{}

// crashes runs f and says whether the process "ended" in it. The mutexes the code
// holds are released by its own defers, which the test needs and a kill does not.
func crashes(t testing.TB, f func()) {
	t.Helper()
	defer func() {
		switch r := recover(); r.(type) {
		case nil:
			t.Fatal("the code did not reach the point where the process ends")
		case crashed:
		default:
			panic(r)
		}
	}()
	f()
}

// faultStore is a segment store that counts its calls, and ends the process or
// fails at the calls it is armed for. only, when set, limits the fault to the names
// that contain it.
type faultStore struct {
	segment.Store
	only                                     string
	killPut, killDelete, failPut, failDelete atomic.Bool
	puts, deletes                            atomic.Int64
	putNames                                 sync.Map
}

func (f *faultStore) hits(name string) bool { return f.only == "" || strings.Contains(name, f.only) }

func (f *faultStore) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	f.puts.Add(1)
	f.putNames.Store(name, true)
	if f.hits(name) {
		if f.killPut.Load() {
			panic(crashed{})
		}
		if f.failPut.Load() {
			return segment.Info{}, errors.New("injected put failure")
		}
	}
	return f.Store.Put(ctx, name, r, size)
}

func (f *faultStore) Delete(ctx context.Context, name string) error {
	f.deletes.Add(1)
	if f.hits(name) {
		if f.killDelete.Load() {
			panic(crashed{})
		}
		if f.failDelete.Load() {
			return errors.New("injected delete failure")
		}
	}
	return f.Store.Delete(ctx, name)
}

// seams puts a faultStore around the backup's segment store and around its journal,
// for the runs that follow; seg and jrn are the latest of each.
type seams struct{ seg, jrn *faultStore }

func installFaultSeams(t *testing.T) *seams {
	t.Helper()
	sm := &seams{}
	setSeam(t, &openSegments, func(root string) (segment.Store, error) {
		d, err := localdir.New(root)
		sm.seg = &faultStore{Store: d, only: "/manifests/"}
		return sm.seg, err
	})
	setSeam(t, &openJournal, func(root string) (segment.Store, error) {
		d, err := localdir.New(root)
		sm.jrn = &faultStore{Store: d}
		return sm.jrn, err
	})
	return sm
}

// crashWriter is the store of a rig, with the points at which the process can end
// around a commit, and the records the store committed.
type crashWriter struct {
	rigStore
	// before ends the process in the write, before the store commits; after, once
	// it has.
	before, after atomic.Bool
	// fail returns an error without committing: a batch the store did not store.
	fail atomic.Bool
	mu   sync.Mutex
	// written are the records the store committed.
	written []store.Record
}

func (w *crashWriter) Write(ctx context.Context, batch []store.Record) error {
	if len(batch) == 0 {
		return w.rigStore.Write(ctx, batch)
	}
	if w.before.Load() {
		panic(crashed{})
	}
	if w.fail.Load() {
		return errors.New("injected failure before the commit")
	}
	if err := w.rigStore.Write(ctx, batch); err != nil {
		return err
	}
	w.mu.Lock()
	for _, r := range batch {
		r.Payload = bytes.Clone(r.Payload)
		w.written = append(w.written, r)
	}
	w.mu.Unlock()
	if w.after.Load() {
		panic(crashed{})
	}
	return nil
}

func (w *crashWriter) records() []store.Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.written)
}

// feedAt writes n batches of the stream through w, as the sequencer would hand
// them to the store: each takes the Seqs after the store's last, so a batch that
// was lost leaves no gap.
func feedAt(t testing.TB, w sequencer.Writer, g *storetest.Generator, st horizonReader, n int) {
	t.Helper()
	for range n {
		b := nextBatch(t, g, st)
		if err := w.Write(t.Context(), b); err != nil {
			t.Fatal(err)
		}
	}
}

// nextBatch is 20 records of the stream with the Seqs after the store's last, less
// those the store would refuse for being before its layer's horizon: the stream does
// not know of a retention.
func nextBatch(t testing.TB, g *storetest.Generator, st horizonReader) []store.Record {
	t.Helper()
	var b []store.Record
	for len(b) == 0 {
		recs := g.Batch(20)
		if len(recs) == 0 {
			t.Fatal("the generator ran out")
		}
		for _, r := range recs {
			if h := st.LayerHorizon(r.Layer); !h.IsZero() && r.EventTime.Before(h.Time) {
				continue
			}
			r.Seq = st.LastSeq() + 1 + uint64(len(b))
			b = append(b, r)
		}
	}
	return b
}

// journalNames are the entries in the journal of the backup in dir.
func journalNames(t testing.TB, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, backupDirName, backupJournalName))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// verifyRestore checks that the backup in dir, stopped, ends at the store's last
// Seq and restores into a memstore and a pebblestore that answer as the live store
// does, at its horizons and after them.
func verifyRestore(t *testing.T, dir string, live *memstore.Store, written []store.Record, meta backup.StoreMeta) backup.Manifest {
	t.Helper()
	m, seg := stoppedBackup(t, dir)
	if m.Seq != live.LastSeq() {
		t.Fatalf("the backup ends at seq %d, the store at %d", m.Seq, live.LastSeq())
	}
	seen := map[identity.Fingerprint]bool{}
	var entities []identity.Fingerprint
	for _, r := range written {
		for _, fp := range []identity.Fingerprint{r.Subject.A, r.Subject.B} {
			if fp != (identity.Fingerprint{}) && !seen[fp] {
				seen[fp] = true
				entities = append(entities, fp)
			}
		}
	}
	for _, engine := range []string{"memstore", "pebblestore"} {
		var dst store.Store
		var err error
		if engine == "memstore" {
			dst, err = memstore.Open(memstore.Options{Offsets: meta.Offsets, Keep: meta.Keep})
		} else {
			o := pebblestore.DefaultOptions()
			o.Offsets, o.Keep = meta.Offsets, meta.Keep
			dst, err = pebblestore.Open(filepath.Join(t.TempDir(), "store"), o)
		}
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = dst.Close() })
		res, err := backup.Restore(t.Context(), seg, m, dst, backup.RestoreOptions{Store: meta, TempDir: t.TempDir()})
		if err != nil {
			t.Fatalf("restore into %s: %v", engine, err)
		}
		if res.Records != int64(len(written)) {
			t.Errorf("restore into %s wrote %d records, want %d", engine, res.Records, len(written))
		}
		if err := storetest.CompareRestored(live, dst, storetest.RestoredOptions{Entities: entities, Written: written, Span: time.Minute}); err != nil {
			t.Errorf("the restore into %s answers differently from the live store: %v", engine, err)
		}
	}
	return m
}

// The journal holds each batch from before the store is asked to commit it until a
// manifest holds it: one entry for each batch, written once, with nothing else
// written to the backup's segments until the cut, and deleted after the cut.
func TestBackupJournalHoldsEachBatchUntilItIsCut(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, false)
	cw := &crashWriter{rigStore: st}
	w, g := r.run.writer(st, cw), newGen(t)

	// The entry is there when the store is asked to commit.
	var seenAtCommit []string
	probe := &probeWriter{Writer: cw, before: func() { seenAtCommit = journalNames(t, r.dir) }}
	feedAt(t, r.run.writer(st, probe), g, st, 1)
	if want := []string{"1-20.parquet"}; !slices.Equal(seenAtCommit, want) {
		t.Errorf("the journal holds %v when the store commits, want %v", seenAtCommit, want)
	}
	feedAt(t, w, g, st, 2)
	if want := []string{"1-20.parquet", "21-40.parquet", "41-60.parquet"}; !slices.Equal(journalNames(t, r.dir), want) {
		t.Errorf("the journal holds %v, want %v", journalNames(t, r.dir), want)
	}
	if got := sm.jrn.puts.Load(); got != 3 {
		t.Errorf("%d writes to the journal for 3 batches, want 3", got)
	}
	if got := sm.seg.puts.Load(); got != 0 {
		t.Errorf("%d writes to the segments before a cut, want 0", got)
	}
	s := r.run.status()
	if s.Mode != backupOn || s.Local.JournalBatches != 3 {
		t.Errorf("status %+v, want on with 3 journal batches", s)
	}

	// Each entry is its batch.
	recs, err := r.run.jr.read(t.Context(), journalEntry{21, 40})
	if err != nil {
		t.Fatal(err)
	}
	written := cw.records()
	if len(recs) != 20 || !slices.EqualFunc(recs, written[20:40], func(a, b store.Record) bool {
		return a.Seq == b.Seq && a.Kind == b.Kind && a.Subject == b.Subject && a.EventTime.Equal(b.EventTime) && bytes.Equal(a.Payload, b.Payload)
	}) {
		t.Error("the journal's entry is not the batch that was written")
	}

	r.run.cut(t.Context())
	if got := journalNames(t, r.dir); len(got) != 0 {
		t.Errorf("the journal holds %v after the cut, want nothing", got)
	}
	if got := r.run.status().Local.JournalBatches; got != 0 {
		t.Errorf("%d journal batches in the status after the cut, want 0", got)
	}
	if got := sm.jrn.puts.Load(); got != 3 {
		t.Errorf("%d writes to the journal after the cut, want 3", got)
	}
	r.run.stop()
}

// probeWriter calls before just before the write it passes on.
type probeWriter struct {
	sequencer.Writer
	before func()
}

func (p *probeWriter) Write(ctx context.Context, batch []store.Record) error {
	p.before()
	return p.Writer.Write(ctx, batch)
}

// A crash at any step of a commit or a cut: the next start goes on from the journal
// with no gap, and what it backs up restores to a store that answers as the live
// store does. The first start after the crash ends at once, as a second crash, and
// the one after it must find the same.
func TestBackupSurvivesACrash(t *testing.T) {
	horizon := storetest.Tiny().Start.Add(10 * time.Second)
	for _, tt := range []struct {
		name string
		// cutFirst is whether a manifest exists before the crash.
		cutFirst bool
		crash    func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams)
		// seq is the Seq of the manifest the start finds.
		seq uint64
		// replayed is the number of batches the start takes from the journal.
		replayed int
		// retention is whether the start logs a retention.
		retention bool
	}{
		{
			name: "after the journal entry, before the store commits", cutFirst: true, seq: 60, replayed: 2,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				cw.before.Store(true)
				crashes(t, func() { feedAt(t, w, g, r.st, 1) })
				if got := len(journalNames(t, r.dir)); got != 3 {
					t.Fatalf("%d entries in the journal at the crash, want 3", got)
				}
			},
		},
		{
			name: "after the store commits, before the batch is added", cutFirst: true, seq: 60, replayed: 3,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				cw.after.Store(true)
				crashes(t, func() { feedAt(t, w, g, r.st, 1) })
			},
		},
		{
			name: "during a cut, after the files and before the manifest", cutFirst: true, seq: 60, replayed: 2,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				sm.seg.killPut.Store(true)
				crashes(t, func() { r.run.cut(t.Context()) })
				if sm.seg.puts.Load() == 0 {
					t.Fatal("the cut stored nothing")
				}
			},
		},
		{
			name: "after the manifest, before the journal is trimmed", cutFirst: true, seq: 100, replayed: 0,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				sm.jrn.killDelete.Store(true)
				crashes(t, func() { r.run.cut(t.Context()) })
				if m, _ := stoppedBackup(t, r.dir); m.Seq != r.st.LastSeq() {
					t.Fatalf("the manifest ends at seq %d, the store at %d", m.Seq, r.st.LastSeq())
				}
				if got := len(journalNames(t, r.dir)); got == 0 {
					t.Fatal("the journal was trimmed")
				}
			},
		},
		{
			name: "before the first cut", cutFirst: false, seq: 0, replayed: 3,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				cw.after.Store(true)
				crashes(t, func() { feedAt(t, w, g, r.st, 1) })
			},
		},
		{
			name: "between a retention and the next cut", cutFirst: true, seq: 60, replayed: 3, retention: true,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				if err := r.run.retainer(r.st, r.st).Retain(t.Context(), horizon); err != nil {
					t.Fatal(err)
				}
				g.SetHorizon(horizon)
				feedAt(t, w, g, r.st, 1)
			},
		},
		{
			name: "between a retention and the next cut, with nothing after it", cutFirst: true, seq: 60, replayed: 2, retention: true,
			crash: func(t *testing.T, r *rig, cw *crashWriter, w sequencer.Writer, g *storetest.Generator, sm *seams) {
				feedAt(t, w, g, r.st, 2)
				if err := r.run.retainer(r.st, r.st).Retain(t.Context(), horizon); err != nil {
					t.Fatal(err)
				}
			},
		},
	} {
		for _, starts := range []int{1, 2} {
			t.Run(fmt.Sprintf("%s, %d start", tt.name, starts), func(t *testing.T) {
				sm := installFaultSeams(t)
				st := rigMemstore(t, rigMeta)
				cw := &crashWriter{rigStore: st}
				r := newRig(t, "", st, rigMeta, false)
				w, g := r.run.writer(st, cw), newGen(t)
				if tt.cutFirst {
					feedAt(t, w, g, st, 3)
					r.run.cut(t.Context())
				}
				tt.crash(t, r, cw, w, g, sm)
				cw.before.Store(false) // the new process has nothing armed
				cw.after.Store(false)

				// The process is gone: nothing is stopped or closed. With two starts, the
				// first is gone too, before it does anything.
				for i := range starts {
					r = newRig(t, r.dir, st, rigMeta, false)
					s := r.run.status()
					if s.Mode != backupOn || !r.run.tee {
						t.Fatalf("start %d: status %+v tee %v, want on with the tee", i+1, s, r.run.tee)
					}
					if s.Local.Seq != tt.seq || s.Local.JournalBatches != tt.replayed {
						t.Errorf("start %d: the backup stands at seq %d with %d journal batches, want %d and %d",
							i+1, s.Local.Seq, s.Local.JournalBatches, tt.seq, tt.replayed)
					}
				}
				logs := r.logs.String()
				if got := strings.Contains(logs, "from the journal"); got != (tt.replayed > 0) {
					t.Errorf("the log says it took batches from the journal: %v, want %v:\n%s", got, tt.replayed > 0, logs)
				}
				if got := strings.Contains(logs, "logged a retention the store applied"); got != tt.retention {
					t.Errorf("the log says it logged a retention: %v, want %v:\n%s", got, tt.retention, logs)
				}

				// It goes on: more batches, a stop, and a restore that answers as the store.
				w = r.run.writer(st, cw)
				feedAt(t, w, g, st, 2)
				if tt.retention {
					r.run.retire(t.Context())
					if s := r.run.status(); s.Retire == nil || s.Retire.LastError != "" || s.Mode != backupOn {
						t.Errorf("status %+v after the retirement the retention asked for", s)
					}
				}
				r.run.stop()
				m := verifyRestore(t, r.dir, st, cw.records(), rigMeta)
				if tt.retention && len(m.Retentions) == 0 {
					t.Error("the backup holds no retention")
				}
				if got := journalNames(t, r.dir); len(got) != 0 {
					t.Errorf("the journal holds %v after the stop, want nothing", got)
				}
			})
		}
	}
}

// A batch the store did not store has no entry in the journal, and the batch that
// takes its Seqs is journaled and replayed in its place.
func TestBackupJournalDropsABatchThatWasNotStored(t *testing.T) {
	installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 2)
	cw.fail.Store(true)
	b := nextBatch(t, g, st)
	if err := w.Write(t.Context(), b); err == nil {
		t.Fatal("the injected failure was not returned")
	}
	if got := journalNames(t, r.dir); !slices.Equal(got, []string{"1-20.parquet", "21-40.parquet"}) {
		t.Errorf("the journal holds %v after a batch that was not stored, want the two before it", got)
	}
	if s := r.run.status(); s.Mode != backupOn || s.Local.JournalBatches != 2 {
		t.Errorf("status %+v, want on with 2 journal batches", s)
	}
	// The retry takes the same Seqs, with other records.
	cw.fail.Store(false)
	feedAt(t, w, g, st, 1)
	if got := journalNames(t, r.dir); !slices.Equal(got, []string{"1-20.parquet", "21-40.parquet", "41-60.parquet"}) {
		t.Errorf("the journal holds %v after the retry", got)
	}
	// A crash now: the next start replays what the store holds.
	r2 := newRig(t, r.dir, st, rigMeta, false)
	if s := r2.run.status(); s.Mode != backupOn || s.Local.JournalBatches != 3 {
		t.Fatalf("status %+v after the crash, want on with 3 journal batches", s)
	}
	r2.run.stop()
	verifyRestore(t, r.dir, st, cw.records(), rigMeta)
}

// An entry of a batch that was not stored and could not be deleted is deleted again
// before a batch takes its Seqs, and the backup is degraded until it is.
func TestBackupJournalRetriesAnEntryItCouldNotDelete(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 1)
	sm.jrn.failDelete.Store(true)
	cw.fail.Store(true)
	b := nextBatch(t, g, st)
	if err := w.Write(t.Context(), b); err == nil {
		t.Fatal("the injected failure was not returned")
	}
	if s := r.run.status(); s.Mode != backupDegraded || !strings.Contains(s.Reason, "could not delete") {
		t.Fatalf("status %+v, want degraded: an entry that could not be deleted", s)
	}
	// The retry's Seqs are the stale entry's: the delete fails again, so the batch is
	// committed without an entry, and the backup says so.
	cw.fail.Store(false)
	feedAt(t, w, g, st, 1)
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Fatalf("status %+v, want degraded", s)
	}
	// The delete works again: the next batch deletes the stale entry, and a cut makes
	// the journal whole.
	sm.jrn.failDelete.Store(false)
	feedAt(t, w, g, st, 1)
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Errorf("status %+v, want degraded until a cut holds the batch that has no entry", s)
	}
	r.run.cut(t.Context())
	if s := r.run.status(); s.Mode != backupOn || s.Reason != "" || s.Local.JournalBatches != 0 {
		t.Errorf("status %+v after the cut, want on", s)
	}
	r.run.stop()
	verifyRestore(t, r.dir, st, cw.records(), rigMeta)
}

// An entry of a batch that was not stored, which could not be deleted, is gone before
// the retry that takes its Seqs is journaled: the backup is on again at once, and a
// crash then goes on from the retry's entry and not the old one's.
func TestBackupJournalReplacesAnEntryItCouldNotDelete(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 1)
	sm.jrn.failDelete.Store(true)
	cw.fail.Store(true)
	if err := w.Write(t.Context(), nextBatch(t, g, st)); err == nil {
		t.Fatal("the injected failure was not returned")
	}
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Fatalf("status %+v, want degraded", s)
	}
	sm.jrn.failDelete.Store(false)
	cw.fail.Store(false)
	feedAt(t, w, g, st, 1)
	if s := r.run.status(); s.Mode != backupOn || s.Local.JournalBatches != 2 {
		t.Errorf("status %+v after the retry, want on with 2 journal batches", s)
	}
	// The marker stays until a cut passes the stale entry's last seq, so a crash before
	// it is not replayed; a trim through an earlier seq does not delete the retry's
	// entry as it would the old one's.
	r.run.trimJournal(t.Context(), 0)
	if got := journalNames(t, r.dir); !slices.Equal(got, []string{"1-20.parquet", "21-40.parquet"}) {
		t.Errorf("the journal holds %v after a trim, want both entries", got)
	}
	r2 := newRig(t, r.dir, st, rigMeta, false)
	if s := r2.run.status(); s.Mode != backupBroken || !strings.Contains(s.Reason, untrustedReason) {
		t.Fatalf("status %+v after a crash, want broken: %s", s, untrustedReason)
	}
	_ = cw
}

// A journal that cannot be written never fails the write: the request is answered,
// the backup says it is degraded, and the next cut that holds the batch puts it
// back to on.
func TestBackupJournalFailureNeverReachesTheWrite(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 1)

	sm.jrn.failPut.Store(true)
	feedAt(t, w, g, st, 1) // t.Fatal on a write that failed
	s := r.run.status()
	if s.Mode != backupDegraded || !strings.Contains(s.Reason, "not in the journal") || !strings.Contains(s.Reason, "injected put failure") {
		t.Fatalf("status %+v, want degraded: a batch is not in the journal", s)
	}
	if strings.Contains(s.Reason, r.dir) {
		t.Errorf("the reason %q names the data directory", s.Reason)
	}
	if got := strings.Count(r.logs.String(), "backup is degraded"); got != 1 {
		t.Errorf("degraded is logged %d times, want once", got)
	}
	feedAt(t, w, g, st, 1)
	if got := strings.Count(r.logs.String(), "backup is degraded"); got != 1 {
		t.Errorf("degraded is logged %d times after another batch, want once", got)
	}
	if s := r.run.status(); s.Mode != backupDegraded || s.Local.JournalBatches != 1 {
		t.Errorf("status %+v, want degraded with the one entry that was written", s)
	}

	// The disk is back, and a cut holds every batch: the journal is whole.
	sm.jrn.failPut.Store(false)
	feedAt(t, w, g, st, 1)
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Errorf("status %+v, want degraded until the cut", s)
	}
	r.run.cut(t.Context())
	if s := r.run.status(); s.Mode != backupOn || s.Reason != "" {
		t.Errorf("status %+v after the cut, want on", s)
	}
	if !strings.Contains(r.logs.String(), "backup is on again") {
		t.Errorf("the log does not say the backup is on again:\n%s", r.logs)
	}
	feedAt(t, w, g, st, 1)
	r.run.stop()
	verifyRestore(t, r.dir, st, cw.records(), rigMeta)
}

// A crash while the journal lacks a batch ends the backup, and the start says where
// the journal stops: with a manifest behind the store, and with none.
func TestBackupACrashWhileDegradedBreaksTheBackup(t *testing.T) {
	for _, tt := range []struct {
		name     string
		cutFirst bool
		want     string
		seq      uint64
	}{
		{"with a manifest", true, "ahead of its backup by seqs 21..60, and the journal does not hold them: no entry of the journal starts at seq 21", 20},
		{"with no manifest", false, "ahead of its backup by seqs 1..40, and the journal does not hold them: no entry of the journal starts at seq 21", 0},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sm := installFaultSeams(t)
			st := rigMemstore(t, rigMeta)
			r := newRig(t, "", st, rigMeta, false)
			w, g := r.run.writer(st, st), newGen(t)
			feedAt(t, w, g, st, 1)
			if tt.cutFirst {
				r.run.cut(t.Context())
			}
			sm.jrn.failPut.Store(true)
			feedAt(t, w, g, st, 1)
			sm.jrn.failPut.Store(false)
			if tt.cutFirst {
				feedAt(t, w, g, st, 1) // journaled, after the one that is missing
			}
			r2 := newRig(t, r.dir, st, rigMeta, false)
			s := r2.run.status()
			if s.Mode != backupBroken || !strings.Contains(s.Reason, tt.want) || r2.run.tee {
				t.Errorf("status %+v tee %v, want broken: %s", s, r2.run.tee, tt.want)
			}
			if tt.cutFirst && (s.Local == nil || s.Local.Seq != tt.seq) {
				t.Errorf("local status %+v, want the manifest at seq %d", s.Local, tt.seq)
			}
		})
	}
}

// A cut that does not hold a batch whose entry is missing leaves the backup
// degraded: only a cut through that batch makes the journal whole.
func TestBackupJournalGapIsClearedOnlyByACutThroughIt(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 1)
	r.run.cut(t.Context())
	sm.jrn.failPut.Store(true)
	feedAt(t, w, g, st, 1)
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Fatalf("status %+v, want degraded", s)
	}
	// The trim of a cut through the first batch only, which is what a cut that ran
	// before the second batch was added does, does not clear it.
	r.run.trimJournal(t.Context(), 20)
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Errorf("status %+v after a trim through seq 20, want degraded", s)
	}
	r.run.trimJournal(t.Context(), 40)
	if s := r.run.status(); s.Mode != backupOn {
		t.Errorf("status %+v after a trim through seq 40, want on", s)
	}
	r.run.stop()
}

// What the journal holds at start is read, not trusted: a hole, two entries for one
// Seq, an entry that overlaps another, a corrupt file and an entry of the wrong
// size each break the backup, with the reason, and nothing is deleted.
func TestBackupJournalAtStartBreaksOnWhatDoesNotFollow(t *testing.T) {
	setup := func(t *testing.T) (*memstore.Store, *crashWriter, *rig) {
		installFaultSeams(t)
		st := rigMemstore(t, rigMeta)
		cw := &crashWriter{rigStore: st}
		r := newRig(t, "", st, rigMeta, false)
		w, g := r.run.writer(st, cw), newGen(t)
		feedAt(t, w, g, st, 1)
		r.run.cut(t.Context())
		feedAt(t, w, g, st, 3) // seqs 21..80 in the journal
		return st, cw, r
	}
	jdir := func(r *rig) string { return filepath.Join(r.dir, backupDirName, backupJournalName) }
	copyEntry := func(t *testing.T, r *rig, from, to string) {
		t.Helper()
		b, err := os.ReadFile(filepath.Join(jdir(r), from))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(jdir(r), to), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for _, tt := range []struct {
		name   string
		damage func(t *testing.T, r *rig)
		want   string
	}{
		{"a hole", func(t *testing.T, r *rig) {
			if err := os.Remove(filepath.Join(jdir(r), "41-60.parquet")); err != nil {
				t.Fatal(err)
			}
		}, "no entry of the journal starts at seq 41"},
		{"the first entry missing", func(t *testing.T, r *rig) {
			if err := os.Remove(filepath.Join(jdir(r), "21-40.parquet")); err != nil {
				t.Fatal(err)
			}
		}, "no entry of the journal starts at seq 21"},
		{"two entries for one seq", func(t *testing.T, r *rig) {
			copyEntry(t, r, "41-60.parquet", "41-50.parquet")
		}, "two entries that start at seq 41"},
		{"an overlapping entry", func(t *testing.T, r *rig) {
			copyEntry(t, r, "41-60.parquet", "45-60.parquet")
		}, "overlaps the entry before it"},
		{"an entry that reaches past the store", func(t *testing.T, r *rig) {
			copyEntry(t, r, "61-80.parquet", "61-90.parquet")
			if err := os.Remove(filepath.Join(jdir(r), "61-80.parquet")); err != nil {
				t.Fatal(err)
			}
		}, "no entry of the journal starts at seq 61"},
		{"a corrupt file", func(t *testing.T, r *rig) {
			if err := os.WriteFile(filepath.Join(jdir(r), "41-60.parquet"), []byte("not parquet"), 0o600); err != nil {
				t.Fatal(err)
			}
		}, "41-60.parquet could not be added"},
		{"an entry of another size than its name", func(t *testing.T, r *rig) {
			copyEntry(t, r, "41-60.parquet", "41-70.parquet")
			if err := os.Remove(filepath.Join(jdir(r), "41-60.parquet")); err != nil {
				t.Fatal(err)
			}
			copyEntry(t, r, "61-80.parquet", "71-80.parquet")
			if err := os.Remove(filepath.Join(jdir(r), "61-80.parquet")); err != nil {
				t.Fatal(err)
			}
		}, "could not be added"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st, _, r := setup(t)
			tt.damage(t, r)
			before := journalNames(t, r.dir)
			r2 := newRig(t, r.dir, st, rigMeta, false)
			s := r2.run.status()
			if s.Mode != backupBroken || !strings.Contains(s.Reason, tt.want) || r2.run.tee {
				t.Errorf("status %+v tee %v, want broken: %s", s, r2.run.tee, tt.want)
			}
			if !strings.Contains(s.Reason, "ahead of its backup by seqs 21..80") && !strings.Contains(s.Reason, "could not be added") {
				t.Errorf("reason %q", s.Reason)
			}
			if got := journalNames(t, r.dir); !slices.Equal(got, before) {
				t.Errorf("the journal held %v and holds %v after a start that broke the backup", before, got)
			}
			if s.Local == nil || s.Local.Seq != 20 {
				t.Errorf("local status %+v, want the manifest at seq 20", s.Local)
			}
		})
	}
	t.Run("a file that is not an entry is left alone", func(t *testing.T) {
		st, cw, r := setup(t)
		if err := os.WriteFile(filepath.Join(jdir(r), "notes.txt"), []byte("keep"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(jdir(r), "021-40.parquet"), []byte("not a name"), 0o600); err != nil {
			t.Fatal(err)
		}
		r2 := newRig(t, r.dir, st, rigMeta, false)
		if s := r2.run.status(); s.Mode != backupOn || s.Local.JournalBatches != 3 {
			t.Fatalf("status %+v, want on with 3 journal batches", s)
		}
		r2.run.stop()
		if got := journalNames(t, r.dir); !slices.Equal(got, []string{"021-40.parquet", "notes.txt"}) {
			t.Errorf("the journal holds %v after the stop, want only the files that are not entries", got)
		}
		verifyRestore(t, r.dir, st, cw.records(), rigMeta)
	})
}

// The journal that covers a store that has records and no manifest yet is a backup
// that crashed before its first cut: it goes on. A journal that does not start at
// the first record is a backup that lost its first batch, which is broken; a store
// with no journal at all predates the backup.
func TestBackupJournalAndAStoreThatHasNoManifest(t *testing.T) {
	t.Run("the journal holds every record", func(t *testing.T) {
		installFaultSeams(t)
		st := rigMemstore(t, rigMeta)
		cw := &crashWriter{rigStore: st}
		r := newRig(t, "", st, rigMeta, false)
		feedAt(t, r.run.writer(st, cw), newGen(t), st, 2)
		r2 := newRig(t, r.dir, st, rigMeta, false)
		if s := r2.run.status(); s.Mode != backupOn || !r2.run.tee {
			t.Fatalf("status %+v, want on", s)
		}
		r2.run.stop()
		verifyRestore(t, r.dir, st, cw.records(), rigMeta)
	})
	t.Run("the journal does not start at the first record", func(t *testing.T) {
		installFaultSeams(t)
		st := rigMemstore(t, rigMeta)
		g := newGen(t)
		r := newRig(t, "", st, rigMeta, false)
		feed(t, st, g, 1) // a record the backup never saw
		// The backup breaks on the gap, but the batch is in the journal first.
		feedAt(t, r.run.writer(st, st), g, st, 1)
		if got := journalNames(t, r.dir); !slices.Equal(got, []string{"21-40.parquet"}) {
			t.Fatalf("the journal holds %v", got)
		}
		r2 := newRig(t, r.dir, st, rigMeta, false)
		// A journal was written for these records, so the backup did not start late: it
		// lost the first batch.
		if s := r2.run.status(); s.Mode != backupBroken || !strings.Contains(s.Reason, "no entry of the journal starts at seq 1") {
			t.Errorf("status %+v, want broken at seq 1", s)
		}
	})
	t.Run("an entry the store never stored is deleted", func(t *testing.T) {
		installFaultSeams(t)
		st := rigMemstore(t, rigMeta)
		cw := &crashWriter{rigStore: st}
		r := newRig(t, "", st, rigMeta, false)
		cw.before.Store(true)
		crashes(t, func() { feedAt(t, r.run.writer(st, cw), newGen(t), st, 1) })
		if got := journalNames(t, r.dir); !slices.Equal(got, []string{"1-20.parquet"}) {
			t.Fatalf("the journal holds %v at the crash", got)
		}
		r2 := newRig(t, r.dir, st, rigMeta, false)
		if s := r2.run.status(); s.Mode != backupOn || s.Local.JournalBatches != 0 {
			t.Errorf("status %+v, want on with an empty journal", s)
		}
		if got := journalNames(t, r.dir); len(got) != 0 {
			t.Errorf("the journal holds %v after the start, want nothing", got)
		}
		r2.run.stop()
	})
}

// A journal that cannot be opened does not stop the backup: it is degraded from the
// start, with the reason, and a crash ends it.
func TestBackupWithAJournalThatCannotBeOpened(t *testing.T) {
	setSeam(t, &openJournal, func(root string) (segment.Store, error) { return nil, errors.New("injected open failure") })
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, false)
	s := r.run.status()
	if s.Mode != backupDegraded || !strings.Contains(s.Reason, "the journal could not be opened") || !r.run.tee {
		t.Fatalf("status %+v tee %v, want degraded with the tee", s, r.run.tee)
	}
	cw := &crashWriter{rigStore: st}
	feedAt(t, r.run.writer(st, cw), newGen(t), st, 2)
	r.run.cut(t.Context())
	if s := r.run.status(); s.Mode != backupDegraded {
		t.Errorf("status %+v after a cut, want degraded still", s)
	}
	r.run.stop()
	verifyRestore(t, r.dir, st, cw.records(), rigMeta)
}

// Through serve: the status counts the journal's batches, and a cut and a stop empty
// it. With the backup off there is no journal.
func TestBackupJournalInServe(t *testing.T) {
	ticks := installTicks(t)
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	if st := getBackup(t, s.base); st.Mode != backupOn || st.Local == nil || st.Local.JournalBatches != 0 {
		t.Fatalf("before anything is added: %+v", st)
	}
	postOK(t, s.base, podBody("pod-1", time.Now()))
	postOK(t, s.base, podBody("pod-2", time.Now()))
	st := getBackup(t, s.base)
	if st.Local.JournalBatches < 2 {
		t.Errorf("%d journal batches after two requests, want at least 2", st.Local.JournalBatches)
	}
	if got := len(journalNames(t, dir)); got != st.Local.JournalBatches {
		t.Errorf("%d files in the journal, the status says %d", got, st.Local.JournalBatches)
	}
	if info, err := os.Stat(filepath.Join(dir, backupDirName, backupJournalName)); err != nil || info.Mode().Perm() != 0o700 {
		t.Errorf("the journal's directory: %v (%v), want mode 0700", info, err)
	}
	ticks.fire(t, 15*time.Minute)
	waitBackup(t, s, "a cut and an empty journal", func(b backupStatus) bool {
		return b.Local != nil && b.Local.Generation == 1 && b.Local.JournalBatches == 0
	})
	postOK(t, s.base, podBody("pod-3", time.Now()))
	stopOK(t, s)
	if got := journalNames(t, dir); len(got) != 0 {
		t.Errorf("the journal holds %v after the stop, want nothing", got)
	}
	if m, _ := stoppedBackup(t, dir); m.Seq != storedLastSeq(t, dir) {
		t.Errorf("the backup ends at seq %d, not at the store's last", m.Seq)
	}
}

// A stop whose last cut failed leaves the store ahead of the backup, and the journal
// holds the difference: the next run goes on from it, through serve, and its backup
// ends where the store does.
func TestBackupServeGoesOnFromTheJournal(t *testing.T) {
	ticks := installTicks(t)
	fail := &failingSegments{}
	setSeam(t, &openSegments, func(root string) (segment.Store, error) {
		d, err := localdir.New(root)
		fail.Store = d
		return fail, err
	})
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	postOK(t, s.base, podBody("pod-1", time.Now()))
	ticks.fire(t, 15*time.Minute)
	st := waitBackup(t, s, "a cut", func(b backupStatus) bool { return b.Local != nil && b.Local.Generation == 1 })
	first := st.Local.Seq
	postOK(t, s.base, podBody("pod-2", time.Now()))
	postOK(t, s.base, podBody("pod-3", time.Now()))
	fail.armed.Store(true)
	stopOK(t, s)
	if !strings.Contains(s.stderr.String(), "the last cut of the backup failed") {
		t.Fatalf("the stop's cut did not fail:\n%s", s.stderr)
	}
	last := storedLastSeq(t, dir)
	if m, _ := stoppedBackup(t, dir); m.Seq != first || last <= first {
		t.Fatalf("the manifest ends at seq %d, the first cut's was %d, the store's last is %d", m.Seq, first, last)
	}
	held := journalNames(t, dir)
	if len(held) == 0 {
		t.Fatal("the journal is empty after a cut that failed")
	}

	fail.armed.Store(false)
	s = startServe(t, dirEnv(t, dir), "--backup", "on", "--backup-every", "15m")
	waitIngestOn(t, s.base)
	st = getBackup(t, s.base)
	if st.Mode != backupOn || st.Local.Seq != first || st.Local.JournalBatches != len(held) {
		t.Errorf("status %+v, want on at seq %d with %d journal batches", st, first, len(held))
	}
	if !strings.Contains(s.stderr.String(), "from the journal") {
		t.Errorf("the log does not say the journal was used:\n%s", s.stderr)
	}
	postOK(t, s.base, podBody("pod-4", time.Now()))
	stopOK(t, s)
	if got := journalNames(t, dir); len(got) != 0 {
		t.Errorf("the journal holds %v after a clean stop", got)
	}
	if m, _ := stoppedBackup(t, dir); m.Seq != storedLastSeq(t, dir) || m.Seq <= last {
		t.Errorf("the backup ends at seq %d, the store at %d (was %d)", m.Seq, storedLastSeq(t, dir), last)
	}
}

func untrustedPath(dir string) string { return filepath.Join(dir, backupDirName, backupUntrustedName) }

// staleJournal brings a rig to the state in which the journal holds the entry of a
// batch that was not stored (seqs 21..40) and cannot delete it: the marker is
// written, and the delete keeps failing.
func staleJournal(t *testing.T) (*seams, *memstore.Store, *crashWriter, *rig, sequencer.Writer, *storetest.Generator) {
	t.Helper()
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	cw := &crashWriter{rigStore: st}
	r := newRig(t, "", st, rigMeta, false)
	w, g := r.run.writer(st, cw), newGen(t)
	feedAt(t, w, g, st, 1)
	sm.jrn.failDelete.Store(true)
	cw.fail.Store(true)
	if err := w.Write(t.Context(), nextBatch(t, g, st)); err == nil {
		t.Fatal("the injected failure was not returned")
	}
	cw.fail.Store(false)
	return sm, st, cw, r, w, g
}

// An entry of a batch that was not stored and could not be deleted marks the journal
// untrusted, in a file beside the journal, before anything else is done. If the
// retry that takes its Seqs cannot be journaled either (the entry cannot be deleted
// and so cannot be replaced), the entry is the only one for those Seqs and holds
// other records than the store's: a crash must not replay it. The start with the
// store ahead of the backup is broken and replays nothing; with the store at the
// backup it resumes, and deletes the entry and the marker.
func TestBackupJournalMarkedUntrustedIsNotReplayed(t *testing.T) {
	t.Run("a crash with the store ahead of the backup", func(t *testing.T) {
		sm, st, cw, r, w, g := staleJournal(t)
		b, err := os.ReadFile(untrustedPath(r.dir))
		if err != nil || !strings.Contains(string(b), "21-40.parquet") {
			t.Fatalf("the marker: %q (%v), want it to name the stale entry", b, err)
		}
		// The retry takes the same Seqs, and neither its entry nor the delete works.
		sm.jrn.failPut.Store(true)
		feedAt(t, w, g, st, 1)
		before := journalNames(t, r.dir)
		if !slices.Equal(before, []string{"1-20.parquet", "21-40.parquet"}) {
			t.Fatalf("the journal holds %v", before)
		}
		r2 := newRig(t, r.dir, st, rigMeta, false)
		s := r2.run.status()
		if s.Mode != backupBroken || !strings.Contains(s.Reason, untrustedReason) || r2.run.tee || r2.run.cutter != nil {
			t.Errorf("status %+v, want broken: %s, with nothing replayed", s, untrustedReason)
		}
		if got := journalNames(t, r.dir); !slices.Equal(got, before) {
			t.Errorf("the journal held %v and holds %v after the start", before, got)
		}
		if _, err := os.Stat(untrustedPath(r.dir)); err != nil {
			t.Errorf("the marker was removed by a start that did not trust the journal: %v", err)
		}
		_ = cw
	})
	t.Run("a crash with the store at the backup", func(t *testing.T) {
		sm, st, cw, r, w, g := staleJournal(t)
		sm.jrn.failPut.Store(true)
		feedAt(t, w, g, st, 1)
		sm.jrn.failPut.Store(false)
		r.run.cut(t.Context()) // holds seqs 1..40; the delete still fails
		if _, err := os.Stat(untrustedPath(r.dir)); err != nil {
			t.Fatalf("the marker is gone while the stale entry is: %v", err)
		}
		r2 := newRig(t, r.dir, st, rigMeta, false)
		if s := r2.run.status(); s.Mode != backupOn || !r2.run.tee || s.Local.Seq != st.LastSeq() {
			t.Fatalf("status %+v tee %v, want on at seq %d", s, r2.run.tee, st.LastSeq())
		}
		if got := journalNames(t, r.dir); len(got) != 0 {
			t.Errorf("the journal holds %v after the start, want nothing", got)
		}
		if _, err := os.Stat(untrustedPath(r.dir)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("the marker is still there after the entry was deleted: %v", err)
		}
		feedAt(t, r2.run.writer(st, cw), g, st, 1)
		r2.run.stop()
		verifyRestore(t, r.dir, st, cw.records(), rigMeta)
	})
	t.Run("a start that cannot delete the entry keeps the marker", func(t *testing.T) {
		_, st, _, r, _, _ := staleJournal(t)
		r.run.cut(t.Context()) // holds seqs 1..20; the stale entry is above them
		setSeam(t, &openJournal, func(root string) (segment.Store, error) {
			d, err := localdir.New(root)
			f := &faultStore{Store: d}
			f.failDelete.Store(true)
			return f, err
		})
		r2 := newRig(t, r.dir, st, rigMeta, false)
		if s := r2.run.status(); s.Mode != backupDegraded {
			t.Errorf("status %+v, want degraded: an entry that could not be deleted", s)
		}
		if _, err := os.Stat(untrustedPath(r.dir)); err != nil {
			t.Errorf("the marker was removed with the entry still there: %v", err)
		}
	})
}

// The marker is removed by the first cut that passes the stale entry's last Seq,
// once the entry is deleted, and by none before.
func TestBackupJournalUntrustedMarkerIsClearedByACoveringCut(t *testing.T) {
	sm, st, cw, r, w, g := staleJournal(t)
	has := func() bool { _, err := os.Stat(untrustedPath(r.dir)); return err == nil }
	if !has() {
		t.Fatal("no marker")
	}
	// The delete works again and the retry replaces the entry; the marker stays until
	// a cut.
	sm.jrn.failDelete.Store(false)
	feedAt(t, w, g, st, 1)
	if !has() {
		t.Error("the marker was removed before a cut")
	}
	r.run.trimJournal(t.Context(), 20)
	if !has() {
		t.Error("the marker was removed by a cut that does not reach the stale entry's last seq")
	}
	r.run.cut(t.Context())
	if has() {
		t.Error("the marker is still there after a cut through the stale entry's last seq")
	}
	if s := r.run.status(); s.Mode != backupOn {
		t.Errorf("status %+v, want on", s)
	}
	// While the entry cannot be deleted, no cut removes the marker.
	r.run.stop()
	verifyRestore(t, r.dir, st, cw.records(), rigMeta)

	sm2, st2, _, r2, _, g2 := staleJournal(t)
	feedAt(t, r2.run.writer(st2, &crashWriter{rigStore: st2}), g2, st2, 1)
	r2.run.cut(t.Context())
	if _, err := os.Stat(untrustedPath(r2.dir)); err != nil {
		t.Errorf("the marker was removed while the stale entry is still there: %v", err)
	}
	_ = sm2
}

// A marker that cannot be written leaves the backup degraded, with that reason, and
// is written at the next chance.
func TestBackupJournalMarkerWriteFailureIsDegraded(t *testing.T) {
	setSeam(t, &markUntrusted, func(path string, data []byte) error { return errors.New("injected marker failure") })
	_, st, _, r, w, g := staleJournal(t)
	s := r.run.status()
	if s.Mode != backupDegraded || !strings.Contains(s.Reason, "could not be marked untrusted") || !strings.Contains(s.Reason, "injected marker failure") {
		t.Fatalf("status %+v, want degraded: the marker could not be written", s)
	}
	if _, err := os.Stat(untrustedPath(r.dir)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("a marker exists: %v", err)
	}
	markUntrusted = func(path string, data []byte) error { return nil }
	feedAt(t, w, g, st, 1) // retries the marker
	if s := r.run.status(); s.Mode != backupDegraded || strings.Contains(s.Reason, "marked untrusted") {
		t.Errorf("status %+v, want degraded for the entry, not the marker", s)
	}
}

// A backup that stays degraded says so at every nag, like broken and refused.
func TestBackupDegradedIsNagged(t *testing.T) {
	sm := installFaultSeams(t)
	st := rigMemstore(t, rigMeta)
	r := newRig(t, "", st, rigMeta, false)
	sm.jrn.failPut.Store(true)
	feedAt(t, r.run.writer(st, st), newGen(t), st, 1)
	r.run.nag()
	if got := strings.Count(r.logs.String(), "backup is degraded"); got != 2 {
		t.Errorf("degraded is logged %d times after a nag, want 2:\n%s", got, r.logs)
	}
}
