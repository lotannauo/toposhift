package main

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/segment/localdir"
	"github.com/lotannauo/toposhift/internal/store"
)

// backupJournalName is the directory of the journal in backup/, and
// backupUntrustedName the file beside it (not in it, so that it does not depend on
// the directory that just failed) that says the journal holds an entry of a batch
// the store did not store and could not delete. While it exists, a start does not
// replay the journal.
const (
	backupJournalName   = "journal"
	backupUntrustedName = "journal.untrusted"
)

// untrustedReason is why a journal marked untrusted is not replayed.
const untrustedReason = "the journal held an entry of a batch that was not stored and could not delete it"

// markUntrusted writes the marker, synced: the file, then its directory. A seam, for
// tests.
var markUntrusted = func(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return syncDirOf(path)
}

// syncDirOf flushes the directory that holds path.
func syncDirOf(path string) error {
	d, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// clearUntrusted removes the marker, synced.
func clearUntrusted(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	return syncDirOf(path)
}

// untrustedPresent says whether the marker exists. A marker that cannot be looked
// for counts as present.
func untrustedPresent(path string) bool {
	_, err := os.Stat(path)
	return !errors.Is(err, fs.ErrNotExist)
}

// maxJournalEntry bounds what the start reads of one journal entry.
const maxJournalEntry = segment.MaxSize

// openJournal opens the segment store of the journal: a seam, for tests.
var openJournal = func(root string) (segment.Store, error) { return localdir.New(root) }

// The journal holds each batch the sequencer hands the store, as one activity file,
// from before the store commits it until a manifest that holds it is stored. The
// file is synced, and its name too, before the store is asked to commit, so a
// batch the store committed is in the journal or in a manifest whatever happens to
// the process, and the backup does not end at a crash: the next start adds to the
// cutter what the journal holds beyond the last manifest. Only a backup that is on
// keeps one; nothing derived is in it, only the batch as it was given.
//
// An entry is named <first>-<last>.parquet by the Seqs of its batch. A batch the
// store did not store has its Seqs given to the next one, so the entry of the one
// that was not stored is deleted, and an entry that could not be deleted is
// deleted before an entry with a Seq at or below its last is put.
type journal struct {
	segs segment.Store
	log  *slog.Logger
	// marker is the path of the untrusted marker.
	marker string
	n      atomic.Int64 // len(entries), for the status

	mu sync.Mutex
	// entries are the entries the journal holds for batches the store stored or may
	// store, ascending by first Seq.
	entries []journalEntry
	// stale are entries of batches the store did not store, which could not be
	// deleted. They are tried again, and the journal is not whole while there is one.
	stale []journalEntry
	// gap is the last Seq of the highest batch the store stored and the journal does
	// not hold, 0 for none: a cut through it makes the journal whole again.
	gap    uint64
	gapErr string
	// staleMax is the highest last Seq of an entry made stale since the marker was
	// written, 0 for none. markerErr is why the marker could not be written, "" if it
	// could.
	staleMax  uint64
	markerErr string
}

// journalEntry is one file of the journal.
type journalEntry struct{ first, last uint64 }

func (e journalEntry) name() string { return fmt.Sprintf("%d-%d.parquet", e.first, e.last) }

// parseJournalName reads the name of an entry; a name that is not exactly what
// name returns is not one, and is left alone.
func parseJournalName(name string) (journalEntry, bool) {
	stem, ok := strings.CutSuffix(name, ".parquet")
	if !ok {
		return journalEntry{}, false
	}
	a, b, ok := strings.Cut(stem, "-")
	if !ok {
		return journalEntry{}, false
	}
	first, err1 := strconv.ParseUint(a, 10, 64)
	last, err2 := strconv.ParseUint(b, 10, 64)
	e := journalEntry{first, last}
	if err1 != nil || err2 != nil || first == 0 || last < first || e.name() != name {
		return journalEntry{}, false
	}
	return e, true
}

func newJournal(segs segment.Store, log *slog.Logger, marker string) *journal {
	return &journal{segs: segs, log: log, marker: marker}
}

// markStaleLocked makes e stale: the marker is written first, synced, with the
// ranges of every stale entry, since from now on the journal may hold an entry that
// is not the batch the store stored under its name. A marker that cannot be written
// is held in markerErr and tried again.
func (j *journal) markStaleLocked(e journalEntry) {
	j.stale = append(j.stale, e)
	j.staleMax = max(j.staleMax, e.last)
	j.writeMarkerLocked()
}

// writeMarkerLocked writes the marker for the stale entries.
func (j *journal) writeMarkerLocked() {
	var b strings.Builder
	for _, e := range j.stale {
		b.WriteString(e.name() + "\n")
	}
	if err := markUntrusted(j.marker, []byte(b.String())); err != nil {
		j.markerErr = err.Error()
		j.log.Error("the journal could not be marked untrusted", "err", err)
		return
	}
	j.markerErr = ""
}

// settleMarkerLocked retries a marker that could not be written, and removes the
// marker once no entry is stale and a cut has passed every stale range.
func (j *journal) settleMarkerLocked(cutSeq uint64) {
	switch {
	case len(j.stale) > 0:
		if j.markerErr != "" {
			j.writeMarkerLocked()
		}
	case j.staleMax != 0 && cutSeq >= j.staleMax:
		if err := clearUntrusted(j.marker); err != nil {
			j.log.Warn("the journal could not remove its untrusted marker; it is tried again at the next cut", "err", err)
			return
		}
		j.staleMax, j.markerErr = 0, ""
	}
}

// compareEntries orders entries by first Seq, then last.
func compareEntries(a, b journalEntry) int {
	return cmp.Or(cmp.Compare(a.first, b.first), cmp.Compare(a.last, b.last))
}

// count is the number of entries the journal holds.
func (j *journal) count() int {
	if j == nil {
		return 0
	}
	return int(j.n.Load())
}

// encodeJournalBatch is the batch as an activity file.
func encodeJournalBatch(batch []store.Record) ([]byte, error) {
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{})
	if err != nil {
		return nil, err
	}
	for _, r := range batch {
		if err := w.Write(r); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// handle is what put returns for a batch: its entry, and whether the journal
// holds it.
type journalHandle struct {
	e   journalEntry
	ok  bool
	err error
}

// put writes the entry of a batch, synced, before the store is asked to commit it.
// A failure is in the handle, and does not stop the batch.
func (j *journal) put(ctx context.Context, batch []store.Record) journalHandle {
	e := journalEntry{first: batch[0].Seq, last: batch[len(batch)-1].Seq}
	body, err := encodeJournalBatch(batch)
	if err != nil {
		return journalHandle{e: e, err: fmt.Errorf("encoding the batch: %w", err)}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.settleMarkerLocked(0)
	// Every entry that reaches a Seq the batch has is of a batch the store did not
	// store, since the store's last Seq is below the batch's first.
	if err := j.dropFromLocked(ctx, e.first); err != nil {
		return journalHandle{e: e, err: err}
	}
	if err := j.putLocked(ctx, e, body); err != nil {
		return journalHandle{e: e, err: err}
	}
	j.entries = append(j.entries, e)
	j.n.Store(int64(len(j.entries)))
	return journalHandle{e: e, ok: true}
}

// putLocked stores the entry. A name that exists is a leftover of a batch that was
// not stored: it is replaced.
func (j *journal) putLocked(ctx context.Context, e journalEntry, body []byte) error {
	put := func() error {
		_, err := j.segs.Put(ctx, e.name(), bytes.NewReader(body), int64(len(body)))
		return err
	}
	err := put()
	if errors.Is(err, segment.ErrExists) {
		if err = j.segs.Delete(ctx, e.name()); err == nil {
			err = put()
		}
	}
	if err != nil {
		// A Put whose outcome is not known may have left the entry: it is not held,
		// and must not stay to be read as another batch's.
		_ = j.segs.Delete(ctx, e.name())
		return fmt.Errorf("storing %s: %w", e.name(), err)
	}
	return nil
}

// dropFromLocked deletes every entry, and every stale one, whose last Seq is at or
// above first. An entry that cannot be deleted is stale, and is an error.
func (j *journal) dropFromLocked(ctx context.Context, first uint64) error {
	var failed error
	keepStale := j.stale[:0]
	for _, e := range j.stale {
		if e.last < first {
			keepStale = append(keepStale, e)
		} else if err := j.segs.Delete(ctx, e.name()); err != nil {
			failed = errors.Join(failed, err)
			keepStale = append(keepStale, e)
		}
	}
	j.stale = keepStale
	keep := j.entries[:0]
	for _, e := range j.entries {
		if e.last < first {
			keep = append(keep, e)
		} else if err := j.segs.Delete(ctx, e.name()); err != nil {
			failed = errors.Join(failed, err)
			j.markStaleLocked(e)
		}
	}
	j.entries = keep
	j.n.Store(int64(len(j.entries)))
	if failed != nil {
		return fmt.Errorf("deleting the entries of batches that were not stored: %w", failed)
	}
	return nil
}

// notStored deletes the entry of a batch the store did not store.
func (j *journal) notStored(ctx context.Context, h journalHandle) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if i := slices.Index(j.entries, h.e); i >= 0 {
		j.entries = slices.Delete(j.entries, i, i+1)
		j.n.Store(int64(len(j.entries)))
	}
	if err := j.segs.Delete(ctx, h.e.name()); err != nil {
		j.markStaleLocked(h.e)
		j.log.Warn("the journal could not delete the entry of a batch that was not stored", "entry", h.e.name(), "err", err)
	}
}

// missed notes a batch the store stored and the journal does not hold.
func (j *journal) missed(h journalHandle) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if h.e.last > j.gap {
		j.gap = h.e.last
	}
	if j.gapErr == "" && h.err != nil {
		j.gapErr = h.err.Error()
	}
}

// problem says why a crash now would end the backup, "" when it would not.
func (j *journal) problem() string {
	j.mu.Lock()
	defer j.mu.Unlock()
	switch {
	case j.markerErr != "":
		return "the journal could not be marked untrusted, and holds the entry of a batch that was not stored: " + j.markerErr
	case j.gap != 0:
		return fmt.Sprintf("a batch ending at seq %d is not in the journal: %s", j.gap, j.gapErr)
	case len(j.stale) > 0:
		return "the journal holds the entry of a batch that was not stored and could not delete it"
	}
	return ""
}

// through deletes the entries a manifest through seq holds. It is called once the
// manifest is stored, never before. An entry that cannot be deleted stays, and is
// deleted by the next call, or by the next start, which finds it at or below the
// manifest.
func (j *journal) through(ctx context.Context, seq uint64) {
	j.mu.Lock()
	var todo []journalEntry
	for _, e := range j.entries {
		if e.last <= seq {
			todo = append(todo, e)
		}
	}
	// The stale ones are deleted under the lock: the next batch may take a stale
	// entry's name.
	keepStale := j.stale[:0]
	for _, e := range j.stale {
		if err := j.segs.Delete(ctx, e.name()); err != nil {
			keepStale = append(keepStale, e)
		}
	}
	j.stale = keepStale
	j.settleMarkerLocked(seq)
	j.mu.Unlock()

	var done []journalEntry
	var failed error
	for _, e := range todo {
		if err := j.segs.Delete(ctx, e.name()); err != nil {
			failed = errors.Join(failed, err)
			continue
		}
		done = append(done, e)
	}
	j.mu.Lock()
	j.entries = slices.DeleteFunc(j.entries, func(e journalEntry) bool { return slices.Contains(done, e) })
	j.n.Store(int64(len(j.entries)))
	if j.gap <= seq {
		j.gap, j.gapErr = 0, ""
	}
	j.mu.Unlock()
	if failed != nil {
		j.log.Warn("the journal could not delete the entries a cut holds; they are deleted at the next cut", "err", failed)
	}
}

// journalPlan is what the journal holds at start, sorted by what the start does
// with it.
type journalPlan struct {
	// chain are the entries that cover (m, l] without a hole or an overlap, in
	// order.
	chain []journalEntry
	// below are entries a manifest holds already (last at or below m).
	below []journalEntry
	// above are entries of batches the store did not store (first above l), and bad
	// are entries that cannot be what the store stored (they reach across m or l).
	above, bad []journalEntry
	// reason is why the chain does not cover (m, l], "" when it does.
	reason string
	// held is the number of entries of batches the store could have stored: those
	// that do not lie above its last Seq. A journal with none was never written for
	// these records.
	held int
}

// plan reads the journal at start, for a manifest at m and a store at l, m <= l.
// It decides nothing about the store: the entries from m+1 to l must follow each
// other exactly, or the reason says where they do not.
func (j *journal) plan(ctx context.Context, m, l uint64) journalPlan {
	var p journalPlan
	var all []journalEntry
	for info, err := range j.segs.List(ctx, "") {
		if err != nil {
			p.reason = "the journal could not be listed: " + err.Error()
			return p
		}
		if e, ok := parseJournalName(info.Name); ok {
			all = append(all, e)
		}
	}
	var cand []journalEntry
	for _, e := range all {
		switch {
		case e.last <= m:
			p.below = append(p.below, e)
		case e.first > l:
			p.above = append(p.above, e)
		case e.first <= m || e.last > l:
			p.bad = append(p.bad, e)
		default:
			cand = append(cand, e)
		}
	}
	p.held = len(p.below) + len(p.bad) + len(cand)
	slices.SortFunc(cand, compareEntries)
	need, i := m+1, 0
	for need <= l {
		switch {
		case i < len(cand) && cand[i].first < need:
			p.reason = fmt.Sprintf("the journal holds an entry from seq %d that overlaps the entry before it", cand[i].first)
			return p
		case i >= len(cand) || cand[i].first != need:
			p.reason = fmt.Sprintf("no entry of the journal starts at seq %d", need)
			return p
		case i+1 < len(cand) && cand[i+1].first == need:
			p.reason = fmt.Sprintf("the journal holds two entries that start at seq %d", need)
			return p
		}
		p.chain = append(p.chain, cand[i])
		need = cand[i].last + 1
		i++
	}
	if i < len(cand) {
		p.reason = fmt.Sprintf("the journal holds an entry from seq %d that overlaps the entries before it", cand[i].first)
	}
	return p
}

// read returns the batch of an entry, checked against its name.
func (j *journal) read(ctx context.Context, e journalEntry) ([]store.Record, error) {
	rc, err := j.segs.Open(ctx, e.name())
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(io.LimitReader(rc, maxJournalEntry+1))
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	if len(b) > maxJournalEntry {
		return nil, errors.New("it is larger than a segment may be")
	}
	r, err := activity.NewReader(bytes.NewReader(b), int64(len(b)), activity.ReaderOptions{})
	if err != nil {
		return nil, err
	}
	defer func() { _ = r.Close() }()
	var recs []store.Record
	for {
		rec, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		recs = append(recs, rec)
	}
	if uint64(len(recs)) != e.last-e.first+1 || recs[0].Seq != e.first || recs[len(recs)-1].Seq != e.last {
		return nil, fmt.Errorf("it holds %d records and its name says seqs %d to %d", len(recs), e.first, e.last)
	}
	return recs, nil
}

// adopt takes the plan's chain as the entries the journal holds, and deletes what
// the plan found that no start needs. An entry that cannot be deleted is kept to be
// deleted again: one a manifest holds with the entries, any other as stale.
func (j *journal) adopt(ctx context.Context, p journalPlan) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.entries = slices.Clone(p.chain)
	for _, e := range slices.Concat(p.below, p.above, p.bad) {
		if err := j.segs.Delete(ctx, e.name()); err != nil {
			j.log.Warn("the journal could not delete an entry no start needs", "entry", e.name(), "err", err)
			if slices.Contains(p.below, e) {
				j.entries = append(j.entries, e)
			} else {
				j.markStaleLocked(e)
			}
		}
	}
	// A start that found the marker and resumed has deleted the entries that made
	// the journal untrusted, unless one is stale again.
	if len(j.stale) == 0 && untrustedPresent(j.marker) {
		if err := clearUntrusted(j.marker); err != nil {
			j.log.Warn("the journal could not remove its untrusted marker; it is tried again at the next cut", "err", err)
			j.staleMax = max(j.staleMax, 1) // removed at the next cut
		}
	}
	slices.SortFunc(j.entries, compareEntries)
	j.n.Store(int64(len(j.entries)))
}
