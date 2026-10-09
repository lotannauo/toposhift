package pebblekv

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"slices"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/v2/sstable"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// canonicalDir is the directory, inside the database's, that [KV.Canonicalize]
// writes its tables to before they are ingested. Pebble ignores a directory it did
// not make.
const canonicalDir = "canonical.tmp"

// Canonicalize rewrites what the database holds into tables whose boundaries are a
// function of the data alone. [KV.CompactAll] leaves the bottom level in tables
// whose boundaries are wherever the compactions that wrote them happened to cut,
// which depends on how fast the writer ran against them, and Pebble never compacts
// the bottom level into itself; so two builds of the same data can leave different
// tables, and the reads of one cross other blocks than the reads of the other.
//
// It reads every live key and its value in key order, writes them with the
// database's own writer options for the bottom level (the layout's comparer, key
// schema, block-property collectors, block size, compression and table format) into
// tables cut at the first new key prefix once a table has reached the tuning's
// target file size, and ingests them in place of everything there was, in one
// atomic excise and ingest, so they all land in the bottom level as written. What
// the database answers does not change, and nothing deleted comes back: a deleted
// key is not live and is not written, and so neither is a tombstone, which with
// nothing under it deletes nothing. Pebble gives the ingested tables sequence
// numbers of its own, which nothing a layout stores depends on (a layout's own
// sequence number and horizon are keys, and are rewritten with the rest).
//
// It then checks what it did: the database holds exactly the tables written, all
// in the bottom level and none virtual, and reading everything back gives the same
// keys and values as before. It is meant for a database at rest that nothing
// writes to while it runs, after CompactAll; it needs the room for a second copy of
// the tables while it runs.
func (k *KV) Canonicalize(ctx context.Context) (err error) {
	if k.Config().ReadOnly {
		return fmt.Errorf("pebblekv: canonicalizing: %w", pebble.ErrReadOnly)
	}
	if err := k.Quiesce(ctx); err != nil { // flushes the memtable, and nothing compacts under the copy
		return err
	}
	lo, hi, err := k.tableSpan()
	if err != nil || lo == nil {
		return err
	}
	// An excise takes bare prefixes: from the prefix of the smallest key to just after
	// the prefix of the largest, which holds every table whole.
	opts := k.Options()
	cmp := opts.Comparer
	span := pebble.KeyRange{Start: slices.Clone(lo[:cmp.Split(lo)]), End: cmp.ImmediateSuccessor(nil, hi[:cmp.Split(hi)])}

	fs := opts.FS
	tmp := fs.PathJoin(k.Dir(), canonicalDir)
	if err := fs.RemoveAll(tmp); err != nil {
		return fmt.Errorf("pebblekv: canonicalizing: %w", err)
	}
	if err := fs.MkdirAll(tmp, 0o755); err != nil {
		return fmt.Errorf("pebblekv: canonicalizing: %w", err)
	}
	defer func() {
		if rerr := fs.RemoveAll(tmp); err == nil && rerr != nil {
			err = fmt.Errorf("pebblekv: canonicalizing: %w", rerr)
		}
	}()

	w, err := k.writeCanonical(ctx, fs, tmp)
	if err != nil {
		return err
	}
	if _, err := k.IngestAndExcise(ctx, w.paths, nil, nil, span); err != nil {
		return fmt.Errorf("pebblekv: canonicalizing: ingesting: %w", err)
	}
	if err := k.Quiesce(ctx); err != nil {
		return err
	}
	if err := k.checkCanonical(ctx, w); err != nil {
		return fmt.Errorf("pebblekv: canonicalizing: %w", err)
	}
	return k.awaitDeletions(ctx)
}

// canonicalTables is what [KV.writeCanonical] wrote: the files, in key order, the
// size of each, and the count and digest of the keys and values in them.
type canonicalTables struct {
	paths []string
	sizes []uint64
	keys  int64
	sum   []byte
}

// writeCanonical writes every live key and its value to tables under dir.
func (k *KV) writeCanonical(ctx context.Context, fs vfs.FS, dir string) (out canonicalTables, err error) {
	pebbleOpts := k.Options()
	opts := pebbleOpts.MakeWriterOptions(len(pebbleOpts.Levels)-1, k.TableFormat())
	target := uint64(max(k.Config().Tuning.TargetFileSize, 1))
	split := pebbleOpts.Comparer.Split

	var w *sstable.Writer
	finish := func() error {
		if w == nil {
			return nil
		}
		cw := w
		w = nil
		if err := cw.Close(); err != nil {
			return err
		}
		meta, err := cw.Metadata()
		if err != nil {
			return err
		}
		out.sizes = append(out.sizes, meta.Size)
		return nil
	}
	defer func() {
		if w != nil { // an error left a table open: close it, and its file, for the removal
			_ = w.Close()
		}
	}()

	sum := newKVDigest()
	var prefix []byte
	err = k.scan(ctx, func(key, value []byte) error {
		n := split(key)
		if w != nil && w.Raw().EstimatedSize() >= target && !bytes.Equal(key[:n], prefix) {
			if err := finish(); err != nil {
				return err
			}
		}
		if w == nil {
			path := fs.PathJoin(dir, fmt.Sprintf("%06d.sst", len(out.paths)))
			f, err := fs.Create(path, vfs.WriteCategoryUnspecified)
			if err != nil {
				return err
			}
			out.paths = append(out.paths, path)
			w = sstable.NewWriter(objstorageprovider.NewFileWritable(f), opts)
		}
		if err := w.Set(key, value); err != nil {
			return err
		}
		prefix = append(prefix[:0], key[:n]...)
		sum.add(key, value)
		return nil
	})
	if err == nil {
		err = finish()
	}
	if err != nil {
		return canonicalTables{}, fmt.Errorf("pebblekv: canonicalizing: writing tables: %w", err)
	}
	out.keys, out.sum = sum.n, sum.h.Sum(nil)
	return out, nil
}

// scan calls f with every live key and its value, in key order. It refuses a
// range key, which no layout writes and a copy of the points would drop.
func (k *KV) scan(ctx context.Context, f func(key, value []byte) error) (err error) {
	it, err := k.NewIterWithContext(ctx, &pebble.IterOptions{KeyTypes: pebble.IterKeyTypePointsAndRanges})
	if err != nil {
		return err
	}
	defer func() {
		if cerr := it.Close(); err == nil {
			err = cerr
		}
	}()
	n := 0
	for valid := it.First(); valid; valid = it.Next() {
		hasPoint, hasRange := it.HasPointAndRange()
		if hasRange {
			return fmt.Errorf("the database holds a range key at %q, which a copy of the points would drop", it.Key())
		}
		if !hasPoint {
			continue
		}
		v, err := it.ValueAndErr()
		if err != nil {
			return err
		}
		if err := f(it.Key(), v); err != nil {
			return err
		}
		if n++; n%4096 == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
	}
	return it.Error()
}

// kvDigest is a digest of keys and values in order, each length-prefixed.
type kvDigest struct {
	h hash.Hash
	n int64
	b [binary.MaxVarintLen64]byte
}

func newKVDigest() *kvDigest { return &kvDigest{h: sha256.New()} }

func (d *kvDigest) add(key, value []byte) {
	d.h.Write(d.b[:binary.PutUvarint(d.b[:], uint64(len(key)))])
	d.h.Write(key)
	d.h.Write(d.b[:binary.PutUvarint(d.b[:], uint64(len(value)))])
	d.h.Write(value)
	d.n++
}

// errNotCanonical is what [KV.checkCanonical] returns when the database is not
// what was ingested.
var errNotCanonical = errors.New("the database is not the tables written")

// checkCanonical makes sure the database holds the tables w, and nothing else, all
// in the bottom level and none virtual, and that they read back as the keys and
// values that were written.
func (k *KV) checkCanonical(ctx context.Context, w canonicalTables) error {
	levels, err := k.SSTables()
	if err != nil {
		return err
	}
	bottom := len(levels) - 1
	for l, level := range levels {
		if l != bottom && len(level) > 0 {
			return fmt.Errorf("%d tables in level %d: %w", len(level), l, errNotCanonical)
		}
	}
	got := levels[bottom]
	if len(got) != len(w.sizes) {
		return fmt.Errorf("%d tables in the bottom level, %d written: %w", len(got), len(w.sizes), errNotCanonical)
	}
	for i, t := range got {
		if t.Virtual || t.Size != w.sizes[i] {
			return fmt.Errorf("table %d of the bottom level is %d bytes (virtual %v), %d written: %w", i, t.Size, t.Virtual, w.sizes[i], errNotCanonical)
		}
	}
	sum := newKVDigest()
	if err := k.scan(ctx, func(key, value []byte) error { sum.add(key, value); return nil }); err != nil {
		return err
	}
	if sum.n != w.keys || !bytes.Equal(sum.h.Sum(nil), w.sum) {
		return fmt.Errorf("%d keys read back, %d written, digests equal %v: %w", sum.n, w.keys, bytes.Equal(sum.h.Sum(nil), w.sum), errNotCanonical)
	}
	return nil
}

// awaitDeletions waits until Pebble has deleted the tables the excise replaced, so
// the size of the directory measured next is the tables'.
func (k *KV) awaitDeletions(ctx context.Context) error {
	for {
		s := k.Snapshot()
		if s.ObsoleteBytes == 0 && s.ZombieBytes == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("pebblekv: canonicalizing: %d obsolete and %d zombie bytes not deleted: %w", s.ObsoleteBytes, s.ZombieBytes, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
}
