package rockskv

import (
	"bytes"
	"runtime"

	"github.com/linxGnu/grocksdb"
)

// IterOptions are the bounds an iterator is made with: keys in [LowerBound,
// UpperBound), nil being unbounded, as Pebble's.
type IterOptions struct {
	LowerBound, UpperBound []byte
}

// Iterator is one RocksDB iterator with Pebble's iterator's shape. It is one view
// of the database, RocksDB's implicit snapshot taken when it was made, whatever
// bounds it is given later: a read that walks several prefixes on one iterator
// sees whole commits, as it does on one Pebble iterator.
//
// RocksDB cannot change an iterator's bounds once it is made, so the bounds are
// RocksDB's own only when they are given at the making ([KV.NewIter] with
// options), and are checked here in Go in every case: [Iterator.SetBounds]
// records them, First seeks to the lower bound, SeekGE seeks to the larger of its
// key and the lower bound (Pebble clamps a seek to its lower bound in the same
// way), and the iterator is invalid from the first key at or after the upper
// bound.
//
// Key and Value return memory RocksDB owns, valid until the iterator moves or is
// closed, as Pebble's do: a caller that keeps one copies it.
//
// The seeks and steps are counted as Pebble counts the calls made to its iterator:
// First and SeekGE are seeks and every Next is a step.
type Iterator struct {
	it *grocksdb.Iterator
	ro *grocksdb.ReadOptions
	// lo and hi are the bounds checked in Go. native holds the bounds given to
	// RocksDB, which keeps a pointer to them while the iterator lives: they are
	// never changed or reused until it is closed.
	lo, hi []byte
	native [2][]byte
	key    []byte
	valid  bool

	seeks, steps int64

	// perf is the thread's perf context of a read made by [KV.NewReadIter] with
	// [Config.Perf]; nil otherwise.
	perf *grocksdb.PerfContext
}

// NewIter makes an iterator over the database as it is now. With bounds, they are
// given to RocksDB as well as checked in Go.
func (k *KV) NewIter(o *IterOptions) (*Iterator, error) {
	return k.newIter(o, false), nil
}

// NewReadIter is [KV.NewIter] for a read whose costs [RecordIter] reports: with
// [Config.Perf] the read is locked to its thread from here to [Iterator.Close],
// and RocksDB's perf context counts what the read does on that thread.
func (k *KV) NewReadIter(o *IterOptions) (*Iterator, error) {
	return k.newIter(o, k.cfg.Perf), nil
}

func (k *KV) newIter(o *IterOptions, perf bool) *Iterator {
	i := &Iterator{ro: grocksdb.NewDefaultReadOptions()}
	if o != nil {
		i.lo, i.hi = o.LowerBound, o.UpperBound
		if o.LowerBound != nil {
			i.native[0] = bytes.Clone(o.LowerBound)
			i.ro.SetIterateLowerBound(i.native[0])
		}
		if o.UpperBound != nil {
			i.native[1] = bytes.Clone(o.UpperBound)
			i.ro.SetIterateUpperBound(i.native[1])
		}
	}
	if perf {
		runtime.LockOSThread()
		grocksdb.SetPerfLevel(grocksdb.KEnableCount)
		i.perf = grocksdb.NewPerfContext()
		i.perf.Reset()
	}
	i.it = k.db.NewIterator(i.ro)
	return i
}

// SetBounds sets the bounds the iterator is checked against, and leaves it
// unpositioned, as Pebble's does. The slices are kept, not copied, as Pebble keeps
// them: the caller does not change them while they are the bounds.
func (i *Iterator) SetBounds(lo, hi []byte) {
	i.lo, i.hi = lo, hi
	i.valid, i.key = false, nil
}

// First moves to the first key at or after the lower bound.
func (i *Iterator) First() bool {
	i.seeks++
	if i.lo != nil {
		i.it.Seek(i.lo)
	} else {
		i.it.SeekToFirst()
	}
	return i.check()
}

// SeekGE moves to the first key at or after key, and not before the lower bound.
func (i *Iterator) SeekGE(key []byte) bool {
	i.seeks++
	if i.lo != nil && bytes.Compare(key, i.lo) < 0 {
		key = i.lo
	}
	if i.hi != nil && bytes.Compare(key, i.hi) >= 0 {
		i.valid, i.key = false, nil
		return false
	}
	i.it.Seek(key)
	return i.check()
}

// Next moves to the next key.
func (i *Iterator) Next() bool {
	i.steps++
	i.it.Next()
	return i.check()
}

// check reads the key RocksDB is at and holds it to the upper bound.
func (i *Iterator) check() bool {
	if !i.it.Valid() {
		i.valid, i.key = false, nil
		return false
	}
	key := i.it.KeySlice().Data()
	if i.hi != nil && bytes.Compare(key, i.hi) >= 0 {
		i.valid, i.key = false, nil
		return false
	}
	i.valid, i.key = true, key
	return true
}

// Valid says whether the iterator is at a key.
func (i *Iterator) Valid() bool { return i.valid }

// Key is the key the iterator is at, or nil.
func (i *Iterator) Key() []byte { return i.key }

// Value is the value of the key the iterator is at, or nil.
func (i *Iterator) Value() []byte {
	if !i.valid {
		return nil
	}
	return i.it.ValueSlice().Data()
}

// Error is the error the iterator met, if any.
func (i *Iterator) Error() error { return i.it.Err() }

// Close releases the iterator and, for a read with the perf context, its thread.
func (i *Iterator) Close() error {
	err := i.it.Err()
	i.it.Close()
	i.ro.Destroy()
	i.valid, i.key = false, nil
	if i.perf != nil {
		i.perf.Destroy()
		i.perf = nil
		grocksdb.SetPerfLevel(grocksdb.KDisable)
		runtime.UnlockOSThread()
	}
	return err
}
