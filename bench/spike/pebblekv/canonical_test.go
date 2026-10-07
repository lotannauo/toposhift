package pebblekv_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/cockroachkvs"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// canonLayouts are the two layouts' Pebble settings, each with everything its
// tables can be written with (crdb1 and the time-interval collector for M), and a
// key of theirs: prefix p, version v (0 is none).
var canonLayouts = map[string]struct {
	layout pebblekv.Layout
	cfg    pebblekv.Config
	key    func(p, v int) []byte
}{
	"M": {pebblekv.CockroachLayout, pebblekv.Config{Schema: pebblekv.SchemaCRDB, TimeFilter: true}, func(p, v int) []byte {
		root := fmt.Appendf(nil, "p%05d", p)
		if v == 0 {
			return cockroachkvs.EncodeKey(nil, root, nil)
		}
		return cockroachkvs.EncodeMVCCKey(nil, root, uint64(v)*1e9, 0)
	}},
	"L": {pebblekv.BytewiseLayout, pebblekv.Config{Schema: pebblekv.SchemaDefault}, func(p, v int) []byte {
		return fmt.Appendf(nil, "p%05d/%04d", p, v)
	}},
}

// canonValue is the value of key (p, v) in its round r: of a length that varies,
// so some are long enough to be stored apart from their key.
func canonValue(p, v, r int) []byte {
	b := make([]byte, 20+(p*37+v*11+r)%300)
	for i := range b {
		b[i] = byte(p + v*3 + r + i)
	}
	return b
}

func openCanon(t *testing.T, name string, fs vfs.FS, cfg pebblekv.Config) *pebblekv.KV {
	t.Helper()
	l := canonLayouts[name]
	c := l.cfg
	c.FS, c.Tuning = fs, pebblekv.TinyTuning()
	c.DisableAutoCompactions, c.ReadOnly = cfg.DisableAutoCompactions, cfg.ReadOnly
	kv, err := pebblekv.Open("db", l.layout, c)
	if err != nil {
		t.Fatal(err)
	}
	return kv
}

// writeCanon writes the same data whichever way it is asked to: prefixes of a few
// versions each, written in rounds that overwrite and delete some, then a range of
// prefixes deleted. perCommit is how many keys a batch holds, flushEvery how many
// batches go between flushes (0: none), and reverse writes the keys of a round in
// the opposite order. With resets, a deleted key can be set again in a later round.
func writeCanon(t *testing.T, kv *pebblekv.KV, key func(p, v int) []byte, perCommit, flushEvery int, reverse, settle, resets bool) {
	t.Helper()
	const prefixes, versions, rounds = 300, 4, 3
	type op struct {
		k, v []byte
		del  bool
	}
	b := kv.NewBatch()
	n, commits := 0, 0
	commit := func() {
		if err := b.Commit(kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		b = kv.NewBatch()
		n = 0
		if commits++; flushEvery > 0 && commits%flushEvery == 0 {
			if err := kv.Flush(); err != nil {
				t.Fatal(err)
			}
			if settle {
				if err := kv.Settle(); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for r := range rounds {
		var ops []op
		for p := range prefixes {
			for v := range versions {
				del := r > 0 && (p+v+r)%7 == 0
				if !resets && r < rounds-1 {
					del = false
				}
				ops = append(ops, op{k: key(p, v), v: canonValue(p, v, r), del: del})
			}
		}
		if reverse {
			slices.Reverse(ops)
		}
		for _, o := range ops {
			var err error
			if o.del {
				err = b.Delete(o.k, nil)
			} else {
				err = b.Set(o.k, o.v, nil)
			}
			if err != nil {
				t.Fatal(err)
			}
			if n++; n == perCommit {
				commit()
			}
		}
	}
	if err := b.DeleteRange(key(100, 0), key(140, 0), nil); err != nil {
		t.Fatal(err)
	}
	commit()
}

type kvPair struct{ k, v string }

func everything(t *testing.T, kv *pebblekv.KV) []kvPair {
	t.Helper()
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	var out []kvPair
	for ok := it.First(); ok; ok = it.Next() {
		out = append(out, kvPair{string(it.Key()), string(it.Value())})
	}
	return out
}

// tableShape is a table as a read sees it: its level, its bounds and its bytes.
type tableShape struct {
	level           int
	smallest, large string
	size            uint64
	body            string
}

func shape(t *testing.T, kv *pebblekv.KV, fs vfs.FS) []tableShape {
	t.Helper()
	levels, err := kv.SSTables()
	if err != nil {
		t.Fatal(err)
	}
	var out []tableShape
	for l, level := range levels {
		for _, tb := range level {
			f, err := fs.Open(fs.PathJoin("db", fmt.Sprintf("%06d.sst", tb.FileNum)))
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(f)
			_ = f.Close()
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, tableShape{l, string(tb.Smallest.UserKey), string(tb.Largest.UserKey), tb.Size, string(body)})
		}
	}
	return out
}

func sameShape(a, b []tableShape) bool { return slices.Equal(a, b) }

func describeTables(t *testing.T, kv *pebblekv.KV) [2]string {
	t.Helper()
	d, err := kv.Describe()
	if err != nil {
		t.Fatal(err)
	}
	return [2]string{d["key_schema_in_tables"], d["collectors_in_tables"]}
}

// Canonicalizing changes nothing a read can see, keeps deleted keys deleted (points
// and a range), drops the tombstones, which then delete nothing, and leaves every
// table in the bottom level, written with the layout's key schema and collectors,
// none bigger than it has to be. It survives a reopening.
func TestCanonicalizeKeepsWhatTheDatabaseHolds(t *testing.T) {
	t.Parallel()
	for name, l := range canonLayouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			fs := vfs.NewMem()
			kv := openCanon(t, name, fs, pebblekv.Config{DisableAutoCompactions: true})
			defer func() { _ = kv.Close() }()
			writeCanon(t, kv, l.key, 97, 3, false, false, true)
			if err := kv.Quiesce(ctx); err != nil {
				t.Fatal(err)
			}
			before := everything(t, kv)
			desc := describeTables(t, kv)
			s := kv.Snapshot()
			if s.TombstoneCount == 0 || tables(s) == s.TablesPerLevel[6] {
				t.Fatalf("nothing to show: %d tombstones, tables per level %v", s.TombstoneCount, s.TablesPerLevel)
			}
			if err := kv.Canonicalize(ctx); err != nil {
				t.Fatal(err)
			}
			if after := everything(t, kv); !slices.Equal(before, after) {
				t.Fatalf("the database reads otherwise: %d keys before, %d after", len(before), len(after))
			}
			for _, gone := range [][]byte{l.key(120, 1), l.key(5, 0)} { // in the deleted range; deleted in the last round
				if _, closer, err := kv.Get(gone); !errors.Is(err, pebble.ErrNotFound) {
					if closer != nil {
						_ = closer.Close()
					}
					t.Errorf("a deleted key came back: %v", err)
				}
			}
			s = kv.Snapshot()
			if s.TombstoneCount != 0 || tables(s) != s.TablesPerLevel[6] || s.TablesPerLevel[6] < 2 {
				t.Errorf("after: %d tombstones, tables per level %v", s.TombstoneCount, s.TablesPerLevel)
			}
			if s.ObsoleteBytes != 0 || s.ZombieBytes != 0 || !s.StatsComplete {
				t.Errorf("returned before the replaced tables were deleted or the statistics loaded: %+v", s)
			}
			if got := describeTables(t, kv); got != desc {
				t.Errorf("tables written with %v, before with %v", got, desc)
			}
			target := uint64(pebblekv.TinyTuning().TargetFileSize)
			levels, err := kv.SSTables()
			if err != nil {
				t.Fatal(err)
			}
			cmp := l.layout.Comparer
			for i, tb := range levels[6] {
				if i > 0 { // the versions of a key are never split across tables
					prev, first := levels[6][i-1].Largest.UserKey, tb.Smallest.UserKey
					if bytes.Equal(prev[:cmp.Split(prev)], first[:cmp.Split(first)]) {
						t.Errorf("tables %d and %d share the prefix %q", i-1, i, first[:cmp.Split(first)])
					}
				}
				if i < len(levels[6])-1 && tb.Size < target/2 {
					t.Errorf("table %d of %d is %d bytes, under half the target %d", i, len(levels[6]), tb.Size, target)
				}
				if tb.Size > 3*target {
					t.Errorf("table %d is %d bytes, the target is %d", i, tb.Size, target)
				}
			}
			if entries, err := fs.List("db"); err != nil || slices.Contains(entries, "canonical.tmp") {
				t.Errorf("the tables written are left behind: %v %v", entries, err)
			}
			if err := kv.CloseClean(); err != nil {
				t.Fatal(err)
			}
			ro := openCanon(t, name, fs, pebblekv.Config{ReadOnly: true})
			defer func() { _ = ro.Close() }()
			if after := everything(t, ro); !slices.Equal(before, after) {
				t.Errorf("reopened, it reads otherwise: %d keys before, %d after", len(before), len(after))
			}
		})
	}
}

// What a read does in the logical sense (the points and versions it steps over, the
// bytes of their keys and values) is the same after canonicalizing a compacted
// database as before.
func TestCanonicalizeKeepsWhatAReadSteps(t *testing.T) {
	t.Parallel()
	for name, l := range canonLayouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			kv := openCanon(t, name, vfs.NewMem(), pebblekv.Config{})
			defer func() { _ = kv.Close() }()
			writeCanon(t, kv, l.key, 97, 3, false, false, false)
			if err := kv.CompactAll(ctx); err != nil {
				t.Fatal(err)
			}
			reads := func() map[string]int64 {
				rec := &sums{m: map[string]int64{}}
				for p := 0; p < 300; p += 13 {
					it, err := kv.NewIter(nil)
					if err != nil {
						t.Fatal(err)
					}
					n := 0
					for ok := it.SeekGE(l.key(p, 0)); ok && n < 9; ok = it.Next() {
						n++
					}
					pebblekv.RecordIter(rec, "probe", it)
					if err := it.Close(); err != nil {
						t.Fatal(err)
					}
				}
				scan(t, kv, rec, "scan")
				out := map[string]int64{}
				for k, v := range rec.m {
					// The blocks are what changes. So does where a value is kept: in place
					// (value_bytes counts it whole) or in a value block (value_bytes counts its
					// handle, separated_values the value), which depends on the table a key is
					// in (the first key of a table keeps its value) and on whether a compaction
					// ever met a set over a deletion of the same key (Pebble then keeps it in
					// place): both are the shape of the tables, not what a read steps over.
					if !strings.Contains(k, "block_") && !strings.Contains(k, "separated_value") && !strings.HasSuffix(k, ".value_bytes") {
						out[k] = v
					}
				}
				return out
			}
			before := reads()
			if err := kv.Canonicalize(ctx); err != nil {
				t.Fatal(err)
			}
			if after := reads(); !maps.Equal(before, after) {
				t.Errorf("reads step otherwise:\n%v\n%v", before, after)
			}
		})
	}
}

// sums is a recorder that adds up every count and sample under its name.
type sums struct{ m map[string]int64 }

func (s *sums) Count(name string, n int64)  { s.m[name] += n }
func (s *sums) Sample(name string, v int64) { s.m[name] += v }

var _ engine.Recorder = (*sums)(nil)

// The same data, written slowly in small flushes with the compactions keeping up or
// quickly in large batches with none until the end, is left by CompactAll in tables
// cut in different places; canonicalized, in the same tables to the byte.
func TestCanonicalTablesDoNotDependOnHowTheDataWasWritten(t *testing.T) {
	t.Parallel()
	for name, l := range canonLayouts {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			build := func(slow bool) (*pebblekv.KV, vfs.FS) {
				fs := vfs.NewMem()
				kv := openCanon(t, name, fs, pebblekv.Config{DisableAutoCompactions: !slow})
				t.Cleanup(func() { _ = kv.Close() })
				if slow {
					writeCanon(t, kv, l.key, 41, 1, false, true, true)
				} else {
					writeCanon(t, kv, l.key, 1000, 0, true, false, true)
				}
				if err := kv.CompactAll(ctx); err != nil {
					t.Fatal(err)
				}
				return kv, fs
			}
			slow, slowFS := build(true)
			fast, fastFS := build(false)
			if !slices.Equal(everything(t, slow), everything(t, fast)) {
				t.Fatal("the two ways wrote different data")
			}
			if sameShape(shape(t, slow, slowFS), shape(t, fast, fastFS)) {
				t.Fatal("CompactAll left the same tables either way: the comparison shows nothing")
			}
			for _, kv := range []*pebblekv.KV{slow, fast} {
				if err := kv.Canonicalize(ctx); err != nil {
					t.Fatal(err)
				}
			}
			a, b := shape(t, slow, slowFS), shape(t, fast, fastFS)
			if !sameShape(a, b) {
				t.Errorf("canonical tables differ: %d and %d tables, live bytes %d and %d", len(a), len(b), slow.Snapshot().LiveTableBytes, fast.Snapshot().LiveTableBytes)
			}
			if len(a) < 2 {
				t.Errorf("%d tables: the comparison shows little", len(a))
			}
		})
	}
}

// slowRemoveFS takes its time to remove a table, as a disk busy with other work
// does.
type slowRemoveFS struct{ vfs.FS }

func (f slowRemoveFS) Remove(name string) error {
	if strings.HasSuffix(name, ".sst") {
		time.Sleep(50 * time.Millisecond)
	}
	return f.FS.Remove(name)
}

// Canonicalizing returns once the tables it replaced are gone from the disk, so the
// size measured next is the new tables'.
func TestCanonicalizeWaitsForTheReplacedTablesToGo(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	kv := openCanon(t, "L", slowRemoveFS{vfs.NewMem()}, pebblekv.Config{})
	defer func() { _ = kv.Close() }()
	writeCanon(t, kv, canonLayouts["L"].key, 97, 3, false, false, true)
	if err := kv.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if err := kv.Canonicalize(ctx); err != nil {
		t.Fatal(err)
	}
	if s := kv.Snapshot(); s.ObsoleteBytes != 0 || s.ZombieBytes != 0 {
		t.Errorf("returned with %d obsolete and %d zombie bytes on disk", s.ObsoleteBytes, s.ZombieBytes)
	}
}

// failingFS fails the creation of the files Canonicalize writes once fail is
// reached.
type failingFS struct {
	vfs.FS
	fail, made int
}

func (f *failingFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	if strings.Contains(name, "canonical.tmp") {
		if f.made++; f.made >= f.fail {
			return nil, errors.New("injected")
		}
	}
	return f.FS.Create(name, c)
}

// A canonicalization that fails, or is stopped, leaves the database as it was and
// nothing of its own behind; one of an empty database does nothing; a read-only
// database is refused.
func TestCanonicalizeFailsWithoutHarm(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	l := canonLayouts["L"]

	fs := &failingFS{FS: vfs.NewMem(), fail: 3}
	kv := openCanon(t, "L", fs, pebblekv.Config{})
	defer func() { _ = kv.Close() }()
	writeCanon(t, kv, l.key, 97, 3, false, false, true)
	if err := kv.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	before, tablesBefore := everything(t, kv), shape(t, kv, fs)
	if err := kv.Canonicalize(ctx); err == nil || !strings.Contains(err.Error(), "injected") {
		t.Fatalf("a table that could not be written: %v", err)
	}
	stopped, stop := context.WithCancel(ctx)
	stop()
	if err := kv.Canonicalize(stopped); err == nil {
		t.Error("a stopped canonicalization says it finished")
	}
	if !slices.Equal(before, everything(t, kv)) || !sameShape(tablesBefore, shape(t, kv, fs)) {
		t.Error("a failed canonicalization changed the database")
	}
	if entries, err := fs.List("db"); err != nil || slices.Contains(entries, "canonical.tmp") {
		t.Errorf("a failed canonicalization left its tables behind: %v %v", entries, err)
	}

	// A range key, which no layout writes, is refused, not dropped.
	rk := openCanon(t, "L", vfs.NewMem(), pebblekv.Config{})
	defer func() { _ = rk.Close() }()
	if err := rk.Set([]byte("a"), []byte("v"), rk.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	if err := rk.RangeKeySet([]byte("b"), []byte("c"), nil, []byte("v"), rk.WriteOptions()); err != nil {
		t.Fatal(err)
	}
	if err := rk.Canonicalize(ctx); err == nil || !strings.Contains(err.Error(), "range key") {
		t.Errorf("a range key: %v", err)
	}

	empty := openCanon(t, "M", vfs.NewMem(), pebblekv.Config{})
	defer func() { _ = empty.Close() }()
	if err := empty.Canonicalize(ctx); err != nil || tables(empty.Snapshot()) != 0 {
		t.Errorf("an empty database: %v, %v", err, empty.Snapshot().TablesPerLevel)
	}

	mem := vfs.NewMem()
	w := openCanon(t, "M", mem, pebblekv.Config{})
	writeCanon(t, w, canonLayouts["M"].key, 500, 0, false, false, true)
	if err := w.CloseClean(); err != nil {
		t.Fatal(err)
	}
	ro := openCanon(t, "M", mem, pebblekv.Config{ReadOnly: true})
	defer func() { _ = ro.Close() }()
	if err := ro.Canonicalize(ctx); !errors.Is(err, pebble.ErrReadOnly) {
		t.Errorf("a read-only database: %v", err)
	}
}
