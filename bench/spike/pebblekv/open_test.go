package pebblekv_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/cockroachkvs"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// A database written under these names cannot be opened under others, so they
// are pinned here: a Pebble upgrade that renamed one would fail this test
// instead of failing on an existing database.
func TestPersistedNamesArePinned(t *testing.T) {
	t.Parallel()
	if got := cockroachkvs.Comparer.Name; got != "cockroach_comparator" {
		t.Errorf("comparer name = %q", got)
	}
	if got := cockroachkvs.KeySchema.Name; got != "crdb1" {
		t.Errorf("key schema name = %q", got)
	}
	if got := cockroachkvs.NewMVCCTimeIntervalFilter(0, 1).Name(); got != "MVCCTimeInterval" {
		t.Errorf("block property collector name = %q", got)
	}
	if got := pebblekv.SchemaCRDB.String(); got != "crdb1" {
		t.Errorf("SchemaCRDB = %q", got)
	}
}

func configs() map[string]pebblekv.Config {
	tiny := pebblekv.TinyTuning()
	return map[string]pebblekv.Config{
		"crdb1":               {Schema: pebblekv.SchemaCRDB, Tuning: tiny},
		"crdb1 with filter":   {Schema: pebblekv.SchemaCRDB, TimeFilter: true, Tuning: tiny},
		"default":             {Schema: pebblekv.SchemaDefault, Tuning: tiny},
		"default with filter": {Schema: pebblekv.SchemaDefault, TimeFilter: true, Tuning: tiny},
		"crdb1 with sync":     {Schema: pebblekv.SchemaCRDB, Sync: true, Tuning: tiny},
		"bench settings":      {Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.BenchTuning()},
		"no tuning given":     {Schema: pebblekv.SchemaCRDB},
	}
}

func metaKey(name string) []byte { return cockroachkvs.EncodeKey(nil, append([]byte{0}, name...), nil) }

// Every configuration opens, stores versioned keys in the order the comparer
// defines (newest first), and comes back with everything after being closed,
// through the log and through tables.
func TestOpenWriteReadReopen(t *testing.T) {
	t.Parallel()
	for name, cfg := range configs() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			kv, err := pebblekv.Open(dir, pebblekv.CockroachLayout, cfg)
			if err != nil {
				t.Fatal(err)
			}
			key := func(wall uint64, logical uint32) []byte {
				return cockroachkvs.EncodeMVCCKey(nil, []byte("\x01key"), wall, logical)
			}
			for _, w := range []uint64{5, 9, 7} {
				for _, l := range []uint32{1, 3, 2} {
					if err := kv.Set(key(w, l), []byte{byte(w), byte(l)}, kv.WriteOptions()); err != nil {
						t.Fatal(err)
					}
				}
			}
			check := func(kv *pebblekv.KV) {
				t.Helper()
				it, err := kv.NewIter(nil)
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = it.Close() }()
				var got [][]byte
				for ok := it.First(); ok; ok = it.Next() {
					got = append(got, bytes.Clone(it.Value()))
				}
				// Newest first: wall descending, then logical descending.
				var want [][]byte
				for _, w := range []byte{9, 7, 5} {
					for _, l := range []byte{3, 2, 1} {
						want = append(want, []byte{w, l})
					}
				}
				if !equalAll(got, want) {
					t.Fatalf("versions come back as %v, want %v", got, want)
				}
			}
			check(kv)
			if err := kv.Settle(); err != nil {
				t.Fatal(err)
			}
			check(kv) // now from tables
			if size, err := kv.Size(); err != nil || size <= 0 {
				t.Errorf("Size = %d, %v", size, err)
			}
			if got := kv.Config(); got.Schema != cfg.Schema || got.TimeFilter != cfg.TimeFilter || got.Sync != cfg.Sync || got.Tuning == (pebblekv.Tuning{}) {
				t.Errorf("Config() = %+v, want %+v", got, cfg)
			}
			if err := kv.Close(); err != nil {
				t.Fatal(err)
			}
			kv, err = pebblekv.Open(dir, pebblekv.CockroachLayout, cfg)
			if err != nil {
				t.Fatalf("reopening: %v", err)
			}
			defer func() { _ = kv.Close() }()
			check(kv)
		})
	}
}

func TestOpenRefusesUnknownSchema(t *testing.T) {
	t.Parallel()
	if _, err := pebblekv.Open(t.TempDir(), pebblekv.CockroachLayout, pebblekv.Config{Schema: 9}); err == nil {
		t.Fatal("an unknown schema opened")
	}
}

func TestMetaRoundTrips(t *testing.T) {
	t.Parallel()
	kv, err := pebblekv.Open(t.TempDir(), pebblekv.CockroachLayout, pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()

	if v, err := kv.GetMeta(metaKey(pebblekv.MetaLastSeq)); err != nil || v != nil {
		t.Fatalf("GetMeta on a new database = %x, %v", v, err)
	}
	if seq, err := pebblekv.DecodeSeq(nil); err != nil || seq != 0 {
		t.Errorf("DecodeSeq(nil) = %d, %v; want 0", seq, err)
	}
	if h, err := pebblekv.DecodeHorizon(nil); err != nil || !h.IsZero() {
		t.Errorf("DecodeHorizon(nil) = %v, %v; want the zero time", h, err)
	}
	for _, seq := range []uint64{0, 1, 1 << 32, 1<<63 + 5} {
		back, err := pebblekv.DecodeSeq(pebblekv.EncodeSeq(seq))
		if err != nil || back != seq {
			t.Errorf("seq %d round-trips to %d, %v", seq, back, err)
		}
	}
	for _, h := range []time.Time{
		time.Unix(1_700_000_000, 7).UTC(), {}, time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC), time.Unix(0, 0).UTC(),
	} {
		raw, err := pebblekv.EncodeHorizon(h)
		if err != nil {
			t.Fatal(err)
		}
		back, err := pebblekv.DecodeHorizon(raw)
		if err != nil || !back.Equal(h) {
			t.Errorf("horizon %v round-trips to %v, %v", h, back, err)
		}
	}
	if _, err := pebblekv.DecodeSeq([]byte{1, 2}); err == nil {
		t.Error("a short sequence number decoded")
	}
	if _, err := pebblekv.DecodeHorizon([]byte{1, 2}); err == nil {
		t.Error("a short horizon decoded")
	}

	// The format check writes on a new database, accepts the same format and
	// refuses another.
	key := metaKey(pebblekv.MetaFormat)
	if err := kv.CheckFormat(key, []byte("layout-x/1")); err != nil {
		t.Fatal(err)
	}
	if err := kv.CheckFormat(key, []byte("layout-x/1")); err != nil {
		t.Errorf("the same format was refused: %v", err)
	}
	if err := kv.CheckFormat(key, []byte("layout-y/1")); err == nil {
		t.Error("another layout's format was accepted")
	}
	if err := kv.CheckFormat(key, []byte("layout-x/2")); err == nil {
		t.Error("another version of the format was accepted")
	}
	if v, err := kv.GetMeta(key); err != nil || string(v) != "layout-x/1" {
		t.Errorf("GetMeta = %q, %v", v, err)
	}
}

func equalAll(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}

// A variant must be in effect, not only asked for: a collector that is never
// installed or a schema that is never used would leave a measurement of the
// wrong thing looking fine. The tables say what they were written with.
func TestVariantsAreInEffect(t *testing.T) {
	t.Parallel()
	for name, cfg := range configs() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			kv, err := pebblekv.Open(t.TempDir(), pebblekv.CockroachLayout, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = kv.Close() }()
			for i := range 200 {
				key := cockroachkvs.EncodeMVCCKey(nil, []byte{1, 0, byte(i)}, uint64(i)+1, 1)
				if err := kv.Set(key, []byte("v"), kv.WriteOptions()); err != nil {
					t.Fatal(err)
				}
			}
			if err := kv.Settle(); err != nil {
				t.Fatal(err)
			}
			props, err := kv.TableProperties()
			if err != nil || len(props) == 0 {
				t.Fatalf("TableProperties = %d tables, %v", len(props), err)
			}
			for _, p := range props {
				wantSchema := cockroachkvs.KeySchema.Name
				if cfg.Schema == pebblekv.SchemaDefault {
					wantSchema = "DefaultKeySchema(cockroach_comparator,16)"
				}
				if p.KeySchemaName != wantSchema {
					t.Errorf("a table was written with key schema %q, want %q", p.KeySchemaName, wantSchema)
				}
				if _, has := p.UserProperties["MVCCTimeInterval"]; has != cfg.TimeFilter {
					t.Errorf("table has the time-interval property: %v, want %v (properties %v)", has, cfg.TimeFilter, p.UserProperties)
				}
			}
		})
	}
}

// A database written under one key schema opens under the other, and the tables
// of both are read.
func TestReopenUnderAnotherSchema(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	write := func(schema pebblekv.Schema, wall uint64) {
		kv, err := pebblekv.Open(dir, pebblekv.CockroachLayout, pebblekv.Config{Schema: schema, Tuning: pebblekv.TinyTuning()})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = kv.Close() }()
		if err := kv.Set(cockroachkvs.EncodeMVCCKey(nil, []byte("k"), wall, 1), []byte("v"), kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
		if err := kv.Settle(); err != nil {
			t.Fatal(err)
		}
	}
	write(pebblekv.SchemaCRDB, 1)
	write(pebblekv.SchemaDefault, 2)
	kv, err := pebblekv.Open(dir, pebblekv.CockroachLayout, pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = kv.Close() }()
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = it.Close() }()
	n := 0
	for ok := it.First(); ok; ok = it.Next() {
		n++
	}
	if n != 2 {
		t.Fatalf("read %d keys, want 2", n)
	}
}

func TestBytewiseLayout(t *testing.T) {
	t.Parallel()
	tiny := pebblekv.TinyTuning()
	for name, cfg := range map[string]pebblekv.Config{
		"the crdb1 schema":  {Schema: pebblekv.SchemaCRDB, Tuning: tiny},
		"no schema":         {Tuning: tiny},
		"a time filter":     {Schema: pebblekv.SchemaDefault, TimeFilter: true, Tuning: tiny},
		"a nonsense schema": {Schema: 9, Tuning: tiny},
	} {
		if _, err := pebblekv.Open(t.TempDir(), pebblekv.BytewiseLayout, cfg); err == nil {
			t.Errorf("the bytewise layout opened with %s", name)
		}
	}

	dir := t.TempDir()
	kv, err := pebblekv.Open(dir, pebblekv.BytewiseLayout, pebblekv.Config{Schema: pebblekv.SchemaDefault, Tuning: tiny})
	if err != nil {
		t.Fatal(err)
	}
	// Keys come back in byte order, whatever the order they were written in.
	want := [][]byte{{0}, {1}, {1, 0}, {1, 0, 0xFF}, {1, 1}, {2}, {0xFF, 0xFF}}
	for _, i := range []int{3, 0, 6, 2, 5, 1, 4} {
		if err := kv.Set(want[i], []byte{byte(i)}, kv.WriteOptions()); err != nil {
			t.Fatal(err)
		}
	}
	if err := kv.Settle(); err != nil {
		t.Fatal(err)
	}
	props, err := kv.TableProperties()
	if err != nil || len(props) == 0 {
		t.Fatalf("TableProperties = %d tables, %v", len(props), err)
	}
	for _, p := range props {
		if p.KeySchemaName != "DefaultKeySchema(leveldb.BytewiseComparator,16)" {
			t.Errorf("key schema %q", p.KeySchemaName)
		}
	}
	it, err := kv.NewIter(nil)
	if err != nil {
		t.Fatal(err)
	}
	var got [][]byte
	for ok := it.First(); ok; ok = it.Next() {
		got = append(got, bytes.Clone(it.Key()))
	}
	_ = it.Close()
	if !equalAll(got, want) {
		t.Errorf("keys in order = %x, want %x", got, want)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}

	// A database written under one layout's comparer is refused by the other
	// as an error from Open, not a panic later.
	if _, err := pebblekv.Open(dir, pebblekv.CockroachLayout, pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: tiny}); err == nil {
		t.Error("a bytewise database opened under the cockroachkvs comparer")
	}
}

// The refusal is the same the other way round.
func TestCockroachDatabaseIsRefusedByTheBytewiseLayout(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	kv, err := pebblekv.Open(dir, pebblekv.CockroachLayout, pebblekv.Config{Schema: pebblekv.SchemaCRDB, Tuning: pebblekv.TinyTuning()})
	if err != nil {
		t.Fatal(err)
	}
	if err := kv.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pebblekv.Open(dir, pebblekv.BytewiseLayout, pebblekv.Config{Schema: pebblekv.SchemaDefault, Tuning: pebblekv.TinyTuning()}); err == nil {
		t.Error("a cockroachkvs database opened under the bytewise comparer")
	}
}
