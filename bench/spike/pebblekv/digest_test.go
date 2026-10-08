package pebblekv_test

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// key37 is a key of a layout-L data key's length whose last byte is its kind.
func key37(first, id, kind byte) []byte {
	k := make([]byte, 37)
	k[0], k[1], k[36] = first, id, kind
	return k
}

// quiet drops Pebble's logging.
type quiet struct{}

func (quiet) Infof(string, ...any)  {}
func (quiet) Errorf(string, ...any) {}
func (quiet) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

func memDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open("db", &pebble.Options{FS: vfs.NewMem(), Logger: quiet{}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

type kv struct{ k, v []byte }

func put(t *testing.T, db *pebble.DB, kvs ...kv) {
	t.Helper()
	for _, e := range kvs {
		if err := db.Set(e.k, e.v, pebble.NoSync); err != nil {
			t.Fatal(err)
		}
	}
}

// framed is the digest's definition, written out: each key and value with its
// length in front as 8 bytes big endian, in key order.
func framed(kvs ...kv) [32]byte {
	h := sha256.New()
	for _, e := range kvs {
		for _, b := range [][]byte{e.k, e.v} {
			var n [8]byte
			binary.BigEndian.PutUint64(n[:], uint64(len(b)))
			h.Write(n[:])
			h.Write(b)
		}
	}
	return [32]byte(h.Sum(nil))
}

func TestDigestRangeHashesTheFramedKeysAndValues(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	a := kv{[]byte("a"), []byte("one")}
	b := kv{key37(1, 7, 0), []byte{}}
	c := kv{key37(2, 7, 1), []byte("checkpoint")}
	put(t, db, c, a, b) // written out of order; "a" sorts after the keys that start with a layer byte
	got, err := pebblekv.DigestRange(db, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := framed(b, c, a); got.SHA256 != want {
		t.Errorf("SHA256 = %x, want %x", got.SHA256, want)
	}
	if got.Keys != 3 {
		t.Errorf("Keys = %d, want 3", got.Keys)
	}
}

func TestDigestRangeOfNothingIsTheDigestOfNoBytes(t *testing.T) {
	t.Parallel()
	got, err := pebblekv.DigestRange(memDB(t), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := sha256.Sum256(nil); got != (pebblekv.Digest{SHA256: want}) {
		t.Errorf("DigestRange of an empty database = %+v, want no keys and %x", got, want)
	}
}

func TestDigestRangeIncludesLoAndExcludesHi(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	k := []kv{{[]byte("a"), []byte("1")}, {[]byte("b"), []byte("2")}, {[]byte("c"), []byte("3")}, {[]byte("d"), []byte("4")}}
	put(t, db, k...)
	got, err := pebblekv.DigestRange(db, []byte("b"), []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if want := framed(k[1], k[2]); got.SHA256 != want || got.Keys != 2 {
		t.Errorf("[b, d) = %d keys, %x; want 2 keys, %x (b in, d out)", got.Keys, got.SHA256, want)
	}
	if got, err = pebblekv.DigestRange(db, []byte("c"), nil); err != nil || got.SHA256 != framed(k[2], k[3]) {
		t.Errorf("[c, unbounded) = %x, %v; want the digest of c and d", got.SHA256, err)
	}
	if got, err = pebblekv.DigestRange(db, nil, []byte("b")); err != nil || got.SHA256 != framed(k[0]) {
		t.Errorf("[unbounded, b) = %x, %v; want the digest of a alone", got.SHA256, err)
	}
}

func TestDigestRangeSkipsWhatWasDeleted(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	k := []kv{{[]byte("a"), []byte("1")}, {[]byte("b"), []byte("2")}, {[]byte("c"), []byte("3")}, {[]byte("d"), []byte("4")}, {[]byte("e"), []byte("5")}}
	put(t, db, k...)
	if err := db.Delete([]byte("a"), pebble.NoSync); err != nil {
		t.Fatal(err)
	}
	if err := db.DeleteRange([]byte("c"), []byte("e"), pebble.NoSync); err != nil { // c and d, not e
		t.Fatal(err)
	}
	got, err := pebblekv.DigestRange(db, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if want := framed(k[1], k[4]); got.SHA256 != want || got.Keys != 2 {
		t.Errorf("after deletes = %d keys, %x; want b and e: 2 keys, %x", got.Keys, got.SHA256, want)
	}
}

func TestDigestRangeCountsKindsOfThirtySevenByteKeysOnly(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	short := make([]byte, 36) // last byte 1, but not a data key
	short[0], short[35] = 1, 1
	long := make([]byte, 38)
	long[0], long[37] = 1, 2
	put(t, db,
		kv{key37(1, 1, 0), nil}, kv{key37(1, 2, 0), nil}, kv{key37(2, 1, 0), nil},
		kv{key37(1, 3, 1), nil}, kv{key37(3, 4, 2), nil}, kv{key37(3, 5, 2), nil},
		kv{key37(4, 6, 3), nil}, // a kind that is none of the three
		kv{short, nil}, kv{long, nil}, kv{[]byte{0}, nil},
	)
	got, err := pebblekv.DigestRange(db, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.Keys != 10 || got.Records != 3 || got.Checkpoints != 1 || got.Baselines != 2 {
		t.Errorf("counts = %d keys, %d records, %d checkpoints, %d baselines; want 10, 3, 1, 2",
			got.Keys, got.Records, got.Checkpoints, got.Baselines)
	}
}

func TestDataBoundsAreTheLayers(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	meta := kv{[]byte{0, 'm'}, []byte("meta")}
	l1 := kv{key37(1, 1, 0), []byte("x")}
	l4 := kv{key37(4, 1, 0), []byte("y")}
	beyond := kv{key37(5, 1, 0), []byte("z")}
	put(t, db, meta, l1, l4, beyond)
	got, err := pebblekv.DigestRange(db, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		t.Fatal(err)
	}
	if want := framed(l1, l4); got.SHA256 != want || got.Keys != 2 {
		t.Errorf("data digest = %d keys, %x; want the keys of layers 1 and 4: 2 keys, %x", got.Keys, got.SHA256, want)
	}
}

func TestDigestRangeReadsThroughASnapshot(t *testing.T) {
	t.Parallel()
	db := memDB(t)
	a := kv{[]byte("a"), []byte("1")}
	put(t, db, a)
	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	put(t, db, kv{[]byte("b"), []byte("2")})
	got, err := pebblekv.DigestRange(snap, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.SHA256 != framed(a) || got.Keys != 1 {
		t.Errorf("snapshot digest = %d keys, %x; want only a", got.Keys, got.SHA256)
	}
}
