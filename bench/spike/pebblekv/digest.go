package pebblekv

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
)

// Digest is what [DigestRange] computes: how many keys, by layout-L kind (the last
// byte of a 37-byte key: 0 record, 1 checkpoint, 2 baseline), and a SHA-256 of
// every key and value.
type Digest struct {
	Keys                            int64
	Records, Checkpoints, Baselines int64
	SHA256                          [32]byte
}

// DataLo and DataHi bound layout L's data keyspace: the keys whose first byte is a
// layer, 1 to 4. Its meta keys start with 0x00 and are outside it.
var DataLo, DataHi = []byte{0x01}, []byte{0x05}

// layoutLKeyLen is the length of a layout-L data key, whose last byte is its kind.
const layoutLKeyLen = 37

// DigestRange hashes the live keys in [lo, hi) (nil means unbounded) in key order:
// for each, the key's length as 8 bytes big endian, the key, the value's length as
// 8 bytes big endian, the value. It reads through r (a *pebble.DB, a snapshot or a
// batch). The digest is of what a reader sees, so it does not depend on how the
// database has laid its keys out in tables.
func DigestRange(r pebble.Reader, lo, hi []byte) (d Digest, err error) {
	it, err := r.NewIter(&pebble.IterOptions{LowerBound: lo, UpperBound: hi})
	if err != nil {
		return Digest{}, fmt.Errorf("pebblekv: digesting [%x, %x): %w", lo, hi, err)
	}
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			d, err = Digest{}, fmt.Errorf("pebblekv: digesting [%x, %x): %w", lo, hi, cerr)
		}
	}()
	h := sha256.New()
	var n [8]byte
	for ok := it.First(); ok; ok = it.Next() {
		k, v := it.Key(), it.Value()
		binary.BigEndian.PutUint64(n[:], uint64(len(k)))
		h.Write(n[:])
		h.Write(k)
		binary.BigEndian.PutUint64(n[:], uint64(len(v)))
		h.Write(n[:])
		h.Write(v)
		d.Keys++
		if len(k) == layoutLKeyLen {
			switch k[layoutLKeyLen-1] {
			case 0:
				d.Records++
			case 1:
				d.Checkpoints++
			case 2:
				d.Baselines++
			}
		}
	}
	if err := it.Error(); err != nil {
		return Digest{}, fmt.Errorf("pebblekv: digesting [%x, %x): %w", lo, hi, err)
	}
	h.Sum(d.SHA256[:0])
	return d, nil
}
