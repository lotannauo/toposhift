package rockskv

import (
	"encoding/binary"

	"github.com/linxGnu/grocksdb"
)

// Batch is a RocksDB write batch with Pebble's batch's shape. Its [Batch.Len] is
// the size of its encoding, which RocksDB's and Pebble's batches share (a 12-byte
// header, then each operation as its kind and its length-prefixed key and value),
// kept here so that it is read without a call into RocksDB.
type Batch struct {
	wb    *grocksdb.WriteBatch
	bytes int
	count int
}

// batchHeaderLen is the header of a batch's encoding: a sequence number and a count.
const batchHeaderLen = 12

// NewBatch makes an empty batch.
func (k *KV) NewBatch() *Batch { return &Batch{wb: grocksdb.NewWriteBatch()} }

// lenPrefixed is the bytes of b in an encoding, with its length in front.
func lenPrefixed(b []byte) int {
	var buf [binary.MaxVarintLen64]byte
	return binary.PutUvarint(buf[:], uint64(len(b))) + len(b)
}

// Set adds a key and its value. The last argument is for the shape of Pebble's
// call and is ignored: a batch commits with the options of [KV.Apply].
func (b *Batch) Set(key, value []byte, _ *WriteOptions) error {
	b.wb.Put(key, value)
	b.bytes += 1 + lenPrefixed(key) + lenPrefixed(value)
	b.count++
	return nil
}

// Delete adds the deletion of a key.
func (b *Batch) Delete(key []byte, _ *WriteOptions) error {
	b.wb.Delete(key)
	b.bytes += 1 + lenPrefixed(key)
	b.count++
	return nil
}

// DeleteRange adds the deletion of every key in [start, end).
func (b *Batch) DeleteRange(start, end []byte, _ *WriteOptions) error {
	b.wb.DeleteRange(start, end)
	b.bytes += 1 + lenPrefixed(start) + lenPrefixed(end)
	b.count++
	return nil
}

// Len is the size of the batch's encoding, header included.
func (b *Batch) Len() int { return batchHeaderLen + b.bytes }

// Empty says whether the batch holds no operation.
func (b *Batch) Empty() bool { return b.count == 0 }

// Reset empties the batch, to be used again.
func (b *Batch) Reset() {
	b.wb.Clear()
	b.bytes, b.count = 0, 0
}

// Close releases the batch.
func (b *Batch) Close() error {
	if b.wb != nil {
		b.wb.Destroy()
		b.wb = nil
	}
	return nil
}

// encodedLen is the size of the batch as RocksDB holds it: for a test that
// [Batch.Len] is that size.
func (b *Batch) encodedLen() int { return len(b.wb.Data()) }
