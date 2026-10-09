package activity

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"strings"
)

const digestPrefix = "sha256:"

// canonRow is one row as the content digest sees it: the values of the
// fourteen columns, with a null target or relation as the empty string.
type canonRow struct {
	seq         uint64
	eventNS     int64
	layer       string
	subjectKind string
	source      string
	target      string
	relation    string
	producer    string
	kind        string
	ttlNS       int64
	hasThrough  bool
	throughNS   int64
	payload     []byte
	hasBoot     bool
	boot        string
	basis       int32
}

// digester computes the SHA-256 over the canonical encoding of the rows of a
// row group, in file order. The encoding is part of format version 1; see the
// package documentation.
type digester struct {
	h   hash.Hash
	buf []byte
}

func newDigester() *digester { return &digester{h: sha256.New()} }

func (d *digester) appendString(s string) {
	d.buf = binary.BigEndian.AppendUint32(d.buf, uint32(len(s)))
	d.buf = append(d.buf, s...)
}

func (d *digester) add(r *canonRow) {
	d.buf = d.buf[:0]
	d.buf = binary.BigEndian.AppendUint64(d.buf, r.seq)
	d.buf = binary.BigEndian.AppendUint64(d.buf, uint64(r.eventNS))
	d.appendString(r.layer)
	d.appendString(r.subjectKind)
	d.appendString(r.source)
	d.appendString(r.target)
	d.appendString(r.relation)
	d.appendString(r.producer)
	d.appendString(r.kind)
	d.buf = binary.BigEndian.AppendUint64(d.buf, uint64(r.ttlNS))
	if r.hasThrough {
		d.buf = append(d.buf, 1)
	} else {
		d.buf = append(d.buf, 0)
	}
	d.buf = binary.BigEndian.AppendUint64(d.buf, uint64(r.throughNS))
	d.buf = binary.BigEndian.AppendUint32(d.buf, uint32(len(r.payload)))
	d.h.Write(d.buf)
	d.h.Write(r.payload)
	// The optional boot: a presence byte, then the string (empty when absent).
	d.buf = d.buf[:0]
	if r.hasBoot {
		d.buf = append(d.buf, 1)
	} else {
		d.buf = append(d.buf, 0)
	}
	d.appendString(r.boot)
	// The basis of the event time, as the number of the column.
	d.buf = binary.BigEndian.AppendUint32(d.buf, uint32(r.basis))
	d.h.Write(d.buf)
}

// reset starts a new digest.
func (d *digester) reset() { d.h.Reset() }

// sumHex returns the digest of the rows added since the last reset as 64
// lowercase hex digits.
func (d *digester) sumHex() string { return hex.EncodeToString(d.h.Sum(nil)) }

// fileDigest returns the digest of a whole file in the form of the footer,
// "sha256:" and 64 lowercase hex digits, from the digests of its row groups:
// the SHA-256 of their raw bytes, concatenated in file order. A file without
// row groups has the digest of nothing. The digests must be valid hex.
func fileDigest(groups []string) string {
	h := sha256.New()
	for _, g := range groups {
		raw, _ := hex.DecodeString(g)
		h.Write(raw)
	}
	return digestPrefix + hex.EncodeToString(h.Sum(nil))
}

// validGroupDigest reports whether s is 64 lowercase hex digits.
func validGroupDigest(s string) bool { return validDigest(digestPrefix + s) }

// validDigest reports whether s has the form of [digester.sum].
func validDigest(s string) bool {
	hexPart, ok := strings.CutPrefix(s, digestPrefix)
	if !ok || len(hexPart) != 2*sha256.Size {
		return false
	}
	for i := range len(hexPart) {
		c := hexPart[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}
