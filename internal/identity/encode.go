package identity

import (
	"encoding/binary"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// EncodingVersion is the version of the canonical encoding described in the
// package documentation. It is the version byte inside every preimage.
const EncodingVersion = 1

const magic = "toposhift/identity"

// Value tags in the canonical encoding.
const (
	tagString byte = 0x01
	tagInt    byte = 0x02
	tagTime   byte = 0x03
)

func appendU32(dst []byte, n int) []byte {
	return binary.BigEndian.AppendUint32(dst, uint32(n))
}

func appendBytes(dst []byte, s string) []byte {
	return append(appendU32(dst, len(s)), s...)
}

// appendHeader writes everything before the entries.
func appendHeader(dst []byte, typ catalog.EntityType, entries int) []byte {
	dst = append(dst, magic...)
	dst = append(dst, EncodingVersion)
	dst = appendBytes(dst, string(typ))
	return appendU32(dst, entries)
}

// appendEntry writes one entry. Callers append entries in bytewise name order.
func appendEntry(dst []byte, name catalog.AttributeKey, v value) []byte {
	dst = appendBytes(dst, string(name))
	switch v.kind {
	case catalog.KindString:
		dst = append(dst, tagString)
		return appendBytes(dst, v.s)
	case catalog.KindInt:
		dst = append(dst, tagInt)
		return binary.BigEndian.AppendUint64(dst, uint64(v.i))
	default: // catalog.KindTime
		dst = append(dst, tagTime)
		dst = binary.BigEndian.AppendUint64(dst, uint64(v.i))
		return binary.BigEndian.AppendUint32(dst, v.ns)
	}
}
