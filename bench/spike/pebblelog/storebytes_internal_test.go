package pebblelog

import (
	"bytes"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

// pebblekv.DigestRange counts a key's kind by the last byte of a 37-byte key,
// pebblekv.DataLo and DataHi bound the data keyspace, and cmd/dbhash splits a data
// key into a 20-byte prefix and the rest. They are written as numbers there, because
// that code cannot import this package; this test is what keeps them true.
func TestTheDigestAndDbhashAssumeTheKeyLayout(t *testing.T) {
	t.Parallel()
	if keyLen != 37 {
		t.Errorf("keyLen = %d: pebblekv.DigestRange and cmd/dbhash assume 37", keyLen)
	}
	if prefixLen != 20 {
		t.Errorf("prefixLen = %d: cmd/dbhash assumes 20", prefixLen)
	}
	lo, hi := dataBounds()
	if !bytes.Equal(lo, pebblekv.DataLo) || !bytes.Equal(hi, pebblekv.DataHi) {
		t.Errorf("dataBounds() = [%x, %x), but pebblekv.DataLo, DataHi are [%x, %x)", lo, hi, pebblekv.DataLo, pebblekv.DataHi)
	}
}
