package wire_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/lotannauo/toposhift/internal/wire"
)

// skipsSections reports whether decoding b read the document less than whole: a
// later minor version, or an optional section it skipped. b decoded, so its
// sections are well formed.
func skipsSections(b []byte) bool {
	if b[5] != wire.FormatMinor {
		return true
	}
	for rest := b[headerLen:]; len(rest) > 0; {
		n, k := binary.Uvarint(rest[1:])
		if rest[0] >= 0x80 {
			return true
		}
		rest = rest[1+k+int(n):]
	}
	return false
}

// FuzzDecode: Decode never panics, and what it accepts encodes to the input's own
// bytes (unless it skipped something), decodes again to the same document, and
// has a JSON twin.
func FuzzDecode(f *testing.F) {
	for _, fx := range fixtures() {
		bin, err := os.ReadFile(filepath.Join(goldenDir, fx.name+".bin"))
		if err != nil {
			continue
		}
		f.Add(bin)
	}
	f.Add([]byte{})
	f.Add([]byte("TSGF\x01\x00"))
	f.Fuzz(func(t *testing.T, b []byte) {
		doc, err := wire.Decode(b)
		if err != nil {
			return
		}
		again, err := wire.Encode(doc)
		if err != nil {
			t.Fatalf("Encode of a decoded document: %v", err)
		}
		if !skipsSections(b) && !bytes.Equal(again, b) {
			t.Fatalf("Encode(Decode(b)) is not b:\n% x\n% x", again, b)
		}
		back, err := wire.Decode(again)
		if err != nil || !equal(back, doc) {
			t.Fatalf("Decode(Encode(Decode(b))) is not Decode(b) (%v)", err)
		}
		if _, err := wire.JSON(doc); err != nil {
			t.Fatalf("JSON: %v", err)
		}
	})
}
