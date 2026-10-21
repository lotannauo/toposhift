package mcp

import (
	"bytes"
	"strings"
	"testing"
)

// The bytes a transport writes for an answer are the bytes the last check
// scanned: JSON returns them, they are what marshalling the answer gives, and a
// change made to the answer just before the check, which is scanned, is in them.
func TestSubgraphJSONIsWhatWasScanned(t *testing.T) {
	w := newAddrWorld(t)
	s := sourceOn(t, w.st, nil, nil)
	for _, a := range w.sgAsks() {
		if a.on != onPlain {
			continue
		}
		got, err := a.do(s)
		if err != nil {
			t.Fatalf("%s: %v", a.name, err)
		}
		if got.scanned == nil {
			t.Errorf("%s: the last check kept no bytes", a.name)
		}
		b, err := got.JSON()
		if err != nil || !bytes.Equal(b, sgJSON(t, got)) {
			t.Errorf("%s: JSON differs from marshalling the answer (%v)", a.name, err)
		}
		// The same bytes, not a second marshalling that happens to be equal.
		if len(b) == 0 || len(got.scanned) == 0 || &b[0] != &got.scanned[0] {
			t.Errorf("%s: JSON is not the scanned bytes", a.name)
		}
	}

	// What the hook puts in the answer before the check is checked, and written.
	const mark = "a sentence added just before the check"
	hooked := sourceOn(t, w.st, &egressHooks{inject: func(out any) {
		a := out.(*SubgraphAnswer)
		a.Notes = append(a.Notes, mark)
	}}, nil)
	got, err := w.sgAsks()[1].do(hooked)
	if err != nil {
		t.Fatal(err)
	}
	if b, _ := got.JSON(); !strings.Contains(string(b), mark) {
		t.Error("the bytes written are not the bytes that were checked")
	}

	// With the check off nothing was scanned, and JSON marshals.
	off := sourceOn(t, w.st, &egressHooks{noBackstop: true}, nil)
	got, err = w.sgAsks()[1].do(off)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := got.JSON(); err != nil || got.scanned != nil || !bytes.Equal(b, sgJSON(t, got)) {
		t.Errorf("with no check: %v, scanned %v", err, got.scanned != nil)
	}
}
