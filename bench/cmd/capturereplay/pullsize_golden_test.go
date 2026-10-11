package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The golden bytes below were produced by the standard protobuf encoder, once, outside
// this repository: proto.Marshal (deterministic) of a logspb.LogsData that protojson built
// from the same JSON, with google.golang.org/protobuf v1.36.12 and
// go.opentelemetry.io/proto/otlp v1.11.0, on 2026-10-10. They are never regenerated inside
// the repository: this module does not require the generated types, which is why the
// encoder in pullsize.go is written by hand, and these files are what keeps it honest.
//
//   - testdata/pullsize_fixture.json and testdata/pullsize_fixture.pb: a small LogsData
//     that has every kind of value (strings, ints, negative and large ints, bools, doubles,
//     bytes, nested key-value lists and arrays), empty values, empty and absent fields,
//     trace and span ids, an entity reference, a string over 127 bytes, and a second
//     resource. The .pb is the standard encoder's bytes. OTLP/JSON writes trace and span ids
//     as hex, which the capture package reads and the standard JSON decoder reads as base64,
//     so the ids were given to the standard encoder in base64 (the same bytes).
//   - testdata/pullsize_404.sha256: the SHA-256 and the length in bytes of the standard
//     encoder's bytes for the longest line of run5.jsonl of the k3s capture, the pull of 404
//     pods.

func TestProtobufEncoderMatchesTheStandardEncoderOnTheFixture(t *testing.T) {
	js, err := os.ReadFile(filepath.Join("testdata", "pullsize_fixture.json"))
	if err != nil {
		t.Fatal(err)
	}
	want, err := os.ReadFile(filepath.Join("testdata", "pullsize_fixture.pb"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := capture.DecodeLogs(bytes.TrimSpace(js))
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		i := 0
		for i < len(got) && i < len(want) && got[i] == want[i] {
			i++
		}
		t.Errorf("the encoder writes %d bytes, the standard encoder %d; they differ at byte %d", len(got), len(want), i)
	}
	// The measurement uses the same bytes.
	ps, err := measurePull(bytes.TrimSpace(js))
	if err != nil {
		t.Fatal(err)
	}
	if ps.ProtoBytes != len(want) {
		t.Errorf("measurePull says %d protobuf bytes, want %d", ps.ProtoBytes, len(want))
	}
}

// captureEnv names the capture directory for the check on the real pull. It is not read
// unless this variable is set, so the tests never depend on the capture being there.
const captureEnv = "CAPTUREREPLAY_CAPTURE"

func TestProtobufEncoderMatchesTheStandardEncoderOnThe404PodPull(t *testing.T) {
	dir := os.Getenv(captureEnv)
	if dir == "" {
		t.Skipf("%s is not set: the check on the 404-pod pull reads the capture's run5.jsonl, and runs only when it is named", captureEnv)
	}
	f, err := os.Open(runPath(dir, 5))
	if err != nil {
		t.Skipf("%s names %s, which has no run5.jsonl: %v", captureEnv, dir, err)
	}
	defer f.Close()
	var longest []byte
	r := capture.NewReader(bufio.NewReaderSize(f, 1<<20), capture.ReaderOptions{})
	for {
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if len(l.Raw) > len(longest) {
			longest = l.Raw
		}
	}
	raw, err := os.ReadFile(filepath.Join("testdata", "pullsize_404.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	var wantSum string
	var wantLen int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(raw)), "%s %d", &wantSum, &wantLen); err != nil {
		t.Fatalf("testdata/pullsize_404.sha256: %v", err)
	}
	d, err := capture.DecodeLogs(longest)
	if err != nil {
		t.Fatal(err)
	}
	got, err := encodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(got)
	if len(got) != wantLen || hex.EncodeToString(sum[:]) != wantSum {
		t.Errorf("the longest line of run5.jsonl (%d bytes of JSON) encodes to %d bytes with SHA-256 %x; the standard encoder gave %d bytes with %s",
			len(longest), len(got), sum, wantLen, wantSum)
	}
}
