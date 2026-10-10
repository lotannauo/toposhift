package otlphttp_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
)

// readCaptureLines returns the lines of a capture file, without their newlines.
func readCaptureLines(t testing.TB, path string) [][]byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
}

func otlphttpOptions() otlphttp.Options { return otlphttp.Options{} }
