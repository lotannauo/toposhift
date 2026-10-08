package segment_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/segment"
)

// names builds a name of elements slash-separated elements of the given bytes
// each.
func names(elements, bytes int) string {
	e := strings.Repeat("x", bytes)
	parts := make([]string, elements)
	for i := range parts {
		parts[i] = e
	}
	return strings.Join(parts, "/")
}

func TestValidName(t *testing.T) {
	tests := []struct {
		name  string
		input string
		valid bool
	}{
		{"one letter", "a", true},
		{"every allowed byte", "az09._-", true},
		{"nested", "a/b/c", true},
		{"dot inside an element", "a.b/c.d", true},
		{"dot at the end of an element", "a./b", true},
		{"element of 128 bytes", names(1, 128), true},
		{"name of 512 bytes", names(3, 127) + "/" + names(1, 128), true},

		{"empty", "", false},
		{"upper-case letter", "A", false},
		{"upper-case letter in a later element", "a/B", false},
		{"upper-case letter at the end", "abC", false},
		{"leading slash", "/a", false},
		{"trailing slash", "a/", false},
		{"doubled slash", "a//b", false},
		{"only a slash", "/", false},
		{"dot", ".", false},
		{"dot dot", "..", false},
		{"dot dot inside", "a/../b", false},
		{"leading dot", ".hidden", false},
		{"leading dot in a later element", "a/.b", false},
		{"temporary file shape", ".a.tmp-123", false},
		{"space", "a b", false},
		{"backslash", `a\b`, false},
		{"colon", "a:b", false},
		{"nul", "a\x00b", false},
		{"newline", "a\nb", false},
		{"non-ASCII", "café", false},
		{"invalid UTF-8", "a\xffb", false},
		{"element of 129 bytes", names(1, 129), false},
		{"name of 513 bytes", names(3, 128) + "/" + names(1, 126), false},
		{"name far over the limit", strings.Repeat("a/", 600) + "a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := segment.ValidName(tc.input)
			switch {
			case tc.valid && err != nil:
				t.Errorf("ValidName(%q) = %v, want nil", tc.input, err)
			case !tc.valid && err == nil:
				t.Errorf("ValidName(%q) = nil, want an error", tc.input)
			case !tc.valid && !errors.Is(err, segment.ErrInvalidName):
				t.Errorf("ValidName(%q) = %v, want an error wrapping ErrInvalidName", tc.input, err)
			}
		})
	}
}

func TestValidNameLengths(t *testing.T) {
	if got := len(names(3, 127) + "/" + names(1, 128)); got != 512 {
		t.Fatalf("test name has %d bytes, want 512", got)
	}
	if got := len(names(3, 128) + "/" + names(1, 126)); got != 513 {
		t.Fatalf("test name has %d bytes, want 513", got)
	}
}
