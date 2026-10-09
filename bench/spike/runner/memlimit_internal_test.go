package runner

import (
	"math"
	"testing"
)

func TestMemoryLimitText(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		n    int64
		want string
	}{
		{math.MaxInt64, "none"},
		{11 << 30, "11811160064"},
		{1, "1"},
		{0, "none"},
		{-1, "none"},
	} {
		if got := memoryLimitText(c.n); got != c.want {
			t.Errorf("memoryLimitText(%d) = %q, want %q", c.n, got, c.want)
		}
	}
}

func TestParseGoMemLimit(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		in   string
		want int64
	}{
		{"", math.MaxInt64},
		{"11GiB", 11 << 30},
		{"512MiB", 512 << 20},
		{"1MiB", 1 << 20},
		{"999999GiB", 999999 << 30},
	} {
		got, err := parseGoMemLimit(c.in)
		if err != nil || got != c.want {
			t.Errorf("parseGoMemLimit(%q) = %d, %v; want %d", c.in, got, err, c.want)
		}
	}
	for _, in := range []string{"11gib", "11GB", "0GiB", "011GiB", "11.5GiB", "off", "11 GiB", " 11GiB", "11GiB ", "1234567GiB", "-1GiB", "11", "GiB", "11Gi", "11KiB"} {
		if got, err := parseGoMemLimit(in); err == nil {
			t.Errorf("parseGoMemLimit(%q) = %d, want an error", in, got)
		}
	}
}

// The text of a recorded limit is the largest whole unit it is a multiple of, and a
// value that is not a number is shown as it was recorded.
func TestMemoryLimitWords(t *testing.T) {
	t.Parallel()

	for _, c := range []struct{ in, want string }{
		{"none", "no Go memory limit"},
		{"11811160064", "Go memory limit 11 GiB"},
		{"536870912", "Go memory limit 512 MiB"},
		{"1000", "Go memory limit 1000 bytes"},
		{"lots", `Go memory limit "lots"`},
		{"-5", `Go memory limit "-5"`},
	} {
		if got := memoryLimitWords(c.in); got != c.want {
			t.Errorf("memoryLimitWords(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
