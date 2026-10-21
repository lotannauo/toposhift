package vmengine

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"
)

// ErrFlagsDiffer is wrapped by the error [CheckFlags] returns for a server whose flags are
// not the ones the baseline is written for.
var ErrFlagsDiffer = errors.New("vmengine: the server's flags differ from flags.conf")

// Flag is a line of flags.conf: a server flag and the value it must have, written the way
// the server prints it.
type Flag struct{ Name, Value string }

var flagName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*(\.[A-Za-z][A-Za-z0-9]*)*$`)

// ParseFlags reads flags.conf: one name = value per line, with # starting a comment. A
// name given twice, a line that is not a flag, an empty value and a file with no flag are
// refused, because a flag that is silently ignored would let a measurement run on a
// server it was not written for.
func ParseFlags(r io.Reader) ([]Flag, error) {
	var out []Flag
	seen := map[string]int{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line, _, _ := strings.Cut(sc.Text(), "#")
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		name, value, ok := strings.Cut(line, "=")
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		switch {
		case !ok:
			return nil, fmt.Errorf("flags line %d: %q is not name = value", n, line)
		case !flagName.MatchString(name):
			return nil, fmt.Errorf("flags line %d: %q is not a flag name", n, name)
		case value == "":
			return nil, fmt.Errorf("flags line %d: %s has no value", n, name)
		case seen[name] != 0:
			return nil, fmt.Errorf("flags line %d: %s is set twice (first on line %d)", n, name, seen[name])
		}
		seen[name] = n
		out = append(out, Flag{name, value})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("flags: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("flags: no flag in the file")
	}
	return out, nil
}

// LoadFlags reads flags.conf from a path and returns its flags and the SHA-256 of the
// file as hex, which every output of a run records.
func LoadFlags(path string) ([]Flag, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	f, err := ParseFlags(bytes.NewReader(b))
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", path, err)
	}
	sum := sha256.Sum256(b)
	return f, hex.EncodeToString(sum[:]), nil
}

// ServerOwned are the flags vm-server.sh sets itself, because they depend on where the
// server runs and not on what it is asked: they are not in flags.conf, and a server may
// have them.
var ServerOwned = []string{"storageDataPath", "httpListenAddr"}

// FlagsError lists the flags a server has otherwise than written.
type FlagsError struct{ Problems []string }

func (e *FlagsError) Error() string {
	return ErrFlagsDiffer.Error() + ": " + strings.Join(e.Problems, "; ")
}

// Unwrap lets errors.Is find [ErrFlagsDiffer].
func (e *FlagsError) Unwrap() error { return ErrFlagsDiffer }

// sameValue compares a flag's written value with the server's: as durations if both are
// (the server prints 1h as 1h0m0s), otherwise as text.
func sameValue(want, got string) bool {
	if want == got {
		return true
	}
	w, err1 := time.ParseDuration(want)
	g, err2 := time.ParseDuration(got)
	return err1 == nil && err2 == nil && w == g
}

// CompareFlags checks the flags a server reports against flags.conf: every flag of the
// file must be set to its value, and the server may have no other flag set but those
// vm-server.sh sets itself ([ServerOwned]). The server lists only the flags that were
// set, so a flag of the file that is missing was left at the server's default.
func CompareFlags(want []Flag, got map[string]string) error {
	var problems []string
	known := map[string]bool{}
	for _, f := range want {
		known[f.Name] = true
		v, ok := got[f.Name]
		switch {
		case !ok:
			problems = append(problems, fmt.Sprintf("%s is not set (flags.conf says %s)", f.Name, f.Value))
		case !sameValue(f.Value, v):
			problems = append(problems, fmt.Sprintf("%s is %s (flags.conf says %s)", f.Name, v, f.Value))
		}
	}
	var extra []string
	for name := range got {
		if !known[name] && !slices.Contains(ServerOwned, name) {
			extra = append(extra, name)
		}
	}
	slices.Sort(extra)
	for _, name := range extra {
		problems = append(problems, fmt.Sprintf("%s=%s is set and flags.conf does not have it", name, got[name]))
	}
	if len(problems) > 0 {
		return &FlagsError{Problems: problems}
	}
	return nil
}

// CheckFlags reads the server's flags and compares them with flags.conf.
func CheckFlags(ctx context.Context, c *Client, want []Flag) error {
	got, err := c.Flags(ctx)
	if err != nil {
		return err
	}
	return CompareFlags(want, got)
}
