package pgengine

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
	"strings"
)

// ErrSettingsDiffer is wrapped by the error [CheckSettings] returns for a server
// whose settings are not the ones the baseline is written for.
var ErrSettingsDiffer = errors.New("pgengine: the server's settings differ from settings.conf")

// Setting is a line of settings.conf: a server parameter and the value it must have,
// written the way the server prints it.
type Setting struct{ Name, Value string }

var settingName = regexp.MustCompile(`^[a-z_][a-z0-9_]*(\.[a-z0-9_]+)?$`)

// ParseSettings reads settings.conf: one name = value per line, with # starting a
// comment. A name given twice, a line that is not a setting, an empty value and a file
// with no setting are refused, because a setting that is silently ignored would let a
// measurement run on a server it was not written for.
func ParseSettings(r io.Reader) ([]Setting, error) {
	var out []Setting
	seen := make(map[string]int)
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
			return nil, fmt.Errorf("settings line %d: %q is not name = value", n, line)
		case !settingName.MatchString(name):
			return nil, fmt.Errorf("settings line %d: %q is not a parameter name", n, name)
		case value == "":
			return nil, fmt.Errorf("settings line %d: %s has no value", n, name)
		case seen[name] != 0:
			return nil, fmt.Errorf("settings line %d: %s is set twice (first on line %d)", n, name, seen[name])
		}
		seen[name] = n
		out = append(out, Setting{name, value})
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("settings: %w", err)
	}
	if len(out) == 0 {
		return nil, errors.New("settings: no setting in the file")
	}
	return out, nil
}

// LoadSettings reads settings.conf from a path and returns its settings and the
// SHA-256 of the file as hex, which every output of a run records.
func LoadSettings(path string) ([]Setting, string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, "", err
	}
	s, err := ParseSettings(bytes.NewReader(b))
	if err != nil {
		return nil, "", fmt.Errorf("%s: %w", path, err)
	}
	sum := sha256.Sum256(b)
	return s, hex.EncodeToString(sum[:]), nil
}

// SettingsError lists the settings a server has otherwise than written.
type SettingsError struct {
	// Differences are "name: the server has X, settings.conf has Y".
	Differences []string
}

func (e *SettingsError) Error() string {
	return ErrSettingsDiffer.Error() + ": " + strings.Join(e.Differences, "; ")
}

func (e *SettingsError) Unwrap() error { return ErrSettingsDiffer }

// CheckSettings asks the server for each setting (SHOW) and refuses it unless every
// value is the one written. A parameter the server does not know is a difference
// too: it is a setting the file asks for that the server cannot have. An error from
// the server, as opposed to a different value, is returned as it is.
func CheckSettings(ctx context.Context, q Querier, want []Setting) error {
	var diff []string
	for _, s := range want {
		// missing_ok: a parameter the server does not know is null, not an error that
		// would end a transaction the caller may be in.
		var got *string
		if err := q.QueryRow(ctx, "SELECT current_setting($1, true)", s.Name).Scan(&got); err != nil {
			return fmt.Errorf("pgengine: asking for %s: %w", s.Name, err)
		}
		switch {
		case got == nil:
			diff = append(diff, fmt.Sprintf("%s: the server has no such setting, settings.conf has %s", s.Name, s.Value))
		case *got != s.Value:
			diff = append(diff, fmt.Sprintf("%s: the server has %s, settings.conf has %s", s.Name, *got, s.Value))
		}
	}
	if len(diff) > 0 {
		return &SettingsError{Differences: diff}
	}
	return nil
}
