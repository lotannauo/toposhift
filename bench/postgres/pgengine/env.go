package pgengine

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// ServerInfo is what a server says about itself that an output of the baseline records:
// the build, and the machine the server runs on, which is the one that reads and writes.
type ServerInfo struct {
	// Version is version(), which names the server, the compiler and the platform.
	Version string
	// Configure is the options the server was built with (pg_config --configure).
	Configure string
	// BlockSize and WALBlockSize are the sizes of a page of a table and of the log.
	BlockSize, WALBlockSize int
	// DataChecksums is "on" or "off".
	DataChecksums string
	// CPU is the processor the server runs on, as its host reports it.
	CPU string
}

// ReadServerInfo asks the server. The role must be a superuser (the build options and
// the processor are read with privileges the baseline's role has): a refusal is an
// error, never a field left blank.
func ReadServerInfo(ctx context.Context, q Querier) (ServerInfo, error) {
	var s ServerInfo
	if err := q.QueryRow(ctx, "SELECT version()").Scan(&s.Version); err != nil {
		return s, fmt.Errorf("pgengine: version: %w", err)
	}
	if err := q.QueryRow(ctx, "SELECT setting FROM pg_config WHERE name = 'CONFIGURE'").Scan(&s.Configure); err != nil {
		return s, fmt.Errorf("pgengine: pg_config: %w", err)
	}
	if err := q.QueryRow(ctx, "SELECT current_setting('block_size')::int, current_setting('wal_block_size')::int, current_setting('data_checksums')").
		Scan(&s.BlockSize, &s.WALBlockSize, &s.DataChecksums); err != nil {
		return s, fmt.Errorf("pgengine: block sizes: %w", err)
	}
	var cpuinfo string
	if err := q.QueryRow(ctx, "SELECT pg_read_file('/proc/cpuinfo')").Scan(&cpuinfo); err != nil {
		return s, fmt.Errorf("pgengine: reading the processor of the server: %w", err)
	}
	s.CPU = ParseCPUInfo(cpuinfo)
	return s, nil
}

// ParseCPUInfo names the processor in the text of /proc/cpuinfo: the "model name" of
// the first processor where there is one (x86), and otherwise the implementer,
// architecture, variant and part it reports (arm). A text that names none is "unknown",
// not an empty name.
func ParseCPUInfo(text string) string {
	sc := bufio.NewScanner(strings.NewReader(text))
	fields := map[string]string{}
	for sc.Scan() {
		name, value, ok := strings.Cut(sc.Text(), ":")
		if !ok {
			if strings.TrimSpace(sc.Text()) == "" && len(fields) > 0 {
				break // the first processor ends at the blank line
			}
			continue
		}
		name, value = strings.TrimSpace(name), strings.TrimSpace(value)
		if _, seen := fields[name]; !seen {
			fields[name] = value
		}
	}
	if m := fields["model name"]; m != "" {
		return m
	}
	var parts []string
	for _, k := range []string{"CPU implementer", "CPU architecture", "CPU variant", "CPU part"} {
		if v := fields[k]; v != "" {
			parts = append(parts, k+" "+v)
		}
	}
	if len(parts) > 0 {
		return strings.Join(parts, ", ")
	}
	return "unknown"
}

var imageLine = regexp.MustCompile(`^image='([^']+)'$`)

// ErrNoImage is returned by [PinnedImage] for a script that pins no image.
var ErrNoImage = errors.New("pgengine: the server script pins no image")

// PinnedImage reads the image the server script starts, the reference it pins by
// digest, from the script's own text: the image of a run is recorded as the script
// names it, since a client cannot ask a server which image it was started from.
func PinnedImage(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(b), "\n") {
		if m := imageLine.FindStringSubmatch(strings.TrimSpace(line)); m != nil {
			if !strings.Contains(m[1], "@sha256:") {
				return "", fmt.Errorf("pgengine: %s pins the image %q by a tag, not by digest", path, m[1])
			}
			return m[1], nil
		}
	}
	return "", fmt.Errorf("%s: %w", path, ErrNoImage)
}
