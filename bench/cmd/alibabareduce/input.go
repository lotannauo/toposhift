package main

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/identity"
)

// reason is why a row was skipped. A row is used or skipped, never repaired,
// and the first reason in this order that applies is the one counted.
type reason int

const (
	skipColumns         reason = iota // the wrong number of fields
	skipShifted                       // the rpctype is not an rpc type: the columns are shifted
	skipTimestamp                     // the timestamp is not a non-negative decimal integer
	skipEmptyID                       // an id is empty, blank, too long, not UTF-8 or holds a control character
	skipUnknownEndpoint               // a call endpoint is unknown (expected, not dirt)
	numReasons
)

var reasonNames = [numReasons]string{"columns", "shifted", "timestamp", "empty_id", "unknown_endpoint"}

func (r reason) String() string { return reasonNames[r] }

// fileReport is what is known of one archive: its accounting and its layout.
type fileReport struct {
	Table  string `json:"table"`
	Name   string `json:"name"`
	Index  int    `json:"index"`
	SHA256 string `json:"sha256"`
	// Rows is the data rows read; Used is the rows that passed every check. Rows
	// equals Used plus the sum of Skips, exactly.
	Rows  int64            `json:"rows"`
	Used  int64            `json:"used"`
	Skips map[string]int64 `json:"skips"`
	skips [numReasons]int64
	have  bool
	// MinTimestamp and MaxTimestamp span the timestamps of the rows that passed the
	// timestamp check, as the file has them. Layout is "absolute" if the file's
	// timestamps are from the trace's start and "per-file" if they restart at zero
	// (the reducer then adds the file's offset). OffsetMillis is that offset.
	MinTimestamp int64  `json:"min_timestamp"`
	MaxTimestamp int64  `json:"max_timestamp"`
	Layout       string `json:"layout"`
	OffsetMillis int64  `json:"offset_ms"`
}

func (r *fileReport) skip(why reason) {
	r.skips[why]++
}

// seal fills the exported map from the counters and checks the accounting.
func (r *fileReport) seal() error {
	r.Skips = make(map[string]int64, numReasons)
	var sum int64
	for i := reason(0); i < numReasons; i++ {
		r.Skips[i.String()] = r.skips[i]
		sum += r.skips[i]
	}
	if r.Rows != r.Used+sum {
		return fmt.Errorf("%s: %d rows read, but %d used and %d skipped", r.Name, r.Rows, r.Used, sum)
	}
	return nil
}

// span folds one timestamp into the file's range.
func (r *fileReport) span(ts int64) {
	if !r.have || ts < r.MinTimestamp {
		r.MinTimestamp = ts
	}
	if !r.have || ts > r.MaxTimestamp {
		r.MaxTimestamp = ts
	}
	r.have = true
}

// archive is one input file.
type archive struct {
	Table string
	Index int
	Path  string
	Rel   string // as SHA256SUMS names it, without the leading "./"
}

var archiveName = regexp.MustCompile(`^(.+)_(0|[1-9][0-9]*)\.tar\.gz$`)

// listArchives finds the table's archives under dir, in numeric order, and
// requires exactly want of them, numbered from 0 with no gap.
func listArchives(dir, table string, want int) ([]archive, error) {
	entries, err := os.ReadDir(filepath.Join(dir, table))
	if err != nil {
		return nil, err
	}
	var out []archive
	for _, e := range entries {
		m := archiveName.FindStringSubmatch(e.Name())
		if m == nil || m[1] != table {
			continue
		}
		n, err := strconv.Atoi(m[2])
		if err != nil {
			return nil, fmt.Errorf("%s: %w", e.Name(), err)
		}
		out = append(out, archive{Table: table, Index: n, Path: filepath.Join(dir, table, e.Name()), Rel: table + "/" + e.Name()})
	}
	slices.SortFunc(out, func(a, b archive) int { return a.Index - b.Index })
	for i, a := range out {
		if a.Index != i {
			return nil, fmt.Errorf("%s: archive %d is missing (found %d)", table, i, a.Index)
		}
	}
	if len(out) != want {
		return nil, fmt.Errorf("%s: %d archives found, %d expected", table, len(out), want)
	}
	return out, nil
}

// readSums reads SHA256SUMS: lines of a SHA-256 in hex and a path, keyed by the
// path without any leading "./".
func readSums(dir string) (map[string]string, error) {
	b, err := os.ReadFile(filepath.Join(dir, "SHA256SUMS"))
	if err != nil {
		return nil, err
	}
	sums := make(map[string]string)
	for i, line := range strings.Split(string(b), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		sum, path, ok := strings.Cut(line, " ")
		path = strings.TrimLeft(path, " *")
		path = strings.TrimPrefix(path, "./")
		if !ok || len(sum) != 64 || path == "" {
			return nil, fmt.Errorf("SHA256SUMS line %d is not a digest and a path", i+1)
		}
		if _, err := hex.DecodeString(sum); err != nil {
			return nil, fmt.Errorf("SHA256SUMS line %d: %w", i+1, err)
		}
		sums[path] = sum
	}
	return sums, nil
}

// scanArchive streams the one CSV of a .tar.gz, nothing extracted to disk. It
// checks the header against header, calls row for each data row (with bad set
// and no fields for one the CSV parser refused), and returns the SHA-256 of the
// whole archive file. The archive must hold exactly one regular member.
func scanArchive(path string, header []string, row func(fields []string, bad bool) error) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	br := bufio.NewReaderSize(io.TeeReader(f, h), 1<<20)
	zr, err := gzip.NewReader(br)
	if err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	tr := tar.NewReader(zr)
	hd, err := tr.Next()
	if err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	if hd.Typeflag != tar.TypeReg || !strings.HasSuffix(hd.Name, ".csv") {
		return "", fmt.Errorf("%s: the first member %q is not a CSV file", path, hd.Name)
	}
	cr := csv.NewReader(bufio.NewReaderSize(tr, 1<<20))
	cr.FieldsPerRecord = -1
	cr.LazyQuotes = true
	cr.ReuseRecord = true
	first, err := cr.Read()
	if err != nil {
		return "", fmt.Errorf("%s: the header: %w", path, err)
	}
	if !slices.Equal(first, header) {
		return "", fmt.Errorf("%s: the header is %q, want %q", path, first, header)
	}
	for {
		rec, err := cr.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			var pe *csv.ParseError
			if errors.As(err, &pe) {
				if err := row(nil, true); err != nil {
					return "", err
				}
				continue
			}
			return "", fmt.Errorf("%s: %w", path, err)
		}
		if err := row(rec, false); err != nil {
			return "", err
		}
	}
	if _, err := tr.Next(); !errors.Is(err, io.EOF) {
		return "", fmt.Errorf("%s: the archive holds more than one member", path)
	}
	if _, err := io.Copy(io.Discard, zr); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	if _, err := io.Copy(io.Discard, br); err != nil {
		return "", fmt.Errorf("%s: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// checkSum is an error if the archive's digest is not the one SHA256SUMS lists.
func checkSum(a archive, got string, sums map[string]string) error {
	want, ok := sums[a.Rel]
	if !ok {
		return fmt.Errorf("SHA256SUMS does not list %s", a.Rel)
	}
	if got != want {
		return fmt.Errorf("%s: SHA-256 %s, but SHA256SUMS says %s", a.Rel, got, want)
	}
	return nil
}

// parseMillis reads a non-negative decimal integer, and nothing else.
func parseMillis(s string) (int64, bool) {
	if s == "" || len(s) > 15 {
		return 0, false
	}
	var n int64
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	return n, true
}

// validID is whether the identity layer would take s as the value of a key:
// non-blank, valid UTF-8, within its length limit, and with no control
// character, which a TSV could not carry. Ids are otherwise used exactly as given.
func validID(s string) bool {
	if len(s) == 0 || len(s) > identity.MaxValueLen || !utf8.ValidString(s) {
		return false
	}
	blank := true
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c == 0x7f {
			return false
		}
		if c != ' ' {
			blank = false
		}
	}
	if blank {
		return false
	}
	return strings.TrimSpace(s) != ""
}

// interner numbers distinct strings 1, 2, ... in order of first sight, so a
// token is held once and the rows hold a uint32.
type interner struct {
	ids   map[string]uint32
	names []string
}

func newInterner() *interner { return &interner{ids: make(map[string]uint32)} }

// id returns the id of s, adding it if it is new. The string is copied, so a
// caller's buffer or a long CSV line is not kept alive.
func (in *interner) id(s string) uint32 {
	if v, ok := in.ids[s]; ok {
		return v
	}
	s = strings.Clone(s)
	v := uint32(len(in.names) + 1)
	in.ids[s] = v
	in.names = append(in.names, s)
	return v
}

// name is the string of id, which must have been returned by id.
func (in *interner) name(id uint32) string { return in.names[id-1] }

// size is how many distinct strings the interner holds.
func (in *interner) size() int { return len(in.names) }

// grow returns s with length at least n, keeping what it holds.
func grow[T any](s []T, n int) []T {
	if len(s) >= n {
		return s
	}
	if cap(s) >= n {
		return s[:n]
	}
	t := make([]T, n, n+n/4+16)
	copy(t, s)
	return t
}
