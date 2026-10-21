package duckengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

// Arm is one set of files the baseline reads.
type Arm string

const (
	// ArmA1 is the archive as written: one file in the order of the archive statement,
	// row groups of 1,048,576 rows.
	ArmA1 Arm = "A1"
	// ArmA2 is the record files at their best: the activity file once sorted by source
	// and once by target, row groups of 65,536 rows.
	ArmA2 Arm = "A2"
	// ArmI is the interval files, with A2's record files behind them for the reads an
	// interval cannot answer.
	ArmI Arm = "I"
	// ArmH is the diagnostic arm: A2's two record files, one pair for each hour of
	// ingest, the pairs holding consecutive ranges of Seq, as a store that appends a file
	// for each hour instead of rewriting one would hold them. A read opens them all
	// through a glob. It is counted and never timed, and is not one of [Arms], which are
	// the three arms a baseline's figures are made of.
	ArmH Arm = "H"
)

// Arms lists the arms in the order they are built and printed: the three arms of the
// baseline. The diagnostic arm [ArmH] is built and read only when it is asked for by name.
var Arms = []Arm{ArmA1, ArmA2, ArmI}

// AllArms is [Arms] and the diagnostic arm.
var AllArms = []Arm{ArmA1, ArmA2, ArmI, ArmH}

// ParseArms reads a comma separated list of arms.
func ParseArms(list string) ([]Arm, error) {
	var out []Arm
	for _, name := range strings.Split(list, ",") {
		a := Arm(strings.ToUpper(strings.TrimSpace(name)))
		if !slices.Contains(AllArms, a) {
			return nil, fmt.Errorf("no arm %q: the arms are A1, A2, I and, as a diagnostic, H", name)
		}
		if slices.Contains(out, a) {
			return nil, fmt.Errorf("arm %s is listed twice", a)
		}
		out = append(out, a)
	}
	return out, nil
}

// Role is what a file is for; it is also the name of the view a connection reads it
// through.
type Role string

const (
	RoleActivity Role = "activity"
	RoleArchive  Role = "archive"
	RoleFwd      Role = "fwd"
	RoleRev      Role = "rev"
	RoleIFwd     Role = "ifwd"
	RoleIRev     Role = "irev"
	RoleIEnt     Role = "ient"
	// RoleHFwd and RoleHRev are the forward and the reverse record files of each hour of
	// the diagnostic arm: a role of many files, the paths of which are
	// [SegmentPath](i, role).
	RoleHFwd Role = "hfwd"
	RoleHRev Role = "hrev"
)

// SegmentPath is the path, relative to a build's directory, of a segment's file of a
// role of the diagnostic arm.
func SegmentPath(i int, r Role) string {
	name := "fwd"
	if r == RoleHRev {
		name = "rev"
	}
	return fmt.Sprintf("h/%06d/%s.parquet", i, name)
}

var segmentFile = regexp.MustCompile(`^h/([0-9]{6,})/(fwd|rev)\.parquet$`)

// segmentOf reads the segment and the role a path of the diagnostic arm names.
func segmentOf(path string) (int, Role, bool) {
	m := segmentFile.FindStringSubmatch(path)
	if m == nil {
		return 0, "", false
	}
	i, err := strconv.Atoi(m[1])
	if err != nil {
		return 0, "", false
	}
	role := RoleHFwd
	if m[2] == "rev" {
		role = RoleHRev
	}
	return i, role, true
}

func isSegmentRole(r Role) bool { return r == RoleHFwd || r == RoleHRev }

// The paths of the files of a build, relative to its directory.
var rolePath = map[Role]string{
	RoleActivity: "activity.parquet",
	RoleArchive:  "a1/archive.parquet",
	RoleFwd:      "a2/fwd.parquet",
	RoleRev:      "a2/rev.parquet",
	RoleIFwd:     "i/edges_fwd.parquet",
	RoleIRev:     "i/edges_rev.parquet",
	RoleIEnt:     "i/entities.parquet",
}

// armRoles are the files each arm is made of.
var armRoles = map[Arm][]Role{
	ArmA1: {RoleArchive},
	ArmA2: {RoleFwd, RoleRev},
	ArmI:  {RoleIFwd, RoleIRev, RoleIEnt},
	ArmH:  {RoleHFwd, RoleHRev},
}

// readRoles are the files a reader of an arm opens, which for the interval arm include
// the record files.
func readRoles(a Arm) []Role {
	if a == ArmI {
		return append(slices.Clone(armRoles[ArmI]), armRoles[ArmA2]...)
	}
	return armRoles[a]
}

// ManifestName is the file in a build's directory that says what it holds.
const ManifestName = "manifest.json"

// ManifestFormat is the version of the manifest.
const ManifestFormat = 1

// FileInfo describes one file of a build.
type FileInfo struct {
	Role      Role
	Path      string // relative to the directory, with slashes
	Bytes     int64
	SHA256    string
	Rows      int64
	RowGroups int
}

// Stream is what the files were built from.
type Stream struct {
	// Label names the input the stream was made from, as the caller chose it: for a
	// plan, the SHA-256 of its file.
	Label string
	// Digest is the stream's digest, the one a plan records.
	Digest string
	// Records are the records the stream wrote and Kept those the activity file holds,
	// the rest being before the final horizon and not the baseline of any subject and
	// producer.
	Records, Kept uint64
	LastSeq       uint64
	// HorizonNanos is the final retention horizon in Unix nanoseconds, zero if the
	// stream never moved it, and Retentions how many times it moved.
	HorizonNanos int64
	Retentions   int
}

// Segment is one hour of ingest of the diagnostic arm: the records of a range of Seq,
// which a pair of files holds. Segments are in ascending Seq and no two share one.
type Segment struct {
	Index int
	// StartNanos is the start of the hour, in Unix nanoseconds, that the newest event time
	// of the batches the segment holds had reached.
	StartNanos        int64
	FirstSeq, LastSeq uint64
	Records           int64
}

// Adopted says that an arm's file was not written by the build but is a file that was
// made before, checked to hold the records of the stream.
type Adopted struct {
	// ArchiveSHA256 is the file's SHA-256, the one it was named by.
	ArchiveSHA256 string
	// Records are its rows, and Content the order-free digest of every column of every row, which
	// the activity file it was made from has too.
	Records int64
	Content string
}

// Manifest says what a build's directory holds. It records no time and no path outside
// the directory, so a build repeated gives the same manifest.
type Manifest struct {
	Format         int
	DuckDB         string
	SettingsSHA256 string
	Stream         Stream
	// Arms are the arms built, in the order of [AllArms].
	Arms []Arm
	// Files are the activity file and the files of every arm built, by path.
	Files []FileInfo
	// SegmentNanos and Segments describe the hours of the diagnostic arm, if it was
	// built.
	SegmentNanos int64     `json:",omitempty"`
	Segments     []Segment `json:",omitempty"`
	// Adopted is set for a directory made by [Adopt].
	Adopted *Adopted `json:",omitempty"`
}

// File returns the first entry for a role: the only one of every role but the segments'.
func (m *Manifest) File(r Role) (FileInfo, bool) {
	for _, f := range m.Files {
		if f.Role == r {
			return f, true
		}
	}
	return FileInfo{}, false
}

// FilesOf returns every entry for a role, in the order of the manifest, which is by path.
func (m *Manifest) FilesOf(r Role) []FileInfo {
	var out []FileInfo
	for _, f := range m.Files {
		if f.Role == r {
			out = append(out, f)
		}
	}
	return out
}

// Has reports whether an arm was built.
func (m *Manifest) Has(a Arm) bool { return slices.Contains(m.Arms, a) }

// ReadManifest reads the manifest of a build's directory.
func ReadManifest(dir string) (*Manifest, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestName))
	if err != nil {
		return nil, err
	}
	var m Manifest
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("duckengine: %s: %w", filepath.Join(dir, ManifestName), err)
	}
	if m.Format != ManifestFormat {
		return nil, fmt.Errorf("duckengine: %s is format %d, not %d", filepath.Join(dir, ManifestName), m.Format, ManifestFormat)
	}
	for _, f := range m.Files {
		if isSegmentRole(f.Role) {
			if i, role, ok := segmentOf(f.Path); !ok || role != f.Role || i >= len(m.Segments) {
				return nil, fmt.Errorf("duckengine: %s lists %q at %q, which is not a file of one of its %d segments", ManifestName, f.Role, f.Path, len(m.Segments))
			}
			continue
		}
		if want, ok := rolePath[f.Role]; !ok || want != f.Path {
			return nil, fmt.Errorf("duckengine: %s lists %q at %q", ManifestName, f.Role, f.Path)
		}
	}
	if m.Has(ArmH) {
		if err := m.checkSegments(); err != nil {
			return nil, err
		}
	}
	return &m, nil
}

// checkSegments refuses a manifest whose hours do not make one stream: the segments are
// in ascending Seq, each has a pair of files, and the pairs hold the rows the segments say.
func (m *Manifest) checkSegments() error {
	if len(m.Segments) == 0 || m.SegmentNanos <= 0 {
		return fmt.Errorf("duckengine: %s has the diagnostic arm and no segments", ManifestName)
	}
	for i, s := range m.Segments {
		if s.Index != i || s.Records <= 0 || s.FirstSeq > s.LastSeq || i > 0 && s.FirstSeq <= m.Segments[i-1].LastSeq {
			return fmt.Errorf("duckengine: %s: segment %d (%+v) does not follow the one before it", ManifestName, i, s)
		}
		for _, r := range []Role{RoleHFwd, RoleHRev} {
			var found *FileInfo
			for _, f := range m.FilesOf(r) {
				if f.Path == SegmentPath(i, r) {
					f := f
					found = &f
				}
			}
			if found == nil || found.Rows != s.Records {
				return fmt.Errorf("duckengine: %s: segment %d has no %s file of %d rows", ManifestName, i, r, s.Records)
			}
		}
	}
	if len(m.FilesOf(RoleHFwd)) != len(m.Segments) || len(m.FilesOf(RoleHRev)) != len(m.Segments) {
		return fmt.Errorf("duckengine: %s lists files of segments that do not exist", ManifestName)
	}
	return nil
}

func writeManifest(dir string, m *Manifest) error {
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, ManifestName), append(b, '\n'), 0o644)
}

// ErrFilesDiffer is wrapped by the error [VerifyFiles] returns.
var ErrFilesDiffer = errors.New("duckengine: a file is not the one the manifest names")

// VerifyFiles checks every file of the manifest against its size and, if hashes is
// true, its SHA-256: a file that is not the one built is not read.
func VerifyFiles(dir string, m *Manifest, hashes bool) error {
	for _, f := range m.Files {
		p := filepath.Join(dir, filepath.FromSlash(f.Path))
		st, err := os.Stat(p)
		if err != nil {
			return fmt.Errorf("%w: %w", ErrFilesDiffer, err)
		}
		if st.Size() != f.Bytes {
			return fmt.Errorf("%w: %s has %d bytes, the manifest says %d", ErrFilesDiffer, f.Path, st.Size(), f.Bytes)
		}
		if !hashes {
			continue
		}
		sum, err := fileSHA256(p)
		if err != nil {
			return err
		}
		if sum != f.SHA256 {
			return fmt.Errorf("%w: %s has SHA-256 %s, the manifest says %s", ErrFilesDiffer, f.Path, sum, f.SHA256)
		}
	}
	return nil
}

func fileSHA256(p string) (string, error) {
	f, err := os.Open(p)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// rolePathIn is the absolute path of a role's file in a directory.
func rolePathIn(dir string, r Role) string {
	return filepath.Join(dir, filepath.FromSlash(rolePath[r]))
}
