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
	"slices"
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
)

// Arms lists the arms in the order they are built and printed.
var Arms = []Arm{ArmA1, ArmA2, ArmI}

// ParseArms reads a comma separated list of arms.
func ParseArms(list string) ([]Arm, error) {
	var out []Arm
	for _, name := range strings.Split(list, ",") {
		a := Arm(strings.ToUpper(strings.TrimSpace(name)))
		if !slices.Contains(Arms, a) {
			return nil, fmt.Errorf("no arm %q: the arms are A1, A2 and I", name)
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
)

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

// Manifest says what a build's directory holds. It records no time and no path outside
// the directory, so a build repeated gives the same manifest.
type Manifest struct {
	Format         int
	DuckDB         string
	SettingsSHA256 string
	Stream         Stream
	// Arms are the arms built, in the order of [Arms].
	Arms []Arm
	// Files are the activity file and the files of every arm built, by path.
	Files []FileInfo
}

// File returns the entry for a role.
func (m *Manifest) File(r Role) (FileInfo, bool) {
	for _, f := range m.Files {
		if f.Role == r {
			return f, true
		}
	}
	return FileInfo{}, false
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
		if want, ok := rolePath[f.Role]; !ok || want != f.Path {
			return nil, fmt.Errorf("duckengine: %s lists %q at %q", ManifestName, f.Role, f.Path)
		}
	}
	return &m, nil
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
