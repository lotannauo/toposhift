package backup

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/segment"
	"github.com/lotannauo/toposhift/internal/store"
)

// The format of the manifest. A change to any field, its name, its order or its
// meaning is a new version.
const (
	// Format names the manifest's format in its first field.
	Format = "toposhift.backup.manifest"
	// Version is the version of the manifest this package writes and reads.
	Version = 1
)

// DefaultPrefix is the prefix a backup is written under when none is chosen.
const DefaultPrefix = "backup"

// ErrNoBackup is returned by Latest for a prefix that holds no manifest.
var ErrNoBackup = errors.New("backup: no manifest under the prefix")

// ErrFormat is wrapped by every error that reports a manifest this package cannot
// read, or one whose fields contradict each other.
var ErrFormat = errors.New("backup: invalid manifest")

// StoreMeta is what a restored store must be opened with to answer as the live
// store did: what the store records of itself (its cluster name, and the boot key
// and skew of its lifecycle policy), and what it does not record but retains by
// (each layer's retention offset, and whether it is kept). A restore refuses a
// store opened with other values.
type StoreMeta struct {
	ClusterName    string               `json:"cluster_name"`
	ClusterNameSet bool                 `json:"cluster_name_set"`
	BootKey        catalog.AttributeKey `json:"boot_key"`
	Skew           time.Duration        `json:"skew_ns"`
	// Offsets and Keep are indexed by layer minus L0, as the stores' options are.
	Offsets [4]time.Duration `json:"offsets_ns"`
	Keep    [4]bool          `json:"keep"`
}

// Horizon is one layer's horizon as the store reported it: the zero Horizon is
// time 0 and seq 0.
type Horizon struct {
	TimeNS int64  `json:"time_ns"`
	Seq    uint64 `json:"seq"`
}

// IsZero reports whether no retention had moved the layer.
func (h Horizon) IsZero() bool { return h == Horizon{} }

// Time is the horizon's instant, or the zero time for the zero Horizon.
func (h Horizon) Time() time.Time {
	if h.IsZero() {
		return time.Time{}
	}
	return time.Unix(0, h.TimeNS).UTC()
}

// horizonOf encodes a store's horizon. A time outside what int64 nanoseconds
// hold, or a non-zero horizon at exactly Unix 0 with Seq 0, which would read as
// the zero Horizon, is refused.
func horizonOf(h store.Horizon) (Horizon, error) {
	if h.IsZero() {
		return Horizon{}, nil
	}
	if h.Time.Before(time.Unix(0, math.MinInt64)) || h.Time.After(store.MaxEventTime) {
		return Horizon{}, fmt.Errorf("backup: the horizon %s cannot be logged in nanoseconds", h.Time)
	}
	out := Horizon{TimeNS: h.Time.UnixNano(), Seq: h.Seq}
	if out.IsZero() {
		return Horizon{}, errors.New("backup: a horizon at Unix 0 with seq 0 cannot be told from no horizon")
	}
	return out, nil
}

// Retention is one retention the live store applied, as the backup logged it.
type Retention struct {
	// AskedNS is the instant Retain was asked for, in Unix nanoseconds. Asks never
	// decrease along the log.
	AskedNS int64 `json:"asked_ns"`
	// Seq is the store's LastSeq when Retain was called.
	Seq uint64 `json:"seq"`
	// Horizons are each layer's horizon as the store reported it after the call,
	// indexed by layer minus L0.
	Horizons [4]Horizon `json:"horizons"`
}

// Retired says what of one class the backup has retired.
type Retired struct {
	// ThroughSeq is the last Seq of the class's last retired file (R_c), where the
	// chain of its retained files starts: the first one's AfterSeq.
	ThroughSeq uint64 `json:"through_seq"`
	// ThroughCutSeq is the last Seq of the last cut retired for the class. A cut
	// that ends at or before it holds none of the class's records any more.
	ThroughCutSeq uint64 `json:"through_cut_seq"`
	// Records is how many of the class's records were retired, in all.
	Records int64 `json:"records"`
	// HorizonNS is the horizon of the class's layer that the last retirement of the
	// class was decided at, in Unix nanoseconds.
	HorizonNS int64 `json:"horizon_ns"`
}

// File describes one segment of a class: an activity file (format version 1)
// holding a run of the class's committed records, ascending in Seq.
type File struct {
	Class string `json:"class"`
	Name  string `json:"name"`
	// FirstSeq and LastSeq are its first and last Seq.
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
	// AfterSeq is the LastSeq of the class's file before it, or the class's
	// ThroughSeq for its first retained file: the chain a restore follows.
	AfterSeq uint64 `json:"after_seq"`
	Records  int64  `json:"records"`
	Bytes    int64  `json:"bytes"`
	// SHA256 is the hex SHA-256 of the segment's bytes.
	SHA256 string `json:"sha256"`
	// MaxReachNS is the largest max(EventTime, Through) of its records.
	MaxReachNS int64 `json:"max_reach_ns"`
	// MinShadowableObserveEventNS is the smallest EventTime of an Observe in it
	// that a retired record of its reference could shadow: one whose
	// max(EventTime, Through) - EventTime is above the margin less its own TTL, or
	// one with no TTL. It is the largest int64 if the file holds none.
	MinShadowableObserveEventNS int64 `json:"min_shadowable_observe_event_ns"`
	// WallFirstNS and WallLastNS are the ingest clock of the first and the last
	// commit that put a record in it, after clamping (WallClamps counts the commits
	// whose clock had gone backward and was raised to the one before).
	WallFirstNS int64 `json:"wall_first_ns"`
	WallLastNS  int64 `json:"wall_last_ns"`
	WallClamps  int64 `json:"wall_clamps"`
	// MaxObserveTTL is the largest TTL of an Observe in it; TTL0Observes counts its
	// Observes with no TTL, which only a Delete ends.
	MaxObserveTTL time.Duration `json:"max_observe_ttl_ns"`
	TTL0Observes  int64         `json:"ttl0_observes"`
}

// Cut is one cut: the committed records from FirstSeq to LastSeq, which are dense,
// so its counts per class sum to LastSeq - FirstSeq + 1.
type Cut struct {
	FirstSeq uint64 `json:"first_seq"`
	LastSeq  uint64 `json:"last_seq"`
	// Records counts the cut's records of each class, in class order.
	Records [NumClasses]int64 `json:"records"`
}

// Manifest describes one generation of a backup: what it holds, and enough to
// check that a restore has all of it before anything is written.
type Manifest struct {
	Format  string `json:"format"`
	Version int    `json:"version"`
	// Generation is raised by one for every manifest written, by a cut or a
	// retirement; it names the manifest.
	Generation uint64 `json:"generation"`
	// Seq is the last Seq the backup covers.
	Seq uint64 `json:"seq"`
	// Prev is the hex SHA-256 of the bytes of the manifest this one replaces, or "".
	Prev  string    `json:"prev"`
	Store StoreMeta `json:"store"`
	// Margin is rule R's margin, M; MaxClockAhead is how far ahead of the ingest
	// clock an event time may be.
	Margin        time.Duration `json:"margin_ns"`
	MaxClockAhead time.Duration `json:"max_clock_ahead_ns"`
	// Retentions is the log of the live store's retentions, oldest first.
	Retentions []Retention `json:"retentions"`
	// Retired and NotRetirable are per class, in class order. NotRetirable holds
	// the first reason a class may not be retired, or "".
	Retired      [NumClasses]Retired `json:"retired"`
	NotRetirable [NumClasses]string  `json:"not_retirable"`
	// Files are ordered by class, then first Seq.
	Files []File `json:"files"`
	// Cuts are the cuts that still hold a retained file, ascending.
	Cuts []Cut `json:"cuts"`
}

// Encode returns the manifest's bytes: JSON, indented by two spaces, with a final
// newline. The same manifest always gives the same bytes.
func Encode(m Manifest) ([]byte, error) {
	if m.Retentions == nil {
		m.Retentions = []Retention{}
	}
	if m.Files == nil {
		m.Files = []File{}
	}
	if m.Cuts == nil {
		m.Cuts = []Cut{}
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

// Decode reads a manifest strictly: an unknown field, a trailing value, another
// format or another version is refused with an error wrapping ErrFormat.
func Decode(b []byte) (Manifest, error) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return Manifest{}, fmt.Errorf("%w: %w", ErrFormat, err)
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return Manifest{}, fmt.Errorf("%w: data after the manifest", ErrFormat)
	}
	if m.Format != Format || m.Version != Version {
		return Manifest{}, fmt.Errorf("%w: format %q version %d, want %q version %d", ErrFormat, m.Format, m.Version, Format, Version)
	}
	return m, nil
}

// lastRetention is the latest logged retention, and false if there is none.
func (m Manifest) lastRetention() (Retention, bool) {
	if len(m.Retentions) == 0 {
		return Retention{}, false
	}
	return m.Retentions[len(m.Retentions)-1], true
}

// Names. Seqs and generations are 20-digit zero-padded decimals, so that a listing
// in byte order is in numeric order.

func pad(n uint64) string { return fmt.Sprintf("%020d", n) }

func filesDir(prefix string) string     { return prefix + "/v1/files/" }
func manifestsDir(prefix string) string { return prefix + "/v1/manifests/" }

// FileName is the segment name of a class's file holding first to last.
func FileName(prefix string, c Class, first, last uint64) string {
	return filesDir(prefix) + c.String() + "/" + pad(first) + "-" + pad(last) + ".parquet"
}

// ManifestName is the segment name of a generation's manifest.
func ManifestName(prefix string, generation uint64) string {
	return manifestsDir(prefix) + pad(generation) + ".json"
}

// parseGeneration reads the generation of a manifest's name, if it is one.
func parseGeneration(prefix, name string) (uint64, bool) {
	rest, ok := strings.CutPrefix(name, manifestsDir(prefix))
	if !ok {
		return 0, false
	}
	digits, ok := strings.CutSuffix(rest, ".json")
	if !ok || len(digits) != 20 {
		return 0, false
	}
	g, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return g, true
}

// checkPrefix checks a prefix: a valid segment name, without a trailing slash.
func checkPrefix(prefix string) error {
	if err := segment.ValidName(prefix); err != nil {
		return fmt.Errorf("backup: the prefix %q: %w", prefix, err)
	}
	return nil
}

// manifestGenerations lists the generations of the manifests under prefix, ascending.
func manifestGenerations(ctx context.Context, src segment.Store, prefix string) ([]uint64, error) {
	var gens []uint64
	for info, err := range src.List(ctx, manifestsDir(prefix)) {
		if err != nil {
			return nil, fmt.Errorf("backup: listing the manifests: %w", err)
		}
		if g, ok := parseGeneration(prefix, info.Name); ok {
			gens = append(gens, g)
		}
	}
	return gens, nil
}

// maxManifestBytes bounds what Latest reads.
const maxManifestBytes = segment.MaxSize

// Latest returns the manifest of the highest generation under prefix, with its
// bytes, or an error wrapping ErrNoBackup if there is none. It checks that the
// manifest decodes and that its generation is the one its name gives.
func Latest(ctx context.Context, src segment.Store, prefix string) (Manifest, error) {
	m, _, err := latest(ctx, src, prefix)
	return m, err
}

func latest(ctx context.Context, src segment.Store, prefix string) (Manifest, []byte, error) {
	if err := checkPrefix(prefix); err != nil {
		return Manifest{}, nil, err
	}
	gens, err := manifestGenerations(ctx, src, prefix)
	if err != nil {
		return Manifest{}, nil, err
	}
	if len(gens) == 0 {
		return Manifest{}, nil, fmt.Errorf("%w %q", ErrNoBackup, prefix)
	}
	return readManifest(ctx, src, prefix, gens[len(gens)-1])
}

// readManifest reads and decodes one generation's manifest.
func readManifest(ctx context.Context, src segment.Store, prefix string, gen uint64) (Manifest, []byte, error) {
	name := ManifestName(prefix, gen)
	rc, err := src.Open(ctx, name)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("backup: opening %s: %w", name, err)
	}
	defer func() { _ = rc.Close() }()
	b, err := io.ReadAll(io.LimitReader(rc, maxManifestBytes+1))
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("backup: reading %s: %w", name, err)
	}
	if len(b) > maxManifestBytes {
		return Manifest{}, nil, fmt.Errorf("%w: %s is larger than %d bytes", ErrFormat, name, maxManifestBytes)
	}
	m, err := Decode(b)
	if err != nil {
		return Manifest{}, nil, fmt.Errorf("%s: %w", name, err)
	}
	if m.Generation != gen {
		return Manifest{}, nil, fmt.Errorf("%w: %s holds generation %d", ErrFormat, name, m.Generation)
	}
	return m, b, nil
}
