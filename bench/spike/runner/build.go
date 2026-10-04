package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// Manifest says what a built database is: which candidate, built from which plan,
// by which binary, and what it held when the build finished. A read refuses a
// database whose manifest is not the plan's.
type Manifest struct {
	Candidate, Layout string
	// PlanDigest is [Plan.Digest] of the plan it was built from.
	PlanDigest string
	Stream     StreamInfo
	Build      BuildInfo
	// Untimed says the binary was one a result may not come from.
	Untimed bool
	// Describe is how the engine was set up and what its tables were written
	// with, read back from the tables.
	Describe map[string]string
	// Counters are the engine's own counts while it was building: records and
	// versions written, retention work, checkpoints written, invalidated and
	// failed.
	Counters map[string]int64
	// StatsBuilt is the database as the stream left it, before anything was
	// compacted on purpose, and StatsCompacted after everything was.
	StatsBuilt, StatsCompacted map[string]int64
	// Breakdown is the logical bytes of every part, by "layer/part", and
	// SizeByLayer Pebble's estimate of the table bytes of each layer, both after
	// the compaction. Size is the bytes of the directory as the engine counts it.
	Breakdown, SizeByLayer map[string]int64
	Size                   int64
}

// ManifestFile and ResultsFile are the names of the files in a candidate's
// directory; DBDir is the database.
const (
	ManifestFile = "manifest.json"
	ResultsFile  = "results.json"
	DBDir        = "db"
)

// CandidateDir is where a candidate is built under out.
func CandidateDir(out, name string) string {
	return filepath.Join(out, strings.NewReplacer("/", "_").Replace(name))
}

// partKey names a part of the breakdown.
func partKey(p engine.Part) string { return fmt.Sprintf("%s/%s", p.Layer, p.Kind) }

// optional interfaces a candidate must have to be measured.
type measurable interface {
	engine.Engine
	engine.Quiescer
	engine.ColdStarter
	engine.Statser
	engine.Describer
	engine.Breakdowner
	engine.LayerSizer
}

// buildSink writes the stream to a candidate, and stops the build at the first
// thing that is not as planned.
type buildSink struct{ e measurable }

func (s buildSink) Write(batch []engine.Record) error {
	if err := s.e.Write(batch); err != nil {
		return err
	}
	if got, want := s.e.LastSeq(), batch[len(batch)-1].Seq; got != want {
		return fmt.Errorf("LastSeq is %d after a batch ending at %d", got, want)
	}
	return nil
}

func (s buildSink) Retain(h time.Time) error { return s.e.Retain(h) }

// Build writes the plan's stream to the candidate under dir, compacts
// everything, and records what the database holds in dir's manifest. The
// directory must be new and outside any git worktree.
func Build(ctx context.Context, plan *Plan, v candidates.Variant, dir string, g Guards, say Progress) (*Manifest, error) {
	if err := g.Check(); err != nil {
		return nil, err
	}
	if err := CheckOutside(dir); err != nil {
		return nil, err
	}
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return nil, fmt.Errorf("runner: %s is not empty: a build starts from nothing (a directory with no manifest is a build that did not finish: remove it; one with a manifest and no results is built and not yet read, and `run` reads it)", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	planDigest, err := plan.Digest()
	if err != nil {
		return nil, err
	}

	rec := NewCapture()
	opened, err := v.Open(filepath.Join(dir, DBDir), candidates.Options{CacheBytes: plan.CacheBytes, Recorder: rec, DisableReadCompactions: true})
	if err != nil {
		return nil, err
	}
	e, ok := opened.(measurable)
	if !ok {
		_ = opened.Close()
		return nil, fmt.Errorf("runner: %s cannot report what a measurement needs (compaction, statistics, description, breakdown)", v.Name)
	}
	closed := false
	defer func() {
		if !closed {
			_ = e.Close()
		}
	}()

	say.say("build %s: writing %d records in batches of %d", v.Name, plan.Stream.Records, plan.Spec.BatchSize)
	info, err := Drive(ctx, plan.Spec, buildSink{e})
	if err != nil {
		return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
	}
	if err := sameAsPlan(plan.Stream, info); err != nil {
		return nil, fmt.Errorf("runner: %s wrote a different stream than planned: %w", v.Name, err)
	}
	totals := rec.Totals()
	if n := totals["checkpoint.errors"]; n > 0 {
		return nil, fmt.Errorf("runner: %s failed to write %d checkpoints (counted, not returned)", v.Name, n)
	}
	m := &Manifest{
		Candidate: v.Name, Layout: v.Layout, PlanDigest: planDigest, Stream: info,
		Build: g.Info, Untimed: g.Untimed, Counters: totals, StatsBuilt: e.Stats(),
	}

	say.say("build %s: compacting everything", v.Name)
	if err := e.CompactAll(ctx); err != nil {
		return nil, fmt.Errorf("runner: %s: %w", v.Name, err)
	}
	m.StatsCompacted = e.Stats()
	if m.Describe, err = e.Describe(); err != nil {
		return nil, err
	}
	parts, err := e.Breakdown()
	if err != nil {
		return nil, err
	}
	m.Breakdown = map[string]int64{}
	for p, n := range parts {
		m.Breakdown[partKey(p)] = n
	}
	layers, err := e.SizeByLayer()
	if err != nil {
		return nil, err
	}
	m.SizeByLayer = map[string]int64{}
	for l, n := range layers {
		m.SizeByLayer[l.String()] = n
	}
	if m.Size, err = e.Size(); err != nil {
		return nil, err
	}
	closed = true
	if err := e.Close(); err != nil {
		return nil, err
	}
	if err := writeJSON(filepath.Join(dir, ManifestFile), m); err != nil {
		return nil, err
	}
	say.say("build %s: %d table bytes, %d records", v.Name, m.StatsCompacted["live_table_bytes"], info.Records)
	return m, nil
}

// sameAsPlan returns the first way a stream differs from the planned one.
func sameAsPlan(want, got StreamInfo) error {
	switch {
	case want.Digest != got.Digest:
		return fmt.Errorf("stream digest %s, planned %s: %w", got.Digest, want.Digest, errMismatch)
	case want.Records != got.Records:
		return fmt.Errorf("%d records written, planned %d: %w", got.Records, want.Records, errMismatch)
	case want.Dropped != got.Dropped:
		return fmt.Errorf("%d records dropped before the horizon, planned %d: %w", got.Dropped, want.Dropped, errMismatch)
	case want.LastSeq != got.LastSeq:
		return fmt.Errorf("the stream ends at seq %d, planned %d: %w", got.LastSeq, want.LastSeq, errMismatch)
	case want.OldToken != got.OldToken:
		return fmt.Errorf("the old snapshot is at seq %d, planned %d: %w", got.OldToken, want.OldToken, errMismatch)
	case len(want.Retentions) != len(got.Retentions):
		return fmt.Errorf("%d retentions, planned %d: %w", len(got.Retentions), len(want.Retentions), errMismatch)
	}
	return nil
}

func writeJSON(path string, v any) error {
	b, err := json.MarshalIndent(v, "", " ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, append(b, '\n'))
}

// writeFileAtomic writes the file whole or not at all: a step that is killed
// while writing leaves the old file, or none, and not a truncated one.
func writeFileAtomic(path string, b []byte) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadManifest reads the manifest of the candidate built under dir.
func LoadManifest(dir string) (*Manifest, error) {
	m, _, err := readManifest(dir)
	return m, err
}

// readManifest loads a candidate's manifest and the digest of the file it came
// from.
func readManifest(dir string) (*Manifest, string, error) {
	b, err := os.ReadFile(filepath.Join(dir, ManifestFile))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, "", fmt.Errorf("runner: %s has no manifest: it was not built, or the build did not finish", dir)
		}
		return nil, "", err
	}
	var m Manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, "", fmt.Errorf("runner: %s: %w", filepath.Join(dir, ManifestFile), err)
	}
	sum := sha256.Sum256(b)
	return &m, hex.EncodeToString(sum[:]), nil
}
