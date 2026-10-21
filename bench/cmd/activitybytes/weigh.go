package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

const (
	// DefaultBatch is the number of records written at a time, the batch the spike
	// benchmarks use.
	DefaultBatch = 1000

	checkpointsDefault = "default"
	checkpointsOff     = "off"
)

// compressionErr is the error of a -compression that is not one of the names.
func compressionErr(name string) error {
	_, err := rootkv.ParseCompression(name)
	return err
}

// labelPattern is what a -label may hold.
var labelPattern = regexp.MustCompile(`^[A-Za-z0-9/._-]+$`)

// config is one measurement.
type config struct {
	// In is the activity file; empty with Empty.
	In string
	// Empty weighs a store that nothing is written to.
	Empty bool
	// Out is the directory the store is made in and the result is written to.
	Out string
	// Label names the measurement.
	Label string
	// Checkpoints is checkpointsDefault or checkpointsOff.
	Checkpoints string
	// Compression is the block compression of the store's tables, by the names
	// [rootkv.ParseCompression] takes; empty is "default", which sets nothing.
	Compression string
	// Batch is the number of records per write.
	Batch int
	// ExpectRecords, if not zero, is the number of records the file must hold.
	ExpectRecords int64
	// Samples, if not zero, is the number of samples the records assert, for the
	// bytes per sample.
	Samples int64
	// Keep keeps the store after weighing.
	Keep bool

	// Progress receives the local diagnostics; nil discards them.
	Progress io.Writer

	// mut and judge are for tests. mut is a deliberate fault. judge, if not nil,
	// changes the engine's metrics before the blob guard reads them.
	mut   mutant
	judge func(*pebble.Metrics)
}

// mutant is a deliberate fault. Only tests set one: each is the fault a test must
// catch.
type mutant int

const (
	mutNone mutant = iota
	mutDropLastBatch
	mutKeysAsRecords
	mutSkipCanonicalize
	mutSkipSettle
	mutStripThrough
	mutRenumberSeq
	mutWrongLayer
	mutCheckpointsIgnored
	mutNoBootKey
	mutNoBlobGuard
	mutBlobGuardInverted
	mutEmptyWrites
	mutEmptyAcceptsIn
	mutNoWorkTreeGuard
)

// validate reports the first setting a measurement cannot use.
func (c *config) validate() error {
	switch {
	case c.Empty && c.In != "" && c.mut != mutEmptyAcceptsIn:
		return errors.New("-empty weighs a store with no input: it is refused with -in")
	case c.Empty && c.ExpectRecords != 0:
		return errors.New("-empty weighs a store with no input: it is refused with -expect-records")
	case c.Empty && c.Samples != 0:
		return errors.New("-empty weighs a store with no input: it is refused with -samples")
	case !c.Empty && c.In == "":
		return errors.New("-in is required unless -empty")
	case c.Out == "":
		return errors.New("-out is required")
	case reservedResult(fileLabel(c.Label) + ".json"):
		return fmt.Errorf("-label %q would name the result %s.json, a name check:no-results reserves", c.Label, fileLabel(c.Label))
	case !labelPattern.MatchString(c.Label):
		return fmt.Errorf("-label %q must be one or more of A-Z a-z 0-9 / . _ -", c.Label)
	case c.Checkpoints != checkpointsDefault && c.Checkpoints != checkpointsOff:
		return fmt.Errorf("-checkpoints %q must be %s or %s", c.Checkpoints, checkpointsDefault, checkpointsOff)
	case compressionErr(c.Compression) != nil:
		return fmt.Errorf("-compression %q must be default, snappy, zstd or balanced", c.Compression)
	case c.Batch < 1:
		return fmt.Errorf("-batch %d must be at least 1", c.Batch)
	case c.ExpectRecords < 0 || c.Samples < 0:
		return errors.New("-expect-records and -samples must not be negative")
	}
	return nil
}

func (c *config) logf(format string, args ...any) {
	if c.Progress != nil {
		_, _ = fmt.Fprintf(c.Progress, format+"\n", args...)
	}
}

// reservedResult is whether name is one of the file names check:no-results keeps out
// of the repository, which a result is never given.
func reservedResult(name string) bool {
	return name == "manifest.json" || name == "results.json" || name == "plan.json"
}

// fileLabel is the label as a file name: a / is not a path separator in it.
func fileLabel(label string) string { return strings.ReplaceAll(label, "/", "_") }

// refuseWorkTree is an error if dir, or the nearest directory above it that
// exists, is inside a git work tree: the results are not written into a repository.
func refuseWorkTree(dir string) error {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return err
	}
	for d := abs; ; d = filepath.Dir(d) {
		if _, err := os.Stat(filepath.Join(d, ".git")); err == nil {
			return fmt.Errorf("%s is inside the git work tree %s: the results are not written into a repository", dir, d)
		}
		if filepath.Dir(d) == d {
			return nil
		}
	}
}

// result is what a measurement found, as it is written to LABEL.json. The fields
// of an empty store that have no meaning (the input's, and the figures per record
// and per sample) are empty or left out.
type result struct {
	Label       string `json:"label"`
	Input       string `json:"input"`
	InputDigest string `json:"input_digest"`
	InputSHA256 string `json:"input_sha256"`
	// Records is the number of records written to the store, from the input.
	Records int64 `json:"records"`
	// Keys is the number of data keys the store holds: a record is stored once under
	// its subject, and an edge also under the target it points to, so Keys passes
	// Records.
	Keys int64 `json:"keys"`
	// Checkpoints are the checkpoint keys, split by the kind of prefix they are on.
	Checkpoints       int64 `json:"checkpoints"`
	CheckpointsEntity int64 `json:"checkpoints_entity"`
	CheckpointsEdge   int64 `json:"checkpoints_edge"`
	// LiveTableBytes is the size of the live tables, after CompactAll and Canonicalize.
	LiveTableBytes uint64 `json:"live_table_bytes"`
	// BlobLiveBytes is the size of the live blob files, which must be 0.
	BlobLiveBytes uint64 `json:"blob_live_bytes"`
	// BytesPerRecord is LiveTableBytes over Records, the definition of the spike's G4.
	BytesPerRecord *float64 `json:"bytes_per_record,omitempty"`
	// BytesPerSample is LiveTableBytes over -samples, if given.
	BytesPerSample *float64 `json:"bytes_per_sample,omitempty"`
	TablesPerLevel [7]int64 `json:"tables_per_level"`
	// DataDigest is the digest of the keys and values of the data keyspace, what
	// dbhash -data prints.
	DataDigest        string `json:"data_digest"`
	CheckpointSetting string `json:"checkpoint_setting"`
	// Compression is the block compression the tables were written under. It is left
	// out for the default, so that a result made with no -compression is as it was
	// before the flag.
	Compression string `json:"compression,omitempty"`
	Batch       int    `json:"batch"`
	GoVersion   string `json:"go_version"`
	VCSRevision string `json:"vcs_revision"`
	VCSModified bool   `json:"vcs_modified"`
}

// diagnostics are the counters of the engine that depend on how the compactions
// ran, or that only show the measurement did its steps. They are never in the result.
type diagnostics struct {
	BytesWritten uint64 // flushed and compacted
	Flushes      int64
	Compactions  int64
	Ingests      uint64 // Canonicalize's ingest
	Baselines    int64  // retention was never called, so none
	RecordKeys   int64  // the keys of kind record
}

// dirByte is where a data key says what its prefix is: the byte after the layer and
// the fingerprint. dirEntity is the existence of an entity; 1 and 2 are the edges
// that start at it and the edges that end at it.
const (
	dirByte   = 1 + rootkv.FingerprintKeyLen
	dirEntity = 0

	// keyLen is the length of a data key: the prefix (layer, fingerprint, direction),
	// then the inverted event time and Seq (8 bytes each) and the kind, which is the
	// last byte. See pebblestore/key.go.
	keyLen         = dirByte + 1 + 8 + 8 + 1
	kindCheckpoint = 1
)

// weigh runs one measurement and returns what it found.
func weigh(ctx context.Context, cfg config) (_ *result, _ diagnostics, err error) {
	var diag diagnostics
	if err := cfg.validate(); err != nil {
		return nil, diag, err
	}
	if cfg.mut != mutNoWorkTreeGuard {
		if err := refuseWorkTree(cfg.Out); err != nil {
			return nil, diag, err
		}
	}
	name := fileLabel(cfg.Label)
	jsonPath := filepath.Join(cfg.Out, name+".json")
	if _, err := os.Lstat(jsonPath); err == nil {
		return nil, diag, fmt.Errorf("%s exists: a result is never overwritten", jsonPath)
	}

	res := &result{Label: cfg.Label, Batch: cfg.Batch, CheckpointSetting: cfg.Checkpoints, GoVersion: runtime.Version()}
	res.VCSRevision, res.VCSModified = vcs()

	// The input is read and checked before any directory or store is made, so that a
	// file that is not the one expected costs one read of the file and no store.
	var in *input
	if !cfg.Empty {
		if in, err = openInput(cfg.In); err != nil {
			return nil, diag, err
		}
		defer func() { err = errors.Join(err, in.Close()) }()
		info := in.rd.Info()
		if cfg.ExpectRecords != 0 && info.Records != cfg.ExpectRecords {
			return nil, diag, fmt.Errorf("%s holds %d records in its footer, but %d were expected", cfg.In, info.Records, cfg.ExpectRecords)
		}
		res.Input, res.InputDigest, res.InputSHA256 = in.path, info.Digest, in.sha256
	}

	if err := os.MkdirAll(cfg.Out, 0o755); err != nil {
		return nil, diag, err
	}
	dir := filepath.Join(cfg.Out, name+".store")
	if err := os.Mkdir(dir, 0o755); err != nil {
		return nil, diag, fmt.Errorf("the store directory: %w", err)
	}
	if !cfg.Keep {
		defer func() { err = errors.Join(err, os.RemoveAll(dir)) }()
	}

	opts := pebblestore.DefaultOptions()
	opts.Config = pebblekv.Quiet(opts.Config)
	opts.Sync = false
	comp, err := rootkv.ParseCompression(cfg.Compression)
	if err != nil {
		return nil, diag, err
	}
	opts.Tuning.Compression = comp
	if comp != rootkv.CompressionDefault {
		res.Compression = comp.String()
	}
	if cfg.Checkpoints == checkpointsOff && cfg.mut != mutCheckpointsIgnored {
		opts.Checkpoints = &pebblestore.CheckpointOptions{}
	}
	if cfg.mut == mutNoBootKey {
		opts.Policy = lifecycle.Policy{}
	}
	s, err := pebblestore.Open(dir, opts)
	if err != nil {
		return nil, diag, err
	}
	closed := false
	defer func() {
		if !closed {
			err = errors.Join(err, s.Close())
		}
	}()

	var written int64
	if in != nil {
		if written, err = in.writeTo(ctx, s, cfg); err != nil {
			return nil, diag, err
		}
		if want := in.rd.Info().Records; written != want {
			return nil, diag, fmt.Errorf("%s: %d records were written, the footer says %d", cfg.In, written, want)
		}
		if cfg.ExpectRecords != 0 && written != cfg.ExpectRecords {
			return nil, diag, fmt.Errorf("%s: %d records were written, %d were expected", cfg.In, written, cfg.ExpectRecords)
		}
	} else if cfg.mut == mutEmptyWrites {
		if err := s.Write(ctx, []store.Record{emptyWriteRecord()}); err != nil {
			return nil, diag, err
		}
		written++
	}
	res.Records = written

	kv := pebblekv.Wrap(s.Instrument().KV())
	if cfg.mut != mutSkipSettle {
		if err := kv.CompactAll(ctx); err != nil {
			return nil, diag, fmt.Errorf("compacting: %w", err)
		}
	}
	if cfg.mut != mutSkipSettle && cfg.mut != mutSkipCanonicalize {
		if err := kv.Canonicalize(ctx); err != nil {
			return nil, diag, fmt.Errorf("canonicalizing: %w", err)
		}
	}
	snap := kv.Snapshot()
	m := kv.Metrics()
	if cfg.judge != nil {
		cfg.judge(m)
	}
	if err := judgeBlobs(m, cfg.mut); err != nil {
		return nil, diag, err
	}
	res.LiveTableBytes, res.BlobLiveBytes, res.TablesPerLevel = snap.LiveTableBytes, m.BlobFiles.LiveSize, snap.TablesPerLevel
	diag = diagnostics{
		BytesWritten: snap.BytesFlushed + snap.BytesCompacted, Flushes: snap.Flushes, Compactions: snap.Compactions,
		Ingests: m.Ingest.Count,
	}

	d, err := pebblekv.DigestRange(kv.DB, pebblekv.DataLo, pebblekv.DataHi)
	if err != nil {
		return nil, diag, err
	}
	res.DataDigest = hex.EncodeToString(d.SHA256[:])
	res.Keys, res.Checkpoints = d.Keys, d.Checkpoints
	diag.Baselines, diag.RecordKeys = d.Baselines, d.Records
	if res.CheckpointsEntity, res.CheckpointsEdge, err = checkpointsByPrefix(kv.DB); err != nil {
		return nil, diag, err
	}
	if res.CheckpointsEntity+res.CheckpointsEdge != res.Checkpoints {
		return nil, diag, fmt.Errorf("%d checkpoints on entity prefixes and %d on edge prefixes, but the digest counts %d", res.CheckpointsEntity, res.CheckpointsEdge, res.Checkpoints)
	}
	if cfg.mut == mutKeysAsRecords {
		res.Records = d.Keys
	}
	if !cfg.Empty && res.Records > 0 {
		v := float64(res.LiveTableBytes) / float64(res.Records)
		res.BytesPerRecord = &v
	}
	if cfg.Samples > 0 {
		v := float64(res.LiveTableBytes) / float64(cfg.Samples)
		res.BytesPerSample = &v
	}

	closed = true
	if err := s.Close(); err != nil {
		return nil, diag, err
	}
	cfg.logf("%s: local diagnostics, not results: bytes written %d (flush and compaction), %d flushes, %d compactions, %d ingests, %d record keys, %d baselines",
		cfg.Label, diag.BytesWritten, diag.Flushes, diag.Compactions, diag.Ingests, diag.RecordKeys, diag.Baselines)
	return res, diag, nil
}

// judgeBlobs is an error if the engine holds blob files: value separation is off, and
// the live table bytes hold every value only while it is.
func judgeBlobs(m *pebble.Metrics, mut mutant) error {
	has := m.BlobFiles.LiveCount > 0 || m.BlobFiles.LiveSize > 0
	switch mut {
	case mutNoBlobGuard:
		return nil
	case mutBlobGuardInverted:
		has = !has
	}
	if has {
		return fmt.Errorf("the store holds %d blob files of %d bytes: its live table bytes would leave the values in them out", m.BlobFiles.LiveCount, m.BlobFiles.LiveSize)
	}
	return nil
}

// checkpointsByPrefix counts the checkpoints of the data keyspace by the kind of
// prefix they are on: an entity's existence, or its edges.
func checkpointsByPrefix(db *pebble.DB) (entity, edge int64, err error) {
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: pebblekv.DataLo, UpperBound: pebblekv.DataHi})
	if err != nil {
		return 0, 0, err
	}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) != keyLen || k[keyLen-1] != kindCheckpoint {
			continue
		}
		if k[dirByte] == dirEntity {
			entity++
		} else {
			edge++
		}
	}
	return entity, edge, errors.Join(it.Error(), it.Close())
}

// input is an activity file being read, with its SHA-256 as a file.
type input struct {
	path   string
	f      *os.File
	rd     *activity.Reader
	sha256 string
}

// openInput opens the file, checks its footer, and hashes the whole file.
func openInput(path string) (*input, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(abs)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, st.Size())); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &input{path: abs, f: f, rd: rd, sha256: hex.EncodeToString(h.Sum(nil))}, nil
}

func (in *input) Close() error { return errors.Join(in.rd.Close(), in.f.Close()) }

// writeTo writes every record of the file to the store, in the file's order, in
// batches, and returns how many. The reader has checked each row group against its
// digest before it returned any of its records, and checks the file against its
// footer before io.EOF.
func (in *input) writeTo(ctx context.Context, s *pebblestore.Store, cfg config) (int64, error) {
	var written int64
	batch := make([]store.Record, 0, cfg.Batch)
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := s.Write(ctx, batch); err != nil {
			return fmt.Errorf("writing after %d records: %w", written, err)
		}
		written += int64(len(batch))
		batch = make([]store.Record, 0, cfg.Batch)
		return nil
	}
	var n uint64
	for {
		rec, err := in.rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return written, fmt.Errorf("%s: %w", in.path, err)
		}
		n++
		batch = append(batch, mutate(cfg.mut, rec, n))
		if len(batch) == cfg.Batch {
			if err := flush(); err != nil {
				return written, err
			}
		}
	}
	if cfg.mut == mutDropLastBatch {
		return written, nil
	}
	return written, flush()
}

// mutate is a record with the fault of mut, if it changes records.
func mutate(mut mutant, rec store.Record, n uint64) store.Record {
	switch mut {
	case mutStripThrough:
		rec.Through = time.Time{}
	case mutRenumberSeq:
		rec.Seq = n
	case mutWrongLayer:
		rec.Layer = catalog.L3
	}
	return rec
}

// vcs is the revision this binary was built from and whether the tree was modified.
func vcs() (rev string, modified bool) {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "", false
	}
	for _, s := range bi.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			modified = s.Value == "true"
		}
	}
	return rev, modified
}

// emptyWriteRecord is the one record the mutant that writes into an empty store writes.
func emptyWriteRecord() store.Record {
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "n"}})
	if err != nil {
		panic(err)
	}
	return store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(id.Fingerprint()), Producer: "alibaba/placement",
		EventTime: time.Unix(1, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, TTL: time.Minute,
	}
}
