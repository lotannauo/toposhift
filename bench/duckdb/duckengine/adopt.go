package duckengine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/scripts"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// AdoptOptions say which file is taken as the archive arm.
type AdoptOptions struct {
	// Archive is a Parquet file that was made before, in the order and with the options
	// of the archive statement, and SHA256 the SHA-256 it is named by, as hex. The file
	// is read as it is: a file with any other hash is refused.
	Archive, SHA256 string
}

// countSink counts what a replay writes.
type countSink struct {
	records uint64
	lastSeq uint64
}

func (c *countSink) Write(batch []engine.Record) error {
	c.records += uint64(len(batch))
	if len(batch) > 0 {
		c.lastSeq = batch[len(batch)-1].Seq
	}
	return nil
}
func (*countSink) Retain(time.Time) error { return nil }

// contentOf is the number of rows of a record file and the sum, over its rows, of the hash
// of every column of the row: a digest of the rows that does not depend on their order or
// on how the file is laid out, so that the archive and the activity file it was sorted from
// have the same one.
func contentOf(ctx context.Context, db *DB, path string) (rows int64, sum string, err error) {
	err = db.QueryRowContext(ctx, `SELECT count(*), coalesce(sum(hash(seq, event_time_ns, layer, subject_kind, source, target, relation, producer, kind, ttl_ns, through_ns, payload, boot, event_time_basis)::HUGEINT), 0)::VARCHAR
		FROM read_parquet(`+quote(path)+`)`).Scan(&rows, &sum)
	if err != nil {
		return 0, "", fmt.Errorf("duckengine: reading the rows of %s: %w", filepath.Base(path), err)
	}
	return rows, sum, nil
}

// Adopt makes a directory that holds one arm, [ArmA1], whose file is one that was made
// before: the archive the baseline's published bytes are of, which is read as it is and
// not made again. The directory is checked to be of the stream src replays: the archive
// has the SHA-256 it was named by, the stream replayed from the activity file is the one
// the questions were made from, and the archive holds exactly the rows of that file. db
// must be a connection in [ModeBuild] or [ModeRead].
//
// The manifest it writes has no activity file, which is not the build's to hold, and says
// it was adopted.
func Adopt(ctx context.Context, db *DB, dir string, src Source, opt AdoptOptions) (*Manifest, error) {
	if db.cfg.Mode == ModeObserve {
		return nil, errors.New("duckengine: an adoption needs a connection that is not an audit's")
	}
	if src.File == "" || src.Replay == nil {
		return nil, errors.New("duckengine: an adoption needs the activity file the archive was made from")
	}
	if len(opt.SHA256) != 64 || strings.ToLower(opt.SHA256) != opt.SHA256 {
		return nil, fmt.Errorf("duckengine: the SHA-256 the archive must have, %q, is not 64 lowercase hex digits", opt.SHA256)
	}
	if err := db.Check(ctx); err != nil {
		return nil, err
	}
	got, err := fileSHA256(opt.Archive)
	if err != nil {
		return nil, err
	}
	if got != opt.SHA256 {
		return nil, fmt.Errorf("duckengine: %s has SHA-256 %s, not the %s it was named by", opt.Archive, got, opt.SHA256)
	}

	// The stream, replayed whole: the file is read and verified against its own footer, and
	// is the stream the questions were made from.
	var c countSink
	digest, err := src.Replay(ctx, &c)
	if err != nil {
		return nil, err
	}
	arows, asum, err := contentOf(ctx, db, src.File)
	if err != nil {
		return nil, err
	}
	frows, fsum, err := contentOf(ctx, db, opt.Archive)
	if err != nil {
		return nil, err
	}
	if arows != int64(c.records) {
		return nil, fmt.Errorf("duckengine: the activity file has %d rows in DuckDB, %d records in its replay", arows, c.records)
	}
	if frows != arows || fsum != asum {
		return nil, fmt.Errorf("duckengine: %s does not hold the rows of the activity file: %d rows and content %s, the activity file's are %d and %s", filepath.Base(opt.Archive), frows, fsum, arows, asum)
	}

	if err := emptyDir(dir); err != nil {
		return nil, err
	}
	dst := rolePathIn(dir, RoleArchive)
	if copied, err := copyFileSHA256(opt.Archive, dst); err != nil {
		return nil, err
	} else if copied != opt.SHA256 {
		return nil, fmt.Errorf("duckengine: the copy of %s has SHA-256 %s", opt.Archive, copied)
	}
	m := &Manifest{Format: ManifestFormat, DuckDB: Version, SettingsSHA256: scripts.SettingsSHA256(), Arms: []Arm{ArmA1}}
	m.Stream.Label, m.Stream.Digest = src.Label, digest
	m.Stream.Records, m.Stream.Kept, m.Stream.LastSeq = c.records, c.records, c.lastSeq
	fi, err := describe(ctx, db, dir, RoleArchive, rolePath[RoleArchive])
	if err != nil {
		return nil, err
	}
	m.Files = []FileInfo{fi}
	m.Adopted = &Adopted{ArchiveSHA256: opt.SHA256, Records: frows, Content: fsum}
	if err := writeManifest(dir, m); err != nil {
		return nil, err
	}
	return m, nil
}

// copyFileSHA256 copies a file and returns the SHA-256 of what it wrote.
func copyFileSHA256(src, dst string) (string, error) {
	in, err := os.Open(src)
	if err != nil {
		return "", err
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	defer out.Close()
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(out, h), in); err != nil {
		return "", err
	}
	if err := out.Close(); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
