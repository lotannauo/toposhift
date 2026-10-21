package duckengine

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
)

// A connection in [ModeObserve] logs every read DuckDB makes of a file. That is the
// ground truth of what a statement admitted: the footers say which row groups a scan may
// skip, the log says which it did not. It is an audit of the counting, run on a
// connection that caches nothing; it is not itself a counter, since which reads reach
// the file system is the cache's business.

// Read is one read the file system was asked for.
type Read struct {
	Path       string
	Pos, Bytes int64
}

// ObservedFile is what a statement read of one file: the row groups of which a column's
// pages were read, and the bytes of pages, bloom filters and footer read in all.
type ObservedFile struct {
	Role Role
	// Groups are the row groups read, by position in the file, or, for several files, in
	// the list of all their row groups in order; Files and FilesRead are, for several files,
	// how many had any read of them and how many had a column's pages read.
	Groups           []int
	Files, FilesRead int
	// DataBytes are the bytes read of column pages; BloomBytes of bloom filters;
	// OtherBytes of the footer and what else is outside a column chunk.
	DataBytes, BloomBytes, OtherBytes int64
}

func (d *DB) startLogging(ctx context.Context) error {
	if _, err := d.conn.ExecContext(ctx, "CALL enable_logging('FileSystem')"); err != nil {
		return fmt.Errorf("duckengine: starting the file system log: %w", err)
	}
	return nil
}

func (d *DB) truncateLogs(ctx context.Context) error {
	if _, err := d.conn.ExecContext(ctx, "CALL truncate_duckdb_logs()"); err != nil {
		return fmt.Errorf("duckengine: clearing the file system log: %w", err)
	}
	return nil
}

// fsReads returns the reads in the log, in the order they were made.
func (d *DB) fsReads(ctx context.Context) ([]Read, error) {
	rows, err := d.conn.QueryContext(ctx, "SELECT message FROM duckdb_logs WHERE type = 'FileSystem' ORDER BY timestamp")
	if err != nil {
		return nil, fmt.Errorf("duckengine: reading the file system log: %w", err)
	}
	defer rows.Close()
	var out []Read
	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			return nil, err
		}
		var e map[string]string
		if err := json.Unmarshal([]byte(msg), &e); err != nil {
			return nil, fmt.Errorf("duckengine: a file system log entry %q: %w", msg, err)
		}
		if e["op"] != "READ" {
			continue
		}
		n, err1 := strconv.ParseInt(e["bytes"], 10, 64)
		pos, err2 := strconv.ParseInt(e["pos"], 10, 64)
		if err1 != nil || err2 != nil {
			return nil, fmt.Errorf("duckengine: a file system read %q has no size or position", msg)
		}
		out = append(out, Read{Path: filepath.Clean(e["path"]), Pos: pos, Bytes: n})
	}
	return out, rows.Err()
}

// Observe says what a set of reads of this file touched: the row groups with a column
// chunk read, and the bytes by kind. A read is of a bloom filter if it falls in one's
// range, of a column chunk if it falls in one's, and otherwise of the footer.
func (m *FileMeta) Observe(role Role, reads []Read) ObservedFile {
	o := ObservedFile{Role: role}
	hit := make(map[int]bool)
	touched := make(map[int]bool)
	paths := m.paths()
	byPath := make(map[string]int, len(paths))
	for i, p := range paths {
		byPath[filepath.Clean(p)] = i
	}
	for _, r := range reads {
		fi, ok := byPath[r.Path]
		if !ok {
			continue
		}
		touched[fi] = true
		kind := 'o'
		group := -1
	search:
		for gi, g := range m.Groups {
			if g.file != fi {
				continue
			}
			for _, c := range g.cols {
				if c.bloom && r.Pos >= c.bloomAt && r.Pos < c.bloomAt+c.bloomLen {
					kind, group = 'b', gi
					break search
				}
				if r.Pos >= c.start && r.Pos < c.start+c.compressed {
					kind, group = 'd', gi
					break search
				}
			}
		}
		switch kind {
		case 'b':
			o.BloomBytes += r.Bytes
		case 'd':
			o.DataBytes += r.Bytes
			hit[group] = true
		default:
			o.OtherBytes += r.Bytes
		}
	}
	readFiles := make(map[int]bool)
	for g := range hit {
		o.Groups = append(o.Groups, g)
		readFiles[m.Groups[g].file] = true
	}
	sort.Ints(o.Groups)
	if len(m.Files) > 0 {
		o.Files, o.FilesRead = len(touched), len(readFiles)
	}
	return o
}
