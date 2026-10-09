package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

const defaultBatch = 1000

const replayUsage = `Usage: toposhift replay activity FILE --data-dir DIR [--store pebble|mem] [--batch N] [--no-layer-check]

Replays an activity file (Parquet, records in ascending sequence number) into a
store, in batches, in file order. The file is read through once to check it
before anything is written (for a store that already holds records, after a
quick look at where the store stands), so a file damaged in a way the reader
detects leaves the store as it was.

  --data-dir DIR     the store's directory, created if it does not exist (pebble)
  --store NAME       pebble (default): the durable store in DIR
                     mem: the reference store, which holds every record in
                     memory, keeps nothing and takes no directory. It is for
                     small files, and checks what reading the file does not:
                     that the store's Write accepts every batch
  --batch N          records per batch (default 1000)
  --no-layer-check   skip the check that every record of one edge is in one
                     layer. The check keeps 17 bytes per distinct edge in
                     memory (about 50 with the map's overhead, so about 1 GB at
                     16 million edges) and sees one file only; a file whose
                     edges are known to be consistent may skip it

The file's first sequence number must be above the store's last, so a file
cannot be replayed into a store twice. If a replay is interrupted, the store
holds part of the file: remove DIR and replay every file into it again, in
order (or use a new directory).

Only one toposhift process can have a directory open at a time, and a replay
holds it until it finishes.
`

// replayCmd is toposhift replay.
func replayCmd(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("toposhift replay", flag.ContinueOnError)
	var o replayOptions
	fs.StringVar(&o.dataDir, "data-dir", "", "the store's directory")
	fs.StringVar(&o.kind, "store", "pebble", "the store: pebble or mem")
	fs.IntVar(&o.batch, "batch", defaultBatch, "records per batch")
	fs.BoolVar(&o.noLayerCheck, "no-layer-check", false, "skip the edge layer check")
	if len(args) > 0 && args[0] == "help" {
		args = []string{"-h"}
	}
	pos, err := parseFlags(fs, args, replayUsage, stdout, stderr)
	if err != nil {
		return parseError(err)
	}
	switch {
	case len(pos) == 0 || pos[0] != "activity":
		err = usagef("say what to replay: toposhift replay activity FILE --data-dir DIR")
	case len(pos) != 2:
		err = usagef("give exactly one activity file")
	case o.batch < 1:
		err = usagef("--batch must be at least 1, not %d", o.batch)
	case o.kind != "pebble" && o.kind != "mem":
		err = usagef("unknown store %q; the stores are pebble and mem", o.kind)
	case o.kind == "pebble" && o.dataDir == "":
		err = usagef("--data-dir is required: nothing is written without one")
	case o.kind == "mem" && o.dataDir != "":
		err = usagef("--store mem keeps nothing, so it takes no --data-dir")
	}
	if err != nil {
		return finish(stderr, "replay", err)
	}
	o.file = pos[1]
	return finish(stderr, "replay", replayActivity(ctx, o, stdout))
}

// parseError turns the error of a failed parse into an exit code: the flag
// package has already printed what was wrong.
func parseError(err error) int {
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	return 2
}

type replayOptions struct {
	file, dataDir, kind string
	batch               int
	noLayerCheck        bool
}

// fileSummary is what a pass over an activity file found.
type fileSummary struct {
	records     int64
	first, last uint64
}

// activityFile is an open activity file, and what it was when it was opened.
type activityFile struct {
	*os.File
	path  string
	size  int64
	mtime time.Time
}

func openActivity(path string) (*activityFile, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open the activity file: %w", err)
	}
	info, err := f.Stat()
	if err == nil && info.IsDir() {
		err = fmt.Errorf("%s is a directory", path)
	}
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("cannot read the activity file: %w", err)
	}
	return &activityFile{File: f, path: path, size: info.Size(), mtime: info.ModTime()}, nil
}

// reader starts a reader on the file, which checks its footer.
func (f *activityFile) reader() (*activity.Reader, error) {
	rd, err := activity.NewReader(f.File, f.size, activity.ReaderOptions{})
	if err != nil {
		return nil, fmt.Errorf("%s is not a usable activity file: %w", f.path, err)
	}
	return rd, nil
}

// unchanged reports whether the file still has the size and modification time it
// had when it was opened.
func (f *activityFile) unchanged() bool {
	info, err := f.Stat()
	return err == nil && info.Size() == f.size && info.ModTime().Equal(f.mtime)
}

// edgeKey is the first 128 bits of the SHA-256 of an edge's subject: its source,
// target and relation. Two edges collide with a probability of about n^2/2^129
// for n distinct edges, which is nothing at any n a file can hold.
type edgeKey [16]byte

// keyer makes edge keys, reusing one buffer.
type keyer struct{ buf []byte }

// key hashes the types and hashes of the two ends and the relation, and keeps
// 128 bits. A type name is followed by a zero byte (no name contains one) and
// each end's hash has a fixed length, so two different edges never give the
// same input.
func (k *keyer) key(s store.Subject) edgeKey {
	b := k.buf[:0]
	a, c := s.A.Hash(), s.B.Hash()
	b = append(b, s.A.Type()...)
	b = append(b, 0)
	b = append(b, a[:]...)
	b = append(b, 0)
	b = append(b, s.B.Type()...)
	b = append(b, 0)
	b = append(b, c[:]...)
	b = append(b, 0)
	b = append(b, s.Relation...)
	k.buf = b
	sum := sha256.Sum256(b)
	return edgeKey(sum[:16])
}

// checkFile reads the whole file once, checking everything the reader checks and,
// unless noLayerCheck, that every record of one edge carries the same layer.
//
// The layer rule is the caller's precondition for [store.Store.Write], which a
// store is not required to detect (it would cost a lookup per record), so the
// replay is the place to see it, and only within the one file: an edge that an
// earlier file put in another layer is not seen. The check keeps, for each
// distinct edge in the file, a 16 byte key and the layer in one byte: 17 bytes,
// about 50 with the map's overhead, so about 1 GB at 16 million distinct edges.
// Entities need no check, as the reader validates each against the catalog.
func checkFile(ctx context.Context, rd *activity.Reader, path string, noLayerCheck bool) (fileSummary, error) {
	edges := make(map[edgeKey]catalog.Layer)
	var keys keyer
	var sum fileSummary
	for {
		if sum.records%1024 == 0 {
			if err := ctx.Err(); err != nil {
				return sum, fmt.Errorf("stopped while checking %s: %w", path, err)
			}
		}
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return sum, nil
		}
		if err != nil {
			return sum, fmt.Errorf("%s is not a usable activity file: %w", path, err)
		}
		if sum.records == 0 {
			sum.first = rec.Seq
		}
		sum.records++
		sum.last = rec.Seq
		if noLayerCheck || rec.Subject.Kind != store.SubjectEdge {
			continue
		}
		k := keys.key(rec.Subject)
		if was, ok := edges[k]; !ok {
			edges[k] = rec.Layer
		} else if was != rec.Layer {
			return sum, fmt.Errorf("%s is not a usable activity file: the edge %s %s %s is in layer %s at seq %d and was in layer %s earlier in the file",
				path, rec.Subject.A, rec.Subject.Relation, rec.Subject.B, rec.Layer, rec.Seq, was)
		}
	}
}

// notAbove is the refusal of a file that does not start above the store's last
// sequence number: the footer's first and last Seq against what the store holds.
func notAbove(o replayOptions, info activity.Info, last uint64) error {
	if last < info.MaxSeq {
		return fmt.Errorf("the store holds records through seq %d, which is inside %s (seq %d to %d): if a replay of it was interrupted, remove %s and replay every file into it again, in order (or use a new directory); otherwise the store holds records that overlap the file",
			last, o.file, info.MinSeq, info.MaxSeq, o.dataDir)
	}
	return fmt.Errorf("the store already holds records up to seq %d and %s starts at seq %d; a file must start above the store's last seq, so this file, or one that overlaps it, was replayed here before",
		last, o.file, info.MinSeq)
}

// replayActivity checks the file, refuses one that does not start above the
// store's last sequence number, writes the records in batches in file order, and
// prints one summary line.
func replayActivity(ctx context.Context, o replayOptions, stdout io.Writer) (err error) {
	f, err := openActivity(o.file)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	rd, err := f.reader()
	if err != nil {
		return err
	}
	info := rd.Info()
	if info.Records == 0 {
		_ = rd.Close()
		_, _ = fmt.Fprintf(stdout, "%s holds no records; nothing to replay\n", o.file)
		return nil
	}

	// A directory that holds a store is looked at first, read-only, so that a
	// file that was replayed already is refused before it is read through, and
	// a Pebble database that is not a toposhift store is refused untouched. A
	// new directory is made only once the file has been read.
	if o.kind == "pebble" {
		holds, err := checkDataDir(o.dataDir)
		if err != nil {
			_ = rd.Close()
			return err
		}
		if holds {
			last, err := peekLastSeq(o.dataDir)
			if err != nil {
				_ = rd.Close()
				return err
			}
			if info.MinSeq <= last {
				_ = rd.Close()
				return notAbove(o, info, last)
			}
		}
	}

	sum, err := checkFile(ctx, rd, o.file, o.noLayerCheck)
	_ = rd.Close()
	if err != nil {
		return err
	}

	st, err := openReplayStore(o.kind, o.dataDir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing the store: %w", cerr)
		}
	}()
	if last := st.LastSeq(); sum.first <= last {
		return notAbove(o, info, last)
	}
	if !f.unchanged() {
		return fmt.Errorf("%s changed after it was checked; nothing was written", o.file)
	}

	rd, err = f.reader()
	if err != nil {
		return err
	}
	defer func() { _ = rd.Close() }()

	before := st.LastSeq()
	start := time.Now()
	var stored, batches int64
	buf := make([]store.Record, 0, o.batch)
	flush := func() error {
		first, last := buf[0].Seq, buf[len(buf)-1].Seq
		if err := st.Write(ctx, buf); err != nil {
			return writeFailure(o, batches+1, first, last, err, st, before)
		}
		stored += int64(len(buf))
		batches++
		buf = buf[:0]
		return nil
	}
	for {
		rec, rerr := rd.Next()
		if errors.Is(rerr, io.EOF) {
			break
		}
		if rerr != nil {
			// The file was checked whole a moment ago, so this is a file that
			// changed under the replay.
			return fmt.Errorf("%s changed while it was replayed (%w); the store holds records up to seq %d",
				o.file, rerr, st.LastSeq())
		}
		buf = append(buf, rec)
		if len(buf) == o.batch {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if len(buf) > 0 {
		if err := flush(); err != nil {
			return err
		}
	}
	// Close inside the timing: a store may have work left to do when it closes.
	if err := st.Close(); err != nil {
		return fmt.Errorf("closing the store: %w", err)
	}
	elapsed := time.Since(start)

	rate := 0.0
	if elapsed > 0 {
		rate = float64(stored) / elapsed.Seconds()
	}
	note := ""
	if o.kind == "mem" {
		note = " (in memory; nothing was kept)"
	}
	_, _ = fmt.Fprintf(stdout, "replayed %d records in %d batches, seq %d to %d, written in %s, %.0f records/s%s\n",
		stored, batches, sum.first, sum.last, elapsed.Round(time.Millisecond), rate, note)
	return nil
}

// writeFailure is the error of a batch the store did not take. After a failure
// the caller learns from LastSeq whether the batch was stored, except when the
// store has lost track of its own commit, which only a reopening settles. If the
// store holds some of the file, a replay cannot go on from there, and the
// directory has to be started again.
func writeFailure(o replayOptions, n int64, first, last uint64, err error, st store.Store, before uint64) error {
	switch now := st.LastSeq(); {
	// This depends on the text of the error pebblestore gives when it has lost
	// track of a commit (see uncertainCommit in pebblestore/write.go), which ends
	// "reopen the store": the store offers no other way to tell that error apart.
	case strings.Contains(err.Error(), "reopen the store"):
		return fmt.Errorf("batch %d (seq %d to %d) failed: %w; whether it was stored is known only when the store is reopened: run this replay again, which says where the store stands",
			n, first, last, err)
	case now == before:
		return fmt.Errorf("batch %d (seq %d to %d) failed: %w; nothing of %s was stored", n, first, last, err, o.file)
	case o.kind == "mem":
		return fmt.Errorf("batch %d (seq %d to %d) failed: %w", n, first, last, err)
	default:
		return fmt.Errorf("batch %d (seq %d to %d) failed: %w; the store holds records up to seq %d, so it holds a partial replay of %s: remove %s and replay every file into it again, in order (or use a new directory)",
			n, first, last, err, now, o.file, o.dataDir)
	}
}
