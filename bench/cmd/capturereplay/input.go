package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// A run file is read twice: once to find where every line begins and what order its
// records are in (a light read that keeps one small ref per record), and once to
// replay them, a line at a time, so that no run is ever held decoded.

// lineSpan says where a line of a run file is.
type lineSpan struct {
	Number int
	Offset int64
	// Size is the length of the line without its newline.
	Size int64
}

// runFile is a run file after its first read.
type runFile struct {
	Path string
	N    int // the run's number, from 1
	// Size is the file's bytes.
	Size  int64
	Lines []lineSpan
	// Refs are the records in file order: line by line, and within a line in document
	// order. That is the order the collector exported them in.
	Refs []capture.LogRef
	// MovedBySort is how many records capture.SortRefs would put in another place than the
	// file order has them, and MaxShift the furthest any of them would move. SortErr is
	// the error it gave, if it gave one.
	MovedBySort int
	MaxShift    int
	SortErr     error
}

// scanRun reads a run file's lines and the refs of their records, and measures how much
// capture.SortRefs would reorder them.
func scanRun(path string, n int) (_ *runFile, err error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	rf := &runFile{Path: path, N: n}
	r := capture.NewReader(f, capture.ReaderOptions{})
	for {
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		idx := len(rf.Lines)
		rf.Lines = append(rf.Lines, lineSpan{Number: l.Number, Offset: l.Offset, Size: int64(len(l.Raw))})
		refs, err := capture.LogRefs(l, idx)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		rf.Refs = append(rf.Refs, refs...)
		rf.Size = l.Offset + int64(len(l.Raw))
		if sig := r.Signal(); sig != capture.Logs {
			return nil, fmt.Errorf("%s: a %s capture, not logs", path, sig)
		}
	}
	// The last line may lack its newline; Size is then exact, otherwise it counts the
	// newline the file has.
	if st, err := f.Stat(); err == nil {
		rf.Size = st.Size()
	}
	rf.MovedBySort, rf.MaxShift, rf.SortErr = sortMoves(rf.Refs)
	return rf, nil
}

// sortMoves says how many of the refs capture.SortRefs would put somewhere else, and
// how far the furthest of them would go.
func sortMoves(refs []capture.LogRef) (moved, maxShift int, err error) {
	sorted := slices.Clone(refs)
	if err := capture.SortRefs(sorted); err != nil {
		return 0, 0, err
	}
	// Where each ref is in the sorted order, by its position in the file.
	type pos struct{ index, resource, scope, record int }
	where := make(map[pos]int, len(sorted))
	for i, r := range sorted {
		where[pos{r.Index, r.Resource, r.Scope, r.Record}] = i
	}
	for i, r := range refs {
		j := where[pos{r.Index, r.Resource, r.Scope, r.Record}]
		if j == i {
			continue
		}
		moved++
		d := j - i
		if d < 0 {
			d = -d
		}
		maxShift = max(maxShift, d)
	}
	return moved, maxShift, nil
}

// replayOrder is the order a run's records are replayed in: the order of the file. The
// collector exported them in it, and the ingest path will see them in it.
func replayOrder(refs []capture.LogRef) ([]capture.LogRef, error) {
	return refs, nil
}

// item is one log record with its resource.
type item struct {
	res capture.Resource
	rec capture.LogRecord
	ref capture.LogRef
}

// batch is the consecutive records of one line: what the collector delivered in one
// export.
type batch struct {
	// Line is the index into runFile.Lines, and Bytes the line's length with its newline.
	Line  int
	Bytes int64
	Items []item
}

// walk reads the records in the given order, one line at a time, and calls visit with
// each maximal run of consecutive records that are on one line. A line that does not
// decode is an error.
func (rf *runFile) walk(order []capture.LogRef, visit func(batch) error) (err error) {
	f, err := os.Open(rf.Path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	for i := 0; i < len(order); {
		idx := order[i].Index
		j := i
		for j < len(order) && order[j].Index == idx {
			j++
		}
		span := rf.Lines[idx]
		lr := capture.NewReader(io.NewSectionReader(f, span.Offset, span.Size+1), capture.ReaderOptions{})
		line, err := lr.Next()
		if err != nil {
			return fmt.Errorf("%s:%d: %w", rf.Path, span.Number, err)
		}
		data, err := capture.DecodeLogs(line.Raw)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", rf.Path, span.Number, err)
		}
		b := batch{Line: idx, Bytes: span.Size + 1}
		for _, ref := range order[i:j] {
			if ref.Resource >= len(data.ResourceLogs) ||
				ref.Scope >= len(data.ResourceLogs[ref.Resource].ScopeLogs) ||
				ref.Record >= len(data.ResourceLogs[ref.Resource].ScopeLogs[ref.Scope].LogRecords) {
				return fmt.Errorf("%s:%d: the scan and the replay disagree about the records of the line", rf.Path, span.Number)
			}
			rl := data.ResourceLogs[ref.Resource]
			b.Items = append(b.Items, item{res: rl.Resource, rec: rl.ScopeLogs[ref.Scope].LogRecords[ref.Record], ref: ref})
		}
		if err := visit(b); err != nil {
			return err
		}
		i = j
	}
	return nil
}

// readLine reads one line of the file again, by its index.
func (rf *runFile) readLine(idx int) (_ []byte, err error) {
	f, err := os.Open(rf.Path)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	span := rf.Lines[idx]
	line, err := capture.NewReader(io.NewSectionReader(f, span.Offset, span.Size+1), capture.ReaderOptions{}).Next()
	if err != nil {
		return nil, fmt.Errorf("%s:%d: %w", rf.Path, span.Number, err)
	}
	return line.Raw, nil
}
