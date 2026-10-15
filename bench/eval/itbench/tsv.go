package itbench

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// rows reads one of the dataset's exports: tab-separated, a header line of column
// names, and a field that holds a tab or a line break quoted with double quotes
// (a quote inside it written twice). Rows come to fn with the columns by name;
// the row is only valid during the call.
func rows(path string, required []string, fn func(row *row) error) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	r := csv.NewReader(f)
	r.Comma = '\t'
	r.LazyQuotes = true
	r.FieldsPerRecord = -1
	r.ReuseRecord = true
	head, err := r.Read()
	if err != nil {
		return fmt.Errorf("itbench: %s: header: %w", base(path), err)
	}
	cols := make(map[string]int, len(head))
	for i, h := range head {
		cols[h] = i
	}
	for _, c := range required {
		if _, ok := cols[c]; !ok {
			return fmt.Errorf("itbench: %s: no column %s", base(path), c)
		}
	}
	rw := &row{cols: cols}
	for n := 1; ; n++ {
		rec, err := r.Read()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("itbench: %s: row %d: %w", base(path), n, err)
		}
		if len(rec) != len(head) {
			return fmt.Errorf("itbench: %s: row %d has %d fields, the header %d", base(path), n, len(rec), len(head))
		}
		rw.rec, rw.n = rec, n
		if err := fn(rw); err != nil {
			return fmt.Errorf("itbench: %s: row %d: %w", base(path), n, err)
		}
	}
}

func base(p string) string {
	if i := strings.LastIndexByte(p, '/'); i >= 0 {
		return p[i+1:]
	}
	return p
}

type row struct {
	cols map[string]int
	rec  []string
	n    int
}

func (r *row) get(col string) string { return r.rec[r.cols[col]] }

// exportTime is the layout of a time in the exports: UTC, with no zone, and up to
// nanoseconds.
const exportTime = "2006-01-02 15:04:05.999999999"

func parseExportTime(s string) (time.Time, error) {
	t, err := time.ParseInLocation(exportTime, s, time.UTC)
	if err != nil {
		return time.Time{}, fmt.Errorf("a time that is not %q", exportTime)
	}
	return t, nil
}
