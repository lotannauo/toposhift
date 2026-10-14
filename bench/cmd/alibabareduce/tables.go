package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
)

// tsvWriter writes a tab-separated table with a header row, gzip'd if the path
// ends in .gz. The gzip header carries no time, so equal tables are equal bytes.
type tsvWriter struct {
	f    *os.File
	zw   *gzip.Writer
	bw   *bufio.Writer
	rows int64
	err  error
}

func newTSV(path string, header ...string) (*tsvWriter, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	t := &tsvWriter{f: f}
	if strings.HasSuffix(path, ".gz") {
		t.zw = gzip.NewWriter(f)
		t.bw = bufio.NewWriterSize(t.zw, 1<<20)
	} else {
		t.bw = bufio.NewWriterSize(f, 1<<20)
	}
	t.line(header)
	t.rows = 0
	return t, nil
}

func (t *tsvWriter) line(cols []string) {
	if t.err != nil {
		return
	}
	for i, c := range cols {
		if i > 0 {
			t.err = t.bw.WriteByte('\t')
		}
		if t.err == nil {
			_, t.err = t.bw.WriteString(c)
		}
	}
	if t.err == nil {
		t.err = t.bw.WriteByte('\n')
	}
	t.rows++
}

// row writes one data row.
func (t *tsvWriter) row(cols ...string) { t.line(cols) }

// close finishes the file and returns the first error of any write.
func (t *tsvWriter) close() error {
	err := t.err
	err = errors.Join(err, t.bw.Flush())
	if t.zw != nil {
		err = errors.Join(err, t.zw.Close())
	}
	return errors.Join(err, t.f.Close())
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

func btoa(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

// quantiles is the nearest-rank quantiles of a sorted sample: the smallest
// value with at least p of the sample at or below it.
func quantile(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[min(max(i, 0), len(sorted)-1)]
}

// dist summarises a sample of integers.
type dist struct {
	N    int64   `json:"n"`
	Min  int64   `json:"min"`
	P50  int64   `json:"p50"`
	P90  int64   `json:"p90"`
	P99  int64   `json:"p99"`
	Max  int64   `json:"max"`
	Mean float64 `json:"mean"`
}

func summarize(vals []int64) dist {
	if len(vals) == 0 {
		return dist{}
	}
	s := slices.Clone(vals)
	slices.Sort(s)
	var sum float64
	for _, v := range s {
		sum += float64(v)
	}
	return dist{
		N: int64(len(s)), Min: s[0], P50: quantile(s, 0.5), P90: quantile(s, 0.9), P99: quantile(s, 0.99),
		Max: s[len(s)-1], Mean: math.Round(sum/float64(len(s))*10000) / 10000,
	}
}

// histDist summarises a histogram (value to frequency).
func histDist(h map[int64]int64) dist {
	if len(h) == 0 {
		return dist{}
	}
	vals := make([]int64, 0, len(h))
	var n int64
	for v, c := range h {
		vals = append(vals, v)
		n += c
	}
	slices.Sort(vals)
	at := func(p float64) int64 {
		need := int64(math.Ceil(p * float64(n)))
		var acc int64
		for _, v := range vals {
			acc += h[v]
			if acc >= need {
				return v
			}
		}
		return vals[len(vals)-1]
	}
	var sum float64
	for v, c := range h {
		sum += float64(v) * float64(c)
	}
	return dist{
		N: n, Min: vals[0], P50: at(0.5), P90: at(0.9), P99: at(0.99), Max: vals[len(vals)-1],
		Mean: math.Round(sum/float64(n)*10000) / 10000,
	}
}
