// Command dbhash prints a digest of the keys and values of a Pebble database,
// opened read-only, and counts of the keys by layout-L kind. Two stores built by
// different code from the same stream hold the same bytes if their lines match.
//
//	dbhash <db>...           every key, meta keys included
//	dbhash -data <db>...     only layout L's data keys, those whose first byte is a layer (1 to 4)
//	dbhash -tails <db>...    how long a read of each data prefix is, with and without checkpoints
//
// Each database gets a line "<dir> keys N records R checkpoints C baselines B sha256 <hex>"
// (with -data, the word data follows the directory). The digest is the one
// [pebblekv.DigestRange] computes. The kinds and -data are meaningful for layout L
// only: layout M's keys are ordered by another comparer, which this command does not
// install, so it cannot open a layout-M database.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/cockroachdb/pebble/v2"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

// quiet drops Pebble's logging, which would otherwise put its recovery on the
// output of a tool.
type quiet struct{}

func (quiet) Infof(string, ...any)  {}
func (quiet) Errorf(string, ...any) {}
func (quiet) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf("pebble: "+format, args...))
}

// run is main with its streams: it returns the exit status, 1 if any database
// could not be opened or read (the others still get their lines) and 2 for bad usage.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("dbhash", flag.ContinueOnError)
	fs.SetOutput(stderr)
	data := fs.Bool("data", false, "digest only layout L's data keys (first byte 1 to 4), not the meta keys")
	tailsMode := fs.Bool("tails", false, "report how many keys a read of each data prefix reads, in full and as a tail")
	fs.Usage = func() {
		_, _ = fmt.Fprintln(stderr, "usage: dbhash [-data | -tails] <db>...  (layout L only)")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() == 0 || (*data && *tailsMode) {
		fs.Usage()
		return 2
	}
	status := 0
	for _, dir := range fs.Args() {
		var line string
		var err error
		if *tailsMode {
			line, err = tails(dir)
		} else {
			line, err = hash(dir, *data)
		}
		if err != nil {
			// Say so and go on: the rest of the databases still get their lines.
			_, _ = fmt.Fprintf(stderr, "dbhash: %s: %v\n", dir, err)
			status = 1
			continue
		}
		_, _ = fmt.Fprintln(stdout, line)
	}
	return status
}

func open(dir string) (*pebble.DB, error) {
	return pebble.Open(dir, &pebble.Options{ReadOnly: true, Logger: quiet{}})
}

// hash is the line for one database.
func hash(dir string, dataOnly bool) (line string, err error) {
	db, err := open(dir)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := db.Close(); err == nil && cerr != nil {
			line, err = "", cerr
		}
	}()
	var lo, hi []byte
	label := dir
	if dataOnly {
		lo, hi, label = pebblekv.DataLo, pebblekv.DataHi, dir+" data"
	}
	d, err := pebblekv.DigestRange(db, lo, hi)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s keys %d records %d checkpoints %d baselines %d sha256 %x", label, d.Keys, d.Records, d.Checkpoints, d.Baselines, d.SHA256), nil
}

// tails is the report for one layout-L database: over its data prefixes, how many
// keys a whole-prefix state read reads and how many the tail read does (keys up to
// and including the newest checkpoint and the newest record).
func tails(dir string) (line string, err error) {
	db, err := open(dir)
	if err != nil {
		return "", err
	}
	defer func() {
		if cerr := db.Close(); err == nil && cerr != nil {
			line, err = "", cerr
		}
	}()
	it, err := db.NewIter(&pebble.IterOptions{LowerBound: pebblekv.DataLo})
	if err != nil {
		return "", err
	}
	var fulls, tailsN []int64
	var noCkptKeys, noCkpt, ckFull, ckTail int64
	var cur []byte
	var full, tail int64
	var sawC, sawR, done bool
	flush := func() {
		if cur == nil {
			return
		}
		fulls = append(fulls, full)
		tailsN = append(tailsN, tail)
		if !sawC {
			noCkpt++
			noCkptKeys += full
		} else {
			ckFull += full
			ckTail += tail
		}
	}
	for ok := it.First(); ok; ok = it.Next() {
		k := it.Key()
		if len(k) != 37 {
			continue
		}
		if cur == nil || !bytes.Equal(k[:20], cur) {
			flush()
			cur = slices.Clone(k[:20])
			full, tail, sawC, sawR, done = 0, 0, false, false, false
		}
		full++
		if !done {
			tail++
			switch k[36] {
			case 0:
				sawR = true
			case 1:
				sawC = true
			}
			done = sawC && sawR
		}
	}
	flush()
	if err := errors.Join(it.Error(), it.Close()); err != nil {
		return "", err
	}
	if len(fulls) == 0 {
		return fmt.Sprintf("%s prefixes 0", dir), nil
	}
	sum := func(xs []int64) (s int64) {
		for _, x := range xs {
			s += x
		}
		return
	}
	q := func(xs []int64, f float64) int64 {
		ys := slices.Clone(xs)
		slices.Sort(ys)
		return ys[int(f*float64(len(ys)-1))]
	}
	n := float64(len(fulls))
	return fmt.Sprintf("%s prefixes %d | whole read: mean %.2f p99 %d max %d | tail read: mean %.2f p99 %d max %d | without a checkpoint: %d prefixes, %d keys | with: whole %d tail %d keys",
		dir, len(fulls), float64(sum(fulls))/n, q(fulls, .99), q(fulls, 1), float64(sum(tailsN))/n, q(tailsN, .99), q(tailsN, 1), noCkpt, noCkptKeys, ckFull, ckTail), nil
}
