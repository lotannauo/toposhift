package xtspike

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
)

func (a *App) runQuestions(args []string) error {
	fs := newFlags("questions", a.Stderr)
	var (
		out, activity, expect, label string
		dropBasis, dropBoot          bool
		limit                        int64
	)
	fs.StringVar(&out, "out", "", "the directory questions.json is written to; never inside a git work tree")
	fs.StringVar(&activity, "activity", "", "the activity file (Parquet) to ask about")
	fs.StringVar(&expect, "expect-activity", "", "the content digest of the activity file, or a prefix of at least 12 hex digits (required)")
	fs.StringVar(&label, "label", "", "what the stream is, recorded with every figure from it")
	fs.BoolVar(&dropBasis, "drop-basis", false, "accept records that have an event time basis, and drop it (counted)")
	fs.BoolVar(&dropBoot, "drop-boot", false, "accept records that have a boot id, and drop it (counted)")
	fs.Int64Var(&limit, "limit", 0, "read only about this many records, in whole batches (a prefix, for trying the pipeline); 0 reads the whole file")
	if err := fs.Parse(args); err != nil {
		return err
	}
	switch {
	case fs.NArg() != 0 || activity == "":
		return errors.New("usage: xtspike questions -activity file.parquet -expect-activity digest -out dir [-drop-basis] [-drop-boot] [-limit n] [-label text]")
	case expect == "":
		return errors.New("xtspike: -activity must be named by its content digest (-expect-activity)")
	case out == "":
		return errors.New("xtspike: -out is required")
	case limit < 0:
		return fmt.Errorf("xtspike: -limit %d", limit)
	}
	if err := RefuseInWorkTree(out); err != nil {
		return err
	}
	if err := checkFree(out, "questions.json"); err != nil {
		return err
	}
	set, err := xtinput.BuildQuestions(activity, xtinput.Options{DropBasis: dropBasis, DropBoot: dropBoot}, limit, label, a.say)
	if err != nil {
		return err
	}
	if err := CheckContentDigest(set.ContentDigest, expect); err != nil {
		return err
	}
	if err := checkFree(out, "questions.json"); err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o700); err != nil {
		return err
	}
	if err := set.Save(filepath.Join(out, "questions.json")); err != nil {
		return err
	}
	sum, err := fileSHA(filepath.Join(out, "questions.json"))
	if err != nil {
		return err
	}
	a.out("questions: %d questions over %d records (last seq %d), queries digest %s", len(set.Queries), set.Records, set.LastSeq, set.QueriesDigest)
	a.out("questions.json sha256 %s (name it with -expect-questions)", sum)
	a.out("old token %d; content digest %s", set.OldToken, set.ContentDigest)
	a.out("dropped: %d records with an event time basis, %d with a boot id", set.Stats.BasisDropped, set.Stats.BootDropped)
	a.out("records with a boundary off a whole microsecond: %d", set.Stats.OffMicrosecond)
	if set.Stats.OffMicrosecond > 0 {
		a.out("XTDB cannot hold these records: a load of this stream is refused (a capability row, decided by the owner, never dropped silently)")
	}
	return nil
}
