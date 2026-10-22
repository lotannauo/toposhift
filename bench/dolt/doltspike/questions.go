package doltspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

func runQuestions(_ context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("questions", stderr)
	var (
		activity, expect, out, label string
		dropBasis, dropBoot          bool
		limit                        int64
	)
	fs.StringVar(&activity, "activity", "", "the activity file (Parquet) to choose the questions of")
	fs.StringVar(&expect, "expect-activity", "", "the content digest of the file, or a prefix of at least 12 hex digits (required; checked once the whole file is read)")
	fs.StringVar(&out, "out", "", "the directory to write questions.json in; never inside a git work tree")
	fs.StringVar(&label, "label", "", "what the stream is, recorded with the set")
	fs.BoolVar(&dropBasis, "drop-basis", false, "accept records that have an event time basis, and drop it")
	fs.BoolVar(&dropBoot, "drop-boot", false, "accept records that have a boot id, and drop it")
	fs.Int64Var(&limit, "limit", 0, "read only about this many records, in whole batches of 1,000: a set made from a prefix is labelled so")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if activity == "" || out == "" || fs.NArg() != 0 {
		return errors.New("usage: doltspike questions -activity file -expect-activity digest -out dir [-limit n] [-drop-basis] [-drop-boot] [-label text]")
	}
	if expect == "" {
		return errors.New("doltspike: the activity file must be named by its content digest (-expect-activity)")
	}
	if err := RefuseInWorkTree(out); err != nil {
		return err
	}
	if err := checkFree(out, "questions.json"); err != nil {
		return err
	}
	q, err := doltinput.BuildQuestions(activity, doltinput.Options{DropBasis: dropBasis, DropBoot: dropBoot}, limit, label, progress(stderr))
	if err != nil {
		return err
	}
	// The digest in a footer is checked against the rows only if the read reached the end.
	if limit == 0 {
		if err := CheckContentDigest(q.ContentDigest, expect); err != nil {
			return err
		}
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := q.Save(filepath.Join(out, "questions.json")); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "stream: %d records to seq %d, content digest %s\n", q.Records, q.LastSeq, q.ContentDigest)
	fmt.Fprintf(stdout, "dropped: %d records with an event time basis, %d with a boot id\n", q.Dropped.Basis, q.Dropped.Boot)
	fmt.Fprintf(stdout, "old snapshot: token %d at %s\n", q.OldToken, q.OldAt.UTC().Format("2006-01-02T15:04:05.999999999Z"))
	fmt.Fprintf(stdout, "questions: %d, queries digest %s\n", len(q.Queries), q.QueriesDigest)
	fmt.Fprintf(stdout, "longest payload: %d bytes; the registered payload capacity is %d\n", q.MaxPayload, doltmodel.RegisteredPayloadCapacity)
	if q.MaxPayload > doltmodel.RegisteredPayloadCapacity {
		fmt.Fprintln(stdout, "the stream's payloads are over the registered capacity: a load of it is refused until the registration raises the capacity")
	}
	if limit > 0 {
		fmt.Fprintf(stdout, "a prefix of the file (limit %d): for trying the pipeline, not for a result\n", limit)
	}
	return nil
}
