package filestream

import (
	"bufio"
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/duckdb/duckmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// fileSink writes a stream to an activity file.
type fileSink struct {
	w *activity.Writer
}

func (s *fileSink) Write(batch []engine.Record) error {
	for _, r := range batch {
		sr, err := duckmodel.StoreRecord(r)
		if err != nil {
			return err
		}
		if err := s.w.Write(sr); err != nil {
			return err
		}
	}
	return nil
}
func (s *fileSink) Retain(time.Time) error { return nil }

// generated is a stream the runner made, and the file it was written to.
type generated struct {
	spec   runner.Spec
	info   runner.StreamInfo
	path   string
	digest string
}

func (g generated) options() Options {
	return Options{Period: Period{Start: g.spec.Workload.Start, Duration: g.spec.Workload.Duration}, BatchSize: g.spec.BatchSize, Digest: g.digest}
}

// generate runs the runner's Drive over a workload, with no retention, and writes the
// stream to an activity file.
func generate(t testing.TB, cfg workload.Config, batch int) generated {
	t.Helper()
	spec := runner.DefaultSpec(cfg)
	spec.BatchSize = batch
	spec.MinNonEmpty = 0
	spec.Retentions = nil
	path := filepath.Join(t.TempDir(), "activity.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	bw := bufio.NewWriter(f)
	w, err := activity.NewWriter(bw, activity.WriterOptions{})
	if err != nil {
		t.Fatal(err)
	}
	info, err := runner.Drive(context.Background(), spec, &fileSink{w: w})
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return generated{spec: spec, info: info, path: path, digest: digestOf(t, path)}
}

func digestOf(t testing.TB, path string) string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, _ := f.Stat()
	r, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	return r.Info().Digest
}

// streams are the workloads the replay is held to the runner's description of: batches
// of different sizes, with the late records, runs and extensions the generator makes.
func streams(t testing.TB) []generated {
	t.Helper()
	runs := func() workload.Config {
		c := workload.Tiny()
		c.CoalesceRuns, c.ExtendTTLFraction = true, 0.5
		return c
	}
	inOrder := func() workload.Config {
		c := workload.Tiny()
		c.LateProbability = 0
		return c
	}
	var out []generated
	for _, c := range []struct {
		seed  uint64
		batch int
		cfg   func() workload.Config
	}{
		{1, 64, workload.Tiny},
		{2, 37, workload.Tiny},
		{3, 1000, workload.Tiny},
		{4, 1000, workload.Small},
		{5, 257, workload.Small},
		{6, 64, runs},
		{7, 1000, runs},
		{8, 1, inOrder},
	} {
		cfg := c.cfg()
		cfg.Seed = c.seed
		out = append(out, generate(t, cfg, c.batch))
	}
	return out
}

func replayInfo(t testing.TB, g generated) runner.StreamInfo {
	t.Helper()
	info, err := Replay(context.Background(), g.path, g.options())
	if err != nil {
		t.Fatal(err)
	}
	return info
}

// TestReplayIsTheRunnersStream: the stream replayed from a file of the records the
// runner generated is described as the runner described it, field for field: the digest,
// the counts, the old snapshot token and the instant it is read at, and the period.
func TestReplayIsTheRunnersStream(t *testing.T) {
	var shaped int
	for i, g := range streams(t) {
		got := replayInfo(t, g)
		if !reflect.DeepEqual(got, g.info) {
			t.Errorf("stream %d: the replay is\n%+v\nthe runner's is\n%+v", i, got, g.info)
		}
		nominal := g.info.Start.Add(g.spec.Workload.Duration / 4 * 3)
		if !g.info.OldAt.Equal(nominal) && g.info.OldToken > 0 {
			shaped++
		}
	}
	if shaped == 0 {
		t.Error("no stream has an old snapshot instant that differs from the nominal one: the replay is not held to how it is found")
	}
}

// TestReplayWritesEveryRecordInBatches: the sinks see the records of the file, once, in
// batches of the size asked for but the last, in ascending Seq.
func TestReplayWritesEveryRecordInBatches(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	var sizes []int
	var seqs []uint64
	rec := sinkFunc(func(b []engine.Record) error {
		sizes = append(sizes, len(b))
		for _, r := range b {
			seqs = append(seqs, r.Seq)
		}
		return nil
	})
	info, err := Replay(context.Background(), g.path, g.options(), rec)
	if err != nil {
		t.Fatal(err)
	}
	if uint64(len(seqs)) != info.Records || info.Records != g.info.Records {
		t.Fatalf("%d records seen, %d replayed, %d generated", len(seqs), info.Records, g.info.Records)
	}
	for i, n := range sizes {
		if n != 64 && i != len(sizes)-1 {
			t.Errorf("batch %d has %d records", i, n)
		}
	}
	for i := 1; i < len(seqs); i++ {
		if seqs[i] <= seqs[i-1] {
			t.Fatalf("seq %d after %d", seqs[i], seqs[i-1])
		}
	}
	if seqs[len(seqs)-1] != info.LastSeq {
		t.Errorf("last seq %d, the description says %d", seqs[len(seqs)-1], info.LastSeq)
	}
}

type sinkFunc func([]engine.Record) error

func (f sinkFunc) Write(b []engine.Record) error { return f(b) }
func (sinkFunc) Retain(time.Time) error          { return nil }

// TestTheFaultsOfTheReplayAreCaught: each deliberate fault changes the description of at
// least one of the streams, so that the equivalence above would fail.
func TestTheFaultsOfTheReplayAreCaught(t *testing.T) {
	gs := streams(t)
	defer func() { fault = mutNone }()
	for name, f := range map[string]mutant{
		"the old token taken after the batch":       mutTokenAfterBatch,
		"the old instant not the newest event time": mutOldAtNominal,
		"the bound of the records after the token":  mutNoUnseen,
		"the through time left out of the digest":   mutDigestWithoutThrough,
		"the last, shorter batch left out":          mutLastBatchDropped,
	} {
		fault = f
		caught := false
		for _, g := range gs {
			info, err := Replay(context.Background(), g.path, g.options())
			if err != nil || !reflect.DeepEqual(info, g.info) {
				caught = true
			}
		}
		fault = mutNone
		if !caught {
			t.Errorf("%s: the replay of every stream is still the runner's", name)
		}
	}
}

func TestReplayRefusals(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	ctx := context.Background()
	good := g.options()
	other := "sha256:" + strings.Repeat("0", 64)

	for name, c := range map[string]struct {
		mod  func(*Options)
		want error
	}{
		"another digest":       {func(o *Options) { o.Digest = other }, ErrDigest},
		"no digest":            {func(o *Options) { o.Digest = "" }, nil},
		"an upper case digest": {func(o *Options) { o.Digest = strings.ToUpper(o.Digest) }, nil},
		"a period that starts later": {func(o *Options) {
			o.Period.Start = o.Period.Start.Add(10 * time.Minute)
			o.Period.Duration -= 10 * time.Minute
		}, ErrPeriod},
		"a period that ends earlier": {func(o *Options) { o.Period.Duration = 5 * time.Minute }, ErrPeriod},
		"a period with no duration":  {func(o *Options) { o.Period.Duration = 0 }, nil},
		"a period before 1970":       {func(o *Options) { o.Period.Start = time.Unix(-1, 0) }, nil},
	} {
		o := good
		c.mod(&o)
		_, err := Replay(ctx, g.path, o)
		if err == nil {
			t.Errorf("%s was accepted", name)
		} else if c.want != nil && !errors.Is(err, c.want) {
			t.Errorf("%s: %v, not %v", name, err, c.want)
		}
	}
	if _, err := Replay(ctx, filepath.Join(t.TempDir(), "none.parquet"), good); err == nil {
		t.Error("a file that is not there")
	}
}

// TestReplayRefusesDamage: a file whose bytes were changed is refused, not replayed as
// another stream.
func TestReplayRefusesDamage(t *testing.T) {
	g := generate(t, workload.Tiny(), 64)
	b, err := os.ReadFile(g.path)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)/2] ^= 0xff
	damaged := filepath.Join(t.TempDir(), "damaged.parquet")
	if err := os.WriteFile(damaged, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Replay(context.Background(), damaged, g.options()); err == nil {
		t.Error("a damaged file was replayed")
	}
}

// TestTheConversionDropsTheBasisAndRefusesABootId: the clock that stamped a record's
// time changes no answer, so a file whose records say which clock they were stamped by is
// replayed; a boot id says the file holds host observations, which this baseline would
// answer without it, so a record that has one is refused and not silently stripped.
func TestTheConversionDropsTheBasisAndRefusesABootId(t *testing.T) {
	if _, err := duckmodel.EngineRecord(store.Record{Boot: "boot-1"}, convert); err == nil {
		t.Error("a boot id was dropped silently")
	}
	rec := store.Record{Subject: store.Subject{Kind: store.SubjectEntity}, EventTimeBasis: store.BasisProducerEvent}
	if _, err := duckmodel.EngineRecord(rec, convert); err != nil {
		t.Errorf("an event time basis was refused: %v", err)
	}
}

// TestARecordMustBeInsideThePeriodAndItsSeqAToken tests the check of a record by itself.
func TestARecordMustBeInsideThePeriodAndItsSeqAToken(t *testing.T) {
	start := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	ok := engine.Record{Seq: 1, EventTime: start.Add(time.Minute)}
	if err := checkRecord(ok, start, end); err != nil {
		t.Fatal(err)
	}
	for name, r := range map[string]engine.Record{
		"a seq above the largest int64":   {Seq: math.MaxInt64 + 1, EventTime: start},
		"an event time before the start":  {Seq: 1, EventTime: start.Add(-time.Nanosecond)},
		"an event time after the end":     {Seq: 1, EventTime: end.Add(time.Nanosecond)},
		"a through time after the end":    {Seq: 1, EventTime: start, Through: end.Add(time.Nanosecond)},
		"a through time before the start": {Seq: 1, EventTime: start, Through: start.Add(-time.Nanosecond)},
	} {
		if checkRecord(r, start, end) == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	for name, r := range map[string]engine.Record{
		"an event time at the start": {Seq: 1, EventTime: start},
		"an event time at the end":   {Seq: 1, EventTime: end},
		"a through time at the end":  {Seq: 1, EventTime: start, Through: end},
	} {
		if err := checkRecord(r, start, end); err != nil {
			t.Errorf("%s was refused: %v", name, err)
		}
	}
}
