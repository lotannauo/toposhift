package vminput_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/actfixture"
	"github.com/lotannauo/toposhift/bench/victoria/vminput"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestForEachReturnsTheFilesRecordsAsEngineRecordsInBatches(t *testing.T) {
	rs := actfixture.Placement(actfixture.Default())
	path := actfixture.Write(t, t.TempDir(), "s.parquet", rs)
	var got []engine.Record
	var sizes []int
	info, err := vminput.ForEach(path, 100, func(b []engine.Record) error {
		sizes = append(sizes, len(b))
		got = append(got, b...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if info.Records != int64(len(rs)) || info.Digest == "" || info.MaxSeq != uint64(len(rs)) {
		t.Fatalf("info %+v for %d records", info, len(rs))
	}
	for i, n := range sizes[:len(sizes)-1] {
		if n != 100 {
			t.Fatalf("batch %d has %d records", i, n)
		}
	}
	if len(got) != len(rs) {
		t.Fatalf("%d records read, %d written", len(got), len(rs))
	}
	for i, r := range rs {
		want, err := vminput.Convert(r)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got[i], want) {
			t.Fatalf("record %d: %+v, want %+v", i, got[i], want)
		}
		if got[i].Seq != r.Seq || got[i].Subject.A != r.Subject.A || !got[i].EventTime.Equal(r.EventTime) || got[i].TTL != r.TTL {
			t.Fatalf("record %d does not carry the stored record's identity: %+v", i, got[i])
		}
	}
	// Info reads the footer alone, with the same digest.
	if got, err := vminput.Info(path); err != nil || got.Digest != info.Digest || got.Records != info.Records {
		t.Errorf("Info: %+v, %v", got, err)
	}
	// An error of the function ends the read and is returned as it is; a batch of 0 and a
	// missing file are refused.
	boom := errors.New("boom")
	if _, err := vminput.ForEach(path, 100, func([]engine.Record) error { return boom }); !errors.Is(err, boom) {
		t.Errorf("error: %v", err)
	}
	if _, err := vminput.ForEach(path, 0, func([]engine.Record) error { return nil }); err == nil {
		t.Error("a batch of 0 was accepted")
	}
	if _, err := vminput.ForEach(filepath.Join(t.TempDir(), "missing"), 10, func([]engine.Record) error { return nil }); err == nil {
		t.Error("a missing file was read")
	}
}

func TestConvertRefusesABootIdAndKeepsWhatAnEngineRecordHolds(t *testing.T) {
	host := actfixture.FP(catalog.Host, 1)
	r := store.Record{
		Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "p", EventTime: time.Unix(1_700_000_000, 0).UTC(), Seq: 7,
		Kind: lifecycle.Observe, TTL: time.Minute, Through: time.Unix(1_700_000_100, 0).UTC(), Payload: []byte("x"), EventTimeBasis: store.BasisProducerEvent,
	}
	got, err := vminput.Convert(r)
	if err != nil {
		t.Fatal(err)
	}
	if got.Seq != 7 || got.TTL != time.Minute || !got.Through.Equal(r.Through) || string(got.Payload) != "x" || got.Subject.Kind != engine.SubjectEntity {
		t.Errorf("%+v", got)
	}
	r.Boot = "b"
	if _, err := vminput.Convert(r); err == nil {
		t.Error("a record with a boot id was converted")
	}
}

// The registry of the stream reads its one producer through the TTL its samples carry:
// the converter takes the file's records and refuses a window that is another.
func TestTheAlibabaRegistryAcceptsTheStreamAndOnlyItsWindow(t *testing.T) {
	o := actfixture.Default()
	path := actfixture.Write(t, t.TempDir(), "s.parquet", actfixture.Placement(o))
	convert := func(ttl time.Duration) (vmmodel.Counts, error) {
		reg, err := vminput.AlibabaRegistry(ttl)
		if err != nil {
			t.Fatal(err)
		}
		c, err := vmmodel.NewConverter(vmmodel.Options{Registry: reg})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := vminput.Drive(path, c); err != nil {
			return vmmodel.Counts{}, err
		}
		err = c.Close()
		return c.Counts(), err
	}
	counts, err := convert(vminput.AlibabaC300TTL)
	if err != nil {
		t.Fatal(err)
	}
	rs := actfixture.Placement(o)
	// Every record is a sample (no delete, no watch mode, nothing collapses): the counts
	// are the file's, and a series of each node, each pod and each distinct placement.
	if counts.Samples != uint64(len(rs)) || counts.Markers != 0 || counts.Scraped != 0 || counts.Collapses() != 0 {
		t.Errorf("%d records became %+v", len(rs), counts)
	}
	subjects := map[engine.Subject]struct{}{}
	for _, r := range rs {
		e, err := vminput.Convert(r)
		if err != nil {
			t.Fatal(err)
		}
		subjects[e.Subject] = struct{}{}
	}
	distinct := len(subjects)
	if counts.Series != distinct {
		t.Errorf("%d series, %d distinct subjects", counts.Series, distinct)
	}
	if _, err := convert(10 * time.Minute); !errors.Is(err, vmmodel.ErrWindow) {
		t.Errorf("a registry with another window: %v", err)
	}
}

func TestDriveReadsTheFileIntoSinks(t *testing.T) {
	rs := actfixture.Placement(actfixture.Default())
	path := actfixture.Write(t, t.TempDir(), "s.parquet", rs)
	w := vmmodel.NewWorld()
	st, err := vminput.Drive(path, w)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := vminput.Info(path)
	if st.Records != uint64(len(rs)) || st.LastSeq != uint64(len(rs)) || st.Digest != info.Digest {
		t.Errorf("%+v for %d records, digest %s", st, len(rs), info.Digest)
	}
	// A sink that fails stops the read.
	boom := errors.New("boom")
	if _, err := vminput.Drive(path, failing{boom}); !errors.Is(err, boom) {
		t.Errorf("%v", err)
	}
}

type failing struct{ err error }

func (f failing) Write([]engine.Record) error { return f.err }
func (failing) Retain(time.Time) error        { return nil }

// The question set: chosen from the file with the spike's selector, answered by the
// reference engine fed the whole file, the same every time, and saved and loaded intact.
func TestQuestionsAreTheReferencesAnswersAndAreDeterministic(t *testing.T) {
	ctx := context.Background()
	o := actfixture.Default()
	rs := actfixture.Placement(o)
	path := actfixture.Write(t, t.TempDir(), "s.parquet", rs)

	q, err := vminput.BuildQuestions(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	again, err := vminput.BuildQuestions(ctx, path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q, again) {
		t.Fatal("two builds over one file differ")
	}
	info, _ := vminput.Info(path)
	if q.Digest != info.Digest || q.Records != uint64(len(rs)) || q.LastSeq != uint64(len(rs)) {
		t.Errorf("the questions describe %s (%d records, last seq %d)", q.Digest, q.Records, q.LastSeq)
	}
	if !q.Start.Equal(o.Start) || !q.End.Equal(o.Start.Add(time.Duration(o.Samples-1)*o.Step)) {
		t.Errorf("period %s to %s", q.Start, q.End)
	}
	if q.OldToken == 0 || q.OldToken >= q.LastSeq || q.OldAt.Before(q.Start) || q.OldAt.After(q.End) {
		t.Errorf("old snapshot: token %d at %s", q.OldToken, q.OldAt)
	}

	// The reference engine, fed every record, gives the answers recorded.
	ora := oracle.New()
	for _, r := range rs {
		e, err := vminput.Convert(r)
		if err != nil {
			t.Fatal(err)
		}
		if err := ora.Write([]engine.Record{e}); err != nil {
			t.Fatal(err)
		}
	}
	families := map[string]int{}
	ages := map[string]int{}
	nonEmpty := 0
	for _, qu := range q.Queries {
		a, err := runner.Ask(ora, qu)
		if err != nil {
			t.Fatal(err)
		}
		if a.Digest != qu.Expect || a.Size != qu.Size {
			t.Errorf("%s: recorded %s/%d, the reference gives %s/%d", qu.Name(), qu.Expect, qu.Size, a.Digest, a.Size)
		}
		families[strings.Fields(qu.Group)[0]]++
		ages[qu.Age]++
		if qu.Size > 0 {
			nonEmpty++
		}
	}
	// The three classes the stream has members of are asked, and nothing else.
	for _, f := range []string{"node<-", "pod->", "node"} {
		if families[f] == 0 {
			t.Errorf("no question of family %s: %v", f, families)
		}
	}
	if len(families) != 3 {
		t.Errorf("families %v", families)
	}
	for _, age := range []string{runner.AgeNow, runner.Age1h, runner.AgeOldToken, runner.AgeDead, runner.AgeWindow1h} {
		if ages[age] == 0 {
			t.Errorf("no question at age %s: %v", age, ages)
		}
	}
	if nonEmpty < len(q.Queries)/3 {
		t.Errorf("%d of %d answers are not empty", nonEmpty, len(q.Queries))
	}
	for _, g := range q.Groups {
		if g.Age == runner.AgeDead && g.NonEmpty != 0 {
			t.Errorf("a dead read finds something alive: %+v", g)
		}
	}
	if len(q.Entities()) == 0 {
		t.Error("no entities")
	}

	// Saved and loaded intact; a changed question is refused.
	qp := filepath.Join(t.TempDir(), "questions.json")
	if err := q.Save(qp); err != nil {
		t.Fatal(err)
	}
	back, err := vminput.LoadQuestions(qp)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(q.Queries, back.Queries) || back.Digest != q.Digest || !back.OldAt.Equal(q.OldAt) {
		t.Error("the questions did not survive a save and a load")
	}
	b, _ := os.ReadFile(qp)
	changed := strings.Replace(string(b), `"Rank": 0`, `"Rank": 9`, 1)
	if changed == string(b) {
		t.Fatal("nothing to change")
	}
	if err := os.WriteFile(qp, []byte(changed), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := vminput.LoadQuestions(qp); err == nil {
		t.Error("a changed question was loaded")
	}
}

func TestQuestionsOfAFileThatChangesOrIsEmptyAreRefused(t *testing.T) {
	path := actfixture.Write(t, t.TempDir(), "empty.parquet", nil)
	if _, err := vminput.BuildQuestions(context.Background(), path, nil); err == nil {
		t.Error("questions were built from an empty file")
	}
	if _, err := vminput.BuildQuestions(context.Background(), filepath.Join(t.TempDir(), "missing"), nil); err == nil {
		t.Error("questions were built from a missing file")
	}
}

// The premise of the comparison on an activity file: the product's coalescer, run over the
// every-sample stream, writes a stream of the same existence, and the check says so; a
// stream that differs is refused.
func TestSameFactsOnAnActivityFile(t *testing.T) {
	o := actfixture.Default()
	reg, err := vminput.AlibabaRegistry(o.TTL)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	every := actfixture.Placement(o)
	everyPath := actfixture.Write(t, dir, "s.parquet", every)
	cfg := actfixture.DefaultCoalescer()
	coal := actfixture.Coalesce(t, every, cfg)
	if len(coal) >= len(every) {
		t.Fatalf("the coalescer wrote %d records for %d", len(coal), len(every))
	}
	coalPath := actfixture.Write(t, dir, "c.parquet", coal)

	rep, err := vminput.CheckSameFacts(coalPath, everyPath, reg, vminput.DefaultExtendFraction)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Subjects == 0 || rep.Identical+rep.EndsEarlier != rep.Subjects || rep.EndsEarlier == 0 {
		t.Errorf("%+v", rep)
	}
	// The coalescer extends a run at most once per half its TTL, so an end is early by less
	// than that, and a subject's band is the window and a millisecond.
	if rep.MaxEarly == 0 || rep.MaxEarly >= o.TTL/2 || rep.SmallestBand != o.TTL+vmmodel.Slack {
		t.Errorf("max early %s, smallest band %s", rep.MaxEarly, rep.SmallestBand)
	}

	// Identical files: nothing ends early.
	same, err := vminput.CheckSameFacts(everyPath, everyPath, reg, vminput.DefaultExtendFraction)
	if err != nil || same.EndsEarlier != 0 || same.MaxEarly != 0 {
		t.Errorf("a file against itself: %+v, %v", same, err)
	}

	// A smaller fraction than the coalescer used leaves ends earlier than the check allows.
	if _, err := vminput.CheckSameFacts(coalPath, everyPath, reg, 0.01); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("a bound narrower than the coalescer's: %v", err)
	}
	for _, f := range []float64{0, -1, 1.5} {
		if _, err := vminput.CheckSameFacts(coalPath, everyPath, reg, f); err == nil {
			t.Errorf("fraction %g was accepted", f)
		}
	}

	// An end later in the coalesced stream than in the every-sample one is never allowed:
	// extend one run beyond the last sample.
	later := slices.Clone(coal)
	last := &later[len(later)-1]
	last.Through = last.EventTime.Add(4 * o.Step)
	laterPath := actfixture.Write(t, dir, "later.parquet", later)
	if _, err := vminput.CheckSameFacts(laterPath, everyPath, reg, vminput.DefaultExtendFraction); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("an end later in the coalesced stream: %v", err)
	}

	// A subject missing from the coalesced stream, and one that is only there.
	var without []store.Record
	drop := every[0].Subject
	for _, r := range coal {
		if r.Subject != drop {
			without = append(without, r)
		}
	}
	withoutPath := actfixture.Write(t, dir, "without.parquet", without)
	if _, err := vminput.CheckSameFacts(withoutPath, everyPath, reg, vminput.DefaultExtendFraction); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("a subject missing: %v", err)
	}
	if _, err := vminput.CheckSameFacts(everyPath, withoutPath, reg, vminput.DefaultExtendFraction); !errors.Is(err, vmmodel.ErrFactsDiffer) {
		t.Errorf("a subject only in the coalesced stream: %v", err)
	}
	if _, err := vminput.CheckSameFacts(filepath.Join(dir, "missing"), everyPath, reg, 0.5); err == nil {
		t.Error("a missing coalesced file")
	}
}
