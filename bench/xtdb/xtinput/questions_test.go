package xtinput_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// hour is an hour of a small cluster: 20 nodes and 300 pods, each pod placed on a node and
// refreshed every few minutes with a lease that lapses, some of them replaced, some deleted.
// It is more than a few batches of the stream's size, so the old snapshot is in the middle of
// it.
func hour() []store.Record {
	var out []store.Record
	seq := uint64(0)
	add := func(r store.Record) {
		seq++
		r.Seq = seq
		r.Layer, r.Producer = catalog.L2, "k8s"
		out = append(out, r)
	}
	for n := range 20 {
		add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, n)), EventTime: at(0), Kind: lifecycle.Observe, Payload: []byte("node")})
	}
	for p := range 300 {
		pod, node := fpN(catalog.K8sPod, p), fpN(catalog.K8sNode, p%20)
		start := p * 5
		add(store.Record{Subject: store.EntitySubject(pod), EventTime: at(start), Kind: lifecycle.Observe, TTL: 10 * time.Minute, Payload: []byte("pod")})
		edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
		for r := range 6 {
			add(store.Record{Subject: edge, EventTime: at(start + 1 + 400*r), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e")})
		}
		if p%7 == 0 {
			add(store.Record{Subject: edge, EventTime: at(start + 2000), Kind: lifecycle.Delete})
		}
		if p%5 == 0 { // replaced on another node
			add(store.Record{Subject: store.EdgeSubject(pod, fpN(catalog.K8sNode, (p+1)%20), catalog.ScheduledOn), EventTime: at(start + 2100), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e2")})
		}
	}
	// The last refresh of a node, so that the stream is longer than an hour.
	add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, 0)), EventTime: at(3700), Kind: lifecycle.Observe, Payload: []byte("node")})
	return out
}

func TestQuestionsAreAnsweredAsAReferenceThatSawEveryRecordAnswersThem(t *testing.T) {
	rs := hour()
	path := writeFile(t, rs)
	q, err := xtinput.BuildQuestions(path, xtinput.Options{}, 0, "test hour", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.Records != uint64(len(rs)) || q.LastSeq != rs[len(rs)-1].Seq || q.ContentDigest == "" || q.Limit != 0 {
		t.Fatalf("stream: %d records, last seq %d, digest %q", q.Records, q.LastSeq, q.ContentDigest)
	}
	if q.Stats != (xtinput.Stats{Records: uint64(len(rs))}) {
		t.Errorf("stats %+v", q.Stats)
	}
	// The reference the questions were answered by saw only the records that touch an entity
	// they read; one that saw everything gives the same answers.
	full := oracle.New()
	if _, err := xtinput.ForEach(path, 500, xtinput.Options{}, nil, func(b []engine.Record) error { return full.Write(b) }); err != nil {
		t.Fatal(err)
	}
	ages := map[string]int{}
	nonEmpty := 0
	for _, qu := range q.Queries {
		a, err := runner.Ask(full, qu)
		if err != nil {
			t.Fatal(err)
		}
		if a.Digest != qu.Expect || a.Size != qu.Size {
			t.Errorf("%s: the set says %s (%d), a reference of the whole stream %s (%d)", qu.Name(), qu.Expect, qu.Size, a.Digest, a.Size)
		}
		ages[qu.Age]++
		if qu.Size > 0 {
			nonEmpty++
		}
	}
	if nonEmpty == 0 {
		t.Error("every answer is empty")
	}
	// An hour has the reads of its end, of its old snapshot and the window of the last hour;
	// the read an hour back would be made at the first instant, and it is cut.
	for _, age := range []string{runner.AgeNow, runner.AgeOldToken, runner.AgeWindow1h} {
		if ages[age] == 0 {
			t.Errorf("no question at age %s: %v", age, ages)
		}
	}
	for _, age := range []string{runner.Age1h, runner.Age3h, runner.Age9h, runner.Age1d, runner.AgeWindow1d, runner.AgeDead} {
		if ages[age] != 0 {
			t.Errorf("%d questions at age %s, which a stream of an hour cannot have", ages[age], age)
		}
	}
	if q.OldToken == 0 || q.OldToken >= q.LastSeq || !q.OldAt.After(q.Start) || q.OldAt.After(q.End) {
		t.Errorf("old snapshot: token %d of %d at %s in [%s, %s]", q.OldToken, q.LastSeq, q.OldAt, q.Start, q.End)
	}
	// The token ends a batch of the stream's size, which is the batching a load must use for
	// a read as of the token to see a whole transaction.
	if q.OldToken%xtinput.QuestionBatch != 0 {
		t.Errorf("the old token %d is not the end of a batch of %d", q.OldToken, xtinput.QuestionBatch)
	}
	// The old snapshot is read at an instant before any record after its token takes effect.
	for _, r := range rs {
		if r.Seq > q.OldToken && !r.EventTime.After(q.OldAt) {
			t.Errorf("record seq %d after the token takes effect at %s, not after the old instant %s", r.Seq, r.EventTime, q.OldAt)
			break
		}
	}
}

func TestQuestionsAreTheSameBytesWhenMadeAgainAndRoundTrip(t *testing.T) {
	path := writeFile(t, hour())
	a, err := xtinput.BuildQuestions(path, xtinput.Options{}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := xtinput.BuildQuestions(path, xtinput.Options{}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.QueriesDigest != b.QueriesDigest || !reflect.DeepEqual(a, b) {
		t.Fatal("two sets of one file differ")
	}
	file := filepath.Join(t.TempDir(), "q.json")
	if err := a.Save(file); err != nil {
		t.Fatal(err)
	}
	got, err := xtinput.LoadQuestions(file)
	if err != nil {
		t.Fatal(err)
	}
	if got.QueriesDigest != a.QueriesDigest || len(got.Queries) != len(a.Queries) || got.Info().LastSeq != a.LastSeq || got.OldToken != a.OldToken || got.Stats != a.Stats {
		t.Errorf("after a round trip: %+v", got.Info())
	}
	// A set whose questions were edited does not say what it was made from.
	text, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	for name, edited := range map[string]string{
		"a question":  strings.Replace(string(text), `"Age": "now"`, `"Age": "xx"`, 1),
		"the version": strings.Replace(string(text), `"Version": 1`, `"Version": 9`, 1),
	} {
		if edited == string(text) {
			t.Fatalf("%s: the edit changed nothing", name)
		}
		if err := os.WriteFile(file, []byte(edited), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := xtinput.LoadQuestions(file); !errors.Is(err, xtinput.ErrQuestions) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if _, err := xtinput.LoadQuestions(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Error("a missing set was read")
	}
}

func TestQuestionsOfAPrefixSayTheyAreOneAndAFileTheConverterRefusesIsRefused(t *testing.T) {
	path := writeFile(t, hour())
	q, err := xtinput.BuildQuestions(path, xtinput.Options{}, 1500, "prefix", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.Limit != 1500 || q.Records != 2000 { // whole batches of 1,000
		t.Errorf("limit %d, %d records", q.Limit, q.Records)
	}
	rs := hour()
	rs[3].Boot = "boot-1"
	rs[3].Subject, rs[3].Layer = store.EntitySubject(fpN(catalog.Host, 1)), catalog.L1
	if _, err := xtinput.BuildQuestions(writeFile(t, rs), xtinput.Options{}, 0, "x", nil); err == nil {
		t.Error("a record with a boot id was converted without DropBoot")
	}
	if _, err := xtinput.BuildQuestions(filepath.Join(t.TempDir(), "missing.parquet"), xtinput.Options{}, 0, "x", nil); err == nil {
		t.Error("a missing file was read")
	}
}

// TestTheCountingPassCountsWhatAnEngineWillDropOrRefuse: the set records, before any load,
// the fields dropped (with the option that allows it) and the records off a whole
// microsecond, which a load would be refused for.
func TestTheCountingPassCountsWhatAnEngineWillDropOrRefuse(t *testing.T) {
	rs := hour()
	for _, i := range []int{5, 40} {
		rs[i].EventTimeBasis = store.BasisObserved
	}
	rs[100].Subject, rs[100].Layer, rs[100].Boot = store.EntitySubject(fpN(catalog.Host, 1)), catalog.L1, "boot-1"
	rs[200].EventTime = rs[200].EventTime.Add(1)
	rs[300].TTL = 5*time.Minute + 1
	q, err := xtinput.BuildQuestions(writeFile(t, rs), xtinput.Options{DropBasis: true, DropBoot: true}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	want := xtinput.Stats{Records: uint64(len(rs)), BasisDropped: 2, BootDropped: 1, OffMicrosecond: 2}
	if q.Stats != want {
		t.Errorf("stats %+v, want %+v", q.Stats, want)
	}
}
