package doltinput_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltinput"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/oracle"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

func fpN(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n>>8), byte(n)
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// hour is an hour of a small cluster: 20 nodes and 300 pods, each pod placed on a node
// and refreshed every few minutes with a lease that lapses, some of them replaced, some
// deleted. It is more than a few batches of the stream's size, so the old snapshot is in
// the middle of it.
func hour() []store.Record {
	at := func(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }
	var out []store.Record
	seq := uint64(0)
	add := func(r store.Record) {
		seq++
		r.Seq = seq
		r.Layer, r.Producer = catalog.L2, "k8s"
		out = append(out, r)
	}
	for n := 0; n < 20; n++ {
		add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, n)), EventTime: at(0), Kind: lifecycle.Observe, Payload: []byte("node")})
	}
	for p := 0; p < 300; p++ {
		pod, node := fpN(catalog.K8sPod, p), fpN(catalog.K8sNode, p%20)
		start := p * 5
		add(store.Record{Subject: store.EntitySubject(pod), EventTime: at(start), Kind: lifecycle.Observe, TTL: 10 * time.Minute, Payload: []byte("pod")})
		edge := store.EdgeSubject(pod, node, catalog.ScheduledOn)
		for r := 0; r < 6; r++ {
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
	q, err := doltinput.BuildQuestions(path, doltinput.Options{}, 0, "test hour", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.Records != uint64(len(rs)) || q.LastSeq != rs[len(rs)-1].Seq || q.ContentDigest == "" || q.Limit != 0 {
		t.Fatalf("stream: %d records, last seq %d, digest %q", q.Records, q.LastSeq, q.ContentDigest)
	}
	// The reference the questions were answered by saw only the records that touch an
	// entity they read; one that saw everything gives the same answers.
	full := oracle.New()
	if _, err := doltinput.ForEach(path, 500, doltinput.Options{}, func(b []engine.Record) error { return full.Write(b) }); err != nil {
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
	// An hour has the reads of its end, of its old snapshot and the window of the last
	// hour; the read an hour back would be made at the first instant, and it is cut.
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
	// The old snapshot is read at an instant before any record after its token takes
	// effect: nothing the token did not see is in force at it.
	for _, r := range rs {
		if r.Seq > q.OldToken && !r.EventTime.After(q.OldAt) {
			t.Errorf("record seq %d after the token takes effect at %s, not after the old instant %s", r.Seq, r.EventTime, q.OldAt)
			break
		}
	}
}

func TestQuestionsAreTheSameBytesWhenMadeAgainAndRoundTrip(t *testing.T) {
	path := writeFile(t, hour())
	a, err := doltinput.BuildQuestions(path, doltinput.Options{}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := doltinput.BuildQuestions(path, doltinput.Options{}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.QueriesDigest != b.QueriesDigest || !reflect.DeepEqual(a.Queries, b.Queries) {
		t.Fatal("two sets of one file differ")
	}
	file := filepath.Join(t.TempDir(), "q.json")
	if err := a.Save(file); err != nil {
		t.Fatal(err)
	}
	got, err := doltinput.LoadQuestions(file)
	if err != nil {
		t.Fatal(err)
	}
	if got.QueriesDigest != a.QueriesDigest || len(got.Queries) != len(a.Queries) || got.Info().LastSeq != a.LastSeq || got.OldToken != a.OldToken {
		t.Errorf("after a round trip: %+v", got.Info())
	}
	// A set whose questions were edited does not say what it was made from.
	text, _ := os.ReadFile(file)
	for name, edited := range map[string]string{
		"a question":  replaceOnce(string(text), `"Age": "now"`, `"Age": "xx"`),
		"the version": replaceOnce(string(text), `"Version": 1`, `"Version": 9`),
	} {
		if err := os.WriteFile(file, []byte(edited), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := doltinput.LoadQuestions(file); !errors.Is(err, doltinput.ErrQuestions) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func replaceOnce(s, old, new string) string {
	for i := 0; i+len(old) <= len(s); i++ {
		if s[i:i+len(old)] == old {
			return s[:i] + new + s[i+len(old):]
		}
	}
	panic("not found: " + old)
}

func TestQuestionsOfAPrefixSayTheyAreOneAndAFileTheConverterRefusesIsRefused(t *testing.T) {
	path := writeFile(t, hour())
	q, err := doltinput.BuildQuestions(path, doltinput.Options{}, 1500, "prefix", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.Limit != 1500 || q.Records != 2000 { // whole batches of 1,000
		t.Errorf("limit %d, %d records", q.Limit, q.Records)
	}
	rs := hour()
	rs[3].Boot = "boot-1"
	rs[3].Subject, rs[3].Layer = store.EntitySubject(fpN(catalog.Host, 1)), catalog.L1
	if _, err := doltinput.BuildQuestions(writeFile(t, rs), doltinput.Options{}, 0, "x", nil); err == nil {
		t.Error("a record with a boot id was converted without DropBoot")
	}
	if _, err := doltinput.BuildQuestions(filepath.Join(t.TempDir(), "missing.parquet"), doltinput.Options{}, 0, "x", nil); err == nil {
		t.Error("a missing file was read")
	}
}

func TestQuestionsRecordTheLongestPayloadAndWhatWasDropped(t *testing.T) {
	rs := hour()
	rs[10].Payload = make([]byte, 777)
	rs[11].EventTimeBasis = store.BasisObserved
	for _, i := range []int{12, 13} {
		rs[i].Boot, rs[i].Subject, rs[i].Layer = "b", store.EntitySubject(fpN(catalog.Host, i)), catalog.L1
	}
	rs[13].EventTimeBasis = store.BasisObserved
	path := writeFile(t, rs)
	if _, err := doltinput.BuildQuestions(path, doltinput.Options{}, 0, "x", nil); err == nil {
		t.Fatal("records with a basis and a boot id were accepted without the options")
	}
	q, err := doltinput.BuildQuestions(path, doltinput.Options{DropBasis: true, DropBoot: true}, 0, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if q.MaxPayload != 777 {
		t.Errorf("the longest payload is %d, want 777", q.MaxPayload)
	}
	if q.Dropped != (doltinput.Dropped{Basis: 2, Boot: 2}) {
		t.Errorf("dropped %+v", q.Dropped)
	}
	// The set is a plan for a gate: the questions and the stream's extent, and no spec.
	p := q.Plan()
	if len(p.Queries) != len(q.Queries) || p.QueriesDigest != q.QueriesDigest || p.Stream.LastSeq != q.LastSeq || p.Stream.OldToken != q.OldToken {
		t.Errorf("the plan of the set: %d queries, digest %s, stream %+v", len(p.Queries), p.QueriesDigest, p.Stream)
	}
	// Records written for the longest payload, and a window longer than any: a prefix
	// reads only the payloads it holds.
	pre, err := doltinput.BuildQuestions(path, doltinput.Options{DropBasis: true, DropBoot: true}, 1000, "x", nil)
	if err != nil {
		t.Fatal(err)
	}
	if pre.MaxPayload != 777 || pre.Records != 1000 {
		t.Errorf("a prefix of 1,000: longest payload %d, %d records", pre.MaxPayload, pre.Records)
	}
}
