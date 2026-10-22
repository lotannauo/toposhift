package xtspike

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtinput"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

var base = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(s int) time.Time { return base.Add(time.Duration(s) * time.Second) }

func fpN(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	h[0], h[1] = byte(n>>8), byte(n)
	f, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return f
}

// hour is an hour of a small cluster: 20 nodes and 300 pods, each pod placed on a node and
// refreshed with a lease that lapses, some replaced, some deleted: more than a few batches of
// the questions' size, so the old snapshot is in the middle of it.
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
		if p%5 == 0 {
			add(store.Record{Subject: store.EdgeSubject(pod, fpN(catalog.K8sNode, (p+1)%20), catalog.ScheduledOn), EventTime: at(start + 2100), Kind: lifecycle.Observe, TTL: 5 * time.Minute, Payload: []byte("e2")})
		}
	}
	add(store.Record{Subject: store.EntitySubject(fpN(catalog.K8sNode, 0)), EventTime: at(3700), Kind: lifecycle.Observe, Payload: []byte("node")})
	return out
}

func writeActivity(t *testing.T, rs []store.Record) string {
	t.Helper()
	var buf bytes.Buffer
	w, err := activity.NewWriter(&buf, activity.WriterOptions{RowGroupRows: 500})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range rs {
		if err := w.Write(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "x.parquet")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// contentDigest is the content digest the file's footer gives.
func contentDigest(t *testing.T, path string) string {
	t.Helper()
	info, err := xtinput.ForEach(path, 1000, xtinput.Options{DropBasis: true, DropBoot: true}, nil, func([]engine.Record) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	return info.Digest
}

func TestAnActivityFileThroughQuestionsLoadReadAndSizes(t *testing.T) {
	rs := hour()
	rs[10].EventTimeBasis = store.BasisObserved // dropped, and counted
	file := writeActivity(t, rs)
	digest := contentDigest(t, file)

	r := newRig(t)
	qdir := filepath.Join(t.TempDir(), "q")
	fileArgs := []string{"-activity", file, "-expect-activity", digest, "-drop-basis"}
	if err := r.run(append([]string{"questions", "-out", qdir, "-label", "synthetic hour"}, fileArgs...)...); err != nil {
		t.Fatalf("%v\n%s", err, r.out)
	}
	noDuration(t, "questions", r.out.String()+r.errOut.String())
	if !strings.Contains(r.out.String(), "1 records with an event time basis") || !strings.Contains(r.out.String(), "off a whole microsecond: 0") {
		t.Errorf("%s", r.out)
	}
	set, err := xtinput.LoadQuestions(filepath.Join(qdir, "questions.json"))
	if err != nil {
		t.Fatal(err)
	}
	sum, err := fileSHA(filepath.Join(qdir, "questions.json"))
	if err != nil || !strings.Contains(r.out.String(), sum) {
		t.Fatalf("the digest %s is not in the output:\n%s", sum, r.out)
	}
	if set.Records != uint64(len(rs)) || set.Stats.BasisDropped != 1 || set.Label != "synthetic hour" {
		t.Fatalf("%+v", set.Stats)
	}
	// The questions are not made again over themselves.
	if err := r.run(append([]string{"questions", "-out", qdir}, fileArgs...)...); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Errorf("a second set: %v", err)
	}

	input := []string{"-questions", filepath.Join(qdir, "questions.json"), "-expect-questions", sum}
	out := filepath.Join(t.TempDir(), "rec")
	load := append(append([]string{"load"}, r.common(out)...), append(input, fileArgs...)...)
	if err := r.run(load...); err != nil {
		t.Fatalf("%v\n%s\n%s", err, r.out, r.errOut)
	}
	noDuration(t, "load", r.out.String()+r.errOut.String())
	l := r.readLoadRecord(out)
	if l.Input.Kind != "activity" || l.Input.QuestionsSHA256 != sum || l.Input.ContentDigest != set.ContentDigest || l.Dropped.Basis != 1 || l.Dropped.Boot != 0 ||
		l.Writer.Records != uint64(len(rs)) || l.Writer.Batches != (uint64(len(rs))+999)/1000 || l.Writer.Retentions != 0 || l.Input.Retentions != 0 {
		t.Fatalf("%+v", l)
	}
	if err := r.cmd("read", filepath.Join(t.TempDir(), "r"), input, "-load", filepath.Join(out, "load.json")); err != nil {
		t.Fatalf("%v\n%s", err, r.out)
	}
	if !strings.Contains(r.out.String(), "G0 total: ") {
		t.Errorf("%s", r.out)
	}
}

func TestAnActivityLoadIsRefusedForAFileOrOptionsThatAreNotTheQuestions(t *testing.T) {
	rs := hour()
	file := writeActivity(t, rs)
	digest := contentDigest(t, file)
	r := newRig(t)
	qdir := filepath.Join(t.TempDir(), "q")
	if err := r.run("questions", "-out", qdir, "-activity", file, "-expect-activity", digest); err != nil {
		t.Fatal(err)
	}
	qfile := filepath.Join(qdir, "questions.json")
	sum, err := fileSHA(qfile)
	if err != nil {
		t.Fatal(err)
	}
	input := []string{"-questions", qfile, "-expect-questions", sum}
	try := func(extra ...string) error {
		r.node = newModelNode() // a load that fails after it wrote leaves the node holding the stream
		out := filepath.Join(t.TempDir(), "rec")
		return r.cmd("load", out, input, extra...)
	}
	// Without the file, or its digest.
	if err := try(); err == nil || !strings.Contains(err.Error(), "-activity file") {
		t.Errorf("no file: %v", err)
	}
	if err := try("-activity", file); err == nil || !strings.Contains(err.Error(), "-expect-activity") {
		t.Errorf("no digest: %v", err)
	}
	// Options other than the questions' (they dropped nothing).
	if err := try("-activity", file, "-expect-activity", digest, "-drop-boot"); err == nil || !strings.Contains(err.Error(), "was made with") {
		t.Errorf("other options: %v", err)
	}
	// Another content digest.
	if err := try("-activity", file, "-expect-activity", strings.Repeat("0", 12)); err == nil || !strings.Contains(err.Error(), "content digest") {
		t.Errorf("another digest: %v", err)
	}
	// Another file with the same questions' name: a file that is not the one they were made for.
	other := hour()
	other[100].Payload = []byte("another payload")
	otherFile := writeActivity(t, other)
	if err := try("-activity", otherFile, "-expect-activity", contentDigest(t, otherFile)); err == nil || !strings.Contains(err.Error(), "not the one the questions were made for") {
		t.Errorf("another file: %v", err)
	}
}

func TestAStreamXTDBCannotHoldIsRefusedBeforeTheNodeIsReached(t *testing.T) {
	rs := hour()
	rs[50].EventTime = rs[50].EventTime.Add(1)
	file := writeActivity(t, rs)
	digest := contentDigest(t, file)
	r := newRig(t)
	qdir := filepath.Join(t.TempDir(), "q")
	if err := r.run("questions", "-out", qdir, "-activity", file, "-expect-activity", digest); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.out.String(), "off a whole microsecond: 1") || !strings.Contains(r.out.String(), "a load of this stream is refused") {
		t.Errorf("%s", r.out)
	}
	qfile := filepath.Join(qdir, "questions.json")
	sum, err := fileSHA(qfile)
	if err != nil {
		t.Fatal(err)
	}
	err = r.cmd("load", filepath.Join(t.TempDir(), "rec"), []string{"-questions", qfile, "-expect-questions", sum, "-activity", file, "-expect-activity", digest})
	if err == nil || !strings.Contains(err.Error(), "not a whole microsecond") {
		t.Fatalf("%v", err)
	}
	if r.node.opens != 0 {
		t.Errorf("the node was opened %d times", r.node.opens)
	}
}

func TestQuestionsRefuseWhatTheyNeedToRefuse(t *testing.T) {
	file := writeActivity(t, hour())
	digest := contentDigest(t, file)
	r := newRig(t)
	out := filepath.Join(t.TempDir(), "q")
	for name, args := range map[string][]string{
		"no file":          {"-out", out, "-expect-activity", digest},
		"no digest":        {"-out", out, "-activity", file},
		"no output":        {"-activity", file, "-expect-activity", digest},
		"a wrong digest":   {"-out", out, "-activity", file, "-expect-activity", strings.Repeat("0", 12)},
		"a negative limit": {"-out", out, "-activity", file, "-expect-activity", digest, "-limit", "-1"},
		"a missing file":   {"-out", out, "-activity", filepath.Join(t.TempDir(), "m.parquet"), "-expect-activity", digest},
	} {
		if err := r.run(append([]string{"questions"}, args...)...); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if _, err := os.Stat(filepath.Join(out, "questions.json")); err == nil {
			t.Fatalf("%s: a set was written", name)
		}
	}
	// A prefix is labelled as one.
	if err := r.run("questions", "-out", out, "-activity", file, "-expect-activity", digest, "-limit", "1500"); err != nil {
		t.Fatal(err)
	}
	set, err := xtinput.LoadQuestions(filepath.Join(out, "questions.json"))
	if err != nil || set.Limit != 1500 || set.Records != 2000 {
		t.Fatalf("%+v, %v", set, err)
	}
}
