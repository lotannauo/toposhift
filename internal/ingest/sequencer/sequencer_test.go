package sequencer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// podsOf are n pods p-0 ... p-(n-1), created an hour before t0.
func podsOf(n int, prefix string) []pod {
	out := make([]pod, n)
	for i := range out {
		out[i] = pod{fmt.Sprintf("%s-%d", prefix, i), fmt.Sprintf("%s-%d", prefix, i), "", t0.Add(-time.Hour), time.Time{}, "Running"}
	}
	return out
}

func pullAll(tm time.Time, pods ...pod) []capture.LogRecord {
	var recs []capture.LogRecord
	for _, p := range pods {
		recs = append(recs, pullPod(tm, p))
	}
	return recs
}

// storedPods is the set of the given pod UIDs that have an entity record among
// recs.
func storedPods(t *testing.T, recs []store.Record, uids ...string) map[string]bool {
	t.Helper()
	out := map[string]bool{}
	for _, uid := range uids {
		fp := podFP(t, uid)
		for _, r := range recs {
			if r.Subject.Kind == store.SubjectEntity && r.Subject.A == fp {
				out[uid] = true
			}
		}
	}
	return out
}

// S1b. No request is answered before Write returns: with a store whose Write
// blocks, the requests of the group stay unanswered until it is released.
func TestS1bNoAnswerBeforeWrite(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st)
	g := newGate(t)
	st.inject = func(int, []store.Record) (bool, error) {
		g.hold()
		return true, nil
	}
	a := h.sendAsync(context.Background(), logs("", pullPod(t0, p1)))
	b := h.sendAsync(context.Background(), logs("", pullPod(t0, p2)))
	h.waitQueued(2)
	h.start()
	<-g.entered
	select {
	case got := <-a:
		t.Fatalf("answered %+v while Write had not returned", got)
	case got := <-b:
		t.Fatalf("answered %+v while Write had not returned", got)
	case <-time.After(100 * time.Millisecond):
	}
	g.open()
	for _, ch := range []<-chan answer{a, b} {
		if got := recv(t, ch); got.err != nil {
			t.Fatalf("answer %+v", got)
		}
	}
}

// S4. A retention moves a horizon after the sequencer read it and before the
// write: the write is refused for the horizon, the group is reset and rerun once
// against the new horizon, the records now before it are rejected into the
// partial success, and the rest are stored. A record whose Through reaches the
// horizon is re-timed to it by the coalescer and stored.
func TestS4HorizonMovedBeforeTheWrite(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	horizon := at(-time.Hour)
	moved := false
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) {
		if !moved {
			moved = true
			if err := mem.Retain(context.Background(), horizon); err != nil {
				t.Error(err)
			}
		}
	}})).start()
	old := pod{"old-1", "old-1", "", t0.Add(-3 * time.Hour), time.Time{}, "Running"}
	old2 := pod{"old-2", "old-2", "", t0.Add(-3 * time.Hour), time.Time{}, "Running"}
	current := pod{"cur-1", "cur-1", "", t0.Add(-3 * time.Hour), time.Time{}, "Running"}
	// Two watch events observed two hours back (late, wholly before the horizon)
	// and one pull now.
	p, err := h.send(logs("",
		watchPod(at(-2*time.Hour), "ADDED", old),
		pullPod(t0, current),
		watchPod(at(-2*time.Hour), "ADDED", old2),
	))
	if err != nil {
		t.Fatal(err)
	}
	if p.Rejected != 2 || !strings.Contains(p.Message, "retention horizon") {
		t.Fatalf("partial %+v, want 2 rejected for the horizon", p)
	}
	if n := st.writeCount(); n != 2 {
		t.Errorf("%d writes, want 2: the refused one and its one rerun", n)
	}
	recs := st.records()
	if len(recs) != 1 {
		t.Fatalf("%d records stored, want the current pod's one", len(recs))
	}
	r := recs[0]
	if r.Subject.A != podFP(t, "cur-1") || !r.EventTime.Equal(horizon) || !r.Through.Equal(t0) {
		t.Errorf("stored %s, want the current pod re-timed to the horizon, through t0", show(r))
	}
}

// S4b. A write refused as invalid for one request's records (a case the checks
// missed): that request is answered 400 naming the error, the others are stored,
// each written on its own, and nothing loops.
func TestS4bInvalidWriteSplitsTheGroup(t *testing.T) {
	bad := podFP(t, "pod-2")
	st := &fakeStore{inner: newMem(t)}
	st.inject = func(_ int, batch []store.Record) (bool, error) {
		for _, r := range batch {
			if r.Subject.A == bad {
				return false, fmt.Errorf("fake: batch[0] seq %d: a value the codec cannot encode: %w", r.Seq, store.ErrInvalid)
			}
		}
		return true, nil
	}
	h := newHarness(t, st)
	chs := []<-chan answer{
		h.sendAsync(context.Background(), logs("", pullPod(t0, p1))),
		h.sendAsync(context.Background(), logs("", pullPod(t0, p2))),
		h.sendAsync(context.Background(), logs("", pullPod(t0, p3))),
	}
	h.waitQueued(3)
	h.start()
	for i, ch := range chs {
		got := recv(t, ch)
		var refused *otlphttp.RefusedError
		switch i {
		case 1:
			if !errors.As(got.err, &refused) || !strings.Contains(refused.Message, "codec cannot encode") {
				t.Errorf("request %d: %v, want a 400 naming the error", i, got.err)
			}
		default:
			if got.err != nil {
				t.Errorf("request %d: %v, want 200", i, got.err)
			}
		}
	}
	stored := storedPods(t, st.records(), "pod-1", "pod-2", "pod-3")
	if !stored["pod-1"] || stored["pod-2"] || !stored["pod-3"] {
		t.Errorf("stored %v, want pod-1 and pod-3", stored)
	}
	if n := st.writeCount(); n != 4 {
		t.Errorf("%d writes, want 4: the group, then each request", n)
	}
}

// postJSON sends d to the receiver as a JSON body and returns the response.
func postJSON(t *testing.T, s *sequencer.Sequencer, d capture.LogsData) *httptest.ResponseRecorder {
	t.Helper()
	body, err := capture.EncodeLogs(d)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	otlphttp.Handler(s, otlphttp.Options{}).ServeHTTP(rec, req)
	return rec
}

func partialOf(t *testing.T, rec *httptest.ResponseRecorder) (int64, string) {
	t.Helper()
	var j struct {
		PartialSuccess struct {
			RejectedLogRecords string `json:"rejectedLogRecords"`
			ErrorMessage       string `json:"errorMessage"`
		} `json:"partialSuccess"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &j); err != nil {
		t.Fatalf("body %q: %v", rec.Body.String(), err)
	}
	var n int64
	_, _ = fmt.Sscan(j.PartialSuccess.RejectedLogRecords, &n)
	return n, j.PartialSuccess.ErrorMessage
}

// S5. One OTLP log record in 300 gives a store record that fails Validate: a
// refresh of a pod seen before, observed so late that its deadline is past the
// representable times. The request is answered 200 with partial_success
// {1, ...}, the other 299 refreshes are stored, and the rejected record never
// reaches the coalescer: it is not in the write, and the coalescer holds a run
// for each stored refresh and none for it.
func TestS5OneInvalidRecordIsRejected(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	invalid := pod{"far-future", "far-future", "", t0.Add(-time.Hour), time.Time{}, "Running"}
	var batches [][]store.Record
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func(b []store.Record) {
		batches = append(batches, append([]store.Record(nil), b...))
	}})).start()
	good := podsOf(299, "good")
	// The first pull: every pod's first record, which carries a Through and so
	// starts no run.
	h.sendOK(logs("", append(pullAll(t0, good...), pullPod(t0, invalid))...))
	// The next pull: each refresh starts a run, and the invalid one is rejected.
	h.clock.set(at(5 * time.Minute))
	recs := pullAll(at(5*time.Minute), good[:150]...)
	recs = append(recs, pullPod(store.MaxEventTime.Add(-time.Minute), invalid))
	recs = append(recs, pullAll(at(5*time.Minute), good[150:]...)...)
	rec := postJSON(t, h.seq, logs("", recs...))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
	}
	n, msg := partialOf(t, rec)
	if n != 1 || !strings.Contains(msg, "outside the representable range") {
		t.Fatalf("partial success %d %q, want 1 naming the deadline", n, msg)
	}
	if len(batches) != 2 {
		t.Fatalf("%d writes, want 2", len(batches))
	}
	fp := podFP(t, "far-future")
	for _, r := range batches[1] {
		if r.Subject.A == fp {
			t.Fatalf("the invalid record reached the write: %s", show(r))
		}
	}
	uids := make([]string, len(good))
	for i, p := range good {
		uids[i] = p.uid
	}
	if got := storedPods(t, batches[1], uids...); len(got) != 299 {
		t.Errorf("%d of the 299 refreshes stored", len(got))
	}
	h.stop()
	if runs := sequencer.Runs(h.seq); runs != 299 {
		t.Errorf("the coalescer holds %d runs, want 299: one per stored refresh, none for the rejected one", runs)
	}
}

// S5b. A translator error on one log record (no observed time) is rejected into
// the partial success, never a 500; the other record is stored.
func TestS5bTranslatorError(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	noTime := pullPod(t0, p2)
	noTime.ObservedTimeUnixNano = 0
	rec := postJSON(t, h.seq, logs("", pullPod(t0, p1), noTime))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d, body %q", rec.Code, rec.Body.String())
	}
	if n, msg := partialOf(t, rec); n != 1 || !strings.Contains(msg, "no observed time") {
		t.Fatalf("partial success %d %q", n, msg)
	}
	if got := storedPods(t, st.records(), "pod-1", "pod-2"); !got["pod-1"] || got["pod-2"] {
		t.Errorf("stored %v, want pod-1 alone", got)
	}
}

// S6. With a configured cluster name, a request with a resource whose
// k8s.cluster.name differs is refused whole with 400 naming both values; nothing
// of it is stored and no translator sees it; the group's other requests are
// stored.
func TestS6ClusterNameMismatch(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var mu sync.Mutex
	seen := map[string]bool{}
	h := newHarness(t, st,
		withConfig(func(c *sequencer.Config) { c.ClusterName, c.ClusterNameSet = "prod", true }),
		withHooks(sequencer.Hooks{BeforeTranslate: func(_ string, lr capture.LogRecord) {
			mu.Lock()
			defer mu.Unlock()
			seen[uidOfRecord(lr)] = true
		}}))
	mismatched := capture.LogsData{ResourceLogs: []capture.ResourceLogs{
		resource("prod", pullPod(t0, p2)),
		resource("staging", pullPod(t0, p3)),
	}}
	chs := []<-chan answer{
		h.sendAsync(context.Background(), logs("prod", pullPod(t0, p1))),
		h.sendAsync(context.Background(), mismatched),
		h.sendAsync(context.Background(), logs("", pullPod(t0, p4))),
	}
	h.waitQueued(3)
	h.start()
	for i, ch := range chs {
		got := recv(t, ch)
		var refused *otlphttp.RefusedError
		if i == 1 {
			if !errors.As(got.err, &refused) || !strings.Contains(refused.Message, `"staging"`) || !strings.Contains(refused.Message, `"prod"`) {
				t.Errorf("mismatched request: %v, want a 400 naming both names", got.err)
			}
			continue
		}
		if got.err != nil {
			t.Errorf("request %d: %v", i, got.err)
		}
	}
	stored := storedPods(t, st.records(), "pod-1", "pod-2", "pod-3", "pod-4")
	if !stored["pod-1"] || stored["pod-2"] || stored["pod-3"] || !stored["pod-4"] {
		t.Errorf("stored %v, want pod-1 and pod-4", stored)
	}
	if seen["pod-2"] || seen["pod-3"] {
		t.Errorf("a translator saw the refused request: %v", seen)
	}
	for _, r := range st.records() {
		if r.Producer != "k8sobjects/prod" {
			t.Errorf("record of producer %s", r.Producer)
		}
	}
}

// uidOfRecord is the metadata.uid of an objects receiver record.
func uidOfRecord(lr capture.LogRecord) string {
	if lr.Body == nil || lr.Body.KVListValue == nil {
		return ""
	}
	var find func(l *capture.KeyValueList) string
	find = func(l *capture.KeyValueList) string {
		for _, kv := range l.Values {
			if kv.Key == "uid" && kv.Value.StringValue != nil {
				return *kv.Value.StringValue
			}
			if kv.Value.KVListValue != nil {
				if u := find(kv.Value.KVListValue); u != "" {
					return u
				}
			}
		}
		return ""
	}
	return find(lr.Body.KVListValue)
}

// S7. A k8s.cluster.name of 22 bytes is over the store's limit of 21, and one of
// 54 over the format's 53: both are refused with 400 and the advice. One of 21
// bytes is taken.
func TestS7ClusterNameTooLong(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	for _, n := range []int{22, 54} {
		_, err := h.send(logs(strings.Repeat("c", n), pullPod(t0, p1)))
		var refused *otlphttp.RefusedError
		if !errors.As(err, &refused) || !strings.Contains(refused.Message, "set a shorter k8s.cluster.name") {
			t.Errorf("%d bytes: %v, want a 400 with the advice", n, err)
		}
	}
	h.sendOK(logs(strings.Repeat("c", 21), pullPod(t0, p1)))
	if recs := st.records(); len(recs) == 0 || recs[0].Producer != lifecycle.Producer("k8sobjects/"+strings.Repeat("c", 21)) {
		t.Errorf("the 21-byte name was not stored: %d records", len(recs))
	}
}

// detectorRun is the steps of S8 and S8b. sendSecond says whether the request
// that brings the second kube-system UID is sent.
func detectorRun(t *testing.T, sendSecond bool) ([]store.Record, []error) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	var errs []error
	send := func(d time.Duration, recs ...capture.LogRecord) {
		h.clock.set(at(d))
		_, err := h.send(logs("", recs...))
		errs = append(errs, err)
	}
	send(time.Minute, nsState("ks-1", "kube-system", at(time.Minute)), pullPod(at(time.Minute), p1))
	if sendSecond {
		// After the warmup: the second UID, and p2's first record.
		send(6*time.Minute, nsState("ks-2", "kube-system", at(6*time.Minute)), pullPod(at(6*time.Minute), p2))
	}
	send(7*time.Minute, nsDelete("ks-2", at(7*time.Minute)), pullPod(at(7*time.Minute), p3))
	send(8*time.Minute, pullPod(at(8*time.Minute), p2), pullPod(at(8*time.Minute), p4))
	h.stop()
	return st.records(), errs
}

// S8. A second kube-system UID under one name after the warmup: the very request
// that brings it is refused. The next request carries the delete of that UID,
// which is sighted because the refused request's namespace event entered the
// kube-system set; so it is not refused, and later requests are stored.
func TestS8SecondKubeSystemUID(t *testing.T) {
	recs, errs := detectorRun(t, true)
	var refused *otlphttp.RefusedError
	if errs[0] != nil {
		t.Errorf("first request: %v", errs[0])
	}
	if !errors.As(errs[1], &refused) || !strings.Contains(refused.Message, `"ks-1", "ks-2"`) {
		t.Errorf("the request bringing the second UID: %v, want a 400 naming both UIDs", errs[1])
	}
	for i, err := range errs[2:] {
		if err != nil {
			t.Errorf("request %d after the refusal: %v", i+2, err)
		}
	}
	if got := storedPods(t, recs, "pod-1", "pod-2", "pod-3", "pod-4"); len(got) != 4 {
		t.Errorf("stored %v, want every pod: p2 from its later request", got)
	}
}

// S8b. A request refused by the detector that also carries a pod's first record,
// and then the refusal ends: the store is what it is when the refused request is
// never sent, with no restart effects: the pod's first stored record is still
// its first, asserted at its creation.
func TestS8bRefusedRequestLeavesNoTrace(t *testing.T) {
	with, errs := detectorRun(t, true)
	without, _ := detectorRun(t, false)
	var refused *otlphttp.RefusedError
	if !errors.As(errs[1], &refused) {
		t.Fatalf("the second request was not refused: %v", errs[1])
	}
	if d := diffRecords(with, without); d != "" {
		t.Fatal(d)
	}
}

// S8c. The namespace selector, against the translator: every namespace event of
// the fixtures is selected, and every selected record gives no store records,
// including the cases where the two could read entity.type differently.
func TestS8cNamespaceSelector(t *testing.T) {
	var recs []capture.LogRecord
	for _, name := range []string{"run1.jsonl", "run2.jsonl", "objects.jsonl"} {
		recs = append(recs, fixtureRecords(t, "../k8scluster/testdata/"+name)...)
	}
	synthetic := []capture.LogRecord{
		nsState("a", "kube-system", t0), nsState("b", "default", t0), nsDelete("a", t0),
		withAttr(nsState("c", "kube-system", t0), "entity.type", iv(3)),         // a non-string type after
		withAttr(podState(p1, t0), "entity.type", sv("k8s.namespace")),          // a second type: the last wins
		withAttr(nsState("d", "kube-system", t0), "entity.type", sv("k8s.pod")), // not a namespace by the last
		withAttr(nsState("e", "kube-system", t0), "otel.entity.type", sv("x")),  // the legacy form
		func() capture.LogRecord { r := nsState("f", "kube-system", t0); r.EventName = "entity.other"; return r }(),
	}
	recs = append(recs, synthetic...)
	tr := newClusterTranslator(t)
	selected, namespaces := 0, 0
	for i, lr := range recs {
		typ := lastType(lr)
		isNS := (lr.EventName == "entity.state" || lr.EventName == "entity.delete") && typ == "k8s.namespace"
		if isNS {
			namespaces++
		}
		if got := sequencer.NamespaceEvent(lr); got != isNS {
			t.Errorf("record %d (%s, type %q): selected %v", i, lr.EventName, typ, got)
		}
		if !sequencer.NamespaceEvent(lr) {
			continue
		}
		selected++
		res, err := tr.Translate(capture.Resource{}, lr, t0)
		if err != nil {
			t.Errorf("record %d: %v", i, err)
		}
		if len(res.Records) != 0 {
			t.Errorf("record %d: a selected record gave %d store records", i, len(res.Records))
		}
	}
	if namespaces == 0 || selected < 5 {
		t.Fatalf("%d namespace events, %d selected: the fixtures hold too few", namespaces, selected)
	}
	t.Logf("%d records, %d selected", len(recs), selected)
}

func withAttr(lr capture.LogRecord, key string, v capture.AnyValue) capture.LogRecord {
	lr.Attributes = append(append([]capture.KeyValue(nil), lr.Attributes...), capture.KeyValue{Key: key, Value: v})
	return lr
}

func lastType(lr capture.LogRecord) string {
	typ := ""
	for _, kv := range lr.Attributes {
		if kv.Key == "entity.type" && kv.Value.StringValue != nil {
			typ = *kv.Value.StringValue
		}
	}
	return typ
}

// S9. With the loop held in a long write: a request beyond the full queue gets
// 503 at once; a queued request not claimed within MergeWait gets 503 from its
// own timer while the write still holds the loop; and the loop's later claim of
// it does nothing.
func TestS9MergeWaitAndFullQueue(t *testing.T) {
	const mergeWait = 500 * time.Millisecond
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st, withConfig(func(c *sequencer.Config) { c.QueueLen, c.MergeWait = 2, mergeWait })).start()
	g := newGate(t)
	st.inject = func(i int, _ []store.Record) (bool, error) {
		if i == 0 {
			g.hold()
		}
		return true, nil
	}
	held := h.sendAsync(context.Background(), logs("", pullPod(t0, p1)))
	<-g.entered
	// sent is taken before the queued requests are sent, so that the time since
	// it bounds their wait from above, and their answer at MergeWait is checked
	// from below.
	sent := time.Now()
	q1 := h.sendAsync(context.Background(), logs("", pullPod(t0, p2)))
	q2 := h.sendAsync(context.Background(), logs("", pullPod(t0, p3)))
	h.waitQueued(2)
	beyond := time.Now()
	_, err := h.send(logs("", pullPod(t0, p4)))
	if took := time.Since(beyond); !errors.Is(err, otlphttp.ErrBusy) || took >= mergeWait {
		t.Fatalf("beyond the full queue: %v after %s, want ErrBusy at once", err, took)
	}
	for _, ch := range []<-chan answer{q1, q2} {
		got := recv(t, ch)
		if !errors.Is(got.err, otlphttp.ErrBusy) || !strings.Contains(got.err.Error(), "not merged") {
			t.Fatalf("queued request: %v, want ErrBusy for MergeWait", got.err)
		}
		if g.released.Load() {
			t.Fatal("the queued request was answered only after the write returned")
		}
		if elapsed := time.Since(sent); elapsed < mergeWait {
			t.Errorf("answered after %s, before MergeWait", elapsed)
		}
	}
	g.open()
	if got := recv(t, held); got.err != nil {
		t.Fatalf("held request: %v", got.err)
	}
	h.stop()
	if got := storedPods(t, st.records(), "pod-1", "pod-2", "pod-3", "pod-4"); !got["pod-1"] || len(got) != 1 {
		t.Errorf("stored %v, want pod-1 alone: the expired requests are not merged", got)
	}
}

// S9b. Requests whose clients cancel after the loop claimed them, while it is
// held before the write: the group's other request is still answered 200, and
// the cancelled requests' records are stored. The write takes the sequencer's
// context, not a request's.
func TestS9bCancelAfterClaim(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var g *gate
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) { g.hold() }}))
	g = newGate(t)
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx3, cancel3 := context.WithCancel(context.Background())
	a := h.sendAsync(ctx1, logs("", pullPod(t0, p1)))
	b := h.sendAsync(context.Background(), logs("", pullPod(t0, p2)))
	c := h.sendAsync(ctx3, logs("", pullPod(t0, p3)))
	h.waitQueued(3)
	h.start()
	<-g.entered
	cancel1()
	cancel3()
	g.open()
	if got := recv(t, b); got.err != nil {
		t.Fatalf("the request that stayed: %v", got.err)
	}
	for _, ch := range []<-chan answer{a, c} {
		if got := recv(t, ch); got.err != nil {
			t.Errorf("a cancelled request's outcome is its group's: %v", got.err)
		}
	}
	if got := storedPods(t, st.records(), "pod-1", "pod-2", "pod-3"); len(got) != 3 {
		t.Errorf("stored %v, want all three", got)
	}
}

// A request whose context is done before the loop claims it is dropped.
func TestCancelBeforeClaimDrops(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st)
	ctx, cancel := context.WithCancel(context.Background())
	a := h.sendAsync(ctx, logs("", pullPod(t0, p1)))
	h.waitQueued(1)
	cancel()
	got := recv(t, a)
	if !errors.Is(got.err, otlphttp.ErrBusy) || !errors.Is(got.err, context.Canceled) {
		t.Fatalf("answer %v", got.err)
	}
	h.start()
	h.sendOK(logs("", pullPod(t0, p2)))
	if got := storedPods(t, st.records(), "pod-1", "pod-2"); got["pod-1"] || !got["pod-2"] {
		t.Errorf("stored %v, want pod-2 alone", got)
	}
}

// S10, S11 and S13 use a pod pulled every five minutes whose object has no
// creation time, so that every record of it, its first included, is timed at
// its observation with one basis, and its first record starts a run. (A first
// record timed at the object's creation has another basis, and the run would
// start one pull later.)
var steady = pod{"steady", "steady", "", time.Time{}, time.Time{}, "Running"}

// S10. The flush tick: pulls at 0, 5, 10 and 15 min, then none, and the fake
// clock and tick advance every 30 s to 40 min. The run that began at 10 min
// absorbed the refresh at 15 min; its stored copy lapses at 25 min, so the
// flushed record {10 min through 15 min} is written at the first tick at or
// after 25 min less FlushLead: 24 min, and nothing after it.
func TestS10FlushTick(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var writtenAt []time.Duration
	var h *harness
	h = newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func(b []store.Record) {
		if len(b) > 0 {
			writtenAt = append(writtenAt, h.clock.now().Sub(t0))
		}
	}})).start()
	for m := range 4 {
		h.clock.set(at(time.Duration(m) * 5 * time.Minute))
		h.sendOK(logs("", pullPod(h.clock.now(), steady)))
	}
	before := len(st.records())
	for d := 15*time.Minute + 30*time.Second; d <= 40*time.Minute; d += 30 * time.Second {
		h.clock.set(at(d))
		h.tick()
	}
	recs := st.records()[before:]
	if len(recs) != 1 {
		t.Fatalf("%d records flushed, want 1: %v", len(recs), recs)
	}
	r := recs[0]
	if !r.EventTime.Equal(at(10*time.Minute)) || !r.Through.Equal(at(15*time.Minute)) || r.TTL != 15*time.Minute {
		t.Errorf("flushed %s, want {10m through 15m, TTL 15m}", show(r))
	}
	if last := writtenAt[len(writtenAt)-1]; last != 24*time.Minute {
		t.Errorf("flushed at %s, want 24m (the deadline 25m less the lead)", last)
	}
}

// S11. Stop: a request arriving once it is called gets 503 (stopping); the
// claimed request is answered; the absorbed refresh is written by FlushAll; Run
// returns; a second Stop returns at once.
func TestS11Stop(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var g *gate
	hold := atomic.Bool{}
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) {
		if hold.CompareAndSwap(true, false) {
			g.hold()
		}
	}}))
	g = newGate(t)
	h.start()
	h.sendOK(logs("", pullPod(t0, steady)))
	h.clock.set(at(5 * time.Minute))
	hold.Store(true)
	claimed := h.sendAsync(context.Background(), logs("", pullPod(at(5*time.Minute), steady))) // absorbed
	<-g.entered
	stopped := make(chan error, 1)
	go func() { stopped <- h.seq.Stop(context.Background()) }()
	for !sequencer.Stopping(h.seq) {
		time.Sleep(time.Millisecond)
	}
	_, err := h.send(logs("", pullPod(at(5*time.Minute), p1)))
	var off *otlphttp.UnavailableError
	if !errors.As(err, &off) || !strings.Contains(off.Message, "stopping") {
		t.Fatalf("a request after Stop: %v, want 503 stopping", err)
	}
	g.open()
	if got := recv(t, claimed); got.err != nil {
		t.Fatalf("the claimed request: %v", got.err)
	}
	if err := recv(t, stopped); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := recv(t, h.runErr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.runErr <- nil
	recs := st.records()
	last := recs[len(recs)-1]
	if !last.EventTime.Equal(t0) || !last.Through.Equal(at(5*time.Minute)) {
		t.Errorf("the last record is %s, want the absorbed refresh {0 through 5m}", show(last))
	}
	if storedPods(t, recs, "pod-1")["pod-1"] {
		t.Error("the request after Stop was stored")
	}
	done := make(chan error, 1)
	go func() { done <- h.seq.Stop(context.Background()) }()
	if err := recv(t, done); err != nil {
		t.Errorf("second Stop: %v", err)
	}
}

// S11, a request queued but not yet claimed when Stop is called: the loop
// merges and answers it before it writes FlushAll, and Run returns.
func TestS11StopMergesTheQueued(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var g *gate
	hold := atomic.Bool{}
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) {
		if hold.CompareAndSwap(true, false) {
			g.hold()
		}
	}}))
	g = newGate(t)
	h.start()
	h.sendOK(logs("", pullPod(t0, steady)))
	h.clock.set(at(5 * time.Minute))
	hold.Store(true)
	claimed := h.sendAsync(context.Background(), logs("", pullPod(at(5*time.Minute), steady))) // absorbed
	<-g.entered
	queued := h.sendAsync(context.Background(), logs("", pullPod(at(5*time.Minute), p1)))
	h.waitQueued(1)
	stopped := make(chan error, 1)
	go func() { stopped <- h.seq.Stop(context.Background()) }()
	for !sequencer.Stopping(h.seq) {
		time.Sleep(time.Millisecond)
	}
	g.open()
	for _, ch := range []<-chan answer{claimed, queued} {
		if got := recv(t, ch); got.err != nil {
			t.Fatalf("answer %v, want 200", got.err)
		}
	}
	if err := recv(t, stopped); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := recv(t, h.runErr); err != nil {
		t.Fatalf("Run: %v", err)
	}
	h.runErr <- nil
	recs := st.records()
	pos := -1
	fp := podFP(t, "pod-1")
	for i, r := range recs {
		if r.Subject.A == fp {
			pos = i
		}
	}
	last := recs[len(recs)-1]
	if pos < 0 || pos >= len(recs)-1 {
		t.Fatalf("the queued request's record is at %d of %d, want stored before the flush", pos, len(recs))
	}
	if !last.EventTime.Equal(t0) || !last.Through.Equal(at(5*time.Minute)) {
		t.Errorf("the last record is %s, want FlushAll's {0 through 5m}", show(last))
	}
}

// S13. The coalescer's configuration: a pod refreshed every five minutes with a
// 15-minute TTL. At 10 min the record stored is a new run at its own time,
// without Through; under the default configuration it would be an extension
// keyed at the run's start with Through 10 min.
func TestS13CoalescerConfiguration(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st).start()
	for m := range 3 {
		h.clock.set(at(time.Duration(m) * 5 * time.Minute))
		h.sendOK(logs("", pullPod(h.clock.now(), steady)))
	}
	recs := st.records()
	if len(recs) != 2 {
		t.Fatalf("%d records, want 2 (the refresh at 5 min absorbed)", len(recs))
	}
	r := recs[1]
	if !r.EventTime.Equal(at(10*time.Minute)) || !r.Through.IsZero() || r.TTL != 15*time.Minute {
		t.Errorf("at 10 min: %s, want a new run at 10m without Through", show(r))
	}
}

// A request whose translation panics is refused with 400 naming the panic; the
// translators of its cluster name are made anew and the request before it that
// shares the name is translated again from memory, so the store is what it is
// when the panicking request is never sent; a request of another name is not
// translated again.
func TestPanicInTranslation(t *testing.T) {
	run := func(withPanic bool) ([]store.Record, []answer, map[string]int) {
		st := &fakeStore{inner: newMem(t)}
		var mu sync.Mutex
		translated := map[string]int{}
		h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeTranslate: func(_ string, lr capture.LogRecord) {
			uid := uidOfRecord(lr)
			mu.Lock()
			translated[uid]++
			mu.Unlock()
			if uid == "boom" {
				panic("boom")
			}
		}}))
		reqs := []capture.LogsData{
			logs("other", pullPod(t0, p4)),
			logs("", pullPod(t0, p1), pullPod(t0, p5)),
		}
		if withPanic {
			reqs = append(reqs, logs("", pullPod(t0, p2), pullPod(t0, pod{"boom", "boom", "", t0, time.Time{}, "Running"})))
		}
		reqs = append(reqs, logs("", pullPod(t0, p3)))
		var chs []<-chan answer
		for _, d := range reqs {
			chs = append(chs, h.sendAsync(context.Background(), d))
			h.waitQueued(len(chs))
		}
		h.start()
		var answers []answer
		for _, ch := range chs {
			answers = append(answers, recv(t, ch))
		}
		return st.records(), answers, translated
	}
	got, answers, translated := run(true)
	want, _, _ := run(false)
	var refused *otlphttp.RefusedError
	if !errors.As(answers[2].err, &refused) || !strings.Contains(refused.Message, "boom") {
		t.Fatalf("the panicking request: %v, want a 400 naming the panic", answers[2].err)
	}
	for _, i := range []int{0, 1, 3} {
		if answers[i].err != nil {
			t.Errorf("request %d: %v", i, answers[i].err)
		}
	}
	if d := diffRecords(got, want); d != "" {
		t.Fatal(d)
	}
	if translated["pod-1"] != 2 || translated["pod-4"] != 1 {
		t.Errorf("translations %v: want pod-1 twice (rerun) and pod-4 of the other name once", translated)
	}
}

// With no configured name, MaxClusters bounds the cluster names: one more is
// refused, saying so, until an idle name's slot is freed at a tick.
func TestMaxClusters(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	h := newHarness(t, st, withConfig(func(c *sequencer.Config) { c.MaxClusters = 2 })).start()
	h.sendOK(logs("a", pullPod(t0, p1)))
	h.sendOK(logs("b", pullPod(t0, p2)))
	_, err := h.send(logs("c", pullPod(t0, p3)))
	var refused *otlphttp.RefusedError
	if !errors.As(err, &refused) || !strings.Contains(refused.Message, `"c"`) {
		t.Fatalf("a third name: %v, want a 400 naming it", err)
	}
	// a stays in use; b goes idle for more than three pull intervals.
	h.clock.set(at(10 * time.Minute))
	h.sendOK(logs("a", pullPod(at(10*time.Minute), p1)))
	h.clock.set(at(15*time.Minute + time.Second))
	h.tick()
	h.sendOK(logs("c", pullPod(at(15*time.Minute+time.Second), p3)))
	_, err = h.send(logs("b", pullPod(at(15*time.Minute+time.Second), p2)))
	if !errors.As(err, &refused) {
		t.Fatalf("b after c took its slot: %v, want a 400", err)
	}
}

// A closed store answers 503 with the longer Retry-After, and says so; the
// translators and the coalescer are made anew.
func TestClosedStore(t *testing.T) {
	mem := newMem(t)
	st := &fakeStore{inner: mem}
	h := newHarness(t, st).start()
	if err := mem.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := h.send(logs("", pullPod(t0, p1)))
	var off *otlphttp.UnavailableError
	if !errors.As(err, &off) || !strings.Contains(off.Message, "closed") {
		t.Fatalf("answer %v, want 503 naming the closed store", err)
	}
}

// When the sequencer's context is cancelled, Run returns its error and the
// requests still queued are answered 503.
func TestRunContextCancelled(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	var g *gate
	h := newHarness(t, st, withHooks(sequencer.Hooks{BeforeWrite: func([]store.Record) { g.hold() }}))
	g = newGate(t)
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- h.seq.Run(ctx) }()
	a := h.sendAsync(context.Background(), logs("", pullPod(t0, p1)))
	<-g.entered
	b := h.sendAsync(context.Background(), logs("", pullPod(t0, p2)))
	h.waitQueued(1)
	cancel()
	g.open()
	if got := recv(t, a); !errors.Is(got.err, otlphttp.ErrBusy) {
		t.Errorf("the request in the cancelled write: %v, want 503", got.err)
	}
	if got := recv(t, b); got.err == nil {
		t.Error("the queued request was answered 200")
	}
	if err := recv(t, runErr); !errors.Is(err, context.Canceled) {
		t.Errorf("Run: %v", err)
	}
	if _, err := h.send(logs("", pullPod(t0, p3))); err == nil {
		t.Error("a request after Run returned was answered 200")
	}
}

func TestNewChecksTheConfig(t *testing.T) {
	st := &fakeStore{inner: newMem(t)}
	bad := []sequencer.Config{
		{},
		{Store: st, PullInterval: -1},
		{Store: st, MaxClusters: -1},
		{Store: st, ClusterName: "x"},
		{Store: st, ClusterName: strings.Repeat("x", 22), ClusterNameSet: true},
		{Store: st, ClusterName: "", ClusterNameSet: true},
		{Store: st, CollectionInterval: 400 * 24 * time.Hour},
	}
	for i, c := range bad {
		if _, err := sequencer.New(c); err == nil {
			t.Errorf("config %d: no error", i)
		}
	}
	if _, err := sequencer.New(sequencer.Config{Store: st}); err != nil {
		t.Errorf("the zero config with a store: %v", err)
	}
}

func podFP(t testing.TB, uid string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sPod, []identity.Attr{{Key: catalog.K8sPodUID, Value: uid}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}
