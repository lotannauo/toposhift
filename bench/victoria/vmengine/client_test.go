package vmengine_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/snappy"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/bench/victoria/vmwrite"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// fake is a server that records what it is asked and answers from a function.
type fake struct {
	*httptest.Server
	mu    sync.Mutex
	calls []string
	forms []url.Values
	body  []byte
	hdr   http.Header
}

func newFake(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) (*fake, *vmengine.Client) {
	t.Helper()
	f := &fake{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.calls = append(f.calls, r.Method+" "+r.URL.Path)
		if r.Header.Get("Content-Type") == "application/x-www-form-urlencoded" {
			v, _ := url.ParseQuery(string(b))
			f.forms = append(f.forms, v)
		} else {
			f.body, f.hdr = b, r.Header.Clone()
		}
		f.mu.Unlock()
		r.Body = io.NopCloser(strings.NewReader(string(b)))
		h(w, r)
	}))
	t.Cleanup(f.Close)
	c, err := vmengine.NewClient(f.URL)
	if err != nil {
		t.Fatal(err)
	}
	return f, c
}

func TestNewClientRefusesWhatIsNotALoopbackServer(t *testing.T) {
	for _, ok := range []string{"http://127.0.0.1:8428", "http://localhost:8428/", "http://[::1]:8428"} {
		if _, err := vmengine.NewClient(ok); err != nil {
			t.Errorf("%s: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "127.0.0.1:8428", "ftp://127.0.0.1", "http://example.com:8428", "http://10.0.0.5:8428", "http://127.0.0.1:8428/api", "http://127.0.0.1:8428?x=1", "http://"} {
		if _, err := vmengine.NewClient(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}

const emptyVector = `{"status":"success","data":{"resultType":"vector","result":[]}}`

// A read is an instant query posted as a form: the expression, the time to the
// millisecond, nocache=1. The answer is read as a vector, and a failure is an error and
// never an empty answer.
func TestInstantPostsTheFormAndReadsTheVector(t *testing.T) {
	node := fixture.FP(catalog.K8sNode, 1)
	f, c := newFake(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"status":"success","data":{"resultType":"vector","result":[{"metric":{"layer":"L1","rel":"runs_on","src":%q,"dst":"host:%s","producer":"node-collector"},"value":[1,"1"]}]}}`,
			node.String(), strings.Repeat("00", 16))
	})
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	e := vmengine.New(c, reg, 10)
	ns, err := e.Neighbors(node, engine.Forward, t0.Add(1500*time.Millisecond), engine.Current(catalog.L1))
	if err != nil || len(ns) != 1 || ns[0].Relation != catalog.RunsOn {
		t.Fatalf("%v, %v", ns, err)
	}
	if len(f.calls) != 1 || f.calls[0] != "POST /api/v1/query" {
		t.Fatalf("calls %v", f.calls)
	}
	form := f.forms[0]
	if form.Get("time") != "1767225601.500" || form.Get("nocache") != "1" || !strings.HasPrefix(form.Get("query"), "last_over_time(topo_edge{") || !strings.Contains(form.Get("query"), `producer="node-collector"}[240s])`) {
		t.Errorf("form %v", form)
	}
	if e.Client() != c {
		t.Error("the engine's client")
	}

	for name, h := range map[string]func(http.ResponseWriter, *http.Request){
		"a query that failed": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusUnprocessableEntity)
			fmt.Fprint(w, `{"status":"error","errorType":"bad_data","error":"cannot parse"}`)
		},
		"a partial answer": func(w http.ResponseWriter, _ *http.Request) {
			fmt.Fprint(w, `{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[]}}`)
		},
		"a status that is not 200 with an answer": func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			fmt.Fprint(w, emptyVector)
		},
		"not JSON": func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "<html>") },
	} {
		_, c := newFake(t, h)
		e := vmengine.New(c, reg, 10)
		if ns, err := e.Neighbors(node, engine.Forward, t0, engine.Current(catalog.L1)); err == nil || ns != nil {
			t.Errorf("%s: %v, %v", name, ns, err)
		}
	}
	// Nothing listening is an error too.
	dead, _ := vmengine.NewClient("http://127.0.0.1:1")
	if _, err := vmengine.New(dead, reg, 1).Alive(node, t0, engine.Current(catalog.L2)); err == nil {
		t.Error("a refused connection is an answer")
	}
}

// What the store cannot express is refused before the server is asked.
func TestEngineRefusesWhatTheStoreCannotExpressWithoutAsking(t *testing.T) {
	f, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, emptyVector) })
	reg, _ := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	e := vmengine.New(c, reg, 10)
	node := fixture.FP(catalog.K8sNode, 1)
	var ne *vmquery.NotExpressibleError
	if _, err := e.Window(node, engine.Reverse, t0, t0.Add(time.Hour), engine.Current(catalog.L2)); !errors.As(err, &ne) || ne.Capability != vmmodel.CapWindow {
		t.Errorf("window: %v", err)
	}
	old := engine.Scope{Layer: catalog.L2, AsOf: 5}
	if _, err := e.Neighbors(node, engine.Reverse, t0, old); !errors.As(err, &ne) || ne.Capability != vmmodel.CapAsKnownAt {
		t.Errorf("old token: %v", err)
	}
	if _, err := runner.Ask(e, runner.Query{Op: runner.OpAlive, Layer: catalog.L2, Fps: []identity.Fingerprint{node}, At: t0, AsOf: 3}); !errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("alive at an old token: %v", err)
	}
	if len(f.calls) != 0 {
		t.Errorf("the server was asked %v", f.calls)
	}
	if err := e.Write(nil); !errors.Is(err, vmquery.ErrReadOnly) {
		t.Errorf("Write: %v", err)
	}
	if e.LastSeq() != 10 {
		t.Error("LastSeq")
	}
}

func TestWritePostsSnappyProtobufAndExpects204(t *testing.T) {
	f, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	ts := []vmwrite.TimeSeries{{Labels: []vmmodel.Label{{Name: "__name__", Value: "x"}}, Samples: []vmwrite.Sample{vmwrite.One(1500)}}}
	req := vmwrite.Request{Body: vmwrite.Compress(vmwrite.Marshal(ts)), Samples: 1, Series: 1}
	if err := c.Write(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if f.calls[0] != "POST /api/v1/write" {
		t.Errorf("calls %v", f.calls)
	}
	if f.hdr.Get("Content-Encoding") != "snappy" || f.hdr.Get("Content-Type") != "application/x-protobuf" || f.hdr.Get("X-Prometheus-Remote-Write-Version") != "0.1.0" {
		t.Errorf("headers %v", f.hdr)
	}
	if dec, err := snappy.Decode(nil, f.body); err != nil || string(dec) != string(vmwrite.Marshal(ts)) {
		t.Errorf("the body is not the snappy block of the message: %v", err)
	}
	for _, code := range []int{200, 400, 500} {
		_, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code); fmt.Fprint(w, "no") })
		if err := c.Write(context.Background(), req); err == nil {
			t.Errorf("status %d is not an acceptance", code)
		}
	}
}

func TestFlagsOfTheServerAreParsed(t *testing.T) {
	_, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, "-dedup.minScrapeInterval=\"0s\"\n-httpListenAddr=\":8428\"\n-search.maxStalenessInterval=\"1h0m0s\"\n-retentionPeriod=\"100y\"\n")
	})
	got, err := c.Flags(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"dedup.minScrapeInterval": "0s", "httpListenAddr": ":8428", "search.maxStalenessInterval": "1h0m0s", "retentionPeriod": "100y"}
	if len(got) != len(want) {
		t.Fatalf("%v", got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}
	_, bad := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, "garbage\n") })
	if _, err := bad.Flags(context.Background()); err == nil {
		t.Error("a line that is not a flag")
	}
}

func TestHasSeries(t *testing.T) {
	for body, want := range map[string]bool{
		`{"status":"success","data":[]}`:                         false,
		`{"status":"success","data":[{"__name__":"topo_edge"}]}`: true,
	} {
		f, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) })
		got, err := c.HasSeries(context.Background(), `{__name__=~"topo_.*"}`)
		if err != nil || got != want {
			t.Errorf("%s: %v, %v", body, got, err)
		}
		if f.calls[0] != "GET /api/v1/series" {
			t.Errorf("calls %v", f.calls)
		}
	}
	_, c := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"status":"error"}`) })
	if _, err := c.HasSeries(context.Background(), "x"); err == nil {
		t.Error("an error answer is taken for no series")
	}
}

// ---- the server's counters ----

type scripted struct {
	mu    sync.Mutex
	reads int
	steps []map[string]string // the values of the metrics at each read; the last repeats
	log   []string
}

func (s *scripted) handler(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r.URL.Path {
	case "/metrics":
		i := min(s.reads, len(s.steps)-1)
		s.reads++
		base := map[string]string{
			`vm_rows_inserted_total{type="promremotewrite"}`: "0", "vm_rows_added_to_storage_total": "0", "vm_new_timeseries_created_total": "0",
			`vm_rows{type="storage/small"}`: "0", "vm_active_force_merges": "0", `vm_pending_rows{type="storage"}`: "0",
		}
		for k, v := range s.steps[i] {
			base[k] = v
		}
		for k, v := range base {
			fmt.Fprintf(w, "%s %s\n", k, v)
		}
		fmt.Fprintln(w, "# a comment")
		fmt.Fprintln(w, `vm_rows_ignored_total{reason="a"} 0`)
	default:
		s.log = append(s.log, r.URL.Path)
	}
}

func TestCountersAreReadFromMetrics(t *testing.T) {
	s := &scripted{steps: []map[string]string{{
		`vm_rows_inserted_total{type="promremotewrite"}`: "12", "vm_rows_added_to_storage_total": "11", "vm_new_timeseries_created_total": "3",
		`vm_rows{type="storage/inmemory"}`: "1", `vm_rows{type="storage/small"}`: "2", `vm_rows{type="storage/big"}`: "4",
		`vm_rows_ignored_total{reason="b"}`: "5",
		`vm_parts{type="storage/small"}`:    "6", `vm_active_merges{type="storage/big"}`: "1", "vm_active_force_merges": "1",
		`vm_data_size_bytes{type="storage/small"}`: "100", `vm_data_size_bytes{type="storage/big"}`: "50",
	}}}
	_, c := newFake(t, s.handler)
	got, err := c.Counters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	want := vmengine.Counters{Received: 12, Added: 11, Ignored: 5, NewSeries: 3, Stored: 7, SmallParts: 6, Merging: 2, DataBytes: 150}
	if got != want {
		t.Errorf("%+v, want %+v", got, want)
	}
	_, empty := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "vm_other 1") })
	if _, err := empty.Counters(context.Background()); !errors.Is(err, vmengine.ErrNoMetric) {
		t.Errorf("a server that does not report the counters: %v", err)
	}
	_, bad := newFake(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "vm_other one") })
	if _, err := bad.Counters(context.Background()); err == nil {
		t.Error("a value that is not a number")
	}
}

// The server is idle only after rows have left memory, merges are done and nothing has
// changed over several reads: a single quiet read between two busy ones is not idle.
func TestSettleWaitsForQuiet(t *testing.T) {
	busy := func(parts string) map[string]string {
		return map[string]string{`vm_parts{type="storage/small"}`: parts, `vm_parts{type="storage/inmemory"}`: "3"}
	}
	quiet := func(parts string, merges string) map[string]string {
		return map[string]string{`vm_parts{type="storage/small"}`: parts, `vm_merges_total{type="storage/small"}`: merges}
	}
	s := &scripted{steps: []map[string]string{
		busy("8"), quiet("8", "1"), {`vm_active_merges{type="storage/small"}`: "1"}, // a merge runs
		quiet("8", "1"), quiet("3", "4"), // parts changed
		quiet("3", "4"), quiet("3", "4"), // two quiet reads after the change
	}}
	_, c := newFake(t, s.handler)
	got, err := c.Settle(context.Background(), time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	// Read 1 has rows in memory, read 3 a merge running, and read 5 has the parts changed
	// since read 4: the run that counts is reads 5, 6 and 7.
	if s.reads != 7 {
		t.Errorf("%d reads", s.reads)
	}
	if got.SmallParts != 3 || got.Merges != 4 {
		t.Errorf("%+v", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	forever := &scripted{steps: []map[string]string{busy("1")}}
	_, c2 := newFake(t, forever.handler)
	if _, err := c2.Settle(ctx, time.Millisecond, 3); err == nil {
		t.Error("a server that is never idle settled")
	}
}

// Compact flushes, waits for quiet, forces the merge and waits for quiet again, in that
// order.
func TestCompactOrder(t *testing.T) {
	s := &scripted{steps: []map[string]string{{}}}
	_, c := newFake(t, s.handler)
	if _, err := c.Compact(context.Background(), time.Millisecond, 2); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(s.log, " "); got != "/internal/force_flush /internal/force_merge" {
		t.Errorf("calls %q", got)
	}
}

// ---- loading ----

func fixtureConverter(t *testing.T, emit func([]vmmodel.Sample) error) *vmmodel.Converter {
	t.Helper()
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	conv, err := vmmodel.NewConverter(vmmodel.Options{Registry: reg, Until: t0.Add(10 * time.Minute), Emit: emit})
	if err != nil {
		t.Fatal(err)
	}
	return conv
}

func nodeBeats(n int) []engine.Record {
	node := fixture.FP(catalog.K8sNode, 1)
	var recs []engine.Record
	for i := 0; i < n; i++ {
		recs = append(recs, engine.Record{Layer: catalog.L2, Subject: engine.EntitySubject(node), Producer: "node-collector", EventTime: t0.Add(time.Duration(i) * time.Minute), Seq: uint64(i + 1), Kind: lifecycle.Observe, TTL: 240 * time.Second})
	}
	return recs
}

// metricsAfterWrites serves counters that are zero until a request is written and the
// scenario's after it, as a server whose counters are those of what it was sent.
func metricsAfterWrites(received, added, ignored, series int) func(w http.ResponseWriter, r *http.Request) {
	var mu sync.Mutex
	written := false
	return func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/v1/write":
			written = true
			w.WriteHeader(http.StatusNoContent)
		case "/metrics":
			rc, ad, ig, se := received, added, ignored, series
			if !written {
				rc, ad, ig, se = 0, 0, 0, 0
			}
			fmt.Fprintf(w, "vm_rows_inserted_total{type=\"promremotewrite\"} %d\nvm_rows_added_to_storage_total %d\nvm_new_timeseries_created_total %d\nvm_rows_ignored_total{reason=\"x\"} %d\nvm_rows{type=\"storage/small\"} 0\nvm_active_force_merges 0\n", rc, ad, se, ig)
		}
	}
}

func load(t *testing.T, c *vmengine.Client, size int) (*vmengine.Loader, error) {
	t.Helper()
	defer vmengine.SetSettleInterval(vmengine.SetSettleInterval(time.Millisecond))
	var ld *vmengine.Loader
	conv := fixtureConverter(t, func(ss []vmmodel.Sample) error { return ld.Emit(ss) })
	ld, err := vmengine.NewLoader(context.Background(), c, conv, size)
	if err != nil {
		t.Fatal(err)
	}
	ld.Wait = 50 * time.Millisecond
	if err := conv.Write(nodeBeats(10)); err != nil {
		t.Fatal(err)
	}
	if err := conv.Close(); err != nil {
		t.Fatal(err)
	}
	if err := ld.Flush(); err != nil {
		t.Fatal(err)
	}
	_, err = ld.Verify(context.Background())
	return ld, err
}

// A loader sends the converter's samples in requests of the size asked, and Verify
// compares the server's counters, as they stand after the writes, with the converter's.
func TestLoaderSendsAndVerifies(t *testing.T) {
	f, c := newFake(t, metricsAfterWrites(10, 10, 0, 1))
	ld, err := load(t, c, 4)
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	for _, call := range f.calls {
		if call == "POST /api/v1/write" {
			writes++
		}
	}
	if ld.Samples != 10 || ld.Requests != 3 || writes != 3 { // 4 + 4 + 2
		t.Errorf("sent %d samples in %d requests (%d seen)", ld.Samples, ld.Requests, writes)
	}
}

func TestLoaderVerifyCatchesWhatTheServerDidNotTakeIn(t *testing.T) {
	for name, c := range map[string]struct{ received, added, ignored, series int }{
		"a row ignored":        {10, 10, 1, 1},
		"a row added less":     {10, 9, 0, 1},
		"a series too many":    {10, 10, 0, 2},
		"a row received twice": {11, 11, 0, 1},
		"nothing ever arrived": {0, 0, 0, 0},
	} {
		_, cl := newFake(t, metricsAfterWrites(c.received, c.added, c.ignored, c.series))
		if _, err := load(t, cl, 100); !errors.Is(err, vmengine.ErrNotIngested) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
}
