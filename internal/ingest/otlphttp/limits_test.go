package otlphttp_test

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// where a deep value is put in a batch.
type place string

const (
	inBody      place = "body"
	inRecord    place = "record attribute"
	inResource  place = "resource attribute"
	inScope     place = "scope attribute"
	arrayChain        = "array"
	kvlistChain       = "kvlist"
)

// deepModel is a batch with one value of the given depth (a scalar is 1, each
// array or key-value list adds 1) in the given place.
func deepModel(chain string, depth int, at place) capture.LogsData {
	v := cstr("x")
	for i := 1; i < depth; i++ {
		if chain == arrayChain {
			v = capture.AnyValue{ArrayValue: &capture.ArrayValue{Values: []capture.AnyValue{v}}}
		} else {
			v = capture.AnyValue{KVListValue: &capture.KeyValueList{Values: []capture.KeyValue{{Key: "k", Value: v}}}}
		}
	}
	attr := []capture.KeyValue{{Key: "deep", Value: v}}
	var rl capture.ResourceLogs
	var sl capture.ScopeLogs
	var lr capture.LogRecord
	switch at {
	case inBody:
		lr.Body = &v
	case inRecord:
		lr.Attributes = attr
	case inResource:
		rl.Resource.Attributes = attr
	case inScope:
		sl.Scope.Attributes = attr
	}
	sl.LogRecords = []capture.LogRecord{lr}
	rl.ScopeLogs = []capture.ScopeLogs{sl}
	return capture.LogsData{ResourceLogs: []capture.ResourceLogs{rl}}
}

// deepProto is deepModel as protobuf.
func deepProto(chain string, depth int, at place) *logspb.LogsData {
	v := pstr("x")
	for i := 1; i < depth; i++ {
		if chain == arrayChain {
			v = &commonpb.AnyValue{Value: &commonpb.AnyValue_ArrayValue{ArrayValue: &commonpb.ArrayValue{Values: []*commonpb.AnyValue{v}}}}
		} else {
			v = &commonpb.AnyValue{Value: &commonpb.AnyValue_KvlistValue{KvlistValue: &commonpb.KeyValueList{Values: []*commonpb.KeyValue{{Key: "k", Value: v}}}}}
		}
	}
	return deepProtoValue(v, at)
}

func deepProtoValue(v *commonpb.AnyValue, at place) *logspb.LogsData {
	attr := []*commonpb.KeyValue{{Key: "deep", Value: v}}
	rl := &logspb.ResourceLogs{}
	sl := &logspb.ScopeLogs{}
	lr := &logspb.LogRecord{}
	switch at {
	case inBody:
		lr.Body = v
	case inRecord:
		lr.Attributes = attr
	case inResource:
		rl.Resource = &resourcepb.Resource{Attributes: attr}
	case inScope:
		sl.Scope = &commonpb.InstrumentationScope{Attributes: attr}
	}
	sl.LogRecords = []*logspb.LogRecord{lr}
	rl.ScopeLogs = []*logspb.ScopeLogs{sl}
	return &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{rl}}
}

// Values nest to 32 and no deeper, whatever they are made of, wherever they are
// put and however they are sent.
func TestValueDepthLimit(t *testing.T) {
	for _, chain := range []string{arrayChain, kvlistChain} {
		for _, at := range []place{inBody, inRecord, inResource, inScope} {
			for _, enc := range []string{"json", "protobuf"} {
				for _, c := range []struct {
					depth  int
					status int
				}{{1, 200}, {32, 200}, {33, 400}, {40, 400}} {
					t.Run(fmt.Sprintf("%s %s %s depth %d", enc, chain, at, c.depth), func(t *testing.T) {
						q := request{ctype: jsonType}
						if enc == "json" {
							q.body = encodeJSON(t, deepModel(chain, c.depth, at))
						} else {
							q = request{ctype: protoType, body: marshalProto(t, deepProto(chain, c.depth, at))}
						}
						h, sink := newHandler(otlphttp.Options{})
						rec := do(t, h, q)
						if rec.Code != c.status {
							t.Fatalf("status %d, want %d: %.200s", rec.Code, c.status, rec.Body.String())
						}
						if (sink.calls() == 1) != (c.status == 200) {
							t.Errorf("sink called %d times", sink.calls())
						}
						if c.status == 200 && !reflect.DeepEqual(sink.got[0], deepModel(chain, c.depth, at)) {
							t.Error("the sink did not receive the value that was sent")
						}
					})
				}
			}
		}
	}
}

// A JSON body that nests brackets deeper than 160 is refused before it is
// decoded, in time linear in its size; at 160 it is decoded.
func TestJSONNestingLimit(t *testing.T) {
	// The unknown member "x" is ignored by the model, so the depth is the only
	// thing the body can be refused for: the object is one level, the array of x
	// the rest.
	nested := func(n int) []byte {
		return []byte(`{"resourceLogs":[],"x":` + strings.Repeat("[", n-1) + strings.Repeat("]", n-1) + `}`)
	}
	// A string with brackets and quotes in it does not count.
	inString := []byte(`{"resourceLogs":[],"x":"` + strings.Repeat(`[{\"`, 500) + `"}`)
	for _, c := range []struct {
		name   string
		body   []byte
		status int
	}{
		{"160 levels", nested(160), 200},
		{"161 levels", nested(161), 400},
		{"brackets in a string", inString, 200},
	} {
		t.Run(c.name, func(t *testing.T) {
			h, _ := newHandler(otlphttp.Options{})
			rec := do(t, h, request{ctype: jsonType, body: c.body})
			if rec.Code != c.status {
				t.Fatalf("status %d, want %d: %.200s", rec.Code, c.status, rec.Body.String())
			}
			if c.status == 400 && !strings.Contains(statusOf(t, rec).Message, "nested deeper than 160") {
				t.Errorf("message %q", statusOf(t, rec).Message)
			}
		})
	}

	t.Run("3000 levels of key-value lists are refused at once", func(t *testing.T) {
		const n = 3000
		body := `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"body":` +
			strings.Repeat(`{"kvlistValue":{"values":[{"key":"k","value":`, n) + `{"stringValue":"x"}` +
			strings.Repeat(`}]}}`, n) + `}]}]}]}`
		h, sink := newHandler(otlphttp.Options{})
		start := time.Now()
		rec := do(t, h, request{ctype: jsonType, body: []byte(body)})
		if rec.Code != http.StatusBadRequest || sink.calls() != 0 {
			t.Fatalf("status %d", rec.Code)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("refusing it took %v", d)
		}
	})
}

// The real data of the simulated cluster nests well within the limits, in both
// encodings.
func TestRealRecordsAreWithinTheDepthLimits(t *testing.T) {
	for _, file := range []string{"run1.jsonl", "run2.jsonl"} {
		for i, line := range readCaptureLines(t, "../k8sobjects/testdata/"+file) {
			t.Run(fmt.Sprintf("%s line %d", file, i+1), func(t *testing.T) {
				var msg logspb.LogsData
				if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(line, &msg); err != nil {
					t.Fatal(err)
				}
				for _, q := range []request{
					{ctype: jsonType, body: line},
					{ctype: protoType, body: marshalProto(t, &msg)},
				} {
					h, sink := newHandler(otlphttp.Options{})
					if rec := do(t, h, q); rec.Code != http.StatusOK || sink.calls() != 1 {
						t.Fatalf("%s: status %d: %.200s", q.ctype, rec.Code, rec.Body.String())
					}
				}
			})
		}
	}
}

// A client that declares a length gets a buffer for what it sends and not for
// what it says: claiming the limit and sending ten bytes must not allocate the
// limit.
func TestDeclaredLengthSizesNothing(t *testing.T) {
	h, sink := newHandler(otlphttp.Options{})
	body := []byte("0123456789")
	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, otlphttp.Path, bytes.NewReader(body))
	req.ContentLength = otlphttp.DefaultMaxBodyBytes
	req.Header.Set("Content-Type", jsonType)
	rec := httptest.NewRecorder()

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	h.ServeHTTP(rec, req)
	runtime.ReadMemStats(&after)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", rec.Code)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 2<<20 {
		t.Errorf("the request allocated %d bytes for a body of %d", alloc, len(body))
	}
	if sink.calls() != 0 {
		t.Error("the sink was called")
	}
}

// What a client sends is not echoed back in full: a refusal that names a key
// names the start of it.
func TestRefusalsAreBounded(t *testing.T) {
	huge := strings.Repeat("k", 1<<20)
	strindex := &logspb.LogsData{ResourceLogs: []*logspb.ResourceLogs{{ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
		Attributes: []*commonpb.KeyValue{{Key: huge, KeyStrindex: 1}},
	}}}}}}}
	deep := deepProto(arrayChain, 40, inRecord)
	deep.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes[0].Key = huge
	nestedKey := deepProto(kvlistChain, 3, inRecord)
	nestedKey.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes[0].Value.GetKvlistValue().Values[0].Key = huge
	nestedKey.ResourceLogs[0].ScopeLogs[0].LogRecords[0].Attributes[0].Value.GetKvlistValue().Values[0].KeyStrindex = 1

	cases := []struct {
		name string
		q    request
	}{
		{"protobuf key index", request{ctype: protoType, body: marshalProto(t, strindex)}},
		{"protobuf nested key index", request{ctype: protoType, body: marshalProto(t, nestedKey)}},
		{"protobuf deep value under a long key", request{ctype: protoType, body: marshalProto(t, deep)}},
		{"json bad integer under a long key", request{ctype: jsonType, body: []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"` + huge + `","value":{"intValue":"` + huge + `"}}]}}]}`)}},
		{"json bad double", request{ctype: jsonType, body: []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"k","value":{"doubleValue":"` + huge + `"}}]}}]}`)}},
		{"json two kinds", request{ctype: jsonType, body: []byte(`{"resourceLogs":[{"resource":{"attributes":[{"key":"` + huge + `","value":{"intValue":1,"stringValue":"` + huge + `"}}]}}]}`)}},
		{"json unknown signal", request{ctype: jsonType, body: []byte(`{"` + huge + `":1}`)}},
		{"json garbage", request{ctype: jsonType, body: []byte(`{"resourceLogs":` + huge)}},
		{"json bad trace id", request{ctype: jsonType, body: []byte(`{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"traceId":"` + huge + `"}]}]}]}`)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h, _ := newHandler(otlphttp.Options{})
			rec := do(t, h, c.q)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", rec.Code)
			}
			if n := rec.Body.Len(); n > 1000 {
				t.Errorf("the refusal is %d bytes: %.200s", n, rec.Body.String())
			}
		})
	}
}

// {} is the empty request, like the empty protobuf body; another signal is not.
func TestEmptyRequest(t *testing.T) {
	var fromProto capture.LogsData
	{
		h, sink := newHandler(otlphttp.Options{})
		if rec := do(t, h, request{ctype: protoType}); rec.Code != http.StatusOK {
			t.Fatalf("empty protobuf: status %d", rec.Code)
		}
		fromProto = sink.got[0]
	}
	for _, body := range []string{`{}`, `{ }`, " \n{\n}\r\n", `{"resourceLogs":[]}`} {
		t.Run(fmt.Sprintf("%q", body), func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			rec := do(t, h, request{ctype: jsonType, body: []byte(body)})
			if rec.Code != http.StatusOK || rec.Body.String() != "{}" {
				t.Fatalf("status %d: %q", rec.Code, rec.Body.String())
			}
			if len(sink.got) != 1 || len(sink.got[0].ResourceLogs) != 0 {
				t.Fatalf("the sink got %v", sink.got)
			}
			if body != `{"resourceLogs":[]}` && !reflect.DeepEqual(sink.got[0], fromProto) {
				t.Errorf("the empty JSON request is not the value of the empty protobuf request")
			}
		})
	}
	for _, body := range []string{`{"resourceMetrics":[]}`, `{"resourceSpans":[]}`, `{"resourceLogs":[],"resourceMetrics":[]}`, `{}x`, `{`, `[]`} {
		t.Run(fmt.Sprintf("refuses %q", body), func(t *testing.T) {
			h, sink := newHandler(otlphttp.Options{})
			if rec := do(t, h, request{ctype: jsonType, body: []byte(body)}); rec.Code != http.StatusBadRequest || sink.calls() != 0 {
				t.Fatalf("status %d", rec.Code)
			}
		})
	}
}

// 64-bit integers written as JSON numbers keep every digit.
func TestJSONNumbersKeepPrecision(t *testing.T) {
	const body = `{"resourceLogs":[{"scopeLogs":[{"logRecords":[{"timeUnixNano":9007199254740993,"observedTimeUnixNano":18446744073709551615,"attributes":[{"key":"a","value":{"intValue":9007199254740993}},{"key":"b","value":{"intValue":-9223372036854775808}}]}]}]}]}`
	h, sink := newHandler(otlphttp.Options{})
	if rec := do(t, h, request{ctype: jsonType, body: []byte(body)}); rec.Code != http.StatusOK {
		t.Fatalf("status %d: %s", rec.Code, rec.Body.String())
	}
	rec := sink.got[0].ResourceLogs[0].ScopeLogs[0].LogRecords[0]
	if rec.TimeUnixNano != 9007199254740993 || rec.ObservedTimeUnixNano != 18446744073709551615 {
		t.Errorf("times %d, %d", rec.TimeUnixNano, rec.ObservedTimeUnixNano)
	}
	if *rec.Attributes[0].Value.IntValue != 9007199254740993 || *rec.Attributes[1].Value.IntValue != -9223372036854775808 {
		t.Errorf("integers %d, %d", *rec.Attributes[0].Value.IntValue, *rec.Attributes[1].Value.IntValue)
	}
}

// A client that sends Expect: 100-continue is told to go on, and the request
// then succeeds, against a real server.
func TestExpectContinue(t *testing.T) {
	h, sink := newHandler(otlphttp.Options{})
	srv := httptest.NewServer(h)
	defer srv.Close()
	conn, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	body := marshalProto(t, sampleProto())
	fmt.Fprintf(conn, "POST /v1/logs HTTP/1.1\r\nHost: x\r\nContent-Type: %s\r\nContent-Length: %d\r\nExpect: 100-continue\r\nConnection: close\r\n\r\n", protoType, len(body))
	br := bufio.NewReader(conn)
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusContinue {
		t.Fatalf("first status %d, want 100", resp.StatusCode)
	}
	if _, err := conn.Write(body); err != nil {
		t.Fatal(err)
	}
	resp, err = http.ReadResponse(br, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("final status %d: %s", resp.StatusCode, b)
	}
	if sink.calls() != 1 {
		t.Errorf("sink called %d times", sink.calls())
	}
}

// Requests in flight at once do not disturb one another; run under the race
// detector it also checks the handler shares nothing between them.
func TestConcurrentRequests(t *testing.T) {
	h, sink := newHandler(otlphttp.Options{})
	srv := httptest.NewServer(h)
	defer srv.Close()
	protoBody := marshalProto(t, sampleProto())
	jsonBody := encodeJSON(t, sampleModel())
	posts := []struct {
		ctype, enc string
		body       []byte
	}{
		{protoType, "", protoBody},
		{protoType, "gzip", gz(t, protoBody)},
		{jsonType, "", jsonBody},
		{jsonType, "gzip", gz(t, jsonBody)},
		{jsonType, "", []byte("{")}, // refused
	}
	const workers, each = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*each)
	for w := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range each {
				p := posts[(w+i)%len(posts)]
				req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+otlphttp.Path, bytes.NewReader(p.body))
				if err != nil {
					errs <- err
					return
				}
				req.Header.Set("Content-Type", p.ctype)
				if p.enc != "" {
					req.Header.Set("Content-Encoding", p.enc)
				}
				resp, err := srv.Client().Do(req)
				if err != nil {
					errs <- err
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				want := http.StatusOK
				if string(p.body) == "{" {
					want = http.StatusBadRequest
				}
				if resp.StatusCode != want {
					errs <- fmt.Errorf("status %d, want %d", resp.StatusCode, want)
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	sink.mu.Lock()
	defer sink.mu.Unlock()
	for _, d := range sink.got {
		if !reflect.DeepEqual(d, sampleModel()) {
			t.Fatal("a request reached the sink as something other than the sample")
		}
	}
	if len(sink.got) == 0 {
		t.Fatal("no request reached the sink")
	}
}
