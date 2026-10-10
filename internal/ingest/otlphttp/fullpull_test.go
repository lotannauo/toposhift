package otlphttp_test

import (
	"fmt"
	"net/http"
	"reflect"
	"regexp"
	"slices"
	"testing"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// uidValue finds the uid of the pod in a record of the Kubernetes objects
// receiver.
var uidValue = regexp.MustCompile(`("key":"uid","value":\{"stringValue":")([^"]+)(")`)

// A full pull of a cluster is one batch of a record per pod. The pods here are
// a pod record of the simulated cluster's capture repeated with distinct UIDs,
// 400 of them: about 4.4 MB of JSON, above 4 MiB, which a pull of that many pods
// has to get through in one request because a 413 is not retried.
func TestFullPull(t *testing.T) {
	const pods = 400
	line := readCaptureLines(t, "../k8sobjects/testdata/run1.jsonl")[0]
	if n := len(uidValue.FindAll(line, -1)); n != 1 {
		t.Fatalf("the fixture line has %d uids, want 1", n)
	}

	// One record is decoded, and the others are copies of it with their own UID.
	one, err := capture.DecodeLogs(line)
	if err != nil {
		t.Fatal(err)
	}
	want := one
	scope := &want.ResourceLogs[0].ScopeLogs[0]
	first := scope.LogRecords[0]
	scope.LogRecords = make([]capture.LogRecord, 0, pods)
	for i := range pods {
		scope.LogRecords = append(scope.LogRecords, withUID(first, fmt.Sprintf("00000000-0000-4000-8000-%012d", i)))
	}
	if got := len(scope.LogRecords); got != pods {
		t.Fatalf("%d records, want %d", got, pods)
	}

	asJSON := encodeJSON(t, want)
	if len(asJSON) <= 4<<20 {
		t.Fatalf("the pull is %d bytes of JSON; the test means it to be above 4 MiB", len(asJSON))
	}
	var msg logspb.LogsData
	if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(asJSON, &msg); err != nil {
		t.Fatal(err)
	}
	asProto := marshalProto(t, &msg)
	t.Logf("%d pods: %d bytes of JSON, %d bytes of protobuf, %d and %d gzipped", pods, len(asJSON), len(asProto), len(gz(t, asJSON)), len(gz(t, asProto)))

	cases := []struct {
		name string
		q    request
	}{
		{"json", request{ctype: jsonType, body: asJSON}},
		{"json gzip", request{ctype: jsonType, encoding: "gzip", body: gz(t, asJSON)}},
		{"protobuf", request{ctype: protoType, body: asProto}},
		{"protobuf gzip", request{ctype: protoType, encoding: "gzip", body: gz(t, asProto)}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			h, sink := newHandler(otlphttp.Options{})
			rec := do(t, h, c.q)
			if rec.Code != http.StatusOK {
				t.Fatalf("status %d: %.200s", rec.Code, rec.Body.String())
			}
			if len(sink.got) != 1 || !reflect.DeepEqual(sink.got[0], want) {
				t.Fatal("the sink did not receive the 400 pods that were sent")
			}
		})
	}
}

// withUID returns a copy of a pod record with another metadata.uid. Only the
// values on the way to the UID are copied; the rest is shared, and nothing
// writes to it.
func withUID(rec capture.LogRecord, uid string) capture.LogRecord {
	body := *rec.Body
	body.KVListValue = replaceKey(body.KVListValue, "metadata", func(v capture.AnyValue) capture.AnyValue {
		v.KVListValue = replaceKey(v.KVListValue, "uid", func(capture.AnyValue) capture.AnyValue { return cstr(uid) })
		return v
	})
	rec.Body = &body
	return rec
}

// replaceKey returns a copy of l in which the value of key is f of the old one.
func replaceKey(l *capture.KeyValueList, key string, f func(capture.AnyValue) capture.AnyValue) *capture.KeyValueList {
	out := &capture.KeyValueList{Values: slices.Clone(l.Values)}
	for i := range out.Values {
		if out.Values[i].Key == key {
			out.Values[i].Value = f(out.Values[i].Value)
			return out
		}
	}
	panic("no key " + key)
}
