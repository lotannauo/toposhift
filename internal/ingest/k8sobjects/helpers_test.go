package k8sobjects_test

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// update rewrites the golden files of this package from the current output. It
// is never on by default; the files it rewrote are named in the test log.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

const testProducer = lifecycle.Producer("k8sobjects/test")

// ttls are the two modes every property is checked in: watch semantics, and a
// TTL as a poller would set.
var ttls = []time.Duration{0, 15 * time.Minute}

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

func newTranslator(t testing.TB, ttl time.Duration) *k8sobjects.Translator {
	t.Helper()
	tr, err := k8sobjects.New(k8sobjects.Config{Producer: testProducer, Resolver: resolver(), EntityTTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

// input is one log record of a capture with the place it came from.
type input struct {
	Name string // "file:line", for messages and golden files
	Res  capture.Resource
	Rec  capture.LogRecord
}

// loadCapture reads a file of OTLP JSON lines in testdata.
func loadCapture(t testing.TB, name string) []input {
	t.Helper()
	f, err := os.Open(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var out []input
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<24)
	for line := 1; sc.Scan(); line++ {
		d, err := capture.DecodeLogs(json.RawMessage(sc.Bytes()))
		if err != nil {
			t.Fatalf("%s:%d: %v", name, line, err)
		}
		for _, rl := range d.ResourceLogs {
			for _, sl := range rl.ScopeLogs {
				for _, lr := range sl.LogRecords {
					out = append(out, input{Name: fmt.Sprintf("%s:%d", name, line), Res: rl.Resource, Rec: lr})
				}
			}
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// translateAll feeds the inputs to tr in order and returns the result of each.
func translateAll(t testing.TB, tr *k8sobjects.Translator, in []input) []k8sobjects.Result {
	t.Helper()
	out := make([]k8sobjects.Result, len(in))
	for i, x := range in {
		r, err := tr.Translate(x.Res, x.Rec)
		if err != nil {
			t.Fatalf("%s: %v", x.Name, err)
		}
		out[i] = r
	}
	return out
}

func records(rs []k8sobjects.Result) []store.Record {
	var out []store.Record
	for _, r := range rs {
		out = append(out, r.Records...)
	}
	return out
}

// plainOf decodes a body to plain Go values, for the tests that read the
// fixtures.
func plainOf(v capture.AnyValue) any {
	switch {
	case v.StringValue != nil:
		return *v.StringValue
	case v.IntValue != nil:
		return int64(*v.IntValue)
	case v.BoolValue != nil:
		return *v.BoolValue
	case v.ArrayValue != nil:
		out := make([]any, len(v.ArrayValue.Values))
		for i, e := range v.ArrayValue.Values {
			out[i] = plainOf(e)
		}
		return out
	case v.KVListValue != nil:
		out := map[string]any{}
		for _, kv := range v.KVListValue.Values {
			out[kv.Key] = plainOf(kv.Value)
		}
		return out
	}
	return nil
}

// objectOf returns the watch type ("" for a list item) and the object of a record.
func objectOf(lr capture.LogRecord) (string, map[string]any) {
	if lr.Body == nil {
		return "", nil
	}
	b, _ := plainOf(*lr.Body).(map[string]any)
	if o, ok := b["object"].(map[string]any); ok {
		typ, _ := b["type"].(string)
		return typ, o
	}
	return "", b
}

func sub(m map[string]any, keys ...string) any {
	var cur any = m
	for _, k := range keys {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[k]
	}
	return cur
}

func subStr(m map[string]any, keys ...string) string {
	s, _ := sub(m, keys...).(string)
	return s
}

// fp resolves a UID to a fingerprint of the given type.
func fp(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := resolver().Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func podFP(t testing.TB, uid string) identity.Fingerprint {
	return fp(t, catalog.K8sPod, catalog.K8sPodUID, uid)
}

func nodeFP(t testing.TB, uid string) identity.Fingerprint {
	return fp(t, catalog.K8sNode, catalog.K8sNodeUID, uid)
}

func containerFP(t testing.TB, id string) identity.Fingerprint {
	return fp(t, catalog.Container, catalog.ContainerID, id)
}

// labels names the pods and nodes of a capture, so a golden file can say
// "pod pre-...-42s2c" and not a hash.
func labels(t testing.TB, in []input) map[identity.Fingerprint]string {
	t.Helper()
	out := map[identity.Fingerprint]string{}
	for _, x := range in {
		_, o := objectOf(x.Rec)
		uid, name := subStr(o, "metadata", "uid"), subStr(o, "metadata", "name")
		switch subStr(o, "kind") {
		case "Pod":
			out[podFP(t, uid)] = "pod " + name
		case "Node":
			out[nodeFP(t, uid)] = "node " + name + " " + uid[:8]
		}
	}
	return out
}

type goldenRecord struct {
	Subject   string          `json:"subject"`
	Kind      string          `json:"kind"`
	EventTime string          `json:"event_time"`
	Basis     string          `json:"basis"`
	TTL       string          `json:"ttl"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type goldenStep struct {
	Input   string         `json:"input"`
	Observe string         `json:"observed"`
	Records []goldenRecord `json:"records,omitempty"`
	Pending []string       `json:"pending,omitempty"`
	Skipped map[string]int `json:"skipped,omitempty"`
}

func render(t testing.TB, names map[identity.Fingerprint]string, in []input, rs []k8sobjects.Result) []goldenStep {
	t.Helper()
	label := func(f identity.Fingerprint) string {
		if n, ok := names[f]; ok {
			return n
		}
		return f.String()
	}
	var out []goldenStep
	for i, r := range rs {
		s := goldenStep{
			Input:   in[i].Name,
			Observe: time.Unix(0, int64(in[i].Rec.ObservedTimeUnixNano)).UTC().Format(time.RFC3339Nano),
		}
		if in[i].Rec.EventName != "" {
			s.Input += " " + in[i].Rec.EventName
		} else if typ, o := objectOf(in[i].Rec); o != nil {
			if typ == "" {
				typ = "LIST"
			}
			s.Input += " " + typ + " " + subStr(o, "kind") + " " + subStr(o, "metadata", "name")
		}
		for _, rec := range r.Records {
			g := goldenRecord{
				Kind:      rec.Kind.String(),
				EventTime: rec.EventTime.Format(time.RFC3339Nano),
				Basis:     rec.EventTimeBasis.String(),
				TTL:       rec.TTL.String(),
			}
			if rec.Subject.Kind == store.SubjectEdge {
				g.Subject = fmt.Sprintf("%s %s -> %s", rec.Subject.Relation, label(rec.Subject.A), label(rec.Subject.B))
			} else {
				g.Subject = label(rec.Subject.A)
			}
			if len(rec.Payload) > 0 {
				g.Payload = json.RawMessage(rec.Payload)
			}
			s.Records = append(s.Records, g)
		}
		for _, p := range r.Pending {
			s.Pending = append(s.Pending, fmt.Sprintf("%s -> node name %q at %s", label(p.Pod), p.NodeName, p.Time.Format(time.RFC3339Nano)))
		}
		if len(r.Skipped) > 0 {
			s.Skipped = map[string]int{}
			for _, k := range r.Skipped {
				s.Skipped[k.Reason] = k.Count
			}
		}
		out = append(out, s)
	}
	return out
}

// checkGolden compares got with testdata/name, or rewrites the file with -update.
func checkGolden(t *testing.T, name string, got any) {
	t.Helper()
	b, err := json.MarshalIndent(got, "", " ")
	if err != nil {
		t.Fatal(err)
	}
	b = append(b, '\n')
	path := filepath.Join("testdata", name)
	if *update {
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		t.Logf("rewrote %s", path)
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run with -update to create it)", err)
	}
	if string(want) != string(b) {
		t.Fatalf("%s differs from the output; run with -update, read the diff through, and list the file in the commit message\nfirst difference near: %s", path, firstDiff(string(want), string(b)))
	}
}

func firstDiff(a, b string) string {
	i := 0
	for i < len(a) && i < len(b) && a[i] == b[i] {
		i++
	}
	lo := max(0, i-80)
	return fmt.Sprintf("...%q (want) vs %q (got)", a[lo:min(len(a), i+80)], b[lo:min(len(b), i+80)])
}

// skips sums the skip counts of results by reason.
func skips(rs ...k8sobjects.Result) map[string]int {
	out := map[string]int{}
	for _, r := range rs {
		for _, s := range r.Skipped {
			out[s.Reason] += s.Count
		}
	}
	return out
}

// ---- synthetic records ----

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

func bv(b bool) capture.AnyValue { return capture.AnyValue{BoolValue: &b} }

// val converts plain Go values (string, int, bool, []any, map[string]any) to an
// OTLP value, with map keys in sorted order.
func val(v any) capture.AnyValue {
	switch x := v.(type) {
	case nil:
		return capture.AnyValue{}
	case string:
		return sv(x)
	case int:
		return iv(int64(x))
	case int64:
		return iv(x)
	case bool:
		return bv(x)
	case []any:
		a := &capture.ArrayValue{}
		for _, e := range x {
			a.Values = append(a.Values, val(e))
		}
		return capture.AnyValue{ArrayValue: a}
	case map[string]any:
		kvs := &capture.KeyValueList{}
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			kvs.Values = append(kvs.Values, capture.KeyValue{Key: k, Value: val(x[k])})
		}
		return capture.AnyValue{KVListValue: kvs}
	}
	panic(fmt.Sprintf("val: %T", v))
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

func logRec(observed time.Time, body any, resource string) capture.LogRecord {
	b := val(body)
	return capture.LogRecord{
		ObservedTimeUnixNano: capture.Uint64(observed.UnixNano()),
		Body:                 &b,
		Attributes:           []capture.KeyValue{{Key: "k8s.resource.name", Value: sv(resource)}},
	}
}

// watchRec is a watch event; pullRec is a list item.
func watchRec(observed time.Time, typ string, o map[string]any) capture.LogRecord {
	return logRec(observed, map[string]any{"type": typ, "object": o}, resourceOf(o))
}

func pullRec(observed time.Time, o map[string]any) capture.LogRecord {
	return logRec(observed, o, resourceOf(o))
}

func resourceOf(o map[string]any) string {
	if o["kind"] == "Node" {
		return "nodes"
	}
	return "pods"
}

var noResource capture.Resource

// podSpec describes a synthetic pod.
type podSpec struct {
	UID, Name, Node string
	Created         time.Time
	Phase           string
	Conds           []map[string]any // type, status, lastTransitionTime
	Containers      []map[string]any // name, containerID, restartCount, state
	Extra           map[string]any   // merged into metadata
	Heartbeat       string           // an excluded field that changes with every record
}

func (p podSpec) object() map[string]any {
	meta := map[string]any{
		"uid":               p.UID,
		"name":              p.Name,
		"namespace":         "default",
		"creationTimestamp": ts(p.Created),
		"resourceVersion":   p.Heartbeat,
	}
	for k, v := range p.Extra {
		meta[k] = v
	}
	spec := map[string]any{}
	if p.Node != "" {
		spec["nodeName"] = p.Node
	}
	status := map[string]any{"phase": p.Phase}
	if len(p.Conds) > 0 {
		var cs []any
		for _, c := range p.Conds {
			cs = append(cs, c)
		}
		status["conditions"] = cs
	}
	if len(p.Containers) > 0 {
		var cs []any
		for _, c := range p.Containers {
			cs = append(cs, c)
		}
		status["containerStatuses"] = cs
	}
	return map[string]any{"kind": "Pod", "apiVersion": "v1", "metadata": meta, "spec": spec, "status": status}
}

func nodeObject(uid, name string, created time.Time, heartbeat string) map[string]any {
	return map[string]any{
		"kind": "Node", "apiVersion": "v1",
		"metadata": map[string]any{
			"uid": uid, "name": name, "creationTimestamp": ts(created), "resourceVersion": heartbeat,
			"labels": map[string]any{"kubernetes.io/hostname": name, "irrelevant": "x"},
		},
		"status": map[string]any{
			"conditions": []any{map[string]any{
				"type": "Ready", "status": "True", "reason": "KubeletReady",
				"lastHeartbeatTime": heartbeat, "lastTransitionTime": ts(created),
			}},
		},
	}
}

func cond(typ, status string, at time.Time) map[string]any {
	return map[string]any{"type": typ, "status": status, "lastProbeTime": map[string]any{}, "lastTransitionTime": ts(at)}
}

// only returns the single record of a result, failing the test otherwise.
func only(t testing.TB, r k8sobjects.Result) store.Record {
	t.Helper()
	if len(r.Records) != 1 {
		t.Fatalf("got %d records, want 1: %v", len(r.Records), r.Records)
	}
	return r.Records[0]
}

func translate(t testing.TB, tr *k8sobjects.Translator, lr capture.LogRecord) k8sobjects.Result {
	t.Helper()
	r, err := tr.Translate(noResource, lr)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func hasSkip(r k8sobjects.Result, reason string) bool {
	return slices.ContainsFunc(r.Skipped, func(s k8sobjects.Skip) bool { return s.Reason == reason })
}

func joinSubjects(rs []store.Record) string {
	var parts []string
	for _, r := range rs {
		parts = append(parts, fmt.Sprintf("%s %s", r.Kind, r.Subject.A.Type()))
	}
	return strings.Join(parts, ", ")
}
