package k8scluster_test

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
	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// update rewrites the golden files of this package from the current output. It
// is never on by default; the files it rewrote are named in the test log.
var update = flag.Bool("update", false, "rewrite the golden files in testdata")

const testProducer = lifecycle.Producer("k8scluster/test")

// t0 is the instant most synthetic records are given.
var t0 = time.Date(2026, 10, 9, 16, 0, 0, 0, time.UTC)

// ttl60 is the TTL of an event with a report interval of 60 seconds.
const ttl60 = 3 * time.Minute

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

func newTranslator(t testing.TB, opts ...func(*k8scluster.Config)) *k8scluster.Translator {
	t.Helper()
	c := k8scluster.Config{Producer: testProducer, Resolver: resolver()}
	for _, o := range opts {
		o(&c)
	}
	tr, err := k8scluster.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func distrust(c *k8scluster.Config) { c.DistrustProducerClock = true }

func collection(d time.Duration) func(*k8scluster.Config) {
	return func(c *k8scluster.Config) { c.CollectionInterval = d }
}

// ---- fixtures ----

// input is one log record of a fixture with the place it came from.
type input struct {
	Name string // "file:line", for messages and golden files
	Res  capture.Resource
	Rec  capture.LogRecord
}

// loadFixture reads a file of OTLP JSON lines in testdata.
func loadFixture(t testing.TB, name string) []input {
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

// instantOf is the time a record of a fixture carries.
func instantOf(lr capture.LogRecord) time.Time {
	return time.Unix(0, int64(lr.TimeUnixNano)).UTC()
}

// receivedFor is the receipt time given with a record of a fixture in the
// untrusted runs: a second after the time the record carries.
func receivedFor(lr capture.LogRecord) time.Time { return instantOf(lr).Add(time.Second) }

// translateAll feeds the inputs to tr in order and returns the result of each.
// With a receipt time, each record is given the one receivedFor says.
func translateAll(t testing.TB, tr *k8scluster.Translator, in []input, withReceipt bool) []k8scluster.Result {
	t.Helper()
	out := make([]k8scluster.Result, len(in))
	for i, x := range in {
		var rcv time.Time
		if withReceipt {
			rcv = receivedFor(x.Rec)
		}
		r, err := tr.Translate(x.Res, x.Rec, rcv)
		if err != nil {
			t.Fatalf("%s: %v", x.Name, err)
		}
		out[i] = r
	}
	return out
}

func records(rs []k8scluster.Result) []store.Record {
	var out []store.Record
	for _, r := range rs {
		out = append(out, r.Records...)
	}
	return out
}

// ---- reading the fixtures ----

func attr(lr capture.LogRecord, key string) (capture.AnyValue, bool) {
	for _, kv := range lr.Attributes {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return capture.AnyValue{}, false
}

// typeOf and uidOf are the entity type and the identifying value of an entity
// event.
func typeOf(lr capture.LogRecord) string {
	v, _ := attr(lr, "entity.type")
	if v.StringValue == nil {
		return ""
	}
	return *v.StringValue
}

func uidOf(lr capture.LogRecord) string {
	v, _ := attr(lr, "entity.id")
	if v.KVListValue == nil || len(v.KVListValue.Values) == 0 || v.KVListValue.Values[0].Value.StringValue == nil {
		return ""
	}
	return *v.KVListValue.Values[0].Value.StringValue
}

func descOf(lr capture.LogRecord, key string) string {
	v, _ := attr(lr, "entity.description")
	if v.KVListValue == nil {
		return ""
	}
	for _, kv := range v.KVListValue.Values {
		if kv.Key == key && kv.Value.StringValue != nil {
			return *kv.Value.StringValue
		}
	}
	return ""
}

// find returns the first input of a fixture that satisfies the test, or fails.
func find(t testing.TB, in []input, what string, ok func(input) bool) (int, input) {
	t.Helper()
	for i, x := range in {
		if ok(x) {
			return i, x
		}
	}
	t.Fatalf("no input %s", what)
	return 0, input{}
}

func isEvent(x input, name, typ, uidPrefix string) bool {
	return x.Rec.EventName == name && typeOf(x.Rec) == typ && strings.HasPrefix(uidOf(x.Rec), uidPrefix)
}

// ---- fingerprints ----

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

func ctrFP(t testing.TB, id string) identity.Fingerprint {
	return fp(t, catalog.Container, catalog.ContainerID, id)
}

// ---- golden files ----

type goldenRecord struct {
	Subject   string          `json:"subject"`
	Kind      string          `json:"kind"`
	EventTime string          `json:"event_time"`
	Basis     string          `json:"basis"`
	TTL       string          `json:"ttl"`
	Through   string          `json:"through,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

type goldenSighting struct {
	UID        string `json:"uid"`
	Deleted    bool   `json:"deleted,omitempty"`
	At         string `json:"at"`
	Interval   string `json:"interval"`
	AliveUntil string `json:"alive_until"`
}

type goldenStep struct {
	Input      string           `json:"input"`
	Instant    string           `json:"instant"`
	Records    []goldenRecord   `json:"records,omitempty"`
	Pending    []string         `json:"pending,omitempty"`
	Skipped    map[string]int   `json:"skipped,omitempty"`
	Notes      []string         `json:"notes,omitempty"`
	Namespaces []goldenSighting `json:"namespaces,omitempty"`
}

var kind2name = map[string]string{"pod": "k8s.pod.name", "node": "k8s.node.name"}

// labels names the nodes and pods of a fixture, so a golden file can say
// "pod pre-...-262tj" and not a hash.
func labels(t testing.TB, in []input) map[identity.Fingerprint]string {
	t.Helper()
	out := map[identity.Fingerprint]string{}
	for _, x := range in {
		uid := uidOf(x.Rec)
		short := uid
		if len(short) > 8 {
			short = short[:8]
		}
		var (
			kind string
			fpOf identity.Fingerprint
		)
		switch typeOf(x.Rec) {
		case "k8s.pod":
			kind, fpOf = "pod", podFP(t, uid)
		case "k8s.node":
			kind, fpOf = "node", nodeFP(t, uid)
		default:
			continue
		}
		// A delete has no name: it keeps the one a state gave, and otherwise has
		// the UID alone.
		if n := descOf(x.Rec, kind2name[kind]); n != "" {
			out[fpOf] = kind + " " + n + " " + short
		} else if _, ok := out[fpOf]; !ok {
			out[fpOf] = kind + " " + short
		}
	}
	return out
}

func render(names map[identity.Fingerprint]string, in []input, rs []k8scluster.Result) []goldenStep {
	label := func(f identity.Fingerprint) string {
		if n, ok := names[f]; ok {
			return n
		}
		return f.String()
	}
	var out []goldenStep
	for i, r := range rs {
		uid := uidOf(in[i].Rec)
		if len(uid) > 8 {
			uid = uid[:8]
		}
		s := goldenStep{
			Input:   fmt.Sprintf("%s %s %s %s", in[i].Name, in[i].Rec.EventName, typeOf(in[i].Rec), uid),
			Instant: instantOf(in[i].Rec).Format(time.RFC3339Nano),
		}
		for _, rec := range r.Records {
			g := goldenRecord{
				Kind:      rec.Kind.String(),
				EventTime: rec.EventTime.Format(time.RFC3339Nano),
				Basis:     rec.EventTimeBasis.String(),
				TTL:       rec.TTL.String(),
			}
			if !rec.Through.IsZero() {
				g.Through = rec.Through.Format(time.RFC3339Nano)
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
		for _, n := range r.Notes {
			line := n.Reason
			if n.Detail != "" {
				line += ": " + n.Detail
			}
			s.Notes = append(s.Notes, fmt.Sprintf("%s (x%d)", line, n.Count))
		}
		for _, n := range r.Namespaces {
			s.Namespaces = append(s.Namespaces, goldenSighting{
				UID: n.UID, Deleted: n.Deleted, At: n.At.Format(time.RFC3339Nano),
				Interval: n.Interval.String(), AliveUntil: n.AliveUntil().Format(time.RFC3339Nano),
			})
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

// ---- synthetic records ----

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

func bv(b bool) capture.AnyValue { return capture.AnyValue{BoolValue: &b} }

func kvlist(m map[string]string) capture.AnyValue {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	l := &capture.KeyValueList{}
	for _, k := range keys {
		l.Values = append(l.Values, capture.KeyValue{Key: k, Value: sv(m[k])})
	}
	return capture.AnyValue{KVListValue: l}
}

func ts(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// ent is a synthetic entity event. The zero value of the interval is 60 seconds.
type ent struct {
	Event  string // eventName; "" is entity.state
	Type   string
	IDKey  string
	ID     string
	Time   time.Time // zero: timeUnixNano is not set
	IV     *capture.AnyValue
	NoIV   bool
	Desc   map[string]string
	Extras []capture.KeyValue
}

func (e ent) at(tm time.Time) ent { e.Time = tm; return e }

func (e ent) interval(n int64) ent {
	v := iv(n)
	e.IV = &v
	return e
}

func (e ent) intervalValue(v capture.AnyValue) ent { e.IV = &v; return e }

func (e ent) noInterval() ent { e.NoIV = true; return e }

func (e ent) with(k, v string) ent {
	d := map[string]string{}
	for kk, vv := range e.Desc {
		d[kk] = vv
	}
	d[k] = v
	e.Desc = d
	return e
}

func (e ent) event(name string) ent { e.Event = name; return e }

func (e ent) rec() capture.LogRecord {
	name := e.Event
	if name == "" {
		name = "entity.state"
	}
	lr := capture.LogRecord{EventName: name}
	if !e.Time.IsZero() {
		lr.TimeUnixNano = capture.Uint64(e.Time.UnixNano())
	}
	idv := kvlist(map[string]string{e.IDKey: e.ID})
	lr.Attributes = append(lr.Attributes,
		capture.KeyValue{Key: "entity.id", Value: idv},
		capture.KeyValue{Key: "entity.type", Value: sv(e.Type)},
	)
	if name != "entity.delete" {
		switch {
		case e.IV != nil:
			lr.Attributes = append(lr.Attributes, capture.KeyValue{Key: "entity.report.interval", Value: *e.IV})
		case !e.NoIV:
			lr.Attributes = append(lr.Attributes, capture.KeyValue{Key: "entity.report.interval", Value: iv(60)})
		}
		lr.Attributes = append(lr.Attributes, capture.KeyValue{Key: "entity.description", Value: kvlist(e.Desc)})
	}
	lr.Attributes = append(lr.Attributes, e.Extras...)
	return lr
}

// nodeEv is a state event of a node. A zero created leaves out the creation
// timestamp, so that the event time is the instant.
func nodeEv(uid, name string, at, created time.Time) ent {
	e := ent{Type: "k8s.node", IDKey: "k8s.node.uid", ID: uid, Time: at, Desc: map[string]string{
		"k8s.node.name": name, "k8s.node.condition_ready": "true",
	}}
	if !created.IsZero() {
		e.Desc["node.creation_timestamp"] = ts(created)
	}
	return e
}

// podEv is a state event of a pod; node may be empty.
func podEv(uid, name, node string, at, created time.Time) ent {
	e := ent{Type: "k8s.pod", IDKey: "k8s.pod.uid", ID: uid, Time: at, Desc: map[string]string{
		"k8s.pod.name": name, "k8s.namespace.name": "default", "k8s.pod.phase": "Running",
	}}
	if node != "" {
		e.Desc["k8s.node.name"] = node
	}
	if !created.IsZero() {
		e.Desc["pod.creation_timestamp"] = ts(created)
	}
	return e
}

// ctrEv is a state event of a container with the given runtime id of the pod.
func ctrEv(id, podUID, name string, at, created time.Time) ent {
	e := ent{Type: "container", IDKey: "container.id", ID: id, Time: at, Desc: map[string]string{
		"k8s.container.name": name, "container.status": "running",
	}}
	if podUID != "" {
		e.Desc["k8s.pod.uid"] = podUID
	}
	if !created.IsZero() {
		e.Desc["container.creation_timestamp"] = ts(created)
	}
	return e
}

func deleteEv(typ, uid string, at time.Time) ent {
	keys := map[string]string{
		"k8s.node": "k8s.node.uid", "k8s.pod": "k8s.pod.uid", "container": "container.id",
		"k8s.namespace": "k8s.namespace.uid",
	}
	return ent{Event: "entity.delete", Type: typ, IDKey: keys[typ], ID: uid, Time: at}
}

var noResource capture.Resource

// translate translates a synthetic event with no receipt time.
func translate(t testing.TB, tr *k8scluster.Translator, e ent) k8scluster.Result {
	t.Helper()
	return translateAt(t, tr, e, time.Time{})
}

func translateAt(t testing.TB, tr *k8scluster.Translator, e ent, received time.Time) k8scluster.Result {
	t.Helper()
	r, err := tr.Translate(noResource, e.rec(), received)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// ---- checking results ----

// only returns the single record of a result, failing the test otherwise.
func only(t testing.TB, r k8scluster.Result) store.Record {
	t.Helper()
	if len(r.Records) != 1 {
		t.Fatalf("got %d records, want 1: %s", len(r.Records), describeRecords(r.Records))
	}
	return r.Records[0]
}

func describeRecords(rs []store.Record) string {
	var parts []string
	for _, r := range rs {
		s := fmt.Sprintf("%s %s", r.Kind, r.Subject.A.Type())
		if r.Subject.Kind == store.SubjectEdge {
			s = fmt.Sprintf("%s %s", r.Kind, r.Subject.Relation)
		}
		parts = append(parts, s)
	}
	return strings.Join(parts, ", ")
}

func hasSkip(r k8scluster.Result, reason string) bool { return skipCount(r, reason) > 0 }

func skipCount(r k8scluster.Result, reason string) int {
	for _, s := range r.Skipped {
		if s.Reason == reason {
			return s.Count
		}
	}
	return 0
}

// skips sums the skip counts of results by reason.
func skips(rs ...k8scluster.Result) map[string]int {
	out := map[string]int{}
	for _, r := range rs {
		for _, s := range r.Skipped {
			out[s.Reason] += s.Count
		}
	}
	return out
}

func notesOf(r k8scluster.Result, reason string) []k8scluster.Note {
	var out []k8scluster.Note
	for _, n := range r.Notes {
		if n.Reason == reason {
			out = append(out, n)
		}
	}
	return out
}

func noteCount(r k8scluster.Result, reason string) int {
	n := 0
	for _, x := range notesOf(r, reason) {
		n += x.Count
	}
	return n
}

func noteDetails(r k8scluster.Result, reason string) []string {
	var out []string
	for _, n := range notesOf(r, reason) {
		out = append(out, n.Detail)
	}
	slices.Sort(out)
	return out
}

// edgesOf returns the records of the relation.
func edgesOf(rs []store.Record, rel catalog.RelationType) []store.Record {
	var out []store.Record
	for _, r := range rs {
		if r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == rel {
			out = append(out, r)
		}
	}
	return out
}

func entitiesOf(rs []store.Record, typ catalog.EntityType) []store.Record {
	var out []store.Record
	for _, r := range rs {
		if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == typ {
			out = append(out, r)
		}
	}
	return out
}

func wantTimes(t testing.TB, what string, r store.Record, at time.Time, basis store.EventTimeBasis, through time.Time) {
	t.Helper()
	if !r.EventTime.Equal(at) {
		t.Errorf("%s: event time %s, want %s", what, r.EventTime.Format(time.RFC3339Nano), at.Format(time.RFC3339Nano))
	}
	if r.EventTimeBasis != basis {
		t.Errorf("%s: basis %s, want %s", what, r.EventTimeBasis, basis)
	}
	if !r.Through.Equal(through) {
		t.Errorf("%s: through %s, want %s", what, r.Through.Format(time.RFC3339Nano), through.Format(time.RFC3339Nano))
	}
}
