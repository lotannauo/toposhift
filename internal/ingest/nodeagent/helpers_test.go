package nodeagent_test

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/nodeagent"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// updateGolden rewrites the golden files of this package from the current output.
// It is never on by default; the files it rewrote are named in the test log. The
// flag has its own name because other packages of the module define -update for
// their own files.
var updateGolden = flag.Bool("update-nodeagent-golden", false, "rewrite the golden files in testdata")

const testProducer = lifecycle.Producer("nodeagent/test")

// interval300 is the report interval of the node collectors in these tests, and
// ttl900 the TTL it gives.
const (
	interval300 = 300 * time.Second
	ttl900      = 3 * interval300
)

// t0 is the instant most synthetic records are given.
var t0 = time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)

func resolver() *identity.Resolver { return identity.NewResolver(catalog.Default()) }

func newTranslator(t testing.TB, opts ...func(*nodeagent.Config)) *nodeagent.Translator {
	t.Helper()
	c := nodeagent.Config{Producer: testProducer, Resolver: resolver(), Interval: interval300}
	for _, o := range opts {
		o(&c)
	}
	tr, err := nodeagent.New(c)
	if err != nil {
		t.Fatal(err)
	}
	return tr
}

func distrust(c *nodeagent.Config) { c.DistrustProducerClock = true }
func zones(c *nodeagent.Config)    { c.Zones = true }

// ---- fixtures ----

// input is one log record with the resource it came with and the place it came
// from.
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

// The five fixtures: the first four are entity events of a node collector, and the
// last holds records that are not entity events, from a shared pipeline.
var (
	fixtureReal      = "real-nomachineid.logs.jsonl"
	fixtureDistinct  = "synthetic-distinct-machineids.logs.jsonl"
	fixtureShared    = "synthetic-shared-machineid.logs.jsonl"
	fixtureRecreated = "synthetic-node-recreated-then-repeat.logs.jsonl"
	fixtureMarker    = "alt-k8sobjects-nodepull-marker.logs.jsonl"

	entityFixtures = []string{fixtureReal, fixtureDistinct, fixtureShared, fixtureRecreated}
	allFixtures    = append(slices.Clone(entityFixtures), fixtureMarker)
)

// instantOf is the time a record carries.
func instantOf(lr capture.LogRecord) time.Time {
	return time.Unix(0, int64(lr.TimeUnixNano)).UTC()
}

// ---- editing one decoded line ----

func sv(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }

func iv(n int64) capture.AnyValue {
	v := capture.Int64(n)
	return capture.AnyValue{IntValue: &v}
}

// clone returns a copy of x whose resource attributes and record attributes can be
// edited without changing x.
func (x input) clone() input {
	x.Res.Attributes = slices.Clone(x.Res.Attributes)
	x.Rec.Attributes = slices.Clone(x.Rec.Attributes)
	return x
}

// setRes sets a resource attribute, replacing the first of that key or adding one.
func (x input) setRes(key string, v capture.AnyValue) input {
	x = x.clone()
	for i, kv := range x.Res.Attributes {
		if kv.Key == key {
			x.Res.Attributes[i].Value = v
			return x
		}
	}
	x.Res.Attributes = append(x.Res.Attributes, capture.KeyValue{Key: key, Value: v})
	return x
}

// addRes adds a resource attribute without replacing one of the same key.
func (x input) addRes(key string, v capture.AnyValue) input {
	x = x.clone()
	x.Res.Attributes = append(x.Res.Attributes, capture.KeyValue{Key: key, Value: v})
	return x
}

func (x input) setResStr(key, v string) input { return x.setRes(key, sv(v)) }

// dropRes removes every resource attribute of the key.
func (x input) dropRes(key string) input {
	x = x.clone()
	x.Res.Attributes = slices.DeleteFunc(x.Res.Attributes, func(kv capture.KeyValue) bool { return kv.Key == key })
	return x
}

func (x input) resStr(t testing.TB, key string) string {
	t.Helper()
	for _, kv := range x.Res.Attributes {
		if kv.Key == key && kv.Value.StringValue != nil {
			return *kv.Value.StringValue
		}
	}
	t.Fatalf("%s: no string resource attribute %q", x.Name, key)
	return ""
}

// setRec sets a record attribute, replacing the first of that key or adding one.
func (x input) setRec(key string, v capture.AnyValue) input {
	x = x.clone()
	for i, kv := range x.Rec.Attributes {
		if kv.Key == key {
			x.Rec.Attributes[i].Value = v
			return x
		}
	}
	x.Rec.Attributes = append(x.Rec.Attributes, capture.KeyValue{Key: key, Value: v})
	return x
}

func (x input) dropRec(key string) input {
	x = x.clone()
	x.Rec.Attributes = slices.DeleteFunc(x.Rec.Attributes, func(kv capture.KeyValue) bool { return kv.Key == key })
	return x
}

// at sets the time the record carries.
func (x input) at(tm time.Time) input {
	x = x.clone()
	x.Rec.TimeUnixNano = capture.Uint64(tm.UnixNano())
	return x
}

// named sets the place the record is said to come from.
func (x input) named(name string) input { x.Name = name; return x }

// ---- fingerprints ----

func fp(t testing.TB, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := resolver().Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func hostFP(t testing.TB, id string) identity.Fingerprint {
	return fp(t, catalog.Host, catalog.HostID, id)
}

func nodeFP(t testing.TB, uid string) identity.Fingerprint {
	return fp(t, catalog.K8sNode, catalog.K8sNodeUID, uid)
}

func zoneFP(t testing.TB, id string) identity.Fingerprint {
	return fp(t, catalog.Zone, catalog.ZoneID, id)
}

func podFP(t testing.TB, uid string) identity.Fingerprint {
	return fp(t, catalog.K8sPod, catalog.K8sPodUID, uid)
}

// ---- synthetic events ----

// agentEvent is a host entity event as the node collector sends it, for a node
// and a host, at an instant, with a boot and the description keys the detectors
// give.
func agentEvent(hostID, nodeUID string, at time.Time) input {
	x := input{
		Name: "synthetic",
		Res: capture.Resource{Attributes: []capture.KeyValue{
			{Key: "k8s.node.uid", Value: sv(nodeUID)},
			{Key: "k8s.node.name", Value: sv("node-" + nodeUID)},
			{Key: "host.name", Value: sv("pod-" + nodeUID)},
			{Key: "os.type", Value: sv("linux")},
			{Key: "host.id", Value: sv(hostID)},
			{Key: "host.arch", Value: sv("arm64")},
			{Key: "topo.host.boot.id", Value: sv("boot-" + hostID)},
			{Key: "k8s.cluster.name", Value: sv("c1")},
		}},
		Rec: capture.LogRecord{
			EventName: "entity.state",
			Attributes: []capture.KeyValue{
				{Key: "entity.type", Value: sv("host")},
				{Key: "entity.report.interval", Value: iv(300)},
			},
		},
	}
	return x.at(at)
}

// ---- reading results ----

func records(rs []nodeagent.Result) []store.Record {
	var out []store.Record
	for _, r := range rs {
		out = append(out, r.Records...)
	}
	return out
}

// translateAll feeds the inputs to tr in order and returns the result of each.
// With withReceipt, each record is given the time a second after the one it
// carries as its receipt time.
func translateAll(t testing.TB, tr *nodeagent.Translator, in []input, withReceipt bool) []nodeagent.Result {
	t.Helper()
	out := make([]nodeagent.Result, len(in))
	for i, x := range in {
		var rcv time.Time
		if withReceipt {
			rcv = instantOf(x.Rec).Add(time.Second)
		}
		r, err := tr.Translate(x.Res, x.Rec, rcv)
		if err != nil {
			t.Fatalf("%s: %v", x.Name, err)
		}
		out[i] = r
	}
	return out
}

func translate(t testing.TB, tr *nodeagent.Translator, x input, received time.Time) nodeagent.Result {
	t.Helper()
	r, err := tr.Translate(x.Res, x.Rec, received)
	if err != nil {
		t.Fatalf("%s: %v", x.Name, err)
	}
	return r
}

// skipped is the count of one reason in a result.
func skipped(r nodeagent.Result, reason string) int {
	for _, s := range r.Skipped {
		if s.Reason == reason {
			return s.Count
		}
	}
	return 0
}

func noted(r nodeagent.Result, reason, detail string) int {
	for _, n := range r.Notes {
		if n.Reason == reason && n.Detail == detail {
			return n.Count
		}
	}
	return 0
}

func totalSkipped(rs []nodeagent.Result, reason string) int {
	n := 0
	for _, r := range rs {
		n += skipped(r, reason)
	}
	return n
}

// entityOf returns the records of the entity subject of fp, and edgesOf those of
// the edges of a relation.
func entityOf(recs []store.Record, f identity.Fingerprint) []store.Record {
	return slices.DeleteFunc(slices.Clone(recs), func(r store.Record) bool {
		return r.Subject.Kind != store.SubjectEntity || r.Subject.A != f
	})
}

func edgesOf(recs []store.Record, rel catalog.RelationType) []store.Record {
	return slices.DeleteFunc(slices.Clone(recs), func(r store.Record) bool {
		return r.Subject.Kind != store.SubjectEdge || r.Subject.Relation != rel
	})
}

func describeRecords(recs []store.Record) string {
	s := ""
	for _, r := range recs {
		s += fmt.Sprintf("\n  %v %v %s ttl=%s boot=%q payload=%s through=%v", r.Kind, r.Subject.Kind, r.EventTime.Format(time.RFC3339Nano), r.TTL, r.Boot, r.Payload, r.Through)
	}
	return s
}
