package sequencer

import (
	"context"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// nopWriter is a store the sequencer is made over and never writes to.
type nopWriter struct{}

func (nopWriter) Write(context.Context, []store.Record) error { return nil }
func (nopWriter) LastSeq() uint64                             { return 0 }
func (nopWriter) LayerHorizon(catalog.Layer) store.Horizon    { return store.Horizon{} }

// countingLookup answers every reference with none, and counts the asks.
type countingLookup struct{ asked int }

func (l *countingLookup) Last(context.Context, store.LookupRef) (store.LastAssertion, error) {
	l.asked++
	return store.LastAssertion{Outcome: store.LookupNone}, nil
}

func (l *countingLookup) Close() error { return nil }

func newInternalSequencer(t *testing.T) *Sequencer {
	t.Helper()
	s, err := New(Config{Store: nopWriter{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.clusters["c"]; !ok {
		s.renewCluster("c")
	}
	return s
}

// Each translator kind of a cluster name maps to its own translator and its own
// Comparable for the resume step.
func TestResumerOfEachKind(t *testing.T) {
	s := newInternalSequencer(t)
	c := s.clusters["c"]
	for _, x := range []struct {
		kind translatorKind
		want resume.Resumer
	}{
		{kindObjects, c.objects},
		{kindCluster, c.cluster},
		{kindNodeAgent, c.nodeagent},
	} {
		got, described := s.resumerOf("c", x.kind)
		if got != x.want {
			t.Errorf("kind %d: the resumer is %T %p, want %T %p", x.kind, got, got, x.want, x.want)
		}
		if described == nil {
			t.Errorf("kind %d: no Comparable", x.kind)
		}
	}
	// The node agents' Comparable is the one of its package: the payload as written.
	_, described := s.resumerOf("c", kindNodeAgent)
	if got := string(described(store.Subject{}, []byte(`{"a":"b"}`))); got != `{"a":"b"}` {
		t.Errorf("the node agents' Comparable gave %s", got)
	}
}

// A call of the node agents' kind that carries a mark reaches resume.Apply, with
// its own translator, and one with none does not: the step skips by the marks,
// not by the kind.
func TestAMarkedNodeAgentCallReachesApply(t *testing.T) {
	s := newInternalSequencer(t)
	id, err := s.resolver.Resolve(catalog.Host, []identity.Attr{{Key: catalog.HostID, Value: "h1"}})
	if err != nil {
		t.Fatal(err)
	}
	t0 := time.Date(2026, 10, 14, 9, 0, 0, 0, time.UTC)
	rec := store.Record{
		Layer: catalog.L1, Subject: store.EntitySubject(id.Fingerprint()), Producer: "nodeagent/c",
		EventTime: t0, Through: t0.Add(time.Minute), Kind: lifecycle.Observe, TTL: 15 * time.Minute,
	}
	newWork := func() *work { return &work{out: [][]store.Record{{rec}}} }

	marked := &countingLookup{}
	w := newWork()
	calls := []call{{slot: 0, name: "c", kind: kindNodeAgent, marks: []resume.Mark{{Index: 0}}}}
	if err := s.resume(context.Background(), w, marked, calls); err != nil {
		t.Fatal(err)
	}
	if marked.asked != 1 || w.resumed.New != 1 {
		t.Errorf("a marked call: %d lookups and %d new, want 1 and 1", marked.asked, w.resumed.New)
	}

	unmarked := &countingLookup{}
	w = newWork()
	calls = []call{{slot: 0, name: "c", kind: kindNodeAgent}}
	if err := s.resume(context.Background(), w, unmarked, calls); err != nil {
		t.Fatal(err)
	}
	if unmarked.asked != 0 || len(w.out[0]) != 1 {
		t.Errorf("a call with no mark: %d lookups, %d records", unmarked.asked, len(w.out[0]))
	}
}

// The legacy form is told from the current form by the record, not by the scope
// attribute alone: the current form carries that attribute too.
func TestLegacyEntityEventSelector(t *testing.T) {
	str := func(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }
	yes := true
	scopeAttr := capture.InstrumentationScope{Attributes: []capture.KeyValue{{Key: scopeEventAsLog, Value: capture.AnyValue{BoolValue: &yes}}}}
	none := capture.InstrumentationScope{}
	attr := func(k string) []capture.KeyValue { return []capture.KeyValue{{Key: k, Value: str("x")}} }
	for _, c := range []struct {
		name  string
		scope capture.InstrumentationScope
		lr    capture.LogRecord
		want  bool
	}{
		{"the current form, with the scope attribute", scopeAttr, capture.LogRecord{EventName: "entity.state", Attributes: attr("entity.type")}, false},
		{"a current delete, with the scope attribute", scopeAttr, capture.LogRecord{EventName: "entity.delete", Attributes: attr("entity.type")}, false},
		{"the legacy form: no event name, the scope attribute", scopeAttr, capture.LogRecord{Attributes: attr("otel.entity.type")}, true},
		{"the legacy form: no event name, the scope attribute only", scopeAttr, capture.LogRecord{}, true},
		{"the legacy form: an otel.entity attribute, no scope attribute", none, capture.LogRecord{Attributes: attr("otel.entity.event.type")}, true},
		{"a current event with an otel.entity attribute", scopeAttr, capture.LogRecord{EventName: "entity.state", Attributes: attr("otel.entity.id")}, true},
		{"an objects record", none, capture.LogRecord{Attributes: attr("k8s.resource.name")}, false},
		{"an objects record in a scope with another name", capture.InstrumentationScope{Name: "toposhift.nodeagent"}, capture.LogRecord{}, false},
	} {
		if got := legacyEntityEvent(c.scope, c.lr); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// kindOf routes by the event name and the type, and a host is read as the cluster
// translator reads a type: the last string value.
func TestKindOf(t *testing.T) {
	str := func(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }
	num := capture.Int64(3)
	typ := func(vs ...capture.AnyValue) []capture.KeyValue {
		var out []capture.KeyValue
		for _, v := range vs {
			out = append(out, capture.KeyValue{Key: "entity.type", Value: v})
		}
		return out
	}
	for _, c := range []struct {
		name string
		lr   capture.LogRecord
		want translatorKind
	}{
		{"a host state", capture.LogRecord{EventName: "entity.state", Attributes: typ(str("host"))}, kindNodeAgent},
		{"a host delete", capture.LogRecord{EventName: "entity.delete", Attributes: typ(str("host"))}, kindNodeAgent},
		{"a pod state", capture.LogRecord{EventName: "entity.state", Attributes: typ(str("k8s.pod"))}, kindCluster},
		{"no type", capture.LogRecord{EventName: "entity.state"}, kindCluster},
		{"the last type wins", capture.LogRecord{EventName: "entity.state", Attributes: typ(str("k8s.pod"), str("host"))}, kindNodeAgent},
		{"a later type that is not a string is ignored", capture.LogRecord{EventName: "entity.state", Attributes: typ(str("host"), capture.AnyValue{IntValue: &num})}, kindNodeAgent},
		{"a host type on a record that is not an entity event", capture.LogRecord{Attributes: typ(str("host"))}, kindObjects},
		{"another event name", capture.LogRecord{EventName: "entity.other", Attributes: typ(str("host"))}, kindObjects},
	} {
		if got := kindOf(c.lr); got != c.want {
			t.Errorf("%s: kind %d, want %d", c.name, got, c.want)
		}
	}
}
