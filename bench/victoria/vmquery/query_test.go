package vmquery_test

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/internal/fixture"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func registry(t *testing.T) *vmmodel.Registry {
	t.Helper()
	reg, err := vmmodel.RegistryFor(fixture.Registered(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	return reg
}

func q(op runner.Op, layer catalog.Layer, dir engine.Direction, at time.Time, fps ...identity.Fingerprint) runner.Query {
	return runner.Query{Group: "g", Op: op, Layer: layer, Dir: dir, Fps: fps, Age: "now", At: at, AsOf: engine.Latest}
}

const (
	nodeHex = "ab000000000000000000000000000000"
	hostHex = "cd000000000000000000000000000000"
	svcHex  = "ef000000000000000000000000000000"
)

var (
	node = fixture.FP(catalog.K8sNode, 0xab)
	host = fixture.FP(catalog.Host, 0xcd)
	svc  = fixture.FP(catalog.Service, 0xef)
)

func mustRewrite(t *testing.T, query runner.Query) vmquery.Plan {
	t.Helper()
	p, err := vmquery.Rewrite(registry(t), query)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// Each family is one term per producer that can hold what is asked, with that producer's
// own window, joined with or, with the entity on the end the direction names. A producer
// in watch mode is one of them, read through the window of its scrape interval (240 s).
func TestRewriteWritesOneTermPerProducerWithItsOwnWindow(t *testing.T) {
	cases := []struct {
		name string
		q    runner.Query
		want string
	}{
		{
			"node<- (pods placed on a node)", q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, node),
			`last_over_time(topo_edge{layer="L2",dst="k8s.node:` + nodeHex + `",producer="k8s-cluster"}[3600s])` +
				` or last_over_time(topo_edge{layer="L2",dst="k8s.node:` + nodeHex + `",producer="k8sobjects"}[240s])` +
				` or last_over_time(topo_edge{layer="L2",dst="k8s.node:` + nodeHex + `",producer="kubelet"}[300s])`,
		},
		{
			"pod-> (the node a pod is on)", q(runner.OpNeighbors, catalog.L2, engine.Forward, t0, fixture.FP(catalog.K8sPod, 1)),
			`last_over_time(topo_edge{layer="L2",src="k8s.pod:01000000000000000000000000000000",producer="k8s-cluster"}[3600s])` +
				` or last_over_time(topo_edge{layer="L2",src="k8s.pod:01000000000000000000000000000000",producer="k8sobjects"}[240s])` +
				` or last_over_time(topo_edge{layer="L2",src="k8s.pod:01000000000000000000000000000000",producer="kubelet"}[300s])`,
		},
		{
			"service<- (the instances of a service): only the watch holds it", q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, svc),
			`last_over_time(topo_edge{layer="L2",dst="service:` + svcHex + `",producer="k8sobjects"}[240s])`,
		},
		{
			"pod exists: only the watch holds it", q(runner.OpAlive, catalog.L2, 0, t0, fixture.FP(catalog.K8sPod, 1)),
			`last_over_time(topo_entity{layer="L2",type="k8s.pod",fp="01000000000000000000000000000000",producer="k8sobjects"}[240s])`,
		},
		{
			"host<- (nodes on a host)", q(runner.OpNeighbors, catalog.L1, engine.Reverse, t0, host),
			`last_over_time(topo_edge{layer="L1",dst="host:` + hostHex + `",producer="node-collector"}[240s])`,
		},
		{
			"node->L1 (the host of a node)", q(runner.OpNeighbors, catalog.L1, engine.Forward, t0, node),
			`last_over_time(topo_edge{layer="L1",src="k8s.node:` + nodeHex + `",producer="node-collector"}[240s])`,
		},
		{
			"service<-L3 (callers of a service)", q(runner.OpNeighbors, catalog.L3, engine.Reverse, t0, svc),
			`last_over_time(topo_edge{layer="L3",dst="service:` + svcHex + `",producer="traces"}[1200s])`,
		},
		{
			"node exists", q(runner.OpAlive, catalog.L2, 0, t0, node),
			`last_over_time(topo_entity{layer="L2",type="k8s.node",fp="` + nodeHex + `",producer="node-collector"}[240s])`,
		},
	}
	for _, c := range cases {
		p := mustRewrite(t, c.q)
		if got := p.Expr.String(); got != c.want {
			t.Errorf("%s\n got %s\nwant %s", c.name, got, c.want)
		}
		if p.Request().Query != c.want {
			t.Errorf("%s: the request does not carry the expression", c.name)
		}
	}
}

// A read at an instant is an instant query at that time, in Unix seconds to the
// millisecond (a time with nanoseconds is truncated, not rounded), with the result cache
// off.
func TestRequestForm(t *testing.T) {
	p := mustRewrite(t, q(runner.OpAlive, catalog.L2, 0, t0.Add(250*time.Millisecond+999_999*time.Nanosecond), node))
	form := p.Request().Form()
	if form.Get("time") != "1767225600.250" {
		t.Errorf("time = %q", form.Get("time"))
	}
	if form.Get("nocache") != "1" {
		t.Errorf("nocache = %q: the result cache would answer a repeated ask", form.Get("nocache"))
	}
	if form.Get("query") != p.Expr.String() {
		t.Errorf("query = %q", form.Get("query"))
	}
	if len(form) != 3 {
		t.Errorf("parameters %v", form)
	}
	if got, err := url.ParseQuery(form.Encode()); err != nil || got.Get("query") != p.Expr.String() {
		t.Errorf("the form does not survive its encoding: %v", err)
	}
	whole := vmquery.Request{Query: "x", At: t0}
	if whole.Form().Get("time") != "1767225600.000" {
		t.Errorf("a whole second: %q", whole.Form().Get("time"))
	}
	if vmquery.QueryPath != "/api/v1/query" {
		t.Errorf("path %q", vmquery.QueryPath)
	}
}

// A batch asks about its entities in one query with a regular expression of them, the
// dots of a type name escaped once for the expression and once for the string, sorted and
// without repeats; one distinct entity is an equality match.
func TestBatchQuery(t *testing.T) {
	a, b := fixture.FP(catalog.K8sNode, 0x02), fixture.FP(catalog.K8sNode, 0x01)
	p := mustRewrite(t, q(runner.OpBatch, catalog.L2, engine.Reverse, t0, a, b, a))
	want := `last_over_time(topo_edge{layer="L2",dst=~"k8s\\.node:01000000000000000000000000000000|k8s\\.node:02000000000000000000000000000000",producer="k8s-cluster"}[3600s])` +
		` or last_over_time(topo_edge{layer="L2",dst=~"k8s\\.node:01000000000000000000000000000000|k8s\\.node:02000000000000000000000000000000",producer="k8sobjects"}[240s])` +
		` or last_over_time(topo_edge{layer="L2",dst=~"k8s\\.node:01000000000000000000000000000000|k8s\\.node:02000000000000000000000000000000",producer="kubelet"}[300s])`
	if got := p.Expr.String(); got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	if len(p.Fps) != 3 || p.Fps[0] != a || p.Fps[2] != a {
		t.Errorf("the plan lost the order or the repeats of the entities: %v", p.Fps)
	}
	one := mustRewrite(t, q(runner.OpBatch, catalog.L2, engine.Reverse, t0, a, a))
	if strings.Contains(one.Expr.String(), "=~") {
		t.Errorf("one distinct entity is a regular expression: %s", one.Expr)
	}
}

// The values of labels are put in string literals and, in a regular expression, escaped
// as a pattern first: nothing a value holds can end the literal or the selector.
func TestEscaping(t *testing.T) {
	term := vmquery.Term{
		Metric: vmmodel.MetricEdge, Layer: catalog.L2, Label: vmmodel.LabelDst,
		Values:   []string{`a"b\c` + "\n" + `d`},
		Producer: "p\"q", Window: 1500 * time.Millisecond,
	}
	got := term.String()
	want := `last_over_time(topo_edge{layer="L2",dst="a\"b\\c\nd",producer="p\"q"}[1500ms])`
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	re := vmquery.Term{Metric: vmmodel.MetricEdge, Layer: catalog.L2, Label: vmmodel.LabelDst, Values: []string{`a.b|c`, `d"e`}, Producer: "p", Window: time.Minute}
	if got, want := re.String(), `last_over_time(topo_edge{layer="L2",dst=~"a\\.b\\|c|d\"e",producer="p"}[60s])`; got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	ctl := vmquery.Term{Metric: vmmodel.MetricEdge, Layer: catalog.L2, Label: vmmodel.LabelDst, Values: []string{"a\x01b\x7f\r\t"}, Producer: "p", Window: time.Second}
	if got, want := ctl.String(), `last_over_time(topo_edge{layer="L2",dst="a\x01b\x7f\r\t",producer="p"}[1s])`; got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestRefusalsAreRowsOfTheCapabilityTable(t *testing.T) {
	old := q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, node)
	old.AsOf = 17
	cases := []struct {
		name string
		q    runner.Query
		row  string
	}{
		{"a window", q(runner.OpWindow, catalog.L2, engine.Reverse, t0, node), vmmodel.CapWindow},
		{"a read as of an old token", old, vmmodel.CapAsKnownAt},
		{"an existence read as of an old token", func() runner.Query { x := q(runner.OpAlive, catalog.L2, 0, t0, node); x.AsOf = 3; return x }(), vmmodel.CapAsKnownAt},
	}
	for _, c := range cases {
		_, err := vmquery.Rewrite(registry(t), c.q)
		var ne *vmquery.NotExpressibleError
		if !errors.Is(err, vmquery.ErrNotExpressible) || !errors.As(err, &ne) {
			t.Errorf("%s: err = %v", c.name, err)
			continue
		}
		if ne.Capability != c.row {
			t.Errorf("%s: row %q, want %q", c.name, ne.Capability, c.row)
		}
		if row, ok := vmmodel.CapabilityByID(ne.Capability); !ok || row.Support != vmmodel.No {
			t.Errorf("%s: %q is not a row that says No", c.name, ne.Capability)
		}
		if !strings.Contains(err.Error(), ne.Capability) {
			t.Errorf("%s: the message does not name the row: %v", c.name, err)
		}
	}
}

// A read the store cannot express is refused whatever its other fields say: an old token
// with a window, or a window as of the latest.
func TestAWindowIsRefusedAsAWindowEvenAtAnOldToken(t *testing.T) {
	x := q(runner.OpWindow, catalog.L2, engine.Reverse, t0, node)
	x.AsOf = 4
	var ne *vmquery.NotExpressibleError
	if _, err := vmquery.Rewrite(registry(t), x); !errors.As(err, &ne) || ne.Capability != vmmodel.CapWindow {
		t.Errorf("err = %v", err)
	}
}

func TestRewriteRefusesWhatIsMalformed(t *testing.T) {
	reg := registry(t)
	zero := q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, node)
	zero.Fps = nil
	badDir := q(runner.OpNeighbors, catalog.L2, 0, t0, node)
	two := q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, node, host)
	aliveTwo := q(runner.OpAlive, catalog.L2, 0, t0, node, host)
	badLayer := q(runner.OpNeighbors, 0, engine.Reverse, t0, node)
	unknown := q("nope", catalog.L2, engine.Reverse, t0, node)
	for name, x := range map[string]runner.Query{"no entity": zero, "a direction that is not one": badDir, "two entities in one neighbors read": two, "two entities in one existence read": aliveTwo, "a layer that is not one": badLayer, "an unknown read": unknown} {
		_, err := vmquery.Rewrite(reg, x)
		if err == nil || errors.Is(err, vmquery.ErrNotExpressible) {
			t.Errorf("%s: err = %v", name, err)
		}
	}
	// An entity type no relation touches in the layer and direction has nothing to read.
	if _, err := vmquery.Rewrite(reg, q(runner.OpNeighbors, catalog.L0, engine.Reverse, t0, node)); !errors.Is(err, vmquery.ErrNoRelation) {
		t.Errorf("err = %v", err)
	}
}

// A producer in watch mode asserts once, and what a scraper writes of it is read through
// the window of its scrape interval: no question is left unmodelled for want of a
// window, and no refusal but the capability rows exists.
func TestEveryExpressibleQuestionHasATermPerHolder(t *testing.T) {
	reg := registry(t)
	cases := map[string]runner.Query{
		"the service instances of a service": q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, svc),
		"the same, batched":                  q(runner.OpBatch, catalog.L2, engine.Reverse, t0, svc, svc),
		"the existence of a pod":             q(runner.OpAlive, catalog.L2, 0, t0, fixture.FP(catalog.K8sPod, 1)),
		"the containers of a pod":            q(runner.OpNeighbors, catalog.L2, engine.Reverse, t0, fixture.FP(catalog.K8sPod, 1)),
		"the rack of a host":                 q(runner.OpNeighbors, catalog.L1, engine.Forward, t0, host),
	}
	for name, c := range cases {
		p, err := vmquery.Rewrite(reg, c)
		if err != nil || len(p.Expr) == 0 {
			t.Errorf("%s: %v, %v", name, p.Expr, err)
			continue
		}
		for _, term := range p.Expr {
			if h, ok := reg.Window(term.Producer); !ok || h != term.Window || term.Window <= 0 {
				t.Errorf("%s: term %s has window %s, the registry's is %s", name, term, term.Window, h)
			}
		}
	}
	// The one refusal that is not a capability row is a read nothing in the catalog answers.
	if _, err := vmquery.Rewrite(reg, q(runner.OpNeighbors, catalog.L0, engine.Reverse, t0, node)); !errors.Is(err, vmquery.ErrNoRelation) || errors.Is(err, vmquery.ErrNotExpressible) {
		t.Errorf("err = %v", err)
	}
}
