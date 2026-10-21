// Package vmquery rewrites the spike's reads as instant queries for a PromQL-compatible
// metrics store, and reads the store's answers back as the spike's engines return them.
//
// A metrics store has no time-to-live of its own and no notion of "alive at t". What it
// has is samples, and a user asks "which pods were on node X at t?" by taking the last
// sample of each series within a window before t. The window has to be chosen by whoever
// writes the query, from how often the producer of the series samples. One lookback for
// the whole store cannot serve a producer that samples every minute and one that samples
// every quarter of an hour, so a query here names, for each producer that can hold what
// it asks about, that producer's own window ([vmmodel.Registry]):
//
//	last_over_time(topo_edge{layer="L2",dst="k8s.node:...",producer="k8s-cluster"}[3600s])
//	  or last_over_time(topo_edge{layer="L2",dst="k8s.node:...",producer="kubelet"}[300s])
//
// joined with or. A read at an instant is an instant query at time=t with nocache=1.
// Every holder has a window: a producer in watch mode is re-exported at a scrape interval
// (see [vmmodel.Holder]) and read through the window of the heartbeat TTL factor times
// it. A question the store cannot express is refused with a [NotExpressibleError] that
// names the row of the capability table it falls under.
//
// The package also holds [Evaluate], a model of the server for these queries over a
// [vmmodel.Store], so that the rewriting is checked against the reference without a
// server, and [Engine], which answers a plan's questions through any [Backend]: the
// model, or the server over HTTP (package vmengine). What the model assumes of the server
// is listed in package vmmodel, each with whether a server confirmed it.
package vmquery

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// ErrNotExpressible is wrapped by the error of a question the metrics store cannot be
// asked. Use errors.As with [NotExpressibleError] for the capability row.
var ErrNotExpressible = errors.New("vmquery: the metrics store cannot express this question")

// NotExpressibleError is a refusal. Capability is the ID of a row of
// [vmmodel.Capabilities]: the refusal is a row of the table, not a failure of the store.
type NotExpressibleError struct {
	Capability string
	Question   string
	Why        string
}

func (e *NotExpressibleError) Error() string {
	return fmt.Sprintf("%s: %s (capability %q): %s", ErrNotExpressible, e.Question, e.Capability, e.Why)
}

// Unwrap lets errors.Is find [ErrNotExpressible].
func (e *NotExpressibleError) Unwrap() error { return ErrNotExpressible }

// ErrNoRelation marks a read of an entity type no relation of the catalog touches in the
// layer and direction: nothing could ever be returned, so there is no query to write.
var ErrNoRelation = errors.New("vmquery: no relation of the catalog applies")

// Term is one selector read through one producer's window.
type Term struct {
	// Metric is [vmmodel.MetricEdge] or [vmmodel.MetricEntity].
	Metric string
	Layer  catalog.Layer
	// Type is the entity type label of an entity term. It is empty for an edge.
	Type catalog.EntityType
	// Label is the label the values are matched on: src or dst for an edge, fp for an
	// entity.
	Label string
	// Values are the values of Label, sorted and without repeats. One is an equality
	// match; several are a regular expression of the alternatives, escaped.
	Values   []string
	Producer lifecycle.Producer
	Window   time.Duration
}

// String is the term as MetricsQL: last_over_time of the selector over the window.
func (t Term) String() string {
	var m []string
	m = append(m, "layer="+quote(t.Layer.String()))
	if t.Type != "" {
		m = append(m, "type="+quote(string(t.Type)))
	}
	if len(t.Values) == 1 {
		m = append(m, t.Label+"="+quote(t.Values[0]))
	} else {
		alts := make([]string, len(t.Values))
		for i, v := range t.Values {
			alts[i] = regexp.QuoteMeta(v)
		}
		m = append(m, t.Label+"=~"+quote(strings.Join(alts, "|")))
	}
	m = append(m, "producer="+quote(string(t.Producer)))
	return "last_over_time(" + t.Metric + "{" + strings.Join(m, ",") + "}[" + durationString(t.Window) + "])"
}

// durationString writes a window in whole seconds when it is, otherwise in milliseconds.
func durationString(d time.Duration) string {
	if d%time.Second == 0 {
		return strconv.FormatInt(int64(d/time.Second), 10) + "s"
	}
	return strconv.FormatInt(d.Milliseconds(), 10) + "ms"
}

// quote writes s as a double-quoted string literal. Backslash and double quote are
// escaped, and so are the control characters, so no value can end the literal or the
// selector.
func quote(s string) string {
	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\' || c == '"':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c == '\n':
			b.WriteString(`\n`)
		case c == '\r':
			b.WriteString(`\r`)
		case c == '\t':
			b.WriteString(`\t`)
		case c < 0x20 || c == 0x7f:
			fmt.Fprintf(&b, `\x%02x`, c)
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')
	return b.String()
}

// Expr is the union of terms, one for each producer.
type Expr []Term

// String joins the terms with or.
func (e Expr) String() string {
	parts := make([]string, len(e))
	for i, t := range e {
		parts[i] = t.String()
	}
	return strings.Join(parts, " or ")
}

// QueryPath is the instant-query endpoint.
const QueryPath = "/api/v1/query"

// Request is an instant query: the expression, evaluated at a time.
type Request struct {
	Query string
	At    time.Time
}

// Form returns the parameters of the request: the query, the time as Unix seconds to the
// millisecond (assumption (d)), and nocache=1 (assumption (f): accepted, its effect on instant queries not observable). The result cache would otherwise answer a
// repeated ask from memory, which no engine it is compared with does.
func (r Request) Form() url.Values {
	ms := r.At.UnixMilli()
	v := url.Values{}
	v.Set("query", r.Query)
	v.Set("time", fmt.Sprintf("%d.%03d", ms/1000, ms%1000))
	v.Set("nocache", "1")
	return v
}

// Plan is a read rewritten as an instant query, with what is needed to read the answer.
type Plan struct {
	Op    runner.Op
	Dir   engine.Direction
	Layer catalog.Layer
	// Fps are the entities asked about, as asked: in order and with repeats.
	Fps  []identity.Fingerprint
	Expr Expr
	At   time.Time
}

// Request is the instant query of the plan.
func (p Plan) Request() Request { return Request{Query: p.Expr.String(), At: p.At} }

func refuse(q runner.Query, capability, why string) error {
	return &NotExpressibleError{Capability: capability, Question: q.Name(), Why: why}
}

// Rewrite writes a read as a query for the store, or refuses it:
//
//   - a window (every stored record in [from, to)) has no query: the store does not keep
//     records, only the last state of what it samples;
//   - a read as of a snapshot token other than the latest has no query: it has no time of
//     ingestion to read as of;
//
// Otherwise, for each producer of the registry that can hold what is asked about (it
// holds a relation of the catalog that touches the entity in the layer and direction, or
// the entity itself), it writes one term with that producer's window. A producer in watch
// mode is one of them: it is re-exported at its scrape interval and read through its
// window like any other.
func Rewrite(reg *vmmodel.Registry, q runner.Query) (Plan, error) {
	if q.Op == runner.OpWindow {
		return Plan{}, refuse(q, vmmodel.CapWindow, "a store of samples keeps no record of what was written, only the last sample in a window")
	}
	if q.AsOf != engine.Latest {
		return Plan{}, refuse(q, vmmodel.CapAsKnownAt, "the store has no time of ingestion to read as of")
	}
	if len(q.Fps) == 0 {
		return Plan{}, fmt.Errorf("vmquery: %s asks about no entity", q.Name())
	}
	if err := (engine.Scope{Layer: q.Layer}).Validate(); err != nil {
		return Plan{}, err
	}
	p := Plan{Op: q.Op, Dir: q.Dir, Layer: q.Layer, Fps: slices.Clone(q.Fps), At: q.At}
	switch q.Op {
	case runner.OpNeighbors, runner.OpBatch:
		label := vmmodel.LabelDst
		switch q.Dir {
		case engine.Reverse:
		case engine.Forward:
			label = vmmodel.LabelSrc
		default:
			return Plan{}, fmt.Errorf("vmquery: %s has direction %s", q.Name(), q.Dir)
		}
		if q.Op == runner.OpNeighbors && len(q.Fps) != 1 {
			return Plan{}, fmt.Errorf("vmquery: %s asks about %d entities", q.Name(), len(q.Fps))
		}
		values := make([]string, 0, len(q.Fps))
		holders := map[lifecycle.Producer]time.Duration{}
		related := false
		for _, fp := range q.Fps {
			values = append(values, fp.String())
			for _, r := range relationsTouching(fp.Type(), q.Layer, q.Dir) {
				related = true
				for _, h := range reg.EdgeHolders(r) {
					holders[h.Producer] = h.Window
				}
			}
		}
		if !related {
			return Plan{}, fmt.Errorf("%w: %s in layer %s, direction %s", ErrNoRelation, q.Fps[0].Type(), q.Layer, q.Dir)
		}
		if len(holders) == 0 {
			return Plan{}, fmt.Errorf("%w: no producer of the registry holds a relation of %s in layer %s, direction %s", ErrNoRelation, q.Fps[0].Type(), q.Layer, q.Dir)
		}
		values = sortedUnique(values)
		for _, h := range sortedHolders(holders) {
			p.Expr = append(p.Expr, Term{
				Metric: vmmodel.MetricEdge, Layer: q.Layer, Label: label, Values: values,
				Producer: h.Producer, Window: h.Window,
			})
		}
	case runner.OpAlive:
		if len(q.Fps) != 1 {
			return Plan{}, fmt.Errorf("vmquery: %s asks about %d entities", q.Name(), len(q.Fps))
		}
		fp := q.Fps[0]
		holders := reg.EntityHolders(fp.Type())
		if len(holders) == 0 {
			return Plan{}, fmt.Errorf("%w: no producer of the registry holds %s", ErrNoRelation, fp.Type())
		}
		for _, h := range holders {
			p.Expr = append(p.Expr, Term{
				Metric: vmmodel.MetricEntity, Layer: q.Layer, Type: fp.Type(), Label: vmmodel.LabelFP,
				Values: []string{vmmodel.FPLabel(fp)}, Producer: h.Producer, Window: h.Window,
			})
		}
	default:
		return Plan{}, fmt.Errorf("vmquery: unknown read %q", q.Op)
	}
	return p, nil
}

// relationsTouching lists the relations of the catalog stored in the layer that have the
// entity type at the end the direction reads from: as the target for a reverse read, as
// the source for a forward one.
func relationsTouching(t catalog.EntityType, l catalog.Layer, d engine.Direction) []catalog.RelationType {
	var out []catalog.RelationType
	for r := range catalog.Default().Relations() {
		if r.Derived() || r.Layer() != l {
			continue
		}
		for e := range r.Endpoints() {
			if (d == engine.Forward && e.From == t) || (d == engine.Reverse && e.To == t) {
				out = append(out, r.Type())
				break
			}
		}
	}
	slices.Sort(out)
	return out
}

func sortedUnique(s []string) []string {
	slices.Sort(s)
	return slices.Compact(s)
}

func sortedHolders(m map[lifecycle.Producer]time.Duration) []vmmodel.Holder {
	out := make([]vmmodel.Holder, 0, len(m))
	for p, w := range m {
		out = append(out, vmmodel.Holder{Producer: p, Window: w})
	}
	slices.SortFunc(out, func(a, b vmmodel.Holder) int { return strings.Compare(string(a.Producer), string(b.Producer)) })
	return out
}
