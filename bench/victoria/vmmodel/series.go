package vmmodel

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// The metric names and label names of the series model.
const (
	MetricEntity = "topo_entity"
	MetricEdge   = "topo_edge"

	LabelName     = "__name__"
	LabelLayer    = "layer"
	LabelType     = "type"
	LabelFP       = "fp"
	LabelProducer = "producer"
	LabelRel      = "rel"
	LabelSrc      = "src"
	LabelDst      = "dst"
)

// ErrSeries marks a record that has no series.
var ErrSeries = errors.New("vmmodel: no series for the record")

// Label is one name and value of a series.
type Label struct{ Name, Value string }

// Series is the identity of one series: its labels, sorted by name, with the metric
// name as the label __name__. Identity is all it holds. A changed description of an
// object would be a new series in a real stack, and the spike's answers carry no
// description, so none is stored (see [Capabilities]).
type Series struct{ labels []Label }

// Labels returns the labels sorted by name. It returns a copy.
func (s Series) Labels() []Label { return slices.Clone(s.labels) }

// Get returns the value of a label, or "" if the series has none.
func (s Series) Get(name string) string {
	for _, l := range s.labels {
		if l.Name == name {
			return l.Value
		}
	}
	return ""
}

// Metric is the metric name: [MetricEntity] or [MetricEdge].
func (s Series) Metric() string { return s.Get(LabelName) }

// Key is a string that is equal for two series exactly when their labels are: every
// name and value with its length, so no value can be mistaken for the join of two.
func (s Series) Key() string {
	var b strings.Builder
	for _, l := range s.labels {
		fmt.Fprintf(&b, "%d:%s=%d:%s;", len(l.Name), l.Name, len(l.Value), l.Value)
	}
	return b.String()
}

// String is the series as a selector, for messages. It is not parsed anywhere.
func (s Series) String() string {
	var b strings.Builder
	b.WriteString(s.Metric())
	b.WriteByte('{')
	first := true
	for _, l := range s.labels {
		if l.Name == LabelName {
			continue
		}
		if !first {
			b.WriteByte(',')
		}
		first = false
		fmt.Fprintf(&b, "%s=%q", l.Name, l.Value)
	}
	b.WriteByte('}')
	return b.String()
}

// newSeries sorts the labels by name, which puts __name__ first.
func newSeries(labels ...Label) Series {
	slices.SortFunc(labels, func(a, b Label) int { return strings.Compare(a.Name, b.Name) })
	return Series{labels: labels}
}

// FPLabel is the value of the fp label of an entity: the 32 hex digits of its hash.
func FPLabel(fp identity.Fingerprint) string {
	h := fp.Hash()
	return hex.EncodeToString(h[:])
}

// EntitySeries is the series of one entity as held by one producer:
//
//	topo_entity{layer, type, fp, producer}
func EntitySeries(layer catalog.Layer, fp identity.Fingerprint, producer lifecycle.Producer) Series {
	return newSeries(
		Label{LabelName, MetricEntity},
		Label{LabelLayer, layer.String()},
		Label{LabelType, string(fp.Type())},
		Label{LabelFP, FPLabel(fp)},
		Label{LabelProducer, string(producer)},
	)
}

// EdgeSeries is the series of one directed edge as held by one producer:
//
//	topo_edge{layer, rel, src, dst, producer}
//
// with src and dst written type:hash, as a fingerprint is, so that a read of an
// entity's reverse neighbors is a label match on dst, as a read of kube_pod_info{node=...}
// is a match on its node label.
func EdgeSeries(layer catalog.Layer, from, to identity.Fingerprint, rel catalog.RelationType, producer lifecycle.Producer) Series {
	return newSeries(
		Label{LabelName, MetricEdge},
		Label{LabelLayer, layer.String()},
		Label{LabelRel, string(rel)},
		Label{LabelSrc, from.String()},
		Label{LabelDst, to.String()},
		Label{LabelProducer, string(producer)},
	)
}

// SeriesOf is the series a record is a sample of.
func SeriesOf(r engine.Record) (Series, error) {
	switch r.Subject.Kind {
	case engine.SubjectEntity:
		if r.Subject.A.IsZero() {
			return Series{}, fmt.Errorf("%w: seq %d is an entity with no fingerprint", ErrSeries, r.Seq)
		}
		return EntitySeries(r.Layer, r.Subject.A, r.Producer), nil
	case engine.SubjectEdge:
		if r.Subject.A.IsZero() || r.Subject.B.IsZero() || r.Subject.Relation == "" {
			return Series{}, fmt.Errorf("%w: seq %d is an edge without both ends and a relation", ErrSeries, r.Seq)
		}
		return EdgeSeries(r.Layer, r.Subject.A, r.Subject.B, r.Subject.Relation, r.Producer), nil
	}
	return Series{}, fmt.Errorf("%w: seq %d has subject kind %d", ErrSeries, r.Seq, r.Subject.Kind)
}
