package otlphttp

import (
	"context"
	"fmt"
	"math"
	"net/http"

	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
)

// MetricsSink takes the metrics of one request, in the narrow model the metric
// translators read. It is called once per accepted request, on the goroutine of
// the request, and the request is answered when it returns. The error is mapped
// as [Sink]'s is, and the [Partial] as [PartialSink]'s is, with the data points
// rejected in place of log records.
type MetricsSink interface {
	AcceptMetrics(ctx context.Context, d otlpmetrics.Data) (Partial, error)
}

// MetricsHandler returns the handler of POST /v1/metrics. It panics if s is nil.
// It is [Handler]'s request path with the differences the package documentation
// lists under "Metrics": the body is protobuf only, and it is converted to
// [otlpmetrics.Data].
func MetricsHandler(s MetricsSink, o Options) http.Handler {
	if s == nil {
		panic("otlphttp: nil MetricsSink")
	}
	h := newHandler(MetricsPath, "metrics", false, o)
	h.decode = func(k kind, body []byte) (func(context.Context) (Partial, error), error) {
		d, err := decodeMetrics(k, body)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context) (Partial, error) { return s.AcceptMetrics(ctx, d) }, nil
	}
	return h
}

// decodeMetrics decodes a protobuf body and converts it. Only protobuf reaches
// it: the handler refuses JSON before the body is read.
func decodeMetrics(k kind, body []byte) (otlpmetrics.Data, error) {
	if k != kindProto {
		return otlpmetrics.Data{}, fmt.Errorf("unknown encoding %d", k)
	}
	var msg metricspb.MetricsData
	opts := proto.UnmarshalOptions{DiscardUnknown: true, RecursionLimit: maxProtoDepth}
	if err := opts.Unmarshal(body, &msg); err != nil {
		return otlpmetrics.Data{}, err
	}
	d, err := convertMetrics(&msg)
	if err != nil {
		return otlpmetrics.Data{}, err
	}
	if err := checkMetricsDepth(d); err != nil {
		return otlpmetrics.Data{}, err
	}
	return d, nil
}

// convertMetrics turns a protobuf MetricsData into the narrow model: the points
// of the monotonic sums, and for the rest only how many there were. An unset
// value, and a point flagged as having no recorded value, are NaN, which the
// translators set aside; a value is never guessed.
func convertMetrics(d *metricspb.MetricsData) (otlpmetrics.Data, error) {
	var out otlpmetrics.Data
	for _, rm := range d.GetResourceMetrics() {
		attrs, err := convertAttributes(rm.GetResource().GetAttributes())
		if err != nil {
			return otlpmetrics.Data{}, fmt.Errorf("resource attributes: %w", err)
		}
		res := otlpmetrics.Resource{Attributes: attrs}
		for _, sm := range rm.GetScopeMetrics() {
			scope := sm.GetScope().GetName()
			for _, m := range sm.GetMetrics() {
				if err := convertMetric(&res, scope, m); err != nil {
					return otlpmetrics.Data{}, err
				}
			}
		}
		out.Resources = append(out.Resources, res)
	}
	return out, nil
}

// convertMetric adds the points of one metric to the resource.
func convertMetric(res *otlpmetrics.Resource, scope string, m *metricspb.Metric) error {
	newest := func(t uint64) { res.NewestTimeUnixNano = max(res.NewestTimeUnixNano, t) }
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		sum := data.Sum
		for _, p := range sum.GetDataPoints() {
			newest(p.GetTimeUnixNano())
		}
		if !sum.GetIsMonotonic() {
			res.DroppedNonMonotonic += len(sum.GetDataPoints())
			return nil
		}
		for _, p := range sum.GetDataPoints() {
			attrs, err := convertAttributes(p.GetAttributes())
			if err != nil {
				return fmt.Errorf("metric %s: data point attributes: %w", snippet(m.GetName()), err)
			}
			res.Points = append(res.Points, otlpmetrics.SumPoint{
				Scope:             scope,
				Metric:            m.GetName(),
				Temporality:       temporality(sum.GetAggregationTemporality()),
				Monotonic:         true,
				StartTimeUnixNano: p.GetStartTimeUnixNano(),
				TimeUnixNano:      p.GetTimeUnixNano(),
				Value:             numberValue(p),
				Attributes:        attrs,
			})
		}
	case *metricspb.Metric_Gauge:
		for _, p := range data.Gauge.GetDataPoints() {
			newest(p.GetTimeUnixNano())
		}
		res.DroppedNotSum += len(data.Gauge.GetDataPoints())
	case *metricspb.Metric_Histogram:
		for _, p := range data.Histogram.GetDataPoints() {
			newest(p.GetTimeUnixNano())
		}
		res.DroppedNotSum += len(data.Histogram.GetDataPoints())
	case *metricspb.Metric_ExponentialHistogram:
		for _, p := range data.ExponentialHistogram.GetDataPoints() {
			newest(p.GetTimeUnixNano())
		}
		res.DroppedNotSum += len(data.ExponentialHistogram.GetDataPoints())
	case *metricspb.Metric_Summary:
		for _, p := range data.Summary.GetDataPoints() {
			newest(p.GetTimeUnixNano())
		}
		res.DroppedNotSum += len(data.Summary.GetDataPoints())
	}
	return nil
}

// temporality converts OTLP's aggregation temporality; a value this build does
// not know is unspecified.
func temporality(t metricspb.AggregationTemporality) otlpmetrics.Temporality {
	switch t {
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA:
		return otlpmetrics.Delta
	case metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE:
		return otlpmetrics.Cumulative
	}
	return otlpmetrics.TemporalityUnspecified
}

// numberValue is the value of a number point as a float64, whether it was sent
// as an integer or as a double. A point with no value, or flagged as having no
// recorded value, is NaN.
func numberValue(p *metricspb.NumberDataPoint) float64 {
	if p.GetFlags()&uint32(metricspb.DataPointFlags_DATA_POINT_FLAGS_NO_RECORDED_VALUE_MASK) != 0 {
		return math.NaN()
	}
	switch v := p.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	}
	return math.NaN()
}

// checkMetricsDepth refuses a request with a value that nests deeper than
// maxValueDepth in the attributes of a resource or of a sum point.
func checkMetricsDepth(d otlpmetrics.Data) error {
	for _, r := range d.Resources {
		if err := checkAttributesDepth(r.Attributes); err != nil {
			return err
		}
		for _, p := range r.Points {
			if err := checkAttributesDepth(p.Attributes); err != nil {
				return err
			}
		}
	}
	return nil
}
