package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// listParts is the files of the capture, in name order: the file itself, or the
// regular files of the directory whose names end in .jsonl.
func listParts(path string) ([]string, error) {
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !st.IsDir() {
		return []string{path}, nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	var parts []string
	for _, e := range entries {
		if e.Type().IsRegular() && strings.HasSuffix(e.Name(), ".jsonl") {
			parts = append(parts, filepath.Join(path, e.Name()))
		}
	}
	if len(parts) == 0 {
		return nil, fmt.Errorf("%s holds no .jsonl file", path)
	}
	slices.Sort(parts)
	return parts, nil
}

// digestParts is the SHA-256 of the parts joined, in the order given, and how many
// bytes that was.
func digestParts(parts []string) (sum string, size int64, err error) {
	h := sha256.New()
	for _, p := range parts {
		f, err := os.Open(p)
		if err != nil {
			return "", 0, err
		}
		n, err := io.Copy(h, f)
		err = errors.Join(err, f.Close())
		if err != nil {
			return "", 0, fmt.Errorf("%s: %w", p, err)
		}
		size += n
	}
	return hex.EncodeToString(h.Sum(nil)), size, nil
}

// metricsLine is one line of the capture as the receiver would be given it.
type metricsLine struct {
	// Part and Number say where it is: the file's base name and the line's number.
	Part   string
	Number int
	// Time is the newest time of any of its data points, the instant the collector
	// made the export; zero if it has none.
	Time time.Time
	// Body is the request body: the line as a protobuf MetricsData.
	Body []byte
}

// readLines calls visit with each line of the parts, in order: the JSON object of
// the line converted to protobuf, as an OTLP/HTTP client would send it. A line that
// is not metrics, or does not convert, is an error.
func readLines(parts []string, visit func(metricsLine) error) error {
	unmarshal := protojson.UnmarshalOptions{DiscardUnknown: true}
	for _, path := range parts {
		if err := readPart(path, unmarshal, visit); err != nil {
			return err
		}
	}
	return nil
}

func readPart(path string, unmarshal protojson.UnmarshalOptions, visit func(metricsLine) error) (err error) {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	r := capture.NewReader(f, capture.ReaderOptions{})
	base := filepath.Base(path)
	for {
		l, err := r.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("%s: %w", base, err)
		}
		if l.Signal != capture.Metrics {
			return fmt.Errorf("%s:%d: a %s line: the capture must hold metrics", base, l.Number, l.Signal)
		}
		var msg metricspb.MetricsData
		if err := unmarshal.Unmarshal(l.Raw, &msg); err != nil {
			return fmt.Errorf("%s:%d: %w", base, l.Number, err)
		}
		body, err := proto.Marshal(&msg)
		if err != nil {
			return fmt.Errorf("%s:%d: %w", base, l.Number, err)
		}
		if err := visit(metricsLine{Part: base, Number: l.Number, Time: newestTime(&msg), Body: body}); err != nil {
			return err
		}
	}
}

// newestTime is the latest time of any data point of the message, whatever the
// kind of metric, or the zero time if it has none.
func newestTime(m *metricspb.MetricsData) time.Time {
	var newest uint64
	note := func(t uint64) { newest = max(newest, t) }
	for _, rm := range m.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, metric := range sm.GetMetrics() {
				switch d := metric.GetData().(type) {
				case *metricspb.Metric_Sum:
					for _, p := range d.Sum.GetDataPoints() {
						note(p.GetTimeUnixNano())
					}
				case *metricspb.Metric_Gauge:
					for _, p := range d.Gauge.GetDataPoints() {
						note(p.GetTimeUnixNano())
					}
				case *metricspb.Metric_Histogram:
					for _, p := range d.Histogram.GetDataPoints() {
						note(p.GetTimeUnixNano())
					}
				case *metricspb.Metric_ExponentialHistogram:
					for _, p := range d.ExponentialHistogram.GetDataPoints() {
						note(p.GetTimeUnixNano())
					}
				case *metricspb.Metric_Summary:
					for _, p := range d.Summary.GetDataPoints() {
						note(p.GetTimeUnixNano())
					}
				}
			}
		}
	}
	if newest == 0 || newest > uint64(1<<62) {
		return time.Time{}
	}
	return time.Unix(0, int64(newest)).UTC()
}
