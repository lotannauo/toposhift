package itbench

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/bench/eval/factload"
)

// The columns of the traces export that are read.
const (
	colStart    = "Timestamp"
	colTraceID  = "TraceId"
	colSpanID   = "SpanId"
	colParentID = "ParentSpanId"
	colSpanKind = "SpanKind"
	colService  = "ServiceName"
	colResource = "ResourceAttributes"
)

// The resource attributes that are read. Nothing else of the resource is: not
// host.name, which names the collector's pod, and not the node name.
const (
	attrServiceName      = "service.name"
	attrServiceNamespace = "service.namespace"
	attrInstanceID       = "service.instance.id"
	attrPodUID           = "k8s.pod.uid"
)

func spanKind(s string) factload.SpanKind {
	switch strings.TrimPrefix(strings.ToUpper(s), "SPAN_KIND_") {
	case "CLIENT":
		return factload.KindClient
	case "SERVER":
		return factload.KindServer
	case "PRODUCER":
		return factload.KindProducer
	case "CONSUMER":
		return factload.KindConsumer
	}
	return factload.KindInternal
}

// resource is what is read of a span's resource attributes.
type resource struct {
	service, namespace, instance, pod string
}

// ReadSpans reads a traces export, a row at a time. Only the identity of the
// service and of its instance are taken from the resource, and the pod uid the
// resource carries: never the pod's name or address, a host or a node.
func ReadSpans(path string) ([]factload.Span, error) {
	var out []factload.Span
	memo := map[string]resource{}
	err := rows(path, []string{colStart, colTraceID, colSpanID, colParentID, colSpanKind, colService, colResource}, func(r *row) error {
		raw := r.get(colResource)
		res, ok := memo[raw]
		if !ok {
			attrs, err := parseMap(raw)
			if err != nil {
				return err
			}
			res = resource{
				service: attrs[attrServiceName], namespace: attrs[attrServiceNamespace],
				instance: attrs[attrInstanceID], pod: attrs[attrPodUID],
			}
			memo[raw] = res
		}
		if res.service == "" {
			res.service = r.get(colService)
		}
		start, err := parseExportTime(r.get(colStart))
		if err != nil {
			return err
		}
		out = append(out, factload.Span{
			Start: start, TraceID: r.get(colTraceID), SpanID: r.get(colSpanID), ParentID: r.get(colParentID),
			Kind: spanKind(r.get(colSpanKind)), Service: res.service, Namespace: res.namespace,
			InstanceID: res.instance, PodUID: res.pod,
		})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("itbench: spans: %w", err)
	}
	return out, nil
}

// ReadServiceNames lists the service names that have a span in a traces export, from
// the resource's service.name or, where it has none, the ServiceName column.
func ReadServiceNames(path string) (map[string]bool, error) {
	out := map[string]bool{}
	memo := map[string]string{}
	err := rows(path, []string{colService, colResource}, func(r *row) error {
		raw := r.get(colResource)
		name, ok := memo[raw]
		if !ok {
			attrs, err := parseMap(raw)
			if err != nil {
				return err
			}
			name = attrs[attrServiceName]
			memo[raw] = name
		}
		if name == "" {
			name = r.get(colService)
		}
		if name != "" {
			out[name] = true
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("itbench: service names: %w", err)
	}
	return out, nil
}

// ProbeDir is the folder of a dataset directory that holds, for each scenario a
// probe read, the names of the services seen in it.
const ProbeDir = "probe"

var probeName = regexp.MustCompile(`^Scenario-[0-9]+\.services$`)

// ReadProbeNames reads a probe's names file: one service.name per line, sorted and
// without repeats, and nothing else. A file in any other shape is an error.
func ReadProbeNames(path string) ([]string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 {
		return nil, nil
	}
	text := string(b)
	if !strings.HasSuffix(text, "\n") {
		return nil, fmt.Errorf("itbench: %s: the last line has no line break", filepath.Base(path))
	}
	names := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	for i, n := range names {
		if n == "" || strings.TrimSpace(n) != n {
			return nil, fmt.Errorf("itbench: %s: line %d is empty or has white space at its ends", filepath.Base(path), i+1)
		}
		if i > 0 && names[i-1] >= n {
			return nil, fmt.Errorf("itbench: %s: line %d is not after the one before it (the names are sorted and unique)", filepath.Base(path), i+1)
		}
	}
	return names, nil
}

// PooledServices is the set of service names that have a span in the trace data
// read for the evaluation: every traces export under dir, and every probe names file
// in dir/probe (the names seen in the ranges of a probe of a scenario whose export
// was not fetched). A service outside it is uninstrumented. The set is a union, so it
// only grows as data is read; it is frozen by its hash before any case is scored.
func PooledServices(dir string) (map[string]bool, error) {
	scs, err := Scenarios(dir)
	if err != nil {
		return nil, err
	}
	out := map[string]bool{}
	for _, s := range scs {
		path := filepath.Join(s, TracesFile)
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		names, err := ReadServiceNames(path)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", filepath.Base(s), err)
		}
		for n := range names {
			out[n] = true
		}
	}
	probes, err := os.ReadDir(filepath.Join(dir, ProbeDir))
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, e := range probes {
		if e.IsDir() || !probeName.MatchString(e.Name()) {
			continue
		}
		names, err := ReadProbeNames(filepath.Join(dir, ProbeDir, e.Name()))
		if err != nil {
			return nil, err
		}
		for _, n := range names {
			out[n] = true
		}
	}
	return out, nil
}

// PooledText is the set as its file: one name a line, sorted, unique. Its sha256
// is what freezes the set.
func PooledText(set map[string]bool) string {
	names := make([]string, 0, len(set))
	for n := range set {
		names = append(names, n)
	}
	slices.Sort(names)
	if len(names) == 0 {
		return ""
	}
	return strings.Join(names, "\n") + "\n"
}
