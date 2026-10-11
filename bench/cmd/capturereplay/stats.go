package main

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// What the capture holds, counted record by record whatever the translators make of it.

// kvlist is the key-value list of an object value.
func kvlist(v capture.AnyValue) []capture.KeyValue {
	if v.KVListValue == nil {
		return nil
	}
	return v.KVListValue.Values
}

// field is the value of a key in an object value.
func field(v capture.AnyValue, key string) (capture.AnyValue, bool) {
	for _, kv := range kvlist(v) {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return capture.AnyValue{}, false
}

// path follows keys down through nested objects.
func path(v capture.AnyValue, keys ...string) (capture.AnyValue, bool) {
	for _, k := range keys {
		var ok bool
		if v, ok = field(v, k); !ok {
			return capture.AnyValue{}, false
		}
	}
	return v, true
}

func stringOf(v capture.AnyValue, ok bool) string {
	if !ok || v.StringValue == nil {
		return ""
	}
	return *v.StringValue
}

// recordKind is what a log record of the capture is.
type recordKind struct {
	// Source is "watch", "pull" or "entity".
	Source string
	// Type is the watch type (ADDED, MODIFIED, DELETED) or the entity event name.
	Type string
	// Kind is the object kind (Pod, Node, Event, ...) or the entity type.
	Kind string
	// UID is the object's metadata.uid, or the entity's id value.
	UID string
	// Object is the object of an objects record, for the fields below.
	Object capture.AnyValue
	HasObj bool
}

// resourceKinds maps the receiver's resource name to a kind, as the translator does.
var resourceKinds = map[string]string{"pods": "Pod", "nodes": "Node", "events": "Event"}

// classify says what a record is.
func classify(lr capture.LogRecord) recordKind {
	if dispatch(lr) == srcCluster {
		rk := recordKind{Source: "entity", Type: lr.EventName}
		for _, kv := range lr.Attributes {
			switch kv.Key {
			case "entity.type":
				rk.Kind = stringOf(kv.Value, true)
			case "entity.id":
				for _, idkv := range kvlist(kv.Value) {
					rk.UID = stringOf(idkv.Value, true)
				}
			}
		}
		return rk
	}
	rk := recordKind{Source: "pull"}
	if lr.Body == nil {
		return rk
	}
	body := *lr.Body
	obj := body
	if typ, ok := field(body, "type"); ok && typ.StringValue != nil {
		if o, ok := field(body, "object"); ok && o.KVListValue != nil {
			rk.Source, rk.Type, obj = "watch", *typ.StringValue, o
		}
	}
	rk.Object, rk.HasObj = obj, true
	rk.Kind = stringOf(field(obj, "kind"))
	if rk.Kind == "" {
		for _, kv := range lr.Attributes {
			if kv.Key == "k8s.resource.name" && kv.Value.StringValue != nil {
				if k, ok := resourceKinds[*kv.Value.StringValue]; ok {
					rk.Kind = k
				} else {
					rk.Kind = *kv.Value.StringValue
				}
			}
		}
	}
	rk.UID = stringOf(path(obj, "metadata", "uid"))
	return rk
}

// containerIDs are the runtime ids a pod object reports in its container statuses
// (containers, init containers and ephemeral containers), as written.
func containerIDs(obj capture.AnyValue) []string {
	var out []string
	for _, list := range []string{"containerStatuses", "initContainerStatuses", "ephemeralContainerStatuses"} {
		arr, ok := path(obj, "status", list)
		if !ok || arr.ArrayValue == nil {
			continue
		}
		for _, st := range arr.ArrayValue.Values {
			out = append(out, stringOf(field(st, "containerID")))
		}
	}
	return out
}

var (
	reHex64         = regexp.MustCompile(`^[0-9a-f]{64}$`)
	rePrefixedShape = regexp.MustCompile(`^([a-z0-9.+-]+)://([0-9a-f]{64})$`)
)

// idShape names the shape of a container id: "<runtime>://<64 hex>", a bare "<64 hex>",
// "empty", or "other".
func idShape(id string) string {
	switch m := rePrefixedShape.FindStringSubmatch(id); {
	case id == "":
		return "empty"
	case m != nil:
		return m[1] + "://<64 hex>"
	case reHex64.MatchString(id):
		return "<64 hex> without a runtime prefix"
	}
	return "other"
}

// bareID strips a "<runtime>://" prefix.
func bareID(id string) string {
	if _, rest, ok := strings.Cut(id, "://"); ok {
		return rest
	}
	return id
}

// pullLine is one line of the capture that holds pull records.
type pullLine struct {
	Line     int       `json:"line"`
	Pods     int       `json:"pods"`
	Nodes    int       `json:"nodes"`
	Records  int       `json:"records"`
	Bytes    int64     `json:"bytes"`
	Observed time.Time `json:"observed"`
}

// timeRange is the earliest and latest of some times.
type timeRange struct {
	Min time.Time `json:"min"`
	Max time.Time `json:"max"`
}

func (r *timeRange) add(t time.Time) {
	if r.Min.IsZero() || t.Before(r.Min) {
		r.Min = t
	}
	if t.After(r.Max) {
		r.Max = t
	}
}

// runStats is what one run file holds.
type runStats struct {
	N       int
	Lines   int
	Bytes   int64
	Records int
	// Moved is how many records capture.SortRefs would put elsewhere than the file does.
	MovedBySort int
	MaxShift    int
	SortErr     string

	BySource map[string]int
	// Watch counts watch records by type and kind ("MODIFIED Pod"); Entity entity events by
	// event name and entity type ("entity.state k8s.pod").
	Watch  map[string]int
	Entity map[string]int
	Pulls  []pullLine
	// Observed is the range of the observed times of the objects records, and EntityTime
	// that of the entity events' times. PodPulls is the range of the observed times of
	// the pod pull records.
	Observed   timeRange
	EntityTime timeRange
	PodPulls   timeRange
	// LargestPull is the pull line with the most bytes.
	LargestPull pullLine

	// NodeModified are the observed times of the watch MODIFIED records of each node UID,
	// and NodeState the times of the cluster receiver's state events for it.
	NodeModified map[string][]time.Time
	NodeState    map[string][]time.Time
	// Added is the observed time of the first watch ADDED record of each pod UID in the run.
	Added map[string]time.Time
	// ReportIntervals counts the entity.report.interval values of the state events.
	ReportIntervals map[string]int

	// UIDs seen by each translator's records, by source.
	PodUIDs  [numSources]map[string]struct{}
	NodeUIDs [numSources]map[string]struct{}
	// CtrIDs are the container ids each side reports, bare.
	CtrIDs [numSources]map[string]struct{}
	// IDShapes count the container ids as written by the pod objects (every status of
	// every record) and by the entity events (every container event).
	IDShapes [numSources]map[string]int
	// DistinctIDShapes count distinct ids by shape.
	DistinctIDShapes [numSources]map[string]map[string]struct{}
}

func newRunStats(rf *runFile) *runStats {
	rs := &runStats{
		N: rf.N, Lines: len(rf.Lines), Bytes: rf.Size, MovedBySort: rf.MovedBySort, MaxShift: rf.MaxShift,
		BySource: map[string]int{}, Watch: map[string]int{}, Entity: map[string]int{},
		NodeModified: map[string][]time.Time{}, NodeState: map[string][]time.Time{},
		ReportIntervals: map[string]int{}, Added: map[string]time.Time{},
	}
	if rf.SortErr != nil {
		rs.SortErr = rf.SortErr.Error()
	}
	for s := range numSources {
		rs.PodUIDs[s] = map[string]struct{}{}
		rs.NodeUIDs[s] = map[string]struct{}{}
		rs.CtrIDs[s] = map[string]struct{}{}
		rs.IDShapes[s] = map[string]int{}
		rs.DistinctIDShapes[s] = map[string]map[string]struct{}{}
	}
	return rs
}

func (rs *runStats) shape(s source, id string) {
	sh := idShape(id)
	rs.IDShapes[s][sh]++
	m := rs.DistinctIDShapes[s][sh]
	if m == nil {
		m = map[string]struct{}{}
		rs.DistinctIDShapes[s][sh] = m
	}
	m[id] = struct{}{}
}

// observe counts the records of one line.
func (rs *runStats) observe(b batch) {
	pl := pullLine{Line: b.Line + 1, Bytes: b.Bytes}
	for _, it := range b.Items {
		rs.Records++
		rk := classify(it.rec)
		rs.BySource[rk.Source]++
		switch rk.Source {
		case "entity":
			rs.Entity[rk.Type+" "+rk.Kind]++
			t := srcTime(srcCluster, it.rec)
			rs.EntityTime.add(t)
			switch rk.Kind {
			case "k8s.pod":
				rs.PodUIDs[srcCluster][rk.UID] = struct{}{}
			case "k8s.node":
				rs.NodeUIDs[srcCluster][rk.UID] = struct{}{}
				if rk.Type == "entity.state" {
					rs.NodeState[rk.UID] = append(rs.NodeState[rk.UID], t)
				}
			case "container":
				rs.CtrIDs[srcCluster][bareID(rk.UID)] = struct{}{}
				rs.shape(srcCluster, rk.UID)
			}
			for _, kv := range it.rec.Attributes {
				if kv.Key == "entity.report.interval" {
					switch {
					case kv.Value.IntValue != nil:
						rs.ReportIntervals[fmt.Sprint(int64(*kv.Value.IntValue))]++
					case kv.Value.StringValue != nil:
						rs.ReportIntervals[*kv.Value.StringValue]++
					default:
						rs.ReportIntervals["other"]++
					}
				}
			}
			continue
		case "watch":
			rs.Watch[rk.Type+" "+rk.Kind]++
		case "pull":
			pl.Records++
			switch rk.Kind {
			case "Pod":
				pl.Pods++
			case "Node":
				pl.Nodes++
			}
		}
		obs := srcTime(srcObjects, it.rec)
		rs.Observed.add(obs)
		if rk.Source == "pull" {
			pl.Observed = obs
			if rk.Kind == "Pod" {
				rs.PodPulls.add(obs)
			}
		}
		switch rk.Kind {
		case "Pod":
			rs.PodUIDs[srcObjects][rk.UID] = struct{}{}
			if _, seen := rs.Added[rk.UID]; !seen && rk.Source == "watch" && rk.Type == "ADDED" {
				rs.Added[rk.UID] = obs
			}
			if rk.HasObj {
				for _, id := range containerIDs(rk.Object) {
					rs.shape(srcObjects, id)
					if id != "" {
						rs.CtrIDs[srcObjects][bareID(id)] = struct{}{}
					}
				}
			}
		case "Node":
			rs.NodeUIDs[srcObjects][rk.UID] = struct{}{}
			if rk.Source == "watch" && rk.Type == "MODIFIED" {
				rs.NodeModified[rk.UID] = append(rs.NodeModified[rk.UID], obs)
			}
		}
	}
	if pl.Records > 0 {
		rs.Pulls = append(rs.Pulls, pl)
		if pl.Bytes > rs.LargestPull.Bytes {
			rs.LargestPull = pl
		}
	}
}
