package k8scluster

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The entity types the cluster receiver reports that this package knows.
const (
	typeNode        = "k8s.node"
	typePod         = "k8s.pod"
	typeContainer   = "container"
	typeNamespace   = "k8s.namespace"
	typeDeployment  = "k8s.deployment"
	typeReplicaSet  = "k8s.replicaset"
	typeStatefulSet = "k8s.statefulset"
	typeService     = "k8s.service"
)

// idKeys are the keys of the entity.id value that identify an entity of a type.
var idKeys = map[string]string{
	typeNode:      "k8s.node.uid",
	typePod:       "k8s.pod.uid",
	typeContainer: "container.id",
	typeNamespace: "k8s.namespace.uid",
}

// The description keys that are read for something other than the payload.
const (
	keyNodeName      = "k8s.node.name"
	keyNamespaceName = "k8s.namespace.name"
	keyPodUID        = "k8s.pod.uid"
	keyNodeCreated   = "node.creation_timestamp"
	keyPodCreated    = "pod.creation_timestamp"
	keyCtrCreated    = "container.creation_timestamp"
)

// allowed are the description keys that go into the payload of an entity type.
// Every other key is dropped: the labels can carry private data and have no
// bound on their number or size, and the owner uids are references to entities
// this package does not write.
var allowed = map[string]map[string]struct{}{
	typePod: setOf(
		"k8s.pod.name", "k8s.namespace.name", "k8s.node.name", "k8s.pod.phase",
		"k8s.pod.status_reason", "k8s.workload.kind", "k8s.workload.name",
		"k8s.deployment.name", "k8s.replicaset.name", "k8s.statefulset.name",
		keyPodCreated,
	),
	typeNode: setOf(
		"k8s.node.name", keyNodeCreated,
		"k8s.node.condition_ready", "k8s.node.condition_memory_pressure",
		"k8s.node.condition_disk_pressure", "k8s.node.condition_pid_pressure",
		"k8s.node.condition_network_unavailable",
	),
	typeContainer: setOf(
		"k8s.container.name", "k8s.pod.name", "k8s.namespace.name", "k8s.node.name",
		"container.image.name", "container.image.tag", "container.status",
		"container.status.reason", keyCtrCreated,
	),
}

// readOnly are the description keys that are read but kept out of the payload,
// and are not counted as dropped.
var readOnly = map[string]map[string]struct{}{
	typeContainer: setOf(keyPodUID),
}

func setOf(keys ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}

// entityEvent is a log record of the cluster receiver, read.
type entityEvent struct {
	state bool
	typ   string
	// id is the value of the type's identifying key; hasID says the key was there.
	id    string
	hasID bool
	desc  []capture.KeyValue
	iv    interval
}

// interval is the entity.report.interval attribute of a state event.
type interval struct {
	// present says the attribute is on the record; valid says it is a whole
	// number of seconds that can be used.
	present, valid bool
	d              time.Duration
}

// decode reads a log record. A record that is not an entity event of the
// current form gives the reason it is set aside.
func decode(lr capture.LogRecord) (entityEvent, string) {
	for _, kv := range lr.Attributes {
		if strings.HasPrefix(kv.Key, "otel.entity.") {
			return entityEvent{}, SkipLegacyEntityEvent
		}
	}
	var ev entityEvent
	switch lr.EventName {
	case "entity.state":
		ev.state = true
	case "entity.delete":
	default:
		return entityEvent{}, SkipNotEntityEvent
	}
	var ids []capture.KeyValue
	for _, kv := range lr.Attributes {
		switch kv.Key {
		case "entity.type":
			if kv.Value.StringValue != nil {
				ev.typ = *kv.Value.StringValue
			}
		case "entity.id":
			if kv.Value.KVListValue != nil {
				ids = kv.Value.KVListValue.Values
			}
		case "entity.description":
			if ev.state && kv.Value.KVListValue != nil {
				ev.desc = kv.Value.KVListValue.Values
			}
		case "entity.report.interval":
			if ev.state && !ev.iv.present {
				ev.iv = readInterval(kv.Value)
			}
		}
	}
	if key, ok := idKeys[ev.typ]; ok {
		for _, kv := range ids {
			if kv.Key == key && kv.Value.StringValue != nil {
				ev.id, ev.hasID = *kv.Value.StringValue, true
			}
		}
	}
	return ev, ""
}

// readInterval reads a report interval, which the collector writes in seconds.
// On the wire a 64-bit integer is a decimal string in an intValue, which the
// capture package has already read into IntValue. Anything that is not an
// integer value, a negative number, and a number too large to give a TTL are
// not usable.
func readInterval(v capture.AnyValue) interval {
	iv := interval{present: true}
	if v.IntValue == nil {
		return iv
	}
	n := int64(*v.IntValue)
	if n < 0 || n > int64(maxInterval/time.Second) {
		return iv
	}
	iv.d, iv.valid = time.Duration(n)*time.Second, true
	return iv
}

// stringOf returns the string value of a key, or "" if there is none.
func stringOf(kvs []capture.KeyValue, key string) string {
	var out string
	for _, kv := range kvs {
		if kv.Key == key && kv.Value.StringValue != nil {
			out = *kv.Value.StringValue
		}
	}
	return out
}

// described is the allow-listed part of a description.
type described struct {
	// payload is the canonical JSON of the allow-listed keys.
	payload []byte
	// dropped are the names of the keys left out, sorted and without repeats.
	dropped []string
}

// describe keeps the allow-listed string values of a description.
func describe(typ string, kvs []capture.KeyValue) described {
	keep := make(map[string]string)
	var dropped []string
	for _, kv := range kvs {
		if _, ok := allowed[typ][kv.Key]; ok && kv.Value.StringValue != nil {
			keep[kv.Key] = *kv.Value.StringValue
			continue
		}
		if _, ok := readOnly[typ][kv.Key]; ok {
			continue
		}
		dropped = append(dropped, kv.Key)
	}
	slices.Sort(dropped)
	return described{payload: canonical(keep), dropped: slices.Compact(dropped)}
}

// canonical encodes v as JSON with sorted keys and no insignificant white space.
func canonical(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Only maps of strings are encoded.
		panic("k8scluster: encoding a description: " + err.Error())
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}

// parseTime reads a Kubernetes timestamp (RFC 3339).
func parseTime(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t.UTC(), true
}

// runtimeID strips the "<runtime>://" prefix of a container id, if it has one.
// Stripping is the same on an id that has none.
func runtimeID(s string) string {
	if runtime, id, ok := strings.Cut(s, "://"); ok && runtime != "" {
		return id
	}
	return s
}
