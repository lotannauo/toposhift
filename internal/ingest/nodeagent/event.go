package nodeagent

import (
	"bytes"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// The entity type a node collector reports.
const typeHost = "host"

// The resource keys that are read for something other than the payload.
const (
	keyBoot    = "topo.host.boot.id"
	keyCluster = "k8s.cluster.name"
)

// The cloud keys that make a zone id, in the order of its parts.
const (
	keyCloudProvider = "cloud.provider"
	keyCloudRegion   = "cloud.region"
	keyCloudZone     = "cloud.availability_zone"
)

// allowed are the resource keys that go into the payload of a host. Every other key
// is dropped. os.version and os.description are not here on purpose: the detector
// reads them from the collector's container, not from the host. host.name, the
// node's name and every address are not identity or description of the host.
var allowed = setOf(
	"host.arch", "host.type", "os.type", "host.cpu.model.name",
	keyCloudProvider, "cloud.platform", keyCloudRegion, keyCloudZone, "cloud.resource_id",
)

// readOnly are the resource keys that are read but kept out of the payload, and are
// not counted as dropped: the identities, the boot, and the cluster name the caller
// uses to choose the producer.
var readOnly = setOf("host.id", "k8s.node.uid", keyBoot, keyCluster)

func setOf(keys ...string) map[string]struct{} {
	out := make(map[string]struct{}, len(keys))
	for _, k := range keys {
		out[k] = struct{}{}
	}
	return out
}

// entityEvent is a log record of a node collector, read.
type entityEvent struct {
	iv interval
}

// interval is the entity.report.interval attribute of a state event.
type interval struct {
	// present says the attribute is on the record; valid says it is a whole
	// number of seconds that can be used.
	present, valid bool
	d              time.Duration
}

// decode reads a log record. A record that is not a host entity state of the
// current form gives the reason it is set aside.
func decode(lr capture.LogRecord) (entityEvent, string) {
	for _, kv := range lr.Attributes {
		if strings.HasPrefix(kv.Key, "otel.entity.") {
			return entityEvent{}, SkipLegacyEntityEvent
		}
	}
	del := false
	switch lr.EventName {
	case "entity.state":
	case "entity.delete":
		del = true
	default:
		return entityEvent{}, SkipNotEntityEvent
	}
	var ev entityEvent
	typ := ""
	for _, kv := range lr.Attributes {
		switch kv.Key {
		case "entity.type":
			if kv.Value.StringValue != nil {
				typ = *kv.Value.StringValue
			}
		case "entity.report.interval":
			if !ev.iv.present {
				ev.iv = readInterval(kv.Value)
			}
		}
	}
	switch {
	case typ != typeHost:
		return entityEvent{}, SkipNotHost
	case del:
		return entityEvent{}, SkipHostDelete
	}
	return ev, ""
}

// readInterval reads a report interval, which the collector writes in seconds. On
// the wire a 64-bit integer is a decimal string in an intValue, which the capture
// package has already read into IntValue. Anything that is not an integer value, a
// negative number, and a number too large to give a TTL are not usable.
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

// attribute is what a resource says for one key: its string value if it has one,
// and whether the key was given more than once with different values.
type attribute struct {
	text     *string
	conflict bool
}

// attributes are the resource's attributes by key.
type attributes map[string]attribute

// collect reads the attributes of a resource. A key given twice with different
// values, or with a string and a value of another kind, is in conflict: no value
// is chosen for it, because choosing one would be a guess.
func collect(kvs []capture.KeyValue) attributes {
	out := make(attributes, len(kvs))
	for _, kv := range kvs {
		a, seen := out[kv.Key]
		switch {
		case !seen:
			a = attribute{text: kv.Value.StringValue}
		case a.text != nil && kv.Value.StringValue != nil && *a.text == *kv.Value.StringValue:
			// The same value again: not a conflict.
		default:
			a.conflict = true
		}
		out[kv.Key] = a
	}
	return out
}

// text returns the string value of a key exactly as received, and whether there is
// one: the key must be there once, and be a string.
func (a attributes) text(key string) (string, bool) {
	v, ok := a[key]
	if !ok || v.text == nil || v.conflict {
		return "", false
	}
	return *v.text, true
}

// described is the allow-listed part of a resource.
type described struct {
	// payload is the canonical JSON of the allow-listed keys.
	payload []byte
	// dropped are the names of the keys left out, sorted and without repeats.
	dropped []string
}

// describe keeps the allow-listed, non-empty string values of a resource.
func describe(attrs attributes) described {
	keep := make(map[string]string)
	var dropped []string
	for k, a := range attrs {
		if _, ok := allowed[k]; ok {
			switch {
			case a.text == nil || a.conflict:
				dropped = append(dropped, k)
			case *a.text != "":
				keep[k] = *a.text
			}
			continue
		}
		if _, ok := readOnly[k]; !ok {
			dropped = append(dropped, k)
		}
	}
	slices.Sort(dropped)
	return described{payload: canonical(keep), dropped: dropped}
}

// canonical encodes v as JSON with sorted keys and no insignificant white space.
func canonical(v any) []byte {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		// Only maps of strings are encoded.
		panic("nodeagent: encoding a description: " + err.Error())
	}
	return bytes.TrimRight(b.Bytes(), "\n")
}
