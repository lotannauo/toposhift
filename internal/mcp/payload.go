package mcp

import (
	"bytes"
	"encoding/json"
	"io"
	"slices"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// MCP answers go to whatever the client sends them to, so a description is
// printed only through this table of top-level keys per entity type, version
// PayloadTableVersion. It fails closed: a key that is not listed as allowed is
// withheld, and the answer names it. The listed keys are every key the
// translators write today (k8sobjects, k8scluster, nodeagent, svcgraph and
// spancount), classified as allowed or withheld; a test reads their golden files
// and fails on a key that is in neither list, so a new key has to be classified
// before it can be printed.
//
// What is withheld, and why:
//
//   - k8s.pod, from k8sobjects: host_ip and pod_ips are addresses.
//   - k8s.node, from k8sobjects: addresses, provider_id and node_info (its
//     boot_id) say what the node reports about itself and identify machines.
//     Only name, labels (three well-known keys) and conditions are allowed.
//   - host: cloud.resource_id identifies a cloud instance.
//   - topo.endpoint: every key. An endpoint is an address, and a writer gives it
//     no description. Its id reveals the address too, so an answer prints an
//     endpoint's id only where the request named it (withhold.go).
//   - every key not listed, and every other entity type.
//
// A service's name and namespace are allowed: they are its identity, and the name
// table reads them. A service instance's version is allowed, because the
// translators write it and it is neither an address nor a machine's identity; the
// instance's other keys are not listed, so they are withheld.
//
// The print rule of names.go applies to the value of every allowed key that holds
// text a sender chose: an object's name or namespace, and a service instance's
// version ([printRuleKeys]). A string value that is over 253 bytes, holds a
// control character, holds a format character (Unicode Cf: zero-width, bidi controls, tag characters)
// or holds an endpoint id's text is printed as null, the key is named in
// withheld_keys, and a fixed note says why. The rule reaches the top-level string
// values only, not the objects and lists inside a value.
//
// A container's keys, from either producer, and every key of k8scluster are
// allowed. The table applies to every description the server prints, in
// entity_history and in what_changed. Quarantine details (boot ids) are not
// descriptions and are printed as the store gives them.
var payloadTable = map[catalog.EntityType]struct{ allowed, withheld []string }{
	catalog.K8sPod: {
		allowed: []string{
			// k8sobjects.
			"name", "namespace", "node_name", "phase", "reason", "deletion_timestamp",
			"owner", "conditions", "containers",
			// k8scluster.
			"k8s.pod.name", "k8s.namespace.name", "k8s.node.name", "k8s.pod.phase",
			"k8s.pod.status_reason", "k8s.workload.kind", "k8s.workload.name",
			"k8s.deployment.name", "k8s.replicaset.name", "k8s.statefulset.name",
			"pod.creation_timestamp",
		},
		withheld: []string{"host_ip", "pod_ips"},
	},
	catalog.K8sNode: {
		allowed: []string{
			// k8sobjects.
			"name", "labels", "conditions",
			// k8scluster.
			"k8s.node.name", "node.creation_timestamp",
			"k8s.node.condition_ready", "k8s.node.condition_memory_pressure",
			"k8s.node.condition_disk_pressure", "k8s.node.condition_pid_pressure",
			"k8s.node.condition_network_unavailable",
		},
		withheld: []string{"addresses", "provider_id", "node_info"},
	},
	catalog.Host: {
		allowed: []string{
			"host.arch", "host.type", "os.type", "host.cpu.model.name",
			"cloud.provider", "cloud.platform", "cloud.region", "cloud.availability_zone",
		},
		withheld: []string{"cloud.resource_id"},
	},
	catalog.Container: {
		allowed: []string{
			// k8sobjects.
			"name", "pod_uid",
			// k8scluster.
			"k8s.container.name", "k8s.pod.name", "k8s.namespace.name", "k8s.node.name",
			"container.image.name", "container.image.tag", "container.status",
			"container.status.reason", "container.creation_timestamp",
		},
	},
	catalog.Service:         {allowed: []string{"name", "namespace"}},
	catalog.ServiceInstance: {allowed: []string{"service.version"}},
}

// printRuleKeys are the allowed keys whose value is text a sender chose: the name
// or the namespace of an object, and a service instance's version (the spancount
// translator accepts any valid UTF-8 of up to 256 bytes for it). They are the keys
// the print rule is applied to when a description is printed. A test fails on an
// allowed key that ends in name or namespace and is in neither this table nor
// [notNameKeys], and on a sender-text key that is not in this table.
var printRuleKeys = map[catalog.EntityType][]string{
	catalog.K8sPod: {
		"name", "namespace", "node_name",
		"k8s.pod.name", "k8s.namespace.name", "k8s.node.name", "k8s.workload.name",
		"k8s.deployment.name", "k8s.replicaset.name", "k8s.statefulset.name",
	},
	catalog.K8sNode:         {"name", "k8s.node.name"},
	catalog.Container:       {"name", "k8s.container.name", "k8s.pod.name", "k8s.namespace.name", "k8s.node.name"},
	catalog.Service:         {"name", "namespace"},
	catalog.ServiceInstance: {"service.version"},
}

// notNameKeys are allowed keys that end in name and are not the name of an
// object of the graph: an image's name, which a container's runtime reports, and
// the model name of a host's processor, which the node agent reports.
var notNameKeys = []string{"container.image.name", "host.cpu.model.name"}

// keyClass says how the table classifies a top-level key of a type.
type keyClass uint8

const (
	// keyUnlisted is a key the table does not list: withheld.
	keyUnlisted keyClass = iota
	// keyAllowed is printed.
	keyAllowed
	// keyWithheld is listed as withheld.
	keyWithheld
)

func payloadClass(typ catalog.EntityType, key string) keyClass {
	t, ok := payloadTable[typ]
	switch {
	case !ok:
		return keyUnlisted
	case slices.Contains(t.allowed, key):
		return keyAllowed
	case slices.Contains(t.withheld, key):
		return keyWithheld
	}
	return keyUnlisted
}

// Described is a description printed through the table: the winning producer's
// payload, less the keys the table withholds, which are named. A name or a
// namespace the print rule withholds stays in the payload as null and is named
// among the withheld keys too. A description is derived: it is merged by rank at
// an instant, and never stored as a fact.
type Described struct {
	Producer     string         `json:"producer"`
	Payload      map[string]any `json:"payload"`
	WithheldKeys []string       `json:"withheld_keys,omitempty"`
	Note         string         `json:"note,omitempty"`
}

const (
	notAnObject = "The payload is not a JSON object, so all of it is withheld."
	// valueWithheld is the note of a payload in which the print rule withheld a
	// value. It holds none of the value.
	valueWithheld = "A name, namespace or version value is null in the payload and its key is named in withheld_keys: the value is over 253 bytes, or it holds a control character, a format character (Unicode Cf: zero-width, bidi controls, tag characters) or an endpoint id's text."
)

// describe prints a payload of an entity of type typ through the table.
func describe(typ catalog.EntityType, producer string, payload []byte) *Described {
	kept, withheld, note := filterPayload(typ, payload)
	return &Described{Producer: producer, Payload: kept, WithheldKeys: withheld, Note: note}
}

// filterPayload keeps the allowed top-level keys of a payload that is one JSON
// object, and names the others, sorted. A string value of a print-rule key
// ([printRuleKeys]) that the print rule refuses is kept as null and named too, with a
// note. A payload that is not one object is withheld whole, with a note, and
// printed as the empty object, never null: the output schema says object. An empty
// payload is the empty object.
func filterPayload(typ catalog.EntityType, payload []byte) (kept map[string]any, withheld []string, note string) {
	obj, ok := payloadObject(payload)
	if !ok {
		return map[string]any{}, nil, notAnObject
	}
	kept = map[string]any{}
	for k, v := range obj {
		if payloadClass(typ, k) != keyAllowed {
			withheld = append(withheld, k)
			continue
		}
		if str, isString := v.(string); isString && slices.Contains(printRuleKeys[typ], k) && !printable(str) {
			kept[k] = nil
			withheld = append(withheld, k)
			note = valueWithheld
			continue
		}
		kept[k] = v
	}
	slices.Sort(withheld)
	return kept, withheld, note
}

// payloadObject decodes a payload that is one JSON object, numbers kept as their
// text. Anything else, invalid UTF-8, null and trailing data included, is not an
// object.
func payloadObject(p []byte) (map[string]any, bool) {
	if len(p) == 0 {
		return map[string]any{}, true
	}
	if !utf8.Valid(p) {
		return nil, false
	}
	if t := bytes.TrimLeft(p, " \t\r\n"); len(t) == 0 || t[0] != '{' {
		return nil, false
	}
	dec := json.NewDecoder(bytes.NewReader(p))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return m, true
}
