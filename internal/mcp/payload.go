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
// translators write today (k8sobjects, k8scluster and nodeagent), classified as
// allowed or withheld; a test reads their golden files and fails on a key that
// is in neither list, so a new key has to be classified before it can be
// printed.
//
// What is withheld, and why:
//
//   - k8s.pod, from k8sobjects: host_ip and pod_ips are addresses.
//   - k8s.node, from k8sobjects: addresses, provider_id and node_info (its
//     boot_id) say what the node reports about itself and identify machines.
//     Only name, labels (three well-known keys) and conditions are allowed.
//   - host: cloud.resource_id identifies a cloud instance.
//   - every key not listed, and every other entity type.
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
}

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
// payload, less the keys the table withholds, which are named. A description is
// derived: it is merged by rank at an instant, and never stored as a fact.
type Described struct {
	Producer     string         `json:"producer"`
	Payload      map[string]any `json:"payload"`
	WithheldKeys []string       `json:"withheld_keys,omitempty"`
	Note         string         `json:"note,omitempty"`
}

const notAnObject = "The payload is not a JSON object, so all of it is withheld."

// describe prints a payload of an entity of type typ through the table.
func describe(typ catalog.EntityType, producer string, payload []byte) *Described {
	kept, withheld, note := filterPayload(typ, payload)
	return &Described{Producer: producer, Payload: kept, WithheldKeys: withheld, Note: note}
}

// filterPayload keeps the allowed top-level keys of a payload that is one JSON
// object, and names the others, sorted. A payload that is not one object is
// withheld whole, with a note. An empty payload is the empty object.
func filterPayload(typ catalog.EntityType, payload []byte) (kept map[string]any, withheld []string, note string) {
	obj, ok := payloadObject(payload)
	if !ok {
		return nil, nil, notAnObject
	}
	kept = map[string]any{}
	for k, v := range obj {
		if payloadClass(typ, k) == keyAllowed {
			kept[k] = v
			continue
		}
		withheld = append(withheld, k)
	}
	slices.Sort(withheld)
	return kept, withheld, ""
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
