package mcphttp

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	core "github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// The tools offered are the core's, in its order, with its descriptions, every one
// read-only and idempotent and none open to the world.
func TestListsTheCoreTools(t *testing.T) {
	cs := connect(t, newServer(t, nil))
	lt, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := core.Tools()
	got := map[string]bool{}
	for _, tl := range lt.Tools {
		got[tl.Name] = true
	}
	if len(lt.Tools) != len(want) || len(got) != len(want) {
		t.Fatalf("%d tools offered, the core has %d", len(lt.Tools), len(want))
	}
	if len(want) != 9 {
		t.Errorf("the core has %d tools, want the nine of the design", len(want))
	}
	for _, w := range want {
		if !got[w.Name] {
			t.Errorf("the core's tool %s is not offered", w.Name)
		}
	}
	for _, tl := range lt.Tools {
		var coreTool core.Tool
		for _, w := range want {
			if w.Name == tl.Name {
				coreTool = w
			}
		}
		if tl.Description != coreTool.Description {
			t.Errorf("%s: description %q, the core says %q", tl.Name, tl.Description, coreTool.Description)
		}
		a := tl.Annotations
		if a == nil || !a.ReadOnlyHint || !a.IdempotentHint || a.OpenWorldHint == nil || *a.OpenWorldHint {
			t.Errorf("%s: annotations %+v, want read-only, idempotent and closed-world", tl.Name, a)
		}
		if tl.InputSchema == nil || tl.OutputSchema == nil {
			t.Errorf("%s: no input or output schema", tl.Name)
		}
	}
	// No prompts, no resources: the server says it has tools and nothing more.
	// The tools capability alone, and it promises no list-changed notification
	// (there is no session to send one to).
	caps, err := json.Marshal(cs.InitializeResult().Capabilities)
	if err != nil {
		t.Fatal(err)
	}
	if string(caps) != `{"tools":{}}` {
		t.Errorf("capabilities %s, want {\"tools\":{}}", caps)
	}
}

// Every tool's input schema offers exactly the keys of the core's input type, and
// requires the ones the type does not mark omitempty. resolve_entity's schema is
// written out, so this is what keeps it from drifting.
func TestInputSchemasMatchTheInputTypes(t *testing.T) {
	inputs := map[string]any{
		"blast_radius":    core.BlastRadiusInput{},
		"what_changed":    core.WhatChangedInput{},
		"co_residents":    core.CoResidentsInput{},
		"service_context": core.ServiceContextInput{},
		"neighbors_at":    core.NeighborsAtInput{},
		"entity_history":  core.EntityHistoryInput{},
		"find_entities":   core.FindEntitiesInput{},
		"resolve_entity":  core.ResolveEntityInput{},
		"describe_schema": core.DescribeSchemaInput{},
	}
	cs := connect(t, newServer(t, nil))
	lt, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tl := range lt.Tools {
		b, _ := json.Marshal(tl.InputSchema)
		var schema struct {
			Properties map[string]json.RawMessage `json:"properties"`
			Required   []string                   `json:"required"`
		}
		if err := json.Unmarshal(b, &schema); err != nil {
			t.Fatal(err)
		}
		var keys, required []string
		rt := reflect.TypeOf(inputs[tl.Name])
		for i := range rt.NumField() {
			tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")
			keys = append(keys, tag[0])
			if !slices.Contains(tag[1:], "omitempty") {
				required = append(required, tag[0])
			}
		}
		var props []string
		for k, raw := range schema.Properties {
			props = append(props, k)
			if tl.Name != "find_entities" && tl.Name != "service_context" {
				continue
			}
			var p struct {
				Description string `json:"description"`
			}
			if err := json.Unmarshal(raw, &p); err != nil || p.Description == "" {
				t.Errorf("%s: the property %q has no description (%v)", tl.Name, k, err)
			}
		}
		if tl.Name == "service_context" && slices.Contains(props, "at") {
			t.Error("service_context takes a window, never an instant, and its schema offers \"at\"")
		}
		slices.Sort(keys)
		slices.Sort(props)
		slices.Sort(required)
		slices.Sort(schema.Required)
		if !slices.Equal(keys, props) || !slices.Equal(required, schema.Required) {
			t.Errorf("%s: schema offers %v and requires %v; the type has %v and requires %v", tl.Name, props, schema.Required, keys, required)
		}
	}
}

// toolCall is one call of a tool.
type toolCall struct {
	label string
	name  string
	args  map[string]any
	// ranged says the tool reads a range, so a store without range reads refuses it.
	ranged bool
}

// callsOn calls every tool with every optional field the tool takes, on the
// fixture: a payload with withheld keys, a quarantine, null names, a deleted
// entity, a flapping edge, each display cut.
func callsOn(w *world) []toolCall {
	tok := tokenOf(w.st)
	id := func(fp identity.Fingerprint) string { return fp.String() }
	scope := []string{id(w.h1), id(w.n1), id(w.p1), id(w.p2), id(w.p3), id(w.p4), id(w.p6), id(w.h2)}
	return []toolCall{
		{"blast radius, every field", "blast_radius", map[string]any{"entity": id(w.h1), "from": w10, "to": w1030, "depth": 6, "asof": tok, "limit": 500}, true},
		{"blast radius at an instant", "blast_radius", map[string]any{"entity": id(w.n1), "at": "2026-01-01T10:08:00Z"}, true},
		{"blast radius of a quarantined host", "blast_radius", map[string]any{"entity": id(w.h2), "from": w10, "to": w1030}, true},
		{"blast radius cut by depth and limit", "blast_radius", map[string]any{"entity": id(w.h1), "from": w10, "to": w1030, "depth": 1, "limit": 1}, true},
		{"what changed, every field", "what_changed", map[string]any{"scope": scope, "from": w10, "to": w1030, "asof": tok, "limit": 500}, true},
		{"what changed, cut", "what_changed", map[string]any{"scope": scope, "from": w10, "to": w1030, "limit": 1}, true},
		{"co-residents, every field", "co_residents", map[string]any{"pod": id(w.p1), "from": w10, "to": w1030, "node_only": false, "asof": tok, "limit": 500}, true},
		{"co-residents, node only and cut", "co_residents", map[string]any{"pod": id(w.p1), "from": w10, "to": w1030, "node_only": true, "limit": 1}, true},
		{"co-residents on a quarantined host", "co_residents", map[string]any{"pod": id(w.p5), "from": w10, "to": w1030}, true},
		{"service context, every field", "service_context", map[string]any{"service": id(w.checkout), "from": w10, "to": w1030, "node_only": false, "asof": tok, "limit": 500}, true},
		{"service context, node only and cut", "service_context", map[string]any{"service": id(w.checkout), "from": w10, "to": w1030, "node_only": true, "limit": 1}, true},
		{"neighbors, every field", "neighbors_at", map[string]any{"entity": id(w.n1), "at": "2026-01-01T10:08:00Z", "direction": "both", "asof": tok}, false},
		{"neighbors forward", "neighbors_at", map[string]any{"entity": id(w.n3), "at": "2026-01-01T10:08:00Z", "direction": "forward"}, false},
		{"neighbors of a quarantined host", "neighbors_at", map[string]any{"entity": id(w.h2), "at": "2026-01-01T10:08:00Z"}, false},
		{"history of a pod, every field", "entity_history", map[string]any{"entity": id(w.p1), "from": "2026-01-01T09:00:00Z", "to": w1030, "asof": tok, "limit": 500}, false},
		{"history of a pod, cut", "entity_history", map[string]any{"entity": id(w.p1), "from": "2026-01-01T09:00:00Z", "to": w1030, "limit": 1}, false},
		{"history of a host", "entity_history", map[string]any{"entity": id(w.h1), "from": "2026-01-01T09:00:00Z", "to": w1030}, false},
		{"history of a quarantined host", "entity_history", map[string]any{"entity": id(w.h2), "from": "2026-01-01T09:00:00Z", "to": w1030}, false},
		{"find, every field", "find_entities", map[string]any{"type": "k8s.pod", "name": "WEB", "namespace": "shop", "from": w10, "to": w1030, "asof": tok, "after": numbered(catalog.K8sPod, 0).String(), "limit": 200}, true},
		{"find nodes at an instant", "find_entities", map[string]any{"type": "k8s.node", "at": "2026-01-01T10:08:00Z"}, true},
		{"find, cut", "find_entities", map[string]any{"type": "k8s.pod", "from": w10, "to": w1030, "limit": 1}, true},
		{"resolve, every field", "resolve_entity", map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}, "at": w10, "asof": tok}, false},
		{"resolve a pod at an instant", "resolve_entity", map[string]any{"type": "k8s.pod", "attributes": map[string]any{"k8s.pod.uid": "pod-uid-1"}, "at": w10}, false},
		{"resolve with no instant", "resolve_entity", map[string]any{"type": "host", "attributes": map[string]any{"host.id": "host-two"}}, false},
		{"describe the schema", "describe_schema", map[string]any{}, false},
	}
}

// nilCollisionStore reports every quarantine without detail, as the contract of the
// store allows.
type nilCollisionStore struct{ store.Store }

// ListEntities keeps the listing read of the store underneath, so that the tool
// that needs it answers.
func (n nilCollisionStore) ListEntities(ctx context.Context, typ catalog.EntityType, after identity.Fingerprint, max int) ([]identity.Fingerprint, error) {
	return n.Store.(core.Lister).ListEntities(ctx, typ, after, max)
}

func (n nilCollisionStore) Alive(ctx context.Context, fp identity.Fingerprint, t time.Time, sc store.Scope) (bool, error) {
	alive, err := n.Store.Alive(ctx, fp, t, sc)
	var qe *store.QuarantineError
	if errors.As(err, &qe) {
		return false, &store.QuarantineError{Entity: qe.Entity, Layer: qe.Layer}
	}
	return alive, err
}

// pointReadsOnly hides the in-memory store behind the interface, so that the
// source does not run range tools on it, as on any store [core.RangeReadsFor]
// does not allow.
type pointReadsOnly struct{ store.Store }

// An answer that does not fit its output schema is a protocol error, not a tool
// error. Every tool is called with every optional field, and none fails.
func TestEveryToolAnswersAgainstItsSchema(t *testing.T) {
	for _, tc := range []struct {
		name string
		wrap func(store.Store) store.Store
	}{
		{"a quarantine with its collision", func(st store.Store) store.Store { return st }},
		{"a quarantine with no collision", func(st store.Store) store.Store { return nilCollisionStore{st} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := newWorld(t, rankObjs)
			srv := newServer(t, rankObjs)
			if err := srv.attach(tc.wrap(w.st), true); err != nil {
				t.Fatal(err)
			}
			cs := connect(t, srv)
			var all strings.Builder
			for _, c := range callsOn(w) {
				m := answer(t, c.label, callTool(t, cs, c.name, c.args))
				b, _ := json.Marshal(m)
				all.Write(b)
				if _, ok := m["policy"]; !ok && c.name != "describe_schema" {
					t.Errorf("%s: no policy in %s", c.label, b)
				}
				if c.label == "find, cut" && (m["next_after"] == nil || m["complete"] != false) {
					t.Errorf("%s: next_after %v, complete %v; a cut answer says where to go on", c.label, m["next_after"], m["complete"])
				}
			}
			got := all.String()
			if tc.name == "a quarantine with no collision" {
				if !strings.Contains(got, `"collision":null`) || strings.Contains(got, `"stale_boot"`) {
					t.Error("no quarantine with a null collision was answered")
				}
			} else if !strings.Contains(got, `"stale_boot":"b1"`) {
				t.Error("no quarantine with its collision was answered")
			}
			for _, want := range []string{
				`"withheld_keys":["host_ip","pod_ips"]`, // a payload, filtered
				`"name":null`,                           // a host is unnamed on purpose
				`"name":"web-0"`,                        // a pod is named
				"Showing 1 of",                          // a display cut
				"The walk stopped at depth 1",           // a depth cut
			} {
				if !strings.Contains(got, want) {
					t.Errorf("no answer holds %q", want)
				}
			}
			for _, secret := range []string{hostSecret, podHostIP, podIP, nodeBootSecret, nodeProvider, hostResourceID} {
				if strings.Contains(got, secret) {
					t.Errorf("an answer holds %q, which no tool prints", secret)
				}
			}
		})
	}
}

// On a store with no range reads the point tools answer, with their range parts
// null, and the range tools refuse in a sentence, as a tool error.
func TestStoreWithoutRangeReads(t *testing.T) {
	w := newWorld(t, rankObjs)
	cs := connect(t, attached(t, pointReadsOnly{w.st}, rankObjs))
	const refusal = "Range answers are not available on this store."
	for _, c := range callsOn(w) {
		res := callTool(t, cs, c.name, c.args)
		if c.ranged {
			if !res.IsError || text(t, res) != refusal {
				t.Errorf("%s: %v %q, want the refusal", c.label, res.IsError, text(t, res))
			}
			continue
		}
		m := answer(t, c.label, res)
		if c.name == "entity_history" && m["existence"] != nil {
			t.Errorf("%s: existence %v, want null without range reads", c.label, m["existence"])
		}
		if c.name == "describe_schema" && m["range_reads"] != false {
			t.Errorf("describe_schema says range_reads %v", m["range_reads"])
		}
	}
}

// The ranks given to the source are in every answer that carries names or
// descriptions, as a digest, and other ranks give another digest.
func TestRankDigestInPolicy(t *testing.T) {
	digest := func(rank map[lifecycle.Producer]int) string {
		w := newWorld(t, rank)
		cs := connect(t, attached(t, w.st, rank))
		for _, c := range callsOn(w) {
			if c.label != "history of a pod, every field" {
				continue
			}
			m := answer(t, c.label, callTool(t, cs, c.name, c.args))
			p := m["policy"].(map[string]any)
			d, _ := p["rank"].(string)
			if d == "" || p["names"] != float64(core.NameTableVersion) || p["payload"] != float64(core.PayloadTableVersion) {
				t.Fatalf("policy %v", p)
			}
			return d
		}
		t.Fatal("no call")
		return ""
	}
	a, b := digest(rankObjs), digest(rankClus)
	if a == b {
		t.Errorf("the digest %q is the same for other ranks", a)
	}
	if a != digest(rankObjs) {
		t.Error("the digest differs between runs with the same ranks")
	}
}

// numbered is the fingerprint of the n-th entity of a type.
func numbered(typ catalog.EntityType, n int) identity.Fingerprint {
	var h [identity.FingerprintBytes]byte
	binary.BigEndian.PutUint32(h[:4], uint32(n))
	fp, err := identity.FingerprintFromHash(typ, h)
	if err != nil {
		panic(err)
	}
	return fp
}

// A walk that hits its own caps, and an answer that hits the hard cap of the
// display, still fit the output schema, and say what was left out.
func TestCappedAnswersFitTheSchema(t *testing.T) {
	st := openMem(t, memstore.Options{Policy: lifecycle.Policy{Rank: rankObjs}})
	host, node := numbered(catalog.Host, 1), numbered(catalog.K8sNode, 1)
	recs := []store.Record{
		observe(ent(host), agent, hm(9, 0), 0, `{}`),
		observe(ent(node), objs, hm(9, 0), 0, `{"name":"big-node"}`),
		observe(edge(node, host, catalog.RunsOn), agent, hm(9, 0), 0, `{}`),
	}
	for i := range 4100 {
		p := numbered(catalog.K8sPod, i+1)
		recs = append(recs,
			observe(ent(p), objs, hm(9, 0), 0, fmt.Sprintf(`{"name":"pod-%04d"}`, i)),
			observe(edge(p, node, catalog.ScheduledOn), objs, hm(9, 0), 0, `{}`))
	}
	newFeed(t, st).write(recs...)
	cs := connect(t, attached(t, st, rankObjs))

	m := answer(t, "blast_radius", callTool(t, cs, "blast_radius", map[string]any{"entity": host.String(), "from": w10, "to": w1030, "limit": 500}))
	n := notes(m)
	for _, want := range []string{
		"The walk found",                 // the walk's member cap
		"were reached but not expanded",  // the walk's frontier cap
		"The answer was cut at 256 KiB",  // the hard cap
		"Showing 500 of",                 // the display cap
		"Pass asof=" + tokenOf(st) + " ", // how to read the same world again
	} {
		if !strings.Contains(n, want) {
			t.Errorf("no note %q in\n%s", want, n)
		}
	}
}
