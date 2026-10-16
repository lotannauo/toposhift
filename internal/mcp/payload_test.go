package mcp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// A pod's host_ip and pod_ips, a node's addresses, provider_id and node_info, and
// a host's cloud resource id and host.id never appear in any answer: the filter
// withholds them, and says which keys it withheld.
func TestSecretsNeverAppear(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	secrets := []string{podHostIP, podIP, nodeAddress, nodeProvider, nodeBootSecret, hostResourceID, hostSecret, "boot-h1"}
	for _, a := range allAnswers(t, w, s) {
		if a.echo {
			continue
		}
		for _, secret := range secrets {
			// The boot ids of a quarantine are shown as the store gives them, and the
			// plan asks for them; the quarantined host's boots are b1 and b2.
			if strings.Contains(string(a.json), secret) {
				t.Errorf("%s: %q is in the answer", a.name, secret)
			}
		}
	}

	history := func(fp identity.Fingerprint) EntityHistoryOutput {
		out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	for _, tc := range []struct {
		fp       identity.Fingerprint
		withheld []string
	}{
		{w.p1, []string{"host_ip", "pod_ips"}},
		{w.n1, []string{"addresses", "node_info", "provider_id"}},
		{w.h1, []string{"cloud.resource_id", "host.id"}},
	} {
		out := history(tc.fp)
		got := out.Records[0].WithheldKeys
		if !slices.Equal(got, tc.withheld) {
			t.Errorf("%s: withheld %v, want %v", tc.fp.Type(), got, tc.withheld)
		}
		d := out.Existence.DescriptionAtEnd
		if d == nil || !slices.Equal(d.WithheldKeys, tc.withheld) {
			t.Errorf("%s: the merged description withheld %v, want %v", tc.fp.Type(), d, tc.withheld)
		}
	}
}

// An unknown key is withheld and named, and so is every key of a type the table
// does not list. A payload that is not one JSON object is withheld whole.
func TestFilterFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name     string
		typ      catalog.EntityType
		payload  string
		kept     []string
		withheld []string
		note     bool
	}{
		{"an unknown key", catalog.K8sPod, `{"name":"a","mystery":"leak-me"}`, []string{"name"}, []string{"mystery"}, false},
		{"a known withheld key", catalog.K8sPod, `{"name":"a","host_ip":"1.2.3.4"}`, []string{"name"}, []string{"host_ip"}, false},
		{"the case of a key counts", catalog.K8sPod, `{"Name":"a","NAME":"b"}`, nil, []string{"NAME", "Name"}, false},
		{"a type not in the table", catalog.Zone, `{"zone":"eu-1a"}`, nil, []string{"zone"}, false},
		{"a type not in the table, even for a key another type allows", catalog.Service, `{"name":"x"}`, nil, []string{"name"}, false},
		{"the empty payload", catalog.K8sPod, ``, nil, nil, false},
		{"the empty object", catalog.K8sPod, `{}`, nil, nil, false},
		{"an array", catalog.K8sPod, `["name"]`, nil, nil, true},
		{"a string", catalog.K8sPod, `"name"`, nil, nil, true},
		{"null", catalog.K8sPod, `null`, nil, nil, true},
		{"trailing data", catalog.K8sPod, `{"name":"a"} {"x":1}`, nil, nil, true},
		{"invalid UTF-8", catalog.K8sPod, "{\"name\":\"\xff\"}", nil, nil, true},
		{"not JSON", catalog.K8sPod, `name=a`, nil, nil, true},
	} {
		kept, withheld, note := filterPayload(tc.typ, []byte(tc.payload))
		var keys []string
		for k := range kept {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		if !slices.Equal(keys, tc.kept) || !slices.Equal(withheld, tc.withheld) || (note != "") != tc.note {
			t.Errorf("%s: kept %v, withheld %v, note %q; want %v, %v, note %t", tc.name, keys, withheld, note, tc.kept, tc.withheld, tc.note)
		}
	}

	// In an answer: the unknown key is named, its value is not printed.
	w := newWorld(t, rankObjs)
	p8 := fpOf(t, catalog.K8sPod, catalog.K8sPodUID, "pod-uid-8")
	z := w.z1
	w.feed.write(
		observe(ent(p8), objs, hm(9, 0), 0, `{"name":"web-7","mystery":"leak-me-12345"}`),
		observe(ent(p8), clus, hm(9, 1), 0, `["not","an","object"]`),
	)
	s := w.source(t, rankObjs, true)
	for _, fp := range []identity.Fingerprint{p8, z} {
		out, err := s.EntityHistory(ctx, EntityHistoryInput{Entity: fp.String(), From: "2026-01-01T09:00:00Z", To: w1030})
		if err != nil {
			t.Fatal(err)
		}
		b, _ := json.Marshal(out)
		if strings.Contains(string(b), "leak-me-12345") || strings.Contains(string(b), "eu-1a") && fp == z {
			t.Errorf("%s: a withheld value is in the answer: %s", fp.Type(), b)
		}
		var withheld []string
		for _, r := range out.Records {
			withheld = append(withheld, r.WithheldKeys...)
			if fp == p8 && r.Producer == string(clus) && r.PayloadNote != notAnObject {
				t.Errorf("a payload that is an array: note %q", r.PayloadNote)
			}
		}
		want := "mystery"
		if fp == z {
			want = "zone"
		}
		if !slices.Contains(withheld, want) {
			t.Errorf("%s: %q is not named as withheld: %v", fp.Type(), want, withheld)
		}
	}
}

// A change of a withheld key in what_changed is named, and is in neither list of
// kept keys.
func TestChangedKeysThroughTheTable(t *testing.T) {
	w := newWorld(t, rankObjs)
	w.feed.write(
		observe(ent(w.p2), objs, hm(10, 2), 0, `{"host_ip":"10.9.9.9","mystery":"x","name":"web-1","namespace":"shop-2","phase":"Running","pod_ips":["10.9.9.10"]}`),
	)
	// p2 is deleted at 10:05; ask over a window before that.
	s := w.source(t, rankObjs, true)
	out, err := s.WhatChanged(ctx, WhatChangedInput{Scope: []string{w.p2.String()}, From: "2026-01-01T09:30:00Z", To: "2026-01-01T10:03:00Z"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Changed) != 1 || out.Changed[0].Change == nil {
		t.Fatalf("changed: %+v", out.Changed)
	}
	ch := out.Changed[0].Change
	if !slices.Equal(ch.StateKeys, []string{}) || !slices.Equal(ch.AttributeKeys, []string{"namespace"}) ||
		!slices.Equal(ch.WithheldKeys, []string{"host_ip", "mystery", "pod_ips"}) {
		t.Errorf("state %v, attribute %v, withheld %v", ch.StateKeys, ch.AttributeKeys, ch.WithheldKeys)
	}
	b, _ := json.Marshal(out)
	for _, leak := range []string{"10.9.9.9", "10.9.9.10"} {
		if strings.Contains(string(b), leak) {
			t.Errorf("%q is in the answer", leak)
		}
	}
}

// The table is O2's: pinned here so that a change to it is a change to this test.
func TestPayloadTableIsVersion1(t *testing.T) {
	if PayloadTableVersion != 1 || NameTableVersion != 1 {
		t.Fatalf("versions %d, %d: a new table needs this test updated on purpose", PayloadTableVersion, NameTableVersion)
	}
	for typ, tbl := range payloadTable {
		for _, k := range tbl.allowed {
			if slices.Contains(tbl.withheld, k) {
				t.Errorf("%s: %q is both allowed and withheld", typ, k)
			}
		}
	}
	allowed := func(typ catalog.EntityType, k string) bool { return payloadClass(typ, k) == keyAllowed }
	// k8sobjects pod: everything except host_ip and pod_ips.
	for _, k := range []string{"name", "namespace", "node_name", "phase", "reason", "deletion_timestamp", "owner", "conditions", "containers"} {
		if !allowed(catalog.K8sPod, k) {
			t.Errorf("pod key %q is not allowed", k)
		}
	}
	for _, k := range []string{"host_ip", "pod_ips"} {
		if payloadClass(catalog.K8sPod, k) != keyWithheld {
			t.Errorf("pod key %q is not withheld", k)
		}
	}
	// k8sobjects node: only name, labels and conditions.
	for _, k := range []string{"name", "labels", "conditions"} {
		if !allowed(catalog.K8sNode, k) {
			t.Errorf("node key %q is not allowed", k)
		}
	}
	for _, k := range []string{"addresses", "provider_id", "node_info"} {
		if payloadClass(catalog.K8sNode, k) != keyWithheld {
			t.Errorf("node key %q is not withheld", k)
		}
	}
	// host: everything but the cloud resource id.
	for _, k := range []string{"host.arch", "host.type", "os.type", "host.cpu.model.name", "cloud.provider", "cloud.platform", "cloud.region", "cloud.availability_zone"} {
		if !allowed(catalog.Host, k) {
			t.Errorf("host key %q is not allowed", k)
		}
	}
	if payloadClass(catalog.Host, "cloud.resource_id") != keyWithheld {
		t.Error("cloud.resource_id is not withheld")
	}
	// Anything else is unlisted, and so withheld.
	for _, k := range []string{"host.id", "host.name", "host.ip", "host.mac", "os.description"} {
		if payloadClass(catalog.Host, k) != keyUnlisted {
			t.Errorf("host key %q is not unlisted", k)
		}
	}
}

// The translators' golden files list every key they write. Every key of an
// entity there must be classified in the table, as allowed or withheld, so a new
// key has to be decided before it can be printed. The edges' payloads are never
// printed and are not checked.
func TestPayloadTableCoversTheTranslators(t *testing.T) {
	typeOf := map[string]catalog.EntityType{
		"pod": catalog.K8sPod, "node": catalog.K8sNode, "container": catalog.Container,
		"host": catalog.Host, "zone": catalog.Zone, "endpoint": catalog.NetworkEndpoint,
	}
	edges := []string{"scheduled_on", "runs_on", "located_in", "part_of", "has_address"}
	var files []string
	for _, pat := range []string{"k8sobjects", "k8scluster", "nodeagent"} {
		m, err := filepath.Glob(filepath.Join("..", "ingest", pat, "testdata", "*.golden.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(m) == 0 {
			t.Fatalf("no golden files of %s", pat)
		}
		files = append(files, m...)
	}
	keysSeen := 0
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		var doc any
		if err := json.Unmarshal(raw, &doc); err != nil {
			t.Fatalf("%s: %v", f, err)
		}
		walk(doc, "$", func(path string, obj map[string]any) {
			subject, ok := obj["subject"].(string)
			if !ok {
				return
			}
			if _, has := obj["kind"]; !has {
				return
			}
			word, _, _ := strings.Cut(subject, " ")
			if slices.Contains(edges, word) {
				return
			}
			typ, ok := typeOf[word]
			if !ok {
				t.Errorf("%s %s: the golden file has a subject %q this test does not know; classify its type", f, path, subject)
				return
			}
			payload, _ := obj["payload"].(map[string]any)
			for k := range payload {
				keysSeen++
				if payloadClass(typ, k) == keyUnlisted {
					t.Errorf("%s: the key %q of a %s is neither allowed nor withheld by the payload table", filepath.Base(f), k, typ)
				}
			}
		})
	}
	if keysSeen < 50 {
		t.Errorf("only %d keys read from %d golden files; the test lost its input", keysSeen, len(files))
	}
}
