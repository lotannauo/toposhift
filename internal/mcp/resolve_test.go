package mcp

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

func resolve(s *Source, typ, attrs string) (ResolveEntityOutput, error) {
	return s.ResolveEntity(ctx, ResolveEntityInput{Type: typ, Attributes: rawAttrs(attrs)})
}

// The id the tool gives equals the resolver's on the same attributes, for every
// kind of identifying key.
func TestResolveEqualsTheResolver(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	created := time.Date(2026, 1, 1, 8, 30, 15, 123456789, time.UTC)
	for _, tc := range []struct {
		name  string
		typ   catalog.EntityType
		attrs string
		want  []identity.Attr
	}{
		{"host", catalog.Host, `{"host.id":"h-1"}`, []identity.Attr{{Key: catalog.HostID, Value: "h-1"}}},
		{"pod", catalog.K8sPod, `{"k8s.pod.uid":"u-1"}`, []identity.Attr{{Key: catalog.K8sPodUID, Value: "u-1"}}},
		{"process", catalog.Process, `{"host.id":"h-1","process.pid":4242,"process.creation.time":"2026-01-01T08:30:15.123456789Z"}`, []identity.Attr{
			{Key: catalog.HostID, Value: "h-1"}, {Key: catalog.ProcessPID, Value: int64(4242)}, {Key: catalog.ProcessCreationTime, Value: created},
		}},
		{"process in another zone", catalog.Process, `{"host.id":"h-1","process.pid":4242,"process.creation.time":"2026-01-01T10:30:15.123456789+02:00"}`, []identity.Attr{
			{Key: catalog.HostID, Value: "h-1"}, {Key: catalog.ProcessPID, Value: int64(4242)}, {Key: catalog.ProcessCreationTime, Value: created},
		}},
		{"a negative pid", catalog.Process, `{"host.id":"h","process.pid":-5,"process.creation.time":"2026-01-01T08:30:15Z"}`, []identity.Attr{
			{Key: catalog.HostID, Value: "h"}, {Key: catalog.ProcessPID, Value: int64(-5)}, {Key: catalog.ProcessCreationTime, Value: created.Truncate(time.Second)},
		}},
		{"service without a namespace", catalog.Service, `{"service.name":"cart"}`, []identity.Attr{{Key: catalog.ServiceName, Value: "cart"}}},
		{"service with an empty namespace", catalog.Service, `{"service.name":"cart","service.namespace":""}`, []identity.Attr{{Key: catalog.ServiceName, Value: "cart"}}},
		{"service with a namespace", catalog.Service, `{"service.name":"cart","service.namespace":"shop"}`, []identity.Attr{
			{Key: catalog.ServiceName, Value: "cart"}, {Key: catalog.ServiceNamespace, Value: "shop"},
		}},
		{"instance", catalog.ServiceInstance, `{"service.name":"cart","service.instance.id":"i-1"}`, []identity.Attr{
			{Key: catalog.ServiceName, Value: "cart"}, {Key: catalog.ServiceInstanceID, Value: "i-1"},
		}},
	} {
		got, err := resolve(s, string(tc.typ), tc.attrs)
		if err != nil {
			t.Errorf("%s: %v", tc.name, err)
			continue
		}
		want, err := testResolver.Resolve(tc.typ, tc.want)
		if err != nil {
			t.Fatal(err)
		}
		if got.Entity.ID != want.Fingerprint().String() || got.Entity.Type != string(tc.typ) {
			t.Errorf("%s: %s, want %s", tc.name, got.Entity.ID, want.Fingerprint())
		}
		if got.Token != "" || got.Derived || got.Existence != nil {
			t.Errorf("%s: an answer without \"at\" read the store: %+v", tc.name, got)
		}
	}
}

// The tool is exact: case, white space, extra keys, empty values and a float for
// an integer are refused by name or give another id, and nothing is searched.
func TestResolveIsExact(t *testing.T) {
	w := newWorld(t, rankObjs)
	spy := &spyStore{Store: w.st}
	s, err := NewSource(Config{Store: spy, Rank: rankObjs, RangeReads: true})
	if err != nil {
		t.Fatal(err)
	}
	proc := func(pid string) string {
		return `{"host.id":"h","process.pid":` + pid + `,"process.creation.time":"2026-01-01T08:30:15Z"}`
	}
	for _, tc := range []struct {
		name  string
		typ   string
		attrs string
		want  string
	}{
		{"upper case key", "host", `{"Host.ID":"x"}`, `"Host.ID": not an identifying attribute`},
		{"the key with a space", "host", `{"host.id ":"x"}`, `not an identifying attribute`},
		{"an extra key", "host", `{"host.id":"x","host.name":"worker"}`, `"host.name": not an identifying attribute of this entity type`},
		{"a missing key", "host", `{}`, `"host.id": required attribute is missing`},
		{"no attributes at all", "host", `null`, `"host.id": required attribute is missing`},
		{"an empty value", "host", `{"host.id":""}`, `"host.id": value is empty or whitespace-only`},
		{"a blank value", "host", `{"host.id":"  \t"}`, `"host.id": value is empty or whitespace-only`},
		{"a number for a string", "host", `{"host.id":5}`, `"host.id" must be a JSON string`},
		{"null for a string", "host", `{"host.id":null}`, `"host.id" must be a JSON string`},
		{"an object for a string", "host", `{"host.id":{"a":1}}`, `"host.id" must be a JSON string`},
		{"a float for an integer", "process", proc("4242.0"), `"process.pid" must be a JSON integer`},
		{"an exponent for an integer", "process", proc("4.242e3"), `"process.pid" must be a JSON integer`},
		{"a string for an integer", "process", proc(`"4242"`), `"process.pid" must be a JSON integer`},
		{"minus zero", "process", proc("-0"), `"process.pid" must be a JSON integer`},
		{"too big an integer", "process", proc("9223372036854775808"), `"process.pid" does not fit in 64 bits`},
		{"a time without a zone", "process", `{"host.id":"h","process.pid":1,"process.creation.time":"2026-01-01T08:30:15"}`, `"process.creation.time"`},
		{"a number for a time", "process", `{"host.id":"h","process.pid":1,"process.creation.time":1767256215}`, `"process.creation.time" must be a JSON string`},
		{"two faults are both named", "process", `{"host.id":"","process.pid":1.5,"process.creation.time":"x","extra":1}`, `"process.pid" must be a JSON integer`},
		{"an unknown type", "pod", `{"k8s.pod.uid":"x"}`, `names the entity type "pod", which is not in the catalog`},
		{"not JSON", "host", `{"host.id":}`, ``},
	} {
		var out ResolveEntityOutput
		var err error
		if tc.name == "not JSON" {
			// A malformed value cannot be built by rawAttrs; give the tool one directly.
			out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: tc.typ, Attributes: map[string]json.RawMessage{"host.id": json.RawMessage(`{"x"`)}})
			tc.want = `The attribute "host.id" is not valid JSON.`
		} else if tc.attrs == "null" {
			out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: tc.typ})
		} else {
			out, err = resolve(s, tc.typ, tc.attrs)
		}
		var te *Error
		if !errors.As(err, &te) || !strings.Contains(te.Message, tc.want) {
			t.Errorf("%s: %v, want %q", tc.name, err, tc.want)
		}
		if err == nil {
			t.Errorf("%s: resolved to %s", tc.name, out.Entity.ID)
		}
	}
	// "Two faults are both named" means all of them.
	_, err = resolve(s, "process", `{"host.id":"","process.pid":1,"process.creation.time":"2026-01-01T08:30:15Z","extra":1}`)
	if err == nil || !strings.Contains(err.Error(), `"extra": not an identifying attribute`) || !strings.Contains(err.Error(), `"host.id": value is empty`) {
		t.Errorf("two faults: %v", err)
	}

	// White space inside a value is part of it: two values that differ in it are two ids.
	a, err1 := resolve(s, "host", `{"host.id":"abc"}`)
	b, err2 := resolve(s, "host", `{"host.id":" abc"}`)
	c, err3 := resolve(s, "host", `{"host.id":"ABC"}`)
	if err1 != nil || err2 != nil || err3 != nil {
		t.Fatal(err1, err2, err3)
	}
	if a.Entity.ID == b.Entity.ID || a.Entity.ID == c.Entity.ID || b.Entity.ID == c.Entity.ID {
		t.Errorf("abc, \" abc\" and ABC give %s %s %s", a.Entity.ID, b.Entity.ID, c.Entity.ID)
	}
	// An optional key given empty is the same as omitted; given a value, it is another id.
	n1, _ := resolve(s, "service", `{"service.name":"x"}`)
	n2, _ := resolve(s, "service", `{"service.name":"x","service.namespace":""}`)
	n3, _ := resolve(s, "service", `{"service.name":"x","service.namespace":"ns"}`)
	if n1.Entity.ID != n2.Entity.ID || n1.Entity.ID == n3.Entity.ID {
		t.Errorf("optional namespace: %s %s %s", n1.Entity.ID, n2.Entity.ID, n3.Entity.ID)
	}
	// An integer given as 4242 and a time in two zones are one id each.
	if spy.reads != 0 {
		t.Errorf("%d store reads without \"at\"", spy.reads)
	}
}

// With an instant, the tool says whether the entity exists, at the token it names.
// It echoes only the attributes it was given, and never searches for the entity.
func TestResolveWithAnInstant(t *testing.T) {
	w := newWorld(t, rankObjs)
	s := w.source(t, rankObjs, true)
	host := `{"host.id":"` + hostSecret + `"}`
	token := w.st.LastSeq()

	out, err := s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(host), At: w10})
	if err != nil {
		t.Fatal(err)
	}
	if out.Entity.FP != w.h1 || out.Existence.Status != "alive" || out.Token != "33" || !out.Derived ||
		out.Existence.Sentence != "alive at 2026-01-01T10:00:00Z at token 33" || len(out.Attributes) != 1 || out.Attributes["host.id"] != hostSecret {
		t.Errorf("%+v", out)
	}
	if token != 33 {
		t.Fatalf("the fixture has %d records", token)
	}

	// Not alive before it was seen, and not alive for an id nothing ever asserted.
	out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(host), At: "2026-01-01T08:00:00Z"})
	if err != nil || out.Existence.Status != "not_alive" || out.Existence.Sentence != "not alive at 2026-01-01T08:00:00Z at token 33" {
		t.Errorf("before it was seen: %v %+v", err, out.Existence)
	}
	out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(`{"host.id":"never-seen"}`), At: w10})
	if err != nil || out.Existence.Status != "not_alive" {
		t.Errorf("never seen: %v %+v", err, out.Existence)
	}
	// A pod deleted at 10:05.
	for at, want := range map[string]string{"2026-01-01T10:04:00Z": "alive", "2026-01-01T10:05:00Z": "not_alive"} {
		out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "k8s.pod", Attributes: rawAttrs(`{"k8s.pod.uid":"pod-uid-2"}`), At: at})
		if err != nil || out.Existence.Status != want || out.Entity.FP != w.p2 {
			t.Errorf("pod 2 at %s: %v %+v", at, err, out.Existence)
		}
	}
	// A token before the host was written.
	out, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(host), At: w10, AsOf: "0"})
	if err != nil || out.Existence.Status != "not_alive" || out.Token != "0" {
		t.Errorf("token 0: %v %+v", err, out)
	}
	// asof means nothing without an instant.
	_, err = s.ResolveEntity(ctx, ResolveEntityInput{Type: "host", Attributes: rawAttrs(host), AsOf: "5"})
	var te *Error
	if !errors.As(err, &te) || !errors.Is(err, store.ErrInvalid) || !strings.Contains(te.Message, `"asof" only applies with "at"`) {
		t.Errorf("asof without at: %v", err)
	}
}

func TestCanonicalInteger(t *testing.T) {
	for s, want := range map[string]bool{
		"0": true, "7": true, "-7": true, "4242": true, "9223372036854775807": true,
		"": false, "-": false, "-0": false, "01": false, "+1": false, "1.0": false, "1e3": false, " 1": false, "0x1": false,
	} {
		if got := canonicalInteger(s); got != want {
			t.Errorf("canonicalInteger(%q) = %t", s, got)
		}
	}
}
