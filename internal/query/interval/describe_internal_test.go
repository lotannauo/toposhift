package interval

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// TestPayloadAttributeIsTheStores pins payloadAttr to the attribute
// store.Record.Assertion carries a payload in, for an entity and for a host
// with a boot id.
func TestPayloadAttributeIsTheStores(t *testing.T) {
	host, err := identity.FingerprintFromHash(catalog.Host, [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	for _, boot := range []string{"", "b1"} {
		r := store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "p", EventTime: t0,
			Kind: lifecycle.Observe, Payload: []byte("spec"), Boot: boot,
		}
		var payloads int
		for _, a := range r.Assertion().Attrs {
			if a.Key == payloadAttr {
				payloads++
				if a.Value != "spec" {
					t.Errorf("boot %q: the payload attribute holds %#v", boot, a.Value)
				}
			}
		}
		if payloads != 1 {
			t.Errorf("boot %q: %d attributes named %q in %v", boot, payloads, payloadAttr, r.Assertion().Attrs)
		}
	}
}

// TestDescribeAtRefusesAnInstantWithNoPayload covers the guard of describeAt:
// an instant with no live reference, or a live one with no payload attribute or
// one that is not a string, is an error naming the subject and the instant,
// never a guessed description.
func TestDescribeAtRefusesAnInstantWithNoPayload(t *testing.T) {
	pod, err := identity.FingerprintFromHash(catalog.K8sPod, [identity.FingerprintBytes]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	sub := store.EntitySubject(pod)
	fold := func(attrs ...identity.Attr) lifecycle.Timeline {
		t.Helper()
		tl, err := lifecycle.Fold([]lifecycle.Assertion{{Producer: "p", EventTime: sec(-60), Seq: 1, Kind: lifecycle.Observe, Attrs: attrs}}, lifecycle.Policy{})
		if err != nil {
			t.Fatal(err)
		}
		return tl
	}
	for name, tl := range map[string]lifecycle.Timeline{
		"no live reference":           {},
		"no payload attribute":        fold(),
		"another attribute only":      fold(identity.Attr{Key: "other", Value: "spec"}),
		"a payload that is no string": fold(identity.Attr{Key: payloadAttr, Value: 7}),
	} {
		d, err := describeAt(tl, sub, t0)
		if err == nil || d != nil || !strings.Contains(err.Error(), pod.String()) || !strings.Contains(err.Error(), formatTime(t0)) {
			t.Errorf("%s: %v, %v; want an error naming the subject and the instant", name, d, err)
		}
	}
	for payload, want := range map[string][]byte{"": nil, "spec": []byte("spec")} {
		d, err := describeAt(fold(identity.Attr{Key: payloadAttr, Value: payload}), sub, t0)
		if err != nil || d == nil || d.Producer != "p" || !bytes.Equal(d.Payload, want) || (want == nil) != (d.Payload == nil) {
			t.Errorf("payload %q: %+v, %v", payload, d, err)
		}
	}
}
