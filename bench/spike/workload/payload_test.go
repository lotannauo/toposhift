package workload_test

import (
	"bytes"
	"compress/gzip"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// A payload pad lengthens every payload and changes nothing else: the stream with it
// has the same records in the same order, each payload the one without it followed by
// the pad, and the pads are random, so longer payloads are not flattered by
// compression.
func TestAPayloadPadChangesOnlyThePayloads(t *testing.T) {
	t.Parallel()

	small := workload.Small()
	small.Duration = 10 * time.Minute
	cluster := conformance.Configs()[6] // fresh identities, pod heartbeats, a backlog, coalesced runs
	for name, cfg := range map[string]workload.Config{"small": small, "cluster-shaped": cluster} {
		const pad = 100
		padded := cfg
		padded.PayloadPad = pad
		plain, long := generate(t, cfg), generate(t, padded)
		if len(plain) != len(long) || len(plain) == 0 {
			t.Fatalf("%s: %d records without the pad and %d with it", name, len(plain), len(long))
		}
		var pads bytes.Buffer
		for i := range plain {
			a, b := plain[i], long[i]
			if len(a.Payload) == 0 {
				if len(b.Payload) != 0 {
					t.Fatalf("%s: record %d had no payload and has %d bytes", name, i, len(b.Payload))
				}
			} else if len(b.Payload) != len(a.Payload)+pad || !bytes.Equal(b.Payload[:len(a.Payload)], a.Payload) {
				t.Fatalf("%s: record %d's payload of %d bytes is %d bytes, or does not begin with it", name, i, len(a.Payload), len(b.Payload))
			} else if pads.Len() < 1<<20 {
				pads.Write(b.Payload[len(a.Payload):])
			}
			a.Payload, b.Payload = nil, nil
			if digest([]engine.Record{a}) != digest([]engine.Record{b}) {
				t.Fatalf("%s: record %d differs in more than its payload: %+v and %+v", name, i, a, b)
			}
		}
		var packed bytes.Buffer
		zw := gzip.NewWriter(&packed)
		_, _ = zw.Write(pads.Bytes())
		_ = zw.Close()
		if ratio := float64(packed.Len()) / float64(pads.Len()); ratio < 0.9 {
			t.Errorf("%s: the pads compress to %.0f%% of their size", name, ratio*100)
		}
	}
	bad := small
	bad.PayloadPad = -1
	if _, err := workload.New(bad); err == nil {
		t.Error("a negative pad was accepted")
	}
}
