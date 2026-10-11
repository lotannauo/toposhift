package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"maps"
	"reflect"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
)

// B8. The store serve opens ranks the objects receiver's producer above the
// cluster receiver's, for the configured cluster name and for "default" when
// none is set; every other option is the one every command opens its store
// with.
func TestServeStoreOptionsRankTheProducers(t *testing.T) {
	for _, tt := range []struct {
		name    string
		set     bool
		cluster string
	}{{"prod", true, "prod"}, {"", false, "default"}} {
		o := serveStoreOptions(tt.name, tt.set)
		want := map[lifecycle.Producer]int{
			lifecycle.Producer("k8sobjects/" + tt.cluster): 2,
			lifecycle.Producer("k8scluster/" + tt.cluster): 1,
		}
		if !maps.Equal(o.Policy.Rank, want) {
			t.Errorf("%q set %v: Rank %v, want %v", tt.name, tt.set, o.Policy.Rank, want)
		}
		o.Policy.Rank = nil
		if base := storeOptions(); !reflect.DeepEqual(o, base) {
			t.Errorf("%q set %v: options other than Rank differ from storeOptions:\n%+v\n%+v", tt.name, tt.set, o, base)
		}
	}
	// Each call makes its own map, so a caller cannot change another's.
	a, b := serveStoreOptions("x", true), serveStoreOptions("x", true)
	a.Policy.Rank["k8sobjects/x"] = 9
	if b.Policy.Rank["k8sobjects/x"] != 2 {
		t.Error("two calls share one Rank map")
	}
}

type acceptAll struct{ partial otlphttp.Partial }

func (acceptAll) Accept(context.Context, capture.LogsData) error { return nil }

func (a acceptAll) AcceptPartial(context.Context, capture.LogsData) (otlphttp.Partial, error) {
	return a.partial, nil
}

// The receiver's sink answers as the sink it holds now: a gate's 503, then a
// sink that accepts in part.
func TestSwitchSink(t *testing.T) {
	var s switchSink
	s.set(gateSink{message: ingestStarting})
	_, err := s.AcceptPartial(t.Context(), capture.LogsData{})
	var off *otlphttp.UnavailableError
	if !errors.As(err, &off) || off.Message != ingestStarting {
		t.Fatalf("gate: %v", err)
	}
	s.set(acceptAll{otlphttp.Partial{Rejected: 2}})
	if p, err := s.AcceptPartial(t.Context(), capture.LogsData{}); err != nil || p.Rejected != 2 {
		t.Fatalf("switched: %+v %v", p, err)
	}
	if err := s.Accept(t.Context(), capture.LogsData{}); err != nil {
		t.Fatal(err)
	}
}

// serve -h documents the ingest flags.
func TestServeHelpDocumentsIngest(t *testing.T) {
	var stderr bytes.Buffer
	if code := run(t.Context(), []string{"serve", "-h"}, noEnv, io.Discard, &stderr); code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	for _, want := range []string{
		"-cluster-name", "TOPOSHIFT_CLUSTER_NAME", "-accept-new-cluster-name", "command line only",
		"-pull-interval", "Ingest", "committed to disk", "restarted",
	} {
		if !strings.Contains(stderr.String(), want) {
			t.Errorf("serve -h does not mention %q", want)
		}
	}
}
