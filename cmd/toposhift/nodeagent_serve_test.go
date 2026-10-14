package main

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store"
)

// nodeAgentBody is GET /status's nodeagent object.
type nodeAgentBody struct {
	NodeAgent struct {
		Mode     string           `json:"mode"`
		Interval string           `json:"interval"`
		Counters map[string]int64 `json:"counters"`
		Clusters map[string]struct {
			LastHostEvent time.Time `json:"last_host_event"`
			Silent        bool      `json:"silent"`
		} `json:"clusters"`
	} `json:"nodeagent"`
}

func getNodeAgent(t *testing.T, base string) nodeAgentBody {
	t.Helper()
	code, body := getBody(t, base+"/status")
	var r nodeAgentBody
	if err := json.Unmarshal([]byte(body), &r); code != http.StatusOK || err != nil {
		t.Fatalf("GET /status = %d %q (%v)", code, body, err)
	}
	return r
}

// hostEventBody is a node collector's host entity event, as OTLP JSON, with a
// host.id and no k8s.node.uid when node is empty.
func hostEventBody(t *testing.T, hostID, node string, at time.Time) []byte {
	t.Helper()
	str := func(s string) capture.AnyValue { return capture.AnyValue{StringValue: &s} }
	interval := capture.Int64(120)
	var attrs []capture.KeyValue
	if hostID != "" {
		attrs = append(attrs, capture.KeyValue{Key: "host.id", Value: str(hostID)})
	}
	if node != "" {
		attrs = append(attrs, capture.KeyValue{Key: "k8s.node.uid", Value: str(node)})
	}
	attrs = append(attrs, capture.KeyValue{Key: "topo.host.boot.id", Value: str("boot-1")})
	d := capture.LogsData{ResourceLogs: []capture.ResourceLogs{{
		Resource: capture.Resource{Attributes: attrs},
		ScopeLogs: []capture.ScopeLogs{{LogRecords: []capture.LogRecord{{
			EventName:    "entity.state",
			TimeUnixNano: capture.Uint64(at.UnixNano()),
			Attributes: []capture.KeyValue{
				{Key: "entity.type", Value: str("host")},
				{Key: "entity.report.interval", Value: capture.AnyValue{IntValue: &interval}},
			},
		}}}},
	}}}
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// serve takes a node collector's host entity event through its receiver and
// sequencer: GET /status says the node agents are on, with the interval serve was
// given and the counters; the host and its placement are stored with three times
// that interval as their TTL; an event with no host id is counted as refused and
// stores nothing.
func TestServeStoresNodeAgentEventsAndCountsThem(t *testing.T) {
	dir := t.TempDir()
	s := startServe(t, dirEnv(t, dir), "--retain", "off", "--node-interval", "2m")
	waitIngestOn(t, s.base)
	st := getNodeAgent(t, s.base)
	if st.NodeAgent.Mode != "on" || st.NodeAgent.Interval != "2m0s" || len(st.NodeAgent.Clusters) != 0 {
		t.Fatalf("before any event: %+v", st.NodeAgent)
	}

	now := time.Now().Truncate(time.Second)
	postOK(t, s.base, hostEventBody(t, "host-1", "node-1", now))
	postOK(t, s.base, hostEventBody(t, "", "node-2", now))
	st = getNodeAgent(t, s.base)
	c := st.NodeAgent.Counters
	if c["nodeagent.host_events"] != 2 || c["nodeagent.refused_no_host_id"] != 1 {
		t.Errorf("counters %v, want two host events and one refused for no host id", c)
	}
	last, ok := st.NodeAgent.Clusters["default"]
	if !ok || last.LastHostEvent.Before(now) || last.Silent {
		t.Errorf("clusters %+v, want the default cluster with an event since %s, not silent", st.NodeAgent.Clusters, now)
	}
	stopOK(t, s)

	stopped := openStopped(t, dir)
	resolver := identity.NewResolver(catalog.Default())
	id, err := resolver.Resolve(catalog.Host, []identity.Attr{{Key: catalog.HostID, Value: "host-1"}})
	if err != nil {
		t.Fatal(err)
	}
	recs, err := stopped.EntityWindow(t.Context(), id.Fingerprint(), now.Add(-time.Hour), now.Add(time.Hour), store.Current(catalog.L1))
	if err != nil || len(recs) != 1 {
		t.Fatalf("the host's records: %d, %v", len(recs), err)
	}
	if recs[0].TTL != 6*time.Minute || recs[0].Boot != "boot-1" || recs[0].Producer != "nodeagent/default" {
		t.Errorf("the host's record: TTL %s, boot %q, producer %s; want 6m, boot-1, nodeagent/default", recs[0].TTL, recs[0].Boot, recs[0].Producer)
	}
	// The node with no host id left nothing: no host of its own and no edge.
	node2, err := resolver.Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "node-2"}})
	if err != nil {
		t.Fatal(err)
	}
	ns, err := stopped.Neighbors(t.Context(), node2.Fingerprint(), store.Forward, now, store.Current(catalog.L1))
	if err != nil || len(ns) != 0 {
		t.Errorf("the node with no host id has neighbors %v (%v)", ns, err)
	}
}

// With no node-interval given, serve expects five minutes, and the variable sets
// it.
func TestServeNodeIntervalDefaultsAndTheVariable(t *testing.T) {
	for _, tt := range []struct {
		env  map[string]string
		args []string
		want string
	}{
		{nil, nil, "5m0s"},
		{map[string]string{"TOPOSHIFT_NODE_INTERVAL": "10m"}, nil, "10m0s"},
		{map[string]string{"TOPOSHIFT_NODE_INTERVAL": "10m"}, []string{"--node-interval", "90s"}, "1m30s"},
		{map[string]string{"TOPOSHIFT_NODE_INTERVAL": ""}, nil, "5m0s"},
	} {
		env := map[string]string{"TOPOSHIFT_DATA_DIR": t.TempDir()}
		for k, v := range tt.env {
			env[k] = v
		}
		s := startServe(t, testEnv(t, env), append([]string{"--retain", "off"}, tt.args...)...)
		waitIngestOn(t, s.base)
		if got := getNodeAgent(t, s.base).NodeAgent.Interval; got != tt.want {
			t.Errorf("env %v, args %v: interval %s, want %s", tt.env, tt.args, got, tt.want)
		}
		stopOK(t, s)
	}
}
