package apihttp

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/mcp"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

const (
	objs      lifecycle.Producer = "k8sobjects/c1"
	clus      lifecycle.Producer = "k8scluster/c1"
	agent     lifecycle.Producer = "nodeagent/c1"
	spancount lifecycle.Producer = "spancount/c1"
)

var rank = map[lifecycle.Producer]int{objs: 3, clus: 2, agent: 1}

var resolver = identity.NewResolver(catalog.Default())

// The window every question asks about: [10:00, 10:30) on 2026-01-01.
const (
	w10   = "2026-01-01T10:00:00Z"
	w1030 = "2026-01-01T10:30:00Z"
)

func hm(h, m int) time.Time { return time.Date(2026, 1, 1, h, m, 0, 0, time.UTC) }

func fpOf(tb testing.TB, typ catalog.EntityType, kv ...any) identity.Fingerprint {
	tb.Helper()
	var attrs []identity.Attr
	for i := 0; i < len(kv); i += 2 {
		attrs = append(attrs, identity.Attr{Key: kv[i].(catalog.AttributeKey), Value: kv[i+1]})
	}
	id, err := resolver.Resolve(typ, attrs)
	if err != nil {
		tb.Fatal(err)
	}
	return id.Fingerprint()
}

func layerOf(s store.Subject) catalog.Layer {
	if s.Kind == store.SubjectEntity {
		e, _ := catalog.Default().Entity(s.A.Type())
		return e.Layer()
	}
	r, _ := catalog.Default().Relation(s.Relation)
	return r.Layer()
}

func observe(sub store.Subject, p lifecycle.Producer, at time.Time, payload string) store.Record {
	return store.Record{Layer: layerOf(sub), Subject: sub, Producer: p, EventTime: at, Kind: lifecycle.Observe, Payload: []byte(payload)}
}

func ent(fp identity.Fingerprint) store.Subject { return store.EntitySubject(fp) }

func edge(a, b identity.Fingerprint, rel catalog.RelationType) store.Subject {
	return store.EdgeSubject(a, b, rel)
}

// world is the fixture of the routes. Times are on 2026-01-01 UTC.
//
//	pod P (web-a) on node N from 9:00, and a service svc with an instance part of P
//	pod R (web-r) on N from 10:10
//	P claims the endpoints E1 and E2 from 9:00; N claims E3 from 9:00
type world struct {
	st         *memstore.Store
	p, r, n    identity.Fingerprint
	svc        identity.Fingerprint
	e1, e2, e3 identity.Fingerprint
}

func newWorld(tb testing.TB) *world {
	tb.Helper()
	st, err := memstore.Open(memstore.Options{Policy: lifecycle.Policy{Rank: rank, BootKey: lifecycle.BootID}})
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = st.Close() })
	endpoint := func(addr string) identity.Fingerprint {
		return fpOf(tb, catalog.NetworkEndpoint, catalog.EndpointRealm, "k8s/c1", catalog.EndpointAddress, addr)
	}
	w := &world{
		st:  st,
		p:   fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "pod-p"),
		r:   fpOf(tb, catalog.K8sPod, catalog.K8sPodUID, "pod-r"),
		n:   fpOf(tb, catalog.K8sNode, catalog.K8sNodeUID, "node-n"),
		svc: fpOf(tb, catalog.Service, catalog.ServiceName, "svc"),
		e1:  endpoint("10.42.0.7"), e2: endpoint("10.42.0.8"), e3: endpoint("10.12.0.3"),
	}
	inst := fpOf(tb, catalog.ServiceInstance, catalog.ServiceName, "svc", catalog.ServiceInstanceID, "svc-0")
	nine := hm(9, 0)
	recs := []store.Record{
		observe(ent(w.p), objs, nine, `{"name":"web-a","namespace":"shop"}`),
		observe(ent(w.r), objs, hm(10, 10), `{"name":"web-r","namespace":"shop"}`),
		observe(ent(w.n), objs, nine, `{"name":"worker-n"}`),
		observe(ent(w.e1), objs, nine, `{}`),
		observe(ent(w.e2), objs, nine, `{}`),
		observe(ent(w.e3), objs, nine, `{}`),
		observe(edge(w.p, w.n, catalog.ScheduledOn), objs, nine, `{}`),
		observe(edge(w.r, w.n, catalog.ScheduledOn), objs, hm(10, 10), `{}`),
		observe(edge(w.p, w.e1, catalog.HasAddress), objs, nine, `{}`),
		observe(edge(w.p, w.e2, catalog.HasAddress), objs, nine, `{}`),
		observe(edge(w.n, w.e3, catalog.HasAddress), objs, nine, `{}`),
		observe(ent(w.svc), spancount, nine, `{"name":"svc"}`),
		observe(ent(inst), spancount, nine, `{}`),
		observe(edge(inst, w.svc, catalog.InstanceOf), spancount, nine, `{}`),
		observe(edge(inst, w.p, catalog.PartOf), spancount, nine, `{}`),
	}
	for i := range recs {
		recs[i].Seq = uint64(i + 1)
	}
	if err := st.Write(context.Background(), recs); err != nil {
		tb.Fatal(err)
	}
	return w
}

// sources is a [Sources] that holds one source, or the error to answer with.
type sources struct {
	src *mcp.Source
	err error
}

// Source refuses to be asked for the source of a call that reads no store: a
// route reads one.
func (s sources) Source(readsStore bool) (*mcp.Source, error) {
	if !readsStore {
		return nil, errors.New("a route was given the source of a call that reads no store")
	}
	return s.src, s.err
}

func sourceOn(tb testing.TB, st store.Store, cfg mcp.Config) *mcp.Source {
	tb.Helper()
	cfg.Store, cfg.Rank, cfg.RangeReads = st, rank, true
	src, err := mcp.NewSource(cfg)
	if err != nil {
		tb.Fatal(err)
	}
	return src
}

// apiOn returns the API over a source on st.
func apiOn(tb testing.TB, st store.Store, cfg mcp.Config) (*API, *mcp.Source) {
	tb.Helper()
	src := sourceOn(tb, st, cfg)
	return New(sources{src: src}), src
}

// loopback is a connection accepted on loopback: httptest's own.
func serve(tb testing.TB, a *API) string {
	tb.Helper()
	mux := http.NewServeMux()
	for _, p := range Paths() {
		mux.Handle(p, a.Handler())
	}
	ts := httptest.NewServer(mux)
	tb.Cleanup(ts.Close)
	return ts.URL
}

// reply is an answer, read whole.
type reply struct {
	status int
	header http.Header
	body   []byte
}

func (r reply) text() string { return string(r.body) }

// post sends body to path of base as application/json.
func post(tb testing.TB, base, path, body string) reply {
	tb.Helper()
	return send(tb, base, path, body, nil)
}

func send(tb testing.TB, base, path, body string, headers map[string]string) reply {
	tb.Helper()
	req, err := http.NewRequestWithContext(tb.Context(), http.MethodPost, base+path, strings.NewReader(body))
	if err != nil {
		tb.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		if v == "-" {
			req.Header.Del(k)
			continue
		}
		if k == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Transport: &http.Transport{DisableKeepAlives: true}}).Do(req)
	if err != nil {
		tb.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		tb.Fatal(err)
	}
	return reply{status: resp.StatusCode, header: resp.Header, body: b}
}

func strs(fps ...identity.Fingerprint) []string {
	out := make([]string, len(fps))
	for i, fp := range fps {
		out[i] = fp.String()
	}
	return slices.Clone(out)
}
