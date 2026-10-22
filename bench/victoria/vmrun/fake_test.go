package vmrun_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
)

// fakeVM is a stand-in for the parts of a server the run's commands talk to: its flags
// and counters, remote write, flush and merge. It decodes what is written (it counts the
// rows and the distinct label sets) so a load's counters can be checked against the
// converter's without a server. Every behaviour a guard must catch can be switched on.
type fakeVM struct {
	t   *testing.T
	srv *httptest.Server

	mu      sync.Mutex
	flags   map[string]string
	version string
	// What was written.
	received, added, ignored uint64
	series                   map[string]struct{}
	stored                   uint64
	smallParts, bigParts     uint64
	merges                   uint64
	writes, flushes          int
	forceMerges              int
	// Behaviour.
	hasSeries bool   // the server already holds the baseline's series
	lose      uint64 // rows received but never added
	ignore    uint64 // rows ignored
	stuck     bool   // the counters never move
	// extraSeries are series the server makes that the converter did not.
	extraSeries int
}

func newFakeVM(t *testing.T) *fakeVM {
	t.Helper()
	f := &fakeVM{t: t, series: map[string]struct{}{}, version: "v1.153.0", flags: map[string]string{
		"storageDataPath": "/victoria-metrics-data", "httpListenAddr": ":8428",
	}}
	b, err := os.ReadFile("../scripts/flags.conf")
	if err != nil {
		t.Fatal(err)
	}
	flags, err := vmengine.ParseFlags(strings.NewReader(string(b)))
	if err != nil {
		t.Fatal(err)
	}
	for _, fl := range flags {
		f.flags[fl.Name] = fl.Value
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeVM) client() *vmengine.Client {
	f.t.Helper()
	c, err := vmengine.NewClient(f.srv.URL)
	if err != nil {
		f.t.Fatal(err)
	}
	return c
}

func (f *fakeVM) handle(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/health":
		fmt.Fprint(w, "OK")
	case "/flags":
		names := make([]string, 0, len(f.flags))
		for n := range f.flags {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			fmt.Fprintf(w, "-%s=%q\n", n, f.flags[n])
		}
	case "/api/v1/series":
		if f.hasSeries || len(f.series) > 0 {
			fmt.Fprint(w, `{"status":"success","data":[{"__name__":"topo_entity"}]}`)
		} else {
			fmt.Fprint(w, `{"status":"success","data":[]}`)
		}
	case "/internal/force_flush":
		f.flushes++
		f.stored = f.added
		if f.smallParts == 0 && f.bigParts == 0 && f.added > 0 {
			f.smallParts = 2
		}
	case "/internal/force_merge":
		f.forceMerges++
		if f.smallParts > 0 {
			f.bigParts, f.smallParts = 1, 0
			f.merges++
		}
	case "/metrics":
		fmt.Fprintf(w, "vm_app_version{version=\"victoria-metrics-20260928-000000-tags-%s-0-g0\",short_version=%q} 1\n", f.version, f.version)
		fmt.Fprintf(w, "vm_rows_inserted_total{type=\"promremotewrite\"} %d\n", f.received)
		fmt.Fprintf(w, "vm_rows_added_to_storage_total %d\n", f.added)
		fmt.Fprintf(w, "vm_rows_ignored_total{reason=\"too_small_timestamp\"} %d\n", f.ignored)
		fmt.Fprintf(w, "vm_new_timeseries_created_total %d\n", len(f.series))
		fmt.Fprintf(w, "vm_rows{type=\"storage/small\"} %d\n", f.stored)
		fmt.Fprintf(w, "vm_active_force_merges 0\n")
		fmt.Fprintf(w, "vm_pending_rows{type=\"storage\"} 0\n")
		fmt.Fprintf(w, "vm_parts{type=\"storage/small\"} %d\n", f.smallParts)
		fmt.Fprintf(w, "vm_parts{type=\"storage/big\"} %d\n", f.bigParts)
		fmt.Fprintf(w, "vm_merges_total{type=\"storage/small\"} %d\n", f.merges)
		fmt.Fprintf(w, "vm_data_size_bytes{type=\"storage/small\"} %d\n", f.stored*3)
	case "/api/v1/write":
		f.write(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (f *fakeVM) write(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	raw, err := snappy.Decode(nil, body)
	if err != nil {
		http.Error(w, "snappy: "+err.Error(), http.StatusBadRequest)
		return
	}
	rows, sets, err := decodeWrite(raw)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.writes++
	if f.stuck {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	f.received += rows
	f.added += rows - min(rows, f.lose)
	f.ignored += min(rows, f.ignore)
	for _, s := range sets {
		f.series[s] = struct{}{}
	}
	for i := 0; i < f.extraSeries; i++ {
		f.series[fmt.Sprintf("extra-%d", i)] = struct{}{}
	}
	w.WriteHeader(http.StatusNoContent)
}

// decodeWrite counts the samples of a WriteRequest and returns the distinct label sets.
func decodeWrite(b []byte) (rows uint64, sets []string, err error) {
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, nil, protowire.ParseError(n)
		}
		b = b[n:]
		if num != 1 || typ != protowire.BytesType {
			return 0, nil, fmt.Errorf("unexpected field %d", num)
		}
		ts, n := protowire.ConsumeBytes(b)
		if n < 0 {
			return 0, nil, protowire.ParseError(n)
		}
		b = b[n:]
		var labels []string
		for len(ts) > 0 {
			fn, ft, n := protowire.ConsumeTag(ts)
			if n < 0 {
				return 0, nil, protowire.ParseError(n)
			}
			ts = ts[n:]
			v, n := protowire.ConsumeBytes(ts)
			if n < 0 || ft != protowire.BytesType {
				return 0, nil, fmt.Errorf("bad field %d", fn)
			}
			ts = ts[n:]
			switch fn {
			case 1:
				labels = append(labels, string(v))
			case 2:
				rows++
			}
		}
		sort.Strings(labels)
		sets = append(sets, strings.Join(labels, "\x00"))
	}
	return rows, sets, nil
}
