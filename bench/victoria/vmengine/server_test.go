package vmengine_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/victoria/vmengine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/bench/victoria/vmquery"
	"github.com/lotannauo/toposhift/bench/victoria/vmwrite"
)

// The tests in this file need a VictoriaMetrics server: TOPOSHIFT_VM_URL names it, and
// scripts/vm-server.sh starts the one the baseline is written for, on an empty data
// directory. Without the variable they skip. The tests of the behaviour the baseline
// rests on use metric names of their own and add up what they wrote, so they can be run
// again on the same server; the gate on a plan needs a server that holds none of the
// baseline's series.
const urlEnv = "TOPOSHIFT_VM_URL"

func serverClient(t testing.TB) *vmengine.Client {
	t.Helper()
	u := os.Getenv(urlEnv)
	if u == "" {
		t.Skipf("%s is not set: no VictoriaMetrics server to test against (scripts/vm-server.sh starts one)", urlEnv)
	}
	c, err := vmengine.NewClient(u)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Health(context.Background()); err != nil {
		t.Fatal(err)
	}
	return c
}

// TestServerFlagsAreTheRegisteredOnes: the server is the one flags.conf is written for.
func TestServerFlagsAreTheRegisteredOnes(t *testing.T) {
	c := serverClient(t)
	flags, _, err := vmengine.LoadFlags("../scripts/flags.conf")
	if err != nil {
		t.Fatal(err)
	}
	if err := vmengine.CheckFlags(context.Background(), c, flags); err != nil {
		t.Fatal(err)
	}
}

// semantics is a scratch name for the series of one run of the tests below.
type semantics struct {
	t      *testing.T
	c      *vmengine.Client
	metric string
}

func newSemantics(t *testing.T) *semantics {
	return &semantics{t: t, c: serverClient(t), metric: fmt.Sprintf("vm_semantics_%d", time.Now().UnixNano())}
}

func (s *semantics) labels(id string) []vmmodel.Label {
	return []vmmodel.Label{{Name: "__name__", Value: s.metric}, {Name: "id", Value: id}}
}

// put writes the series and makes them readable.
func (s *semantics) put(ts ...vmwrite.TimeSeries) {
	s.t.Helper()
	for i := range ts {
		ts[i].Labels = s.labels(ts[i].Labels[0].Value)
	}
	if err := s.c.Write(context.Background(), vmwrite.Request{Body: vmwrite.Compress(vmwrite.Marshal(ts))}); err != nil {
		s.t.Fatal(err)
	}
	if err := s.c.Flush(context.Background()); err != nil {
		s.t.Fatal(err)
	}
}

func series(id string, samples ...vmwrite.Sample) vmwrite.TimeSeries {
	return vmwrite.TimeSeries{Labels: []vmmodel.Label{{Name: "id", Value: id}}, Samples: samples}
}

// read is the ids of the series an instant query returns at an instant, by the same
// request a plan's read makes.
func (s *semantics) read(expr string, at time.Time, extra ...string) []string {
	s.t.Helper()
	req := vmquery.Request{Query: strings.ReplaceAll(expr, "$", s.metric), At: at}
	form := req.Form()
	for i := 0; i < len(extra); i += 2 {
		form.Set(extra[i], extra[i+1])
	}
	return s.post(form)
}

func (s *semantics) post(form url.Values) []string {
	s.t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, s.c.URL()+vmquery.QueryPath, strings.NewReader(form.Encode()))
	if err != nil {
		s.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	rs, err := vmquery.ParseResponse(b)
	if err != nil {
		s.t.Fatalf("%s: %v", form.Get("query"), err)
	}
	var ids []string
	for _, r := range rs {
		ids = append(ids, r.Labels["id"])
	}
	return ids
}

func (s *semantics) expect(what, expr string, at time.Time, want ...string) {
	s.t.Helper()
	got := s.read(expr, at)
	if strings.Join(got, ",") != strings.Join(want, ",") && !sameSet(got, want) {
		s.t.Errorf("%s: %s at %s: %v, want %v", what, expr, at.Format("15:04:05.000"), got, want)
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]int{}
	for _, x := range a {
		m[x]++
	}
	for _, x := range b {
		m[x]--
	}
	for _, n := range m {
		if n != 0 {
			return false
		}
	}
	return true
}

func (s *semantics) export() string {
	s.t.Helper()
	f := url.Values{}
	f.Set("match[]", s.metric)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, s.c.URL()+"/api/v1/export?"+f.Encode(), nil)
	if err != nil {
		s.t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return string(b)
}

var epoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func at(sec float64) time.Time { return epoch.Add(time.Duration(sec * float64(time.Second))) }
func ms(sec float64) int64     { return at(sec).UnixMilli() }

// TestServerWindowsAndMarkers is the behaviour the model rests on, checked against the
// server with a few series: the assumptions (a) to (l) of package vmmodel that a query
// depends on. A window is (t-W, t] to the millisecond, a range function does not see a
// staleness marker (so a series ended by a delete reads as present until its window
// ends), a window holding only a marker returns nothing, the default rollup does see a
// marker, samples at any past time are kept in any order, and everything is readable
// after a flush.
func TestServerWindowsAndMarkers(t *testing.T) {
	s := newSemantics(t)
	one, mark := vmwrite.One, vmwrite.Marker
	s.put(
		series("A", one(ms(0)), one(ms(60)), one(ms(120)), mark(ms(150))),
		series("B", one(ms(0)), one(ms(60)), one(ms(120))),
		series("C", one(ms(0)), mark(ms(100)), one(ms(200))),
		series("D", one(ms(1000))),
	)

	// (k) a marker is not seen by last_over_time: A is read until its last sample is a
	// window old, exactly as B, which has no marker.
	for _, sec := range []float64{120, 149.999, 150, 150.001, 160, 300, 359.999} {
		s.expect("markers are skipped", `last_over_time(${id=~"A|B"}[240s])`, at(sec), "A", "B")
	}
	s.expect("the window of A and B ends 240 s after the last sample", `last_over_time(${id=~"A|B"}[240s])`, at(360))
	// C: the marker between two samples ends nothing for a window that holds the first.
	s.expect("a sample, a marker and a sample", `last_over_time(${id="C"}[240s])`, at(150), "C")
	s.expect("C after the first sample's window", `last_over_time(${id="C"}[240s])`, at(250), "C")
	// A window that holds only the marker returns nothing.
	s.expect("only the marker in the window", `last_over_time(${id="A"}[30s])`, at(160))
	s.expect("only the marker in the window, just after it", `last_over_time(${id="A"}[30s])`, at(151))
	// The default rollup does see the marker.
	s.expect("the default rollup before the marker", `$`, at(149.999), "A", "B")
	s.expect("the default rollup at the marker", `$`, at(150), "B")

	// (j) and (i): (t-W, t] to the millisecond, for W written in seconds and in milliseconds.
	for _, w := range []string{"60s", "60000ms"} {
		expr := `last_over_time(${id="D"}[` + w + `])`
		s.expect("before the sample", expr, at(999.999))
		s.expect("at the sample: the right end is in", expr, at(1000), "D")
		s.expect("a millisecond in", expr, at(1000.001), "D")
		s.expect("a millisecond before the window's end", expr, at(1059.999), "D")
		s.expect("a window after: the left end is out", expr, at(1060))
	}

	// (d) the modifier inside the selector gives what time= gives.
	for _, sec := range []float64{999.999, 1000, 1030, 1059.999, 1060} {
		by := s.read(`last_over_time(${id="D"}[60s])`, at(sec))
		mod := s.read(fmt.Sprintf(`last_over_time(${id="D"}[60s] @ %.3f)`, float64(at(sec).UnixMilli())/1000), at(9999))
		if !sameSet(by, mod) {
			t.Errorf("at %v: time= gives %v, the modifier %v", sec, by, mod)
		}
	}

	// (e) the default lookback is the registered staleness interval, an hour.
	s.put(series("E", one(ms(5000))))
	s.expect("an hour less a second after", `${id="E"}`, at(5000+3599), "E")
	s.expect("an hour and a second after", `${id="E"}`, at(5000+3601))

	// (f) nocache=1 is accepted and changes no answer.
	with := s.read(`last_over_time(${id="D"}[60s])`, at(1030))
	without := s.post(url.Values{"query": {strings.ReplaceAll(`last_over_time(${id="D"}[60s])`, "$", s.metric)}, "time": {fmt.Sprintf("%d.000", at(1030).Unix())}})
	if !sameSet(with, without) || len(with) != 1 {
		t.Errorf("nocache=1 %v, without %v", with, without)
	}

	// (b) the marker is stored, as a row of its own, with a value that is not a number.
	if exp := s.export(); !strings.Contains(exp, "null") {
		t.Errorf("the export does not show a marker: %s", exp)
	}
}

// TestServerKeepsSamplesAtAnyTimeAndTheSameMillisecondTwice: (c) samples long before now,
// and a late one written after a later one, are kept; and with deduplication off two
// samples of one series in one millisecond are both kept, which is why the converter
// leaves one.
func TestServerKeepsSamplesAtAnyTimeAndTheSameMillisecondTwice(t *testing.T) {
	s := newSemantics(t)
	y1971 := time.Date(1971, 1, 1, 0, 0, 0, 0, time.UTC)
	y2022 := time.Date(2022, 8, 1, 0, 0, 0, 0, time.UTC)
	s.put(series("old", vmwrite.One(y1971.UnixMilli()), vmwrite.One(y2022.UnixMilli())))
	s.expect("a sample in 1971", `last_over_time(${id="old"}[60s])`, y1971, "old")
	s.expect("a sample in 2022", `last_over_time(${id="old"}[60s])`, y2022, "old")

	s.put(series("late", vmwrite.One(ms(1000))))
	s.put(series("late", vmwrite.One(ms(500))))
	s.expect("the late sample", `last_over_time(${id="late"}[60s])`, at(500), "late")
	s.expect("the earlier write", `last_over_time(${id="late"}[60s])`, at(1000), "late")

	s.put(series("twin", vmwrite.One(ms(10)), vmwrite.Sample{Value: 2, TimestampMs: ms(10)}))
	if exp := s.export(); strings.Count(exp, `"timestamps":[`+fmt.Sprint(ms(10))+`,`+fmt.Sprint(ms(10))+`]`) != 1 {
		t.Errorf("two samples in one millisecond were not both kept: %s", exp)
	}
}

// TestServerCountersAndCompaction: (g) and (h) after a load of samples and markers, the
// server's counters say every row arrived, none was ignored and the series are those
// written; after a flush, a settle and a forced merge, the parts are merged and every row
// is stored.
func TestServerCountersAndCompaction(t *testing.T) {
	c := serverClient(t)
	ctx := context.Background()
	metric := fmt.Sprintf("vm_semantics_load_%d", time.Now().UnixNano())
	// What earlier tests wrote has been counted by the time the server is idle.
	if err := c.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	before, err := c.Settle(ctx, 500*time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	const nseries, per = 50, 400
	var ts []vmwrite.TimeSeries
	rows := 0
	for i := 0; i < nseries; i++ {
		x := vmwrite.TimeSeries{Labels: []vmmodel.Label{{Name: "__name__", Value: metric}, {Name: "id", Value: fmt.Sprint(i)}}}
		for k := 0; k < per; k++ {
			x.Samples = append(x.Samples, vmwrite.One(ms(float64(k*60)+float64(i)/1000)))
		}
		x.Samples = append(x.Samples, vmwrite.Marker(ms(float64(per*60))))
		rows += len(x.Samples)
		ts = append(ts, x)
	}
	for _, chunk := range [][]vmwrite.TimeSeries{ts[:20], ts[20:]} {
		if err := c.Write(ctx, vmwrite.Request{Body: vmwrite.Compress(vmwrite.Marshal(chunk))}); err != nil {
			t.Fatal(err)
		}
	}
	// The counters follow the requests by up to a second or so.
	var got vmengine.Counters
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		if err := c.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		if got, err = c.Counters(ctx); err != nil {
			t.Fatal(err)
		}
		if got.Received-before.Received >= uint64(rows) || time.Now().After(deadline) {
			break
		}
	}
	if d := got.Received - before.Received; d != uint64(rows) {
		t.Errorf("received %d rows, wrote %d", d, rows)
	}
	if d := got.Added - before.Added; d != uint64(rows) {
		t.Errorf("added %d rows, wrote %d", d, rows)
	}
	if d := got.Ignored - before.Ignored; d != 0 {
		t.Errorf("ignored %d rows", d)
	}
	if d := got.NewSeries - before.NewSeries; d != nseries {
		t.Errorf("created %d series, wrote %d", d, nseries)
	}

	done, err := c.Compact(ctx, time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if done.PendingRows != 0 || done.InMemoryParts != 0 || done.Merging != 0 {
		t.Errorf("not idle after Compact: %+v", done)
	}
	if d := done.Stored - before.Stored; d != uint64(rows) {
		t.Errorf("stored %d rows after the merge, wrote %d", d, rows)
	}
	// A second forced merge on merged data changes nothing.
	again, err := c.Compact(ctx, time.Second, 3)
	if err != nil {
		t.Fatal(err)
	}
	if again.Stored != done.Stored || again.SmallParts+again.BigParts != done.SmallParts+done.BigParts {
		t.Errorf("a second merge changed the data: %+v then %+v", done, again)
	}
}

// seriesCount is the server's own count of series, /api/v1/series/count.
func seriesCount(t *testing.T, c *vmengine.Client) int {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, c.URL()+"/api/v1/series/count", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	var out struct {
		Status string `json:"status"`
		Data   []int  `json:"data"`
	}
	if err := json.Unmarshal(b, &out); err != nil || resp.StatusCode != http.StatusOK || out.Status != "success" || len(out.Data) != 1 {
		t.Fatalf("/api/v1/series/count: status %d: %s (%v)", resp.StatusCode, b, err)
	}
	return out.Data[0]
}

// TestServerSeriesAcrossAMonth is the reason a load is refused when its samples fall in
// more than one UTC month: the server's own count of series (/api/v1/series/count) counts
// a series once for each monthly partition it has samples in, while the counter of the
// series it created (vm_new_timeseries_created_total, which a load is checked against) counts
// each once. Series with samples in one month are counted once by both; series with samples
// in two months are counted twice by the first.
func TestServerSeriesAcrossAMonth(t *testing.T) {
	c := serverClient(t)
	ctx := context.Background()
	const n = 3
	write := func(name string, times ...time.Time) (created, counted int) {
		metric := fmt.Sprintf("vm_semantics_%s_%d", name, time.Now().UnixNano())
		if err := c.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		before, err := c.Settle(ctx, 500*time.Millisecond, 3)
		if err != nil {
			t.Fatal(err)
		}
		count0 := seriesCount(t, c)
		var ts []vmwrite.TimeSeries
		for i := 0; i < n; i++ {
			x := vmwrite.TimeSeries{Labels: []vmmodel.Label{{Name: "__name__", Value: metric}, {Name: "id", Value: fmt.Sprint(i)}}}
			for _, at := range times {
				x.Samples = append(x.Samples, vmwrite.One(at.UnixMilli()))
			}
			ts = append(ts, x)
		}
		if err := c.Write(ctx, vmwrite.Request{Body: vmwrite.Compress(vmwrite.Marshal(ts))}); err != nil {
			t.Fatal(err)
		}
		if err := c.Flush(ctx); err != nil {
			t.Fatal(err)
		}
		after, err := c.Settle(ctx, 500*time.Millisecond, 3)
		if err != nil {
			t.Fatal(err)
		}
		return int(after.NewSeries - before.NewSeries), seriesCount(t, c) - count0
	}
	jan := time.Date(2026, 1, 31, 23, 0, 0, 0, time.UTC)
	feb := time.Date(2026, 2, 1, 1, 0, 0, 0, time.UTC)
	oneCreated, oneCounted := write("onemonth", jan, jan.Add(time.Minute))
	twoCreated, twoCounted := write("twomonths", jan, feb)
	t.Logf("%d series in one month: created %d, counted %d; in two months: created %d, counted %d", n, oneCreated, oneCounted, twoCreated, twoCounted)
	if oneCreated != n || twoCreated != n {
		t.Errorf("the counter of created series moved by %d and %d for %d series", oneCreated, twoCreated, n)
	}
	if oneCounted != n || twoCounted != 2*n {
		t.Errorf("the server's count of series moved by %d for series in one month and by %d for series in two, not %d and %d", oneCounted, twoCounted, n, 2*n)
	}
}
