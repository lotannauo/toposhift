package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests run on small datasets of synthetic rows in the trace's format: a
// placement archive holds placementSpan minutes, a call archive callSpan minutes,
// and the hour is two placement archives and four call archives, twelve minutes.
const (
	placementSpan = 6
	callSpan      = 3
	hourLen       = 2 * placementSpan
)

// pl is one placement sample: the minute of the hour, and the three ids.
type pl struct {
	min            int
	ms, inst, node string
	cpu            string
}

func (p pl) csv(offsetMin int) string {
	cpu := p.cpu
	if cpu == "" {
		cpu = "0.25"
	}
	return fmt.Sprintf("%d,%s,%s,%s,%s,0.5", int64(p.min-offsetMin)*60000, p.ms, p.inst, p.node, cpu)
}

// cl is one call.
type cl struct {
	ts                   int64 // ms from the hour's start
	trace, rpcID, rpc    string
	um, umi, dm, dmi, rt string
}

func (c cl) csv(offsetMs int64) string {
	rpc := c.rpc
	if rpc == "" {
		rpc = "rpc"
	}
	rpcID := c.rpcID
	if rpcID == "" {
		rpcID = "0.1"
	}
	rt := c.rt
	if rt == "" {
		rt = "1.0"
	}
	return fmt.Sprintf("%d,%s,S_1,%s,%s,%s,%s,IFACE,%s,%s,%s", c.ts-offsetMs, c.trace, rpcID, rpc, c.um, c.umi, c.dm, c.dmi, rt)
}

// dataset is the rows of a test hour; raw lines are added to an archive as they are.
type dataset struct {
	placement []pl
	calls     []cl
	// rawPlacement and rawCalls are CSV lines added to archive 0 of the table as written.
	rawPlacement, rawCalls []string
	// perFile makes every archive's timestamps restart at zero.
	perFile bool
	// shuffle reverses the order of the rows within each archive.
	shuffle bool
}

// filler keeps every minute of every archive populated, so the layout check
// passes without the test saying so: a pod on a node of its own at every minute,
// and one call in every call archive.
func (d dataset) withFiller() dataset {
	for m := 0; m < hourLen; m++ {
		d.placement = append(d.placement, pl{min: m, ms: "MS_FILL", inst: "MS_FILL_POD_0", node: "NODE_FILL"})
	}
	for f := 0; f < hourLen/callSpan; f++ {
		d.calls = append(d.calls, cl{ts: int64(f) * callSpan * 60000, trace: "T_FILL", um: "MS_FILL_A", umi: "MS_FILL_A_POD_0", dm: "MS_FILL_B", dmi: "MS_FILL_B_POD_0"})
	}
	return d
}

func tarGz(t testing.TB, member, body string) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{Name: member, Mode: 0o644, Size: int64(len(body)), Typeflag: tar.TypeReg}); err != nil {
		t.Fatal(err)
	}
	if _, err := tw.Write([]byte(body)); err != nil {
		t.Fatal(err)
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// write puts the archives and SHA256SUMS in a new directory.
func (d dataset) write(t testing.TB) string {
	t.Helper()
	return d.writeTo(t, t.TempDir())
}

// writeTo puts the archives and SHA256SUMS in dir, and returns it.
func (d dataset) writeTo(t testing.TB, dir string) string {
	t.Helper()
	var sums strings.Builder
	put := func(table string, idx int, header []string, lines []string) {
		if err := os.MkdirAll(filepath.Join(dir, table), 0o755); err != nil {
			t.Fatal(err)
		}
		body := strings.Join(header, ",") + "\n" + strings.Join(lines, "\n")
		if len(lines) > 0 {
			body += "\n"
		}
		name := fmt.Sprintf("%s_%d", table, idx)
		blob := tarGz(t, name+".csv", body)
		if err := os.WriteFile(filepath.Join(dir, table, name+".tar.gz"), blob, 0o644); err != nil {
			t.Fatal(err)
		}
		sum := sha256.Sum256(blob)
		fmt.Fprintf(&sums, "%s  ./%s/%s.tar.gz\n", hex.EncodeToString(sum[:]), table, name)
	}
	pfiles := make([][]string, 2)
	for _, p := range d.placement {
		f := p.min / placementSpan
		off := 0
		if d.perFile {
			off = f * placementSpan
		}
		pfiles[f] = append(pfiles[f], p.csv(off))
	}
	pfiles[0] = append(pfiles[0], d.rawPlacement...)
	cfiles := make([][]string, hourLen/callSpan)
	for _, c := range d.calls {
		f := int(c.ts / (callSpan * 60000))
		off := int64(0)
		if d.perFile {
			off = int64(f) * callSpan * 60000
		}
		cfiles[f] = append(cfiles[f], c.csv(off))
	}
	cfiles[0] = append(cfiles[0], d.rawCalls...)
	for i := range pfiles {
		if d.shuffle {
			slicesReverse(pfiles[i])
		}
		put(placementTable, i, placementHeader, pfiles[i])
	}
	for i := range cfiles {
		if d.shuffle {
			slicesReverse(cfiles[i])
		}
		put(callsTable, i, callsHeader, cfiles[i])
	}
	if err := os.WriteFile(filepath.Join(dir, "SHA256SUMS"), []byte(sums.String()), 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func slicesReverse(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

// testConfig is the configuration for the small hour.
func testConfig(t testing.TB, in string) Config {
	t.Helper()
	cfg := defaultConfig()
	cfg.InDir = in
	cfg.OutDir = filepath.Join(t.TempDir(), "run")
	cfg.PlacementFiles = 2
	cfg.PlacementFileMinutes = placementSpan
	cfg.CallFiles = hourLen / callSpan
	cfg.CallFileMillis = callSpan * 60000
	cfg.FixtureMinutes = 4
	cfg.VerifyStreams = true
	return cfg
}

// run reduces a dataset with the test configuration after change has adjusted it.
func run(t testing.TB, d dataset, change func(*Config)) (*results, Config) {
	t.Helper()
	cfg := testConfig(t, d.withFiller().write(t))
	if change != nil {
		change(&cfg)
	}
	res, err := reduce(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return res, cfg
}

// table reads a TSV, gzip'd if the path ends in .gz, as rows of columns by name.
func table(t testing.TB, path string) []map[string]string {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		zr, err := gzip.NewReader(f)
		if err != nil {
			t.Fatal(err)
		}
		defer zr.Close()
		r = zr
	}
	cr := csv.NewReader(r)
	cr.Comma = '\t'
	cr.LazyQuotes = true
	all, err := cr.ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	var out []map[string]string
	for _, row := range all[1:] {
		m := map[string]string{}
		for i, h := range all[0] {
			m[h] = row[i]
		}
		out = append(out, m)
	}
	return out
}

func outPath(cfg Config, name string) string { return filepath.Join(cfg.OutDir, "out", name) }

// records reads every record of an activity file.
func records(t testing.TB, path string) []store.Record {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer rd.Close()
	var out []store.Record
	for {
		r, err := rd.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, r)
	}
}

// intervalsOf lists the intervals of the pod on a node at a cadence from the
// placement_intervals table, as "start-end" in minutes from the epoch.
func intervalsOf(t testing.TB, cfg Config, cadence, pod string) []string {
	t.Helper()
	var out []string
	for _, row := range table(t, outPath(cfg, "placement_intervals.tsv.gz")) {
		if row["cadence"] != cadence || row["pod"] != pod {
			continue
		}
		s, _ := strconv.ParseInt(row["start_ns"], 10, 64)
		e, _ := strconv.ParseInt(row["end_ns"], 10, 64)
		base := cfg.Epoch.UnixNano()
		min := int64(60e9)
		out = append(out, fmt.Sprintf("%s:%d-%d", row["node"], (s-base)/min, (e-base)/min))
	}
	return out
}

// referenceThreshold is floor(2^64 * want / nodes) computed with big integers, or
// the largest value when every node is kept: the sampling rule, stated again.
func referenceThreshold(nodes, want int) uint64 {
	if nodes <= want {
		return ^uint64(0)
	}
	n := new(big.Int).Lsh(big.NewInt(int64(want)), 64)
	n.Div(n, big.NewInt(int64(nodes)))
	return n.Uint64()
}

// snippet reads the rows of a file in testdata, without its comment lines.
func snippet(t testing.TB, name string) []string {
	t.Helper()
	var rows []string
	for _, line := range strings.Split(string(readFileT(t, filepath.Join("testdata", name))), "\n") {
		if line != "" && !strings.HasPrefix(line, "#") {
			rows = append(rows, line)
		}
	}
	return rows
}

func readFileT(t testing.TB, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
