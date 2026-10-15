package bytesgate_test

import (
	"bytes"
	"math"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/andybalholm/brotli"

	"github.com/lotannauo/toposhift/internal/wire"
	"github.com/lotannauo/toposhift/internal/wire/bytesgate"
)

func TestBrotliRoundTrips(t *testing.T) {
	in := bytes.Repeat([]byte("temporal topology "), 5000)
	packed, err := bytesgate.Brotli(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(packed) >= len(in)/10 {
		t.Errorf("%d bytes packed to %d", len(in), len(packed))
	}
	out, err := bytesgate.Unbrotli(packed)
	if err != nil || !bytes.Equal(out, in) {
		t.Errorf("the round trip: %v", err)
	}
	if again, _ := bytesgate.Brotli(in); !bytes.Equal(again, packed) {
		t.Error("brotli is not deterministic")
	}
	// The settings are quality 11 and a window of 2^22, written out here apart
	// from the constants.
	var want bytes.Buffer
	w := brotli.NewWriterOptions(&want, brotli.WriterOptions{Quality: 11, LGWin: 22})
	if _, err := w.Write(in); err != nil || w.Close() != nil {
		t.Fatal("the reference writer failed")
	}
	if !bytes.Equal(want.Bytes(), packed) {
		t.Error("Brotli is not brotli at quality 11 with a window of 2^22")
	}
}

// The numbers depend on the brotli version and on arrow-go's writer, so the gate
// is run against the versions it was written for. A routine bump of either, which
// arrow-go also gets for other packages, must not fail the checks of every
// change, so this runs with the gate and not before it.
func TestCompressorsArePinned(t *testing.T) {
	if os.Getenv("TOPOSHIFT_BYTES_GATE") == "" {
		t.Skip("set TOPOSHIFT_BYTES_GATE=1 to check the versions the gate was written against")
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		t.Skip("no build information in this binary")
	}
	want := map[string]string{
		"github.com/andybalholm/brotli":  "v1.2.3",
		"github.com/apache/arrow-go/v18": "v18.8.0",
	}
	found := 0
	for _, d := range info.Deps {
		if v, ok := want[d.Path]; ok {
			found++
			if d.Version != v {
				t.Errorf("%s is %s, the gate was written against %s", d.Path, d.Version, v)
			}
		}
	}
	if found == 0 {
		t.Skip("no dependencies in this binary's build information")
	}
	if found != len(want) {
		t.Errorf("%d of %d pinned modules are in the binary", found, len(want))
	}
}

func row(name string, custom, arrow int) bytesgate.Row {
	return bytesgate.Row{Name: name, Custom: bytesgate.Sizes{Raw: custom * 3, Brotli: custom}, Arrow: bytesgate.Sizes{Raw: arrow * 3, Brotli: arrow}}
}

func TestRatioGeometricMeanAndLargest(t *testing.T) {
	rep := bytesgate.Report{Rows: []bytesgate.Row{row("a", 100, 100), row("b", 100, 400), row("c", 100, 50)}}
	if got := rep.Rows[1].Ratio(); got != 4 {
		t.Errorf("ratio %v", got)
	}
	// 1, 4 and 0.5 multiply to 2.
	if got, want := rep.GeoMean(), math.Cbrt(2); math.Abs(got-want) > 1e-12 {
		t.Errorf("geometric mean %v, want %v", got, want)
	}
	if r, v := rep.Largest(); r.Name != "b" || v != 4 {
		t.Errorf("largest %s %v", r.Name, v)
	}
	if !math.IsNaN(bytesgate.Report{}.GeoMean()) {
		t.Error("the geometric mean of nothing is a number")
	}
	if _, v := (bytesgate.Report{}).Largest(); !math.IsNaN(v) {
		t.Error("the largest of nothing is a number")
	}
}

// The harness prints the ratios and no judgment of them: a threshold is set
// separately, and nothing here knows one.
func TestTableStatesNoVerdict(t *testing.T) {
	rep := bytesgate.Report{Rows: []bytesgate.Row{row("a", 100, 100), row("b", 100, 400)}}
	var out strings.Builder
	if err := rep.WriteTable(&out); err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(out.String())
	for _, word := range []string{"pass", "fail", "threshold", "within", "exceed", "accept", "reject", "limit", " ok", "t1", "t2"} {
		if strings.Contains(text, word) {
			t.Errorf("the table says %q:\n%s", word, out.String())
		}
	}
	for _, want := range []string{"geometric mean of r: ", "largest r: ", "brotli quality 11, window 2^22"} {
		if !strings.Contains(text, want) {
			t.Errorf("the table lacks %q:\n%s", want, out.String())
		}
	}
}

// The whole pipeline on small fixtures: build, encode, compress, tabulate. Each
// size is the length of its encoding, and each compressed size is the length of a
// brotli stream that decompresses to it. The figures are not asserted: they are of
// fixtures far smaller than the gate's.
func TestRunOnSmallFixtures(t *testing.T) {
	docs := build(t, small())
	rep, err := bytesgate.Run(ctx, small())
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Rows) != 5 {
		t.Fatalf("%d rows", len(rep.Rows))
	}
	for _, r := range rep.Rows {
		d := docs[r.Name]
		custom, _ := wire.Encode(d)
		arrowBytes, _ := bytesgate.Arrow(d)
		dictBytes, _ := bytesgate.ArrowDictionaries(d)
		twin, _ := wire.JSON(d)
		meta, err := bytesgate.Metadata(arrowBytes)
		if err != nil || r.ArrowMeta != meta {
			t.Errorf("fixture %s: %d metadata bytes, want %d (%v)", r.Name, r.ArrowMeta, meta, err)
		}
		for _, e := range []struct {
			name string
			raw  []byte
			got  bytesgate.Sizes
		}{{"custom", custom, r.Custom}, {"arrow", arrowBytes, r.Arrow}, {"arrow-dict", dictBytes, r.ArrowDict}, {"json", twin, r.JSON}} {
			if e.got.Raw != len(e.raw) || e.got.Brotli == 0 || e.got.Brotli >= e.got.Raw {
				t.Errorf("fixture %s, %s: sizes %+v for %d bytes", r.Name, e.name, e.got, len(e.raw))
			}
			packed, err := bytesgate.Brotli(e.raw)
			if err != nil || len(packed) != e.got.Brotli {
				t.Errorf("fixture %s, %s: brotli is %d bytes, the report says %d (%v)", r.Name, e.name, len(packed), e.got.Brotli, err)
			}
			if back, err := bytesgate.Unbrotli(packed); err != nil || !bytes.Equal(back, e.raw) {
				t.Errorf("fixture %s, %s: the compressed bytes do not decompress to the encoding: %v", r.Name, e.name, err)
			}
		}
		for _, ratio := range []float64{r.Ratio(), r.RatioDict()} {
			if math.IsNaN(ratio) || ratio <= 0 {
				t.Errorf("fixture %s: ratio %v", r.Name, ratio)
			}
		}
		if want := float64(r.Arrow.Brotli) / float64(r.Custom.Brotli); r.Ratio() != want {
			t.Errorf("fixture %s: ratio %v, want %v", r.Name, r.Ratio(), want)
		}
	}
	var out strings.Builder
	if err := rep.WriteTable(&out); err != nil {
		t.Fatal(err)
	}
	if lines := strings.Count(out.String(), "\n"); lines != 4+1+5+5+2 {
		t.Errorf("the table has %d lines:\n%s", lines, out.String())
	}
}

// An encoding of more than the brotli window is marked, with a line that says
// what the mark means, and one of less is not.
func TestTableMarksWhatIsOverTheWindow(t *testing.T) {
	big, edge := row("a", 100, 100), row("b", 100, 100)
	big.Arrow.Raw = 1<<22 + 1
	edge.Arrow.Raw = 1 << 22
	var out strings.Builder
	if err := (bytesgate.Report{Rows: []bytesgate.Row{big}}).WriteTable(&out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "4194305*") || !strings.Contains(out.String(), "* more than 4194304 bytes") {
		t.Errorf("an encoding over the window is not marked:\n%s", out.String())
	}
	out.Reset()
	if err := (bytesgate.Report{Rows: []bytesgate.Row{edge}}).WriteTable(&out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "*") {
		t.Errorf("an encoding at the window is marked:\n%s", out.String())
	}
}

// TestReport is how the gate is run: the five fixtures at the reference scale,
// printed as a table. It is skipped unless TOPOSHIFT_BYTES_GATE is set, because
// the fixtures are large and brotli at quality 11 is slow.
func TestReport(t *testing.T) {
	if os.Getenv("TOPOSHIFT_BYTES_GATE") == "" {
		t.Skip("set TOPOSHIFT_BYTES_GATE=1 to measure the reference fixtures")
	}
	rep, err := bytesgate.Run(ctx, bytesgate.Reference())
	if err != nil {
		t.Fatal(err)
	}
	if err := rep.WriteTable(os.Stdout); err != nil {
		t.Fatal(err)
	}
	// The variants follow in a block of their own. They are measured after the
	// registered fixtures and enter nothing above.
	variants, err := bytesgate.RunVariants(ctx, bytesgate.Reference(), rep)
	if err != nil {
		t.Fatal(err)
	}
	if err := bytesgate.WriteVariants(os.Stdout, bytesgate.Reference(), variants); err != nil {
		t.Fatal(err)
	}
}

// The custom-only mode measures the format's own bytes and nothing else: no
// Arrow figure, no JSON figure and no ratio, in the rows or in the table.
func TestCustomOnlyPrintsNoArrowAndNoRatio(t *testing.T) {
	rows, err := bytesgate.RunCustomOnly(ctx, small())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 5 {
		t.Fatalf("%d rows", len(rows))
	}
	docs := build(t, small())
	for _, r := range rows {
		raw, _ := wire.Encode(docs[r.Name])
		packed, _ := bytesgate.Brotli(raw)
		if r.Custom.Raw != len(raw) || r.Custom.Brotli != len(packed) {
			t.Errorf("fixture %s: sizes %+v, want %d and %d", r.Name, r.Custom, len(raw), len(packed))
		}
	}
	var out strings.Builder
	if err := bytesgate.WriteCustomTable(&out, rows); err != nil {
		t.Fatal(err)
	}
	text := strings.ToLower(out.String())
	for _, word := range []string{"arrow", "json", "ratio", "mean", "largest", "dict", "meta", "r-", " r ", "pass", "fail", "threshold"} {
		if strings.Contains(text, word) {
			t.Errorf("the custom-only table says %q:\n%s", word, out.String())
		}
	}
	if lines := strings.Count(out.String(), "\n"); lines != 2+5+5 {
		t.Errorf("the table has %d lines:\n%s", lines, out.String())
	}
	if strings.Contains(out.String(), "%") || strings.Contains(out.String(), "NaN") {
		t.Errorf("a figure that is not a size:\n%s", out.String())
	}
}

// TestReportCustomOnly prints the custom encoding's sizes for the five fixtures
// at the reference scale and nothing else. It is skipped unless
// TOPOSHIFT_BYTES_GATE_CUSTOM_ONLY is set, and it is independent of
// TOPOSHIFT_BYTES_GATE: it never writes the Arrow encoding.
func TestReportCustomOnly(t *testing.T) {
	if os.Getenv("TOPOSHIFT_BYTES_GATE_CUSTOM_ONLY") == "" {
		t.Skip("set TOPOSHIFT_BYTES_GATE_CUSTOM_ONLY=1 to measure the custom encoding of the reference fixtures")
	}
	rows, err := bytesgate.RunCustomOnly(ctx, bytesgate.Reference())
	if err != nil {
		t.Fatal(err)
	}
	if err := bytesgate.WriteCustomTable(os.Stdout, rows); err != nil {
		t.Fatal(err)
	}
}
