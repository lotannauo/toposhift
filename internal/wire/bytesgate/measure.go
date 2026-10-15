package bytesgate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"runtime/debug"
	"text/tabwriter"

	"github.com/andybalholm/brotli"

	"github.com/lotannauo/toposhift/internal/wire"
)

// The brotli settings of the gate: the highest quality, and a window of 2^22
// bytes, which holds a whole fixture's first megabytes.
const (
	brotliQuality = 11
	brotliLGWin   = 22
	brotliWindow  = 1 << brotliLGWin
)

// Brotli compresses b at the gate's settings.
func Brotli(b []byte) ([]byte, error) {
	var out bytes.Buffer
	w := brotli.NewWriterOptions(&out, brotli.WriterOptions{Quality: brotliQuality, LGWin: brotliLGWin})
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

// Unbrotli is the inverse of [Brotli].
func Unbrotli(b []byte) ([]byte, error) { return io.ReadAll(brotli.NewReader(bytes.NewReader(b))) }

// Sizes are the bytes of one encoding of a document, as written and after
// brotli.
type Sizes struct{ Raw, Brotli int }

// Row is the measurement of one fixture.
type Row struct {
	Name, Title string
	// The shape of the document: its entities, its elements, its keyframes and
	// its deltas.
	Entities, Elements, Keyframes, Deltas int
	// Custom is the wire format, Arrow the Arrow encoding the gate decides on, and
	// JSON the canonical twin. ArrowDict is Arrow with a dictionary of entities in
	// each stream in place of indices: it is reported for what it shows and is
	// not what r is made of. ArrowMeta is the bytes of Arrow that are not the
	// bodies of its messages: its schemas, headers and end-of-stream markers.
	Custom, Arrow, ArrowDict, JSON Sizes
	ArrowMeta                      int
}

// Ratio is the Arrow bytes over the custom bytes, both after brotli, over the
// whole response.
func (r Row) Ratio() float64 { return float64(r.Arrow.Brotli) / float64(r.Custom.Brotli) }

// RatioDict is the same ratio for ArrowDict. It is informational.
func (r Row) RatioDict() float64 { return float64(r.ArrowDict.Brotli) / float64(r.Custom.Brotli) }

// Measure builds a fixture and measures its three encodings.
func Measure(ctx context.Context, f Fixture) (Row, error) {
	d, err := f.Build(ctx)
	if err != nil {
		return Row{}, err
	}
	return MeasureDocument(f.Name, f.Title, d)
}

// MeasureDocument measures the three encodings of a document.
func MeasureDocument(name, title string, d wire.Document) (Row, error) {
	row := Row{
		Name: name, Title: title, Entities: len(d.Subgraph.Entities), Elements: len(d.Subgraph.Elements),
		Keyframes: len(d.Subgraph.Keyframes), Deltas: len(d.Subgraph.Deltas),
	}
	custom, err := wire.Encode(d)
	if err != nil {
		return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
	}
	arrowBytes, err := Arrow(d)
	if err != nil {
		return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
	}
	if row.ArrowMeta, err = Metadata(arrowBytes); err != nil {
		return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
	}
	dictBytes, err := ArrowDictionaries(d)
	if err != nil {
		return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
	}
	twin, err := wire.JSON(d)
	if err != nil {
		return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
	}
	for _, e := range []struct {
		raw []byte
		to  *Sizes
	}{{custom, &row.Custom}, {arrowBytes, &row.Arrow}, {dictBytes, &row.ArrowDict}, {twin, &row.JSON}} {
		packed, err := Brotli(e.raw)
		if err != nil {
			return Row{}, fmt.Errorf("bytesgate: fixture %s: %w", name, err)
		}
		*e.to = Sizes{Raw: len(e.raw), Brotli: len(packed)}
	}
	return row, nil
}

// Report is the measurement of a set of fixtures.
type Report struct{ Rows []Row }

// Run builds and measures every fixture of a scale, in order.
func Run(ctx context.Context, s Scale) (Report, error) {
	var rep Report
	for _, f := range Fixtures(s) {
		row, err := Measure(ctx, f)
		if err != nil {
			return Report{}, err
		}
		rep.Rows = append(rep.Rows, row)
	}
	return rep, nil
}

// GeoMean is the geometric mean of the fixtures' ratios, NaN with no fixture.
func (r Report) GeoMean() float64 {
	if len(r.Rows) == 0 {
		return math.NaN()
	}
	var sum float64
	for _, row := range r.Rows {
		sum += math.Log(row.Ratio())
	}
	return math.Exp(sum / float64(len(r.Rows)))
}

// Largest is the fixture with the largest ratio, and its ratio. It is the zero
// Row and NaN with no fixture.
func (r Report) Largest() (Row, float64) {
	if len(r.Rows) == 0 {
		return Row{}, math.NaN()
	}
	best := r.Rows[0]
	for _, row := range r.Rows[1:] {
		if row.Ratio() > best.Ratio() {
			best = row
		}
	}
	return best, best.Ratio()
}

// versions are the modules whose bytes the numbers depend on, as this binary was
// built with them.
func versions() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "module versions unavailable"
	}
	var out string
	for _, d := range info.Deps {
		switch d.Path {
		case "github.com/andybalholm/brotli", "github.com/apache/arrow-go/v18":
			out += fmt.Sprintf(" %s %s", d.Path, d.Version)
		}
	}
	return "modules:" + out
}

// size is a number of bytes for a table, marked when it is more than the
// brotli window, which then cannot see all of it at once.
func size(n int, marked *bool) string {
	if n > brotliWindow {
		*marked = true
		return fmt.Sprintf("%d*", n)
	}
	return fmt.Sprint(n)
}

// WriteTable writes the report as a table: the shape and the sizes of each
// fixture, the ratio, and over the fixtures the geometric mean of the ratio and
// the largest one. Sizes are in bytes. It states what was measured and nothing
// about what the numbers mean.
func (r Report) WriteTable(w io.Writer) error {
	fmt.Fprintf(w, "brotli quality %d, window 2^%d; %s\n", brotliQuality, brotliLGWin, versions())
	fmt.Fprintln(w, "r is the Arrow bytes over the custom bytes, both after brotli, for the whole response")
	fmt.Fprintln(w, "arrow refers to entities by index; arrow-dict gives each stream a dictionary of the entities it names, and r-dict is informational")
	fmt.Fprintln(w, "arrow meta is the bytes of arrow that are not the bodies of its messages, in the raw size")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "fixture\tentities\telements\tkeyframes\tdeltas\tcustom raw\tcustom br\tarrow raw\tarrow br\tarrow meta\tarrow-dict raw\tarrow-dict br\tjson raw\tjson br\tr\tr-dict\t")
	var marked bool
	for _, row := range r.Rows {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%s\t%d\t%s\t%d\t%d\t%s\t%d\t%s\t%d\t%.4f\t%.4f\t\n",
			row.Name, row.Entities, row.Elements, row.Keyframes, row.Deltas,
			size(row.Custom.Raw, &marked), row.Custom.Brotli, size(row.Arrow.Raw, &marked), row.Arrow.Brotli, row.ArrowMeta,
			size(row.ArrowDict.Raw, &marked), row.ArrowDict.Brotli, size(row.JSON.Raw, &marked), row.JSON.Brotli,
			row.Ratio(), row.RatioDict())
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if marked {
		fmt.Fprintf(w, "* more than %d bytes, the size of the brotli window\n", brotliWindow)
	}
	for _, row := range r.Rows {
		fmt.Fprintf(w, "%s: %s\n", row.Name, row.Title)
	}
	fmt.Fprintf(w, "geometric mean of r: %.4f\n", r.GeoMean())
	big, ratio := r.Largest()
	_, err := fmt.Fprintf(w, "largest r: %.4f (fixture %s)\n", ratio, big.Name)
	return err
}
