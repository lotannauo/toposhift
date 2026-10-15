package bytesgate

import (
	"context"
	"fmt"
	"io"
	"text/tabwriter"

	"github.com/lotannauo/toposhift/internal/wire"
)

// CustomRow is the measurement of one fixture's custom encoding alone.
type CustomRow struct {
	Name, Title                           string
	Entities, Elements, Keyframes, Deltas int
	Custom                                Sizes
}

// RunCustomOnly builds every fixture of a scale and measures the format's own
// bytes, raw and after brotli, and nothing else: it does not write the Arrow
// encoding or the JSON twin and forms no ratio. It is how the size of a stored
// result can be read before any comparison exists.
func RunCustomOnly(ctx context.Context, s Scale) ([]CustomRow, error) {
	var rows []CustomRow
	for _, f := range Fixtures(s) {
		d, err := f.Build(ctx)
		if err != nil {
			return nil, err
		}
		raw, err := wire.Encode(d)
		if err != nil {
			return nil, fmt.Errorf("bytesgate: fixture %s: %w", f.Name, err)
		}
		packed, err := Brotli(raw)
		if err != nil {
			return nil, fmt.Errorf("bytesgate: fixture %s: %w", f.Name, err)
		}
		rows = append(rows, CustomRow{
			Name: f.Name, Title: f.Title, Entities: len(d.Subgraph.Entities), Elements: len(d.Subgraph.Elements),
			Keyframes: len(d.Subgraph.Keyframes), Deltas: len(d.Subgraph.Deltas),
			Custom: Sizes{Raw: len(raw), Brotli: len(packed)},
		})
	}
	return rows, nil
}

// WriteCustomTable writes the rows of [RunCustomOnly]: the shape of each fixture
// and the size of its custom encoding, in bytes.
func WriteCustomTable(w io.Writer, rows []CustomRow) error {
	fmt.Fprintf(w, "custom encoding only; brotli quality %d, window 2^%d\n", brotliQuality, brotliLGWin)
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "fixture\tentities\telements\tkeyframes\tdeltas\tcustom raw\tcustom br\t")
	for _, r := range rows {
		fmt.Fprintf(tw, "%s\t%d\t%d\t%d\t%d\t%d\t%d\t\n", r.Name, r.Entities, r.Elements, r.Keyframes, r.Deltas, r.Custom.Raw, r.Custom.Brotli)
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	for _, r := range rows {
		if _, err := fmt.Fprintf(w, "%s: %s\n", r.Name, r.Title); err != nil {
			return err
		}
	}
	return nil
}
