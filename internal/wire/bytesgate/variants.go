package bytesgate

import (
	"context"
	"fmt"
	"io"
	"slices"
	"text/tabwriter"
)

// Variant is a registered fixture, or several, with one thing changed: how fast
// its pods are replaced, whether its filler pods are mapped to services, whether
// its pods have containers, or whether its scope is still. A variant is measured
// the way a fixture is and reported beside it. It enters no ratio, decides
// nothing and carries no threshold.
type Variant struct {
	// Name is the numeral of the variant and Title what it changes.
	Name, Title string
	// Fixtures are the letters of the registered fixtures it applies to.
	Fixtures []string
	// Scale is the scale it builds them at: the registered scale with one thing
	// changed.
	Scale Scale
}

// Variants are the four sensitivity variants of a scale.
func Variants(s Scale) []Variant {
	slow := s
	slow.ReplaceEvery = 10 * s.ReplaceEvery
	mapped := s
	mapped.FillerServices = 23
	boxed := s
	boxed.Containers = true
	still := s
	still.StableScopes = true
	return []Variant{
		{"i", "churn ten times lower: a pod replaced every " + slow.ReplaceEvery.String(), []string{"a", "b", "c"}, slow},
		{"ii", "filler pods mapped to the 16 other services: 23 services on a node, the seven of the cell keeping their one pod", []string{"b", "c"}, mapped},
		{"iii", "1, 2 or 3 containers on each pod, 69.7, 24.2 and 6.1 in 100, each with its part_of edge", []string{"d", "e"}, boxed},
		{"iv", "scopes fully stable: no pod appears, disappears or flaps", []string{"d", "e"}, still},
	}
}

// VariantRow is the measurement of one fixture under one variant, with the
// registered fixture's own beside it.
type VariantRow struct {
	Variant string
	Row     Row
	// Registered is the registered fixture of the same letter.
	Registered Row
}

// RunVariants builds and measures every fixture of every variant of a scale. rep
// holds the registered fixtures, measured at the same scale.
func RunVariants(ctx context.Context, s Scale, rep Report) ([]VariantRow, error) {
	var out []VariantRow
	for _, v := range Variants(s) {
		for _, f := range Fixtures(v.Scale) {
			if !slices.Contains(v.Fixtures, f.Name) {
				continue
			}
			i := slices.IndexFunc(rep.Rows, func(r Row) bool { return r.Name == f.Name })
			if i < 0 {
				return nil, fmt.Errorf("bytesgate: variant %s: no registered fixture %s in the report", v.Name, f.Name)
			}
			row, err := Measure(ctx, f)
			if err != nil {
				return nil, fmt.Errorf("bytesgate: variant %s: %w", v.Name, err)
			}
			out = append(out, VariantRow{Variant: v.Name, Row: row, Registered: rep.Rows[i]})
		}
	}
	return out, nil
}

// WriteVariants writes the block of the report that holds the variants: for each,
// the shape and sizes of its document and its r, with the registered fixture's r
// beside it. Nothing is computed over the variants and nothing is said of them.
func WriteVariants(w io.Writer, s Scale, rows []VariantRow) error {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "sensitivity variants (informational)")
	fmt.Fprintln(w, "each is a registered fixture with one thing changed; r is the same ratio as above, with the registered fixture's beside it")
	fmt.Fprintln(w, "nothing is computed over the variants")
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', tabwriter.AlignRight)
	fmt.Fprintln(tw, "variant\tfixture\tentities\telements\tkeyframes\tdeltas\tcustom raw\tcustom br\tarrow raw\tarrow br\tarrow meta\tr\tr registered\t")
	var marked bool
	for _, vr := range rows {
		r := vr.Row
		fmt.Fprintf(tw, "%s\t%s\t%d\t%d\t%d\t%d\t%s\t%d\t%s\t%d\t%d\t%.4f\t%.4f\t\n",
			vr.Variant, r.Name, r.Entities, r.Elements, r.Keyframes, r.Deltas,
			size(r.Custom.Raw, &marked), r.Custom.Brotli, size(r.Arrow.Raw, &marked), r.Arrow.Brotli, r.ArrowMeta,
			r.Ratio(), vr.Registered.Ratio())
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	if marked {
		fmt.Fprintf(w, "* more than %d bytes, the size of the brotli window\n", brotliWindow)
	}
	for _, v := range Variants(s) {
		if _, err := fmt.Fprintf(w, "%s (fixtures %v): %s\n", v.Name, v.Fixtures, v.Title); err != nil {
			return err
		}
	}
	return nil
}
