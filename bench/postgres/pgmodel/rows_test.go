package pgmodel_test

import (
	"math"
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestRowReadRule pins the rule every read applies: [Start, End) half open, an open
// row never ends, and a provisional row ends at its deadline however far its range
// reaches.
func TestRowReadRule(t *testing.T) {
	for _, c := range []struct {
		name string
		row  pgmodel.Row
		in   []int64
		out  []int64
		ends bool // EndsAtOrBefore(20)
	}{
		{"final", pgmodel.Final(10, 20, lifecycle.EndProducer), []int64{10, 19}, []int64{9, 20, 21}, true},
		{"open", pgmodel.OpenRow(10), []int64{10, 20, math.MaxInt64}, []int64{9}, false},
		{"provisional", pgmodel.Provisional(10, 20, lifecycle.EndLivenessExpiry), []int64{10, 19}, []int64{9, 20, 21}, true},
		{"provisional ending later", pgmodel.Provisional(10, 21, lifecycle.EndLivenessExpiry), []int64{20}, []int64{21}, false},
	} {
		for _, ns := range c.in {
			if !c.row.Contains(ns) {
				t.Errorf("%s: does not contain %d", c.name, ns)
			}
		}
		for _, ns := range c.out {
			if c.row.Contains(ns) {
				t.Errorf("%s: contains %d", c.name, ns)
			}
		}
		if got := c.row.EndsAtOrBefore(20); got != c.ends {
			t.Errorf("%s: EndsAtOrBefore(20) = %v", c.name, got)
		}
	}
}

// TestClippedTablesDoNotDependOnHowARowSpellsItsEnd: a provisional row (unbounded
// range, deadline) and a final one with the same end are one interval once clipped, and
// a row that ends at or before the horizon is gone however it was made.
func TestClippedTablesDoNotDependOnHowARowSpellsItsEnd(t *testing.T) {
	mk := func(rows ...pgmodel.Row) pgmodel.Tables {
		var tb pgmodel.Tables
		for _, r := range rows {
			tb.Entities = append(tb.Entities, pgmodel.EntityRow{Fp: pod1, Layer: catalog.L2, Row: r})
		}
		return tb
	}
	provisional := mk(pgmodel.Provisional(10, 30, lifecycle.EndLivenessExpiry), pgmodel.Final(5, 8, lifecycle.EndProducer))
	final := mk(pgmodel.Final(10, 30, lifecycle.EndLivenessExpiry))
	if !provisional.Clipped(9).Equal(final.Clipped(9)) {
		t.Errorf("a provisional row and a final row with one end differ: %s", provisional.Clipped(9).FirstDifference(final.Clipped(9)))
	}
	if provisional.Equal(final) {
		t.Error("the unclipped tables, which hold a row more, are equal")
	}
	// Clipping moves a start that is before the horizon to it and drops a row that ends at it.
	c := mk(pgmodel.Final(5, 20, lifecycle.EndProducer), pgmodel.Final(25, 30, lifecycle.EndProducer), pgmodel.OpenRow(40)).Clipped(30)
	if len(c.Entities) != 1 || c.Entities[0].Start != 40 || !c.Entities[0].Open {
		t.Errorf("clipped at 30: %+v", c.Entities)
	}
	c = mk(pgmodel.Final(5, 20, lifecycle.EndProducer)).Clipped(10)
	if len(c.Entities) != 1 || c.Entities[0].Start != 10 || c.Entities[0].End != 20 {
		t.Errorf("clipped at 10: %+v", c.Entities)
	}
	// A row that differs in its end source is a different row.
	if mk(pgmodel.Final(10, 30, lifecycle.EndProducer)).Equal(final) {
		t.Error("end sources are not compared")
	}
}

// TestADigestWriterGivesTheDigestOfTheTablesItWasFedFrom: rows added one at a time, as
// a scan of a table too large for memory would add them, digest as the tables do, and
// with a horizon as the clipped tables do.
func TestADigestWriterGivesTheDigestOfTheTablesItWasFedFrom(t *testing.T) {
	var tb pgmodel.Tables
	tb.Edges = []pgmodel.EdgeRow{
		{Src: pod1, Dst: node1, Rel: catalog.ScheduledOn, Layer: catalog.L2, Row: pgmodel.Final(5, 20, lifecycle.EndProducer)},
		{Src: pod1, Dst: node1, Rel: catalog.ScheduledOn, Layer: catalog.L2, Row: pgmodel.Provisional(30, 50, lifecycle.EndLivenessExpiry)},
		{Src: pod1, Dst: node1, Rel: catalog.ScheduledOn, Layer: catalog.L2, Row: pgmodel.OpenRow(60)},
	}
	tb.Entities = []pgmodel.EntityRow{
		{Fp: pod1, Layer: catalog.L2, Row: pgmodel.Final(1, 9, lifecycle.EndProducer)},
		{Fp: pod1, Layer: catalog.L2, Row: pgmodel.OpenRow(12)},
	}
	for _, horizon := range []int64{math.MinInt64, 0, 10, 25, 55, 100} {
		w := pgmodel.NewDigestWriter(horizon)
		for _, e := range tb.Edges {
			w.Edge(e)
		}
		for _, e := range tb.Entities {
			w.Entity(e)
		}
		want := tb
		if horizon != math.MinInt64 {
			want = tb.Clipped(horizon)
		}
		if got := w.Sum(); got != want.Digest() {
			t.Errorf("horizon %d: digest %s, the tables' %s", horizon, got, want.Digest())
		}
		if int(w.Rows()) != want.Len() {
			t.Errorf("horizon %d: %d rows, the tables have %d", horizon, w.Rows(), want.Len())
		}
	}
	// The digest depends on the rows and their order.
	a, b := pgmodel.NewDigestWriter(math.MinInt64), pgmodel.NewDigestWriter(math.MinInt64)
	a.Edge(tb.Edges[0])
	b.Edge(tb.Edges[2])
	if a.Sum() == b.Sum() {
		t.Error("two different rows have one digest")
	}
}
