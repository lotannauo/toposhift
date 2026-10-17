package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
)

// fixtureEpoch is the time zero of testdata/fixture.tsv.
var fixtureEpoch = time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)

// fixture is testdata/fixture.tsv as an activity file in a test directory.
type fixture struct {
	path    string
	records []store.Record
	samples int64
}

// fixtureRecords reads testdata/fixture.tsv: see the comment at its top for the form.
// The records are ordered by event time, then by line, and numbered from 1 in steps
// of 3.
func fixtureRecords(tb testing.TB) []store.Record {
	tb.Helper()
	f, err := os.Open(filepath.Join("testdata", "fixture.tsv"))
	if err != nil {
		tb.Fatal(err)
	}
	defer f.Close()
	res := identity.NewResolver(catalog.Default())
	fp := func(typ catalog.EntityType, key catalog.AttributeKey, name string) identity.Fingerprint {
		id, err := res.Resolve(typ, []identity.Attr{{Key: key, Value: name}})
		if err != nil {
			tb.Fatal(err)
		}
		return id.Fingerprint()
	}
	type line struct {
		at  time.Time
		idx int
		rec store.Record
	}
	var lines []line
	sc := bufio.NewScanner(f)
	for n := 0; sc.Scan(); n++ {
		text := strings.TrimSpace(sc.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		c := strings.Fields(text)
		if len(c) != 9 {
			tb.Fatalf("fixture.tsv line %d has %d columns, want 9: %q", n+1, len(c), text)
		}
		num := func(s string) int64 {
			v, err := strconv.ParseInt(s, 10, 64)
			if err != nil {
				tb.Fatalf("fixture.tsv line %d: %v", n+1, err)
			}
			return v
		}
		start, count, step, ttl := num(c[3]), num(c[4]), num(c[5]), num(c[6])
		base := store.Record{Layer: catalog.L2, Producer: "alibaba/placement", Kind: lifecycle.Observe, TTL: time.Duration(ttl) * time.Second, EventTimeBasis: store.BasisProducerEvent}
		if c[8] != "-" {
			base.Payload = []byte(c[8])
		}
		switch c[0] {
		case "pod":
			base.Subject = store.EntitySubject(fp(catalog.K8sPod, catalog.K8sPodUID, c[1]))
		case "node":
			base.Subject = store.EntitySubject(fp(catalog.K8sNode, catalog.K8sNodeUID, c[1]))
		case "edge":
			base.Subject = store.EdgeSubject(fp(catalog.K8sPod, catalog.K8sPodUID, c[1]), fp(catalog.K8sNode, catalog.K8sNodeUID, c[2]), catalog.ScheduledOn)
		default:
			tb.Fatalf("fixture.tsv line %d: kind %q is not pod, node or edge", n+1, c[0])
		}
		for i := range count {
			r := base
			r.EventTime = fixtureEpoch.Add(time.Duration(start+i*step) * time.Second)
			if i == count-1 && c[7] != "-" {
				r.Through = fixtureEpoch.Add(time.Duration(num(c[7])) * time.Second)
			}
			lines = append(lines, line{at: r.EventTime, idx: n, rec: r})
		}
	}
	if err := sc.Err(); err != nil {
		tb.Fatal(err)
	}
	slices.SortStableFunc(lines, func(a, b line) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return a.idx - b.idx
	})
	recs := make([]store.Record, len(lines))
	for i, l := range lines {
		l.rec.Seq = uint64(3*i + 1)
		if err := l.rec.Validate(); err != nil {
			tb.Fatalf("fixture record %d: %v", i, err)
		}
		recs[i] = l.rec
	}
	return recs
}

// newFixture writes the fixture's records as an activity file in several row groups.
func newFixture(tb testing.TB) fixture {
	tb.Helper()
	recs := fixtureRecords(tb)
	return fixture{path: writeActivity(tb, recs), records: recs, samples: int64(len(recs))}
}

// writeActivity writes records as an activity file of row groups of 50.
func writeActivity(tb testing.TB, recs []store.Record) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "in.parquet")
	f, err := os.Create(path)
	if err != nil {
		tb.Fatal(err)
	}
	w, err := activity.NewWriter(f, activity.WriterOptions{RowGroupRows: 50})
	if err != nil {
		tb.Fatal(err)
	}
	for _, r := range recs {
		if err := w.Write(r); err != nil {
			tb.Fatalf("writing seq %d: %v", r.Seq, err)
		}
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	if err := f.Close(); err != nil {
		tb.Fatal(err)
	}
	return path
}

// TestFixtureHasWhatTheTestsNeed shows the fixture is the shape the loader's tests
// rely on, so that a test that passes is not passing on a fixture that lost it.
func TestFixtureHasWhatTheTestsNeed(t *testing.T) {
	recs := fixtureRecords(t)
	edgePrefix := map[string]int{}   // the records stored under each edge prefix
	entityPrefix := map[string]int{} // and under each entity prefix
	var through, edges int
	for _, r := range recs {
		if !r.Through.IsZero() {
			through++
		}
		if s := r.Subject; s.Kind == store.SubjectEdge {
			edges++
			edgePrefix[fmt.Sprintf("fwd %x", s.A)]++
			edgePrefix[fmt.Sprintf("rev %x", s.B)]++
		} else {
			entityPrefix[fmt.Sprintf("%x", r.Subject.A)]++
		}
	}
	if n := maxOf(edgePrefix); n <= 64 {
		t.Errorf("the busiest edge prefix holds %d records, want more than 64 (the default checkpoint policy's K_min)", n)
	}
	if n := maxOf(entityPrefix); n <= 64 {
		t.Errorf("the busiest entity prefix holds %d records, want more than 64, so that a checkpoint on an entity prefix can show", n)
	}
	if through != 2 {
		t.Errorf("%d records carry a Through, want 2 (one pod and one edge)", through)
	}
	if edges == 0 || len(recs) < 300 {
		t.Errorf("%d records, %d of edges: the fixture is too small", len(recs), edges)
	}
}

func maxOf(m map[string]int) (best int) {
	for _, n := range m {
		best = max(best, n)
	}
	return best
}
