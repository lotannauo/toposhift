package xtengine

import (
	"math"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestNoTwoSegmentsHaveOneID: over every entity type and relation of the catalog, producers
// that contain the separator and producers that differ only after it, no two documents of a
// table have one `_id`, and the parts before the producer never contain the separator.
func TestNoTwoSegmentsHaveOneID(t *testing.T) {
	var fps []identity.Fingerprint
	for e := range catalog.Default().Entities() {
		for b := range byte(3) {
			fps = append(fps, fpOf(e.Type(), b))
		}
	}
	var rels []catalog.RelationType
	for r := range catalog.Default().Relations() {
		rels = append(rels, r.Type())
	}
	producers := []lifecycle.Producer{"a", "b", "ab", "a|b", "|", "||", "a||", "|a", "x|y|z", "k8s.pod:00", "a b"}
	seen := make(map[string]string)
	check := func(s engine.Subject) {
		for _, p := range producers {
			id := segID(xtmodel.Key{Table: xtmodel.SegF, Subject: s, Producer: p})
			desc := s.A.String() + " " + s.B.String() + " " + string(s.Relation) + " " + string(p)
			if prev, dup := seen[id]; dup {
				t.Fatalf("%q is the id of %q and of %q", id, prev, desc)
			}
			seen[id] = desc
		}
	}
	for _, a := range fps {
		check(engine.EntitySubject(a))
		for _, b := range fps[:6] {
			for _, r := range rels {
				check(engine.EdgeSubject(a, b, r))
			}
		}
	}
	for _, s := range []string{fps[0].String(), string(rels[0])} {
		if strings.Contains(s, "|") {
			t.Errorf("%q contains the separator", s)
		}
	}
	if len(seen) < 1000 {
		t.Errorf("only %d ids were made", len(seen))
	}
	// The two directions of an edge are two tables, so one id.
	s := engine.EdgeSubject(fps[0], fps[1], rels[0])
	if segID(xtmodel.Key{Table: xtmodel.SegF, Subject: s, Producer: "a"}) != segID(xtmodel.Key{Table: xtmodel.SegR, Subject: s, Producer: "a"}) {
		t.Error("the two directions of an edge have different ids")
	}
}

// TestLogIDsSortAsSequenceNumbers: the id of a log document sorts as its sequence number,
// to the largest.
func TestLogIDsSortAsSequenceNumbers(t *testing.T) {
	seqs := []uint64{1, 2, 9, 10, 11, 99, 100, 1 << 32, 1<<63 - 1, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64}
	ids := make([]string, len(seqs))
	for i, s := range seqs {
		ids[i] = logID(s)
		if len(ids[i]) != 20 {
			t.Errorf("the id of %d is %q", s, ids[i])
		}
	}
	if !slices.IsSorted(ids) {
		t.Errorf("ids %v do not sort as %v", ids, seqs)
	}
}

// TestASequenceNumberKeepsItsOrderInABigint: the stored form of a sequence number compares
// as the number does, and is read back as it was written.
func TestASequenceNumberKeepsItsOrderInABigint(t *testing.T) {
	seqs := []uint64{0, 1, 2, 1<<63 - 2, 1<<63 - 1, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64}
	for i, s := range seqs {
		if seqFromDB(seqToDB(s)) != s {
			t.Errorf("%d does not survive", s)
		}
		if i > 0 && seqToDB(seqs[i-1]) >= seqToDB(s) {
			t.Errorf("the stored form of %d is not above that of %d", s, seqs[i-1])
		}
	}
	// The newest token sees every record: it is the largest bigint.
	if seqToDB(engine.Latest) != math.MaxInt64 {
		t.Errorf("the stored form of the newest token is %d", seqToDB(engine.Latest))
	}
}

func TestTablePrefixes(t *testing.T) {
	n, err := namesFor("")
	if err != nil {
		t.Fatal(err)
	}
	want := tableNames{"seg_f", "seg_r", "ent", "log_f", "log_r", "log_e", "meta"}
	if n != want {
		t.Errorf("names %v, want %v", n, want)
	}
	n, err = namesFor("t1_")
	if err != nil || n.of(xtmodel.SegR) != "t1_seg_r" || n.of(xtmodel.Meta) != "t1_meta" {
		t.Errorf("prefixed names %v, %v", n, err)
	}
	for _, bad := range []string{"T1", "1t", "t-1", "t 1", "t;drop", `t"`, strings.Repeat("a", 32), "é"} {
		if _, err := namesFor(bad); err == nil {
			t.Errorf("the prefix %q was accepted", bad)
		}
	}
	if len(want) != xtmodel.NumTables {
		t.Errorf("%d names for %d tables", len(want), xtmodel.NumTables)
	}
}

// TestPlaceholdersCastWhatTheSimpleProtocolQuotes: the driver's simple protocol writes every
// argument as a quoted string, so an integer is cast to a bigint and bytes to a binary value,
// and a string, a timestamp and a null are as they are.
func TestPlaceholdersCastWhatTheSimpleProtocolQuotes(t *testing.T) {
	var q sqlText
	ts := time.Unix(10, 0).UTC()
	q.s("x ").p(int64(5)).s(" ").p("s").s(" ").p(hexOf([]byte{0, 255})).s(" ").p(ts).s(" ").p(nil)
	if want := "x CAST($1 AS BIGINT) $2 CAST($3 AS VARBINARY) $4 $5"; q.b.String() != want {
		t.Errorf("the statement is %q, want %q", q.b.String(), want)
	}
	if want := []any{int64(5), "s", "00ff", ts, nil}; !reflect.DeepEqual(q.args, want) {
		t.Errorf("the arguments are %v, want %v", q.args, want)
	}
}
