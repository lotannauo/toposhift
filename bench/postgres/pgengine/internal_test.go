package pgengine

import (
	"errors"
	"math"
	"regexp"
	"strconv"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// TestSequenceNumbersKeepTheirOrderAsBigints: a uint64 and its stored bigint order
// alike, across 2^63, which is what a comparison with a snapshot token in SQL needs.
func TestSequenceNumbersKeepTheirOrderAsBigints(t *testing.T) {
	seqs := []uint64{0, 1, 2, 1<<32 - 1, 1 << 32, math.MaxInt64 - 1, math.MaxInt64, 1 << 63, 1<<63 + 1, math.MaxUint64 - 1, math.MaxUint64}
	for i, a := range seqs {
		if got := seqFromDB(seqToDB(a)); got != a {
			t.Errorf("%d comes back as %d", a, got)
		}
		for _, b := range seqs[i+1:] {
			if seqToDB(a) >= seqToDB(b) {
				t.Errorf("%d is stored as %d, which is not below %d's %d", a, seqToDB(a), b, seqToDB(b))
			}
		}
	}
	if seqToDB(engine.Latest) != math.MaxInt64 {
		t.Error("the latest token must be the largest bigint, which sees every record")
	}
}

// TestAnEmptyRangeIsNotStored: an interval with no length would be an empty range,
// which has no lower bound for the start column's check to compare, so it would pass.
func TestAnEmptyRangeIsNotStored(t *testing.T) {
	var c rowColumns
	c.add(2, pgmodel.Final(5, 5, lifecycle.EndProducer))
	if c.err == nil {
		t.Error("a row with no length was accepted")
	}
	c = rowColumns{}
	c.add(2, pgmodel.Final(5, 6, lifecycle.EndProducer))
	c.add(2, pgmodel.OpenRow(7))
	c.add(2, pgmodel.Provisional(9, 20, lifecycle.EndLivenessExpiry))
	if c.err != nil || len(c.end) != 3 || *c.end[0] != 6 || c.end[1] != nil || c.end[2] != nil || c.deadline[0] != nil || c.deadline[1] != nil || *c.deadline[2] != 20 {
		t.Errorf("ends %v deadlines %v: %v", c.end, c.deadline, c.err)
	}
}

func TestEachArmReadsWhatItsIndexesAnswer(t *testing.T) {
	last := int64(math.MaxInt64)
	for _, c := range []struct {
		arm  pgschema.Arm
		t    int64
		want string
	}{
		{pgschema.Stream, 5, "during @> $3::bigint AND (deadline_ns IS NULL OR $3::bigint < deadline_ns)"},
		{pgschema.StreamNoPartition, last, "during @> $3::bigint AND (deadline_ns IS NULL OR $3::bigint < deadline_ns)"},
		{pgschema.Bulk, 5, "during @> $3::bigint"},
		{pgschema.BulkB, 5, "coalesce(upper(during), 9223372036854775807) > $3::bigint AND lower(during) <= $3::bigint"},
		// Nothing ends after the last instant; only an open row contains it.
		{pgschema.BulkB, last, "upper_inf(during) AND lower(during) <= $3::bigint"},
	} {
		lay, err := newLayout(c.arm)
		if err != nil {
			t.Fatal(err)
		}
		if got := lay.live(c.t); got != c.want {
			t.Errorf("%s at %d: %q, want %q", c.arm, c.t, got, c.want)
		}
		for _, sql := range []string{lay.neighborsSQL(engine.Forward, c.t), lay.batchSQL(engine.Reverse, c.t), lay.aliveSQL(c.t)} {
			if !strings.Contains(sql, c.want) {
				t.Errorf("%s: %q lacks the arm's condition", c.arm, sql)
			}
		}
	}
	// The rows are read in the order of the contract, with no collation: bytea, then a
	// text column that is collated "C".
	lay, _ := newLayout(pgschema.Stream)
	if sql := lay.neighborsSQL(engine.Reverse, 1); !strings.Contains(sql, "SELECT src, rel FROM edges WHERE dst = $1") || !strings.HasSuffix(sql, "ORDER BY src, rel") {
		t.Errorf("a reverse read: %s", sql)
	}
}

func TestArmsThatHaveNoEngineAreRefused(t *testing.T) {
	for _, arm := range []pgschema.Arm{pgschema.BulkID, "pg-nothing", ""} {
		if _, err := newLayout(arm); !errors.Is(err, engine.ErrInvalid) {
			t.Errorf("arm %q: %v", arm, err)
		}
	}
	for _, arm := range []pgschema.Arm{pgschema.Stream, pgschema.StreamNoPartition, pgschema.StreamNaive, pgschema.Bulk, pgschema.BulkB} {
		if _, err := newLayout(arm); err != nil {
			t.Errorf("arm %q: %v", arm, err)
		}
	}
}

func TestWindowStatementsNumberTheirParametersWithoutGaps(t *testing.T) {
	// A parameter that is not used leaves a gap the server cannot type.
	for _, unbounded := range []bool{false, true} {
		sql := windowSQL(engine.Forward, unbounded)
		used := make(map[string]bool)
		for _, m := range regexp.MustCompile(`\$(\d+)`).FindAllStringSubmatch(sql, -1) {
			used[m[1]] = true
		}
		want := map[bool]int{false: 5, true: 4}[unbounded]
		for i := 1; i <= want; i++ {
			if !used[strconv.Itoa(i)] {
				t.Errorf("unbounded %v: $%d is not used in %s", unbounded, i, sql)
			}
		}
		if len(used) != want {
			t.Errorf("unbounded %v: %d parameters in %s, want %d", unbounded, len(used), sql, want)
		}
	}
	if a, b := windowSQL(engine.Forward, false), windowSQL(engine.Reverse, false); a == b || !strings.Contains(b, "WHERE dst = $1") {
		t.Error("a reverse window must read by the target")
	}
}
