package pgschema_test

import (
	"math"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgschema"
)

func setup(t *testing.T, a pgschema.Arm) []string {
	t.Helper()
	s, err := pgschema.Setup(a)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func all(t *testing.T, a pgschema.Arm) string {
	t.Helper()
	stmts := setup(t, a)
	idx, err := pgschema.Indexes(a)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(append(stmts, idx...), ";\n")
}

// tableColumns returns the column names of the CREATE TABLE statement of a table, in
// order, from the statements of an arm.
func tableColumns(t *testing.T, stmts []string, table string) []string {
	t.Helper()
	for _, s := range stmts {
		head := "CREATE TABLE " + table + " ("
		if !strings.HasPrefix(s, head) {
			continue
		}
		body := s[len(head):]
		depth, start := 1, 0
		var parts []string
		for i, c := range body {
			switch c {
			case '(':
				depth++
			case ')':
				depth--
				if depth == 0 {
					parts = append(parts, body[start:i])
					var names []string
					for _, p := range parts {
						f := strings.Fields(p)
						if len(f) == 0 || f[0] == "PRIMARY" || f[0] == "CHECK" {
							continue
						}
						names = append(names, f[0])
					}
					return names
				}
			case ',':
				if depth == 1 {
					parts = append(parts, body[start:i])
					start = i + 1
				}
			}
		}
	}
	t.Fatalf("no CREATE TABLE %s in %v", table, stmts)
	return nil
}

func TestStatementsHaveTheShapeOfStatements(t *testing.T) {
	for _, a := range pgschema.Arms() {
		stmts := setup(t, a)
		idx, err := pgschema.Indexes(a)
		if err != nil {
			t.Fatal(err)
		}
		for _, s := range append(stmts, idx...) {
			if !strings.HasPrefix(s, "CREATE ") || strings.Contains(s, ";") || strings.TrimSpace(s) != s {
				t.Errorf("%s: %q is not one CREATE statement without a semicolon", a, s)
			}
			if strings.Count(s, "(") != strings.Count(s, ")") || strings.Count(s, "'")%2 != 0 || strings.Count(s, `"`)%2 != 0 {
				t.Errorf("%s: %q has unbalanced brackets or quotes", a, s)
			}
			if strings.Contains(s, "%!") || strings.Contains(s, "{") {
				t.Errorf("%s: %q has an unformatted verb", a, s)
			}
		}
		if _, err := pgschema.Setup("pg-nothing"); err == nil {
			t.Error("an unknown arm was accepted")
		}
	}
}

// TestTablesHaveTheColumnsOfTheLists holds the DDL to the lists the statements that
// fill the tables use, in order.
func TestTablesHaveTheColumnsOfTheLists(t *testing.T) {
	for _, a := range pgschema.Arms() {
		stmts := setup(t, a)
		for table, cols := range map[string][]pgschema.Column{
			"log": pgschema.LogColumns, "edges": pgschema.EdgeColumns, "entities": pgschema.EntityColumns,
		} {
			if got, want := tableColumns(t, stmts, table), pgschema.Names(cols); !slices.Equal(got, want) {
				t.Errorf("%s: %s has columns %v, the list %v", a, table, got, want)
			}
		}
		if got, want := tableColumns(t, stmts, "log_base"), pgschema.Names(pgschema.LogColumns); !slices.Equal(got, want) {
			t.Errorf("%s: log_base has columns %v, the list %v", a, got, want)
		}
		if got, want := tableColumns(t, stmts, "meta"), []string{"one", "last_seq", "horizon_ns", "past_end", "fold_version", "arm"}; !slices.Equal(got, want) {
			t.Errorf("%s: meta has columns %v, want %v", a, got, want)
		}
	}
}

func TestEachArmHasWhatItIsFor(t *testing.T) {
	for _, c := range []struct {
		arm        pgschema.Arm
		has, lacks []string
	}{
		{
			pgschema.Stream,
			[]string{
				"PARTITION BY RANGE (event_ns)", "PRIMARY KEY (src, dst, rel, start_ns)", "PRIMARY KEY (fp, start_ns)", "fillfactor = 85",
				"CREATE TABLE meta", "CREATE TABLE log_base", "CREATE INDEX log_base_subject ON log_base (src, dst, rel, producer)",
				"CREATE INDEX log_e ON log (src, layer, event_ns, seq) WHERE subject_kind = 'e'",
				"CREATE INDEX log_g_subject ON log (src, dst, rel, event_ns, seq) WHERE subject_kind = 'g'",
				"CHECK (start_ns = lower(during))", "CHECK (deadline_ns IS NULL OR upper_inf(during))", "edges_end ON edges (upper(during)) WHERE NOT upper_inf(during)",
				"USING gist (src, layer, during) INCLUDE (dst, rel)", "USING gist (dst, layer, during) INCLUDE (src, rel)",
				"USING gist (fp, layer, during)", "src bytea NOT NULL", "deadline_ns bigint,\n",
			},
			[]string{"CREATE TABLE entity ", "edges_src_b"},
		},
		{
			pgschema.StreamNoPartition,
			[]string{"PRIMARY KEY (src, dst, rel, start_ns)", "CREATE TABLE log_base", "edges_end"},
			[]string{"PARTITION BY"},
		},
		{
			pgschema.Bulk,
			[]string{
				"USING gist (src, layer, during) INCLUDE (dst, rel)", "USING gist (fp, layer, during)",
				"CREATE TABLE meta", "CREATE TABLE log_base", "log_base_subject",
			},
			[]string{"PRIMARY KEY (src", "PRIMARY KEY (fp", "fillfactor", "edges_end", "entities_end", "PARTITION BY", "edges_src_b"},
		},
		{
			pgschema.BulkB,
			[]string{
				"edges_src_b ON edges (src, layer, coalesce(upper(during), 9223372036854775807)) INCLUDE (dst, rel, during)",
				"edges_dst_b ON edges (dst, layer, coalesce(upper(during), 9223372036854775807)) INCLUDE (src, rel, during)",
				"entities_b ON entities (fp, layer, coalesce(upper(during), 9223372036854775807)) INCLUDE (during)",
			},
			[]string{"USING gist", "PRIMARY KEY (src", "PRIMARY KEY (fp", "lower(during) <="},
		},
		{
			pgschema.BulkID,
			[]string{"CREATE TABLE entity (\n  id bigint PRIMARY KEY,\n  fp bytea NOT NULL UNIQUE\n)", "src bigint NOT NULL", "dst bigint NOT NULL", "fp bigint NOT NULL"},
			[]string{"src bytea", "dst bytea", "PARTITION BY"},
		},
	} {
		s := all(t, c.arm)
		for _, want := range c.has {
			if !strings.Contains(s, want) {
				t.Errorf("%s: no %q in\n%s", c.arm, want, s)
			}
		}
		for _, bad := range c.lacks {
			if strings.Contains(s, bad) {
				t.Errorf("%s: has %q", c.arm, bad)
			}
		}
	}
}

// TestIndexSetsAreBuiltWhereTheyBelong: a streaming arm has its indexes from the
// start, a bulk arm builds them after the load.
func TestIndexSetsAreBuiltWhereTheyBelong(t *testing.T) {
	for _, a := range pgschema.Arms() {
		in := strings.Join(setup(t, a), "\n")
		hasIndex := strings.Contains(in, "CREATE INDEX")
		if hasIndex != a.Streaming() {
			t.Errorf("%s: Setup has indexes = %v, streaming = %v", a, hasIndex, a.Streaming())
		}
	}
}

// TestEveryArmChecksItsRanges: the start and the deadline columns agree with the range
// in every arm, the bulk ones included.
func TestEveryArmChecksItsRanges(t *testing.T) {
	for _, a := range pgschema.Arms() {
		if n := strings.Count(all(t, a), "CHECK (start_ns = lower(during))"); n != 2 {
			t.Errorf("%s: %d start checks, want one for edges and one for entities", a, n)
		}
		if n := strings.Count(all(t, a), "CHECK (deadline_ns IS NULL OR upper_inf(during))"); n != 2 {
			t.Errorf("%s: %d deadline checks", a, n)
		}
	}
	if pgschema.RetentionPredicate != "upper(during) <= $1 OR deadline_ns <= $1" {
		t.Error(pgschema.RetentionPredicate)
	}
}

func TestExtensionsAreInPublic(t *testing.T) {
	for _, s := range pgschema.Extensions() {
		if !regexp.MustCompile(`^CREATE EXTENSION IF NOT EXISTS \w+ SCHEMA public$`).MatchString(s) {
			t.Errorf("%q", s)
		}
	}
	if !strings.Contains(strings.Join(pgschema.Extensions(), ";"), "btree_gist") {
		t.Error("GiST on bytea and smallint needs btree_gist")
	}
}

func TestSchemaName(t *testing.T) {
	a, b := pgschema.SchemaName("/tmp/x"), pgschema.SchemaName("/tmp/y")
	if a == b || a != pgschema.SchemaName("/tmp/x") || !regexp.MustCompile(`^s_[0-9a-f]{12}$`).MatchString(a) {
		t.Errorf("schema names %q %q", a, b)
	}
}

func TestDayPartition(t *testing.T) {
	day := int64(24 * time.Hour)
	for _, c := range []struct {
		name string
		ns   int64
		want string
	}{
		{"the epoch", 0, "CREATE TABLE IF NOT EXISTS log_p19700101 PARTITION OF log FOR VALUES FROM (0) TO (86400000000000)"},
		{"the last nanosecond of the first day", day - 1, "CREATE TABLE IF NOT EXISTS log_p19700101 PARTITION OF log FOR VALUES FROM (0) TO (86400000000000)"},
		{"the second day", day, "CREATE TABLE IF NOT EXISTS log_p19700102 PARTITION OF log FOR VALUES FROM (86400000000000) TO (172800000000000)"},
		{"2026-01-01", 1767225600*int64(time.Second) + 5, "CREATE TABLE IF NOT EXISTS log_p20260101 PARTITION OF log FOR VALUES FROM (1767225600000000000) TO (1767312000000000000)"},
		{"the last instant", math.MaxInt64, "CREATE TABLE IF NOT EXISTS log_p22620411 PARTITION OF log FOR VALUES FROM (9223286400000000000) TO (MAXVALUE)"},
	} {
		_, got, err := pgschema.DayPartition(c.ns)
		if err != nil || got != c.want {
			t.Errorf("%s: %q (%v), want %q", c.name, got, err, c.want)
		}
	}
	if _, _, err := pgschema.DayPartition(-1); err == nil {
		t.Error("a day before the epoch was accepted")
	}
}
