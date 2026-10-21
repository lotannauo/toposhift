package doltmodel_test

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

var timeType = regexp.MustCompile(`(?i)\b(DATETIME|TIMESTAMP|DATE|TIME|YEAR)\b`)

// columnOf names the column a row type's field is stored in.
func columnOf(field string) string {
	switch field {
	case "TTLNs":
		return "ttl_ns"
	}
	var sb strings.Builder
	for i, r := range field {
		if i > 0 && r >= 'A' && r <= 'Z' {
			sb.WriteByte('_')
		}
		sb.WriteRune(r)
	}
	return strings.ToLower(sb.String())
}

// flags are the fields that say a nullable column is NULL, not columns of their own.
var flags = map[string]string{"HasThrough": "through_ns", "Open": "deadline_ns"}

// checkSchema is what the tables must be: each row type's fields are columns of its table,
// the keys are the design's, every time is a BIGINT and none a DATETIME, only the two columns
// that can be absent are nullable, and the entity tables hold the layer as a value, outside
// the key, where the edge tables key on it.
func checkSchema(tables []doltmodel.Table) error {
	by := map[string]doltmodel.Table{}
	for _, t := range tables {
		by[t.Name] = t
		if timeType.MatchString(t.DDL()) {
			return fmt.Errorf("table %s has a time type: %s", t.Name, t.DDL())
		}
		for _, c := range t.Columns {
			if strings.HasSuffix(c.Name, "_ns") && c.Type != "BIGINT" {
				return fmt.Errorf("%s.%s is a %s, a time is a BIGINT of nanoseconds", t.Name, c.Name, c.Type)
			}
			if c.Nullable && c.Name != "through_ns" && c.Name != "deadline_ns" {
				return fmt.Errorf("%s.%s is nullable", t.Name, c.Name)
			}
		}
	}
	for table, row := range map[string]reflect.Type{
		"log": reflect.TypeFor[doltmodel.LogRow](), "ent_log": reflect.TypeFor[doltmodel.EntLogRow](),
		"state": reflect.TypeFor[doltmodel.StateRow](), "ent_state": reflect.TypeFor[doltmodel.EntStateRow](),
	} {
		t, ok := by[table]
		if !ok {
			return fmt.Errorf("no table %s", table)
		}
		var want []string
		for i := range row.NumField() {
			f := row.Field(i).Name
			if col, isFlag := flags[f]; isFlag {
				if !slices.ContainsFunc(t.Columns, func(c doltmodel.Column) bool { return c.Name == col && c.Nullable }) {
					return fmt.Errorf("%s: %s is a flag for the nullable column %s, which is missing or not nullable", table, f, col)
				}
				continue
			}
			want = append(want, columnOf(f))
		}
		var got []string
		for _, c := range t.Columns {
			got = append(got, c.Name)
		}
		slices.Sort(want)
		slices.Sort(got)
		if !slices.Equal(want, got) {
			return fmt.Errorf("%s: columns %v, the row type's fields %v", table, got, want)
		}
	}
	wantKey := map[string][]string{
		"log":       {"layer", "dir", "a", "peer", "rel", "producer", "event_ns", "seq"},
		"ent_log":   {"fp", "producer", "event_ns", "seq"},
		"state":     {"layer", "dir", "a", "peer", "rel", "producer"},
		"ent_state": {"fp", "producer"},
		"meta":      {"id"},
	}
	for table, key := range wantKey {
		if got := by[table].Key; !slices.Equal(got, key) {
			return fmt.Errorf("%s: key %v, want %v", table, got, key)
		}
	}
	for _, table := range []string{"ent_log", "ent_state"} {
		if !slices.ContainsFunc(by[table].Columns, func(c doltmodel.Column) bool { return c.Name == "layer" && !c.Nullable }) {
			return fmt.Errorf("%s has no layer column", table)
		}
	}
	if got := by["log"].Indexes; len(got) != 1 || got[0].Name != "log_time" || !slices.Equal(got[0].Columns, []string{"layer", "dir", "a", "event_ns", "seq"}) {
		return fmt.Errorf("log indexes %v", got)
	}
	return nil
}

func TestTheSchemaHasEveryColumnOfEveryRowTheKeysAndNoDatetime(t *testing.T) {
	if err := checkSchema(doltmodel.Schema()); err != nil {
		t.Fatal(err)
	}
	ddl := doltmodel.DDL()
	if len(ddl) != 5 || !strings.Contains(ddl[1], "layer TINYINT NOT NULL") || !strings.Contains(ddl[1], "PRIMARY KEY (fp, producer, event_ns, seq)") {
		t.Fatalf("DDL:\n%s", strings.Join(ddl, ";\n"))
	}
	if !strings.Contains(ddl[0], fmt.Sprintf("payload VARBINARY(%d) NOT NULL", doltmodel.RegisteredPayloadCapacity)) {
		t.Fatalf("the payload column is not the registered capacity:\n%s", ddl[0])
	}
}

// TestSchemaMutantsAreCaught: a layer in an entity table's key, no layer column there, a
// DATETIME for a time, a payload column left out and a column a row type does not have are
// each refused by the check.
func TestSchemaMutantsAreCaught(t *testing.T) {
	edit := func(table string, f func(*doltmodel.Table)) []doltmodel.Table {
		tables := doltmodel.Schema()
		for i := range tables {
			if tables[i].Name == table {
				tables[i].Columns = slices.Clone(tables[i].Columns)
				tables[i].Key = slices.Clone(tables[i].Key)
				f(&tables[i])
			}
		}
		return tables
	}
	drop := func(name string) func(*doltmodel.Table) {
		return func(t *doltmodel.Table) {
			t.Columns = slices.DeleteFunc(t.Columns, func(c doltmodel.Column) bool { return c.Name == name })
		}
	}
	for name, tables := range map[string][]doltmodel.Table{
		"layer in an entity state key": edit("ent_state", func(t *doltmodel.Table) { t.Key = append([]string{"layer"}, t.Key...) }),
		"no layer in the entity log":   edit("ent_log", drop("layer")),
		"an entity layer that is null": edit("ent_state", func(t *doltmodel.Table) {
			for i := range t.Columns {
				if t.Columns[i].Name == "layer" {
					t.Columns[i].Nullable = true
				}
			}
		}),
		"a datetime for a time": edit("log", func(t *doltmodel.Table) {
			for i := range t.Columns {
				if t.Columns[i].Name == "event_ns" {
					t.Columns[i].Type = "DATETIME(6)"
				}
			}
		}),
		"an int for a time": edit("state", func(t *doltmodel.Table) {
			for i := range t.Columns {
				if t.Columns[i].Name == "deadline_ns" {
					t.Columns[i].Type = "INT"
				}
			}
		}),
		"no payload":                edit("log", drop("payload")),
		"an extra column":           edit("state", func(t *doltmodel.Table) { t.Columns = append(t.Columns, doltmodel.Column{Name: "note", Type: "TEXT"}) }),
		"seq out of the log key":    edit("log", func(t *doltmodel.Table) { t.Key = t.Key[:len(t.Key)-1] }),
		"a nullable payload":        edit("log", func(t *doltmodel.Table) { t.Columns[len(t.Columns)-1].Nullable = true }),
		"no through column":         edit("ent_log", drop("through_ns")),
		"a producer-less state key": edit("state", func(t *doltmodel.Table) { t.Key = t.Key[:len(t.Key)-1] }),
	} {
		if err := checkSchema(tables); err == nil {
			t.Errorf("%s passed", name)
		}
	}
}
