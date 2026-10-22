package pgmodel_test

import (
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/bench/postgres/pgmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

func TestTheTwoTriggersAreAutovacuumsFormulas(t *testing.T) {
	for _, c := range []struct {
		dead, rows int64
		want       bool
	}{
		{50, 0, false}, {51, 0, true}, // 50 + 0.01 * 0
		{150, 10_000, false}, {151, 10_000, true}, // 50 + 0.01 * 10,000
		{0, 0, false}, {51, -1, true}, // a table never vacuumed has no rows to scale by
	} {
		if got := pgmodel.VacuumDead(c.dead, c.rows); got != c.want {
			t.Errorf("%d dead rows of %d: %v, want %v", c.dead, c.rows, got, c.want)
		}
	}
	for _, c := range []struct {
		inserted, rows int64
		want           bool
	}{
		{1000, 0, false}, {1001, 0, true}, // 1000 + 0.05 * 0
		{1500, 10_000, false}, {1501, 10_000, true}, // 1000 + 0.05 * 10,000
	} {
		if got := pgmodel.VacuumInserted(c.inserted, c.rows); got != c.want {
			t.Errorf("%d inserted rows of %d: %v, want %v", c.inserted, c.rows, got, c.want)
		}
	}
}

func TestTheAnalyzeTriggerIsAutovacuumsFormula(t *testing.T) {
	for _, c := range []struct {
		changed, rows int64
		want          bool
	}{
		{50, 0, false}, {51, 0, true}, // 50 + 0.1 * 0
		{150, 1000, false}, {151, 1000, true}, // 50 + 0.1 * 1,000
		{51, -1, true}, // a table never vacuumed or analyzed has no rows to scale by
	} {
		if got := pgmodel.AnalyzeChanged(c.changed, c.rows); got != c.want {
			t.Errorf("%d changed rows of %d: %v, want %v", c.changed, c.rows, got, c.want)
		}
	}
}

// actions flattens what a schedule calls for into "table", "table+analyze" and
// "table analyze only" strings.
func actions(as []pgmodel.Action) []string {
	var out []string
	for _, a := range as {
		switch {
		case a.Vacuum && a.Analyze:
			out = append(out, a.Table+" vacuum+analyze")
		case a.Vacuum:
			out = append(out, a.Table+" vacuum")
		default:
			out = append(out, a.Table+" analyze")
		}
	}
	return out
}

func TestAScheduleCountsUpdatesAsDeadRowsAndInsertsOnTheirOwn(t *testing.T) {
	v := pgmodel.NewVacuumSchedule()
	// An update leaves a dead row, as a delete does; an insert does not. 61 changed rows
	// are also past the analyze trigger, so the table gets both in one command.
	v.Add("edges", 10, 30, 21)
	if due := actions(v.Due()); !slices.Equal(due, []string{"edges vacuum+analyze"}) {
		t.Fatalf("51 dead rows: %v", due)
	}
	// Done: the counts start again.
	if due := v.Due(); len(due) != 0 {
		t.Fatalf("a table just vacuumed is due again: %v", due)
	}
	// Inserts alone pass the second trigger, and not the first; they pass the analyze
	// trigger long before it.
	v.Add("log", 60, 0, 0)
	if due := actions(v.Due()); !slices.Equal(due, []string{"log analyze"}) {
		t.Fatalf("60 inserts: %v", due)
	}
	// On a large table the inserted-rows trigger (1000 plus 5 %) is passed before the
	// analyze trigger (50 plus 10 % of the rows changed), and a vacuum comes alone.
	v.Restore("big", 100_000)
	v.Add("big", 6000, 0, 0)
	if due := v.Due(); len(due) != 0 {
		t.Fatalf("6,000 inserts into 100,000: %v", actions(due))
	}
	v.Add("big", 1, 0, 0)
	if due := actions(v.Due()); !slices.Equal(due, []string{"big vacuum"}) {
		t.Fatalf("6,001 inserts into 100,000: %v", due)
	}
	// A dropped table is forgotten, and several that are due come out in name order.
	v.Add("log_p19700102", 5000, 0, 0)
	v.Drop("log_p19700102")
	v.Add("entities", 0, 60, 0)
	v.Add("edges", 0, 0, 60)
	if due := actions(v.Due()); !slices.Equal(due, []string{"edges vacuum+analyze", "entities vacuum+analyze"}) {
		t.Fatalf("%v", due)
	}
	runs, analyzes := v.Runs(), v.AnalyzeRuns()
	if runs["edges"] != 2 || runs["entities"] != 1 || runs["big"] != 1 || analyzes["edges"] != 2 || analyzes["entities"] != 1 || analyzes["log"] != 1 || analyzes["big"] != 0 {
		t.Fatalf("vacuums %v, analyzes %v", runs, analyzes)
	}
}

// TestAnAnalyzeIsDueByChangedRowsWhateverTheDeadRows: a table whose rows are rewritten
// in place has dead rows and changed rows alike, and one that only grows has changed rows
// alone, and either way the planner gets statistics once the rows changed pass 10 % of the
// table and the threshold.
func TestAnAnalyzeIsDueByChangedRowsWhateverTheDeadRows(t *testing.T) {
	v := pgmodel.NewVacuumSchedule()
	v.Restore("edges", 10_000)
	v.Add("edges", 1000, 0, 0)
	if due := v.Due(); len(due) != 0 {
		t.Fatalf("1,000 changed rows of 10,000: %v", due)
	}
	v.Add("edges", 51, 0, 0)
	if due := actions(v.Due()); !slices.Equal(due, []string{"edges analyze"}) {
		t.Fatalf("1,051 changed rows of 10,000: %v", due)
	}
}

// TestAModelVacuumsWhenItsTablesPassTheFormulas: a stream of refreshes updates the rows
// of one table until it is due, and writes enough log to pass the insert trigger of each
// day's partition; a retention that drops a partition stops counting it.
func TestAModelVacuumsWhenItsTablesPassTheFormulas(t *testing.T) {
	m := pgmodel.New()
	day := int64(24 * time.Hour / time.Second)
	var seq uint64
	// 1,100 refreshes of one edge on one day: 1,100 inserts into the log of that day and
	// 1,099 updates of one row, which leaves 51 dead rows (more than 50) 21 times.
	for i := range int64(1100) {
		seq++
		r := observe(seq, "a", 10+i, 100)
		if err := m.Write([]engine.Record{r}); err != nil {
			t.Fatal(err)
		}
	}
	runs := m.VacuumRuns()
	if m.Counts().Analyzes == 0 || m.AnalyzeRuns()["edges"] == 0 || m.AnalyzeRuns()["log_p19700101"] == 0 {
		t.Fatalf("analyzes after 1,100 refreshes: %v", m.AnalyzeRuns())
	}
	if runs["edges"] != 21 || runs["log_p19700101"] != 1 || len(runs) != 2 {
		t.Fatalf("vacuums after 1,100 refreshes: %v", runs)
	}
	if got := m.Counts().Vacuums; got != 22 {
		t.Fatalf("%d vacuums counted", got)
	}
	// Records of the next day, a retention to the day after it, and the first day's
	// partition is dropped.
	seq++
	if err := m.Write([]engine.Record{observe(seq, "a", day+10, 100)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Retain(at(day + 20)); err != nil {
		t.Fatal(err)
	}
	if got := m.Counts().PartitionsDropped; got != 1 {
		t.Fatalf("%d partitions dropped, want 1", got)
	}
}
