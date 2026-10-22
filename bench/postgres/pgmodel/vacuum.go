package pgmodel

import (
	"maps"
	"slices"
)

// The settings of autovacuum's three triggers, as the deterministic regime of the baseline
// registers them (see scripts/settings.conf): a table is vacuumed when its dead rows pass
// a threshold plus a fraction of its rows, and when the rows inserted since its last
// vacuum pass another; and it is analyzed when the rows changed since its last analyze
// (inserted, updated and deleted) pass a third. The server's defaults for the threshold of
// the first (50), the second (1000) and the third (50) are kept; the fractions are 1 % and
// 5 % for the vacuums and the server's default 10 % for the analyze.
const (
	VacuumThreshold         = 50
	VacuumScaleFactor       = 0.01
	VacuumInsertThreshold   = 1000
	VacuumInsertScaleFactor = 0.05
	AnalyzeThreshold        = 50
	AnalyzeScaleFactor      = 0.1
)

// AnalyzeChanged says whether a table is due an analyze by autovacuum's third trigger,
// given the rows changed since its last analyze and its rows at the time of its last
// vacuum or analyze.
func AnalyzeChanged(changed, reltuples int64) bool {
	return float64(changed) > AnalyzeThreshold+AnalyzeScaleFactor*float64(max(reltuples, 0))
}

// VacuumDead says whether a table with dead rows is due a vacuum by autovacuum's first
// trigger, given its rows at the time of its last vacuum.
func VacuumDead(dead, reltuples int64) bool {
	return float64(dead) > VacuumThreshold+VacuumScaleFactor*float64(max(reltuples, 0))
}

// VacuumInserted says whether a table is due a vacuum by autovacuum's second trigger:
// the rows inserted since its last vacuum pass the threshold.
func VacuumInserted(inserted, reltuples int64) bool {
	return float64(inserted) > VacuumInsertThreshold+VacuumInsertScaleFactor*float64(max(reltuples, 0))
}

type tableCounts struct {
	// live is what the writer counts as the table's rows; reltuples is what it was when
	// the table was last vacuumed (autovacuum reads it from the catalog), and zero before.
	live, reltuples int64
	// dead are the rows updated or deleted, and inserted the rows added, since the last
	// vacuum.
	dead, inserted int64
	// changed are the rows inserted, updated and deleted since the last analyze.
	changed int64
}

// VacuumSchedule is the vacuum schedule of the deterministic regime: autovacuum is off
// and the writer vacuums a table when its own count of the rows it has changed passes
// autovacuum's formula, so that when a table is vacuumed is a function of the stream and
// not of when a background process looked. Rows are counted the way the statistics
// collector counts them: an update leaves a dead row, as does a delete.
//
// It also schedules the analyze autovacuum would run, so that a table is read with the
// planner statistics it would have under autovacuum, and by the same rule: from the
// stream. When both a vacuum and an analyze are due the table gets one VACUUM (ANALYZE),
// as autovacuum does. ANALYZE draws its sample at random once a table has more rows than
// the sample (300 times default_statistics_target), so the statistics, and the plans that
// follow from them, can differ from one load of a stream to the next on a large table:
// the plan shapes of two loads are held to the registered rule (equal, else not
// repeatable), and the schedule itself is a function of the stream.
type VacuumSchedule struct {
	tables   map[string]*tableCounts
	runs     map[string]int
	analyzes map[string]int
}

// Action is what the schedule calls for on a table: a vacuum, an analyze, or both in one.
type Action struct {
	Table           string
	Vacuum, Analyze bool
}

// NewVacuumSchedule returns a schedule that has counted nothing.
func NewVacuumSchedule() *VacuumSchedule {
	return &VacuumSchedule{tables: make(map[string]*tableCounts), runs: make(map[string]int), analyzes: make(map[string]int)}
}

func (v *VacuumSchedule) table(name string) *tableCounts {
	t := v.tables[name]
	if t == nil {
		t = &tableCounts{}
		v.tables[name] = t
	}
	return t
}

// Add counts rows inserted, updated and deleted in a table.
func (v *VacuumSchedule) Add(table string, inserted, updated, deleted int64) {
	if inserted == 0 && updated == 0 && deleted == 0 {
		return
	}
	t := v.table(table)
	t.changed += inserted + updated + deleted
	t.inserted += inserted
	t.dead += updated + deleted
	t.live += inserted - deleted
}

// Restore sets a table's rows, as a schedule that is started over a database that has
// them is: they are the rows it has, and the rows at its last vacuum, and nothing has
// changed since.
func (v *VacuumSchedule) Restore(table string, rows int64) {
	v.tables[table] = &tableCounts{live: rows, reltuples: rows}
}

// Drop forgets a table that no longer exists, a partition that was dropped.
func (v *VacuumSchedule) Drop(table string) { delete(v.tables, table) }

// Due returns what the schedule calls for, by table name, and counts it as done: after a
// vacuum the table's dead and inserted counts start again, after an analyze its changed
// count does, and after either its rows are what it now has.
func (v *VacuumSchedule) Due() []Action {
	var out []Action
	for _, name := range slices.Sorted(maps.Keys(v.tables)) {
		t := v.tables[name]
		a := Action{
			Table:   name,
			Vacuum:  VacuumDead(t.dead, t.reltuples) || VacuumInserted(t.inserted, t.reltuples),
			Analyze: AnalyzeChanged(t.changed, t.reltuples),
		}
		if !a.Vacuum && !a.Analyze {
			continue
		}
		if a.Vacuum {
			t.dead, t.inserted = 0, 0
			v.runs[name]++
		}
		if a.Analyze {
			t.changed = 0
			v.analyzes[name]++
		}
		t.reltuples = max(t.live, 0)
		out = append(out, a)
	}
	return out
}

// Runs is how many times each table has been vacuumed.
func (v *VacuumSchedule) Runs() map[string]int { return maps.Clone(v.runs) }

// AnalyzeRuns is how many times each table has been analyzed, alone or with a vacuum.
func (v *VacuumSchedule) AnalyzeRuns() map[string]int { return maps.Clone(v.analyzes) }

// Total is how many vacuums the schedule has called for.
func (v *VacuumSchedule) Total() int {
	n := 0
	for _, r := range v.runs {
		n += r
	}
	return n
}
