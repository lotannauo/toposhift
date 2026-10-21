package pgengine_test

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
)

// Autovacuum vacuums a table whose dead rows pass 50 plus 1 % of its live rows.
func TestVacuumIsDueByAutovacuumsFormula(t *testing.T) {
	for _, c := range []struct {
		dead, live int64
		due        bool
	}{
		{0, 0, false},
		{50, 0, false},
		{51, 0, true},
		{150, 10_000, false},
		{151, 10_000, true},
		{10_050, 1_000_000, false},
		{10_051, 1_000_000, true},
		{51, -5, true}, // a count that went below zero is none
	} {
		if got := pgengine.VacuumDue(c.dead, c.live); got != c.due {
			t.Errorf("%d dead of %d live: %v", c.dead, c.live, got)
		}
	}
}

func TestTheProcessorIsNamedFromWhatTheHostReports(t *testing.T) {
	const x86 = "processor\t: 0\nvendor_id\t: GenuineIntel\nmodel name\t: Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz\ncpu MHz\t\t: 2793\n\nprocessor\t: 1\nmodel name\t: Another\n"
	const arm = "processor\t: 0\nBogoMIPS\t: 48.00\nCPU implementer\t: 0x61\nCPU architecture: 8\nCPU variant\t: 0x0\nCPU part\t: 0x000\n\nprocessor\t: 1\nCPU part\t: 0x111\n"
	for text, want := range map[string]string{
		x86:                        "Intel(R) Xeon(R) Platinum 8370C CPU @ 2.80GHz",
		arm:                        "CPU implementer 0x61, CPU architecture 8, CPU variant 0x0, CPU part 0x000",
		"":                         "unknown",
		"processor\t: 0\nflags: x": "unknown",
	} {
		if got := pgengine.ParseCPUInfo(text); got != want {
			t.Errorf("%q: %q, want %q", text, got, want)
		}
	}
}

func TestTheImageIsReadFromTheScriptThatStartsTheServer(t *testing.T) {
	got, err := pgengine.PinnedImage("../scripts/pg-server.sh")
	if err != nil || !strings.HasPrefix(got, "postgres:17.11-trixie@sha256:") {
		t.Errorf("%q, %v", got, err)
	}
	dir := t.TempDir()
	for name, text := range map[string]string{
		"a tag":    "image='postgres:17.11'\n",
		"no image": "echo hello\n",
	} {
		path := dir + "/" + strings.ReplaceAll(name, " ", "-") + ".sh"
		if err := writeFile(path, text); err != nil {
			t.Fatal(err)
		}
		if _, err := pgengine.PinnedImage(path); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := pgengine.PinnedImage(dir + "/missing.sh"); err == nil {
		t.Error("a script that is not there")
	}
	path := dir + "/none.sh"
	if err := writeFile(path, "echo\n"); err != nil {
		t.Fatal(err)
	}
	if _, err := pgengine.PinnedImage(path); !errors.Is(err, pgengine.ErrNoImage) {
		t.Errorf("no image: %v", err)
	}
}

func TestOnlyTheFoldOfTheLogIsAFoldStatement(t *testing.T) {
	for sql, want := range map[string]bool{
		"SELECT peer, rel FROM log WHERE a UNION ALL SELECT peer, rel FROM log_base WHERE a ORDER BY 1, 2": true,
		"SELECT seq FROM log WHERE a UNION ALL\nSELECT seq FROM log_base WHERE a":                          true,
		"SELECT dst, rel FROM edges WHERE src = $1":                                                        false,
		"SELECT seq, event_ns FROM log WHERE src = $1 ORDER BY event_ns, seq":                              false,
		"SELECT last_seq FROM meta":                                                                        false,
		"INSERT INTO log_base (a) SELECT a FROM log UNION ALL SELECT a FROM log_base":                      false,
	} {
		if got := pgengine.FoldStatement(sql); got != want {
			t.Errorf("%q: %v", sql, got)
		}
	}
}

func TestTheRecorderKeepsTheStatementsOfARecordingAndNotTransactionControl(t *testing.T) {
	r := &pgengine.Recorder{}
	ctx := context.Background()
	send := func(sql string, args ...any) {
		r.TraceQueryStart(ctx, nil, pgx.TraceQueryStartData{SQL: sql, Args: args})
	}
	send("SELECT 1") // not recording
	r.Start()
	for _, s := range []string{"begin isolation level repeatable read read only", "BEGIN", "SELECT last_seq FROM meta", " commit", "rollback", "END", "ROLLBACK TO SAVEPOINT x"} {
		send(s)
	}
	args := []any{1, "a"}
	send("SELECT dst FROM edges WHERE src = $1 AND rel = $2", args...)
	args[0] = 99 // the recorder keeps its own copy of the arguments
	got := r.Stop()
	if len(got) != 2 || got[0].SQL != "SELECT last_seq FROM meta" || got[1].Args[0] != 1 || got[1].Args[1] != "a" {
		t.Errorf("recorded %+v", got)
	}
	send("SELECT 2")
	if got := r.Stop(); len(got) != 0 {
		t.Errorf("after the recording: %+v", got)
	}
	// A recording starts empty.
	r.Start()
	send("SELECT 3")
	r.Start()
	if got := r.Stop(); len(got) != 0 {
		t.Errorf("a second recording kept %+v of the first", got)
	}
}

func writeFile(path, text string) error { return os.WriteFile(path, []byte(text), 0o600) }
