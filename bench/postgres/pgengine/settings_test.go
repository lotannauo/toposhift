package pgengine_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/lotannauo/toposhift/bench/postgres/pgengine"
)

const settingsPath = "../scripts/settings.conf"

func TestSettingsFileIsParsedAsWritten(t *testing.T) {
	got, err := pgengine.ParseSettings(strings.NewReader(`
# a comment, and a blank line above
shared_buffers = 4GB   # a trailing comment
 work_mem=64MB
jit = off
pg_stat_statements.max = 100
`))
	if err != nil {
		t.Fatal(err)
	}
	want := []pgengine.Setting{{"shared_buffers", "4GB"}, {"work_mem", "64MB"}, {"jit", "off"}, {"pg_stat_statements.max", "100"}}
	if !slices.Equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestAMalformedSettingsFileIsRefused(t *testing.T) {
	for name, file := range map[string]string{
		"nothing":             "",
		"only comments":       "# nothing here\n\n",
		"no equals sign":      "shared_buffers 4GB\n",
		"no value":            "shared_buffers =\n",
		"no name":             "= 4GB\n",
		"an upper case name":  "Shared_Buffers = 4GB\n",
		"a name with a space": "shared buffers = 4GB\n",
		"a name set twice":    "jit = off\nwork_mem = 64MB\njit = on\n",
	} {
		if _, err := pgengine.ParseSettings(strings.NewReader(file)); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestShippedSettingsAreWellFormed(t *testing.T) {
	got, sum, err := pgengine.LoadSettings(settingsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(sum) {
		t.Errorf("digest %q", sum)
	}
	have := make(map[string]string)
	for _, s := range got {
		have[s.Name] = s.Value
	}
	// The settings the registration will name; one that goes missing changes what is
	// measured.
	for name, value := range map[string]string{
		"shared_buffers": "4GB", "effective_cache_size": "10GB", "work_mem": "64MB", "maintenance_work_mem": "1GB",
		"max_parallel_workers_per_gather": "0", "max_parallel_maintenance_workers": "0", "jit": "off",
		"random_page_cost": "1.1", "effective_io_concurrency": "200",
		"synchronous_commit": "off", "fsync": "on", "full_page_writes": "on",
		"wal_level": "minimal", "max_wal_senders": "0", "wal_compression": "lz4", "min_wal_size": "1GB", "wal_buffers": "64MB",
		"checkpoint_timeout": "1d", "autovacuum": "off", "plan_cache_mode": "force_custom_plan", "default_statistics_target": "100",
	} {
		if have[name] != value {
			t.Errorf("settings.conf has %s = %q, the design says %q", name, have[name], value)
		}
	}
	if _, ok := have["max_wal_size"]; !ok {
		t.Error("settings.conf does not set max_wal_size")
	}
	if _, ok := have["listen_addresses"]; ok {
		t.Error("listen_addresses depends on where the server runs, and pg-server.sh sets it")
	}
	if _, _, err := pgengine.LoadSettings(filepath.Join(t.TempDir(), "missing.conf")); err == nil {
		t.Error("a missing file was accepted")
	}
}

// show is a Querier whose current_setting answers come from a map; a name it does not
// have is null, as the server's missing_ok form says.
type show map[string]string

type row struct {
	v   *string
	err error
}

func (r row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(**string) = r.v
	return nil
}

func (s show) QueryRow(_ context.Context, _ string, args ...any) pgx.Row {
	if v, ok := s[args[0].(string)]; ok {
		return row{v: &v}
	}
	return row{}
}

type broken struct{}

func (broken) QueryRow(context.Context, string, ...any) pgx.Row {
	return row{err: errors.New("no connection")}
}

func TestAServerWhoseSettingsDifferIsRefused(t *testing.T) {
	want := []pgengine.Setting{{"shared_buffers", "4GB"}, {"jit", "off"}, {"wal_compression", "lz4"}}
	ok := show{"shared_buffers": "4GB", "jit": "off", "wal_compression": "lz4", "work_mem": "anything else is not asked"}
	if err := pgengine.CheckSettings(context.Background(), ok, want); err != nil {
		t.Fatalf("a server with the settings: %v", err)
	}
	for name, srv := range map[string]show{
		"a different value":      {"shared_buffers": "128MB", "jit": "off", "wal_compression": "lz4"},
		"a missing setting":      {"shared_buffers": "4GB", "jit": "off"},
		"a value in other units": {"shared_buffers": "4096MB", "jit": "off", "wal_compression": "lz4"},
	} {
		err := pgengine.CheckSettings(context.Background(), srv, want)
		var se *pgengine.SettingsError
		if !errors.Is(err, pgengine.ErrSettingsDiffer) || !errors.As(err, &se) || len(se.Differences) != 1 {
			t.Errorf("%s: %v", name, err)
		}
	}
	// Every difference is listed, not the first.
	err := pgengine.CheckSettings(context.Background(), show{}, want)
	var se *pgengine.SettingsError
	if !errors.As(err, &se) || len(se.Differences) != 3 {
		t.Errorf("a server with none of them: %v", err)
	}
	// An error from the server is not a difference.
	err = pgengine.CheckSettings(context.Background(), broken{}, want)
	if err == nil || errors.Is(err, pgengine.ErrSettingsDiffer) {
		t.Errorf("a broken connection: %v", err)
	}
}

func TestTheServerScriptPinsTheImageByDigestAndParses(t *testing.T) {
	b, err := os.ReadFile("../scripts/pg-server.sh")
	if err != nil {
		t.Fatal(err)
	}
	m := regexp.MustCompile(`(?m)^image='(postgres:17\.11-trixie@sha256:[0-9a-f]{64})'$`).FindSubmatch(b)
	if m == nil {
		t.Fatal("pg-server.sh does not pin the image as postgres:17.11-trixie@sha256:<digest>")
	}
	if got := string(m[1]); got != "postgres:17.11-trixie@sha256:2d2b8998d31037bf721cfdf764d76ba74171b4fab3431b7f72c27c56ddbdf9e3" {
		t.Errorf("the image is %s, not the approved one", got)
	}
	// Nothing else starts a server from an unpinned image.
	if n := len(regexp.MustCompile(`(?m)^\s*docker run `).FindAll(b, -1)); n != 1 {
		t.Errorf("pg-server.sh has %d docker run commands, want the one that uses the pinned image", n)
	}
	bash, err := exec.LookPath("bash")
	if err != nil {
		t.Skip("no bash to check the syntax of the script")
	}
	if out, err := exec.CommandContext(t.Context(), bash, "-n", "../scripts/pg-server.sh").CombinedOutput(); err != nil {
		t.Errorf("bash -n: %v\n%s", err, out)
	}
	// The script refuses what it does not know, before it touches docker.
	out, err := exec.CommandContext(t.Context(), bash, "../scripts/pg-server.sh", "restart").CombinedOutput()
	if err == nil || !strings.Contains(string(out), "usage:") {
		t.Errorf("an unknown command: %v\n%s", err, out)
	}
}
