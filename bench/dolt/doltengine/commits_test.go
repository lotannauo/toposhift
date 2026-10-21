package doltengine

import (
	"math"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

func TestCommitMessagesRoundTrip(t *testing.T) {
	for _, c := range []struct {
		msg   string
		kind  doltmodel.CommitKind
		token uint64
	}{
		{"seq 1", doltmodel.BatchCommit, 1},
		{"seq 18446744073709551615", doltmodel.BatchCommit, math.MaxUint64},
		{"retain 20000000000 seq 3", doltmodel.RetentionCommit, 3},
		{"retain 9223372036854775807 seq 0", doltmodel.RetentionCommit, 0},
		{"retain 0 seq 7", doltmodel.RetentionCommit, 7},
	} {
		kind, token, ok := parseMessage(c.msg)
		if !ok || kind != c.kind || token != c.token {
			t.Errorf("%q parsed as %v, %d, %v", c.msg, kind, token, ok)
		}
	}
	if batchMessage(5) != "seq 5" || retainMessage(20, 3) != "retain 20 seq 3" {
		t.Errorf("messages %q, %q", batchMessage(5), retainMessage(20, 3))
	}
	// Only the canonical spelling is the engine's: the commit Dolt makes when it creates a
	// database, and anything a person wrote, is not.
	for _, bad := range []string{
		"", "Initialize data repository", "seq", "seq ", "seq -1", "seq +5", "seq 05", "seq 5 ", " seq 5", "seq 5x", "seq 18446744073709551616",
		"retain", "retain 5", "retain 5 seq", "retain 5 seq ", "retain x seq 5", "retain 05 seq 5", "retain 5 seq 05", "retain -5 seq 5", "retain 5  seq 5",
		"retain 9223372036854775808 seq 1", "seq 5\nseq 6", "SEQ 5",
	} {
		if kind, token, ok := parseMessage(bad); ok {
			t.Errorf("%q was parsed as %v, %d", bad, kind, token)
		}
	}
}

func TestACommitIsMadeAtTheSyntheticDateByTheSyntheticAuthor(t *testing.T) {
	if got := dateArg(1); got != "2000-01-01T00:00:01Z" {
		t.Errorf("the first commit's date is %s", got)
	}
	if got := dateArg(3600); got != "2000-01-01T01:00:00Z" {
		t.Errorf("the 3600th commit's date is %s", got)
	}
	if got := Author(); got != "toposhift-bench <bench@localhost.invalid>" {
		t.Errorf("author %q", got)
	}
	s, err := commitStatement(2, "seq 9", &faults{})
	want := "CALL DOLT_COMMIT('-A', '-m', 'seq 9', '--author', 'toposhift-bench <bench@localhost.invalid>', '--date', '2000-01-01T00:00:02Z')"
	if err != nil || s != want {
		t.Errorf("got %q, %v; want %q", s, err, want)
	}
	if s, _ := commitStatement(2, "seq 9", &faults{wallClockDate: true}); strings.Contains(s, "--date") {
		t.Errorf("the defect that commits at the server's time still has a date: %s", s)
	}
	if s, _ := commitStatement(2, "seq 9", &faults{sessionAuthor: true}); strings.Contains(s, "--author") {
		t.Errorf("the defect that commits as the session still has an author: %s", s)
	}
	for _, msg := range []string{"it's", `back\slash`, "seq 1'); DROP TABLE log; --"} {
		if _, err := commitStatement(1, msg, &faults{}); err == nil {
			t.Errorf("a message with %q needed no quoting", msg)
		}
	}
}
