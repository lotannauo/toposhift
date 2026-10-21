package doltengine

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
)

// Every commit the engine makes has a message, an author and a date that are functions of the
// stream, so that two loads of one stream end at the same head commit hash. The author is
// neutral (never a person's), the date of the n-th commit is
// [doltmodel.CommitDate] of n, and the message says what the commit is of.
const (
	// AuthorName and AuthorEmail are the author of every commit the engine makes.
	AuthorName  = "toposhift-bench"
	AuthorEmail = "bench@localhost.invalid"
)

// Author is the --author argument of a commit.
func Author() string { return AuthorName + " <" + AuthorEmail + ">" }

// Commit is a Dolt commit the engine made: the n-th (counting from 1, retention commits
// among them), what it is of, and its hash.
type Commit struct {
	Ordinal int
	Kind    doltmodel.CommitKind
	// Token is the highest sequence number committed when the commit was made.
	Token   uint64
	Message string
	Date    time.Time
	Hash    string
}

// batchMessage is the message of the commit of a batch ending at seq last.
func batchMessage(last uint64) string { return "seq " + strconv.FormatUint(last, 10) }

// retainMessage is the message of the commit of a retention to horizon h (nanoseconds; the
// largest int64 for a horizon past the last instant), when seq last was committed.
func retainMessage(h int64, last uint64) string {
	return "retain " + strconv.FormatInt(h, 10) + " seq " + strconv.FormatUint(last, 10)
}

// parseMessage is the inverse of the two messages. A message that is not exactly one of
// them, in its canonical spelling, is not an engine commit (the one Dolt makes when a
// database is created is the example).
func parseMessage(msg string) (kind doltmodel.CommitKind, token uint64, ok bool) {
	rest, found := strings.CutPrefix(msg, "seq ")
	if found {
		n, err := strconv.ParseUint(rest, 10, 64)
		if err == nil && batchMessage(n) == msg {
			return doltmodel.BatchCommit, n, true
		}
		return 0, 0, false
	}
	rest, found = strings.CutPrefix(msg, "retain ")
	if !found {
		return 0, 0, false
	}
	hs, ts, found := strings.Cut(rest, " seq ")
	if !found {
		return 0, 0, false
	}
	h, err := strconv.ParseInt(hs, 10, 64)
	if err != nil || h < 0 {
		return 0, 0, false // a horizon is a time since 1970
	}
	n, err := strconv.ParseUint(ts, 10, 64)
	if err != nil || retainMessage(h, n) != msg {
		return 0, 0, false
	}
	return doltmodel.RetentionCommit, n, true
}

// dateArg is the --date argument of the commit with the ordinal: the date in UTC, with its
// zone written, so that the server's own zone cannot change it.
func dateArg(ordinal int) string {
	return doltmodel.CommitDate(ordinal).Format("2006-01-02T15:04:05Z07:00")
}

// commitStatement is the call that commits the working set with every table staged. Its
// parts are all constants of the stream (digits and fixed text), so they are written into
// the statement and need no quoting.
func commitStatement(ordinal int, message string, f *faults) (string, error) {
	if strings.ContainsAny(message, "'\\") {
		return "", fmt.Errorf("doltengine: commit message %q needs quoting", message)
	}
	s := "CALL DOLT_COMMIT('-A', '-m', '" + message + "'"
	if !f.sessionAuthor {
		s += ", '--author', '" + Author() + "'"
	}
	if !f.wallClockDate {
		s += ", '--date', '" + dateArg(ordinal) + "'"
	}
	return s + ")", nil
}

// InitMessage is the message of the commit the server makes when it creates a database,
// once the engine has made it its own: it is not a batch or a retention, and is dated
// 2000-01-01T00:00:00Z, the instant before the first commit of a stream.
const InitMessage = "toposhift init"

// amendStatement is the call that replaces the server's first commit with the engine's.
func amendStatement() string {
	return "CALL DOLT_COMMIT('--amend', '-m', '" + InitMessage + "', '--author', '" + Author() + "', '--date', '" + dateArg(0) + "')"
}
