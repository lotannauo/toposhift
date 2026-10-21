package doltengine

import (
	"fmt"
	"strings"
	"testing"
)

func checkArgs(s stmt) error {
	if got, want := len(s.args), strings.Count(s.text, "?"); got != want {
		return fmt.Errorf("%d arguments for %d placeholders in:\n%s", got, want, s.text)
	}
	return nil
}

func ctxOf(batch bool, hash string, f *faults) readCtx {
	c := readCtx{hash: hash, layer: 2, dir: 1, at: 100, token: 7, f: f}
	if batch {
		c.batch, c.subjects = true, `["00","01"]`
	} else {
		c.subject = []byte{1, 2, 3}
	}
	return c
}

func TestEveryStatementHasAnArgumentForEachPlaceholder(t *testing.T) {
	for _, f := range []faults{{}, {deadlineInclusive: true}, {eventBefore: true}, {useRowAfterT: true}, {noLogFallback: true}, {logOnly: true}, {tieByEvent: true}, {dropSeqFilter: true}} {
		for _, hash := range []string{"", aHash} {
			for _, batch := range []bool{false, true} {
				c := ctxOf(batch, hash, &f)
				s, err := neighborsRule(c)
				if err != nil {
					t.Fatal(err)
				}
				if err := checkArgs(s); err != nil {
					t.Errorf("neighbors, batch %v, hash %q, %+v: %v", batch, hash, f, err)
				}
				if err := checkArgs(neighborsLog(c)); err != nil {
					t.Errorf("neighbors from the log, batch %v, %+v: %v", batch, f, err)
				}
			}
			c := ctxOf(false, hash, &f)
			s, err := aliveRule(c)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkArgs(s); err != nil {
				t.Errorf("alive, hash %q, %+v: %v", hash, f, err)
			}
			if err := checkArgs(aliveLog(c)); err != nil {
				t.Errorf("alive from the log, %+v: %v", f, err)
			}
		}
		for _, unbounded := range []bool{false, true} {
			if err := checkArgs(windowSQL(2, 1, []byte{1}, 5, 10, unbounded, 7, &f)); err != nil {
				t.Errorf("window, unbounded %v: %v", unbounded, err)
			}
		}
	}
	for n := 1; n < 4; n++ {
		s := stmt{text: insertRows("INSERT", "log", logCols, n), args: make([]any, n*len(logCols))}
		if err := checkArgs(s); err != nil {
			t.Error(err)
		}
	}
	for _, past := range []bool{false, true} {
		s := stmt{text: retainLog("log", []string{"layer", "a"}, past), args: retainLogArgs(5, past)}
		if err := checkArgs(s); err != nil {
			t.Error(err)
		}
	}
}

func TestACommitHashIsCheckedBeforeItIsWrittenIntoAStatement(t *testing.T) {
	if got, err := asOfClause(""); err != nil || got != "" {
		t.Errorf("no commit: %q, %v", got, err)
	}
	if got, err := asOfClause(aHash); err != nil || got != " AS OF '"+aHash+"'" {
		t.Errorf("a hash: %q, %v", got, err)
	}
	for _, bad := range []string{
		"0123456789abcdefghijklmnopqrstu",   // 31 characters
		"0123456789abcdefghijklmnopqrstuvw", // 33
		"0123456789ABCDEFGHIJKLMNOPQRSTUV",  // upper case
		"0123456789abcdefghijklmnopqrstuw",  // outside the alphabet
		"' OR 1=1 --0123456789abcdefghijkl", // not a hash
		"HEAD", "main", "0123456789abcdefghijklmnopqrstu'",
	} {
		if _, err := asOfClause(bad); err == nil {
			t.Errorf("%q was accepted", bad)
		}
		if _, err := neighborsRule(readCtx{hash: bad, f: &faults{}}); err == nil {
			t.Errorf("a neighbors read as of %q was built", bad)
		}
		if _, err := aliveRule(readCtx{hash: bad, f: &faults{}}); err == nil {
			t.Errorf("an alive read as of %q was built", bad)
		}
	}
}

// TestTheRuleStatementSettlesEachKeyByItsRow: the text of the statement has the parts the rule
// is made of, and each defect of the rule changes the part it is about.
func TestTheRuleStatementSettlesEachKeyByItsRow(t *testing.T) {
	build := func(f faults, batch bool) string {
		s, err := neighborsRule(ctxOf(batch, aHash, &f))
		if err != nil {
			t.Fatal(err)
		}
		return s.text
	}
	base := build(faults{}, false)
	asOf := "state AS OF '" + aHash + "' s"
	for _, part := range []string{
		asOf + " WHERE s.layer = ? AND s.dir = ? AND s.a = ? AND s.event_ns <= ?", // the rows that settle their keys
		asOf + " WHERE s.layer = ? AND s.dir = ? AND s.a = ? AND s.event_ns > ?",  // the keys sent to the log
		"LEFT JOIN log l ON",             // a key with no record at or before the instant is still a key
		"l.event_ns <= ? AND l.seq <= ?", // the log's filters
		"ROW_NUMBER() OVER (PARTITION BY k.ord, k.peer, k.rel, k.producer ORDER BY l.event_ns DESC, l.seq DESC)",
		"s.deadline_ns IS NULL OR s.deadline_ns > ?",                                                                              // a state row's hold test
		"(CASE WHEN x.through_ns IS NOT NULL AND x.through_ns > x.event_ns THEN x.through_ns ELSE x.event_ns END) + x.ttl_ns > ?", // a log row's
		"UNION ALL",
		"GROUP BY u.ord, u.peer, u.rel",
	} {
		if !strings.Contains(base, part) {
			t.Errorf("the statement lacks %q:\n%s", part, base)
		}
	}
	if strings.Count(base, "AS OF '"+aHash+"'") != 2 {
		t.Errorf("both reads of the state table must be as of the commit:\n%s", base)
	}
	batch := build(faults{}, true)
	for _, part := range []string{"JSON_TABLE(?, '$[*]' COLUMNS (ord FOR ORDINALITY", "s.a = UNHEX(k.a_hex)", "k.ord AS ord"} {
		if !strings.Contains(batch, part) {
			t.Errorf("the batched statement lacks %q:\n%s", part, batch)
		}
	}
	if strings.Contains(base, "JSON_TABLE") {
		t.Errorf("a single read uses JSON_TABLE:\n%s", base)
	}
	if head, _ := neighborsRule(ctxOf(false, "", &faults{})); strings.Contains(head.text, "AS OF") {
		t.Errorf("a read of the current state has an AS OF clause:\n%s", head.text)
	}

	for name, f := range map[string]faults{
		"deadline inclusive": {deadlineInclusive: true}, "event before": {eventBefore: true}, "no log fallback": {noLogFallback: true},
		"log only": {logOnly: true}, "tie by event": {tieByEvent: true},
	} {
		if build(f, false) == base {
			t.Errorf("the defect %q does not change the statement", name)
		}
	}
	// A defect that changes an argument and not the text: the row settles a key whatever its
	// event, and the log is never asked for one; and the log's filter ignores the token.
	s, _ := neighborsRule(ctxOf(false, aHash, &faults{useRowAfterT: true}))
	if s.args[4] != any(int64(1<<63-1)) {
		t.Errorf("useRowAfterT args %v", s.args)
	}
	s, _ = neighborsRule(ctxOf(false, aHash, &faults{dropSeqFilter: true}))
	if s.args[len(s.args)-1] != any(int64(1<<63-1)) {
		t.Errorf("dropSeqFilter args %v", s.args)
	}
	if s, _ := neighborsRule(ctxOf(false, aHash, &faults{})); s.args[len(s.args)-1] != any(int64(7)) {
		t.Errorf("the log's token filter is %v, the read's is 7", s.args[len(s.args)-1])
	}
}

func TestRetentionStatements(t *testing.T) {
	text := retainLog("log", []string{"layer", "dir", "a", "peer", "rel", "producer"}, false)
	for _, part := range []string{
		"DELETE l FROM log l JOIN (",
		"ROW_NUMBER() OVER (PARTITION BY layer, dir, a, peer, rel, producer ORDER BY event_ns DESC, seq DESC) AS rn FROM log WHERE event_ns < ?",
		") x WHERE x.rn > 1) d ON l.layer = d.layer AND l.dir = d.dir AND l.a = d.a AND l.peer = d.peer AND l.rel = d.rel AND l.producer = d.producer AND l.event_ns = d.event_ns AND l.seq = d.seq",
	} {
		if !strings.Contains(text, part) {
			t.Errorf("the retention statement lacks %q:\n%s", part, text)
		}
	}
	if strings.Contains(text, "EXISTS") {
		t.Errorf("the retention statement is the quadratic one:\n%s", text)
	}
	ent := retainLog("ent_log", []string{"fp", "producer"}, false)
	if !strings.Contains(ent, "PARTITION BY fp, producer ORDER BY") || !strings.Contains(ent, "l.fp = d.fp AND l.producer = d.producer AND l.event_ns = d.event_ns AND l.seq = d.seq") {
		t.Errorf("the entity log's retention:\n%s", ent)
	}
	past := retainLog("log", []string{"layer"}, true)
	if strings.Contains(past, "event_ns <") || strings.Contains(past, "?") {
		t.Errorf("a horizon past the end has a bound: %s", past)
	}
	if got := retainLogArgs(5, false); len(got) != 1 || got[0] != any(int64(5)) {
		t.Errorf("args %v", got)
	}
	if got := retainLogArgs(5, true); len(got) != 0 {
		t.Errorf("args past the end %v", got)
	}
	if !strings.Contains(retainState, "deadline_ns IS NOT NULL AND deadline_ns <= ?") || !strings.Contains(retainEntState, "deadline_ns IS NOT NULL AND deadline_ns <= ?") {
		t.Error("the state retention is not 'deadline at or before the horizon, never an open row'")
	}
}

func TestInsertRows(t *testing.T) {
	got := insertRows("REPLACE", "state", []string{"a", "b"}, 3)
	if want := "REPLACE INTO state (a, b) VALUES (?, ?), (?, ?), (?, ?)"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if per := rowsPerStatement(len(logCols)); per*len(logCols) > maxPlaceholders || per < 100 {
		t.Errorf("%d rows of %d columns per statement", per, len(logCols))
	}
	if rowsPerStatement(maxPlaceholders+5) != 1 {
		t.Error("a row wider than the limit is still sent")
	}
}
