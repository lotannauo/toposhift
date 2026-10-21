package doltengine

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
)

// The statements of the engine, as text. Each read statement is built with its arguments
// in one place, so that a placeholder and its argument cannot drift apart, and so that a
// test can look at the text a read sends without a server.

// stmt is a statement and its arguments, built together in the order the text uses them.
type stmt struct {
	text string
	args []any
}

func (s *stmt) add(text string, args ...any) {
	s.text += text
	s.args = append(s.args, args...)
}

// commitHash is what a Dolt commit hash looks like: 32 characters of a base-32 alphabet. A
// hash is only ever put into a statement (an AS OF clause takes it as a literal) after
// it has matched.
var commitHash = regexp.MustCompile(`^[0-9a-v]{32}$`)

// asOfClause is the clause that reads a table as a commit left it, or nothing for the
// current table.
func asOfClause(hash string) (string, error) {
	if hash == "" {
		return "", nil
	}
	if !commitHash.MatchString(hash) {
		return "", fmt.Errorf("doltengine: %q is not a commit hash", hash)
	}
	return " AS OF '" + hash + "'", nil
}

// readCtx is what the statements of one read are built from.
type readCtx struct {
	// hash is the commit the state table is read as of, or "" for the current state.
	hash string
	// layer and dir are the scope and the direction (for an edge read; an entity read
	// uses the layer alone).
	layer, dir int
	// at is the instant, and token the snapshot token, in the encodings of the tables.
	at, token int64
	// subject is the encoded fingerprint of a single read; subjects is the JSON text of
	// the batch's fingerprints, as hex, for a batched read.
	subject  []byte
	subjects string
	batch    bool
	f        *faults
}

// holdsLog is the SQL for "this log row, the newest of its key at or before the instant,
// holds at the instant" over the columns of an alias x: an Observe whose deadline, the
// later of its event time and Through plus the TTL, is after the instant, or that has
// none. The deadline is computed here, as the state row stores it, and the row is NULL
// (not holding) when the key has no such record. The later of the two times is a CASE, not
// GREATEST: the server's GREATEST gives a type whose sum with the TTL is refused as out of
// range for a deadline exactly at the last representable instant.
func (c readCtx) holdsLog(x string) string {
	op := ">"
	if c.f.deadlineInclusive {
		op = ">="
	}
	return fmt.Sprintf("%[1]s.kind = 'o' AND (%[1]s.ttl_ns = 0 OR (CASE WHEN %[1]s.through_ns IS NOT NULL AND %[1]s.through_ns > %[1]s.event_ns THEN %[1]s.through_ns ELSE %[1]s.event_ns END) + %[1]s.ttl_ns %s ?)", x, op)
}

func (c readCtx) holdsState(x string) string {
	op := ">"
	if c.f.deadlineInclusive {
		op = ">="
	}
	return fmt.Sprintf("%[1]s.kind = 'o' AND (%[1]s.deadline_ns IS NULL OR %[1]s.deadline_ns %s ?)", x, op)
}

// logToken is the token of a log read: the read's own, or with the defect that ignores
// it, the largest.
func (c readCtx) logToken() int64 {
	if c.f.dropSeqFilter {
		return 1<<63 - 1
	}
	return c.token
}

// scanState adds a read of the state table, as of the commit if there is one, restricted
// to the subject or subjects, with its rows' columns named by the alias s; where is a
// condition on s that follows the subject's. The ord column is 0 for a single read and
// the ordinal of the fingerprint in the batch otherwise.
//
// A single read is "FROM state s WHERE s.layer = ? AND s.dir = ? AND s.a = ?". A batched
// read is JSON_TABLE over the fingerprints, joined to the state table on the key's
// prefix, so that one statement answers every list and the order of the lists is the
// ordinals'.
func (c readCtx) scanState(s *stmt, cols, table, asOf, where string, whereArgs ...any) {
	if c.batch {
		s.add(fmt.Sprintf("SELECT k.ord AS ord, %s FROM JSON_TABLE(?, '$[*]' COLUMNS (ord FOR ORDINALITY, a_hex VARCHAR(%d) PATH '$')) k JOIN %s%s s ON s.layer = ? AND s.dir = ? AND s.a = UNHEX(k.a_hex)", cols, hexColumn, table, asOf),
			c.subjects, c.layer, c.dir)
	} else {
		s.add(fmt.Sprintf("SELECT 0 AS ord, %s FROM %s%s s WHERE s.layer = ? AND s.dir = ? AND s.a = ?", cols, table, asOf),
			c.layer, c.dir, c.subject)
	}
	if where != "" {
		s.add(" AND "+where, whereArgs...)
	}
}

// hexColumn is the width of the hex text of the longest encoded fingerprint.
const hexColumn = 2 * (255 + 1 + 16)

// neighborsRule is the statement of the rule for a neighbors read (one fingerprint, or a
// batch): the state table as of the commit settles the keys whose row is at or before
// the instant, and the log answers the keys whose row is after it. For each (list,
// peer, relation) it returns how many keys the rows settled, how many it sent to the
// log, and whether any of them holds, so that the caller has the answer and the counts
// of the one read.
//
// The log branch is driven by the state rows after the instant and outer-joined to the
// log, so that a key none of whose records is at or before the instant (or visible at the
// token) still counts as a key sent to the log, and answers false.
func neighborsRule(c readCtx) (stmt, error) {
	asOf, err := asOfClause(c.hash)
	if err != nil {
		return stmt{}, err
	}
	var s stmt
	s.add("SELECT u.ord, u.peer, u.rel, SUM(u.settled) AS keys_state, COUNT(*) - SUM(u.settled) AS keys_log, MAX(u.holds) AS holds\nFROM (\n")

	// The keys whose row settles them.
	settle, after := "s.event_ns <= ?", "s.event_ns > ?"
	settleAt, afterAt := c.at, c.at
	switch {
	case c.f.eventBefore:
		settle, after = "s.event_ns < ?", "s.event_ns >= ?"
	case c.f.useRowAfterT:
		settleAt, afterAt = 1<<63-1, 1<<63-1 // every row settles; none goes to the log
	}
	if !c.f.logOnly {
		cols := "s.peer AS peer, s.rel AS rel, 1 AS settled, IF(" + c.holdsState("s") + ", 1, 0) AS holds"
		// The hold test's argument comes first in the text, so it goes first.
		inner := stmt{}
		c.scanState(&inner, cols, "state", asOf, settle, settleAt)
		s.add(inner.text, append([]any{c.at}, inner.args...)...)
		s.add("\n")
	}
	if !c.f.logOnly && !c.f.noLogFallback {
		s.add("UNION ALL\n")
	}
	if !c.f.noLogFallback {
		s.add("SELECT x.ord, x.peer, x.rel, 0 AS settled, IF("+c.holdsLog("x")+", 1, 0) AS holds\nFROM (\n", c.at)
		keys := stmt{}
		c.scanState(&keys, "s.a AS a, s.peer AS peer, s.rel AS rel, s.producer AS producer", "state", asOf, after, afterAt)
		order := "l.event_ns DESC, l.seq DESC"
		if c.f.tieByEvent {
			order = "l.event_ns DESC"
		}
		s.add("SELECT k.ord, k.peer, k.rel, l.kind, l.ttl_ns, l.through_ns, l.event_ns,\n" +
			"       ROW_NUMBER() OVER (PARTITION BY k.ord, k.peer, k.rel, k.producer ORDER BY " + order + ") AS rn\nFROM (")
		s.add(keys.text, keys.args...)
		s.add(") k\nLEFT JOIN log l ON l.layer = ? AND l.dir = ? AND l.a = k.a AND l.peer = k.peer AND l.rel = k.rel AND l.producer = k.producer AND l.event_ns <= ? AND l.seq <= ?\n",
			c.layer, c.dir, c.at, c.logToken())
		s.add(") x\nWHERE x.rn = 1\n")
	}
	s.add(") u\nGROUP BY u.ord, u.peer, u.rel")
	return s, nil
}

// neighborsLog is the statement of a read whose token names no commit: there is no state
// to read, and every key is answered from the log. It returns, for each (list, peer,
// relation), whether any key holds. A key is any (peer, relation, producer) the log
// holds for the subject, so that the caller can tell that the subject has keys at all.
func neighborsLog(c readCtx) stmt {
	var s stmt
	hit := "(l.event_ns <= ? AND l.seq <= ?)"
	order := hit + " DESC, l.event_ns DESC, l.seq DESC"
	if c.f.tieByEvent {
		order = hit + " DESC, l.event_ns DESC"
	}
	s.add("SELECT x.ord, x.peer, x.rel, MAX(IF(x.hit = 1 AND "+c.holdsLog("x")+", 1, 0)) AS holds\nFROM (\n", c.at)
	if c.batch {
		s.add(fmt.Sprintf("SELECT k.ord AS ord, l.peer AS peer, l.rel AS rel, l.kind, l.ttl_ns, l.through_ns, l.event_ns, %[1]s AS hit,\n"+
			"       ROW_NUMBER() OVER (PARTITION BY k.ord, l.peer, l.rel, l.producer ORDER BY %[2]s) AS rn\n"+
			"FROM JSON_TABLE(?, '$[*]' COLUMNS (ord FOR ORDINALITY, a_hex VARCHAR(%[3]d) PATH '$')) k JOIN log l ON l.layer = ? AND l.dir = ? AND l.a = UNHEX(k.a_hex)\n",
			hit, order, hexColumn),
			c.at, c.logToken(), c.at, c.logToken(), c.subjects, c.layer, c.dir)
	} else {
		s.add(fmt.Sprintf("SELECT 0 AS ord, l.peer AS peer, l.rel AS rel, l.kind, l.ttl_ns, l.through_ns, l.event_ns, %[1]s AS hit,\n"+
			"       ROW_NUMBER() OVER (PARTITION BY l.peer, l.rel, l.producer ORDER BY %[2]s) AS rn\n"+
			"FROM log l WHERE l.layer = ? AND l.dir = ? AND l.a = ?\n",
			hit, order),
			c.at, c.logToken(), c.at, c.logToken(), c.layer, c.dir, c.subject)
	}
	s.add(") x\nWHERE x.rn = 1\nGROUP BY x.ord, x.peer, x.rel")
	return s
}

// aliveRule is the same rule for the existence of one entity: the entity state table as of
// the commit settles the producers whose row is at or before the instant, the entity log
// answers the others. It returns one row: the number of keys, how many of them the rows
// settled, and whether any holds. The entity tables hold the layer as a value, and an
// entity in another layer has no key here.
func aliveRule(c readCtx) (stmt, error) {
	asOf, err := asOfClause(c.hash)
	if err != nil {
		return stmt{}, err
	}
	var s stmt
	s.add("SELECT COUNT(*) AS keys_n, COALESCE(SUM(u.settled), 0) AS keys_state, COALESCE(MAX(u.holds), 0) AS holds\nFROM (\n")
	settle, after := "s.event_ns <= ?", "s.event_ns > ?"
	settleAt, afterAt := c.at, c.at
	switch {
	case c.f.eventBefore:
		settle, after = "s.event_ns < ?", "s.event_ns >= ?"
	case c.f.useRowAfterT:
		settleAt, afterAt = 1<<63-1, 1<<63-1
	}
	if !c.f.logOnly {
		s.add("SELECT 1 AS settled, IF("+c.holdsState("s")+", 1, 0) AS holds FROM ent_state"+asOf+" s WHERE s.fp = ? AND s.layer = ? AND "+settle+"\n",
			c.at, c.subject, c.layer, settleAt)
	}
	if !c.f.logOnly && !c.f.noLogFallback {
		s.add("UNION ALL\n")
	}
	if !c.f.noLogFallback {
		order := "l.event_ns DESC, l.seq DESC"
		if c.f.tieByEvent {
			order = "l.event_ns DESC"
		}
		s.add("SELECT 0 AS settled, IF("+c.holdsLog("x")+", 1, 0) AS holds\nFROM (\n"+
			"SELECT l.kind, l.ttl_ns, l.through_ns, l.event_ns,\n       ROW_NUMBER() OVER (PARTITION BY k.producer ORDER BY "+order+") AS rn\n"+
			"FROM (SELECT s.producer AS producer FROM ent_state"+asOf+" s WHERE s.fp = ? AND s.layer = ? AND "+after+") k\n"+
			"LEFT JOIN ent_log l ON l.fp = ? AND l.producer = k.producer AND l.event_ns <= ? AND l.seq <= ?\n"+
			") x\nWHERE x.rn = 1\n",
			c.at, c.subject, c.layer, afterAt, c.subject, c.at, c.logToken())
	}
	s.add(") u")
	return s, nil
}

// aliveLog is the existence read whose token names no commit: every producer's newest
// record at or before the instant, from the log. It returns one row: the number of
// producers the entity has in the layer, and whether any holds.
func aliveLog(c readCtx) stmt {
	var s stmt
	hit := "(l.event_ns <= ? AND l.seq <= ?)"
	order := hit + " DESC, l.event_ns DESC, l.seq DESC"
	if c.f.tieByEvent {
		order = hit + " DESC, l.event_ns DESC"
	}
	s.add("SELECT COUNT(*) AS keys_n, COALESCE(MAX(IF(x.hit = 1 AND "+c.holdsLog("x")+", 1, 0)), 0) AS holds\nFROM (\n"+
		"SELECT l.kind, l.ttl_ns, l.through_ns, l.event_ns, "+hit+" AS hit,\n"+
		"       ROW_NUMBER() OVER (PARTITION BY l.producer ORDER BY "+order+") AS rn\n"+
		"FROM ent_log l WHERE l.fp = ? AND l.layer = ?\n) x\nWHERE x.rn = 1",
		c.at, c.at, c.logToken(), c.at, c.logToken(), c.subject, c.layer)
	return s
}

// windowSQL reads the edge records of a subject set in a window of event time, through
// the index on event time, with the token's filter, in the order the contract promises.
// An unbounded window has no upper bound.
func windowSQL(layer, dir int, a []byte, lo, hi int64, unbounded bool, token int64, f *faults) stmt {
	var s stmt
	s.add("SELECT peer, rel, producer, event_ns, seq, kind, ttl_ns, through_ns, payload FROM log\n"+
		"WHERE layer = ? AND dir = ? AND a = ? AND event_ns >= ?", layer, dir, a, lo)
	if !unbounded {
		s.add(" AND event_ns < ?", hi)
	}
	if f.dropSeqFilter {
		token = 1<<63 - 1
	}
	s.add(" AND seq <= ?\nORDER BY event_ns, seq", token)
	return s
}

// insertRows is a multi-row statement: the verb, the table, its columns, and n rows of
// placeholders.
func insertRows(verb, table string, cols []string, n int) string {
	row := "(" + strings.TrimSuffix(strings.Repeat("?, ", len(cols)), ", ") + ")"
	return verb + " INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES " +
		strings.TrimSuffix(strings.Repeat(row+", ", n), ", ")
}

// The columns of each table's rows, in the order the writer binds them.
var (
	logCols      = []string{"layer", "dir", "a", "peer", "rel", "producer", "event_ns", "seq", "kind", "ttl_ns", "through_ns", "payload"}
	entLogCols   = []string{"fp", "producer", "layer", "event_ns", "seq", "kind", "ttl_ns", "through_ns", "payload"}
	stateCols    = []string{"layer", "dir", "a", "peer", "rel", "producer", "event_ns", "seq", "kind", "deadline_ns"}
	entStateCols = []string{"fp", "producer", "layer", "event_ns", "seq", "kind", "deadline_ns"}
)

// maxPlaceholders is how many placeholders a statement may carry: the protocol's limit is
// 65,535, and a statement stays well under it.
const maxPlaceholders = 30000

// rowsPerStatement is how many rows of the given width one statement inserts.
func rowsPerStatement(width int) int { return max(1, maxPlaceholders/width) }

// retention statements. The log keeps, for each key, the newest row before the horizon
// (the baseline that stands for the rows before it), and the state tables lose the rows
// whose deadline is at or before the horizon. A horizon past the last representable
// instant has every row before it, which no comparison with a time says, so its log
// statement has no bound of its own.
const (
	retainState    = "DELETE FROM state WHERE deadline_ns IS NOT NULL AND deadline_ns <= ?"
	retainEntState = "DELETE FROM ent_state WHERE deadline_ns IS NOT NULL AND deadline_ns <= ?"
)

// retainLog is the statement that deletes the rows of a log older than the newest of
// their key before the horizon: the rows before the horizon are numbered, newest first, within
// their key, and those numbered after the first go. key lists the columns before the event
// time of the table's primary key, and the join back to the table is on the whole key. The
// baseline, the newest row of a key before the horizon, stays; a retention that stands in for
// an earlier one finds the earlier baseline the oldest of its key, and replaces it. (A
// correlated DELETE ... WHERE EXISTS on the same table says the same and is quadratic: it
// did not finish on a hundred thousand rows.)
func retainLog(table string, key []string, pastEnd bool) string {
	cols := strings.Join(append(slices.Clone(key), "event_ns", "seq"), ", ")
	where := ""
	if !pastEnd {
		where = " WHERE event_ns < ?"
	}
	var on []string
	for _, k := range append(slices.Clone(key), "event_ns", "seq") {
		on = append(on, "l."+k+" = d."+k)
	}
	return "DELETE l FROM " + table + " l JOIN (SELECT " + cols + " FROM (SELECT " + cols +
		", ROW_NUMBER() OVER (PARTITION BY " + strings.Join(key, ", ") + " ORDER BY event_ns DESC, seq DESC) AS rn FROM " + table + where +
		") x WHERE x.rn > 1) d ON " + strings.Join(on, " AND ")
}

// retainLogArgs is the arguments of [retainLog]: the horizon, or none past the end.
func retainLogArgs(h int64, pastEnd bool) []any {
	if pastEnd {
		return nil
	}
	return []any{h}
}
