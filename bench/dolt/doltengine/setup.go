package doltengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// setup creates the tables if the database has none, or checks that the ones it has are the
// engine's and rebuilds what the writer remembers from them.
func (e *Engine) setup(ctx context.Context) error {
	have, err := e.tableNames(ctx)
	if err != nil {
		return err
	}
	var want []string
	for _, t := range doltmodel.Schema() {
		want = append(want, t.Name)
	}
	slices.Sort(want)
	switch {
	case len(have) == 0:
		if err := e.create(ctx); err != nil {
			return err
		}
	case !slices.Equal(have, want):
		return fmt.Errorf("doltengine: database %s holds tables %v, the engine's are %v: %w", e.name, have, want, ErrSchema)
	}
	return e.rebuild(ctx)
}

// tableNames lists the tables of the database other than the server's own system tables.
func (e *Engine) tableNames(ctx context.Context) ([]string, error) {
	tx, err := e.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("doltengine: listing tables: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema = ?", e.name)
	if err != nil {
		return nil, fmt.Errorf("doltengine: listing tables: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, fmt.Errorf("doltengine: listing tables: %w", err)
		}
		if !strings.HasPrefix(n, "dolt_") {
			out = append(out, n)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doltengine: listing tables: %w", err)
	}
	slices.Sort(out)
	return out, nil
}

// create makes the tables of a new database and its meta row. The tables are not committed:
// the first commit, a batch's, stages every table.
func (e *Engine) create(ctx context.Context) error {
	tx, err := e.wdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	// The commit the server makes when it creates a database carries the server's time and its
	// own author, so two loads would end at different heads. It is made the engine's: dated
	// before the first commit of the stream and authored as every other.
	if _, err := tx.ExecContext(ctx, amendStatement()); err != nil {
		return fmt.Errorf("doltengine: making the database's first commit the engine's: %w", err)
	}
	for _, ddl := range doltmodel.DDL() {
		if _, err := tx.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("doltengine: %.60s...: %w", strings.ReplaceAll(ddl, "\n", " "), err)
		}
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO meta (id, last_seq, horizon_ns, past_end, schema_version) VALUES (1, ?, 0, 0, ?)",
		doltmodel.EncodeSeq(0), doltmodel.SchemaVersion); err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("doltengine: creating the tables: %w", err)
	}
	return nil
}

// rebuild reads the database as it is and sets what the engine remembers: the token, the
// horizon, the layer of each subject, each state key's newest record, and the commits.
func (e *Engine) rebuild(ctx context.Context) error {
	tx, err := e.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var last, horizon int64
	var pastEnd bool
	var version int
	err = tx.QueryRowContext(ctx, "SELECT last_seq, horizon_ns, past_end, schema_version FROM meta WHERE id = 1").Scan(&last, &horizon, &pastEnd, &version)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("doltengine: database %s has no meta row: %w", e.name, ErrSchema)
	case err != nil:
		return fmt.Errorf("doltengine: reading the meta row: %w", err)
	case version != doltmodel.SchemaVersion:
		return fmt.Errorf("doltengine: database %s has schema version %d, this engine writes %d: %w", e.name, version, doltmodel.SchemaVersion, ErrSchema)
	}
	w := newWriter(&e.f)
	w.lastSeq, w.horizon, w.pastEnd = doltmodel.DecodeSeq(last), horizon, pastEnd

	if err := e.rebuildLayers(ctx, tx, w); err != nil {
		return err
	}
	if err := e.rebuildState(ctx, tx, w); err != nil {
		return err
	}
	commits, err := e.readCommits(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case e.arm == doltmodel.ArmRows && len(commits) > 0:
		return fmt.Errorf("doltengine: database %s holds %d commits of the arm with commits, and is opened as the arm with none: %w", e.name, len(commits), ErrSchema)
	case e.arm == doltmodel.ArmCommit && w.lastSeq > 0 && len(commits) == 0:
		return fmt.Errorf("doltengine: database %s holds batches and no commits, and is opened as the arm with commits: %w", e.name, ErrSchema)
	case e.arm == doltmodel.ArmCommit && len(commits) > 0 && commits[len(commits)-1].Token != w.lastSeq:
		return fmt.Errorf("doltengine: database %s is at seq %d and its newest commit is at seq %d: a batch has no commit: %w", e.name, w.lastSeq, commits[len(commits)-1].Token, ErrSchema)
	}

	e.wmu.Lock()
	defer e.wmu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.w = w
	e.lastSeq = w.lastSeq
	e.commits = commits
	e.byToken = make(map[uint64]int, len(commits))
	for i, c := range commits {
		e.byToken[c.Token] = i
	}
	return nil
}

func (e *Engine) rebuildLayers(ctx context.Context, tx *sql.Tx, w *writer) error {
	rows, err := tx.QueryContext(ctx, "SELECT DISTINCT layer, a, peer, rel FROM log WHERE dir = ?", int(engine.Forward))
	if err != nil {
		return fmt.Errorf("doltengine: reading the subjects: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var layer int
		var a, peer, rel []byte
		if err := rows.Scan(&layer, &a, &peer, &rel); err != nil {
			return fmt.Errorf("doltengine: reading the subjects: %w", err)
		}
		s, err := edgeSubject(a, peer, rel)
		if err != nil {
			return err
		}
		w.layers[s] = catalog.Layer(layer)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("doltengine: reading the subjects: %w", err)
	}
	ents, err := tx.QueryContext(ctx, "SELECT DISTINCT fp, layer FROM ent_log")
	if err != nil {
		return fmt.Errorf("doltengine: reading the subjects: %w", err)
	}
	defer func() { _ = ents.Close() }()
	for ents.Next() {
		var fp []byte
		var layer int
		if err := ents.Scan(&fp, &layer); err != nil {
			return fmt.Errorf("doltengine: reading the subjects: %w", err)
		}
		f, err := doltmodel.DecodeFingerprint(fp)
		if err != nil {
			return fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
		}
		w.layers[engine.EntitySubject(f)] = catalog.Layer(layer)
	}
	if err := ents.Err(); err != nil {
		return fmt.Errorf("doltengine: reading the subjects: %w", err)
	}
	return nil
}

func (e *Engine) rebuildState(ctx context.Context, tx *sql.Tx, w *writer) error {
	rows, err := tx.QueryContext(ctx, "SELECT a, peer, rel, producer, event_ns, seq, deadline_ns FROM state WHERE dir = ?", int(engine.Forward))
	if err != nil {
		return fmt.Errorf("doltengine: reading the state: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var a, peer, rel, producer []byte
		var ev, seq int64
		var deadline sql.NullInt64
		if err := rows.Scan(&a, &peer, &rel, &producer, &ev, &seq, &deadline); err != nil {
			return fmt.Errorf("doltengine: reading the state: %w", err)
		}
		s, err := edgeSubject(a, peer, rel)
		if err != nil {
			return err
		}
		w.newest[subjectProducer{s, producerOf(producer)}] = stateEntry{eventNs: ev, seq: seq, open: !deadline.Valid, deadlineNs: deadline.Int64}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("doltengine: reading the state: %w", err)
	}
	ents, err := tx.QueryContext(ctx, "SELECT fp, producer, event_ns, seq, deadline_ns FROM ent_state")
	if err != nil {
		return fmt.Errorf("doltengine: reading the state: %w", err)
	}
	defer func() { _ = ents.Close() }()
	for ents.Next() {
		var fp, producer []byte
		var ev, seq int64
		var deadline sql.NullInt64
		if err := ents.Scan(&fp, &producer, &ev, &seq, &deadline); err != nil {
			return fmt.Errorf("doltengine: reading the state: %w", err)
		}
		f, err := doltmodel.DecodeFingerprint(fp)
		if err != nil {
			return fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
		}
		w.newest[subjectProducer{engine.EntitySubject(f), producerOf(producer)}] = stateEntry{eventNs: ev, seq: seq, open: !deadline.Valid, deadlineNs: deadline.Int64}
	}
	if err := ents.Err(); err != nil {
		return fmt.Errorf("doltengine: reading the state: %w", err)
	}
	return nil
}

func edgeSubject(a, peer, rel []byte) (engine.Subject, error) {
	fa, err := doltmodel.DecodeFingerprint(a)
	if err != nil {
		return engine.Subject{}, fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
	}
	fb, err := doltmodel.DecodeFingerprint(peer)
	if err != nil {
		return engine.Subject{}, fmt.Errorf("doltengine: %w: %w", err, ErrSchema)
	}
	return engine.EdgeSubject(fa, fb, catalog.RelationType(rel)), nil
}

// readCommits reads the commits the engine made from the commit log, oldest first. A
// commit whose message is neither of the engine's (the one Dolt makes when it creates a
// database) is not one of them. Each commit's date must be the synthetic date of its
// ordinal: a history that is not a function of the stream is not the engine's.
func (e *Engine) readCommits(ctx context.Context, tx *sql.Tx) ([]Commit, error) {
	rows, err := tx.QueryContext(ctx, "SELECT commit_hash, message, date FROM dolt_log ORDER BY date ASC, commit_order ASC")
	if err != nil {
		return nil, fmt.Errorf("doltengine: reading the commit log: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var out []Commit
	for rows.Next() {
		var hash, msg string
		var date []byte
		if err := rows.Scan(&hash, &msg, &date); err != nil {
			return nil, fmt.Errorf("doltengine: reading the commit log: %w", err)
		}
		kind, token, ok := parseMessage(msg)
		if !ok {
			continue
		}
		if !commitHash.MatchString(hash) {
			return nil, fmt.Errorf("doltengine: the commit log holds %q, not a commit hash: %w", hash, ErrSchema)
		}
		c := Commit{Ordinal: len(out) + 1, Kind: kind, Token: token, Message: msg, Hash: hash, Date: doltmodel.CommitDate(len(out) + 1)}
		got, err := parseDate(date)
		if err != nil || !got.Equal(c.Date) {
			return nil, fmt.Errorf("doltengine: commit %d (%q) is dated %s, the stream's date is %s: %w", c.Ordinal, msg, date, c.Date.Format(time.RFC3339), ErrSchema)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("doltengine: reading the commit log: %w", err)
	}
	return out, nil
}

// parseDate reads a date the way the commit log's date column prints it.
func parseDate(b []byte) (time.Time, error) {
	for _, layout := range []string{"2006-01-02 15:04:05.999999999", "2006-01-02T15:04:05.999999999Z07:00", "2006-01-02 15:04:05"} {
		if t, err := time.ParseInLocation(layout, string(b), time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("doltengine: %q is not a date", b)
}

func producerOf(b []byte) lifecycle.Producer { return lifecycle.Producer(b) }
