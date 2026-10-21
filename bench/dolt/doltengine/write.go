package doltengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// step is one change of the database that ends in a commit: a batch or a retention. Its
// statements run in a transaction of the writer's connection; then, with the engine's
// view locked, the Dolt commit is made, the transaction ends, and the engine's memory
// moves to the new state. The sequence number rises in that last step, after both have
// returned.
type step struct {
	kind    doltmodel.CommitKind
	token   uint64
	message func(ordinal int) string
	apply   func(ctx context.Context, tx *sql.Tx) error
	// early is what the defect that publishes before the commits return does.
	early func()
	// publish moves the writer's memory, once the step has committed.
	publish func(c *Commit)
}

// run makes a step. A failure before the commit leaves the database and the engine as they
// were. A failure to end the transaction after the Dolt commit was made leaves the engine
// unable to say what the database holds, and it refuses everything after that: it has to
// be opened again, which rebuilds it from the database.
func (e *Engine) run(s step) error {
	ctx := e.ctx
	tx, err := e.wdb.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := s.apply(ctx, tx); err != nil {
		return err
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.f.publishBeforeCommit && s.early != nil {
		s.early()
	}
	var c *Commit
	if e.arm == doltmodel.ArmCommit {
		ordinal := len(e.commits) + 1
		msg := s.message(ordinal)
		text, err := commitStatement(ordinal, msg, &e.f)
		if err != nil {
			return err
		}
		hash, err := commitCall(ctx, tx, text)
		if err != nil {
			return err
		}
		c = &Commit{Ordinal: ordinal, Kind: s.kind, Token: s.token, Message: msg, Date: doltmodel.CommitDate(ordinal), Hash: hash}
	}
	if err := tx.Commit(); err != nil {
		e.fatal = fmt.Errorf("doltengine: ending the transaction after its commit: %w; open the engine again", err)
		return e.fatal
	}
	s.publish(c)
	if c != nil {
		e.commits = append(e.commits, *c)
		e.byToken[c.Token] = len(e.commits) - 1
	}
	return nil
}

// commitCall makes the Dolt commit and returns its hash.
func commitCall(ctx context.Context, tx *sql.Tx, text string) (string, error) {
	rows, err := tx.QueryContext(ctx, text)
	if err != nil {
		return "", fmt.Errorf("doltengine: committing: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var hash string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return "", fmt.Errorf("doltengine: committing: %w", err)
		}
		return "", errors.New("doltengine: the commit returned no hash")
	}
	if err := rows.Scan(&hash); err != nil {
		return "", fmt.Errorf("doltengine: committing: %w", err)
	}
	if err := rows.Close(); err != nil {
		return "", fmt.Errorf("doltengine: committing: %w", err)
	}
	if !commitHash.MatchString(hash) {
		return "", fmt.Errorf("doltengine: the commit returned %q, not a commit hash", hash)
	}
	return hash, nil
}

// Write implements [engine.Engine]. The batch is validated whole before anything is sent, so
// a refused batch changes nothing; then its records are inserted in the logs and the state
// rows they supersede are replaced, in one transaction, which is committed in Dolt as
// one commit (in the arm with commits) before the sequence number rises.
func (e *Engine) Write(batch []engine.Record) error {
	if len(batch) == 0 {
		return nil
	}
	e.wmu.Lock()
	defer e.wmu.Unlock()
	if e.fatal != nil {
		return e.fatal
	}
	p, capacityRefused, err := e.w.plan(batch)
	if err != nil {
		if capacityRefused {
			e.addCounts(doltmodel.Counts{PayloadRefusals: 1})
		}
		return err
	}
	return e.run(step{
		kind: doltmodel.BatchCommit, token: p.last,
		message: func(int) string { return batchMessage(p.last) },
		apply:   func(ctx context.Context, tx *sql.Tx) error { return applyBatch(ctx, tx, p) },
		early:   func() { e.lastSeq = p.last },
		publish: func(c *Commit) {
			e.w.commit(p)
			e.lastSeq = p.last
			counts := p.counts
			if c != nil {
				counts.Commits++
			}
			e.addCounts(counts)
		},
	})
}

// applyBatch sends the rows of a plan: the logs, the state rows that replace the stored
// ones, and the meta row with the new token.
func applyBatch(ctx context.Context, tx *sql.Tx, p *batchPlan) error {
	if err := insertAll(ctx, tx, "INSERT", "log", logCols, p.log, logArgs); err != nil {
		return err
	}
	if err := insertAll(ctx, tx, "INSERT", "ent_log", entLogCols, p.entLog, entLogArgs); err != nil {
		return err
	}
	if err := insertAll(ctx, tx, "REPLACE", "state", stateCols, p.state, stateArgs); err != nil {
		return err
	}
	if err := insertAll(ctx, tx, "REPLACE", "ent_state", entStateCols, p.entState, entStateArgs); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE meta SET last_seq = ? WHERE id = 1", doltmodel.EncodeSeq(p.last)); err != nil {
		return fmt.Errorf("doltengine: updating the meta row: %w", err)
	}
	return nil
}

// insertAll sends rows in as few statements as the placeholder limit allows.
func insertAll[R any](ctx context.Context, tx *sql.Tx, verb, table string, cols []string, rows []R, args func(R) []any) error {
	per := rowsPerStatement(len(cols))
	for len(rows) > 0 {
		n := min(per, len(rows))
		all := make([]any, 0, n*len(cols))
		for _, r := range rows[:n] {
			all = append(all, args(r)...)
		}
		if _, err := tx.ExecContext(ctx, insertRows(verb, table, cols, n), all...); err != nil {
			return fmt.Errorf("doltengine: %s into %s: %w", verb, table, err)
		}
		rows = rows[n:]
	}
	return nil
}

// A binary column is bound as bytes, so that no character set can change what is stored;
// an absent Through or deadline is NULL.
func logArgs(r doltmodel.LogRow) []any {
	var through any
	if r.HasThrough {
		through = r.ThroughNs
	}
	return []any{r.Layer, r.Dir, []byte(r.A), []byte(r.Peer), []byte(r.Rel), []byte(r.Producer), r.EventNs, r.Seq, r.Kind, r.TTLNs, through, []byte(r.Payload)}
}

func entLogArgs(r doltmodel.EntLogRow) []any {
	var through any
	if r.HasThrough {
		through = r.ThroughNs
	}
	return []any{[]byte(r.Fp), []byte(r.Producer), r.Layer, r.EventNs, r.Seq, r.Kind, r.TTLNs, through, []byte(r.Payload)}
}

func stateArgs(r doltmodel.StateRow) []any {
	var deadline any
	if !r.Open {
		deadline = r.DeadlineNs
	}
	return []any{r.Layer, r.Dir, []byte(r.A), []byte(r.Peer), []byte(r.Rel), []byte(r.Producer), r.EventNs, r.Seq, r.Kind, deadline}
}

func entStateArgs(r doltmodel.EntStateRow) []any {
	var deadline any
	if !r.Open {
		deadline = r.DeadlineNs
	}
	return []any{[]byte(r.Fp), []byte(r.Producer), r.Layer, r.EventNs, r.Seq, r.Kind, deadline}
}
