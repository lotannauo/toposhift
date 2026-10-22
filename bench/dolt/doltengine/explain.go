package doltengine

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
)

// Statement is a statement a read sends to the server and the arguments it sends with
// it, as the engine builds them.
type Statement struct {
	// Kind says which statement of the design it is: "rule" (the state as of a commit
	// and the log for the keys it cannot settle), "log" (no commit to read: the log
	// alone), "alive-rule", "alive-log" or "window".
	Kind string
	Text string
	Args []any
}

// ReadStatements returns the statements the engine would send to answer a question of a
// plan, as it would build them now: the view of the question's token, the commit the
// state table is read as of, and the arguments. A question the engine answers without a
// query (an instant before the range, a window with no extent) has none.
func (e *Engine) ReadStatements(q runner.Query) ([]Statement, error) {
	sc := engine.Scope{Layer: q.Layer, AsOf: q.AsOf}
	if err := sc.Validate(); err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	hash, have := e.view(sc.AsOf)
	switch q.Op {
	case runner.OpNeighbors, runner.OpBatch:
		at, ok := doltmodel.ClampInstant(q.At)
		if !ok || len(q.Fps) == 0 {
			return nil, nil
		}
		batch := q.Op == runner.OpBatch
		s, err := e.neighborStatement(q.Fps, batch, q.Dir, at, sc, hash, have)
		if err != nil {
			return nil, err
		}
		return []Statement{{Kind: statementKind(have, "rule", "log"), Text: s.text, Args: s.args}}, nil
	case runner.OpAlive:
		at, ok := doltmodel.ClampInstant(q.At)
		if !ok || len(q.Fps) == 0 {
			return nil, nil
		}
		s, err := e.aliveStatement(q.Fps[0], at, sc, hash, have)
		if err != nil {
			return nil, err
		}
		return []Statement{{Kind: statementKind(have, "alive-rule", "alive-log"), Text: s.text, Args: s.args}}, nil
	case runner.OpWindow:
		b := doltmodel.ClampWindow(q.From, q.To)
		if b.Empty || len(q.Fps) == 0 {
			return nil, nil
		}
		s := windowSQL(int(sc.Layer), int(q.Dir), doltmodel.EncodeFingerprint(q.Fps[0]), b.Lo, b.Hi, b.Unbounded, doltmodel.EncodeSeq(sc.AsOf), &e.f)
		return []Statement{{Kind: "window", Text: s.text, Args: s.args}}, nil
	}
	return nil, fmt.Errorf("doltengine: no read %q", q.Op)
}

func statementKind(have bool, withState, without string) string {
	if have {
		return withState
	}
	return without
}

// Literal writes an argument as the text of a SQL literal, for the one place a statement
// is sent with its arguments in its text: EXPLAIN, which the server takes no arguments
// for. An integer is its digits, bytes are a hexadecimal literal, text is quoted with its
// quotes and backslashes doubled; nothing else is an argument of a read.
func Literal(arg any) (string, error) {
	switch v := arg.(type) {
	case int:
		return strconv.Itoa(v), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case uint64:
		return strconv.FormatUint(v, 10), nil
	case []byte:
		if len(v) == 0 {
			return "X''", nil
		}
		return "0x" + hex.EncodeToString(v), nil
	case string:
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `''`).Replace(v) + "'", nil
	}
	return "", fmt.Errorf("doltengine: no literal for an argument of type %T", arg)
}

// Interpolate is the statement with its arguments written into its text. It refuses a
// statement whose placeholders and arguments are not as many.
func (s Statement) Interpolate() (string, error) {
	// A placeholder is a question mark outside a quoted string; the statements of the
	// engine hold none inside one (a test holds them to it).
	if n := strings.Count(s.Text, "?"); n != len(s.Args) {
		return "", fmt.Errorf("doltengine: a statement with %d placeholders and %d arguments", n, len(s.Args))
	}
	var b strings.Builder
	i := 0
	for _, r := range s.Text {
		if r != '?' {
			b.WriteRune(r)
			continue
		}
		lit, err := Literal(s.Args[i])
		if err != nil {
			return "", err
		}
		b.WriteString(lit)
		i++
	}
	return b.String(), nil
}

// Explain returns the server's plan, as the lines of EXPLAIN FORMAT=TREE, of a statement,
// with its arguments written into it, in one read-only transaction of the reader. The
// statement is not run.
func (e *Engine) Explain(ctx context.Context, s Statement) ([]string, error) {
	text, err := s.Interpolate()
	if err != nil {
		return nil, err
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	tx, err := e.rdb.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("doltengine: explain: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, "EXPLAIN FORMAT=TREE "+text)
	if err != nil {
		return nil, fmt.Errorf("doltengine: explain: %w", err)
	}
	defer func() { _ = rows.Close() }()
	var lines []string
	for rows.Next() {
		var l string
		if err := rows.Scan(&l); err != nil {
			return nil, fmt.Errorf("doltengine: explain: %w", err)
		}
		lines = append(lines, l)
	}
	return lines, rows.Err()
}
