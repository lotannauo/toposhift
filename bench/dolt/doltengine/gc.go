package doltengine

import (
	"context"
	"errors"
	"fmt"

	"github.com/go-sql-driver/mysql"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// ErrBusy is wrapped by the error [GC] returns when a connection other than its own is open
// in the database: garbage collection breaks every open connection of the database, so it
// is run only with none.
var ErrBusy = errors.New("doltengine: a connection is open in the database")

// GC runs garbage collection on a database ("CALL DOLT_GC('--full')": everything no commit
// references is dropped, the rest is rewritten). The server documents that it blocks writes
// and breaks every open connection of the database, so GC refuses, with [ErrBusy], a
// database that has any other connection open in it, and the caller closes its engines
// first and opens them again afterwards. It uses a connection of its own, which it closes.
//
// Commits keep every row they reference: GC frees nothing a commit still holds, so in the arm
// with a commit per batch the rows that retention deleted stay.
func GC(ctx context.Context, dsn, database string) error {
	if !dbName.MatchString(database) {
		return fmt.Errorf("doltengine: %q is not a usable database name: %w", database, engine.ErrInvalid)
	}
	dc, err := mysql.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("doltengine: DSN: %w", err)
	}
	dc.InterpolateParams = InterpolateParams
	dc.MultiStatements = false
	admin, err := connect(dc, "")
	if err != nil {
		return err
	}
	defer func() { _ = admin.Close() }()
	if err := CheckServer(ctx, admin); err != nil {
		return err
	}
	var n int
	if err := admin.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.processlist WHERE db = ?", database).Scan(&n); err != nil {
		return fmt.Errorf("doltengine: listing the connections of %s: %w", database, err)
	}
	if n > 0 {
		return fmt.Errorf("doltengine: %d connections are open in database %s: %w", n, database, ErrBusy)
	}
	// A connection to the database would itself be listed, so the call is made from the
	// administrative one, naming the database in the session.
	conn, err := admin.Conn(ctx)
	if err != nil {
		return fmt.Errorf("doltengine: %w", err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "USE `"+database+"`"); err != nil {
		return fmt.Errorf("doltengine: using %s: %w", database, err)
	}
	if _, err := conn.ExecContext(ctx, "CALL DOLT_GC('--full')"); err != nil {
		return fmt.Errorf("doltengine: garbage collection of %s: %w", database, err)
	}
	return nil
}
