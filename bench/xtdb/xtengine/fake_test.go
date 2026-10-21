package xtengine

import (
	"cmp"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// A fake XTDB node, for the tests that need no server. It is not a SQL engine: it
// understands exactly the statements the engine writes (their shapes are the
// engine's own), and it holds the tables as XTDB is taken to hold them (see the
// package documentation): every document is a set of versions, each over a stretch
// of valid time and a stretch of system time, an insert over a stretch of valid time
// overwrites that stretch and a portion delete removes it, a transaction's statements
// apply in order at the transaction's system time, which must be later than the last,
// and a read-only transaction reads the node as it was when it began.
//
// A statement it does not understand fails, and so does a read that names no valid
// time: a test that passes against it has sent statements of the known shapes.

const forever = math.MaxInt64 // valid or system time: the end of time, in microseconds

type fver struct {
	vFrom, vTo int64
	sFrom, sTo int64
	cols       map[string]any
}

type fdoc struct {
	live, closed []fver
}

type ftable struct {
	docs map[any]*fdoc
	// byFP indexes the documents by their fp column, which every read names, so that a
	// read does not scan the table.
	byFP map[string][]any
}

type fakeNode struct {
	mu      sync.Mutex
	tables  map[string]*ftable
	lastSys int64
	// failExec, if set, is asked before each statement a connection runs, and may fail
	// it; its arguments are the connection's name and the statement.
	failExec func(conn, sql string) error
	// hang, if set, makes a statement wait until its context is done (or, if it has no
	// deadline, for two seconds, and then fail with errFakeHung): a node that does not answer.
	hang func(conn, sql string) bool
	// loseCommit makes the commit of a write transaction return success and apply nothing: an
	// indexer that has failed and says nothing.
	loseCommit bool
	// statements are the statements run, by connection, in order.
	statements map[string][]loggedStmt
}

// loggedStmt is a statement a connection ran, as it was sent.
type loggedStmt struct {
	sql  string
	args []any
}

func newFakeNode() *fakeNode {
	return &fakeNode{tables: make(map[string]*ftable), statements: make(map[string][]loggedStmt)}
}

func (n *fakeNode) conn(name string) *fakeConn { return &fakeConn{node: n, name: name} }

type fakeTx struct {
	write bool
	sys   int64 // a write transaction's system time; a read one's snapshot
	ops   []func(n *fakeNode, sys int64)
}

// fakeConn is a connection to the fake node.
type fakeConn struct {
	node   *fakeNode
	name   string
	tx     *fakeTx
	closed bool
}

func (c *fakeConn) Close(context.Context) error { c.closed = true; return nil }

var (
	reBeginWrite = regexp.MustCompile(`^BEGIN READ WRITE WITH \(SYSTEM_TIME = TIMESTAMP '([0-9T:.\-]+Z)'\)$`)
	reInsert     = regexp.MustCompile(`^INSERT INTO (\w+) \(([\w, ]+)\) VALUES \(([^()]*(?:CAST\([^()]*\)[^()]*)*)\)$`)
	reDelete     = regexp.MustCompile(`^DELETE FROM (\w+) FOR PORTION OF VALID_TIME FROM \$(\d+) TO \$(\d+) WHERE _id = \$(\d+)$`)
	reSelect     = regexp.MustCompile(`^SELECT (.+?) FROM (\w+)((?: FOR (?:VALID_TIME|SYSTEM_TIME) (?:ALL|AS OF \$\d+))*)(?: WHERE (.+?))?(?: ORDER BY (.+?))?(?: LIMIT (\d+))?$`)
	reClause     = regexp.MustCompile(`FOR (VALID_TIME|SYSTEM_TIME) (ALL|AS OF \$(\d+))`)
	rePlace      = regexp.MustCompile(`^(?:CAST\()?\$(\d+)(?: AS (BIGINT|VARBINARY)\))?$`)
)

// micros is a timestamp argument in microseconds, or the end of time for none.
func micros(v any) (int64, error) {
	switch t := v.(type) {
	case nil:
		return forever, nil
	case time.Time:
		return t.UnixMicro(), nil
	}
	return 0, fmt.Errorf("fake: %T is not a timestamp", v)
}

// errFakeHung is what a statement that hangs returns when its context never ends.
var errFakeHung = errors.New("fake: the node did not answer")

// waitHang blocks a statement that the node is made to hang on.
func (n *fakeNode) waitHang(ctx context.Context, conn, sql string) error {
	n.mu.Lock()
	hang := n.hang
	n.mu.Unlock()
	if hang == nil || !hang(conn, sql) {
		return nil
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(2 * time.Second):
		return errFakeHung
	}
}

// noBytes refuses a byte slice as an argument: the driver's simple protocol would write it as
// a quoted string, and the node cannot cast that to a binary value (see [hexBytes]).
func noBytes(args []any) error {
	for _, a := range args {
		if _, ok := a.([]byte); ok {
			return errors.New("fake: bytes sent as an argument, which the simple protocol writes as text")
		}
	}
	return nil
}

func (c *fakeConn) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	n := c.node
	if err := n.waitHang(ctx, c.name, sql); err != nil {
		return pgconn.CommandTag{}, err
	}
	if err := noBytes(args); err != nil {
		return pgconn.CommandTag{}, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.statements[c.name] = append(n.statements[c.name], loggedStmt{sql, slices.Clone(args)})
	if n.failExec != nil {
		if err := n.failExec(c.name, sql); err != nil {
			return pgconn.CommandTag{}, err
		}
	}
	simple := len(args) == 1 && args[0] == pgx.QueryExecModeSimpleProtocol
	switch {
	case sql == "ROLLBACK":
		c.tx = nil
		return pgconn.NewCommandTag("ROLLBACK"), nil
	case sql == "BEGIN READ ONLY":
		if c.tx != nil {
			return pgconn.CommandTag{}, errors.New("fake: a transaction is already open")
		}
		c.tx = &fakeTx{sys: n.lastSys}
		return pgconn.NewCommandTag("BEGIN"), nil
	case reBeginWrite.MatchString(sql):
		if c.tx != nil {
			return pgconn.CommandTag{}, errors.New("fake: a transaction is already open")
		}
		t, err := time.Parse("2006-01-02T15:04:05.999999Z", reBeginWrite.FindStringSubmatch(sql)[1])
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		c.tx = &fakeTx{write: true, sys: t.UnixMicro()}
		return pgconn.NewCommandTag("BEGIN"), nil
	case sql == "COMMIT":
		tx := c.tx
		c.tx = nil
		if tx == nil {
			return pgconn.CommandTag{}, errors.New("fake: no transaction")
		}
		if !tx.write {
			return pgconn.NewCommandTag("COMMIT"), nil
		}
		if tx.sys <= n.lastSys {
			return pgconn.CommandTag{}, &pgconn.PgError{Message: fmt.Sprintf("fake: system time %d is not after the last, %d", tx.sys, n.lastSys)}
		}
		if n.loseCommit {
			return pgconn.NewCommandTag("COMMIT"), nil
		}
		for _, op := range tx.ops {
			op(n, tx.sys)
		}
		n.lastSys = tx.sys
		return pgconn.NewCommandTag("COMMIT"), nil
	case simple:
		return pgconn.CommandTag{}, fmt.Errorf("fake: control statement %q", sql)
	}
	if c.tx == nil || !c.tx.write {
		return pgconn.CommandTag{}, fmt.Errorf("fake: %.50q outside a write transaction", sql)
	}
	if m := reInsert.FindStringSubmatch(sql); m != nil {
		op, err := n.insert(m[1], m[2], m[3], args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		c.tx.ops = append(c.tx.ops, op)
		return pgconn.NewCommandTag("INSERT 0 0"), nil
	}
	if m := reDelete.FindStringSubmatch(sql); m != nil {
		op, err := n.deletePortion(m, args)
		if err != nil {
			return pgconn.CommandTag{}, err
		}
		c.tx.ops = append(c.tx.ops, op)
		return pgconn.NewCommandTag("DELETE 0"), nil
	}
	return pgconn.CommandTag{}, fmt.Errorf("fake: cannot execute %.80q", sql)
}

// argAt is the argument a placeholder "$n" names.
func argAt(ph string, args []any) (any, error) {
	m := rePlace.FindStringSubmatch(strings.TrimSpace(ph))
	if m == nil {
		return nil, fmt.Errorf("fake: %q is not a placeholder", ph)
	}
	i, _ := strconv.Atoi(m[1])
	if i < 1 || i > len(args) {
		return nil, fmt.Errorf("fake: placeholder $%d of %d arguments", i, len(args))
	}
	a := args[i-1]
	// The driver's simple protocol sends every argument as a quoted string, so what a cast
	// would make of it is made here, and a number that is not cast stays a string.
	switch m[2] {
	case "BIGINT":
		switch v := a.(type) {
		case int64:
			return v, nil
		case string:
			n, err := strconv.ParseInt(v, 10, 64)
			return n, err
		}
		return nil, fmt.Errorf("fake: %T cannot be cast to a bigint", a)
	case "VARBINARY":
		s, ok := a.(string)
		if !ok {
			return nil, fmt.Errorf("fake: %T cannot be cast to a binary value", a)
		}
		b, err := hex.DecodeString(s)
		if err != nil {
			return nil, err
		}
		return b, nil
	}
	if _, isInt := a.(int64); isInt {
		return nil, fmt.Errorf("fake: an int64 argument that is not cast reaches the node as a string")
	}
	return a, nil
}

func (n *fakeNode) table(name string) *ftable {
	t := n.tables[name]
	if t == nil {
		t = &ftable{docs: make(map[any]*fdoc), byFP: make(map[string][]any)}
		n.tables[name] = t
	}
	return t
}

func (n *fakeNode) insert(table, cols, places string, args []any) (func(*fakeNode, int64), error) {
	names := strings.Split(cols, ", ")
	ps := strings.Split(places, ", ")
	if len(names) != len(ps) || len(ps) != len(args) {
		return nil, fmt.Errorf("fake: %d columns, %d placeholders, %d arguments", len(names), len(ps), len(args))
	}
	doc := make(map[string]any, len(names))
	for i, name := range names {
		a, err := argAt(ps[i], args)
		if err != nil {
			return nil, err
		}
		if m := rePlace.FindStringSubmatch(strings.TrimSpace(ps[i])); m == nil || m[1] != strconv.Itoa(i+1) {
			return nil, fmt.Errorf("fake: placeholders out of order: %s", places)
		}
		doc[name] = a
	}
	id, ok := doc["_id"]
	if !ok {
		return nil, errors.New("fake: an insert without an _id")
	}
	hasFrom, hasTo := false, false
	var from, to int64
	var err error
	if v, ok := doc["_valid_from"]; ok {
		hasFrom = true
		if from, err = micros(v); err != nil {
			return nil, err
		}
		delete(doc, "_valid_from")
	}
	if v, ok := doc["_valid_to"]; ok {
		hasTo = true
		if to, err = micros(v); err != nil {
			return nil, err
		}
		delete(doc, "_valid_to")
	}
	delete(doc, "_id")
	if hasFrom && hasTo && from >= to {
		return nil, fmt.Errorf("fake: valid time [%d, %d) is empty", from, to)
	}
	return func(n *fakeNode, sys int64) {
		f, t := from, to
		if !hasFrom {
			f = sys
		}
		if !hasTo {
			t = forever
		}
		n.table(table).paint(id, f, t, sys, doc)
	}, nil
}

func (n *fakeNode) deletePortion(m []string, args []any) (func(*fakeNode, int64), error) {
	fv, err := argAt("$"+m[2], args)
	if err != nil {
		return nil, err
	}
	tv, err := argAt("$"+m[3], args)
	if err != nil {
		return nil, err
	}
	id, err := argAt("$"+m[4], args)
	if err != nil {
		return nil, err
	}
	from, err := micros(fv)
	if err != nil {
		return nil, err
	}
	to, err := micros(tv)
	if err != nil {
		return nil, err
	}
	if from >= to {
		return nil, fmt.Errorf("fake: valid time [%d, %d) is empty", from, to)
	}
	table := m[1]
	return func(n *fakeNode, sys int64) { n.table(table).paint(id, from, to, sys, nil) }, nil
}

// paint applies a statement at a system time: the document has these columns over
// [from, to) if cols is not nil and is absent there otherwise.
func (t *ftable) paint(id any, from, to, sys int64, cols map[string]any) {
	d := t.docs[id]
	if d == nil {
		if cols == nil {
			return
		}
		d = &fdoc{}
		t.docs[id] = d
		if fp, ok := cols["fp"].(string); ok {
			t.byFP[fp] = append(t.byFP[fp], id)
		}
	}
	var next []fver
	for _, v := range d.live {
		if v.vTo <= from || v.vFrom >= to {
			next = append(next, v)
			continue
		}
		if v.sFrom < sys {
			d.closed = append(d.closed, fver{v.vFrom, v.vTo, v.sFrom, sys, v.cols})
		}
		if v.vFrom < from {
			next = append(next, fver{v.vFrom, from, sys, forever, v.cols})
		}
		if v.vTo > to {
			next = append(next, fver{to, v.vTo, sys, forever, v.cols})
		}
	}
	if cols != nil {
		next = append(next, fver{from, to, sys, forever, cols})
	}
	slices.SortFunc(next, func(a, b fver) int { return cmp.Compare(a.vFrom, b.vFrom) })
	d.live = next
}

// cond is a conjunct of a WHERE clause.
type cond struct {
	col, op string
	args    []any
}

func (c cond) holds(v any) bool {
	switch c.op {
	case "IN":
		for _, a := range c.args {
			if cmpAny(v, a) == 0 {
				return true
			}
		}
		return false
	}
	r := cmpAny(v, c.args[0])
	switch c.op {
	case "=":
		return r == 0
	case ">=":
		return r >= 0
	case ">":
		return r > 0
	case "<=":
		return r <= 0
	case "<":
		return r < 0
	}
	return false
}

// cmpAny compares two column values of one type; a null is not equal to anything.
func cmpAny(a, b any) int {
	switch x := a.(type) {
	case int64:
		if y, ok := b.(int64); ok {
			return cmp.Compare(x, y)
		}
	case string:
		if y, ok := b.(string); ok {
			return strings.Compare(x, y)
		}
	case bool:
		if y, ok := b.(bool); ok && x == y {
			return 0
		}
	}
	return 2 // incomparable: only "<>" would be true, and nothing asks it
}

var reCond = regexp.MustCompile(`^(\w+) (=|>=|<=|>|<|IN) (.+)$`)

func parseWhere(where string, args []any) ([]cond, error) {
	if where == "" {
		return nil, nil
	}
	var out []cond
	for _, part := range strings.Split(where, " AND ") {
		m := reCond.FindStringSubmatch(part)
		if m == nil {
			return nil, fmt.Errorf("fake: cannot read the condition %q", part)
		}
		c := cond{col: m[1], op: m[2]}
		rhs := m[3]
		if c.op == "IN" {
			rhs = strings.TrimSuffix(strings.TrimPrefix(rhs, "("), ")")
			for ph := range strings.SplitSeq(rhs, ", ") {
				a, err := argAt(ph, args)
				if err != nil {
					return nil, err
				}
				c.args = append(c.args, a)
			}
		} else {
			a, err := argAt(rhs, args)
			if err != nil {
				return nil, err
			}
			c.args = []any{a}
		}
		out = append(out, c)
	}
	return out, nil
}

func (c *fakeConn) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	n := c.node
	if err := n.waitHang(ctx, c.name, sql); err != nil {
		return nil, err
	}
	if err := noBytes(args); err != nil {
		return nil, err
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	n.statements[c.name] = append(n.statements[c.name], loggedStmt{sql, slices.Clone(args)})
	if n.failExec != nil {
		if err := n.failExec(c.name, sql); err != nil {
			return nil, err
		}
	}
	m := reSelect.FindStringSubmatch(sql)
	if m == nil {
		return nil, fmt.Errorf("fake: cannot query %.80q", sql)
	}
	cols, table, clauses, where, order, limit := strings.Split(m[1], ", "), m[2], m[3], m[4], m[5], m[6]
	conds, err := parseWhere(where, args)
	if err != nil {
		return nil, err
	}
	// The snapshot: a read-only transaction's, or the newest.
	snap := n.lastSys
	if c.tx != nil && !c.tx.write {
		snap = c.tx.sys
	}
	validAll, validAt, haveValid := false, int64(0), false
	sysAt, haveSys := snap, false
	for _, cl := range reClause.FindAllStringSubmatch(clauses, -1) {
		var at int64
		all := cl[2] == "ALL"
		if !all {
			a, err := argAt("$"+cl[3], args)
			if err != nil {
				return nil, err
			}
			if at, err = micros(a); err != nil {
				return nil, err
			}
		}
		switch cl[1] {
		case "VALID_TIME":
			validAll, validAt, haveValid = all, at, true
		case "SYSTEM_TIME":
			if all {
				return nil, errors.New("fake: FOR SYSTEM_TIME ALL is not used")
			}
			sysAt, haveSys = at, true
		}
	}
	_ = haveSys
	if !haveValid {
		return nil, fmt.Errorf("fake: %.60q reads no valid time: the default is the wall clock", sql)
	}
	type row map[string]any
	var rows []row
	tbl := n.table(table)
	candidates := func(yield func(id any, d *fdoc) bool) {
		for _, cd := range conds {
			if cd.col != "fp" || (cd.op != "=" && cd.op != "IN") {
				continue
			}
			for _, a := range cd.args {
				fp, _ := a.(string)
				for _, id := range tbl.byFP[fp] {
					if !yield(id, tbl.docs[id]) {
						return
					}
				}
			}
			return
		}
		for id, d := range tbl.docs {
			if !yield(id, d) {
				return
			}
		}
	}
	for id, d := range candidates {
		for _, v := range append(slices.Clone(d.live), d.closed...) {
			if v.sFrom > sysAt || sysAt >= v.sTo {
				continue
			}
			if !validAll && (v.vFrom > validAt || validAt >= v.vTo) {
				continue
			}
			r := row{"_id": id}
			for k, x := range v.cols {
				r[k] = x
			}
			ok := true
			for _, cd := range conds {
				val, present := r[cd.col]
				if !present || val == nil || !cd.holds(val) {
					ok = false
					break
				}
			}
			if ok {
				rows = append(rows, r)
			}
		}
	}
	if len(cols) == 1 && strings.HasPrefix(cols[0], "min(") {
		col := strings.TrimSuffix(strings.TrimPrefix(cols[0], "min("), ")")
		var best any
		for _, r := range rows {
			if x := r[col]; x != nil && (best == nil || cmpAny(x, best) < 0) {
				best = x
			}
		}
		return &fakeRows{rows: [][]any{{best}}}, nil
	}
	if order != "" {
		keys := strings.Split(order, ", ")
		slices.SortFunc(rows, func(a, b row) int {
			for _, k := range keys {
				if c := cmpAny(a[k], b[k]); c != 0 {
					return c
				}
			}
			return 0
		})
	}
	if limit != "" {
		l, _ := strconv.Atoi(limit)
		if len(rows) > l {
			rows = rows[:l]
		}
	}
	out := &fakeRows{}
	for _, r := range rows {
		vals := make([]any, len(cols))
		for i, k := range cols {
			if _, ok := r[k]; !ok && k != "_id" {
				return nil, fmt.Errorf("fake: the table %s has no column %s", table, k)
			}
			vals[i] = r[k]
		}
		out.rows = append(out.rows, vals)
	}
	return out, nil
}

// fakeRows is the result of a query.
type fakeRows struct {
	rows [][]any
	i    int
	err  error
}

func (r *fakeRows) Close()                                       {}
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.NewCommandTag("SELECT") }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return nil }
func (r *fakeRows) Next() bool {
	r.i++
	return r.i <= len(r.rows)
}
func (r *fakeRows) Values() ([]any, error) { return r.rows[r.i-1], nil }

func (r *fakeRows) Scan(dest ...any) error {
	row := r.rows[r.i-1]
	if len(dest) != len(row) {
		return fmt.Errorf("fake: %d values, %d destinations", len(row), len(dest))
	}
	for i, d := range dest {
		v := row[i]
		switch p := d.(type) {
		case *string:
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("fake: column %d is %T, not a string", i, v)
			}
			*p = s
		case *int64:
			x, ok := v.(int64)
			if !ok {
				return fmt.Errorf("fake: column %d is %T, not an int64", i, v)
			}
			*p = x
		case *bool:
			x, ok := v.(bool)
			if !ok {
				return fmt.Errorf("fake: column %d is %T, not a bool", i, v)
			}
			*p = x
		case **int64:
			if v == nil {
				*p = nil
				continue
			}
			x, ok := v.(int64)
			if !ok {
				return fmt.Errorf("fake: column %d is %T, not an int64", i, v)
			}
			*p = &x
		case *[]byte:
			if v == nil {
				*p = nil
				continue
			}
			x, ok := v.([]byte)
			if !ok {
				return fmt.Errorf("fake: column %d is %T, not bytes", i, v)
			}
			*p = slices.Clone(x)
		default:
			return fmt.Errorf("fake: cannot scan into %T", d)
		}
	}
	return nil
}
