package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

const queryUsage = `Usage: toposhift query OP --data-dir DIR [flags]

Asks a store that toposhift replay filled a question. Every answer is JSON
lines, one object per fact; every object carries the snapshot token it was
answered at (asof), and every identifier is shown as the fingerprint text next to
its entity type.

Operations:
  neighbors --fp F --dir fwd|rev --at T       the edges alive at T that touch F
  alive     --fp F --at T                     whether F exists at T
  window    --fp F --dir fwd|rev --from T --to T
                                              the edge records touching F with
                                              from <= event time < to
  history   --fp F --from T --to T            the existence records of F itself
                                              with from <= event time < to

A fingerprint with no entity records is "no history", an error, for alive and
history. For neighbors and window an empty answer stays an answer (exit 0, no
output) with a note on stderr, since edges can exist for an endpoint that has
no entity records.

Flags:
  --data-dir DIR   the store's directory (required). It is opened read-only,
                   which means a query writes nothing; Pebble's lock on the
                   directory is still exclusive, so a query fails, saying so,
                   while a replay or another query has the directory open
  --layer L0..L3   the layer to read. Without it, alive and history read the
                   layer F's entity type lives in, and neighbors and window read
                   all four layers (every object names its layer)
  --asof N|latest  the snapshot token: records up to Seq N (default latest,
                   shown as the number it resolved to; a number above the
                   store's last Seq is answered at the last, and says so)
  --fp F           a fingerprint, type:32 hex digits
  --dir fwd|rev    forward reads the edges F is the source of, reverse those it
                   is the target of
  --at, --from, --to   RFC 3339 times, such as 2026-01-01T00:10:00Z
`

// queryOp is what an operation requires of the command line.
type queryOp struct {
	needs []string // flags the operation requires; it accepts no others besides --data-dir, --layer and --asof
}

var queryOps = map[string]queryOp{
	"neighbors": {needs: []string{"fp", "dir", "at"}},
	"alive":     {needs: []string{"fp", "at"}},
	"window":    {needs: []string{"fp", "dir", "from", "to"}},
	"history":   {needs: []string{"fp", "from", "to"}},
}

// query is a parsed command line.
type query struct {
	op       string
	dataDir  string
	fp       identity.Fingerprint
	dir      store.Direction
	at       time.Time
	from, to time.Time
	layers   []catalog.Layer // empty: the operation's default
	asof     string
}

// queryCmd is toposhift query.
func queryCmd(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	if len(args) > 0 && args[0] == "help" {
		args = []string{"-h"}
	}
	q, err := parseQueryArgs(args, stdout, stderr)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return finish(stderr, "query", err)
	}
	return finish(stderr, "query", runQuery(ctx, q, stdout, stderr))
}

// parseQueryArgs reads a command line. A mistake in the flags has already been
// printed by the flag package and comes back as a flagError; flag.ErrHelp means
// help was printed to stdout.
func parseQueryArgs(args []string, stdout, stderr io.Writer) (*query, error) {
	fs := flag.NewFlagSet("toposhift query", flag.ContinueOnError)
	var dataDir, fp, dir, at, from, to, layer, asof string
	fs.StringVar(&dataDir, "data-dir", "", "the store's directory")
	fs.StringVar(&fp, "fp", "", "a fingerprint")
	fs.StringVar(&dir, "dir", "", "fwd or rev")
	fs.StringVar(&at, "at", "", "an RFC 3339 time")
	fs.StringVar(&from, "from", "", "an RFC 3339 time")
	fs.StringVar(&to, "to", "", "an RFC 3339 time")
	fs.StringVar(&layer, "layer", "", "L0 to L3")
	fs.StringVar(&asof, "asof", "latest", "a snapshot token or latest")
	ops, err := parseFlags(fs, args, queryUsage, stdout, stderr)
	if err != nil {
		return nil, err
	}
	return parseQuery(ops, fs, dataDir, fp, dir, at, from, to, layer, asof)
}

// parseQuery checks the operation and the flag values against what the operation
// needs, and returns the query they make. A mistake is a usageError.
func parseQuery(ops []string, fs *flag.FlagSet, dataDir, fp, dir, at, from, to, layer, asof string) (*query, error) {
	if len(ops) != 1 {
		return nil, usagef("give exactly one operation: neighbors, alive, window or history")
	}
	op, ok := queryOps[ops[0]]
	if !ok {
		return nil, usagef("unknown operation %q; the operations are neighbors, alive, window and history", ops[0])
	}
	if dataDir == "" {
		return nil, usagef("--data-dir is required")
	}
	set := setFlags(fs)
	for _, need := range op.needs {
		if !set[need] {
			return nil, usagef("%s needs --%s", ops[0], need)
		}
	}
	for name := range set {
		if !slices.Contains(op.needs, name) && !slices.Contains([]string{"data-dir", "layer", "asof"}, name) {
			return nil, usagef("--%s does not apply to %s", name, ops[0])
		}
	}
	q := &query{op: ops[0], dataDir: dataDir, asof: asof}
	var err error
	if set["fp"] {
		if q.fp, err = identity.ParseFingerprint(fp); err != nil {
			return nil, usagef("--fp: %v", err)
		}
	}
	if set["dir"] {
		switch strings.ToLower(dir) {
		case "fwd", "forward":
			q.dir = store.Forward
		case "rev", "reverse":
			q.dir = store.Reverse
		default:
			return nil, usagef("--dir %q is not fwd or rev", dir)
		}
	}
	for _, t := range []struct {
		name string
		in   string
		out  *time.Time
	}{{"at", at, &q.at}, {"from", from, &q.from}, {"to", to, &q.to}} {
		if !set[t.name] {
			continue
		}
		if *t.out, err = time.Parse(time.RFC3339Nano, t.in); err != nil {
			return nil, usagef("--%s %q is not an RFC 3339 time such as 2026-01-01T00:10:00Z", t.name, t.in)
		}
	}
	if set["layer"] {
		l, ok := parseLayer(layer)
		if !ok {
			return nil, usagef("--layer %q is not L0, L1, L2 or L3", layer)
		}
		q.layers = []catalog.Layer{l}
	}
	if asof != "latest" {
		if _, err := strconv.ParseUint(asof, 10, 64); err != nil {
			return nil, usagef("--asof %q is not a sequence number or latest", asof)
		}
	}
	return q, nil
}

// parseLayer reads L0 to L3, in either case.
func parseLayer(s string) (catalog.Layer, bool) {
	for _, l := range []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3} {
		if strings.EqualFold(s, l.String()) {
			return l, true
		}
	}
	return 0, false
}

// The JSON lines. Field order is the order of the struct.

// A neighbor is one edge alive at a time, seen from fp.
type neighborLine struct {
	Op       string `json:"op"`
	AsOf     uint64 `json:"asof"`
	Layer    string `json:"layer"`
	At       string `json:"at"`
	FP       string `json:"fp"`
	FPType   string `json:"fp_type"`
	Dir      string `json:"dir"`
	Peer     string `json:"peer"`
	PeerType string `json:"peer_type"`
	Relation string `json:"relation"`
}

type aliveLine struct {
	Op     string `json:"op"`
	AsOf   uint64 `json:"asof"`
	Layer  string `json:"layer"`
	At     string `json:"at"`
	FP     string `json:"fp"`
	FPType string `json:"fp_type"`
	Alive  bool   `json:"alive"`
}

// A recordLine is one stored record. Through is omitted when the record stands
// for no run, and boot when it carries none; target and relation are omitted for
// an entity.
type recordLine struct {
	Op          string `json:"op"`
	AsOf        uint64 `json:"asof"`
	Layer       string `json:"layer"`
	FP          string `json:"fp"`
	FPType      string `json:"fp_type"`
	Dir         string `json:"dir,omitempty"`
	SubjectKind string `json:"subject_kind"`
	Source      string `json:"source"`
	SourceType  string `json:"source_type"`
	Target      string `json:"target,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Relation    string `json:"relation,omitempty"`
	Producer    string `json:"producer"`
	Kind        string `json:"kind"`
	EventTime   string `json:"event_time"`
	Seq         uint64 `json:"seq"`
	TTLNanos    int64  `json:"ttl_ns"`
	Through     string `json:"through,omitempty"`
	Basis       string `json:"basis"`
	PayloadLen  int    `json:"payload_len"`
	Boot        string `json:"boot,omitempty"`
}

func formatTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func dirName(d store.Direction) string {
	if d == store.Reverse {
		return "reverse"
	}
	return "forward"
}

func newRecordLine(op string, asof uint64, fp identity.Fingerprint, dir store.Direction, r store.Record) recordLine {
	l := recordLine{
		Op: op, AsOf: asof, Layer: r.Layer.String(), FP: fp.String(), FPType: string(fp.Type()),
		SourceType: string(r.Subject.A.Type()), Source: r.Subject.A.String(),
		Producer: string(r.Producer), Kind: r.Kind.String(), EventTime: formatTime(r.EventTime), Seq: r.Seq,
		TTLNanos: int64(r.TTL), Basis: r.EventTimeBasis.String(), PayloadLen: len(r.Payload), Boot: r.Boot,
	}
	if dir != 0 {
		l.Dir = dirName(dir)
	}
	if r.Subject.Kind == store.SubjectEdge {
		l.SubjectKind = "edge"
		l.Target, l.TargetType = r.Subject.B.String(), string(r.Subject.B.Type())
		l.Relation = string(r.Subject.Relation)
	} else {
		l.SubjectKind = "entity"
	}
	if !r.Through.IsZero() {
		l.Through = formatTime(r.Through)
	}
	return l
}

// runQuery opens the store read-only, answers the query and prints its lines.
// Nothing is printed unless the whole question was answered.
func runQuery(ctx context.Context, q *query, stdout, stderr io.Writer) (err error) {
	st, err := openQueryStore(q.dataDir)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := st.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing the store: %w", cerr)
		}
	}()
	asof, clamped := q.resolveAsOf(st.LastSeq())
	if clamped {
		_, _ = fmt.Fprintf(stderr, "toposhift query: --asof %s is above the store's last seq; answered at seq %d\n", q.asof, asof)
	}
	lines, note, err := answer(ctx, st, q, asof)
	if err != nil {
		return err
	}
	if note != "" {
		_, _ = fmt.Fprintf(stderr, "toposhift query: note: %s\n", note)
	}
	return render(stdout, lines)
}

// render prints the lines of an answer as JSON lines.
func render(w io.Writer, lines []any) error {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	for _, l := range lines {
		if err := enc.Encode(l); err != nil {
			return fmt.Errorf("writing the answer: %w", err)
		}
	}
	return nil
}

// answer asks the store the query's question in every layer it covers, all at
// one snapshot token, and returns the lines of the answer. For neighbors and
// window an empty answer about a fingerprint with no entity records is still an
// answer, and note says what is known of it; for alive and history it is the
// error "no history".
func answer(ctx context.Context, st store.Store, q *query, asof uint64) (lines []any, note string, err error) {
	layers := q.layers
	if len(layers) == 0 {
		switch q.op {
		case "alive", "history":
			l, err := entityLayer(q.fp)
			if err != nil {
				return nil, "", err
			}
			layers = []catalog.Layer{l}
		default:
			layers = []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}
		}
	}

	for _, layer := range layers {
		scope := store.Scope{Layer: layer, AsOf: asof}
		got, err := q.ask(ctx, st, scope)
		if err != nil {
			return nil, "", explain(st, layer, err)
		}
		lines = append(lines, got...)
	}
	// An alive that says no, or an answer with nothing in it, needs telling
	// apart from a fingerprint nothing was ever recorded about.
	if (q.op == "alive" && !lines[0].(aliveLine).Alive) || (q.op != "alive" && len(lines) == 0) {
		err := requireEntity(ctx, st, q.fp, asof)
		var none noEntityError
		switch {
		case errors.As(err, &none) && (q.op == "neighbors" || q.op == "window"):
			return lines, none.detail, nil
		case err != nil:
			return nil, "", err
		}
	}
	return lines, "", nil
}

// noEntityError says a fingerprint has no entity records to be found.
type noEntityError struct{ detail string }

func (e noEntityError) Error() string { return "no history: " + e.detail }

// entityLayer is the layer the entity type of fp lives in.
func entityLayer(fp identity.Fingerprint) (catalog.Layer, error) {
	e, ok := catalog.Default().Entity(fp.Type())
	if !ok {
		return 0, noEntityError{fmt.Sprintf("%s: %q is not an entity type", fp, fp.Type())}
	}
	return e.Layer(), nil
}

// requireEntity is the one probe behind "no history": a single read of the
// existence records of fp, in the layer its entity type lives in whatever layer
// the question named, over the whole of time the layer still answers for. A
// fingerprint with none gets a [noEntityError]. So does a fingerprint that
// appears only as an edge endpoint: that is the price of a probe that costs one
// bounded read.
func requireEntity(ctx context.Context, st store.Store, fp identity.Fingerprint, asof uint64) error {
	layer, err := entityLayer(fp)
	if err != nil {
		return err
	}
	from := store.MinEventTime
	if h := st.LayerHorizon(layer).Time; h.After(from) {
		from = h
	}
	rs, err := st.EntityWindow(ctx, fp, from, store.MaxEventTime, store.Scope{Layer: layer, AsOf: asof})
	if err != nil {
		return explain(st, layer, err)
	}
	if len(rs) == 0 {
		return noEntityError{fmt.Sprintf("no entity records for %s as of seq %d in layer %s (a fingerprint that only appears as an edge endpoint has no entity history)",
			fp, asof, layer)}
	}
	return nil
}

// resolveAsOf is the token the query is answered at: latest is the store's last
// sequence number, taken once so that every layer is read in one world, and a
// token above it is that number too, which clamped reports.
func (q *query) resolveAsOf(last uint64) (asof uint64, clamped bool) {
	if q.asof == "latest" {
		return last, false
	}
	n, _ := strconv.ParseUint(q.asof, 10, 64) // checked when the command line was parsed
	return min(n, last), n > last
}

// ask runs the query's operation in one layer and returns its lines.
func (q *query) ask(ctx context.Context, st store.Store, scope store.Scope) ([]any, error) {
	layer := scope.Layer.String()
	switch q.op {
	case "neighbors":
		ns, err := st.Neighbors(ctx, q.fp, q.dir, q.at, scope)
		if err != nil {
			return nil, err
		}
		out := make([]any, len(ns))
		for i, n := range ns {
			out[i] = neighborLine{
				Op: "neighbors", AsOf: scope.AsOf, Layer: layer, At: formatTime(q.at),
				FP: q.fp.String(), FPType: string(q.fp.Type()), Dir: dirName(q.dir),
				Peer: n.Peer.String(), PeerType: string(n.Peer.Type()), Relation: string(n.Relation),
			}
		}
		return out, nil
	case "alive":
		alive, err := st.Alive(ctx, q.fp, q.at, scope)
		if err != nil {
			return nil, err
		}
		return []any{aliveLine{
			Op: "alive", AsOf: scope.AsOf, Layer: layer, At: formatTime(q.at),
			FP: q.fp.String(), FPType: string(q.fp.Type()), Alive: alive,
		}}, nil
	case "window":
		rs, err := st.Window(ctx, q.fp, q.dir, q.from, q.to, scope)
		if err != nil {
			return nil, err
		}
		return recordLines("window", scope.AsOf, q.fp, q.dir, rs), nil
	default: // history
		rs, err := st.EntityWindow(ctx, q.fp, q.from, q.to, scope)
		if err != nil {
			return nil, err
		}
		return recordLines("history", scope.AsOf, q.fp, 0, rs), nil
	}
}

func recordLines(op string, asof uint64, fp identity.Fingerprint, dir store.Direction, rs []store.Record) []any {
	out := make([]any, len(rs))
	for i, r := range rs {
		out[i] = newRecordLine(op, asof, fp, dir, r)
	}
	return out
}

// explain turns a store's refusal into the sentence a person needs: a
// retention horizon is named, and a quarantine is said as it is.
func explain(st store.Store, layer catalog.Layer, err error) error {
	var qe *store.QuarantineError
	switch {
	case errors.As(err, &qe):
		return err
	case errors.Is(err, store.ErrBeforeHorizon):
		h := st.LayerHorizon(layer)
		return fmt.Errorf("layer %s has been trimmed to a horizon at %s (seq %d), and the question reaches before it: %w",
			layer, formatTime(h.Time), h.Seq, err)
	}
	return err
}
