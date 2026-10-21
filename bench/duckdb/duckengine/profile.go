package duckengine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// RawProfile is the profile DuckDB reports for the last statement of a connection: the
// metrics of the query and of each operator, by name and as text, and the operators
// below it. Its shape is that of the driver's profiling info, so that a profile recorded
// as JSON is read exactly as a live one.
type RawProfile struct {
	Metrics  map[string]string
	Children []RawProfile `json:",omitempty"`
}

// ErrUnknownMetric is wrapped by the error [ParseProfile] returns for a metric it has no
// classification for. Every metric is either kept, because it is a function of the
// files and the statement, or stripped, because it depends on time, memory or what the
// connection had cached; a metric a new version adds is neither until someone decides.
var ErrUnknownMetric = errors.New("duckengine: a profile metric that is neither kept nor stripped")

// The classification of the metrics of version 1.5.6.
var (
	// queryKept are the query-level metrics that are counts of rows.
	queryKept = []string{"ROWS_RETURNED", "CUMULATIVE_CARDINALITY", "CUMULATIVE_ROWS_SCANNED"}
	// operatorKept are the operator-level metrics read: what the operator is and the
	// rows it was given and handed on.
	operatorKept = []string{"OPERATOR_TYPE", "OPERATOR_NAME", "OPERATOR_CARDINALITY", "OPERATOR_ROWS_SCANNED", "EXTRA_INFO"}
	// stripped are the metrics that are times, memory or cache-dependent: a latency or
	// CPU time, bytes read (which is the footer's bytes the first time a file is read
	// and nothing after), memory and temporary file sizes. They are dropped before a
	// profile is hashed or written. The cumulative figures of an operator repeat those
	// of its children.
	stripped = []string{
		"LATENCY", "CPU_TIME", "BLOCKED_THREAD_TIME", "OPERATOR_TIMING",
		"ATTACH_LOAD_STORAGE_LATENCY", "ATTACH_REPLAY_WAL_LATENCY", "CHECKPOINT_LATENCY",
		"COMMIT_LOCAL_STORAGE_LATENCY", "WAITING_TO_ATTACH_LATENCY", "WRITE_TO_WAL_LATENCY",
		"TOTAL_BYTES_READ", "TOTAL_BYTES_WRITTEN", "TOTAL_MEMORY_ALLOCATED",
		"SYSTEM_PEAK_BUFFER_MEMORY", "SYSTEM_PEAK_TEMP_DIR_SIZE", "RESULT_SET_SIZE",
		"WAL_REPLAY_ENTRY_COUNT", "QUERY_NAME",
	}
)

func classified(key string, root bool) bool {
	for _, k := range stripped {
		if k == key {
			return true
		}
	}
	if root {
		for _, k := range queryKept {
			if k == key {
				return true
			}
		}
		return key == "EXTRA_INFO"
	}
	for _, k := range operatorKept {
		if k == key {
			return true
		}
	}
	return key == "CUMULATIVE_CARDINALITY" || key == "CUMULATIVE_ROWS_SCANNED"
}

// Node is one operator of a plan, as kept.
type Node struct {
	Type, Name  string
	Depth       int
	Cardinality int64
	// RowsScanned is the rows a table scan was handed: the rows of every file it opened,
	// whatever row groups it skipped.
	RowsScanned int64
	// Function, Projections and Filters are a table scan's: the function that read the
	// file, the columns it projected and the filters it was given, one conjunct each.
	Function    string   `json:",omitempty"`
	Projections []string `json:",omitempty"`
	Filters     []string `json:",omitempty"`
}

// Profile is what a statement's profile amounts to once everything that is not a count
// of rows is stripped: the plan, operator by operator, depth first.
type Profile struct {
	RowsReturned          int64
	CumulativeCardinality int64
	CumulativeRowsScanned int64
	Nodes                 []Node
}

// ParseProfile reads a profile, refusing a metric it cannot classify, a metric that is
// not a count where a count is expected, and a profile with no operator.
func ParseProfile(raw RawProfile) (Profile, error) {
	for _, k := range sortedKeys(raw.Metrics) {
		if !classified(k, true) {
			return Profile{}, fmt.Errorf("%w: %s (query)", ErrUnknownMetric, k)
		}
	}
	var p Profile
	var err error
	if p.RowsReturned, err = count(raw.Metrics, "ROWS_RETURNED"); err != nil {
		return Profile{}, err
	}
	if p.CumulativeCardinality, err = count(raw.Metrics, "CUMULATIVE_CARDINALITY"); err != nil {
		return Profile{}, err
	}
	if p.CumulativeRowsScanned, err = count(raw.Metrics, "CUMULATIVE_ROWS_SCANNED"); err != nil {
		return Profile{}, err
	}
	var walk func(r RawProfile, depth int) error
	walk = func(r RawProfile, depth int) error {
		for _, k := range sortedKeys(r.Metrics) {
			if !classified(k, false) {
				return fmt.Errorf("%w: %s (operator %s)", ErrUnknownMetric, k, r.Metrics["OPERATOR_NAME"])
			}
		}
		n := Node{Type: r.Metrics["OPERATOR_TYPE"], Name: r.Metrics["OPERATOR_NAME"], Depth: depth}
		if n.Type == "" || n.Name == "" {
			return errors.New("duckengine: an operator of a profile has no type or no name")
		}
		var err error
		if n.Cardinality, err = count(r.Metrics, "OPERATOR_CARDINALITY"); err != nil {
			return err
		}
		if n.RowsScanned, err = count(r.Metrics, "OPERATOR_ROWS_SCANNED"); err != nil {
			return err
		}
		if n.Type == "TABLE_SCAN" {
			extra, err := parseExtra(r.Metrics["EXTRA_INFO"])
			if err != nil {
				return err
			}
			n.Function = extra["Function"]
			n.Projections = lines(extra["Projections"])
			n.Filters = lines(extra["Filters"])
		}
		p.Nodes = append(p.Nodes, n)
		for _, c := range r.Children {
			if err := walk(c, depth+1); err != nil {
				return err
			}
		}
		return nil
	}
	if len(raw.Children) == 0 {
		return Profile{}, errors.New("duckengine: a profile has no operator")
	}
	for _, c := range raw.Children {
		if err := walk(c, 0); err != nil {
			return Profile{}, err
		}
	}
	return p, nil
}

func count(m map[string]string, key string) (int64, error) {
	s, ok := m[key]
	if !ok {
		return 0, fmt.Errorf("duckengine: a profile has no %s", key)
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("duckengine: %s is %q, not a count", key, s)
	}
	return n, nil
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func lines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// parseExtra reads the text of an operator's EXTRA_INFO: braces around key=value pairs
// separated by a comma and a space, a value in single quotes if it has a newline or a
// comma in it, with a quote inside escaped by a backslash.
func parseExtra(s string) (map[string]string, error) {
	out := make(map[string]string)
	if s == "" || s == "{}" {
		return out, nil
	}
	if !strings.HasPrefix(s, "{") || !strings.HasSuffix(s, "}") {
		return nil, fmt.Errorf("duckengine: EXTRA_INFO %q is not in braces", s)
	}
	body := s[1 : len(s)-1]
	i := 0
	for i < len(body) {
		eq := strings.IndexByte(body[i:], '=')
		if eq < 0 {
			return nil, fmt.Errorf("duckengine: EXTRA_INFO %q has a pair without =", s)
		}
		key := strings.Trim(body[i:i+eq], "'")
		i += eq + 1
		var val strings.Builder
		if i < len(body) && body[i] == '\'' {
			i++
			for {
				if i >= len(body) {
					return nil, fmt.Errorf("duckengine: EXTRA_INFO %q has an unclosed quote", s)
				}
				if body[i] == '\\' && i+1 < len(body) {
					val.WriteByte(body[i+1])
					i += 2
					continue
				}
				if body[i] == '\'' {
					i++
					break
				}
				val.WriteByte(body[i])
				i++
			}
		} else {
			end := strings.Index(body[i:], ", ")
			if end < 0 {
				end = len(body) - i
			}
			val.WriteString(body[i : i+end])
			i += end
		}
		out[key] = val.String()
		i += len(", ")
		if i > len(body) {
			break
		}
	}
	return out, nil
}

// Scans are the table scans of the plan.
func (p Profile) Scans() []Node {
	var out []Node
	for _, n := range p.Nodes {
		if n.Type == "TABLE_SCAN" {
			out = append(out, n)
		}
	}
	return out
}

// RowsScanned is the rows the table scans of the plan were handed, each counted once.
// The cumulative figure of a node repeats its children's, so adding those up would
// count a scan once for every operator above it.
func (p Profile) RowsScanned() int64 {
	var n int64
	for _, s := range p.Scans() {
		n += s.RowsScanned
	}
	return n
}

// Shape is the plan as one line: the operators, depth first, indented by depth.
func (p Profile) Shape() string {
	parts := make([]string, len(p.Nodes))
	for i, n := range p.Nodes {
		parts[i] = strings.Repeat(">", n.Depth) + n.Name
	}
	return strings.Join(parts, " ")
}

// Digest is the SHA-256 of the profile as kept, in hex. Two reads of one statement have
// one digest, however long each took.
func (p Profile) Digest() string {
	b, err := json.Marshal(p)
	if err != nil {
		panic(err) // a Profile is plain data
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
