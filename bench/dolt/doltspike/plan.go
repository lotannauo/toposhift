package doltspike

import (
	"fmt"
	"strings"
	"unicode/utf8"
)

// Access is one read of a table in a plan EXPLAIN FORMAT=TREE printed: which table, through
// which index, and how many of the index's leading columns the access fixes to one value.
type Access struct {
	Table string
	// Index is the index's columns, in order.
	Index []string
	// Bound is the number of leading columns of the index that the access fixes to a single
	// value: a point of a range, or a key of a lookup join. The rows the access visits are
	// those that share the bound prefix.
	Bound int
	// Ranged says the column after the bound ones is bounded by a range, as the event time
	// of a window is.
	Ranged bool
	// Lookup says the access is a lookup by the keys of a join, and not a range of its own.
	Lookup bool
	// Unbounded says the access names neither a range nor keys: it reads the whole index.
	Unbounded bool
}

// ParsePlan reads the lines of EXPLAIN FORMAT=TREE and returns the table reads it names, in
// order, and whether the plan holds a node that scans a table with no index. It reads only
// what the planner prints for an indexed access: the table, the index's columns, the ranges
// of its filters and the keys of a lookup.
func ParsePlan(lines []string) (accesses []Access, scans bool, err error) {
	for i, line := range lines {
		col, text := textOf(line)
		switch {
		case strings.HasPrefix(text, "IndexedTableAccess("):
			table := strings.TrimSuffix(strings.TrimPrefix(text, "IndexedTableAccess("), ")")
			a := Access{Table: table}
			for _, child := range lines[i+1:] {
				ccol, ctext := textOf(child)
				if ccol <= col {
					break
				}
				switch {
				case strings.HasPrefix(ctext, "index: ["):
					a.Index = indexColumns(table, strings.TrimSuffix(strings.TrimPrefix(ctext, "index: ["), "]"))
				case strings.HasPrefix(ctext, "filters: "):
					a.Bound, a.Ranged, err = boundOfFilters(strings.TrimPrefix(ctext, "filters: "))
					if err != nil {
						return nil, false, fmt.Errorf("doltspike: plan line %q: %w", ctext, err)
					}
				case strings.HasPrefix(ctext, "keys: "):
					a.Lookup = true
					a.Bound = len(splitTop(strings.TrimPrefix(ctext, "keys: ")))
				}
			}
			a.Unbounded = a.Bound == 0 && !a.Ranged
			if len(a.Index) == 0 {
				return nil, false, fmt.Errorf("doltspike: the access to %s names no index", table)
			}
			accesses = append(accesses, a)
		case text == "Table" || strings.HasPrefix(text, "Table("):
			scans = true
		}
	}
	return accesses, scans, nil
}

// textOf is the text of a plan line, without the glyphs that draw the tree, and the column it
// starts at, so that a node's children are the lines after it that start further right.
func textOf(line string) (col int, text string) {
	for i, r := range line {
		switch r {
		case ' ', '│', '├', '└', '─':
			continue
		}
		return utf8.RuneCountInString(line[:i]), line[i:]
	}
	return 0, ""
}

func indexColumns(table, list string) []string {
	var out []string
	for _, c := range strings.Split(list, ",") {
		out = append(out, strings.TrimPrefix(strings.TrimSpace(c), table+"."))
	}
	return out
}

// splitTop splits a list at the commas that are not inside brackets or parentheses.
func splitTop(s string) []string {
	var out []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '[', '(', '{':
			depth++
		case ']', ')', '}':
			depth--
		case ',':
			if depth == 0 {
				out = append(out, strings.TrimSpace(s[start:i]))
				start = i + 1
			}
		}
	}
	return append(out, strings.TrimSpace(s[start:]))
}

// boundOfFilters reads the ranges the planner printed for an access, "[{r1, r2, ...}]", one
// per column of the index: a range whose two ends are the same is a point. It returns how
// many leading columns are points, and whether the column after them is bounded.
func boundOfFilters(s string) (bound int, ranged bool, err error) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "[{") || !strings.HasSuffix(s, "}]") {
		return 0, false, fmt.Errorf("filters %q are not a list of ranges", s)
	}
	cols := splitTop(strings.TrimSuffix(strings.TrimPrefix(s, "[{"), "}]"))
	for i, c := range cols {
		lo, hi, ok := rangeEnds(c)
		if !ok {
			return 0, false, fmt.Errorf("range %q is not [low, high]", c)
		}
		if lo == hi {
			if i == bound {
				bound++
			}
			continue
		}
		if i == bound && (!strings.HasSuffix(lo, "NULL") || hi != "∞") {
			ranged = true
		}
		break
	}
	return bound, ranged, nil
}

// rangeEnds splits "[lo, hi]", "[lo, hi)" or "(lo, hi]" at its top-level comma.
func rangeEnds(r string) (lo, hi string, ok bool) {
	r = strings.TrimSpace(r)
	if len(r) < 2 || (r[0] != '[' && r[0] != '(') || (r[len(r)-1] != ']' && r[len(r)-1] != ')') {
		return "", "", false
	}
	parts := splitTop(r[1 : len(r)-1])
	if len(parts) != 2 {
		return "", "", false
	}
	return parts[0], parts[1], true
}
