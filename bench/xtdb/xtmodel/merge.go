package xtmodel

import (
	"slices"
	"sort"
)

// vop is a statement about one segment document, before it is written to the
// tables of its direction: the document is present (an insert) or absent (a portion
// delete) over the valid-time range [from, to).
type vop struct {
	insert   bool
	from, to Instant
}

// piece is a stretch of a document's valid time that a batch decided.
type piece struct {
	from, to Instant
	present  bool
}

// merge reduces the statements one batch makes about one document to few that have
// the same effect, to be applied in the order returned.
//
// A statement overwrites the stretch of valid time it names: an insert makes the
// document present there, a portion delete makes it absent, and what the document was
// there before does not matter. So the effect of a sequence of statements is what each
// instant is last made, and it can be had by painting that, however it was first
// cut: the statements, applied in order, paint a line, and the stretches that end up
// with one paint and touch are one stretch. Every insert on one document is the same
// document (its columns are those of its key), so two present stretches that touch are
// one insert.
//
// Painted stretches that touch form a group, and a group alternates present and absent
// stretches. A group is painted in as few statements as the two paints allow: either
// all of the span first absent to last absent is deleted and the present stretches
// inserted over it, or the span first present to last present is inserted and the
// absent stretches deleted in it, whichever is fewer (the insert first when they tie, as a record's own statements are). The order of the statements
// decides the effect where they overlap, and the order of the records is the order of
// their Seq: a later record overwrites an earlier one, so reversing it (reverse, a
// test's fault) changes the effect.
//
// If fresh, the document has nothing stored, so a portion delete would remove nothing
// and is left out.
func merge(ops []vop, fresh bool, reverse bool) []vop {
	var line []piece
	paint := func(o vop) {
		// The pieces that overlap [from, to) are line[i:j].
		i := sort.Search(len(line), func(k int) bool { return line[k].to > o.from })
		j := sort.Search(len(line), func(k int) bool { return line[k].from >= o.to })
		var repl []piece
		if i < j && line[i].from < o.from {
			repl = append(repl, piece{line[i].from, o.from, line[i].present})
		}
		repl = append(repl, piece{o.from, o.to, o.insert})
		if i < j && line[j-1].to > o.to {
			repl = append(repl, piece{o.to, line[j-1].to, line[j-1].present})
		}
		line = slices.Replace(line, i, j, repl...)
	}
	if reverse {
		for k := len(ops) - 1; k >= 0; k-- {
			paint(ops[k])
		}
	} else {
		for _, o := range ops {
			paint(o)
		}
	}

	// Join the touching stretches of one paint, then split the line into groups of
	// touching stretches.
	var joined []piece
	for _, p := range line {
		if n := len(joined); n > 0 && joined[n-1].present == p.present && joined[n-1].to == p.from {
			joined[n-1].to = p.to
			continue
		}
		joined = append(joined, p)
	}
	var out []vop
	for lo := 0; lo < len(joined); {
		hi := lo + 1
		for hi < len(joined) && joined[hi].from == joined[hi-1].to {
			hi++
		}
		out = append(out, paintGroup(joined[lo:hi], fresh)...)
		lo = hi
	}
	return out
}

// paintGroup is the statements for a group of touching stretches that alternate
// present and absent.
func paintGroup(g []piece, fresh bool) []vop {
	var present, absent []piece
	for _, p := range g {
		if p.present {
			present = append(present, p)
		} else {
			absent = append(absent, p)
		}
	}
	insert := func(p piece) vop { return vop{insert: true, from: p.from, to: p.to} }
	remove := func(p piece) vop { return vop{from: p.from, to: p.to} }
	var out []vop
	switch {
	case len(absent) == 0 || fresh:
		for _, p := range present {
			out = append(out, insert(p))
		}
	case len(present) == 0:
		out = append(out, remove(absent[0]))
	case len(present) < len(absent):
		// Delete the span of the absent stretches, which holds the present ones in
		// between, and insert those.
		out = append(out, vop{from: absent[0].from, to: absent[len(absent)-1].to})
		for _, p := range present {
			out = append(out, insert(p))
		}
	default:
		// Insert the span of the present stretches, and delete the absent ones in it
		// and beside it.
		out = append(out, vop{insert: true, from: present[0].from, to: present[len(present)-1].to})
		for _, p := range absent {
			out = append(out, remove(p))
		}
	}
	return out
}
