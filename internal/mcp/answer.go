package mcp

import (
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

// tail is a list an answer may shorten from its end.
type tail interface {
	length() int
	// keep keeps the first n entries of the list as it was before any keep.
	keep(n int)
}

type tailOf[E any] struct {
	p    *[]E
	orig []E
}

func listTail[E any](p *[]E) *tailOf[E] { return &tailOf[E]{p: p, orig: *p} }

func (t *tailOf[E]) length() int { return len(t.orig) }

func (t *tailOf[E]) keep(n int) { *t.p = t.orig[:n:n] }

// namedTail is a list with the name its omission is reported under.
type namedTail struct {
	name string
	list tail
}

// budgetSlack leaves room in the cap for the sentences added after a list is
// shortened.
const budgetSlack = 2 << 10

// fitAnswer shortens the lists of an answer until its JSON is at most max bytes,
// dropping whole trailing entries from the first list given until it is empty,
// then from the next. It returns the sentences that say what was left out, and
// none if nothing was.
func fitAnswer(out any, max int, lists ...namedTail) ([]string, error) {
	size := func() (int, error) {
		b, err := json.Marshal(out)
		return len(b), err
	}
	apply := func(drop int) {
		for _, l := range lists {
			n := l.list.length()
			d := min(drop, n)
			drop -= d
			l.list.keep(n - d)
		}
	}
	budget := max - budgetSlack
	n, err := size()
	if err != nil {
		return nil, err
	}
	if n <= budget {
		return nil, nil
	}
	total := 0
	for _, l := range lists {
		total += l.list.length()
	}
	var firstErr error
	// The smallest number of dropped entries that fits, or all of them.
	drop := sort.Search(total+1, func(d int) bool {
		apply(d)
		n, err := size()
		if err != nil && firstErr == nil {
			firstErr = err
		}
		return n <= budget
	})
	if firstErr != nil {
		return nil, firstErr
	}
	apply(drop)

	var notes []string
	left := drop
	for _, l := range lists {
		n := l.list.length()
		d := min(left, n)
		left -= d
		if d > 0 {
			notes = append(notes, fmt.Sprintf("The answer was cut at %d KiB: the last %d entries of %s were left out.", max>>10, d, l.name))
		}
	}
	return notes, nil
}

// shown reports a list shortened by the display cap, with the exact count.
func shown(listed, total int, what, order string) string {
	return fmt.Sprintf("Showing %d of %d %s, %s.", listed, total, what, order)
}

// passAsOf is the sentence every answer ends with.
func passAsOf(token uint64) string {
	return "Pass asof=" + strconv.FormatUint(token, 10) +
		" to see the same world, until the next retention (at most a day) or a restart with other ranks."
}

// notesFor starts the notes of an answer with the sentence about a token that
// was ahead of the store, if there is one.
func notesFor(ahead string) []string {
	if ahead == "" {
		return nil
	}
	return []string{ahead}
}

// head returns the first n entries of a list, and the list itself if it is
// shorter.
func head[E any](xs []E, n int) []E {
	if len(xs) <= n {
		return xs
	}
	return xs[:n:n]
}

func tokenString(t uint64) string { return strconv.FormatUint(t, 10) }
