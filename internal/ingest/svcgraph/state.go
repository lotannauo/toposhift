package svcgraph

import "time"

// series is what the translator keeps of one series of the connector's counter.
// Times are Unix nanoseconds.
type series struct {
	// start and value are the start time and the value of the latest point that
	// was used, and last its time.
	start, last int64
	value       float64
	// increase is the time of the latest point that was a sighting, or that made
	// the state without being one.
	increase int64
}

// scalars are the translator's state that is not a series.
type scalars struct {
	// anchor is the time the warm-up began: the time of the first point used, or of
	// the latest point set aside for want of room. It means something only when
	// armed.
	anchor int64
	armed  bool
	// maxGap is the largest gap, of at most R, seen between two consecutive points of
	// one series.
	maxGap int64
	// newest is the newest time among the points that changed the state.
	newest int64
}

// outcome is what the rule says of one cumulative point.
type outcome uint8

const (
	// sighted: the point is evidence of a new call.
	sighted outcome = iota
	// unchanged: the point is not evidence, and the state followed it.
	unchanged
	// warmingUp: a point of a series with no state, with a value above zero, before
	// the warm-up has ended. It is not evidence.
	warmingUp
	// stale: the point is not later than the state's last point.
	stale
	// secondConnector: the point's start time is earlier than the state's.
	secondConnector
	// overflowed: the series has no state and there is no room for one.
	overflowed
)

// ledger is the state of every series, with the split that a failed group needs:
// the committed state, which the sequencer has promoted, and a pending state, the
// changes of the group being translated. A lookup sees both, pending first.
// commit promotes the pending state and rollback drops it.
type ledger struct {
	maxSeries  int
	flush      int64
	evictAfter int64
	idle       int64

	committed map[string]series
	// pending holds the series the group changed. It replaces the committed state
	// of the same series; pendingNew counts those that committed does not have.
	pending    map[string]series
	pendingNew int

	// com is the committed scalars and cur the current ones, the committed ones with
	// the group's changes. lastSweep is the newest time at the last sweep; it only
	// changes in commit.
	com, cur  scalars
	lastSweep int64
}

func newLedger(maxSeries int, flush, idle time.Duration) *ledger {
	return &ledger{
		maxSeries:  maxSeries,
		flush:      int64(flush),
		idle:       int64(idle),
		evictAfter: int64(idle) + 2*int64(flush),
		committed:  make(map[string]series),
		pending:    make(map[string]series),
	}
}

// size is the number of series held, evictable or not.
func (l *ledger) size() int { return len(l.committed) + l.pendingNew }

func (l *ledger) held(key string) bool {
	if _, ok := l.pending[key]; ok {
		return true
	}
	_, ok := l.committed[key]
	return ok
}

// lookup returns the state of a series if there is one that has not been evicted by
// the time newest: a series is evicted when newest is later than its last point by
// more than the connector's longest idle life and two flushes.
func (l *ledger) lookup(key string, newest int64) (series, bool) {
	s, ok := l.pending[key]
	if !ok {
		s, ok = l.committed[key]
	}
	if !ok || newest-s.last > l.evictAfter {
		return series{}, false
	}
	return s, true
}

func (l *ledger) put(key string, s series) {
	if !l.held(key) {
		l.pendingNew++
	}
	l.pending[key] = s
}

// pastWarmUp says whether a point at time at comes after the warm-up: two flushes,
// or two of the largest gap seen between points of one series if that is longer,
// after the anchor.
func (l *ledger) pastWarmUp(at int64) bool {
	return l.cur.armed && at-l.cur.anchor >= 2*max(l.flush, l.cur.maxGap)
}

// rearm starts a warm-up at time at.
func (l *ledger) rearm(at int64) {
	if !l.cur.armed || at > l.cur.anchor {
		l.cur.anchor, l.cur.armed = at, true
	}
}

// cumulative applies the rule to one point of a cumulative series: start and at are
// the point's start time and time, and value its value. The cases are numbered as
// in the package documentation.
func (l *ledger) cumulative(key string, start, at int64, value float64) outcome {
	newest := max(l.cur.newest, at)
	st, ok := l.lookup(key, newest)
	if !ok {
		if !l.held(key) && l.size() >= l.maxSeries {
			l.cur.newest = newest
			l.rearm(at)
			return overflowed
		}
		l.cur.newest = newest
		if !l.cur.armed {
			l.cur.anchor, l.cur.armed = at, true
		}
		// A state made without a sighting takes this point's time as its last
		// increase, or case 5 would fire on its next point.
		l.put(key, series{start: start, last: at, value: value, increase: at})
		switch {
		case value <= 0:
			return unchanged
		case l.pastWarmUp(at):
			return sighted
		}
		return warmingUp
	}
	switch {
	case at <= st.last: // case 1
		return stale
	case start < st.start: // case 2
		return secondConnector
	}
	var sight bool
	switch {
	case start == st.start && value > st.value: // case 3
		sight = true
	case start > st.start || value < st.value: // case 4
		sight = value > 0
	case at-st.increase > l.idle: // case 5
		sight = value > 0
	}
	l.cur.newest = newest
	// A gap longer than R is not a flush: the connector removed the series and made it
	// again, so it says nothing about how far apart flushes are.
	if gap := at - st.last; gap <= l.idle {
		l.cur.maxGap = max(l.cur.maxGap, gap)
	}
	next := series{start: start, last: at, value: value, increase: st.increase}
	if sight {
		next.increase = at
	}
	l.put(key, next)
	if sight {
		return sighted
	}
	return unchanged
}

// commit promotes the pending state to committed, and drops the series that have
// gone unseen for longer than the connector keeps one, at most once for each flush
// of newest time.
func (l *ledger) commit() {
	if len(l.pending) > 0 {
		for k, s := range l.pending {
			l.committed[k] = s
		}
		clear(l.pending)
		l.pendingNew = 0
	}
	l.com = l.cur
	if l.com.newest-l.lastSweep >= l.flush {
		for k, s := range l.committed {
			if l.com.newest-s.last > l.evictAfter {
				delete(l.committed, k)
			}
		}
		l.lastSweep = l.com.newest
	}
}

// rollback drops the pending state, and the changes of the group to the scalars.
func (l *ledger) rollback() {
	clear(l.pending)
	l.pendingNew = 0
	l.cur = l.com
}
