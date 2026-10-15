package svcgraph_test

import (
	"maps"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/otlpmetrics"
	"github.com/lotannauo/toposhift/internal/ingest/svcgraph"
)

// The oracle is the rule of the package documentation written as plainly as it can
// be: no overlay of pending changes on committed ones, but a copy of the whole table
// made when a group begins and kept or thrown away when it ends; times as times, not
// as numbers; and the cases as the list they are, one after the other. It sees the
// points of a whole scripted run, and answers, for each, what the rule says.

// verdict is the oracle's answer for one point, named by the reason the translator
// counts it with, where there is one.
type verdict string

const (
	vSighting    verdict = "sighting"
	vNone        verdict = "none"
	vWarmUp      verdict = svcgraph.SkipWarmUp
	vStale       verdict = svcgraph.SkipStale
	vSecond      verdict = svcgraph.SkipSecondConnector
	vOverflow    verdict = svcgraph.SkipSeriesOverflow
	vClockAhead  verdict = otlpmetrics.SkipClockAhead
	maxClockEdge         = otlpmetrics.MaxClockAhead
)

// oPoint is a cumulative point of a series.
type oPoint struct {
	e         edgeID
	start, at time.Time
	value     float64
	// conn is the connector that made the point, and lastCall the time of the call the
	// connector last counted for the series. The rule does not read them; the
	// soundness check does.
	conn     int
	lastCall time.Time
}

type oSeries struct {
	start, last, increase time.Time
	value                 float64
	conn                  int
}

type oWorld struct {
	series    map[edgeID]oSeries
	anchor    time.Time
	armed     bool
	maxGap    time.Duration
	newest    time.Time
	lastSweep time.Time
}

func (w oWorld) clone() oWorld {
	w.series = maps.Clone(w.series)
	return w
}

type oracle struct {
	flush, r, evict time.Duration
	maxSeries       int
	committed, cur  oWorld
}

func newOracle(flush, cacheLoop time.Duration, maxSeries int) *oracle {
	r := 15*time.Minute + cacheLoop + flush
	o := &oracle{flush: flush, r: r, evict: r + 2*flush, maxSeries: maxSeries}
	o.committed = oWorld{series: map[edgeID]oSeries{}}
	o.cur = o.committed.clone()
	return o
}

// commit keeps the group, and drops the series nothing has been heard of for longer
// than the connector keeps one, at most once for each flush of time.
func (o *oracle) commit() {
	w := o.cur
	if w.newest.Sub(w.lastSweep) >= o.flush {
		for e, s := range w.series {
			if w.newest.Sub(s.last) > o.evict {
				delete(w.series, e)
			}
		}
		w.lastSweep = w.newest
	}
	o.committed = w
	o.cur = o.committed.clone()
}

func (o *oracle) rollback() { o.cur = o.committed.clone() }

func (o *oracle) warmedUp(at time.Time) bool {
	w := o.cur
	wait := o.flush
	if w.maxGap > wait {
		wait = w.maxGap
	}
	return w.armed && !at.Before(w.anchor.Add(2*wait))
}

// point is the rule for one cumulative point of a request received at received.
func (o *oracle) point(p oPoint, received time.Time) verdict {
	if p.at.After(received.Add(maxClockEdge)) {
		return vClockAhead
	}
	w := &o.cur
	newest := w.newest
	if p.at.After(newest) {
		newest = p.at
	}
	s, held := w.series[p.e]
	if !held || newest.Sub(s.last) > o.evict {
		// No state.
		if !held && len(w.series) >= o.maxSeries {
			w.newest = newest
			if !w.armed || p.at.After(w.anchor) {
				w.anchor, w.armed = p.at, true
			}
			return vOverflow
		}
		w.newest = newest
		if !w.armed {
			w.anchor, w.armed = p.at, true
		}
		w.series[p.e] = oSeries{start: p.start, last: p.at, increase: p.at, value: p.value, conn: p.conn}
		switch {
		case p.value <= 0:
			return vNone
		case o.warmedUp(p.at):
			return vSighting
		}
		return vWarmUp
	}
	// 1. Not later than the last point.
	if !p.at.After(s.last) {
		return vStale
	}
	// 2. A start time before the state's.
	if p.start.Before(s.start) {
		return vSecond
	}
	sighting := false
	switch {
	case p.start.Equal(s.start) && p.value > s.value: // 3
		sighting = true
	case p.start.After(s.start) || p.value < s.value: // 4
		sighting = p.value > 0
	case p.at.Sub(s.increase) > o.r: // 5
		sighting = p.value > 0
	}
	w.newest = newest
	if gap := p.at.Sub(s.last); gap > w.maxGap && gap <= o.r {
		w.maxGap = gap
	}
	inc := s.increase
	if sighting {
		inc = p.at
	}
	w.series[p.e] = oSeries{start: p.start, last: p.at, increase: inc, value: p.value, conn: p.conn}
	if sighting {
		return vSighting
	}
	return vNone
}
