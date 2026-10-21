package xtengine

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtmodel"
)

// XTDB's temporal columns hold microseconds (see the package documentation). The
// model holds nanoseconds, so two conversions are needed, and each is exact only
// because of a rule:
//
//   - a boundary the planner writes (an event time, a deadline, the event time of the
//     next assertion) must be a whole microsecond, or the batch is refused: [Engine.timestamp]
//     returns an error for one that is not;
//   - an instant a read asks about is floored to a whole microsecond: with every
//     boundary on a whole microsecond, "s <= t < e" holds exactly when "s <= floor(t) <
//     e" does, so the answer is the same. Rounding to the nearest or up would not give
//     that.

const nanosPerMicro = int64(time.Microsecond)

// timestamp is a boundary as the argument of a statement: nil for the end of time (a
// null `_valid_to`, or a portion with no end), and the instant otherwise, which must
// be a whole microsecond.
func (e *Engine) timestamp(i xtmodel.Instant) (any, error) {
	if i == xtmodel.Forever {
		return nil, nil
	}
	if e.mut == mutWriteOffMicrosecond && i < xtmodel.PastEnd {
		return time.UnixMicro(int64(i) / nanosPerMicro).UTC(), nil
	}
	if i >= xtmodel.PastEnd || int64(i)%nanosPerMicro != 0 {
		return nil, fmt.Errorf("xtengine: %s is not a whole microsecond: %w", i, engine.ErrInvalid)
	}
	return time.UnixMicro(int64(i) / nanosPerMicro).UTC(), nil
}

// floorMicros is the instant of a read: the largest whole microsecond not after it. An
// instant is unsigned, and [xtmodel.PastEnd] is above the largest int64, so the division is
// made on the unsigned value.
func (e *Engine) floorMicros(i xtmodel.Instant) time.Time {
	us := uint64(i) / uint64(nanosPerMicro)
	switch e.mut {
	case mutRoundMicros:
		us = (uint64(i) + uint64(nanosPerMicro)/2) / uint64(nanosPerMicro)
	case mutCeilMicros:
		us = (uint64(i) + uint64(nanosPerMicro) - 1) / uint64(nanosPerMicro)
	}
	return time.UnixMicro(int64(us)).UTC()
}

// systemTimestamp is the system time of a transaction.
func systemTimestamp(i xtmodel.Instant) time.Time {
	return time.UnixMicro(int64(i) / nanosPerMicro).UTC()
}

// literal is a timestamp written as an SQL literal, for the transaction's system time.
func literal(t time.Time) string {
	return "TIMESTAMP '" + t.UTC().Format("2006-01-02T15:04:05.000000Z") + "'"
}
