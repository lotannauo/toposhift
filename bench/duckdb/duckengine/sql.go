package duckengine

import (
	"fmt"
	"strings"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// The SQL of every read. Each statement is written here once, as a function of the
// direction and, for a batch, of how many fingerprints it asks about, so that the text
// a test holds to the model is the text a read runs. The rule they implement is the one
// the model's package documentation states: a subject holds at t under token S if, for
// some producer, its newest record with event time at or before t and Seq at or below S
// (ties at one instant to the higher Seq) is an observation that has not lapsed.

// sqlMutant is a deliberate fault in a statement. Only tests set one: each is a fault a
// test of this package must catch, named for what it gets wrong. The zero value is the
// honest statement.
type sqlMutant uint8

const (
	sqlHonest sqlMutant = iota
	// sqlNewestPerEdge takes the newest record of an edge, not of each producer's
	// reference to it.
	sqlNewestPerEdge
	// sqlNewestByEventOnly breaks a tie at one instant by nothing, not by the higher Seq.
	sqlNewestByEventOnly
	// sqlDeadlineInclusive keeps a record alive at the instant its deadline is reached.
	sqlDeadlineInclusive
	// sqlThroughIgnored counts a deadline from the event time, not from the later of it
	// and the through time.
	sqlThroughIgnored
	// sqlReverseBySource reads a reverse neighbor from the record file by its source.
	sqlReverseBySource
	// sqlTokenIgnored leaves out the snapshot token.
	sqlTokenIgnored
	// sqlWindowInclusive makes a window's upper bound inclusive.
	sqlWindowInclusive
	// sqlWindowUnordered leaves out the order of a window.
	sqlWindowUnordered
	// sqlIntervalEndInclusive keeps an interval at the instant it ends.
	sqlIntervalEndInclusive
	// sqlOpenIntervalDropped treats an interval with no end as ended.
	sqlOpenIntervalDropped
)

// The columns of a record, as a window returns them.
const recordColumns = "layer, subject_kind, source, target, relation, producer, event_time_ns, seq, kind, ttl_ns, through_ns, payload"

// ends names the columns a direction reads: the entity the read is from, and the one it
// finds.
func ends(dir engine.Direction, mut sqlMutant) (view, key, peer string) {
	if dir == engine.Reverse {
		view, key, peer = "rev", "target", "source"
		if mut == sqlReverseBySource {
			key = "source"
		}
		return
	}
	return "fwd", "source", "target"
}

// holdsAt is the condition on the newest record of a producer that makes its subject
// hold at the instant the parameter t names.
func holdsAt(t string, mut sqlMutant) string {
	last := "greatest(event_time_ns, coalesce(through_ns, event_time_ns))"
	switch mut {
	case sqlThroughIgnored:
		last = "event_time_ns"
	}
	op := ">"
	if mut == sqlDeadlineInclusive {
		op = ">="
	}
	return fmt.Sprintf("kind = 'observe' AND (ttl_ns = 0 OR %s + ttl_ns %s %s)", last, op, t)
}

// newest keeps the newest record of each group of the partition.
func newest(partition string, mut sqlMutant) string {
	if mut == sqlNewestPerEdge {
		partition = strings.TrimSuffix(partition, ", producer")
	}
	order := "event_time_ns DESC, seq DESC"
	if mut == sqlNewestByEventOnly {
		order = "event_time_ns DESC"
	}
	return fmt.Sprintf("QUALIFY row_number() OVER (PARTITION BY %s ORDER BY %s) = 1", partition, order)
}

func tokenCondition(param string, mut sqlMutant) string {
	if mut == sqlTokenIgnored {
		return "(seq <= " + param + "::UBIGINT OR TRUE)"
	}
	// The cast makes the token a constant of the column's own type, so that the scan
	// is given a filter on seq it can compare with a row group's bounds; compared with a
	// signed parameter, DuckDB widens the column instead and the filter is an
	// expression.
	return "seq <= " + param + "::UBIGINT"
}

// neighborsRecords is the neighbors of one entity from a record file: $1 the layer,
// $2 the entity, $3 the instant, $4 the token.
func neighborsRecords(dir engine.Direction, mut sqlMutant) string {
	view, key, peer := ends(dir, mut)
	return fmt.Sprintf(`SELECT %[2]s, relation FROM (
  SELECT %[2]s, relation, kind, ttl_ns, through_ns, event_time_ns FROM %[1]s
  WHERE layer = $1 AND subject_kind = 'edge' AND %[3]s = $2 AND event_time_ns <= $3 AND %[4]s
  %[5]s)
WHERE %[6]s
GROUP BY %[2]s, relation`, view, peer, key, tokenCondition("$4", mut), newest(peer+", relation, producer", mut), holdsAt("$3", mut))
}

// batchRecords is the neighbors of n entities at once: $1 the layer, $2 the instant,
// $3 the token, $4 to $(3+n) the entities. It returns the entity with each neighbor.
func batchRecords(dir engine.Direction, n int, mut sqlMutant) string {
	view, key, peer := ends(dir, mut)
	return fmt.Sprintf(`SELECT %[3]s, %[2]s, relation FROM (
  SELECT %[3]s, %[2]s, relation, kind, ttl_ns, through_ns, event_time_ns FROM %[1]s
  WHERE layer = $1 AND subject_kind = 'edge' AND %[3]s IN (%[4]s) AND event_time_ns <= $2 AND %[5]s
  %[6]s)
WHERE %[7]s
GROUP BY %[3]s, %[2]s, relation`, view, peer, key, params(4, n), tokenCondition("$3", mut), newest(key+", "+peer+", relation, producer", mut), holdsAt("$2", mut))
}

// aliveRecords counts the producers' references to an entity that hold: $1 the layer,
// $2 the entity, $3 the instant, $4 the token. The entity is alive if the count is not
// zero. It counts, and does not stop at the first, so that a read of a live entity does
// the work of one that is not.
func aliveRecords(mut sqlMutant) string {
	return fmt.Sprintf(`SELECT count(*) FROM (
  SELECT kind, ttl_ns, through_ns, event_time_ns FROM fwd
  WHERE layer = $1 AND subject_kind = 'entity' AND source = $2 AND event_time_ns <= $3 AND %[1]s
  %[2]s)
WHERE %[3]s`, tokenCondition("$4", mut), newest("producer", mut), holdsAt("$3", mut))
}

// windowRecords is the edge records of an entity from a record file with
// $3 <= event time < $4 (and no upper bound when unbounded), as of the token $5, or $4
// without an upper bound: $1 the layer, $2 the entity.
func windowRecords(dir engine.Direction, unbounded bool, mut sqlMutant) string {
	view, key, _ := ends(dir, mut)
	hi := "event_time_ns < $4"
	token := "$5"
	if mut == sqlWindowInclusive {
		hi = "event_time_ns <= $4"
	}
	if unbounded {
		hi, token = "TRUE", "$4"
	}
	order := "ORDER BY event_time_ns, seq"
	if mut == sqlWindowUnordered {
		order = ""
	}
	return fmt.Sprintf(`SELECT %s FROM %s
WHERE layer = $1 AND subject_kind = 'edge' AND %s = $2 AND event_time_ns >= $3 AND %s AND %s
%s`, recordColumns, view, key, hi, tokenCondition(token, mut), order)
}

// intervalHolds is the condition on an interval row that makes it hold at $t.
func intervalHolds(t string, mut sqlMutant) string {
	end := ">"
	if mut == sqlIntervalEndInclusive {
		end = ">="
	}
	if mut == sqlOpenIntervalDropped {
		return fmt.Sprintf("start_ns <= %s AND end_ns %s %s", t, end, t)
	}
	return fmt.Sprintf("start_ns <= %s AND (end_ns IS NULL OR end_ns %s %s)", t, end, t)
}

// ivView is the view of the interval file a direction reads.
func ivView(dir engine.Direction) string {
	if dir == engine.Reverse {
		return "irev"
	}
	return "ifwd"
}

// neighborsIntervals is the neighbors of one entity from an interval file: $1 the
// layer, $2 the entity, $3 the instant.
func neighborsIntervals(dir engine.Direction, mut sqlMutant) string {
	return fmt.Sprintf(`SELECT peer, relation FROM %s WHERE layer = $1 AND a = $2 AND %s`, ivView(dir), intervalHolds("$3", mut))
}

// batchIntervals is the neighbors of n entities from an interval file: $1 the layer, $2
// the instant, $3 to $(2+n) the entities.
func batchIntervals(dir engine.Direction, n int, mut sqlMutant) string {
	return fmt.Sprintf(`SELECT a, peer, relation FROM %s WHERE layer = $1 AND a IN (%s) AND %s`, ivView(dir), params(3, n), intervalHolds("$2", mut))
}

// aliveIntervals counts the intervals of an entity that hold: $1 the layer, $2 the
// entity, $3 the instant.
func aliveIntervals(mut sqlMutant) string {
	return fmt.Sprintf(`SELECT count(*) FROM ient WHERE layer = $1 AND a = $2 AND %s`, intervalHolds("$3", mut))
}

// params is the placeholders $from, $from+1, ... for n parameters.
func params(from, n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = fmt.Sprintf("$%d", from+i)
	}
	return strings.Join(parts, ", ")
}
