package k8scluster

import "time"

// A NamespaceSighting is a state or delete event of the kube-system namespace
// seen by this producer. A later detector of cluster identity reads them; this
// package keeps no state across producers and refuses nothing for them.
type NamespaceSighting struct {
	// UID is the namespace's k8s.namespace.uid.
	UID string
	// Deleted says the event is an entity.delete.
	Deleted bool
	// At is the instant of the record (see the package documentation).
	At time.Time
	// Interval is the report interval of a state event, or the translator's
	// [Config.CollectionInterval] if the event has none, as for a TTL. It is zero
	// for a delete, and for a state event only if the event reports an interval
	// of zero or has none and no collection interval is configured.
	Interval time.Duration
}

// AliveUntil is the instant the sighting stops vouching for its UID: At plus
// [DefaultTTLMultiple] intervals for a state event, At for a delete, and the zero
// time (no end) for a state event with no interval.
func (s NamespaceSighting) AliveUntil() time.Time {
	switch {
	case s.Deleted:
		return s.At
	case s.Interval <= 0:
		return time.Time{}
	}
	return s.At.Add(DefaultTTLMultiple * s.Interval)
}
