package lifecycle

import (
	"sort"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// Attribute is one merged attribute and the producer that supplied it.
type Attribute struct {
	// Value is as the producer gave it; it is not copied.
	Value    any
	Producer Producer
}

// Description is the merged description of a subject at an instant.
type Description map[catalog.AttributeKey]Attribute

// DescribeAt returns the merged description of the subject at x and whether it
// exists then. Each producer with a live reference contributes its latest
// complete description, and each attribute goes to the producer that
// outranks the others under the policy; see the package documentation. A
// subject that does not exist at x has no description.
func (t Timeline) DescribeAt(x time.Time) (Description, bool) {
	var desc Description
	for producer, list := range t.refs {
		a, ok := liveAt(list, x)
		if !ok {
			continue
		}
		if desc == nil {
			desc = make(Description, len(a.Attrs))
		}
		for _, attr := range a.Attrs {
			if cur, has := desc[attr.Key]; !has || t.policy.outranks(producer, cur.Producer) {
				desc[attr.Key] = Attribute{Value: attr.Value, Producer: producer}
			}
		}
	}
	return desc, desc != nil
}

// liveAt returns the assertion that holds a producer's reference at x, if the
// reference is live: the latest assertion at or before x, if it is an Observe
// that has not passed its deadline. list is ascending by event time.
func liveAt(list []Assertion, x time.Time) (Assertion, bool) {
	i := sort.Search(len(list), func(i int) bool { return list[i].EventTime.After(x) })
	if i == 0 {
		return Assertion{}, false
	}
	a := list[i-1]
	if a.Kind != Observe || (a.TTL != 0 && !x.Before(a.deadline())) {
		return Assertion{}, false
	}
	return a, true
}
