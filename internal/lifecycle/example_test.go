package lifecycle_test

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

func offsets(ivs []lifecycle.Interval) {
	for _, i := range ivs {
		end := "open"
		if !i.Open() {
			end = i.End.Sub(base).String()
		}
		fmt.Printf("[%s, %s) ended by %s\n", i.Start.Sub(base), end, i.EndSource)
	}
}

// One producer deleting a subject another still sees does not end it: the
// subject is gone only when the last producer lets go, and silence ends a
// reference at its deadline.
func ExampleFold() {
	tl, _ := lifecycle.Fold([]lifecycle.Assertion{
		{Producer: "k8sobjects", EventTime: at(0), Seq: 1, Kind: lifecycle.Observe}, // watch mode: no TTL
		{
			Producer: "node-collector", EventTime: at(time.Minute), Through: at(12 * time.Minute), Seq: 2,
			Kind: lifecycle.Observe, TTL: 5 * time.Minute, // heartbeats from 1m through 12m
		},
		{Producer: "k8sobjects", EventTime: at(10 * time.Minute), Seq: 3, Kind: lifecycle.Delete},
	}, lifecycle.Policy{})

	offsets(tl.Existence())
	fmt.Println(tl.AliveAt(at(11*time.Minute)), tl.AliveAt(at(17*time.Minute)))

	// Output:
	// [0s, 17m0s) ended by liveness_expiry
	// true false
}

// A heartbeating producer's refreshes collapse into one run, with the same
// answers.
func ExampleCoalesce() {
	var beats []lifecycle.Assertion
	for i := range 1440 { // a day of one-minute heartbeats
		beats = append(beats, lifecycle.Assertion{
			Producer: "node-collector", EventTime: at(time.Duration(i) * time.Minute), Seq: uint64(i + 1),
			Kind: lifecycle.Observe, TTL: 4 * time.Minute,
			Attrs: []identity.Attr{{Key: catalog.AttributeKey("host.name"), Value: "web-1"}},
		})
	}
	runs := lifecycle.Coalesce(beats)
	fmt.Println(len(beats), "->", len(runs))

	tl, _ := lifecycle.Fold(runs, lifecycle.Policy{})
	offsets(tl.Existence())

	// Output:
	// 1440 -> 1
	// [0s, 24h3m0s) ended by liveness_expiry
}
