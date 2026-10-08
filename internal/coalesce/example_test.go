package coalesce_test

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// Three refreshes of one node, a minute apart, with a TTL of three minutes, become
// the record of the first and one extension of it. The refresh in between is absorbed:
// the extension at the third stands for it, and existence ends early by less than the
// half TTL the coalescer was configured to wait.
func Example() {
	co, err := coalesce.New(coalesce.Config{ExtendTTLFraction: 0.5})
	if err != nil {
		fmt.Println(err)
		return
	}
	id, err := identity.NewResolver(catalog.Default()).Resolve(catalog.K8sNode, []identity.Attr{{Key: catalog.K8sNodeUID, Value: "node-1"}})
	if err != nil {
		fmt.Println(err)
		return
	}
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	var seq uint64 // the sequencer's: Add leaves Seq to its caller
	for i := range 3 {
		refresh := store.Record{
			Layer:     catalog.L2,
			Subject:   store.EntitySubject(id.Fingerprint()),
			Producer:  "node-collector",
			EventTime: start.Add(time.Duration(i) * time.Minute),
			Kind:      lifecycle.Observe,
			TTL:       3 * time.Minute,
			Payload:   []byte("ready"),
		}
		written := co.Add(refresh, nil)
		fmt.Printf("refresh at +%dm: %d record(s)\n", i, len(written))
		for _, r := range written {
			seq++
			r.Seq = seq
			fmt.Printf("  seq %d: event time +%s, through %s\n", r.Seq, r.EventTime.Sub(start), through(start, r))
		}
	}
	fmt.Println("runs remembered:", co.Runs())

	// Output:
	// refresh at +0m: 1 record(s)
	//   seq 1: event time +0s, through none
	// refresh at +1m: 0 record(s)
	// refresh at +2m: 1 record(s)
	//   seq 2: event time +0s, through +2m0s
	// runs remembered: 1
}

func through(start time.Time, r store.Record) string {
	if r.Through.IsZero() {
		return "none"
	}
	return "+" + r.Through.Sub(start).String()
}
