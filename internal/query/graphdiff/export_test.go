package graphdiff

import (
	"context"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// ReadScope makes a diff's reads and returns every element read, the unchanged
// ones included, so the tests can check the presence of each against the
// store's point reads.
func ReadScope(ctx context.Context, st store.Store, scope []identity.Fingerprint, t1, t2 time.Time, asOf uint64, rank map[lifecycle.Producer]int) (token uint64, entities, edges []interval.Element, err error) {
	fps, _, err := check(scope, t1, t2, Policy{MaxScope: len(scope), MaxListed: 1})
	if err != nil {
		return 0, nil, nil, err
	}
	rd, err := readScope(ctx, st, fps, interval.Window{From: t1, To: t2.Add(time.Nanosecond), AsOf: asOf}, rank)
	return rd.token, rd.entities, rd.edges, err
}
