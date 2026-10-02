package lifecycle_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// FuzzFold builds a valid assertion set from arbitrary bytes. Whatever the
// shape, the fold must not panic, must agree with the oracle on whether the
// subject exists, and must give the same answer once coalesced.
func FuzzFold(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{0, 1, 0, 2, 1, 0, 0, 2, 5, 2, 1, 0})
	f.Add([]byte{0, 0, 0, 1, 0, 3, 1, 0, 1, 1, 0, 4, 2, 0, 9, 3, 1, 8})

	f.Fuzz(func(t *testing.T, data []byte) {
		var as []lifecycle.Assertion
		for i := 0; i+6 <= len(data) && len(as) < 40; i += 6 {
			b := data[i : i+6]
			a := lifecycle.Assertion{
				Producer:  genProducers[int(b[0])%len(genProducers)],
				EventTime: sec(int64(b[2] % 40)),
				Seq:       uint64(len(as) + 1),
			}
			if b[1]%5 == 0 {
				a.Kind = lifecycle.Delete
			} else {
				a.Kind = lifecycle.Observe
				a.TTL = time.Duration(genTTLs[int(b[3])%len(genTTLs)]) * time.Second
				if b[4]&1 != 0 {
					a.Attrs = append(a.Attrs, identity.Attr{Key: "a", Value: genValues[int(b[4]>>1)%len(genValues)]})
				}
				if b[5]%4 == 0 {
					a.Through = a.EventTime.Add(time.Duration(b[5]%10) * time.Second)
				}
			}
			as = append(as, a)
		}

		tl, err := lifecycle.Fold(as, lifecycle.Policy{})
		if err != nil {
			t.Fatalf("a valid set was rejected: %v", err)
		}
		short, err := lifecycle.Fold(lifecycle.Coalesce(as), lifecycle.Policy{})
		if err != nil {
			t.Fatalf("a coalesced set was rejected: %v", err)
		}
		for _, x := range probes(as) {
			want := oracleAlive(as, x)
			if tl.AliveAt(x) != want || short.AliveAt(x) != want {
				t.Fatalf("AliveAt(%s) = %v / coalesced %v, oracle says %v", x.Sub(base), tl.AliveAt(x), short.AliveAt(x), want)
			}
		}
	})
}
