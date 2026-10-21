package doltengine_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltmodel"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// A scenario is a short, hand-made history that puts one rule of the design under the
// engine, and checks the engine against the model after each step that matters. Each is
// deterministic, so a defect of the engine is caught or it is not.
type scenario struct {
	name string
	run  func(ctx context.Context, p *pair) error
}

// steps runs the steps in order and stops at the first error.
func steps(ctx context.Context, p *pair, fs ...func() error) error {
	for i, f := range fs {
		if err := f(); err != nil {
			return fmt.Errorf("step %d: %w", i+1, err)
		}
	}
	return nil
}

func (p *pair) agreeNow(ctx context.Context) func() error {
	return func() error { return p.agree(ctx) }
}

func (p *pair) writing(rs ...engine.Record) func() error {
	return func() error { return p.write(rs...) }
}

func (p *pair) retaining(h time.Time) func() error { return func() error { return p.retain(h) } }

var scenarios = []scenario{
	// A late batch has an older event time than the batch before it. A read as of a commit
	// takes each key's row if the row's event is at or before the instant, and sends the key
	// to the log otherwise, so a late record is found and a record after the instant never is.
	{"settlement", func(ctx context.Context, p *pair) error {
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 100, 0)),
			p.writing(del(edge1, 2, "a", 150)),
			p.writing(observe(edge1, 3, "a", 50, 0)),
			p.agreeNow(ctx),
			p.writing(observe(edge2, 4, "a", 160, 0), observe(edge2, 5, "b", 160, 0)),
			p.agreeNow(ctx),
		)
	}},
	// At one instant the higher sequence number is the newer, whichever batch it is in.
	{"ties", func(ctx context.Context, p *pair) error {
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 100, 30), observe(edge1, 2, "a", 100, 5)),
			p.writing(observe(edge2, 3, "a", 100, 0)),
			p.writing(del(edge2, 4, "a", 100)),
			p.writing(observe(edge2, 5, "b", 100, 0), observe(edge2, 6, "a", 100, 0)),
			p.agreeNow(ctx),
		)
	}},
	// A record's life ends at its deadline: its event time plus the TTL, the later of that
	// and Through plus the TTL for a run, never for a record with no TTL, at its event time
	// for a delete.
	{"deadlines", func(ctx context.Context, p *pair) error {
		run := observe(edge1, 2, "a", 100, 30)
		run.Through = at(120)
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "b", 100, 30), run, observe(edge3, 3, "a", 100, 0)),
			p.writing(observe(edge2, 4, "a", 10, 1), del(edge3, 5, "b", 130)),
			p.agreeNow(ctx),
		)
	}},
	// An entity is alive by the same rule as an edge, in its own layer.
	{"entities", func(ctx context.Context, p *pair) error {
		return steps(ctx, p,
			p.writing(existence(pod1, 1, "a", 100, 30), existence(node1, 2, "a", 90, 0)),
			p.writing(existence(pod1, 3, "b", 120, 0)),
			p.writing(existence(pod1, 4, "a", 105, 0), existence(node1, 5, "a", 95, 1)),
			p.agreeNow(ctx),
			func() error {
				d := existence(pod1, 6, "b", 140, 0)
				d.Kind, d.Payload = lifecycle.Delete, nil
				return p.write(d)
			},
			p.agreeNow(ctx),
		)
	}},
	// Records arrive in any order of event time, and a late one changes no state.
	{"late records", func(ctx context.Context, p *pair) error {
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 200, 0)),
			p.writing(observe(edge1, 2, "a", 100, 0), observe(edge1, 3, "a", 150, 40)),
			p.writing(observe(edge1, 4, "b", 120, 0), existence(pod1, 5, "a", 130, 0)),
			p.writing(observe(edge1, 6, "b", 110, 0), observe(edge2, 7, "a", 105, 0), existence(pod1, 8, "a", 125, 0)),
			p.agreeNow(ctx),
		)
	}},
	// A retention keeps what holds at the horizon and after, deletes the state rows that
	// have lapsed, a delete among them, and refuses what is before it; and the reads at the
	// horizon, after it, and as of a token at or after the retention's are what they were.
	{"retention", func(ctx context.Context, p *pair) error {
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 10, 0), del(edge1, 2, "a", 12), observe(edge2, 3, "a", 10, 5), observe(edge3, 4, "a", 10, 0)),
			p.writing(observe(edge1, 5, "b", 14, 0), existence(pod1, 6, "a", 11, 3), existence(pod2, 7, "a", 11, 0), observe(edge2, 8, "c", 19, 0)),
			p.agreeNow(ctx),
			p.retaining(at(20)),
			p.agreeNow(ctx),
			p.writing(observe(edge2, 9, "a", 25, 0), existence(pod2, 10, "a", 26, 0)),
			func() error { return expectRefusal(p.write(observe(edge1, 11, "a", 19, 0)), engine.ErrBeforeHorizon) },
			p.writing(observe(edge1, 11, "a", 20, 0)),
			p.agreeNow(ctx),
			p.retaining(at(10)), // earlier: ignored
			p.retaining(at(40)),
			p.writing(observe(edge3, 12, "a", 41, 5), del(edge2, 13, "a", 50)),
			p.agreeNow(ctx),
		)
	}},
	// A horizon past the last instant leaves each key's newest record, and every later
	// record is before it.
	{"retention past the end", func(ctx context.Context, p *pair) error {
		last := observe(edge2, 4, "a", 0, 0)
		last.EventTime = engine.MaxEventTime
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 10, 0), observe(edge1, 2, "a", 11, 0), existence(pod1, 3, "a", 10, 0)),
			p.writing(last),
			p.retaining(engine.MaxEventTime.Add(time.Nanosecond)),
			func() error { return expectRefusal(p.write(observe(edge1, 5, "a", 100, 0)), engine.ErrBeforeHorizon) },
			func() error { return expectRefusal(p.write(last), engine.ErrInvalid) },
			p.agreeNow(ctx),
		)
	}},
	// A sequence number is a uint64: the tables keep its order across 2^63 and up to 2^64-1.
	{"sequence numbers across 2^63", func(ctx context.Context, p *pair) error {
		s := uint64(1<<63 - 3)
		return steps(ctx, p,
			p.writing(observe(edge1, s, "a", 100, 0), observe(edge2, s+1, "a", 101, 0)),
			p.writing(observe(edge1, s+2, "b", 90, 0), del(edge2, s+3, "a", 120)),
			p.writing(observe(edge3, math.MaxUint64-2, "a", 130, 0), observe(edge1, math.MaxUint64-1, "a", 140, 0)),
			p.agreeNow(ctx),
		)
	}},
	// Refused batches leave nothing behind, in the tables or in the counters.
	{"refusals", func(ctx context.Context, p *pair) error {
		big := observe(edge1, 4, "a", 100, 0)
		big.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity+1)
		long := observe(edge1, 4, "a", 100, 0)
		long.Producer = lifecycle.Producer(strings.Repeat("p", doltmodel.MaxProducerLen+1))
		layer := observe(edge1, 4, "a", 100, 0)
		layer.Layer = catalog.L3
		return steps(ctx, p,
			p.writing(observe(edge1, 1, "a", 100, 0), observe(edge2, 2, "a", 100, 0)),
			func() error { return expectRefusal(p.write(observe(edge2, 3, "b", 100, 0), big), engine.ErrInvalid) },
			func() error { return expectRefusal(p.write(observe(edge2, 3, "b", 100, 0), long), engine.ErrInvalid) },
			func() error { return expectRefusal(p.write(observe(edge2, 3, "b", 100, 0), layer), engine.ErrInvalid) },
			func() error { return expectRefusal(p.write(observe(edge2, 2, "b", 100, 0)), engine.ErrInvalid) },
			func() error {
				bad := observe(edge2, 4, "b", 100, 0)
				bad.Producer = ""
				return expectRefusal(p.write(observe(edge2, 3, "b", 100, 0), bad), engine.ErrInvalid)
			},
			p.writing(observe(edge2, 3, "b", 100, 0)), // the valid record of the refused batches was not stored
			p.agreeNow(ctx),
		)
	}},
	// The edges of the range: the first and the last instants, deadlines at the last.
	{"extremes", func(ctx context.Context, p *pair) error {
		first := observe(edge1, 1, "a", 0, 0)
		last := observe(edge2, 2, "a", 0, 0)
		last.EventTime = engine.MaxEventTime
		nearEnd := observe(edge3, 3, "a", 0, 0)
		nearEnd.EventTime, nearEnd.TTL = engine.MaxEventTime.Add(-time.Hour), time.Hour
		// A deadline exactly at the last instant, from the TTL and from a run's Through (the
		// sum is the largest the column holds).
		exact := observe(engine.EdgeSubject(pod2, node1, "part_of"), 4, "a", 0, 0)
		exact.EventTime, exact.TTL = engine.MaxEventTime.Add(-time.Minute), time.Minute
		run := observe(engine.EdgeSubject(pod1, node1, "part_of"), 5, "a", 0, 0)
		run.EventTime, run.TTL, run.Through = engine.MaxEventTime.Add(-time.Hour), time.Minute, engine.MaxEventTime.Add(-time.Minute)
		return steps(ctx, p, p.writing(first, last, nearEnd), p.writing(exact, run), p.agreeNow(ctx))
	}},
	// A column's largest values: a producer and a relation of 255 bytes, fingerprints of a
	// type name of 255 bytes, a payload of the column's size.
	{"the longest keys", func(ctx context.Context, p *pair) error {
		long := strings.Repeat("a", catalog.MaxNameLen)
		a, err := identity.FingerprintFromHash(catalog.EntityType(long), [identity.FingerprintBytes]byte{255, 255})
		if err != nil {
			return err
		}
		b, err := identity.FingerprintFromHash(catalog.EntityType(long[:len(long)-1]+"b"), [identity.FingerprintBytes]byte{0, 0})
		if err != nil {
			return err
		}
		s := engine.EdgeSubject(a, b, catalog.RelationType(strings.Repeat("r", catalog.MaxNameLen)))
		r := observe(s, 1, lifecycle.Producer(strings.Repeat("p", doltmodel.MaxProducerLen)), 100, 0)
		r.Payload = make([]byte, doltmodel.RegisteredPayloadCapacity)
		for i := range r.Payload {
			r.Payload[i] = byte(i)
		}
		if err := p.write(r); err != nil {
			return err
		}
		tables, err := p.e.Tables(ctx)
		if err != nil {
			return err
		}
		if d := tables.FirstDifference(p.m.Tables()); d != "" {
			return fmt.Errorf("the longest keys: %s", d)
		}
		for _, c := range []struct {
			f   identity.Fingerprint
			dir engine.Direction
		}{{a, engine.Forward}, {b, engine.Reverse}} {
			want, _ := p.m.Neighbors(c.f, c.dir, at(200), engine.Current(catalog.L2))
			got, err := p.e.Neighbors(c.f, c.dir, at(200), engine.Current(catalog.L2))
			if err != nil {
				return fmt.Errorf("neighbors of a fingerprint with a 255-byte type: %w", err)
			}
			if len(got) != 1 || len(want) != 1 || got[0] != want[0] {
				return fmt.Errorf("neighbors of a fingerprint with a 255-byte type: %v; the model %v", got, want)
			}
		}
		return nil
	}},
	// Fingerprints are ordered as their bytes are, the type then a zero byte then the hash: a
	// type that is a prefix of another sorts first, and a hash that ends in zero bytes is not
	// padded or trimmed.
	{"fingerprint order", func(ctx context.Context, p *pair) error {
		mk := func(typ string, hash ...byte) identity.Fingerprint {
			var h [identity.FingerprintBytes]byte
			copy(h[:], hash)
			f, err := identity.FingerprintFromHash(catalog.EntityType(typ), h)
			if err != nil {
				panic(err)
			}
			return f
		}
		peers := []identity.Fingerprint{
			mk("k8s.pod", 1), mk("k8s", 9), mk("k8s.pod", 1, 0), mk("k8s.pod", 0), mk("k8s.pod"), mk("k8s.pod.x", 5), mk("k8sx", 1), mk("k8s", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1),
		}
		var batch []engine.Record
		for i, peer := range peers {
			batch = append(batch, observe(engine.EdgeSubject(node1, peer, catalog.ScheduledOn), uint64(i+1), "a", 100, 0))
		}
		if err := p.write(batch...); err != nil {
			return err
		}
		for _, dir := range []engine.Direction{engine.Forward, engine.Reverse} {
			for _, f := range append([]identity.Fingerprint{node1}, peers...) {
				want, _ := p.m.Neighbors(f, dir, at(200), engine.Current(catalog.L2))
				got, err := p.e.Neighbors(f, dir, at(200), engine.Current(catalog.L2))
				if err != nil {
					return err
				}
				if fmt.Sprint(got) != fmt.Sprint(want) {
					return fmt.Errorf("neighbors of %s %s: the engine %v, the model %v", f, dir, got, want)
				}
			}
		}
		return p.storesAgree(ctx)
	}},
	// A run of an entity and edges together, a payload of every length, empty payloads.
	{"payloads", func(ctx context.Context, p *pair) error {
		var batch []engine.Record
		for i, n := range []int{0, 1, 2, 17, 96, 255, 256} {
			r := observe(engine.EdgeSubject(pod1, fp("k8s.node", byte(10+i)), catalog.ScheduledOn), uint64(i+1), "a", 100+int64(i), 0)
			r.Payload = make([]byte, n)
			for j := range r.Payload {
				r.Payload[j] = byte(j * 7)
			}
			batch = append(batch, r)
		}
		return steps(ctx, p, p.writing(batch...), p.agreeNow(ctx))
	}},
}

func expectRefusal(err, class error) error {
	if !errors.Is(err, class) {
		return fmt.Errorf("a batch that must be refused with %w: %w", class, errors.Join(err, errors.New("(accepted)")))
	}
	return nil
}
