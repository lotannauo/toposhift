package backup_test

import (
	"errors"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/backup"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// T5: the boot history of a long-gone host. Host X is seen only well before the L1
// horizon, then the backup retires l1, and X's records must all still be there:
// the live store remembers every boot and every collision for ever.
//
// The asked horizon is base + 3h, so L1's is base + 2h30m and its records are
// retired up to base + 2h20m.
const nodeCollector = lifecycle.Producer("node-collector")

type hostScript struct {
	*script
	x, y, rack identity.Fingerprint
}

func newHostScript(t *testing.T, hooks backup.Hooks) *hostScript {
	t.Helper()
	return &hostScript{
		script: newScript(t, storetest.QuarantinePolicy(), hooks),
		x:      fingerprint(t, catalog.Host, catalog.HostID, "host-x"),
		y:      fingerprint(t, catalog.Host, catalog.HostID, "host-y"),
		rack:   fingerprint(t, catalog.Rack, catalog.RackID, "rack-1"),
	}
}

// hostSeen is an Observe of host X at base+at, with a boot id or none.
func (s *hostScript) hostSeen(at time.Duration, boot string) store.Record {
	r := observe(catalog.L1, store.EntitySubject(s.x), nodeCollector, base.Add(at), shadowTTL)
	r.Boot = boot
	return r
}

// located is host Y's placement in the rack, an l1 edge, at base+at.
func (s *hostScript) located(at time.Duration) store.Record {
	return observe(catalog.L1, store.EdgeSubject(s.y, s.rack, catalog.LocatedIn), "fabric", base.Add(at), shadowTTL)
}

// finish commits l1 placements early and late, cuts each, retains to base + 3h,
// and retires, which must retire l1.
func (s *hostScript) finish() backup.Retirement {
	s.t.Helper()
	s.commit(base.Add(time.Hour), s.located(time.Hour))
	s.cutNow()
	s.commit(base.Add(170*time.Minute), s.located(170*time.Minute))
	s.cutNow()
	s.retain(base.Add(3 * time.Hour))
	ret := s.retire()
	if ret.Files[backup.ClassL1] == 0 {
		s.t.Fatal("no l1 file was retired")
	}
	return ret
}

// lateB1 is X observed with b1 again at base + 3h10m, after the horizons.
func (s *hostScript) lateB1(seq uint64) store.Record {
	r := s.hostSeen(190*time.Minute, "b1")
	r.Seq = seq
	return r
}

// check restores into each engine and compares; with late, both stores then take
// X's late sighting of b1 before the comparison. It returns the first mismatch.
func (s *hostScript) check(engine string, late bool, o ...func(*backup.RestoreOptions)) error {
	s.t.Helper()
	r, _ := restoreInto(s.t, s.segs, prefix, engine, s.meta, 7, o...)
	written := s.written
	if late {
		rec := s.lateB1(s.live.LastSeq() + 1)
		for _, st := range []store.Store{s.live, r} {
			if err := st.Write(ctx, []store.Record{rec}); err != nil {
				s.t.Fatal(err)
			}
		}
		written = append(written, rec)
	}
	return storetest.CompareRestored(s.live, r, storetest.RestoredOptions{
		Entities: []identity.Fingerprint{s.x, s.y, s.rack}, Written: written, Span: 10 * time.Minute,
	})
}

// quarantined reports whether X is quarantined in the live store, with the
// collision it names.
func (s *hostScript) quarantined() *lifecycle.CloneCollisionError {
	_, err := s.live.Alive(ctx, s.x, base.Add(4*time.Hour), store.Current(catalog.L1))
	var qe *store.QuarantineError
	if errors.As(err, &qe) {
		return qe.Collision
	}
	return nil
}

func TestHostBootHistoryIsKept(t *testing.T) {
	t.Parallel()
	retireHosts := backup.Hooks{RetireHosts: true}
	byBoot := backup.Hooks{ClassifyByBoot: true}
	// T5a: b1 at 10 min, then the reboot b2 at 30 min, both lapse. b1 seen again
	// after b2 first appeared is a collision by the boot rule: stale b1 observed at
	// 3h10m, newer b2 first seen at 30 min.
	t5a := func(s *hostScript) {
		s.commit(base.Add(10*time.Minute), s.hostSeen(10*time.Minute, "b1"))
		s.cutNow()
		s.commit(base.Add(30*time.Minute), s.hostSeen(30*time.Minute, "b2"))
		s.cutNow()
	}
	// T5b: b1, then b2, then b1 again, all within three minutes and long before the
	// horizon: a collision older than the cut.
	t5b := func(s *hostScript) {
		s.commit(base.Add(10*time.Minute), s.hostSeen(10*time.Minute, "b1"))
		s.commit(base.Add(11*time.Minute), s.hostSeen(11*time.Minute, "b2"))
		s.commit(base.Add(12*time.Minute), s.hostSeen(12*time.Minute, "b1"))
		s.cutNow()
	}
	// T5c: at b1's only sighting, a later record of the same producer at the same
	// instant carries no boot, so the fold takes no sighting of b1 there. b1 then
	// first appears at its late sighting, after b2, which is no collision.
	t5c := func(s *hostScript) {
		s.commit(base.Add(10*time.Minute), s.hostSeen(10*time.Minute, "b1"), s.hostSeen(10*time.Minute, ""))
		s.cutNow()
		s.commit(base.Add(30*time.Minute), s.hostSeen(30*time.Minute, "b2"))
		s.cutNow()
	}
	tests := []struct {
		name  string
		setup func(*hostScript)
		late  bool
		// quarantined is the live store's answer for X, once the late batch is in.
		quarantined bool
		// negative is the hook whose restore must differ, and its restore options.
		negative backup.Hooks
		negOpt   []func(*backup.RestoreOptions)
	}{
		{name: "T5a, an old boot returns", setup: t5a, late: true, quarantined: true, negative: retireHosts},
		{name: "T5b, an old collision", setup: t5b, quarantined: true, negative: retireHosts},
		{
			name: "T5c, a boot suppressed by a record without one", setup: t5c, late: true, quarantined: false,
			negative: byBoot, negOpt: []func(*backup.RestoreOptions){func(o *backup.RestoreOptions) { *o = backup.RestoreClassifiedByBoot(*o) }},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			for _, engine := range []string{engineMem, enginePebble} {
				s := newHostScript(t, backup.Hooks{})
				tt.setup(s)
				s.finish()
				if err := s.check(engine, tt.late); err != nil {
					t.Fatalf("%s: %v", engine, err)
				}
				cc := s.quarantined()
				if (cc != nil) != tt.quarantined {
					t.Fatalf("%s: X quarantined: %v, want %v", engine, cc, tt.quarantined)
				}
				if tt.name[:3] == "T5a" && (cc.StaleBoot != "b1" || cc.NewerBoot != "b2" ||
					!cc.ObservedAt.Equal(base.Add(190*time.Minute)) || !cc.NewerFirstSeen.Equal(base.Add(30*time.Minute))) {
					t.Fatalf("%s: the collision is %+v", engine, cc)
				}
				// The negative case: the defect it stands for must change an answer.
				s = newHostScript(t, tt.negative)
				tt.setup(s)
				ret := s.finish()
				if tt.negative.RetireHosts && ret.Files[backup.ClassHosts] == 0 {
					t.Fatalf("%s: retiring hosts like l1 retired none", engine)
				}
				if err := s.check(engine, tt.late, tt.negOpt...); !errors.Is(err, storetest.ErrMismatch) {
					t.Fatalf("%s: the negative case %+v restores without a difference: %v", engine, tt.negative, err)
				}
			}
		})
	}
}
