package xtmodel

import (
	"math/rand/v2"
	"slices"
	"testing"
)

const domain = 24

// paintAll applies statements, in order, to a picture of valid time: each instant is
// present or absent.
func paintAll(base [domain]bool, ops []vop) [domain]bool {
	pic := base
	for _, o := range ops {
		for i := o.from; i < min(o.to, domain); i++ {
			pic[i] = o.insert
		}
	}
	return pic
}

// TestMergeKeepsTheEffectAndShrinksTheStatements: on a small valid-time domain, the
// merged statements, applied in order to any starting picture, give the picture the
// original statements give, and there are no more of them. For a document with nothing stored (fresh), the
// starting picture is empty and no delete is kept. The streams are of a fixed seed.
func TestMergeKeepsTheEffectAndShrinksTheStatements(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 11))
	for iter := range 5000 {
		var ops []vop
		for range 1 + rng.IntN(8) {
			from := Instant(rng.IntN(domain - 1))
			to := from + 1 + Instant(rng.IntN(domain-int(from)))
			if rng.IntN(5) == 0 {
				to = Forever
			}
			ops = append(ops, vop{insert: rng.IntN(2) == 0, from: from, to: to})
		}
		fresh := iter%2 == 0
		var base [domain]bool
		if !fresh {
			for i := range base {
				base[i] = rng.IntN(2) == 0
			}
		}
		got := merge(ops, fresh, false)
		if want, have := paintAll(base, ops), paintAll(base, got); want != have {
			t.Fatalf("ops %+v (fresh %v): merged %+v paints %v, not %v", ops, fresh, got, have, want)
		}
		if len(got) > len(ops) {
			t.Fatalf("ops %+v merged into more: %+v", ops, got)
		}
		for _, o := range got {
			if o.from >= o.to || fresh && !o.insert {
				t.Fatalf("ops %+v (fresh %v): bad statement %+v in %+v", ops, fresh, o, got)
			}
		}
	}
}

// TestMergePaintsAGroupInTheFewestStatements: the cases a naive cut of the line gets
// wrong. A delete with an insert inside it is two statements, not three, and a hole in
// an insert is two, not three.
func TestMergePaintsAGroupInTheFewestStatements(t *testing.T) {
	ins := func(a, b Instant) vop { return vop{insert: true, from: a, to: b} }
	del := func(a, b Instant) vop { return vop{from: a, to: b} }
	for _, c := range []struct {
		name  string
		ops   []vop
		fresh bool
		want  []vop
	}{
		{"an insert inside a delete", []vop{del(13, 24), ins(22, 23)}, false, []vop{del(13, 24), ins(22, 23)}},
		{"a delete inside an insert", []vop{ins(10, 30), del(15, 20)}, false, []vop{ins(10, 30), del(15, 20)}},
		{"an insert with a delete either side", []vop{ins(10, 20), del(5, 10), del(20, 25)}, false, []vop{del(5, 25), ins(10, 20)}},
		{"a delete inside an insert, nothing stored", []vop{ins(10, 30), del(15, 20)}, true, []vop{ins(10, 15), ins(20, 30)}},
		{"separate stretches stay separate", []vop{ins(10, 12), del(20, 25), ins(30, 32)}, false, []vop{ins(10, 12), del(20, 25), ins(30, 32)}},
		{"touching inserts are one", []vop{ins(10, 12), ins(12, 20), ins(20, Forever)}, false, []vop{ins(10, Forever)}},
		{"a later delete cancels an earlier insert", []vop{ins(10, 20), del(10, 20)}, false, []vop{del(10, 20)}},
		{"and is nothing when nothing is stored", []vop{ins(10, 20), del(10, 20)}, true, nil},
	} {
		if got := merge(c.ops, c.fresh, false); !slices.Equal(got, c.want) {
			t.Errorf("%s: %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestMergeInReverseOrderChangesTheEffect(t *testing.T) {
	ops := []vop{{insert: true, from: 10, to: Forever}, {from: 10, to: Forever}}
	if got := merge(ops, false, false); !slices.Equal(got, []vop{{from: 10, to: Forever}}) {
		t.Errorf("in order: %+v", got)
	}
	if got := merge(ops, false, true); !slices.Equal(got, []vop{{insert: true, from: 10, to: Forever}}) {
		t.Errorf("reversed: %+v", got)
	}
}
