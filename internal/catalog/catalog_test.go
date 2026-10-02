package catalog_test

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// validEntities and validRelations return a small valid catalog. Each case
// below breaks exactly one rule in a copy of it.
func validEntities() []catalog.EntitySpec {
	return []catalog.EntitySpec{
		{Type: "host", Layer: catalog.L1, Keys: []catalog.Key{{Name: "host.id", Kind: catalog.KindString}}},
		{Type: "pod", Layer: catalog.L2, Keys: []catalog.Key{{Name: "pod.uid", Kind: catalog.KindString}}},
	}
}

func validRelations() []catalog.RelationSpec {
	return []catalog.RelationSpec{{
		Type:        "runs_on",
		Endpoints:   []catalog.Endpoint{{From: "pod", To: "host"}},
		Propagation: catalog.PropagateDown,
		Storage:     catalog.StorageFrom,
	}}
}

func TestNewRejects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(e *[]catalog.EntitySpec, r *[]catalog.RelationSpec)
		want   error  // sentinel the error must wrap
		offend string // text the error must contain to name the offender
	}{
		{
			name: "duplicate entity",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				*e = append(*e, (*e)[0])
			},
			want: catalog.ErrDuplicate, offend: `"host"`,
		},
		{
			name: "entity name with colon",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Type = "ho:st"
			},
			want: catalog.ErrInvalid, offend: `"ho:st"`,
		},
		{
			name: "entity name uppercase",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Type = "Host"
			},
			want: catalog.ErrInvalid, offend: `"Host"`,
		},
		{
			name: "entity name empty",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Type = ""
			},
			want: catalog.ErrInvalid, offend: `entity ""`,
		},
		{
			name: "zero layer",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Layer = 0
			},
			want: catalog.ErrInvalid, offend: "Layer(0)",
		},
		{
			name: "layer out of range",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Layer = catalog.L3 + 1
			},
			want: catalog.ErrInvalid, offend: "Layer(5)",
		},
		{
			name: "no keys",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys = nil
			},
			want: catalog.ErrInvalid, offend: "no identifying keys",
		},
		{
			name: "duplicate key",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys = []catalog.Key{{Name: "host.id", Kind: catalog.KindString}, {Name: "host.id", Kind: catalog.KindString}}
			},
			want: catalog.ErrDuplicate, offend: `key "host.id"`,
		},
		{
			name: "key name invalid",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys = []catalog.Key{{Name: "host id", Kind: catalog.KindString}}
			},
			want: catalog.ErrInvalid, offend: `key "host id"`,
		},
		{
			name: "all keys optional",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys = []catalog.Key{{Name: "host.id", Kind: catalog.KindString, Optional: true}}
			},
			want: catalog.ErrInvalid, offend: "all keys are optional",
		},
		{
			name: "zero key kind",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys[0].Kind = 0
			},
			want: catalog.ErrInvalid, offend: "Kind(0)",
		},
		{
			name: "key kind out of range",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[0].Keys[0].Kind = catalog.KindTime + 1
			},
			want: catalog.ErrInvalid, offend: "Kind(4)",
		},
		{
			name: "same key with two kinds",
			mutate: func(e *[]catalog.EntitySpec, _ *[]catalog.RelationSpec) {
				(*e)[1].Keys = []catalog.Key{{Name: "host.id", Kind: catalog.KindInt}}
			},
			want: catalog.ErrInvalid, offend: `conflicts with kind string declared by entity "host"`,
		},
		{
			name: "duplicate relation",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				*r = append(*r, (*r)[0])
			},
			want: catalog.ErrDuplicate, offend: `"runs_on"`,
		},
		{
			name: "relation name invalid",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Type = "runs on"
			},
			want: catalog.ErrInvalid, offend: `"runs on"`,
		},
		{
			name: "zero propagation",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Propagation = 0
			},
			want: catalog.ErrInvalid, offend: "Propagation(0)",
		},
		{
			name: "zero storage",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Storage = 0
			},
			want: catalog.ErrInvalid, offend: "Storage(0)",
		},
		{
			name: "stored relation without endpoints",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Endpoints = nil
			},
			want: catalog.ErrInvalid, offend: "at least one endpoint",
		},
		{
			name: "endpoint from unregistered type",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Endpoints = []catalog.Endpoint{{From: "ghost", To: "host"}}
			},
			want: catalog.ErrUnknown, offend: `"ghost"`,
		},
		{
			name: "endpoint to unregistered type",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				(*r)[0].Endpoints = []catalog.Endpoint{{From: "pod", To: "ghost"}}
			},
			want: catalog.ErrUnknown, offend: `"ghost"`,
		},
		{
			name: "duplicate endpoint",
			mutate: func(_ *[]catalog.EntitySpec, r *[]catalog.RelationSpec) {
				ep := (*r)[0].Endpoints[0]
				(*r)[0].Endpoints = []catalog.Endpoint{ep, ep}
			},
			want: catalog.ErrDuplicate, offend: "endpoint pod -> host",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e, r := validEntities(), validRelations()
			tt.mutate(&e, &r)

			c, err := catalog.New(e, r)
			if err == nil {
				t.Fatalf("New succeeded, want error wrapping %v", tt.want)
			}
			if c != nil {
				t.Error("New returned a catalog alongside an error")
			}
			if !errors.Is(err, tt.want) {
				t.Errorf("error does not wrap %v: %v", tt.want, err)
			}
			if !strings.Contains(err.Error(), tt.offend) {
				t.Errorf("error does not name %q: %v", tt.offend, err)
			}
		})
	}
}

func TestNewReportsEveryViolation(t *testing.T) {
	t.Parallel()

	e, r := validEntities(), validRelations()
	e[0].Layer = 0                                                   // ErrInvalid
	e = append(e, e[1])                                              // ErrDuplicate
	r[0].Endpoints = []catalog.Endpoint{{From: "ghost", To: "host"}} // ErrUnknown

	_, err := catalog.New(e, r)
	for _, want := range []error{catalog.ErrInvalid, catalog.ErrDuplicate, catalog.ErrUnknown} {
		if !errors.Is(err, want) {
			t.Errorf("error does not wrap %v: %v", want, err)
		}
	}
}

func TestNewAcceptsDerivedRelations(t *testing.T) {
	t.Parallel()

	for name, endpoints := range map[string][]catalog.Endpoint{
		"unconstrained": nil,
		"constrained":   {{From: "pod", To: "pod"}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := append(validRelations(), catalog.RelationSpec{
				Type:        "co_resident",
				Endpoints:   endpoints,
				Propagation: catalog.PropagateNone,
				Storage:     catalog.StorageDerived,
			})
			c, err := catalog.New(validEntities(), r)
			if err != nil {
				t.Fatal(err)
			}
			rel, _ := c.Relation("co_resident")
			if !rel.Derived() {
				t.Error("Derived() = false, want true")
			}
		})
	}
}

// TestNewCopiesItsInputs checks that mutating the specs after New cannot
// change the catalog.
func TestNewCopiesItsInputs(t *testing.T) {
	t.Parallel()

	e, r := validEntities(), validRelations()
	c, err := catalog.New(e, r)
	if err != nil {
		t.Fatal(err)
	}

	e[0].Keys[0].Name = "mutated"
	r[0].Endpoints[0].To = "mutated"

	host, _ := c.Entity("host")
	if _, ok := host.Key("host.id"); !ok {
		t.Error("mutating the entity spec changed the catalog")
	}
	runsOn, _ := c.Relation("runs_on")
	if !runsOn.Allows("pod", "host") {
		t.Error("mutating the relation spec changed the catalog")
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	c, err := catalog.New(validEntities(), validRelations())
	if err != nil {
		t.Fatal(err)
	}

	if e, ok := c.Entity("pod"); !ok || e.Type() != "pod" || e.Layer() != catalog.L2 {
		t.Errorf("Entity(pod) = %v, %v", e, ok)
	}
	if _, ok := c.Entity("ghost"); ok {
		t.Error("Entity(ghost) found")
	}
	if r, ok := c.Relation("runs_on"); !ok || r.Type() != "runs_on" {
		t.Errorf("Relation(runs_on) = %v, %v", r, ok)
	}
	if _, ok := c.Relation("ghost"); ok {
		t.Error("Relation(ghost) found")
	}

	var got []catalog.EntityType
	for e := range c.Entities() {
		got = append(got, e.Type())
	}
	if want := []catalog.EntityType{"host", "pod"}; !slices.Equal(got, want) {
		t.Errorf("Entities() order = %v, want declaration order %v", got, want)
	}
}

func TestRelationAllows(t *testing.T) {
	t.Parallel()

	c, err := catalog.New(validEntities(), append(validRelations(), catalog.RelationSpec{
		Type:        "same_as",
		Propagation: catalog.PropagateNone,
		Storage:     catalog.StorageDerived,
	}))
	if err != nil {
		t.Fatal(err)
	}
	runsOn, _ := c.Relation("runs_on")
	sameAs, _ := c.Relation("same_as")

	tests := []struct {
		name     string
		rel      catalog.Relation
		from, to catalog.EntityType
		want     bool
	}{
		{"declared direction", runsOn, "pod", "host", true},
		{"reversed", runsOn, "host", "pod", false},
		{"wrong types", runsOn, "host", "host", false},
		{"unconstrained accepts any pair", sameAs, "host", "pod", true},
	}
	for _, tt := range tests {
		if got := tt.rel.Allows(tt.from, tt.to); got != tt.want {
			t.Errorf("%s: Allows(%s, %s) = %v, want %v", tt.name, tt.from, tt.to, got, tt.want)
		}
	}
}

func TestEnumStrings(t *testing.T) {
	t.Parallel()

	tests := []struct{ got, want string }{
		{catalog.L0.String(), "L0"},
		{catalog.L3.String(), "L3"},
		{catalog.Layer(0).String(), "Layer(0)"},
		{catalog.KindString.String(), "string"},
		{catalog.KindInt.String(), "int"},
		{catalog.KindTime.String(), "time"},
		{catalog.Kind(0).String(), "Kind(0)"},
		{catalog.PropagateDown.String(), "down"},
		{catalog.PropagateUp.String(), "up"},
		{catalog.PropagateNone.String(), "none"},
		{catalog.Propagation(0).String(), "Propagation(0)"},
		{catalog.StorageFrom.String(), "from"},
		{catalog.StorageDerived.String(), "derived"},
		{catalog.Storage(0).String(), "Storage(0)"},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("String() = %q, want %q", tt.got, tt.want)
		}
	}
}

func TestNameValid(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want bool
	}{
		{"host", true},
		{"k8s.node", true},
		{"process.creation.time", true},
		{"a_b.c_d", true},
		{"", false},
		{"Host", false},
		{"host:id", false},
		{"host id", false},
		{".host", false},
		{"host.", false},
		{"host..id", false},
		{"1host", false},
		{"host\n", false},
		{strings.Repeat("a", catalog.MaxNameLen), true},
		{strings.Repeat("a", catalog.MaxNameLen+1), false},
	}
	for _, tt := range tests {
		if got := catalog.EntityType(tt.name).Valid(); got != tt.want {
			t.Errorf("EntityType(%q).Valid() = %v, want %v", tt.name, got, tt.want)
		}
		if got := catalog.RelationType(tt.name).Valid(); got != tt.want {
			t.Errorf("RelationType(%q).Valid() = %v, want %v", tt.name, got, tt.want)
		}
		if got := catalog.AttributeKey(tt.name).Valid(); got != tt.want {
			t.Errorf("AttributeKey(%q).Valid() = %v, want %v", tt.name, got, tt.want)
		}
	}
}
