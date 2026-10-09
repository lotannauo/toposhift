package store_test

import (
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// fp resolves one fingerprint of the given type, failing the test if the
// identity does not resolve.
func fp(t *testing.T, typ catalog.EntityType, key catalog.AttributeKey, v string) identity.Fingerprint {
	t.Helper()
	id, err := identity.NewResolver(catalog.Default()).Resolve(typ, []identity.Attr{{Key: key, Value: v}})
	if err != nil {
		t.Fatal(err)
	}
	return id.Fingerprint()
}

func TestValidate(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	good := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s",
		EventTime: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Seq: 1, Kind: lifecycle.Observe,
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("a good record was rejected: %v", err)
	}

	edge := func(f func(*store.Record)) store.Record { r := good; f(&r); return r }
	invalid := map[string]store.Record{
		"zero subject":      edge(func(r *store.Record) { r.Subject = store.Subject{} }),
		"edge without peer": edge(func(r *store.Record) { r.Subject.B = identity.Fingerprint{} }),
		"edge without rel":  edge(func(r *store.Record) { r.Subject.Relation = "" }),
		"entity with peer":  edge(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Subject.B = node }),
		"entity with rel":   edge(func(r *store.Record) { r.Subject = store.EntitySubject(pod); r.Subject.Relation = catalog.PartOf }),
		"empty producer":    edge(func(r *store.Record) { r.Producer = "" }),
		"before 1970":       edge(func(r *store.Record) { r.EventTime = time.Unix(-1, 0) }),
		"after 2262":        edge(func(r *store.Record) { r.EventTime = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC) }),
		"through before":    edge(func(r *store.Record) { r.Through = r.EventTime.Add(-time.Second) }),
		"unset kind":        edge(func(r *store.Record) { r.Kind = 0 }),
		"unknown kind":      edge(func(r *store.Record) { r.Kind = 9 }),
		"entity in a layer other than its type's": edge(func(r *store.Record) {
			r.Subject = store.EntitySubject(pod) // a pod lives in L2
			r.Layer = catalog.L1
		}),
		"negative TTL":       edge(func(r *store.Record) { r.TTL = -time.Second }),
		"unset layer":        edge(func(r *store.Record) { r.Layer = 0 }),
		"layer out of range": edge(func(r *store.Record) { r.Layer = catalog.L3 + 1 }),
		"delete with TTL":    edge(func(r *store.Record) { r.Kind, r.TTL = lifecycle.Delete, time.Minute }),
		"delete with through": edge(func(r *store.Record) {
			r.Kind, r.Through = lifecycle.Delete, r.EventTime.Add(time.Minute)
		}),
		"delete with payload": edge(func(r *store.Record) { r.Kind, r.Payload = lifecycle.Delete, []byte("x") }),
		"through after 2262": edge(func(r *store.Record) {
			r.Through = time.Date(2263, 1, 1, 0, 0, 0, 0, time.UTC)
		}),
		"deadline after 2262": edge(func(r *store.Record) {
			r.EventTime, r.TTL = store.MaxEventTime.Add(-time.Second), time.Minute
		}),
		"run deadline after 2262": edge(func(r *store.Record) {
			r.EventTime = store.MaxEventTime.Add(-time.Hour)
			r.Through, r.TTL = store.MaxEventTime.Add(-time.Second), time.Minute
		}),
	}
	for name, r := range invalid {
		if err := r.Validate(); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: err = %v, want ErrInvalid", name, err)
		}
	}

	entity := good
	entity.Subject = store.EntitySubject(pod)
	atEnd := good
	atEnd.EventTime, atEnd.TTL = store.MaxEventTime.Add(-time.Minute), time.Minute
	atMin, atMax := good, good
	atMin.EventTime, atMax.EventTime = store.MinEventTime, store.MaxEventTime
	valid := map[string]store.Record{
		"an entity in the layer its type belongs to": entity,
		"a deadline exactly at the end of the range": atEnd,
		"the first representable event time":         atMin,
		"the last representable event time":          atMax,
	}
	for name, r := range valid {
		if err := r.Validate(); err != nil {
			t.Errorf("%s was rejected: %v", name, err)
		}
	}

	if store.MaxEventTime.UnixNano() != math.MaxInt64 {
		t.Errorf("MaxEventTime is %d ns, want the largest int64", store.MaxEventTime.UnixNano())
	}
}

// TestValidateWrapsTheFoldError checks that a record the lifecycle
// specification rejects reports both errors, so a caller can match either.
func TestValidateWrapsTheFoldError(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	r := store.Record{
		Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn),
		EventTime: time.Unix(10, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, // no producer
	}
	err := r.Validate()
	if !errors.Is(err, store.ErrInvalid) || !errors.Is(err, lifecycle.ErrInvalid) {
		t.Errorf("err = %v, want both store.ErrInvalid and lifecycle.ErrInvalid", err)
	}
}

func TestAssertionCarriesThePayloadAsADescription(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	obs := store.Record{
		Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: time.Unix(10, 0).UTC(), Seq: 3,
		Kind: lifecycle.Observe, TTL: time.Minute, Through: time.Unix(20, 0).UTC(), Payload: []byte("abc"),
	}
	a := obs.Assertion()
	if a.Producer != "k8s" || a.Seq != 3 || a.TTL != time.Minute || !a.Through.Equal(obs.Through) || len(a.Attrs) != 1 {
		t.Errorf("assertion = %+v", a)
	}
	if !a.EventTime.Equal(obs.EventTime) {
		t.Errorf("assertion event time = %s, want the record's %s", a.EventTime, obs.EventTime)
	}
	if a.Kind != lifecycle.Observe {
		t.Errorf("assertion kind = %v, want Observe", a.Kind)
	}

	// Equal payloads give equal descriptions, which is what lets a run of
	// refreshes coalesce into one assertion that extends through the last.
	later := obs
	later.Seq, later.EventTime, later.Through = 4, time.Unix(20, 0).UTC(), time.Time{}
	obs.Through = time.Time{}
	first, second := obs.Assertion(), later.Assertion()
	got := lifecycle.Coalesce([]lifecycle.Assertion{first, second})
	if len(got) != 1 || !got[0].EventTime.Equal(first.EventTime) || !got[0].Through.Equal(second.EventTime) {
		t.Errorf("two identical refreshes coalesced to %+v, want one run from 10s through 20s", got)
	}
	changed := later
	changed.Payload = []byte("different")
	if got := lifecycle.Coalesce([]lifecycle.Assertion{first, changed.Assertion()}); len(got) != 2 {
		t.Errorf("a changed payload coalesced to %d assertions, want 2", len(got))
	}

	del := obs
	del.Kind, del.TTL, del.Through, del.Payload = lifecycle.Delete, 0, time.Time{}, nil
	if got := del.Assertion(); len(got.Attrs) != 0 {
		t.Errorf("a delete carries attributes: %+v", got)
	}
}

func TestSubjectConstructors(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	if got, want := store.EntitySubject(pod), (store.Subject{Kind: store.SubjectEntity, A: pod}); got != want {
		t.Errorf("EntitySubject = %+v, want %+v", got, want)
	}
	if got, want := store.EdgeSubject(pod, node, catalog.ScheduledOn), (store.Subject{Kind: store.SubjectEdge, A: pod, B: node, Relation: catalog.ScheduledOn}); got != want {
		t.Errorf("EdgeSubject = %+v, want %+v", got, want)
	}
}

func TestAHostRecordCarriesItsBoot(t *testing.T) {
	t.Parallel()

	host := fp(t, catalog.Host, catalog.HostID, "h")
	node := fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	obs := func(at int64, boot string) store.Record {
		return store.Record{
			Layer: catalog.L1, Subject: store.EntitySubject(host), Producer: "node-collector",
			EventTime: time.Unix(at, 0).UTC(), Seq: uint64(at), Kind: lifecycle.Observe, TTL: time.Hour,
			Payload: []byte("d"), Boot: boot,
		}
	}
	if err := obs(10, "boot-1").Validate(); err != nil {
		t.Errorf("a host observation with a boot id: %v", err)
	}
	if err := obs(10, strings.Repeat("b", store.MaxBootLen)).Validate(); err != nil {
		t.Errorf("a boot id of exactly the cap: %v", err)
	}
	// White space is part of the value: padded and unpadded are two boots.
	if err := obs(10, " boot-1").Validate(); err != nil {
		t.Errorf("a padded boot id: %v", err)
	}
	if got := len(obs(10, "boot-1").Assertion().Attrs); got != 2 {
		t.Errorf("a host observation with a boot carries %d attributes, want the payload and the boot", got)
	}
	if err := obs(10, "").Validate(); err != nil {
		t.Errorf("a host observation without a boot id: %v", err)
	}

	for name, r := range map[string]store.Record{
		"blank boot id": obs(10, "  \t"),
		"delete with boot": func() store.Record {
			r := obs(10, "boot-1")
			r.Kind, r.TTL, r.Payload = lifecycle.Delete, 0, nil
			return r
		}(),
		"boot on a pod": {
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s",
			EventTime: time.Unix(10, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, TTL: time.Hour, Boot: "boot-1",
		},
		"boot on a node": {
			Layer: catalog.L2, Subject: store.EntitySubject(node), Producer: "k8s",
			EventTime: time.Unix(10, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, TTL: time.Hour, Boot: "boot-1",
		},
		"boot over the length cap":     obs(10, strings.Repeat("b", store.MaxBootLen+1)),
		"boot that is not valid UTF-8": obs(10, "boot-\xff"),
		"edge with boot": {
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, "scheduled_on"), Producer: "k8s",
			EventTime: time.Unix(10, 0).UTC(), Seq: 1, Kind: lifecycle.Observe, TTL: time.Hour, Boot: "boot-1",
		},
	} {
		if err := r.Validate(); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("%s: err = %v, want one wrapping ErrInvalid", name, err)
		}
	}

	// The boot reaches the lifecycle assertion as the specification's boot
	// attribute, so a policy that names it tells boots apart and reports two
	// live boots of one host as a clone collision.
	a := obs(10, "boot-1").Assertion()
	var got string
	for _, attr := range a.Attrs {
		if attr.Key == lifecycle.BootID {
			got, _ = attr.Value.(string)
		}
	}
	if got != "boot-1" {
		t.Errorf("assertion boot attribute = %q, want boot-1 (attrs %+v)", got, a.Attrs)
	}
	if len(obs(10, "").Assertion().Attrs) != 1 {
		t.Errorf("a record without a boot id carries %d attributes, want only the payload", len(obs(10, "").Assertion().Attrs))
	}
	policy := lifecycle.Policy{BootKey: lifecycle.BootID}
	clone := []lifecycle.Assertion{obs(10, "boot-1").Assertion(), obs(20, "boot-2").Assertion(), obs(30, "boot-1").Assertion()}
	if _, err := lifecycle.Fold(clone, policy); !errors.Is(err, lifecycle.ErrCloneCollision) {
		t.Errorf("a boot seen again after another: err = %v, want a clone collision", err)
	}
	reboot := []lifecycle.Assertion{obs(10, "boot-1").Assertion(), obs(20, "boot-2").Assertion()}
	tl, err := lifecycle.Fold(reboot, policy)
	if err != nil || len(tl.Boots()) != 2 {
		t.Errorf("a reboot: boots = %d, err = %v, want 2 boots and no error", len(tl.Boots()), err)
	}

	// A changed boot is a changed description: a run of refreshes does not
	// coalesce across it.
	if got := lifecycle.Coalesce([]lifecycle.Assertion{obs(10, "boot-1").Assertion(), obs(20, "boot-2").Assertion()}); len(got) != 2 {
		t.Errorf("refreshes across a boot change coalesced to %d assertions, want 2", len(got))
	}
}

func TestEventTimeBasisNamesAndValidity(t *testing.T) {
	t.Parallel()

	for _, c := range []struct {
		b     store.EventTimeBasis
		name  string
		valid bool
	}{
		{store.BasisUnknown, "unknown", true},
		{store.BasisObjectField, "object_field", true},
		{store.BasisObserved, "observed", true},
		{store.BasisReceipt, "receipt", true},
		{store.BasisProducerEvent, "producer_event", true},
		{5, "EventTimeBasis(5)", false},
		{255, "EventTimeBasis(255)", false},
	} {
		if got := c.b.String(); got != c.name {
			t.Errorf("basis %d: String = %q, want %q", uint8(c.b), got, c.name)
		}
		if got := c.b.Valid(); got != c.valid {
			t.Errorf("basis %d: Valid = %v, want %v", uint8(c.b), got, c.valid)
		}
	}
	if store.BasisUnknown != 0 {
		t.Errorf("the zero basis is %s, want unknown", store.EventTimeBasis(0))
	}
}

func TestValidateChecksTheEventTimeBasis(t *testing.T) {
	t.Parallel()

	pod, node := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p"), fp(t, catalog.K8sNode, catalog.K8sNodeUID, "n")
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	records := map[string]store.Record{
		"an entity observation": {
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: at, Seq: 1,
			Kind: lifecycle.Observe, Payload: []byte("p"),
		},
		"an entity delete": {
			Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: at, Seq: 2,
			Kind: lifecycle.Delete,
		},
		"an edge observation": {
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s", EventTime: at, Seq: 3,
			Kind: lifecycle.Observe, Payload: []byte("e"),
		},
		"an edge delete": {
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8s", EventTime: at, Seq: 4,
			Kind: lifecycle.Delete,
		},
	}
	for name, r := range records {
		for _, b := range []store.EventTimeBasis{store.BasisUnknown, store.BasisObjectField, store.BasisObserved, store.BasisReceipt, store.BasisProducerEvent} {
			r.EventTimeBasis = b
			if err := r.Validate(); err != nil {
				t.Errorf("%s with basis %s was rejected: %v", name, b, err)
			}
		}
		for _, b := range []store.EventTimeBasis{store.BasisProducerEvent + 1, 255} {
			r.EventTimeBasis = b
			err := r.Validate()
			if !errors.Is(err, store.ErrInvalid) {
				t.Errorf("%s with basis %d: err = %v, want ErrInvalid", name, uint8(b), err)
			} else if !strings.Contains(err.Error(), b.String()) {
				t.Errorf("%s with basis %d: err = %q, want it to name the value", name, uint8(b), err)
			}
		}
	}

	// A bad basis is refused for itself, before the fold: a record that is also
	// wrong for the fold reports the basis.
	r := records["an entity observation"]
	r.EventTimeBasis, r.Producer = store.BasisProducerEvent+1, ""
	if err := r.Validate(); err == nil || !strings.Contains(err.Error(), "event time basis") || errors.Is(err, lifecycle.ErrInvalid) {
		t.Errorf("a bad basis and an empty producer: err = %v, want the basis reported before the fold", err)
	}
}

// The value 3 is reserved for an operator purge. It is refused until its
// meaning is decided, and there is no constant for it.
func TestValidateRefusesTheReservedKind(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	r := store.Record{
		Layer: catalog.L2, Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: time.Unix(10, 0).UTC(), Seq: 1,
		Kind: 3,
	}
	for _, b := range []store.EventTimeBasis{store.BasisUnknown, store.BasisReceipt} {
		r.EventTimeBasis = b
		if err := r.Validate(); !errors.Is(err, store.ErrInvalid) {
			t.Errorf("kind 3 with basis %s: err = %v, want ErrInvalid", b, err)
		}
	}
}

func TestAssertionIgnoresTheEventTimeBasis(t *testing.T) {
	t.Parallel()

	pod := fp(t, catalog.K8sPod, catalog.K8sPodUID, "p")
	for _, kind := range []lifecycle.Kind{lifecycle.Observe, lifecycle.Delete} {
		r := store.Record{
			Subject: store.EntitySubject(pod), Producer: "k8s", EventTime: time.Unix(10, 0).UTC(), Seq: 3, Kind: kind,
		}
		if kind == lifecycle.Observe {
			r.TTL, r.Payload = time.Minute, []byte("abc")
		}
		want := r.Assertion()
		for _, b := range []store.EventTimeBasis{store.BasisObjectField, store.BasisObserved, store.BasisReceipt, store.BasisProducerEvent} {
			r.EventTimeBasis = b
			if got := r.Assertion(); !reflect.DeepEqual(got, want) {
				t.Errorf("%s with basis %s: assertion = %+v, want %+v", kind, b, got, want)
			}
		}
	}
}
