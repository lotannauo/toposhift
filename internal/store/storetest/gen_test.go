package storetest_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"pgregory.net/rapid"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// bootPolicy is the policy under which a store tells the boots of a host apart by
// the boot ID its records carry.
var bootPolicy = lifecycle.Policy{BootKey: lifecycle.BootID}

// tb is what the helpers need of a test, so that a test and a rapid case both fit.
type tb interface {
	Helper()
	Fatalf(format string, args ...any)
}

func mustGenerator(t tb, c storetest.Config) *storetest.Generator {
	t.Helper()
	g, err := storetest.NewGenerator(c)
	if err != nil {
		t.Fatalf("NewGenerator(%+v): %v", c, err)
	}
	return g
}

func allRecords(t tb, c storetest.Config) []store.Record {
	t.Helper()
	return mustGenerator(t, c).All()
}

// everything is Tiny with every feature on.
func everything() storetest.Config {
	c := storetest.Tiny()
	c.Runs = true
	c.RebootProbability, c.CloneProbability = 0.2, 0.5
	c.LateProbability = 0.2
	c.ConfirmProbability = 0.5
	return c
}

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	type mod = func(*storetest.Config)
	tests := map[string]struct {
		mod   mod
		field string // the field the error must name; empty if the config is valid
	}{
		"Tiny":                               {func(*storetest.Config) {}, ""},
		"everything on":                      {func(c *storetest.Config) { *c = everything() }, ""},
		"watch mode":                         {func(c *storetest.Config) { c.HeartbeatInterval, c.TTLFactor = 0, 0 }, ""},
		"watch mode with a stale TTL factor": {func(c *storetest.Config) { c.HeartbeatInterval = 0 }, ""},
		"the smallest interval":              {func(c *storetest.Config) { c.HeartbeatInterval = 2 * time.Second }, ""},
		"the largest first seq":              {func(c *storetest.Config) { c.FirstSeq = 1 << 63 }, ""},
		"the widest payloads":                {func(c *storetest.Config) { c.PayloadMin, c.PayloadMax = 1, 256 }, ""},
		"no churn":                           {func(c *storetest.Config) { c.ChurnPerMinute = 0 }, ""},
		"no kubelet, no lateness":            {func(c *storetest.Config) { c.ConfirmProbability, c.LateProbability = 0, 0 }, ""},
		"unused confirm TTL and late max": {func(c *storetest.Config) {
			c.ConfirmProbability, c.ConfirmTTL, c.LateProbability, c.LateMax = 0, 0, 0, 0
		}, ""},
		"start in another location":           {func(c *storetest.Config) { c.Start = c.Start.In(time.FixedZone("x", 3600)) }, ""},
		"start zero":                          {func(c *storetest.Config) { c.Start = time.Time{} }, "Start"},
		"start off a second":                  {func(c *storetest.Config) { c.Start = c.Start.Add(500 * time.Millisecond) }, "Start"},
		"start before 1970":                   {func(c *storetest.Config) { c.Start = time.Date(1969, 12, 31, 0, 0, 0, 0, time.UTC) }, "Start"},
		"duration zero":                       {func(c *storetest.Config) { c.Duration = 0 }, "Duration"},
		"duration negative":                   {func(c *storetest.Config) { c.Duration = -time.Minute }, "Duration"},
		"duration off a second":               {func(c *storetest.Config) { c.Duration = 1500 * time.Millisecond }, "Duration"},
		"first seq zero":                      {func(c *storetest.Config) { c.FirstSeq = 0 }, "FirstSeq"},
		"first seq above 1<<63":               {func(c *storetest.Config) { c.FirstSeq = 1<<63 + 1 }, "FirstSeq"},
		"no racks":                            {func(c *storetest.Config) { c.Racks = 0 }, "Racks"},
		"no hosts":                            {func(c *storetest.Config) { c.Hosts = 0 }, "Hosts"},
		"no pods":                             {func(c *storetest.Config) { c.Pods = 0 }, "Pods"},
		"no services":                         {func(c *storetest.Config) { c.Services = 0 }, "Services"},
		"negative churn":                      {func(c *storetest.Config) { c.ChurnPerMinute = -1 }, "ChurnPerMinute"},
		"NaN churn":                           {func(c *storetest.Config) { c.ChurnPerMinute = math.NaN() }, "ChurnPerMinute"},
		"infinite churn":                      {func(c *storetest.Config) { c.ChurnPerMinute = math.Inf(1) }, "ChurnPerMinute"},
		"negative infinite churn":             {func(c *storetest.Config) { c.ChurnPerMinute = math.Inf(-1) }, "ChurnPerMinute"},
		"interval of one second":              {func(c *storetest.Config) { c.HeartbeatInterval = time.Second }, "HeartbeatInterval"},
		"interval off a second":               {func(c *storetest.Config) { c.HeartbeatInterval = 2500 * time.Millisecond }, "HeartbeatInterval"},
		"negative interval":                   {func(c *storetest.Config) { c.HeartbeatInterval = -time.Minute }, "HeartbeatInterval"},
		"no TTL factor":                       {func(c *storetest.Config) { c.TTLFactor = 0 }, "TTLFactor"},
		"negative TTL factor":                 {func(c *storetest.Config) { c.TTLFactor = -1 }, "TTLFactor"},
		"negative TTL factor in watch mode":   {func(c *storetest.Config) { c.HeartbeatInterval, c.TTLFactor = 0, -1 }, "TTLFactor"},
		"TTL that overflows":                  {func(c *storetest.Config) { c.TTLFactor = math.MaxInt }, "TTLFactor"},
		"confirm probability below 0":         {func(c *storetest.Config) { c.ConfirmProbability = -0.1 }, "ConfirmProbability"},
		"confirm probability above 1":         {func(c *storetest.Config) { c.ConfirmProbability = 1.1 }, "ConfirmProbability"},
		"confirm probability NaN":             {func(c *storetest.Config) { c.ConfirmProbability = math.NaN() }, "ConfirmProbability"},
		"confirm TTL zero with confirmations": {func(c *storetest.Config) { c.ConfirmTTL = 0 }, "ConfirmTTL"},
		"confirm TTL off a second":            {func(c *storetest.Config) { c.ConfirmTTL = 1500 * time.Millisecond }, "ConfirmTTL"},
		"confirm TTL negative":                {func(c *storetest.Config) { c.ConfirmTTL = -time.Minute }, "ConfirmTTL"},
		"late probability below 0":            {func(c *storetest.Config) { c.LateProbability = -0.1 }, "LateProbability"},
		"late probability above 1":            {func(c *storetest.Config) { c.LateProbability = 1.1 }, "LateProbability"},
		"late probability NaN":                {func(c *storetest.Config) { c.LateProbability = math.NaN() }, "LateProbability"},
		"late max zero with lateness":         {func(c *storetest.Config) { c.LateMax = 0 }, "LateMax"},
		"late max off a second":               {func(c *storetest.Config) { c.LateMax = 1500 * time.Millisecond }, "LateMax"},
		"late max negative":                   {func(c *storetest.Config) { c.LateMax = -time.Minute }, "LateMax"},
		"reboots in watch mode":               {func(c *storetest.Config) { c.HeartbeatInterval, c.TTLFactor, c.RebootProbability = 0, 0, 0.1 }, "RebootProbability"},
		"reboot probability above 1":          {func(c *storetest.Config) { c.RebootProbability = 1.5 }, "RebootProbability"},
		"reboot probability below 0":          {func(c *storetest.Config) { c.RebootProbability = -0.5 }, "RebootProbability"},
		"reboot probability NaN":              {func(c *storetest.Config) { c.RebootProbability = math.NaN() }, "RebootProbability"},
		"clones without reboots":              {func(c *storetest.Config) { c.CloneProbability = 0.5 }, "CloneProbability"},
		"clone probability above 1":           {func(c *storetest.Config) { c.RebootProbability, c.CloneProbability = 0.5, 1.5 }, "CloneProbability"},
		"clone probability below 0":           {func(c *storetest.Config) { c.RebootProbability, c.CloneProbability = 0.5, -0.5 }, "CloneProbability"},
		"clone probability NaN":               {func(c *storetest.Config) { c.RebootProbability, c.CloneProbability = 0.5, math.NaN() }, "CloneProbability"},
		"payload minimum zero":                {func(c *storetest.Config) { c.PayloadMin = 0 }, "Payload"},
		"payload maximum below minimum":       {func(c *storetest.Config) { c.PayloadMin, c.PayloadMax = 10, 9 }, "Payload"},
		"payload maximum 257":                 {func(c *storetest.Config) { c.PayloadMax = 257 }, "Payload"},
		"period ends after the latest time":   {func(c *storetest.Config) { c.Start = store.MaxEventTime.Truncate(time.Second).Add(-10 * time.Minute) }, "latest event time"},
		"lateness and the hour pass the latest": {func(c *storetest.Config) {
			c.Start = store.MaxEventTime.Truncate(time.Hour).Add(-c.Duration - c.LateMax)
		}, "latest event time"},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			c := storetest.Tiny()
			tt.mod(&c)
			err := c.Validate()
			_, newErr := storetest.NewGenerator(c)
			if tt.field == "" {
				if err != nil || newErr != nil {
					t.Errorf("Validate = %v, NewGenerator = %v; want a valid config", err, newErr)
				}
				return
			}
			if err == nil || newErr == nil {
				t.Fatalf("Validate = %v, NewGenerator = %v; want both to refuse the config", err, newErr)
			}
			if !strings.HasPrefix(err.Error(), "storetest: ") || !strings.Contains(err.Error(), tt.field) {
				t.Errorf("Validate error %q should start with %q and name %q", err, "storetest: ", tt.field)
			}
		})
	}
}

// genConfig draws a valid config of a small cluster, reaching every feature.
func genConfig() *rapid.Generator[storetest.Config] {
	probability := rapid.SampledFrom([]float64{0, 0.5, 1})
	return rapid.Custom(func(t *rapid.T) storetest.Config {
		c := storetest.Tiny()
		c.Seed = rapid.Uint64().Draw(t, "seed")
		c.Duration = time.Duration(rapid.IntRange(10, 40).Draw(t, "minutes")) * time.Minute
		c.FirstSeq = rapid.SampledFrom([]uint64{1, 1<<32 - 300, 1<<63 - 300}).Draw(t, "firstSeq")
		c.Racks = rapid.IntRange(1, 4).Draw(t, "racks")
		c.Hosts = rapid.IntRange(1, 8).Draw(t, "hosts")
		c.Pods = rapid.IntRange(1, 30).Draw(t, "pods")
		c.Services = rapid.IntRange(1, 4).Draw(t, "services")
		c.ChurnPerMinute = rapid.SampledFrom([]float64{0, 3, 20}).Draw(t, "churn")
		c.HeartbeatInterval = rapid.SampledFrom([]time.Duration{0, 30 * time.Second, time.Minute}).Draw(t, "interval")
		c.TTLFactor = rapid.IntRange(1, 4).Draw(t, "ttlFactor")
		c.ConfirmProbability = probability.Draw(t, "confirm")
		c.ConfirmTTL = time.Duration(rapid.IntRange(1, 600).Draw(t, "confirmTTL")) * time.Second
		c.LateProbability = probability.Draw(t, "late")
		c.LateMax = time.Duration(rapid.IntRange(1, 300).Draw(t, "lateMax")) * time.Second
		c.Runs = rapid.Bool().Draw(t, "runs")
		if c.HeartbeatInterval > 0 {
			c.RebootProbability = probability.Draw(t, "reboot")
		} else {
			c.RebootProbability = 0
		}
		if c.RebootProbability > 0 {
			c.CloneProbability = probability.Draw(t, "clone")
		} else {
			c.CloneProbability = 0
		}
		c.PayloadMin = rapid.IntRange(1, 8).Draw(t, "payloadMin")
		c.PayloadMax = rapid.IntRange(c.PayloadMin, 40).Draw(t, "payloadMax")
		if err := c.Validate(); err != nil {
			t.Fatalf("drew an invalid config: %v", err)
		}
		return c
	})
}

// sameRecord compares two records field by field, with times by Equal and
// payloads by bytes.Equal.
func sameRecord(a, b store.Record) bool {
	return a.Layer == b.Layer && a.Subject == b.Subject && a.Producer == b.Producer &&
		a.EventTime.Equal(b.EventTime) && a.Seq == b.Seq && a.Kind == b.Kind && a.TTL == b.TTL &&
		a.Through.Equal(b.Through) && bytes.Equal(a.Payload, b.Payload) && a.Boot == b.Boot
}

func sameRecords(a, b []store.Record) bool {
	return slices.EqualFunc(a, b, sameRecord)
}

func TestTheSameConfigYieldsTheSameStream(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		first, second := allRecords(t, c), allRecords(t, c)
		if !sameRecords(first, second) {
			t.Fatalf("two generators for %+v produced different streams (%d and %d records)", c, len(first), len(second))
		}
		// Next, Batch and All draw the same stream, however it is cut.
		g := mustGenerator(t, c)
		var batched []store.Record
		for n := 1; ; n = n%7 + 1 {
			batch := g.Batch(n)
			batched = append(batched, batch...)
			if len(batch) < n {
				break
			}
		}
		if !sameRecords(first, batched) {
			t.Fatalf("batches of 1 to 7 records produced a different stream than All for %+v", c)
		}
		if r, ok := g.Next(); ok {
			t.Fatalf("Next after the end returned %+v", r)
		}
		if got := g.Batch(3); len(got) != 0 {
			t.Fatalf("Batch after the end returned %d records", len(got))
		}
	})
}

func TestEveryRecordIsValid(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		g := mustGenerator(t, c)
		recs := g.All()
		if len(recs) == 0 {
			t.Fatalf("no records for %+v", c)
		}
		for i, r := range recs {
			if err := r.Validate(); err != nil {
				t.Fatalf("record %d %+v: %v", i, r, err)
			}
			if want := c.FirstSeq + uint64(i); r.Seq != want {
				t.Fatalf("record %d has seq %d, want %d", i, r.Seq, want)
			}
			for name, at := range map[string]time.Time{"event time": r.EventTime, "through": r.Through} {
				if !at.IsZero() && at.Nanosecond() != 0 {
					t.Fatalf("record %d: %s %s is not on a whole second", i, name, at.Format(time.RFC3339Nano))
				}
			}
			if r.EventTime.Before(g.Start()) {
				t.Fatalf("record %d at %s is before Start", i, r.EventTime.Format(time.RFC3339))
			}
			if (r.Kind == lifecycle.Observe) != (len(r.Payload) > 0) {
				t.Fatalf("record %d: kind %s with a payload of %d bytes", i, r.Kind, len(r.Payload))
			}
		}
	})
}

func TestSubjectsKeepTheirLayerAndRelationsAreAllowed(t *testing.T) {
	t.Parallel()
	edgeLayers := map[catalog.RelationType]catalog.Layer{
		catalog.LocatedIn: catalog.L1, catalog.RunsOn: catalog.L1, catalog.DependsOn: catalog.L3,
		catalog.PartOf: catalog.L2, catalog.ScheduledOn: catalog.L2,
	}
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		layers := map[store.Subject]catalog.Layer{}
		for i, r := range allRecords(t, c) {
			if l, ok := layers[r.Subject]; ok && l != r.Layer {
				t.Fatalf("record %d: subject %v is in layer %s and then %s", i, r.Subject, l, r.Layer)
			}
			layers[r.Subject] = r.Layer
			switch r.Subject.Kind {
			case store.SubjectEntity:
				e, ok := catalog.Default().Entity(r.Subject.A.Type())
				if !ok || e.Layer() != r.Layer {
					t.Fatalf("record %d: entity %s in layer %s, the catalog says %v", i, r.Subject.A, r.Layer, e.Layer())
				}
			case store.SubjectEdge:
				rel, ok := catalog.Default().Relation(r.Subject.Relation)
				if !ok || rel.Derived() || !rel.Allows(r.Subject.A.Type(), r.Subject.B.Type()) {
					t.Fatalf("record %d: relation %s from %s to %s is not an allowed stored relation",
						i, r.Subject.Relation, r.Subject.A.Type(), r.Subject.B.Type())
				}
				if want, ok := edgeLayers[r.Subject.Relation]; !ok || want != r.Layer {
					t.Fatalf("record %d: relation %s in layer %s, want %s", i, r.Subject.Relation, r.Layer, want)
				}
			default:
				t.Fatalf("record %d: subject kind %d", i, r.Subject.Kind)
			}
		}
	})
}

func TestEntitiesListEveryFingerprintOnce(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		g := mustGenerator(t, c)
		listed := g.Entities()
		seen := map[identity.Fingerprint]bool{}
		for _, fp := range listed {
			if fp.IsZero() {
				t.Fatal("the zero fingerprint is listed")
			}
			if seen[fp] {
				t.Fatalf("%s is listed twice", fp)
			}
			seen[fp] = true
		}
		inRecords := map[identity.Fingerprint]bool{}
		for _, r := range g.All() {
			inRecords[r.Subject.A] = true
			if !r.Subject.B.IsZero() {
				inRecords[r.Subject.B] = true
			}
		}
		for fp := range inRecords {
			if !seen[fp] {
				t.Fatalf("%s is in a record but not listed", fp)
			}
		}
		for fp := range seen {
			if !inRecords[fp] {
				t.Fatalf("%s is listed but in no record", fp)
			}
		}
		if want := c.Racks + 2*c.Hosts + c.Services + 2*c.Pods; len(listed) != want {
			t.Fatalf("%d entities listed, want %d", len(listed), want)
		}
		// The order is fixed by the config, and groups racks, hosts, nodes, services,
		// pods and containers, in that order.
		if again := mustGenerator(t, c).Entities(); !slices.Equal(listed, again) {
			t.Fatal("two generators list the entities in a different order")
		}
		var types []catalog.EntityType
		for _, fp := range listed {
			if len(types) == 0 || types[len(types)-1] != fp.Type() {
				types = append(types, fp.Type())
			}
		}
		want := []catalog.EntityType{catalog.Rack, catalog.Host, catalog.K8sNode, catalog.Service, catalog.K8sPod, catalog.Container}
		if !slices.Equal(types, want) {
			t.Fatalf("entity types in order: %v, want %v", types, want)
		}
	})
}

func TestEntitiesReturnsAFreshSlice(t *testing.T) {
	t.Parallel()
	g := mustGenerator(t, storetest.Tiny())
	first := g.Entities()
	want := slices.Clone(first)
	for i := range first {
		first[i] = identity.Fingerprint{}
	}
	if got := g.Entities(); !slices.Equal(got, want) {
		t.Errorf("changing the slice Entities returned changed the next call's answer")
	}
	if len(want) == 0 {
		t.Error("no entities")
	}
}

func TestPayloads(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		type key struct {
			subject  store.Subject
			producer lifecycle.Producer
		}
		fixed := map[key][]byte{} // heartbeat payloads, fixed per subject (a host's too)
		for i, r := range allRecords(t, c) {
			if r.Kind != lifecycle.Observe {
				if len(r.Payload) != 0 {
					t.Fatalf("record %d: a delete with a payload", i)
				}
				continue
			}
			isHost := r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Host
			// Only a host observation carries a boot ID, and every one does.
			if isHost != (r.Boot != "") || (isHost && !strings.HasPrefix(r.Boot, "boot-host-")) {
				t.Fatalf("record %d: %v of %s with the boot %q", i, r.Subject.Kind, r.Subject.A.Type(), r.Boot)
			}
			if n := len(r.Payload); n < c.PayloadMin || n > c.PayloadMax {
				t.Fatalf("record %d: payload of %d bytes, want %d to %d", i, n, c.PayloadMin, c.PayloadMax)
			}
			if r.Producer == storetest.ProducerNodeCollector || r.Producer == storetest.ProducerTraces {
				k := key{r.Subject, r.Producer}
				if prev, ok := fixed[k]; ok && !bytes.Equal(prev, r.Payload) {
					t.Fatalf("record %d: heartbeat payload of %v changed", i, r.Subject)
				}
				fixed[k] = r.Payload
			}
		}
	})
}

func TestBootIDsOnlyGoUpAndClonesReportAnOldOne(t *testing.T) {
	t.Parallel()
	c := everything()
	c.Runs = false
	recs := allRecords(t, c)
	boots := map[identity.Fingerprint][]string{} // each host's boot IDs from node-collector, in order
	clones := 0
	for _, r := range recs {
		if r.Kind != lifecycle.Observe || r.Subject.Kind != store.SubjectEntity || r.Subject.A.Type() != catalog.Host {
			continue
		}
		switch r.Producer {
		case storetest.ProducerNodeCollector:
			h := boots[r.Subject.A]
			if id := r.Boot; len(h) == 0 || h[len(h)-1] != id {
				boots[r.Subject.A] = append(h, id)
			}
		case storetest.ProducerClone:
			clones++
			h := boots[r.Subject.A]
			if len(h) < 2 || r.Boot != h[len(h)-2] {
				t.Errorf("clone at %s reports %q, want the boot before the current one (%v)", r.EventTime.Format(time.RFC3339), r.Boot, h)
			}
			if r.TTL != c.HeartbeatInterval {
				t.Errorf("clone TTL = %s, want %s", r.TTL, c.HeartbeatInterval)
			}
		}
	}
	if clones == 0 {
		t.Error("no clone records with CloneProbability 0.5")
	}
	for host, ids := range boots {
		seen := map[string]bool{}
		for _, id := range ids {
			if seen[id] {
				t.Errorf("host %s reports boot %q again after another", host, id)
			}
			seen[id] = true
		}
	}
}

// fold folds all of a subject's records.
func fold(recs []store.Record, p lifecycle.Policy) (lifecycle.Timeline, error) {
	as := make([]lifecycle.Assertion, len(recs))
	for i, r := range recs {
		as[i] = r.Assertion()
	}
	return lifecycle.Fold(as, p)
}

func bySubject(recs []store.Record) map[store.Subject][]store.Record {
	out := map[store.Subject][]store.Record{}
	for _, r := range recs {
		out[r.Subject] = append(out[r.Subject], r)
	}
	return out
}

func sameIntervals(a, b []lifecycle.Interval) bool {
	return slices.EqualFunc(a, b, func(x, y lifecycle.Interval) bool {
		return x.Start.Equal(y.Start) && x.End.Equal(y.End) && x.EndSource == y.EndSource
	})
}

func sameBoots(a, b []lifecycle.Boot) bool {
	return slices.EqualFunc(a, b, func(x, y lifecycle.Boot) bool {
		return x.ID == y.ID && x.FirstSeen.Equal(y.FirstSeen) && x.LastSeen.Equal(y.LastSeen)
	})
}

// checkRunsEquivalence compares the stream of c with runs and without, subject by
// subject: the same existence, and under the policy that reads an entity's
// payload as its boot the same boots and the same clone collision. mid, if it is
// not nil, is called after half of the stream with the generator, to change it.
func checkRunsEquivalence(t tb, c storetest.Config, mid func(*storetest.Generator, store.Record)) {
	t.Helper()
	c.Runs = false
	plain := allRecords(t, c)
	c.Runs = true
	g := mustGenerator(t, c)
	var runs []store.Record
	for i := 0; ; i++ {
		r, ok := g.Next()
		if !ok {
			break
		}
		runs = append(runs, r)
		if mid != nil && i == len(plain)/2 {
			mid(g, r)
		}
	}
	if len(runs) != len(plain) {
		t.Fatalf("%d records with runs, %d without: a run changes how refreshes are recorded, not how many arrive", len(runs), len(plain))
	}
	extensions := 0
	for _, r := range runs {
		if !r.Through.IsZero() {
			extensions++
		}
	}
	if c.HeartbeatInterval > 0 && extensions == 0 {
		t.Fatalf("no run extensions in %d records with heartbeats", len(runs))
	}

	withRuns, without := bySubject(runs), bySubject(plain)
	if len(withRuns) != len(without) {
		t.Fatalf("%d subjects with runs, %d without", len(withRuns), len(without))
	}
	for sub, rs := range without {
		for name, p := range map[string]lifecycle.Policy{"zero policy": {}, "boot policy": bootPolicy} {
			if p.BootKey != "" && sub.Kind != store.SubjectEntity {
				continue
			}
			a, errA := fold(rs, p)
			b, errB := fold(withRuns[sub], p)
			var ccA, ccB *lifecycle.CloneCollisionError
			isA, isB := errors.As(errA, &ccA), errors.As(errB, &ccB)
			if isA != isB || (errA == nil) != (errB == nil) {
				t.Fatalf("%v under the %s: without runs err = %v, with runs err = %v", sub, name, errA, errB)
			}
			if isA {
				if ccA.StaleBoot != ccB.StaleBoot || ccA.NewerBoot != ccB.NewerBoot ||
					!ccA.ObservedAt.Equal(ccB.ObservedAt) || !ccA.NewerFirstSeen.Equal(ccB.NewerFirstSeen) {
					t.Fatalf("%v: collision without runs %v, with runs %v", sub, ccA, ccB)
				}
				continue
			}
			if errA != nil {
				t.Fatalf("%v under the %s: %v", sub, name, errA)
			}
			if !sameIntervals(a.Existence(), b.Existence()) {
				t.Fatalf("%v under the %s: existence without runs %v, with runs %v", sub, name, a.Existence(), b.Existence())
			}
			if !sameBoots(a.Boots(), b.Boots()) {
				t.Fatalf("%v under the %s: boots without runs %v, with runs %v", sub, name, a.Boots(), b.Boots())
			}
		}
	}
}

func TestRunsKeepEveryAnswer(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		checkRunsEquivalence(t, c, nil)
	})
}

func TestRunsKeepEveryAnswerWithEveryFeature(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 3; seed++ {
		c := everything()
		c.Seed = seed
		checkRunsEquivalence(t, c, nil)
	}
}

func TestRunsKeepEveryAnswerAcrossAHorizon(t *testing.T) {
	t.Parallel()
	c := everything()
	c.LateProbability = 0
	checkRunsEquivalence(t, c, func(g *storetest.Generator, r store.Record) { g.SetHorizon(r.EventTime) })
}

func TestNothingIsRecordedBeforeTheHorizonOnceItIsKnown(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		c.LateProbability = 0
		g := mustGenerator(t, c)
		n := len(allRecords(t, c))
		k := rapid.IntRange(0, n-1).Draw(t, "records before the horizon")
		back := time.Duration(rapid.IntRange(0, 120).Draw(t, "seconds back")) * time.Second

		var h time.Time
		for i := 0; i <= k; i++ {
			r, _ := g.Next()
			// The horizon is at or before the event time of a record already
			// returned; the record may be an extension, whose event time is its run's.
			h = r.EventTime.Add(-back)
		}
		g.SetHorizon(h)
		g.SetHorizon(h.Add(-time.Hour)) // it only moves forward
		for i, r := range g.All() {
			if r.EventTime.Before(h) {
				t.Fatalf("record %d after the horizon %s is at %s (through %s, runs %v)", k+1+i, h.Format(time.RFC3339), r.EventTime.Format(time.RFC3339), r.Through.Format(time.RFC3339), c.Runs)
			}
		}
	})
}

func TestTheHorizonStopsRunsFromBeingExtended(t *testing.T) {
	t.Parallel()
	c := storetest.Tiny()
	c.LateProbability = 0
	c.Runs = true
	g := mustGenerator(t, c)
	horizon := g.Start().Add(10 * time.Minute)
	var past, after []store.Record
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		if r.EventTime.Before(horizon) && !r.Through.IsZero() && r.Through.After(horizon) {
			t.Fatalf("a run starting before the horizon was extended past it before the horizon was set: %+v", r)
		}
		if r.Through.IsZero() && !r.EventTime.Before(horizon) {
			g.SetHorizon(horizon)
			after = append(after, g.All()...)
			break
		}
		past = append(past, r)
	}
	if len(past) == 0 || len(after) == 0 {
		t.Fatalf("%d records before the horizon and %d after it", len(past), len(after))
	}
	extended := 0
	for _, r := range after {
		if r.EventTime.Before(horizon) {
			t.Fatalf("a record at %s after the horizon %s: %+v", r.EventTime.Format(time.RFC3339), horizon.Format(time.RFC3339), r)
		}
		if !r.Through.IsZero() {
			extended++
		}
	}
	if extended == 0 {
		t.Error("no run was extended after the horizon: new runs should be extended again")
	}
}

func TestLatenessIsBounded(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		var latest time.Time
		for i, r := range allRecords(t, c) {
			if c.Runs && !r.Through.IsZero() {
				continue // an extension is dated at the start of its run
			}
			if !latest.IsZero() && r.EventTime.Before(latest.Add(-c.LateMax)) {
				t.Fatalf("record %d at %s is more than %s before the latest event time so far, %s", i, r.EventTime.Format(time.RFC3339), c.LateMax, latest.Format(time.RFC3339))
			}
			if c.LateProbability == 0 && !c.Runs && r.EventTime.Before(latest) {
				t.Fatalf("record %d at %s goes back in time from %s with no lateness", i, r.EventTime.Format(time.RFC3339), latest.Format(time.RFC3339))
			}
			if r.EventTime.After(latest) {
				latest = r.EventTime
			}
		}
	})
}

func TestNoLatenessMeansEventTimeNeverGoesBackEvenWithRuns(t *testing.T) {
	t.Parallel()
	c := everything()
	c.LateProbability = 0
	var latest time.Time
	for i, r := range allRecords(t, c) {
		if !r.Through.IsZero() {
			continue
		}
		if r.EventTime.Before(latest) {
			t.Fatalf("record %d at %s is before %s", i, r.EventTime.Format(time.RFC3339), latest.Format(time.RFC3339))
		}
		latest = r.EventTime
	}
}

func TestStreamsNearTheEndOfRepresentableTime(t *testing.T) {
	t.Parallel()
	c := everything()
	// The latest start the config allows: the period, the lateness and the longest
	// TTL (an hour) end within a second of the latest event time.
	c.Start = store.MaxEventTime.Truncate(time.Second).Add(-c.Duration - c.LateMax - time.Hour)
	g := mustGenerator(t, c)
	for i, r := range g.All() {
		if err := r.Validate(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	c.Start = c.Start.Add(time.Second)
	if _, err := storetest.NewGenerator(c); err == nil {
		t.Error("a config one second too late was accepted")
	}
}

func TestStreamsNearTheEndOfRepresentableTimeWithLongIntervalsAndClones(t *testing.T) {
	t.Parallel()
	c := storetest.Tiny()
	c.ChurnPerMinute = 2
	c.Duration = 6 * time.Hour
	c.HeartbeatInterval, c.TTLFactor = 2*time.Hour, 1
	c.RebootProbability, c.CloneProbability = 1, 1
	c.Runs = true
	// The longest thing after the period is a clone's record: half an interval
	// after a beat, held for a whole interval, which is three hours here and more
	// than the heartbeat TTL (two hours) and the hour every config allows for.
	c.Start = store.MaxEventTime.Truncate(time.Second).Add(-c.Duration - c.LateMax - 3*time.Hour)
	g := mustGenerator(t, c)
	clones := 0
	for i, r := range g.All() {
		if err := r.Validate(); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if r.Producer == storetest.ProducerClone {
			clones++
		}
	}
	if clones == 0 {
		t.Error("no clone records")
	}
	c.Start = c.Start.Add(time.Second)
	if err := c.Validate(); err == nil {
		t.Error("a config whose clone records would end after the latest event time was accepted")
	}
}

// quarantinedHosts folds every entity of a stream with the policy that reads boot
// IDs, and returns the ones whose existence cannot be folded. It fails on any
// other error.
func quarantinedHosts(t tb, recs []store.Record) map[identity.Fingerprint]bool {
	t.Helper()
	out := map[identity.Fingerprint]bool{}
	for sub, rs := range bySubject(recs) {
		if sub.Kind != store.SubjectEntity {
			continue
		}
		_, err := fold(rs, bootPolicy)
		var cc *lifecycle.CloneCollisionError
		switch {
		case err == nil:
		case errors.As(err, &cc):
			if sub.A.Type() != catalog.Host || !strings.HasPrefix(cc.StaleBoot, "boot-host-") {
				t.Fatalf("%s %s is quarantined: %v", sub.A.Type(), sub.A, err)
			}
			out[sub.A] = true
		default:
			t.Fatalf("%s %s: %v", sub.A.Type(), sub.A, err)
		}
	}
	return out
}

func TestWithoutClonesNothingIsQuarantined(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		c.CloneProbability = 0
		if got := quarantinedHosts(t, allRecords(t, c)); len(got) != 0 {
			t.Fatalf("%d hosts are quarantined with CloneProbability 0 (reboot probability %v)", len(got), c.RebootProbability)
		}
	})
}

func TestEveryClonedHostIsQuarantinedAndNoOtherEntity(t *testing.T) {
	t.Parallel()
	rapid.Check(t, func(t *rapid.T) {
		c := genConfig().Draw(t, "config")
		recs := allRecords(t, c)
		cloned := map[identity.Fingerprint]bool{}
		for _, r := range recs {
			if r.Producer == storetest.ProducerClone {
				cloned[r.Subject.A] = true
			}
		}
		got := quarantinedHosts(t, recs)
		if len(got) != len(cloned) {
			t.Fatalf("%d hosts are quarantined, %d were cloned", len(got), len(cloned))
		}
		for host := range cloned {
			if !got[host] {
				t.Fatalf("%s was cloned and is not quarantined", host)
			}
		}
	})
}

func TestClonesQuarantineSomeHostsOfAWideCluster(t *testing.T) {
	t.Parallel()
	for seed := uint64(1); seed <= 3; seed++ {
		c := everything()
		c.Seed = seed
		c.Hosts = 12
		for name, runs := range map[string]bool{"with runs": true, "without runs": false} {
			c.Runs = runs
			if got := quarantinedHosts(t, allRecords(t, c)); len(got) == 0 {
				t.Errorf("seed %d %s: no host is quarantined with CloneProbability %v", seed, name, c.CloneProbability)
			}
		}
	}
}

func TestSetHorizonWithoutRunsChangesNothing(t *testing.T) {
	t.Parallel()
	c := everything()
	c.Runs = false
	plain := allRecords(t, c)
	g := mustGenerator(t, c)
	var got []store.Record
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		got = append(got, r)
		g.SetHorizon(r.EventTime)
	}
	if !sameRecords(plain, got) {
		t.Error("SetHorizon changed a stream without runs")
	}
}

func TestStartAndEnd(t *testing.T) {
	t.Parallel()
	c := storetest.Tiny()
	c.Start = c.Start.In(time.FixedZone("x", 7200))
	g := mustGenerator(t, c)
	if !g.Start().Equal(c.Start) || g.Start().Location() != time.UTC {
		t.Errorf("Start = %s, want %s in UTC", g.Start(), c.Start)
	}
	if want := c.Start.Add(c.Duration); !g.End().Equal(want) {
		t.Errorf("End = %s, want %s", g.End(), want)
	}
}

func TestPayloadsReturnedAreTheCallersToChange(t *testing.T) {
	t.Parallel()
	c := storetest.Tiny()
	want := allRecords(t, c)
	g := mustGenerator(t, c)
	var got []store.Record
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		got = append(got, r)
		for i := range r.Payload { // heartbeats repeat a payload: changing one must not change the next
			r.Payload[i] = 0xff
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d records, want %d", len(got), len(want))
	}
	for i := range got {
		if len(want[i].Payload) != len(got[i].Payload) {
			t.Fatalf("record %d: payload length changed", i)
		}
	}
	again := mustGenerator(t, c).All()
	if !sameRecords(want, again) {
		t.Error("mutating returned payloads changed another generator's stream")
	}
}

// goldenLine is the line a record contributes to a stream's digest.
func goldenLine(r store.Record) string {
	var through int64
	if !r.Through.IsZero() {
		through = r.Through.UnixNano()
	}
	return fmt.Sprintf("%d|%d|%s|%s|%s|%s|%d|%d|%d|%d|%d|%s|%q\n",
		r.Layer, r.Subject.Kind, r.Subject.A, r.Subject.B, r.Subject.Relation, r.Producer,
		r.EventTime.UnixNano(), r.Seq, r.Kind, int64(r.TTL), through, hex.EncodeToString(r.Payload), r.Boot)
}

func digest(recs []store.Record) string {
	h := sha256.New()
	for _, r := range recs {
		h.Write([]byte(goldenLine(r)))
	}
	return hex.EncodeToString(h.Sum(nil))
}

// The generator is deterministic by contract: one config, one stream, on every
// machine. These digests pin the stream of Tiny and of a config with every
// feature on. A change to either value is a change to every stream the
// conformance suite runs, and is made deliberately.
const (
	goldenTiny       = "a314d170de99100cb73b8be180545fa9d9790bf7b0ef5b002f7101ec708086ed"
	goldenEverything = "387432a870aa67994233da75864432e185b2ee2518aac8e93125cc6c44bd40a3"
)

func TestGoldenDigests(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		config storetest.Config
		want   string
	}{
		"Tiny":       {storetest.Tiny(), goldenTiny},
		"everything": {everything(), goldenEverything},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			recs := allRecords(t, tt.config)
			if got := digest(recs); got != tt.want {
				t.Errorf("digest of the %d records of %s = %s, want %s", len(recs), name, got, tt.want)
			}
		})
	}
}

func TestTinyIsAUsefulSize(t *testing.T) {
	t.Parallel()
	n := len(allRecords(t, storetest.Tiny()))
	t.Logf("Tiny yields %d records", n)
	if n < 1000 || n > 4000 {
		t.Errorf("Tiny yields %d records, want 1000 to 4000", n)
	}
}

func TestDifferentSeedsDiffer(t *testing.T) {
	t.Parallel()
	a, b := storetest.Tiny(), storetest.Tiny()
	b.Seed = 2
	if digest(allRecords(t, a)) == digest(allRecords(t, b)) {
		t.Error("two seeds gave the same stream")
	}
}
