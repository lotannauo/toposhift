package sequencer_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/query/decay"
)

// TestIngestCoalescesUnderTheSeedWalkBudgetsConfig pins the coalescer
// configuration of ingest. pebblestore's seed-walk budget (SW1, in its seeded.go)
// holds only for a store written under it.
func TestIngestCoalescesUnderTheSeedWalkBudgetsConfig(t *testing.T) {
	want := coalesce.Config{ExtendTTLFraction: 0.5, RunAgeFromTTL: true}
	got := sequencer.CoalesceConfig()
	if got != want {
		t.Errorf("sequencer.CoalesceConfig() = %+v, want %+v: pebblestore's seed-walk budget (SW1, its seeded.go) holds only under this configuration, a run's age bounded by its own extension interval; change the budget by a new dated paragraph there before changing this", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("sequencer.CoalesceConfig() is not a valid coalescer configuration: %v", err)
	}

	s, err := sequencer.New(sequencer.Config{Store: newMem(t)})
	if err != nil {
		t.Fatal(err)
	}
	if used := sequencer.CoalescerConfigOf(s); used != sequencer.CoalesceConfig() {
		t.Errorf("the sequencer coalesces under %+v, not under sequencer.CoalesceConfig() = %+v: pebblestore's seed-walk budget (SW1, its seeded.go) holds only under the latter", used, sequencer.CoalesceConfig())
	}
}

// TestDecayPolicyPinsTheIngestCoalescer pins the premise of the bound package
// decay derives: a run's record falls short of its producer's last sighting by
// less than the extension interval, which holds because ingest re-asserts a run
// at most once per ExtendTTLFraction of its TTL and bounds each run's age by that
// same interval (RunAgeFromTTL), with no other age bound and no fixed interval. A
// change to the coalescer's configuration is a change to what the bound means, and
// raises decay.PolicyVersion with a policy of its own.
func TestDecayPolicyPinsTheIngestCoalescer(t *testing.T) {
	p := decay.DefaultPolicy()
	got := sequencer.CoalesceConfig()
	if got.ExtendTTLFraction != p.ExtendFraction || !got.RunAgeFromTTL || got.ExtendEvery != 0 || got.RunMaxAge != 0 {
		t.Errorf("sequencer.CoalesceConfig() = %+v, but decay policy version %d derives its bound from an extension fraction of %g with each run's age bounded by its own extension interval and by nothing else: raise decay.PolicyVersion with a policy that matches before changing this",
			got, p.Version, p.ExtendFraction)
	}
	for _, ttl := range []time.Duration{time.Nanosecond, time.Second, time.Minute, 15 * time.Minute, 6 * time.Hour, 24 * time.Hour} {
		if x, want := p.Extension(ttl), got.ExtensionInterval(ttl); x != want {
			t.Errorf("decay's extension interval for a TTL of %s is %s, the coalescer's %s", ttl, x, want)
		}
	}
}
