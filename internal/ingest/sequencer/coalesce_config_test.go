package sequencer_test

import (
	"testing"

	"github.com/lotannauo/toposhift/internal/coalesce"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
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
