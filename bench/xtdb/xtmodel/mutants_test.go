package xtmodel

import (
	"errors"
	"fmt"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/engine"
)

// mutantSeeds is how many fixed streams a mutant is offered.
const mutantSeeds = 30

// streams offers a model the fixed streams, with and without retentions, and returns
// the first difference from the reference.
func streams(m func() *Model) error {
	for seed := uint64(1); seed <= seeds(mutantSeeds); seed++ {
		if err := run(m(), genStream(seedDrawer(seed), 25, seed%3 == 0)); err != nil {
			return fmt.Errorf("seed %d: %w", seed, err)
		}
	}
	return nil
}

// retentionCheck: after Retain a write before the horizon is refused whole.
func retentionCheck(m *Model) error {
	if err := m.Write([]engine.Record{observe(1, "a", 10, 0)}); err != nil {
		return err
	}
	if err := m.Retain(at(20)); err != nil {
		return err
	}
	err := m.Write([]engine.Record{observe(2, "a", 30, 0), observe(3, "a", 19, 0)})
	if !errors.Is(err, engine.ErrBeforeHorizon) {
		return fmt.Errorf("a write before the horizon was not refused as such: %w", orNil(err))
	}
	if m.LastSeq() != 1 || m.Counts().Batches != 1 {
		return fmt.Errorf("a refused batch changed the model: last seq %d, %d batches", m.LastSeq(), m.Counts().Batches)
	}
	return m.Write([]engine.Record{observe(2, "a", 30, 0)})
}

// systemTimeCheck: the system time of the n-th committed batch is the base plus n
// microseconds, a batch the server refuses takes no ordinal and no sequence number,
// and times strictly increase.
func systemTimeCheck(m *Model) error {
	if err := m.Write([]engine.Record{observe(1, "a", 10, 0), observe(2, "a", 11, 0)}); err != nil {
		return err
	}
	m.beforeCommit = func(*Txn) error { return errors.New("the server refused the transaction") }
	if err := m.Write([]engine.Record{observe(3, "a", 12, 0), observe(4, "b", 13, 0)}); err == nil {
		return errors.New("a refused transaction was reported as written")
	}
	m.beforeCommit = nil
	if m.LastSeq() != 2 || m.Counts().Batches != 1 || m.Counts().Records != 2 {
		return fmt.Errorf("a refused batch left last seq %d, %d batches, %d records", m.LastSeq(), m.Counts().Batches, m.Counts().Records)
	}
	for _, b := range [][]engine.Record{
		{observe(3, "a", 12, 0), observe(4, "b", 13, 0)},
		{observe(5, "a", 14, 0)},
		{observe(8, "c", 15, 0)},
	} {
		if err := m.Write(b); err != nil {
			return err
		}
	}
	if m.LastSeq() != 8 {
		return fmt.Errorf("last seq %d", m.LastSeq())
	}
	prev := Instant(0)
	for i, d := range m.img.metas {
		n := uint64(i) + 1
		if d.n != n || d.sys != SystemTime(n) || d.sys != BaseSystemTime+Instant(n)*1000 || d.sys <= prev {
			return fmt.Errorf("batch %d: ordinal %d at system time %d (previous %d)", n, d.n, d.sys, prev)
		}
		prev = d.sys
	}
	return nil
}

// TestEveryFaultIsCaught: each check passes on a correct model and fails on a model
// with the fault it exists for.
func TestEveryFaultIsCaught(t *testing.T) {
	streamCheck := func(m func() *Model) error { return streams(m) }
	modelCheck := func(c func(*Model) error) func(func() *Model) error {
		return func(m func() *Model) error { return c(m()) }
	}
	for _, c := range []struct {
		fault string
		mut   mutation
		check func(func() *Model) error
	}{
		{"a late record's next assertion taken as the end of time", mutLateNextIsEnd, streamCheck},
		{"the leftover between a shrinking deadline and the next assertion is kept", mutKeepLeftover, streamCheck},
		{"a late delete reaches the end of time", mutDeleteToEnd, streamCheck},
		{"a tie at one instant won by the lower Seq", mutLowerSeqWins, streamCheck},
		{"a batch's statements merged out of Seq order", mutMergeOutOfSeqOrder, streamCheck},
		{"FOR SYSTEM_TIME omitted at old tokens", mutNoSystemTime, streamCheck},
		{"a token inside a batch answered by system time", mutMidBatchBySystemTime, streamCheck},
		{"the layer filter dropped", mutNoLayerFilter, streamCheck},
		{"Window deduplicating overwritten records", mutWindowDedup, streamCheck},
		{"a batched read dropping an empty list", mutBatchDropsEmpty, streamCheck},
		{"a write before the horizon accepted after Retain", mutAcceptBeforeHorizon, modelCheck(retentionCheck)},
		{"a refused batch taking an ordinal", mutFailedBatchTakesOrdinal, modelCheck(systemTimeCheck)},
		{"the next batch at the system time of the one before", mutRepeatSystemTime, modelCheck(systemTimeCheck)},
		{"LastSeq raised before the transaction commits", mutLastSeqEarly, modelCheck(systemTimeCheck)},
	} {
		t.Run(c.fault, func(t *testing.T) {
			t.Parallel()
			if err := c.check(New); err != nil {
				t.Fatalf("a correct model fails: %v", err)
			}
			mutant := func() *Model {
				m := New()
				m.setMutation(c.mut)
				return m
			}
			err := c.check(mutant)
			if err == nil {
				t.Fatal("the fault went unnoticed")
			}
			t.Logf("caught: %.200v", err)
		})
	}
}

func orNil(err error) error {
	if err == nil {
		return errors.New("no error")
	}
	return err
}
