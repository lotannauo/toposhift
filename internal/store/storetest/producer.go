package storetest

import (
	"fmt"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// producerFormatBytes are the bytes a producer name may hold: the ASCII letters and
// digits and the five marks . _ : / -.
const producerFormatBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:/-"

// CheckProducerNames checks the producer names a store writes. A name of exactly
// [store.MaxProducerLen] bytes is accepted, and so is a record whose producer is any
// one byte of the format. A name of one byte more, a name holding a space, an @, a
// control byte or a rune outside ASCII, and the empty name are refused, for an
// observation, for a delete and for an edge record alike. A batch with one such
// record is refused whole, with store.ErrInvalid: LastSeq does not move and the
// valid records before it are not readable. The refusal comes at the record's turn,
// in the order the store checks a batch: a stale record ahead of an over-long
// producer is refused for the retention horizon, and an over-long producer ahead of
// a stale record is refused as invalid. It leaves the store with a retention
// horizon, so use a fresh store opened with the zero policy.
func CheckProducerNames(s store.Store) error {
	pod, err := entityFP(catalog.K8sPod, catalog.K8sPodUID, "producer-names-pod")
	if err != nil {
		return err
	}
	node, err := entityFP(catalog.K8sNode, catalog.K8sNodeUID, "producer-names-node")
	if err != nil {
		return err
	}
	x := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	seq := uint64(seqBase)
	entity := func(p lifecycle.Producer, at time.Time) store.Record {
		seq++
		return store.Record{
			Layer: entityLayer(catalog.K8sPod), Subject: store.EntitySubject(pod), Producer: p,
			EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte("entity"),
		}
	}
	edge := func(p lifecycle.Producer, at time.Time) store.Record {
		seq++
		return store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: p,
			EventTime: at, Seq: seq, Kind: lifecycle.Observe, Payload: []byte("edge"),
		}
	}
	del := func(p lifecycle.Producer, at time.Time) store.Record {
		seq++
		return store.Record{
			Layer: entityLayer(catalog.K8sPod), Subject: store.EntitySubject(pod), Producer: p,
			EventTime: at, Seq: seq, Kind: lifecycle.Delete,
		}
	}
	long := lifecycle.Producer(strings.Repeat("l", store.MaxProducerLen+1))

	// refused writes a batch that must be refused as invalid, and checks that
	// nothing of it was stored. Its sequence numbers are given back, so the next
	// batch can use them.
	refused := func(what string, batch ...store.Record) error {
		before := s.LastSeq()
		first := batch[0].Seq
		err := s.Write(bg, cloneRecords(batch))
		seq = first - 1
		if err := wantInvalid(what, err); err != nil {
			return err
		}
		if got := s.LastSeq(); got != before {
			return fmt.Errorf("%s: refused, but LastSeq moved from %d to %d", what, before, got)
		}
		for _, r := range batch {
			held, err := holds(s, r)
			if err != nil {
				return fmt.Errorf("%s: reading back: %w", what, err)
			}
			if held {
				return fmt.Errorf("%s: refused, but the record of seq %d is readable", what, r.Seq)
			}
		}
		return nil
	}

	// A name of exactly the limit is accepted.
	atLimit := lifecycle.Producer(strings.Repeat("m", store.MaxProducerLen))
	rec := entity(atLimit, x)
	if err := s.Write(bg, cloneRecords([]store.Record{rec})); err != nil {
		return fmt.Errorf("a producer of %d bytes, the limit, was refused: %w", store.MaxProducerLen, err)
	}
	held, err := holds(s, rec)
	if err != nil {
		return fmt.Errorf("a producer of %d bytes was accepted but reading its record failed: %w", store.MaxProducerLen, err)
	}
	if !held {
		return fmt.Errorf("a producer of %d bytes was accepted but its record is not readable", store.MaxProducerLen)
	}

	// Every byte of the format is accepted, each as a producer of its own.
	for i := 0; i < len(producerFormatBytes); i++ {
		p := lifecycle.Producer(producerFormatBytes[i : i+1])
		if err := s.Write(bg, []store.Record{entity(p, x.Add(time.Second))}); err != nil {
			return fmt.Errorf("the producer %q was refused: %w", string(p), err)
		}
	}
	good := s.LastSeq()

	// A batch whose second record has one byte more than the limit is refused whole.
	first := edge("fabric", x.Add(2*time.Second))
	if err := refused("a batch whose second record has a producer one byte over the limit", first, entity(long, x.Add(2*time.Second))); err != nil {
		return err
	}
	if s.LastSeq() != good {
		return fmt.Errorf("LastSeq is %d after refused batches, want %d", s.LastSeq(), good)
	}

	// Names outside the format, and the empty name.
	for _, c := range []struct {
		what string
		p    lifecycle.Producer
	}{
		{"a space", "a b"},
		{"an @", "a@b"},
		{"a control byte", "a\x01b"},
		{"a line feed", "a\n"},
		{"a NUL byte", "a\x00"},
		{"a rune outside ASCII, two bytes", "café"},
		{"a high byte", "\xff"},
		{"the empty name", ""},
		{"a name of 65 bytes", lifecycle.Producer(strings.Repeat("a", store.MaxProducerFormat+1))},
	} {
		if err := refused("a producer with "+c.what, entity(c.p, x.Add(3*time.Second))); err != nil {
			return err
		}
	}

	// A delete and an edge record are held to the same rule.
	if err := refused("a delete with a producer one byte over the limit", del(long, x.Add(3*time.Second))); err != nil {
		return err
	}
	if err := refused("a delete with a producer holding a space", del("a b", x.Add(3*time.Second))); err != nil {
		return err
	}
	if err := refused("an edge with a producer one byte over the limit", edge(long, x.Add(3*time.Second))); err != nil {
		return err
	}
	if err := refused("an edge with a producer holding a space", edge("a b", x.Add(3*time.Second))); err != nil {
		return err
	}
	if err := refused("an edge with the empty producer", edge("", x.Add(3*time.Second))); err != nil {
		return err
	}

	// A refusal comes at the record's turn. After a retention, every layer has a
	// horizon.
	h := x.Add(10 * time.Minute)
	if err := s.Retain(bg, h); err != nil {
		return fmt.Errorf("retaining: %w", err)
	}
	last := s.LastSeq()
	stale, over := entity("fabric", h.Add(-time.Nanosecond)), entity(long, h)
	err = s.Write(bg, cloneRecords([]store.Record{stale, over}))
	if err := wantIs("a batch of a record before the horizon, then one with a producer over the limit", err, store.ErrBeforeHorizon); err != nil {
		return err
	}
	if got := s.LastSeq(); got != last {
		return fmt.Errorf("a batch refused for the horizon moved LastSeq to %d, want %d", got, last)
	}
	seq = last
	over, stale = entity(long, h), entity("fabric", h.Add(-time.Nanosecond))
	err = s.Write(bg, cloneRecords([]store.Record{over, stale}))
	if err := wantInvalid("a batch of a record with a producer over the limit, then one before the horizon", err); err != nil {
		return err
	}
	if got := s.LastSeq(); got != last {
		return fmt.Errorf("a batch refused as invalid moved LastSeq to %d, want %d", got, last)
	}
	return nil
}
