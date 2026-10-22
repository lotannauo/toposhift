package vminput

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
)

// DefaultExtendFraction is the share of its TTL after which the coalescer that wrote the
// Alibaba placement streams re-asserts a run: a half, the product's.
const DefaultExtendFraction = 0.5

// CheckSameFacts checks that two activity files describe the same existence: the
// coalesced stream the reference is read from and the every-sample stream the store is
// written, as [vmmodel.CheckSameFacts] does for a generated stream. Every subject of the
// every-sample file must be in the coalesced one with the same intervals, an end earlier
// by less than the fraction of the run's TTL the coalescer re-asserts after (and less
// than the subject's band) and never later.
//
// It holds both files' records, without payloads, in memory, one record per subject
// occurrence: the memory is the records of the larger file.
func CheckSameFacts(coalesced, every string, reg *vmmodel.Registry, fraction float64) (vmmodel.FactsReport, error) {
	if !(fraction > 0 && fraction <= 1) {
		return vmmodel.FactsReport{}, fmt.Errorf("vminput: the extension fraction must be in (0, 1], not %g", fraction)
	}
	a, b := vmmodel.NewFactSink(nil), vmmodel.NewFactSink(nil)
	if _, err := Drive(coalesced, a); err != nil {
		return vmmodel.FactsReport{}, fmt.Errorf("vminput: the coalesced stream: %w", err)
	}
	if _, err := Drive(every, b); err != nil {
		return vmmodel.FactsReport{}, fmt.Errorf("vminput: the every-sample stream: %w", err)
	}
	rep, err := vmmodel.CompareFacts(a.Records(), b.Records(), func(s engine.Subject) time.Duration {
		return vmmodel.ExtensionBound(true, 0, fraction, reg, s)
	})
	rep.SmallestBand = vmmodel.SmallestBand(reg, b.Records())
	return rep, err
}
