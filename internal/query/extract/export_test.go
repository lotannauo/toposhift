package extract

import "github.com/lotannauo/toposhift/internal/store"

// MemoCap is the size of the memo NewExtractor gives each extraction.
const MemoCap = memoCap

// NewExtractorNoMemo returns an extractor whose reads all reach st: its memo
// has the size 0, which keeps nothing.
func NewExtractorNoMemo(st store.Store) *Extractor { return NewExtractorCap(st, 0) }

// NewExtractorCap returns an extractor whose memo has the given size.
func NewExtractorCap(st store.Store, capacity int) *Extractor {
	return &Extractor{st: st, capacity: capacity}
}
