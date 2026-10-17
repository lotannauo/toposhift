package otlphttp

import "sync/atomic"

// Budget is a bound on the bytes that the requests being served hold together,
// counted as the package documentation says under "Admission". Handlers given
// the same Budget through [Options.Budget] share it, so that the logs route and
// the metrics route together stay within one bound. A Budget is safe for
// concurrent use and must not be copied.
type Budget struct {
	limit    int64
	inFlight atomic.Int64
}

// NewBudget returns a budget of limit bytes. A limit of zero or less is no
// bound: it returns nil, and a nil budget admits every request. A nil Budget in
// [Options.Budget] falls back to [Options.MaxInFlightBytes].
func NewBudget(limit int64) *Budget {
	if limit <= 0 {
		return nil
	}
	return &Budget{limit: limit}
}

// admit reserves n bytes of the budget, and reports whether it could. A nil
// budget admits everything.
func (b *Budget) admit(n int64) bool {
	if b == nil {
		return true
	}
	for {
		cur := b.inFlight.Load()
		if cur+n > b.limit {
			return false
		}
		if b.inFlight.CompareAndSwap(cur, cur+n) {
			return true
		}
	}
}

// release returns n bytes that admit reserved.
func (b *Budget) release(n int64) {
	if b != nil {
		b.inFlight.Add(-n)
	}
}
