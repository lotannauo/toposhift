package vmengine

import "time"

// SetSettleInterval changes how often a loader polls an idle server, for the tests that
// load into a fake one, and returns what it was.
func SetSettleInterval(d time.Duration) time.Duration {
	old := settleInterval
	settleInterval = d
	return old
}
