package keys

import (
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// newRand is a deterministic source of random bytes.
func newRand(seed byte) *rand.ChaCha8 {
	var s [32]byte
	s[0] = seed
	return rand.NewChaCha8(s)
}

// clock is a settable clock.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestStore opens a Store on a new directory with a fixed clock and
// deterministic randomness. The optional clock is shared with the caller.
func newTestStore(t testing.TB, clk *clock) *Store {
	t.Helper()
	return openIn(t, t.TempDir(), clk, 1)
}

// openIn opens another Store on the same directory; seed selects its random
// stream, so that two Stores never draw the same key.
func openIn(t testing.TB, dir string, clk *clock, seed byte) *Store {
	t.Helper()
	if clk == nil {
		clk = newClock()
	}
	s, err := Open(dir, Options{Now: clk.Now, Rand: newRand(seed)})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
