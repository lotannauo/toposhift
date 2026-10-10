package coalesce

import (
	"errors"
	"fmt"
	"time"
)

// ErrConfig marks an invalid [Config]. Every error [Config.Validate] returns
// wraps it.
var ErrConfig = errors.New("coalesce: invalid config")

// Config says how runs are extended and bounded. See the package doc for the
// trade-offs.
type Config struct {
	// ExtendTTLFraction, in [0, 1], re-asserts a run at most once per this share of its TTL.
	// Exclusive with ExtendEvery. Both zero: every refresh extends (nothing is absorbed).
	ExtendTTLFraction float64
	// ExtendEvery is the same as a fixed interval; not negative.
	ExtendEvery time.Duration
	// RunMaxAge continues a run that has reached this age, counted from its first event
	// time, with a new run at the refresh's own time. Zero is no bound; otherwise at least
	// a minute.
	RunMaxAge time.Duration
	// RunAgeFromTTL bounds each run's age by its own extension interval; see the package doc.
	// Exclusive with RunMaxAge, and it needs ExtendTTLFraction or ExtendEvery.
	RunAgeFromTTL bool
}

// DefaultConfig is the setting the layout was measured with at a 15-minute pod
// heartbeat: extend at most once per half a TTL, and bound a run's age at 30
// minutes.
func DefaultConfig() Config {
	return Config{ExtendTTLFraction: 0.5, RunMaxAge: 30 * time.Minute}
}

// Validate reports the first rule broken, wrapping [ErrConfig].
func (c Config) Validate() error {
	bad := func(format string, args ...any) error {
		return fmt.Errorf("%w: %s", ErrConfig, fmt.Sprintf(format, args...))
	}
	switch {
	case !(c.ExtendTTLFraction >= 0 && c.ExtendTTLFraction <= 1): // also false for NaN
		return bad("ExtendTTLFraction %g is not in [0, 1]", c.ExtendTTLFraction)
	case c.ExtendEvery < 0:
		return bad("ExtendEvery %s is negative", c.ExtendEvery)
	case c.ExtendTTLFraction > 0 && c.ExtendEvery > 0:
		return bad("ExtendTTLFraction and ExtendEvery are two ways to bound extension: use one")
	case c.RunMaxAge < 0 || (c.RunMaxAge > 0 && c.RunMaxAge < time.Minute):
		return bad("RunMaxAge %s must be zero or at least a minute", c.RunMaxAge)
	case c.RunAgeFromTTL && c.RunMaxAge > 0:
		return bad("RunAgeFromTTL and RunMaxAge are exclusive")
	case c.RunAgeFromTTL && c.ExtendTTLFraction == 0 && c.ExtendEvery == 0:
		return bad("RunAgeFromTTL needs an extension interval")
	}
	return nil
}

// ExtensionInterval is how close to its last extension a run with this TTL may be
// refreshed without a new record: ExtendEvery, or ExtendTTLFraction x ttl, or 0.
func (c Config) ExtensionInterval(ttl time.Duration) time.Duration {
	if c.ExtendTTLFraction > 0 {
		return time.Duration(c.ExtendTTLFraction * float64(ttl))
	}
	return c.ExtendEvery
}

// ageBound is the age at which a run with this TTL, counted from its first event
// time, is continued by a new run: its own extension interval when RunAgeFromTTL is
// on, otherwise RunMaxAge. Zero is no bound.
func (c Config) ageBound(ttl time.Duration) time.Duration {
	if c.RunAgeFromTTL {
		return c.ExtensionInterval(ttl)
	}
	return c.RunMaxAge
}

// Describe names the settings, for a caller to record next to its producers'
// intervals: "extend_ttl_fraction", "extend_every" and "run_max_age" (Go duration
// strings, and %g for the fraction), and "run_age_from_ttl" ("true") only when
// RunAgeFromTTL is on.
func (c Config) Describe() map[string]string {
	d := map[string]string{
		"extend_ttl_fraction": fmt.Sprintf("%g", c.ExtendTTLFraction),
		"extend_every":        c.ExtendEvery.String(),
		"run_max_age":         c.RunMaxAge.String(),
	}
	if c.RunAgeFromTTL {
		d["run_age_from_ttl"] = "true"
	}
	return d
}
