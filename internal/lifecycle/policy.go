package lifecycle

import (
	"fmt"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
)

// BootID is the descriptive attribute that tells a host's boots apart:
// /proc/sys/kernel/random/boot_id on Linux. OpenTelemetry defines no boot ID,
// so it carries a topo prefix. It is never part of an identity: a reboot is
// one host with a new boot, not a new host.
const BootID catalog.AttributeKey = "topo.host.boot.id"

// Policy configures a fold. The zero value ranks no producer, tracks no
// boots and tolerates no clock skew.
type Policy struct {
	// Rank is the authority order for merging descriptions: a higher rank
	// wins. Producers not listed rank below every listed one.
	Rank map[Producer]int
	// BootKey, if set, names the attribute whose value identifies a
	// boot, and turns on boot tracking and clone-collision detection.
	// Use [BootID] for hosts.
	BootKey catalog.AttributeKey
	// Skew is how long after a newer boot first appears an older boot may
	// still be observed without being a clone collision, to absorb clock
	// differences between producers. Never negative; zero is strict.
	Skew time.Duration
}

func (p Policy) validate() error {
	if p.Skew < 0 {
		return fmt.Errorf("policy: negative skew %s: %w", p.Skew, ErrInvalid)
	}
	if p.BootKey != "" && !p.BootKey.Valid() {
		return fmt.Errorf("policy: boot key %q is not a valid attribute name: %w", p.BootKey, ErrInvalid)
	}
	return nil
}

// outranks reports whether producer a wins over producer b: a listed producer
// beats an unlisted one, a higher rank beats a lower one, and the smaller name
// breaks every remaining tie, so the order is total and never depends on
// timing.
func (p Policy) outranks(a, b Producer) bool {
	ra, okA := p.Rank[a]
	rb, okB := p.Rank[b]
	switch {
	case okA != okB:
		return okA
	case okA && ra != rb:
		return ra > rb
	}
	return a < b
}
