package backup

import (
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// Hooks are the cutter's test hooks, which show what each rule and the hosts
// class are for. The zero value is the product's behavior.
type Hooks struct {
	// StartSeq is the first Seq an empty backup takes; zero means 1.
	StartSeq uint64
	// ClassifyByBoot classes a record by whether it carries a boot id, not by
	// whether it is a host's: the class an earlier design proposed.
	ClassifyByBoot bool
	// RetireHosts retires the hosts class as l1 is retired.
	RetireHosts bool
	// NoMinEvent drops rule R's condition (b) for the classes it marks.
	NoMinEvent [NumClasses]bool
	// NoCutBeforeRetire has Retire decide on what is cut only.
	NoCutBeforeRetire bool
	// ZeroMargin and NoRefusal set the margin to zero and drop the refusal for the
	// classes they mark.
	ZeroMargin, NoRefusal [NumClasses]bool
}

// SetHooks installs hooks on a cutter that has added nothing yet.
func SetHooks(c *Cutter, h Hooks) {
	c.h = hooks{
		startSeq:          h.StartSeq,
		retireHosts:       h.RetireHosts,
		noMinEvent:        h.NoMinEvent,
		noCutBeforeRetire: h.NoCutBeforeRetire,
		zeroMargin:        h.ZeroMargin,
		noRefusal:         h.NoRefusal,
	}
	if h.ClassifyByBoot {
		c.h.classify = classifyByBoot
	}
}

// RestoreClassifiedByBoot is a restore of a backup cut with ClassifyByBoot.
func RestoreClassifiedByBoot(o RestoreOptions) RestoreOptions {
	o.classify = classifyByBoot
	return o
}

// classifyByBoot puts a record that carries a boot id in hosts, and every other
// record in its layer's class.
func classifyByBoot(r store.Record) Class {
	if r.Boot != "" {
		return ClassHosts
	}
	return Class(r.Layer - catalog.L0)
}
