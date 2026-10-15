package backup

import (
	"fmt"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/store"
)

// Class is the part of the backup a record is kept in: its layer, or hosts.
type Class uint8

// The five classes, in the order the manifest lists them.
const (
	// ClassL0 holds the records of layer L0, which are never retired.
	ClassL0 Class = iota
	// ClassL1 holds the records of layer L1 other than the hosts' own.
	ClassL1
	// ClassL2 holds the records of layer L2.
	ClassL2
	// ClassL3 holds the records of layer L3.
	ClassL3
	// ClassHosts holds every entity record of a host, of any kind, with or without
	// a boot id. It is never retired, so that a restored store folds every boot
	// sighting and every clone collision the live store folded.
	ClassHosts
)

// NumClasses is the number of classes.
const NumClasses = 5

var classNames = [NumClasses]string{"l0", "l1", "l2", "l3", "hosts"}

// String is the class's name in segment names and in the manifest: "l0" to "l3",
// or "hosts".
func (c Class) String() string {
	if int(c) < NumClasses {
		return classNames[c]
	}
	return fmt.Sprintf("Class(%d)", uint8(c))
}

// parseClass is the class a name names.
func parseClass(s string) (Class, bool) {
	for i, n := range classNames {
		if n == s {
			return Class(i), true
		}
	}
	return 0, false
}

// Layer is the layer whose horizon retires the class: hosts are L1 records.
func (c Class) Layer() catalog.Layer {
	if c == ClassHosts {
		return catalog.L1
	}
	return catalog.L0 + catalog.Layer(c)
}

// ClassOf is the class of a record: hosts for an entity record whose entity is a
// host, whatever its kind and whether or not it carries a boot id, and otherwise
// the record's layer. A record whose layer is not L0 to L3 is invalid, and is
// refused before it is classed.
func ClassOf(r store.Record) Class {
	if r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Host {
		return ClassHosts
	}
	return Class(r.Layer - catalog.L0)
}

// retirable reports whether rule R may ever retire the class: l1, l2 and l3.
func (c Class) retirable() bool { return c == ClassL1 || c == ClassL2 || c == ClassL3 }
