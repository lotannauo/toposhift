package storetest

import (
	"fmt"
	"os"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/store"
)

// CheckReopen runs workloads 0, 2 and 5 through a store that is closed and opened
// again three times along the way, with retentions between, comparing every
// answer with the reference's after each reopening. After each it also requires
// the horizon to be what it was, the closed handle to report ErrClosed, and an
// entity that was quarantined to be quarantined still. f must be durable.
func CheckReopen(f Factory) error {
	workloads := Workloads()
	for _, i := range []int{0, 2, 5} {
		if err := checkReopen(f, workloads[i]); err != nil {
			return fmt.Errorf("workload %q: %w", workloads[i].Name, err)
		}
	}
	return nil
}

func checkReopen(f Factory, w Workload) error {
	dir, err := os.MkdirTemp("", "storetest-reopen")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(dir) }()
	cand, err := f.Open(dir, w.Policy)
	if err != nil {
		return err
	}
	g, err := NewGenerator(w.Config)
	if err != nil {
		return err
	}
	var hosts []identity.Fingerprint
	for _, fp := range g.Entities() {
		if fp.Type() == catalog.Host {
			hosts = append(hosts, fp)
		}
	}
	// Whatever store is current at the end, or when something fails, is closed
	// here; Reopen closes the one it replaces.
	current := &cand
	defer func() { _ = (*current).Close() }()
	return Check(cand, w, Options{
		RetainAt: []float64{0.45, 0.8}, ReopenAt: []float64{0.2, 0.5, 0.85}, CheckEvery: 15,
		Reopen: func(old store.Store) (store.Store, error) {
			type fate struct {
				ok  bool
				err error
			}
			before := map[identity.Fingerprint]fate{}
			if w.Policy.BootKey != "" {
				for _, fp := range hosts {
					ok, err := old.Alive(bg, fp, store.MaxEventTime, store.Current(entityLayer(catalog.Host)))
					if err != nil && !isQuarantine(err) {
						return nil, fmt.Errorf("asking Alive of %s before closing: %w", fp, err)
					}
					before[fp] = fate{ok, err}
				}
			}
			if err := old.Close(); err != nil {
				return nil, err
			}
			next, err := f.Open(dir, w.Policy)
			if err != nil {
				return nil, err
			}
			*current = next
			for fp, was := range before {
				ok, err := next.Alive(bg, fp, store.MaxEventTime, store.Current(entityLayer(catalog.Host)))
				if err != nil && !isQuarantine(err) {
					return next, fmt.Errorf("asking Alive of %s after reopening: %w", fp, err)
				}
				if msg := aliveDiff(ok, err, was.ok, was.err); msg != "" {
					return next, fmt.Errorf("%w: Alive of %s after reopening = %s; before closing it was %s", ErrMismatch, fp, describeAlive(ok, err), describeAlive(was.ok, was.err))
				}
			}
			return next, nil
		},
	})
}
