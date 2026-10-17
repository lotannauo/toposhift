package demo

import "github.com/lotannauo/toposhift/internal/store"

// withTee is o with a second store that is written every batch the build commits,
// after the build's own store has committed it.
func withTee(o Options, second store.Store) Options {
	o.tee = second
	return o
}
