package pebblekv

import (
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// Wrap gives a database that something else opened (the root module's store,
// which opens its own) what a measurement reads off one: [KV.Snapshot],
// [KV.Quiesce], [KV.CompactAll], [KV.Canonicalize] and [KV.Describe]. It reads the
// comparer and the options text from the database, where [Open] had them from the
// layout. The caller keeps the ownership: close the owner, never the wrapper. The
// owner has its own state to wind down (the root store's Close does it), and
// closing the wrapper would close the database underneath it.
func Wrap(base *rootkv.KV) *KV {
	opts := base.Options()
	return &KV{KV: base, cmp: opts.Comparer.Compare, layoutName: opts.Comparer.Name, options: opts.String()}
}

// Quiet is cfg with Pebble's messages dropped and a fatal condition turned into a
// panic, as [Open] does unless the config names a logger: for a database that
// something else opens.
func Quiet(cfg Config) Config {
	if cfg.Logger == nil {
		cfg.Logger = quietLogger{}
	}
	return cfg
}
