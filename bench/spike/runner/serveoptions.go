package runner

import (
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/candidates"
)

// ServeOptionsKey is the key of a manifest's Describe that says the build opened the
// root store with `serve`'s options ("true"): the boot key, L0 kept and L1 retained
// behind L2 (see [candidates.Options].ServeOptions). A build that did not has no
// such key, and a build with it is never compared or pooled with one without.
const ServeOptionsKey = "serve_options"

// checkServeOptions refuses the option for a candidate that is not built through the
// root store: the options are the root store's.
func checkServeOptions(v candidates.Variant, opts BuildOptions) error {
	if opts.ServeOptions && !v.Root() {
		return fmt.Errorf("runner: %s is not built through the root store: only a build through the root store (Lroot/...) takes serve's options", v.Name)
	}
	return nil
}
