package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// childArg, as the first argument of the test binary, makes it run serve
// instead of the tests: serve on the data directory in the second argument,
// with a retention check every 50 ms, and with every retention pass held, for
// good, once its rewriting is over. TestP6KilledAfterARetainResumes runs it and
// kills it.
const childArg = "-serve-retain-child"

// childHeld is what the child logs when it holds a pass.
const childHeld = "the test holds the retention pass"

func init() {
	if len(os.Args) != 3 || os.Args[1] != childArg {
		return
	}
	retainEvery = 50 * time.Millisecond
	openStore = func(path string, o pebblestore.Options) (*pebblestore.Store, error) {
		o.Observer = holdForever{o.Observer}
		return pebblestore.Open(path, o)
	}
	os.Exit(run(context.Background(), []string{"serve", "--listen", "127.0.0.1:0", "--data-dir", os.Args[2], "--retain", "on"}, noEnv, os.Stdout, os.Stderr))
}

// holdForever passes the phases on, and never returns from the end of a pass's
// rewriting.
type holdForever struct{ pebblestore.RetainObserver }

func (h holdForever) RewriteEnded(gen uint64, at time.Time) {
	h.RetainObserver.RewriteEnded(gen, at)
	_, _ = fmt.Fprintln(os.Stderr, childHeld)
	select {}
}
