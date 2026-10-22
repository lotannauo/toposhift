// Command xtspike loads a stream into an XTDB node and records what the node answered, held
// and was asked to hold, in counts, bytes and digests. See package xtspike for its
// subcommands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/lotannauo/toposhift/bench/xtdb/xtspike"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := xtspike.Run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
