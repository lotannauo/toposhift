// Command doltspike loads a stream into Dolt, a commit per batch, and records what the reads
// of a plan's questions are answered as and what the history costs in bytes, in counts and
// never in time. See package doltspike for its subcommands.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/lotannauo/toposhift/bench/dolt/doltspike"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := doltspike.Run(ctx, os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "doltspike:", err)
		os.Exit(1)
	}
}
