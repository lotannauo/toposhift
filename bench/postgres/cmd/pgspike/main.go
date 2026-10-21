// Command pgspike loads a stream into Postgres and records what its reads cost there, in
// counts, bytes and log written, never in time. See package pgspike for the commands.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lotannauo/toposhift/bench/postgres/pgspike"
)

func main() {
	if err := pgspike.Run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "pgspike:", err)
		os.Exit(1)
	}
}
