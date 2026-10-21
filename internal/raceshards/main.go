// Command raceshards splits the root module's packages between the two race
// shards of CI (mise run ci:raceheavy and ci:racerest), so that the slowest
// packages under the race detector do not share one runner's four processors
// with everything else.
//
// It reads the module's package list on standard input, one import path a line
// (the output of go list ./...), and prints the part asked for:
//
//	go list ./... | go run ./internal/raceshards heavy
//	go list ./... | go run ./internal/raceshards rest
//
// heavy is the packages named in Heavy, in the order listed; rest is every other
// package, in the order read. The two parts partition the input by construction:
// a package is in heavy if and only if it is named in Heavy, and in rest
// otherwise, so none is in both and none is in neither. What can go wrong is a
// stale name, and that fails the command: every name in Heavy must be in the
// input exactly once, and the input must hold no duplicate and no blank line.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// Heavy is the packages of the heavy shard: the slowest under the race detector
// on CI's x86-64 runners, chosen so that the two shards take about the same
// time. In the verify run of 2026-10-20 (job 118451827072, ubuntu-24.04) they
// took 505 s, 415 s, 287 s and 277 s of the root module's 3091 s, and the
// longest of them bounds the shard: the forty-two other packages sum to 1607 s
// and the longest of those is 259 s. Re-balance from the "Slowest tests" job
// summaries when a package moves; nothing else needs to change.
var Heavy = []string{
	"github.com/lotannauo/toposhift/internal/query/storediff",
	"github.com/lotannauo/toposhift/internal/store/pebblestore",
	"github.com/lotannauo/toposhift/internal/store/storetest",
	"github.com/lotannauo/toposhift/internal/replay/activity",
}

func main() {
	if len(os.Args) != 2 || (os.Args[1] != "heavy" && os.Args[1] != "rest") {
		fmt.Fprintln(os.Stderr, "usage: go list ./... | raceshards heavy|rest")
		os.Exit(2)
	}
	all, err := readPackages(os.Stdin)
	if err == nil {
		var out []string
		out, err = split(all, Heavy, os.Args[1] == "heavy")
		for _, p := range out {
			fmt.Println(p)
		}
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "raceshards:", err)
		os.Exit(1)
	}
}

// readPackages reads one import path a line and refuses a blank line, a line
// with spaces and a duplicate, each of which would mean the list is not the
// output of go list ./... and the two parts might not partition the module.
func readPackages(r io.Reader) ([]string, error) {
	var all []string
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		p := sc.Text()
		if p == "" || strings.ContainsAny(p, " \t") {
			return nil, fmt.Errorf("line %d: %q is not an import path", n, p)
		}
		if seen[p] {
			return nil, fmt.Errorf("line %d: %s is listed twice", n, p)
		}
		seen[p] = true
		all = append(all, p)
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	if len(all) == 0 {
		return nil, errors.New("no packages on standard input")
	}
	return all, nil
}

// split returns the heavy part (the packages of heavy, in heavy's order) or the
// rest (every other package of all, in all's order). It fails if a name in heavy
// is not in all or is listed twice: a stale name would otherwise leave the heavy
// shard short and the package tested by neither shard.
func split(all, heavy []string, wantHeavy bool) ([]string, error) {
	in := map[string]bool{}
	for _, p := range all {
		in[p] = true
	}
	isHeavy := map[string]bool{}
	for _, p := range heavy {
		if !in[p] {
			return nil, fmt.Errorf("heavy package %s is not in the module's package list", p)
		}
		if isHeavy[p] {
			return nil, fmt.Errorf("heavy package %s is listed twice", p)
		}
		isHeavy[p] = true
	}
	var out []string
	if wantHeavy {
		out = append(out, heavy...)
		return out, nil
	}
	for _, p := range all {
		if !isHeavy[p] {
			out = append(out, p)
		}
	}
	return out, nil
}
