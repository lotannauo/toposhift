// Command streamstats prints what a workload preset amounts to: records a day by
// kind, how many distinct peers a busy node meets, how long each kind of prefix
// is, how much of the stream is late, and how many dead entities sit behind the
// live ones. It exists so that whoever reads a measurement can first ask whether
// the workload is believable.
//
//	go run ./cmd/streamstats -preset ci
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/lotannauo/toposhift/bench/spike/workload"
)

func main() {
	preset := flag.String("preset", "ci", "ci, week or month")
	days := flag.Int("days", 0, "override the number of days (a cluster of the preset's size)")
	seed := flag.Uint64("seed", 0, "override the seed")
	flag.Parse()

	var cfg workload.Config
	switch *preset {
	case "ci":
		cfg = workload.CI()
	case "week":
		cfg = workload.Week()
	case "month":
		cfg = workload.Month()
	default:
		fmt.Fprintf(os.Stderr, "streamstats: unknown preset %q\n", *preset)
		os.Exit(2)
	}
	if *days > 0 {
		cfg.Duration = time.Duration(*days) * 24 * time.Hour
	}
	if *seed > 0 {
		cfg.Seed = *seed
	}
	if err := run(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "streamstats:", err)
		os.Exit(1)
	}
}

func run(cfg workload.Config) error {
	g, err := workload.New(cfg)
	if err != nil {
		return err
	}
	a := workload.NewAnalyzer(cfg.Start, cfg.Duration)
	begin := time.Now()
	for {
		r, ok := g.Next()
		if !ok {
			break
		}
		a.Add(r)
	}
	elapsed := time.Since(begin)
	a.Report().Write(os.Stdout)
	fmt.Printf("\ngenerated and analyzed in %s\n", elapsed.Round(time.Second))
	return nil
}
