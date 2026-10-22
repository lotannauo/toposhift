package xtspike

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/bench/xtdb/xtengine"
	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
)

// The registered sizing rule: the stream of the plan, loaded into a node of its own on a CI
// runner, must take at most this long and leave the node's directories at most this share of
// the free space the runner's disk had before the load. A 7-day dispatch is added only if the
// rule holds on both architectures.
const (
	RegisteredMaxLoad    = 75 * time.Minute
	RegisteredMaxDiskPct = 30
)

// SizingRule is the rule as the command prints it. The time of a load includes, in each
// batch's commit, the read-back of the batch's meta document ([WriteLabel]).
func SizingRule(maxLoad time.Duration, maxPct int) string {
	return fmt.Sprintf("sizing rule: the load of the plan's stream (each batch's %s included) is done within %d minutes, and the node's log and storage directories, once it has finished its last block and settled, take at most %d%% of the free space of their disk before the load",
		WriteLabel, int(maxLoad/time.Minute), maxPct)
}

// sizing is what a sizing run measured, and its verdict: nothing in it is printed but the
// verdict and the rule it was held to. A load that was stopped at its limit never finished,
// so nothing is known of the node after it.
type sizing struct {
	load     time.Duration
	stopped  bool
	storage  uint64
	free     uint64
	finished bool
	settled  bool
	settleBy string
}

// verdict applies the rule: nil reasons is a pass. The reasons name the limit that was
// exceeded and never the figure.
func (z sizing) verdict(maxLoad time.Duration, maxPct int) []string {
	if z.stopped {
		return []string{"the load did not finish within the time limit"}
	}
	var why []string
	if z.load > maxLoad {
		why = append(why, "the load exceeded the time limit")
	}
	if !z.finished {
		why = append(why, "the node did not finish its block ("+z.settleBy+")")
	} else if !z.settled {
		why = append(why, "the node did not settle ("+z.settleBy+")")
	}
	// Both are far below 2^57, so the products do not overflow.
	if z.storage*100 > z.free*uint64(maxPct) {
		why = append(why, fmt.Sprintf("the node's directories are over %d%% of the free space", maxPct))
	}
	return why
}

func (a *App) runSizing(ctx context.Context, args []string) error {
	fs := newFlags("sizing", a.Stderr)
	var c common
	var in inputFlags
	var data, base string
	maxLoad := RegisteredMaxLoad
	maxPct := RegisteredMaxDiskPct
	a.register(&c, fs)
	in.register(fs, false)
	fs.StringVar(&data, "data", a.dataDefault(), "the directory the node keeps its log and storage in (default $TOPOSHIFT_XTDB_DATA)")
	fs.StringVar(&base, "base", "", "the system time of the zeroth transaction, for a test that shares a node (default and registered: 2000-01-01T00:00:00Z)")
	fs.DurationVar(&maxLoad, "max-load", RegisteredMaxLoad, "the longest the load may take")
	fs.IntVar(&maxPct, "max-disk-percent", RegisteredMaxDiskPct, "the share of the free space the node's directories may take")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: xtspike sizing -script s -dsn d -data dir -plan p -expect-plan sha   (on CI only)")
	}
	if err := RequireCI(a.Getenv); err != nil {
		return err
	}
	if in.questions != "" || in.activity != "" {
		return errors.New("xtspike: sizing is of a plan's stream")
	}
	if err := c.validate(false, true); err != nil {
		return err
	}
	if data == "" {
		return errors.New("xtspike: -data is required (or $TOPOSHIFT_XTDB_DATA)")
	}
	if maxLoad <= 0 || maxPct <= 0 || maxPct > 100 {
		return fmt.Errorf("xtspike: a limit of %s and %d%%", maxLoad, maxPct)
	}
	baseTime, err := parseBase(base, c.registered)
	if err != nil {
		return err
	}
	s, err := in.resolve(false)
	if err != nil {
		return err
	}
	if s.plan == nil {
		return errors.New("xtspike: sizing is of a plan's stream: name it with -plan")
	}
	// A registered run of the sizing is not asked for: the sizing is a decision about what to
	// dispatch, and is disclosed with the node it ran against.
	if _, err := a.buildEnv(ctx, &c); err != nil {
		return err
	}
	free, err := a.Free(data)
	if err != nil {
		return err
	}
	n, err := a.openFresh(ctx, xtengine.Config{DSN: c.dsn, Prefix: c.prefix, Base: baseTime})
	if err != nil {
		return err
	}
	defer n.Close()
	a.out("%s", SizingRule(maxLoad, maxPct))

	var z sizing
	z.free = free
	start := a.Now()
	// The load is stopped at its limit, so that a load that cannot be done in time ends the
	// run with a verdict and does not run to the end of the job. The engine's own writes are
	// not cut short: the stream stops before the first batch that would begin after the
	// limit, and a batch in flight is written first.
	lctx, cancel := context.WithTimeout(ctx, maxLoad)
	defer cancel()
	switch err := a.loadPlan(lctx, n, s); {
	case err == nil:
	case errors.Is(err, context.DeadlineExceeded) && lctx.Err() != nil && ctx.Err() == nil:
		z.stopped = true
	default:
		return err
	}
	z.load = a.Now().Sub(start)
	if !z.stopped {
		if n.Transactions() == 0 {
			return errors.New("xtspike: the load wrote nothing")
		}
		res, err := a.finishAndSettle(ctx, &c)
		if err != nil {
			return err
		}
		z.finished, z.settled, z.settleBy = res.BlockFinished, res.Settled, res.Reason
		for _, name := range dataDirs {
			d, err := xtnode.MeasureDir(filepath.Join(data, name))
			if err != nil {
				return err
			}
			z.storage += uint64(d.Bytes)
		}
	}
	why := z.verdict(maxLoad, maxPct)
	if len(why) > 0 {
		a.out("sizing: fail (%s)", strings.Join(why, "; "))
	} else {
		a.out("sizing: pass")
	}
	return summary(a, why, maxLoad, maxPct)
}

// summary adds the verdict to the job's summary page when the runner has one.
func summary(a *App, why []string, maxLoad time.Duration, maxPct int) error {
	path, ok := a.Getenv("GITHUB_STEP_SUMMARY")
	if !ok || path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	verdict := "pass"
	if len(why) > 0 {
		verdict = "fail (" + strings.Join(why, "; ") + ")"
	}
	arch, _ := a.Getenv("RUNNER_ARCH")
	_, err = fmt.Fprintf(f, "### XTDB sizing (%s)\n\n%s\n\nsizing: %s\n", arch, SizingRule(maxLoad, maxPct), verdict)
	return err
}
