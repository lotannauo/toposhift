package doltspike

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/lotannauo/toposhift/bench/dolt/doltengine"
)

// SizingLimits are what a runner must be able to take for the registered load to be run on
// it: the time the load may take and the share of the free disk its data directory may fill.
type SizingLimits struct {
	Budget    time.Duration
	DiskShare float64
}

// Sizing is the verdict on one load of a stream: whether it fits, and which limit it broke.
// It holds no duration and no byte count: the sizing step is pass or fail, and a time from it
// is not a figure.
type Sizing struct {
	Fits     bool
	OverTime bool
	OverDisk bool
}

// sizing loads the stream into a database of the arm, as load does, under a deadline, and
// says whether the load finished in time and the database's directory fits the share of the
// disk that was free when it began. The database is dropped. A load that breaks another way is
// an error and not a verdict.
func sizing(ctx context.Context, o loadOpts, env Env, dataDir string, lim SizingLimits) (Sizing, error) {
	free, err := freeBytes(dataDir)
	if err != nil {
		return Sizing{}, fmt.Errorf("doltspike: the free disk of %s: %w", dataDir, err)
	}
	o.Replace = true
	lctx, cancel := context.WithTimeout(ctx, lim.Budget)
	defer cancel()
	var v Sizing
	_, lerr := load(lctx, o, env)
	switch {
	case lerr == nil:
	case errors.Is(lerr, context.DeadlineExceeded) && ctx.Err() == nil:
		v.OverTime = true
	default:
		return Sizing{}, lerr
	}
	if lerr == nil {
		// The load has ended and its engine is closed, but the database is still there to be
		// measured: load drops one only when it fails.
		b, err := MeasureDir(filepath.Join(dataDir, o.Database))
		if err != nil {
			return Sizing{}, err
		}
		v.OverDisk = float64(b.Total) > lim.DiskShare*float64(free)
		if err := doltengine.DropDatabase(ctx, o.DSN, o.Database); err != nil {
			return Sizing{}, err
		}
	}
	v.Fits = !v.OverTime && !v.OverDisk
	return v, nil
}

// ErrNotOnCI is wrapped by the error the sizing command returns off CI: it decides on a time,
// and a time from anywhere but the runners is not one to decide on.
var ErrNotOnCI = errors.New("doltspike: sizing decides on a time, and is run on CI only (GITHUB_ACTIONS is not true)")

func runSizing(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	fs := newFlags("sizing", stderr)
	var (
		c      common
		in     inputFlags
		arm    string
		data   string
		budget time.Duration
		share  float64
	)
	c.register(fs, true)
	in.register(fs, true)
	fs.StringVar(&arm, "arm", "commit", "the arm to load: commit or rows")
	fs.StringVar(&data, "data", os.Getenv("TOPOSHIFT_DOLT_DATA"), "the server's data directory on this machine (default $TOPOSHIFT_DOLT_DATA)")
	fs.DurationVar(&budget, "budget", 75*time.Minute, "the time the load may take")
	fs.Float64Var(&share, "disk-share", 0.30, "the share of the free disk the database's directory may fill")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if data == "" || fs.NArg() != 0 {
		return errors.New("usage: doltspike sizing -dsn dsn -image image -db name -data dir [-arm commit|rows] [-budget 75m] [-disk-share 0.30] (-plan plan.json -expect-plan sha | -questions q.json -expect-questions sha -activity file -expect-activity digest)")
	}
	if os.Getenv("GITHUB_ACTIONS") != "true" {
		return ErrNotOnCI
	}
	if budget <= 0 || share <= 0 || share >= 1 {
		return errors.New("doltspike: the budget must be positive and the disk share between 0 and 1")
	}
	a, err := parseArm(arm, false)
	if err != nil {
		return err
	}
	if err := c.validate(false, true); err != nil {
		return err
	}
	s, err := in.resolve(true)
	if err != nil {
		return err
	}
	env, err := serverEnv(ctx, c.dsn, c.image)
	if err != nil {
		return err
	}
	v, err := sizing(ctx, loadOpts{DSN: c.dsn, Database: c.db, Arm: a, Stream: s, Say: progress(stderr)}, env, data, SizingLimits{Budget: budget, DiskShare: share})
	if err != nil {
		return err
	}
	fmt.Fprintf(stdout, "sizing: input %s, arm %s, database %s\n", s.input().Kind, a, c.db)
	if v.Fits {
		fmt.Fprintln(stdout, "sizing: pass")
		return nil
	}
	if v.OverTime {
		fmt.Fprintln(stdout, "sizing: fail: the load did not finish within its time budget")
	}
	if v.OverDisk {
		fmt.Fprintln(stdout, "sizing: fail: the data directory is over its share of the free disk")
	}
	return errors.New("doltspike: sizing: fail")
}
