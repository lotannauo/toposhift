package xtspike

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/xtdb/xtnode"
	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

// dataDirs are the directories the node's configuration gives its log and storage, under
// the directory the server script mounts into the container.
var dataDirs = []string{"log", "storage"}

// dataFlag registers -data.
func (a *App) dataDefault() string {
	v, _ := a.Getenv("TOPOSHIFT_XTDB_DATA")
	return v
}

func (a *App) runSizes(ctx context.Context, args []string) error {
	fs := newFlags("sizes", a.Stderr)
	var c common
	var in inputFlags
	var loadPath, data string
	a.register(&c, fs)
	in.register(fs, false)
	loadFlag(fs, &loadPath)
	fs.StringVar(&data, "data", a.dataDefault(), "the directory the node keeps its log and storage in (default $TOPOSHIFT_XTDB_DATA)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: xtspike sizes -script s -out dir -data dir -load load.json (-plan p -expect-plan sha | -questions q -expect-questions sha)")
	}
	if err := c.validate(true, false); err != nil {
		return err
	}
	if data == "" {
		return errors.New("xtspike: -data is required (or $TOPOSHIFT_XTDB_DATA)")
	}
	if err := checkFree(c.out, "sizes.json"); err != nil {
		return err
	}
	s, err := in.resolve(false)
	if err != nil {
		return err
	}
	load, loadSHA, err := readLoad(loadPath)
	if err != nil {
		return err
	}
	if err := sameInput(load.Input, s.input()); err != nil {
		return err
	}
	if !load.BlockFinished {
		return fmt.Errorf("xtspike: the load record says the node's last block was not finished (%s): a size taken now would miss the tail of the stream that is still in the log, so none is taken", load.SettleReason)
	}
	env, err := a.buildEnv(ctx, &c)
	if err != nil {
		return err
	}
	if err := sameBuild(load.Env, env); err != nil {
		return err
	}
	if env.Container != nil {
		if dir, err := a.mountedData(ctx, &c); err != nil {
			return err
		} else if dir != "" && !sameDir(dir, data) {
			return fmt.Errorf("xtspike: -data is %s, and the container %s keeps its files in %s", data, c.container, dir)
		}
	}
	res, err := a.settle(ctx, &c)
	if err != nil {
		return err
	}
	rec := xtreport.Sizes{
		Version: xtreport.Version, Kind: "sizes", Env: env, Input: s.input(), Prefix: load.Prefix, LoadSHA256: loadSHA,
		BlockFinished: load.BlockFinished, Settled: res.Settled, SettleReason: res.Reason, RecordsHeld: load.Writer.Records,
		Note: "the whole directories of the node (every table of it), after the node finished its last block and settled; XTDB holds the whole stream, since retention is not mapped",
	}
	h := sha256.New()
	for _, name := range dataDirs {
		d, err := xtnode.MeasureDir(filepath.Join(data, name))
		if err != nil {
			return err
		}
		rec.Dirs = append(rec.Dirs, xtreport.Dir{Path: name, Bytes: d.Bytes, Files: d.Files, ListingDigest: d.ListingDigest})
		rec.Bytes += d.Bytes
		fmt.Fprintf(h, "%s %s\n", name, d.ListingDigest)
	}
	rec.ListingDigest = hex.EncodeToString(h.Sum(nil))
	if rec.RecordsHeld > 0 {
		rec.BytesPerRecord = float64(rec.Bytes) / float64(rec.RecordsHeld)
	}
	if !rec.Settled {
		rec.Note += "; the node did not settle, so these are not the sizes of a settled node and are not to be compared"
	}
	if err := writeNew(c.out, "sizes.json", rec); err != nil {
		return err
	}
	for _, d := range rec.Dirs {
		a.out("%-8s %d bytes in %d files", d.Path, d.Bytes, d.Files)
	}
	a.out("total %d bytes for %d records held: %.1f bytes per record (listing digest %s)", rec.Bytes, rec.RecordsHeld, rec.BytesPerRecord, rec.ListingDigest)
	if !rec.Settled {
		a.out("settle: DID NOT SETTLE (%s); these sizes are not those of a settled node", rec.SettleReason)
	}
	a.out("record: %s/sizes.json", c.out)
	return nil
}

// mountedData is the host directory the container keeps its files in.
func (a *App) mountedData(ctx context.Context, c *common) (string, error) {
	cont, err := xtnode.Inspect(ctx, a.Docker, c.container)
	if err != nil {
		return "", err
	}
	return cont.DataDir, nil
}

// sameDir reports whether two paths name one directory.
func sameDir(a, b string) bool {
	ai, aerr := os.Stat(a)
	bi, berr := os.Stat(b)
	return aerr == nil && berr == nil && os.SameFile(ai, bi)
}
