package xtspike

import (
	"context"
	"errors"
	"strings"

	"github.com/lotannauo/toposhift/bench/xtdb/xtreport"
)

func (a *App) runEnv(ctx context.Context, args []string) error {
	fs := newFlags("env", a.Stderr)
	var c common
	a.register(&c, fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 {
		return errors.New("usage: xtspike env -script scripts/xt-server.sh [-container name] [-registered] [-out dir]")
	}
	if err := c.validate(false, false); err != nil {
		return err
	}
	if c.out != "" {
		if err := checkFree(c.out, "env.json"); err != nil {
			return err
		}
	}
	e, err := a.buildEnv(ctx, &c)
	if err != nil {
		return err
	}
	if c.out != "" {
		if err := writeNew(c.out, "env.json", xtreport.EnvRecord{Version: xtreport.Version, Kind: "env", Env: e}); err != nil {
			return err
		}
	}
	a.out("image: %s", e.Image)
	a.out("JVM options: %s", e.JavaOptions)
	a.out("configuration file sha256: %s", e.ConfigSHA256)
	a.out("query mode: %s", e.QueryMode)
	if e.Container != nil {
		a.out("container %s: image %s, JVM options %q, XTDB %s (revision %s), configuration sha256 %s", e.Container.Name, e.Container.ImageRef, e.Container.JavaOptions, e.Container.XTDBVersion, e.Container.GitSHA, e.Container.ConfigSHA256)
	} else {
		a.out("container: not inspected (-container names it)")
	}
	a.out("binary: commit %s (modified %s), sha256 %s, %s %s/%s", e.Commit, e.Modified, e.BinarySHA256, e.GoVersion, e.GOOS, e.GOARCH)
	a.out("machine: %s, %d processors", e.CPUModel, e.NumCPU)
	if e.Registered {
		a.out("registered: yes")
	} else {
		a.out("registered: NO\n  %s", strings.Join(e.NotRegistered, "\n  "))
	}
	return nil
}
