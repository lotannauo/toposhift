package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// stores are the two stores of a pass and the directory the Pebble one lives in.
type stores struct {
	mem *memstore.Store
	peb *pebblestore.Store
	dir string
}

func (s *stores) asStores() (mem, peb store.Store) { return s.mem, s.peb }

// openStores opens a memstore and a pebblestore (retention synchronous, none run) in a
// new directory under root.
func openStores(root, name string) (_ *stores, err error) {
	dir, err := os.MkdirTemp(root, "capturereplay-"+name+"-*")
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(dir))
		}
	}()
	mem, err := memstore.Open(memstore.Options{})
	if err != nil {
		return nil, err
	}
	peb, err := pebblestore.Open(filepath.Join(dir, "db"), pebblestore.Options{
		Config:    pebblekv.Quiet(pebblekv.Config{Tuning: pebblekv.BenchTuning()}),
		Retention: pebblestore.Synchronous,
	})
	if err != nil {
		return nil, errors.Join(err, mem.Close())
	}
	return &stores{mem: mem, peb: peb, dir: dir}, nil
}

// close closes both stores and, unless keep, removes the directory, which this process
// made.
func (s *stores) close(keep bool) error {
	err := errors.Join(s.mem.Close(), s.peb.Close())
	if !keep {
		err = errors.Join(err, os.RemoveAll(s.dir))
	} else {
		fmt.Fprintln(os.Stderr, "capturereplay: kept", s.dir)
	}
	return err
}
