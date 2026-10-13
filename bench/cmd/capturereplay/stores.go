package main

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// stores are the two stores of a pass, the directory the Pebble one lives in, and what
// the Pebble one counted.
type stores struct {
	mem *memstore.Store
	peb *pebblestore.Store
	dir string
	rec *storeCounts
}

func (s *stores) asStores() (mem, peb store.Store) { return s.mem, s.peb }

// lookup is the opener of the Pebble store's own lookup, which the resume step reads.
func (s *stores) lookup() (resume.Lookup, error) {
	lk, err := s.peb.NewLookup()
	if err != nil {
		return nil, err
	}
	return lk, nil
}

// storeCounts is the Pebble store's recorder: the sum of each counter and the largest of
// each sample.
type storeCounts struct {
	mu   sync.Mutex
	sums map[string]int64
	maxs map[string]int64
}

func newStoreCounts() *storeCounts {
	return &storeCounts{sums: map[string]int64{}, maxs: map[string]int64{}}
}

func (c *storeCounts) Count(name string, n int64) {
	c.mu.Lock()
	c.sums[name] += n
	c.mu.Unlock()
}

func (c *storeCounts) Sample(name string, v int64) {
	c.mu.Lock()
	if v > c.maxs[name] {
		c.maxs[name] = v
	}
	c.mu.Unlock()
}

func (c *storeCounts) sum(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sums[name]
}

func (c *storeCounts) max(name string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.maxs[name]
}

// openStores opens a memstore and a pebblestore (retention synchronous, none run, the
// store's default checkpoint policy, and a recorder) in a new directory under root.
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
	rec := newStoreCounts()
	peb, err := pebblestore.Open(filepath.Join(dir, "db"), pebblestore.Options{
		Config:    pebblekv.Quiet(pebblekv.Config{Tuning: pebblekv.BenchTuning()}),
		Retention: pebblestore.Synchronous,
		Recorder:  rec,
	})
	if err != nil {
		return nil, errors.Join(err, mem.Close())
	}
	return &stores{mem: mem, peb: peb, dir: dir, rec: rec}, nil
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
