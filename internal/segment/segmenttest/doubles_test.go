package segmenttest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"iter"
	"slices"
	"strings"
	"sync"

	"github.com/lotannauo/toposhift/internal/segment"
)

// A flaw is one way a store can break the contract.
type flaw int

const (
	flawNone               flaw = iota
	flawOverwrite               // Put replaces an existing segment
	flawPartialOnReadError      // a failed Put leaves what it had read
	flawInsertionOrder          // List yields in the order of writing
	flawDotDot                  // a name with ".." is accepted
	flawIgnoreContextInPut      // Put never looks at its context
	flawDeleteMissingErr        // Delete of a missing name fails with ErrNotFound
	flawRacyPut                 // the existence check and the write are apart
	flawDropDataWithEOF         // bytes returned together with io.EOF are lost
	flawIgnoreSize              // the announced size is not checked
	flawNoLimit                 // MaxSize is not enforced
	flawWrongSHAOnStat          // Stat reports a wrong digest
	flawWrongSHAOnPut           // Put reports a wrong digest
	flawUpperCase               // upper-case letters are accepted in names
	flawLimitOffByOne           // a segment of exactly the limit is refused
)

// doubleMax is the limit the honest double enforces. It is smaller than
// segment.MaxSize so that the checks need not move 256 MiB through memory.
const doubleMax = 4 << 20

// double is an in-memory store that is honest unless it carries a flaw.
type double struct {
	flaw flaw

	// gate holds back every Put of raceName until all contenders have passed
	// the existence check (flawRacyPut only). Its count is the suite's number
	// of writers of that name: fewer would let the first release early, more
	// would block the last forever.
	gate sync.WaitGroup

	mu    sync.Mutex
	data  map[string][]byte
	order []string // names in the order they were written
}

func newDouble(f flaw) *double {
	d := &double{flaw: f, data: map[string][]byte{}}
	if f == flawRacyPut {
		d.gate.Add(contenders) // the suite starts exactly this many writers of raceName
	}
	return d
}

func (d *double) validName(name string) error {
	switch {
	case d.flaw == flawDotDot && strings.Contains(name, ".."):
		return nil
	case d.flaw == flawUpperCase:
		return segment.ValidName(strings.ToLower(name))
	}
	return segment.ValidName(name)
}

func (d *double) validate(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.validName(name)
}

func (d *double) limit() int64 {
	if d.flaw == flawNoLimit {
		return 1 << 62
	}
	return doubleMax
}

func (d *double) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if d.flaw == flawIgnoreContextInPut {
		if err := d.validName(name); err != nil {
			return segment.Info{}, err
		}
	} else if err := d.validate(ctx, name); err != nil {
		return segment.Info{}, err
	}
	if size < -1 {
		return segment.Info{}, errors.New("double: bad size")
	}
	if size > d.limit() || (d.flaw == flawLimitOffByOne && size == d.limit()) {
		return segment.Info{}, segment.ErrTooLarge
	}
	var buf bytes.Buffer
	chunk := make([]byte, 32<<10)
	for {
		if d.flaw != flawIgnoreContextInPut {
			if err := ctx.Err(); err != nil {
				return segment.Info{}, err
			}
		}
		n, err := r.Read(chunk)
		if err == io.EOF && d.flaw == flawDropDataWithEOF {
			n = 0
		}
		if int64(buf.Len()+n) > d.limit() || (d.flaw == flawLimitOffByOne && int64(buf.Len()+n) == d.limit()) {
			return segment.Info{}, segment.ErrTooLarge
		}
		if d.flaw != flawIgnoreSize && size >= 0 && int64(buf.Len()+n) > size {
			return segment.Info{}, errors.New("double: reader yields more than announced")
		}
		buf.Write(chunk[:n])
		if err == io.EOF {
			break
		}
		if err != nil {
			if d.flaw == flawPartialOnReadError {
				d.store(name, buf.Bytes())
			}
			return segment.Info{}, err
		}
	}
	data := buf.Bytes()
	if d.flaw != flawIgnoreSize && size >= 0 && int64(len(data)) != size {
		return segment.Info{}, fmt.Errorf("double: reader yields %d bytes, %d announced", len(data), size)
	}
	info := segment.Info{Name: name, Size: int64(len(data)), SHA256: sha256.Sum256(data)}
	if d.flaw == flawWrongSHAOnPut {
		info.SHA256[0] ^= 1
	}

	if d.flaw == flawRacyPut && name == raceName {
		d.mu.Lock()
		_, exists := d.data[name]
		d.mu.Unlock()
		if exists {
			return segment.Info{}, segment.ErrExists
		}
		// Every contender has checked before any of them writes.
		d.gate.Done()
		d.gate.Wait()
		d.store(name, data)
		return info, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.data[name]; exists && d.flaw != flawOverwrite {
		return segment.Info{}, segment.ErrExists
	}
	d.storeLocked(name, data)
	return info, nil
}

func (d *double) store(name string, data []byte) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.storeLocked(name, data)
}

func (d *double) storeLocked(name string, data []byte) {
	if _, exists := d.data[name]; !exists {
		d.order = append(d.order, name)
	}
	d.data[name] = bytes.Clone(data)
}

func (d *double) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := d.validate(ctx, name); err != nil {
		return nil, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	data, ok := d.data[name]
	if !ok {
		return nil, segment.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(data))), nil
}

func (d *double) Stat(ctx context.Context, name string) (segment.Info, error) {
	if err := d.validate(ctx, name); err != nil {
		return segment.Info{}, err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	data, ok := d.data[name]
	if !ok {
		return segment.Info{}, segment.ErrNotFound
	}
	info := segment.Info{Name: name, Size: int64(len(data)), SHA256: sha256.Sum256(data)}
	if d.flaw == flawWrongSHAOnStat {
		info.SHA256[0] ^= 1
	}
	return info, nil
}

// List leaves SHA256 zero, as the contract allows.
func (d *double) List(ctx context.Context, prefix string) iter.Seq2[segment.Info, error] {
	return func(yield func(segment.Info, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(segment.Info{}, err)
			return
		}
		if prefix != "" {
			if err := d.validName(strings.TrimSuffix(prefix, "/")); err != nil {
				yield(segment.Info{}, err)
				return
			}
		}
		d.mu.Lock()
		names := slices.Clone(d.order)
		sizes := make(map[string]int64, len(names))
		for name, data := range d.data {
			sizes[name] = int64(len(data))
		}
		d.mu.Unlock()
		if d.flaw != flawInsertionOrder {
			slices.Sort(names)
		}
		for _, name := range names {
			if _, ok := sizes[name]; !ok || !strings.HasPrefix(name, prefix) {
				continue
			}
			if !yield(segment.Info{Name: name, Size: sizes[name]}, nil) {
				return
			}
		}
	}
}

func (d *double) Delete(ctx context.Context, name string) error {
	if err := d.validate(ctx, name); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.data[name]; !ok {
		if d.flaw == flawDeleteMissingErr {
			return segment.ErrNotFound
		}
		return nil
	}
	delete(d.data, name)
	d.order = slices.DeleteFunc(d.order, func(n string) bool { return n == name })
	return nil
}

var _ segment.Store = (*double)(nil)
