// Package segmenttest is the suite every [segment.Store] implementation runs.
//
// An implementation calls [Run] from one of its tests, giving it a function
// that opens a fresh, empty store. Each check then runs as a subtest on its own
// store. A check fails with a message that names the property it holds the
// store to, so a failure says what is broken rather than which line.
package segmenttest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"testing/iotest"

	"github.com/lotannauo/toposhift/internal/segment"
)

// Option adjusts [Run].
type Option func(*config)

type config struct{ limit int64 }

// WithMaxSize tells Run that the store under test accepts at most n bytes per
// segment instead of [segment.MaxSize], so that a fake need not move 256 MiB
// through memory. n must be at least 4 MiB, which the other checks use.
func WithMaxSize(n int64) Option {
	return func(c *config) { c.limit = n }
}

// Run runs every check as a subtest on a store opened by open, each fresh. The
// subtests run in parallel, so open must give each call a store of its own (for
// a directory, one under t.TempDir()).
func Run(t *testing.T, open func(t *testing.T) segment.Store, opts ...Option) {
	t.Helper()
	cfg := config{limit: segment.MaxSize}
	for _, o := range opts {
		o(&cfg)
	}
	if cfg.limit < 4<<20 {
		t.Fatalf("segmenttest: the size limit %d is below the 4 MiB the checks need", cfg.limit)
	}
	for _, c := range checksFor(cfg.limit) {
		t.Run(c.name, func(t *testing.T) {
			// Each check has a store of its own.
			t.Parallel()
			if err := c.run(open(t)); err != nil {
				t.Error(err)
			}
		})
	}
}

// A check holds a store to one property and returns an error that names it.
type check struct {
	name string
	run  func(segment.Store) error
}

// checksFor returns the checks for a store whose segment size limit is limit.
func checksFor(limit int64) []check {
	return []check{
		{"round_trip", checkRoundTrip},
		{"immutability", checkImmutability},
		{"not_found", checkNotFound},
		{"names", checkNames},
		{"list", checkList},
		{"atomicity", checkAtomicity},
		{"context", checkContext},
		{"concurrency", checkConcurrency},
		{"size", func(s segment.Store) error { return checkSize(s, limit) }},
		{"digest", checkDigest},
	}
}

var bg = context.Background()

// The segment name that concurrent writers fight over, and how many of them.
const (
	raceName   = "race/one"
	contenders = 8
)

// payload returns n deterministic pseudo-random bytes, different for each seed.
func payload(seed byte, n int) []byte {
	var key [32]byte
	key[0] = seed
	b := make([]byte, n)
	_, _ = rand.NewChaCha8(key).Read(b) // never fails
	return b
}

func put(s segment.Store, name string, data []byte) error {
	return putFrom(s, name, bytes.NewReader(data), data)
}

// putFrom puts the bytes r yields, which must be data, announcing their size.
func putFrom(s segment.Store, name string, r io.Reader, data []byte) error {
	info, err := s.Put(bg, name, r, int64(len(data)))
	if err != nil {
		return fmt.Errorf("Put(%q): %w", name, err)
	}
	if info.Name != name || info.Size != int64(len(data)) {
		return fmt.Errorf("Put(%q) returned %+v, want name %q and size %d", name, info, name, len(data))
	}
	return nil
}

func read(s segment.Store, name string) ([]byte, error) {
	rc, err := s.Open(bg, name)
	if err != nil {
		return nil, fmt.Errorf("Open(%q): %w", name, err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("read of %q: %w", name, err)
	}
	return data, nil
}

func list(s segment.Store, prefix string) ([]segment.Info, error) {
	var infos []segment.Info
	for info, err := range s.List(bg, prefix) {
		if err != nil {
			return nil, fmt.Errorf("List(%q): %w", prefix, err)
		}
		infos = append(infos, info)
	}
	return infos, nil
}

func listNames(s segment.Store, prefix string) ([]string, error) {
	infos, err := list(s, prefix)
	if err != nil {
		return nil, err
	}
	names := make([]string, len(infos))
	for i, info := range infos {
		names[i] = info.Name
	}
	return names, nil
}

func sameNames(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// errString shows an error that may be nil, for messages about what a store
// returned.
func errString(err error) string {
	if err == nil {
		return "<nil>"
	}
	return err.Error()
}

// expect fails with property's name unless err wraps want.
func expect(property, what string, err, want error) error {
	if !errors.Is(err, want) {
		return fmt.Errorf("%s: %s returned %v, want an error wrapping %s", property, what, errString(err), want.Error())
	}
	return nil
}

// expectAbsent checks that nothing is stored under name.
func expectAbsent(property string, s segment.Store, name string) error {
	if _, err := s.Stat(bg, name); !errors.Is(err, segment.ErrNotFound) {
		return fmt.Errorf("%s: Stat(%q) returned %v, want ErrNotFound", property, name, errString(err))
	}
	names, err := listNames(s, "")
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	for _, n := range names {
		if n == name {
			return fmt.Errorf("%s: %q is listed", property, name)
		}
	}
	return nil
}

// nameOfLen builds a name of exactly n bytes whose elements are all valid
// (1 to 100 bytes), so that only its length can make it invalid.
func nameOfLen(n int) string {
	unit := strings.Repeat("x", 100) + "/"
	b := []byte(strings.Repeat(unit, n/len(unit)+1)[:n])
	if b[n-1] == '/' {
		b[n-1] = 'x'
	}
	return string(b)
}

// checkRoundTrip: what is put is what is read, at the sizes that matter.
func checkRoundTrip(s segment.Store) error {
	const property = "round trip"
	cases := []struct {
		name string
		data []byte
	}{
		{"empty", nil},
		{"one-byte", []byte{0x7f}},
		{"big/three-mib", payload(1, 3<<20)},
		{"a/b/c", payload(2, 1000)},
		{"year/2026/segment-0001.log", payload(3, 70<<10)},
	}
	for _, c := range cases {
		if err := put(s, c.name, c.data); err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
	}
	// Readers that hand the bytes over in other ways than one big read: the
	// last bytes together with io.EOF, a byte at a time, half of what was asked.
	// The size is not announced for the last, which also covers -1.
	readers := []struct {
		name string
		data []byte
		wrap func(io.Reader) io.Reader
	}{
		{"readers/data-and-eof", payload(8, 150<<10+17), iotest.DataErrReader},
		{"readers/one-byte", payload(9, 3000), iotest.OneByteReader},
		{"readers/half", payload(10, 300<<10+1), iotest.HalfReader},
	}
	for _, c := range readers {
		if err := putFrom(s, c.name, c.wrap(bytes.NewReader(c.data)), c.data); err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
		cases = append(cases, struct {
			name string
			data []byte
		}{c.name, c.data})
	}
	unknown := payload(11, 200<<10+3)
	if _, err := s.Put(bg, "readers/unknown-size", iotest.DataErrReader(bytes.NewReader(unknown)), -1); err != nil {
		return fmt.Errorf("%s: Put with size -1: %w", property, err)
	}
	cases = append(cases, struct {
		name string
		data []byte
	}{"readers/unknown-size", unknown})
	for _, c := range cases {
		info, err := s.Stat(bg, c.name)
		if err != nil {
			return fmt.Errorf("%s: Stat(%q): %w", property, c.name, err)
		}
		if info.Name != c.name || info.Size != int64(len(c.data)) {
			return fmt.Errorf("%s: Stat(%q) = %+v, want size %d", property, c.name, info, len(c.data))
		}
		got, err := read(s, c.name)
		if err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
		if !bytes.Equal(got, c.data) {
			return fmt.Errorf("%s: %q read back %d bytes that differ from the %d written", property, c.name, len(got), len(c.data))
		}
	}
	return nil
}

// checkImmutability: a segment is never replaced.
func checkImmutability(s segment.Store) error {
	const property = "immutability"
	first, second := payload(4, 5000), payload(5, 7000)
	if err := put(s, "seg/one", first); err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	_, err := s.Put(bg, "seg/one", bytes.NewReader(second), int64(len(second)))
	if err := expect(property, "a second Put of the name", err, segment.ErrExists); err != nil {
		return err
	}
	info, err := s.Stat(bg, "seg/one")
	if err != nil {
		return fmt.Errorf("%s: Stat after the refused Put: %w", property, err)
	}
	if info.Size != int64(len(first)) {
		return fmt.Errorf("%s: size is %d after the refused Put, want the first segment's %d", property, info.Size, len(first))
	}
	got, err := read(s, "seg/one")
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	if !bytes.Equal(got, first) {
		return fmt.Errorf("%s: the stored bytes are not the first segment's", property)
	}
	// A refused empty Put must not replace a non-empty segment either.
	_, err = s.Put(bg, "seg/one", bytes.NewReader(nil), 0)
	return expect(property, "a Put of an empty segment over the name", err, segment.ErrExists)
}

// checkNotFound: a missing name is reported as such, except to Delete.
func checkNotFound(s segment.Store) error {
	const property = "not found"
	if err := put(s, "present/x", []byte("x")); err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	for _, name := range []string{"missing", "present/y", "present", "other/x", "present/x/deeper"} {
		rc, err := s.Open(bg, name)
		if rc != nil {
			rc.Close()
		}
		if err := expect(property, fmt.Sprintf("Open(%q)", name), err, segment.ErrNotFound); err != nil {
			return err
		}
		_, err = s.Stat(bg, name)
		if err := expect(property, fmt.Sprintf("Stat(%q)", name), err, segment.ErrNotFound); err != nil {
			return err
		}
		// Removing what is not there is not an error, however often.
		for range 2 {
			if err := s.Delete(bg, name); err != nil {
				return fmt.Errorf("%s: Delete(%q) of a missing name: %w", property, name, err)
			}
		}
	}
	if _, err := s.Stat(bg, "present/x"); err != nil {
		return fmt.Errorf("%s: Delete of missing names removed a stored segment: %w", property, err)
	}
	// A Delete of a segment, and of it again.
	if err := s.Delete(bg, "present/x"); err != nil {
		return fmt.Errorf("%s: Delete of a stored segment: %w", property, err)
	}
	if err := s.Delete(bg, "present/x"); err != nil {
		return fmt.Errorf("%s: a second Delete of the name: %w", property, err)
	}
	_, err := s.Stat(bg, "present/x")
	return expect(property, "Stat after Delete", err, segment.ErrNotFound)
}

// checkNames: the name rules hold on every method, before any I/O.
func checkNames(s segment.Store) error {
	const property = "names"
	// Names that are invalid as segment names. prefixOK marks those that List
	// accepts as a prefix.
	invalid := []struct {
		name     string
		prefixOK bool
	}{
		{"", true},
		{"/a", false},
		{"a/", true},
		{"a//b", false},
		{".", false},
		{"..", false},
		{"a/../b", false},
		{".hidden", false},
		{"a/.b", false},
		{"a b", false},
		{"A", false},
		{"a/B", false},
		{"abC", false},
		{`a\b`, false},
		{strings.Repeat("a", 129), false},
		{nameOfLen(513), false},
		{"a\xffb", false},
	}
	for _, c := range invalid {
		label := fmt.Sprintf("%.40q", c.name)
		_, err := s.Put(bg, c.name, strings.NewReader("data"), 4)
		if err := expect(property, "Put of "+label, err, segment.ErrInvalidName); err != nil {
			return err
		}
		rc, err := s.Open(bg, c.name)
		if rc != nil {
			rc.Close()
		}
		if err := expect(property, "Open of "+label, err, segment.ErrInvalidName); err != nil {
			return err
		}
		_, err = s.Stat(bg, c.name)
		if err := expect(property, "Stat of "+label, err, segment.ErrInvalidName); err != nil {
			return err
		}
		err = s.Delete(bg, c.name)
		if err := expect(property, "Delete of "+label, err, segment.ErrInvalidName); err != nil {
			return err
		}
		if !c.prefixOK {
			_, err := list(s, c.name)
			if err := expect(property, "List with prefix "+label, err, segment.ErrInvalidName); err != nil {
				return err
			}
		}
	}
	if names, err := listNames(s, ""); err != nil || len(names) != 0 {
		return fmt.Errorf("%s: after only refused Puts the store lists %v (error %s), want nothing", property, names, errString(err))
	}

	valid := []string{"a", "a-b_c.d", "z9", "a./b", nameOfLen(512), strings.Repeat("e", 128)}
	for _, name := range valid {
		label := fmt.Sprintf("%.40q", name)
		if err := put(s, name, []byte(name)); err != nil {
			return fmt.Errorf("%s: valid name %s refused: %w", property, label, err)
		}
		if got, err := read(s, name); err != nil || string(got) != name {
			return fmt.Errorf("%s: valid name %s does not read back (error %s)", property, label, errString(err))
		}
	}
	return nil
}

// checkList: order, prefixes and deletion.
func checkList(s segment.Store) error {
	const property = "list"
	if names, err := listNames(s, ""); err != nil || len(names) != 0 {
		return fmt.Errorf("%s: an empty store lists %v (error %s), want nothing", property, names, errString(err))
	}
	written := []string{"a0", "a/b", "b", "a.b", "x/y/2", "x/y/1", "x/z", "a/a", "ab"}
	for i, name := range written {
		if err := put(s, name, payload(byte(10+i), 10+i)); err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
	}
	all := []string{"a.b", "a/a", "a/b", "a0", "ab", "b", "x/y/1", "x/y/2", "x/z"}
	got, err := list(s, "")
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	names := make([]string, len(got))
	for i, info := range got {
		names[i] = info.Name
		wantSize := int64(0)
		for j, w := range written {
			if w == info.Name {
				wantSize = int64(10 + j)
			}
		}
		if info.Size != wantSize {
			return fmt.Errorf("%s: %q listed with size %d, want %d", property, info.Name, info.Size, wantSize)
		}
	}
	if !sameNames(names, all) {
		return fmt.Errorf("%s: List(\"\") = %v, want %v in ascending byte order of name", property, names, all)
	}

	prefixes := []struct {
		prefix string
		want   []string
	}{
		{"a", []string{"a.b", "a/a", "a/b", "a0", "ab"}},
		{"a/", []string{"a/a", "a/b"}},
		{"a/b", []string{"a/b"}},
		{"a.", []string{"a.b"}},
		{"x", []string{"x/y/1", "x/y/2", "x/z"}},
		{"x/", []string{"x/y/1", "x/y/2", "x/z"}},
		{"x/y", []string{"x/y/1", "x/y/2"}},
		{"x/y/", []string{"x/y/1", "x/y/2"}},
		{"b", []string{"b"}},
		{"c", nil},
		{"x/q/", nil},
	}
	for _, p := range prefixes {
		got, err := listNames(s, p.prefix)
		if err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
		if !sameNames(got, p.want) {
			return fmt.Errorf("%s: List(%q) = %v, want %v", property, p.prefix, got, p.want)
		}
	}

	// Stopping early must be honoured.
	n := 0
	for _, err := range s.List(bg, "") {
		if err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
		n++
		if n == 2 {
			break
		}
	}
	if n != 2 {
		return fmt.Errorf("%s: a loop stopped after two segments saw %d", property, n)
	}

	if err := s.Delete(bg, "a/b"); err != nil {
		return fmt.Errorf("%s: Delete: %w", property, err)
	}
	got2, err := listNames(s, "a/")
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	if !sameNames(got2, []string{"a/a"}) {
		return fmt.Errorf("%s: after Delete(\"a/b\") List(\"a/\") = %v, want [a/a]", property, got2)
	}
	for _, name := range all {
		if name == "a/b" {
			continue
		}
		if err := s.Delete(bg, name); err != nil {
			return fmt.Errorf("%s: Delete(%q): %w", property, name, err)
		}
	}
	if names, err := listNames(s, ""); err != nil || len(names) != 0 {
		return fmt.Errorf("%s: after deleting everything the store lists %v (error %s)", property, names, errString(err))
	}
	return nil
}

// errBoom is the read error injected into a segment's reader.
var errBoom = errors.New("segmenttest: injected read error")

// brokenReader serves size bytes of data, then calls onLimit once and returns
// its error (or io.EOF when it is nil) in place of the rest.
type brokenReader struct {
	data    []byte
	limit   int
	read    int
	onLimit func() error
}

func (r *brokenReader) Read(p []byte) (int, error) {
	if r.read >= r.limit {
		if r.onLimit != nil {
			if err := r.onLimit(); err != nil {
				return 0, err
			}
		}
		if r.read >= len(r.data) {
			return 0, io.EOF
		}
	}
	end := min(r.read+len(p), len(r.data))
	if r.read < r.limit {
		end = min(end, r.limit)
	}
	n := copy(p, r.data[r.read:end])
	r.read += n
	return n, nil
}

// checkAtomicity: a Put that fails part way leaves nothing, and the name stays free.
func checkAtomicity(s segment.Store) error {
	const property = "atomicity"
	const total, limit = 2 << 20, 1 << 20
	data := payload(6, total)

	// A reader that fails after 1 MiB.
	failing := &brokenReader{data: data, limit: limit, onLimit: func() error { return errBoom }}
	if _, err := s.Put(bg, "torn/read-error", failing, total); err == nil {
		return fmt.Errorf("%s: Put returned nil after its reader failed", property)
	}
	if err := expectAbsent(property+" (reader error)", s, "torn/read-error"); err != nil {
		return err
	}

	// A context cancelled by the reader after 1 MiB.
	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	cancelling := &brokenReader{data: data, limit: limit, onLimit: func() error { cancel(); return nil }}
	if _, err := s.Put(ctx, "torn/cancelled", cancelling, total); err == nil {
		return fmt.Errorf("%s: Put returned nil although its context ended before the segment was complete", property)
	}
	if err := expectAbsent(property+" (cancelled context)", s, "torn/cancelled"); err != nil {
		return err
	}
	if names, err := listNames(s, ""); err != nil || len(names) != 0 {
		return fmt.Errorf("%s: the store lists %v (error %s) after failed Puts, want nothing", property, names, errString(err))
	}

	// Both names can now be written, and read back whole.
	for _, name := range []string{"torn/read-error", "torn/cancelled"} {
		if err := put(s, name, data); err != nil {
			return fmt.Errorf("%s: the name cannot be written after a failed Put: %w", property, err)
		}
		got, err := read(s, name)
		if err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
		if !bytes.Equal(got, data) {
			return fmt.Errorf("%s: %q does not read back whole after the retry", property, name)
		}
	}
	return nil
}

// checkContext: a done context stops every method and changes nothing.
func checkContext(s segment.Store) error {
	const property = "context"
	kept := payload(7, 100)
	if err := put(s, "kept", kept); err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	ctx, cancel := context.WithCancel(bg)
	cancel()

	if _, err := s.Put(ctx, "new", bytes.NewReader(kept), int64(len(kept))); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: Put with a cancelled context returned %s, want an error wrapping context.Canceled", property, errString(err))
	}
	if _, err := s.Put(ctx, "kept", bytes.NewReader(kept), int64(len(kept))); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: Put of an existing name with a cancelled context returned %s, want an error wrapping context.Canceled", property, errString(err))
	}
	rc, err := s.Open(ctx, "kept")
	if rc != nil {
		rc.Close()
	}
	if !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: Open with a cancelled context returned %s, want an error wrapping context.Canceled", property, errString(err))
	}
	if _, err := s.Stat(ctx, "kept"); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: Stat with a cancelled context returned %s, want an error wrapping context.Canceled", property, errString(err))
	}
	if err := s.Delete(ctx, "kept"); !errors.Is(err, context.Canceled) {
		return fmt.Errorf("%s: Delete with a cancelled context returned %s, want an error wrapping context.Canceled", property, errString(err))
	}
	items, listErr := 0, error(nil)
	for _, err := range s.List(ctx, "") {
		if err != nil {
			listErr = err
			break
		}
		items++
	}
	if items != 0 || !errors.Is(listErr, context.Canceled) {
		return fmt.Errorf("%s: List with a cancelled context yielded %d segments and error %s, want an error wrapping context.Canceled first", property, items, errString(listErr))
	}

	// Nothing changed.
	names, err := listNames(s, "")
	if err != nil || !sameNames(names, []string{"kept"}) {
		return fmt.Errorf("%s: after the cancelled calls the store lists %v (error %s), want [kept]", property, names, errString(err))
	}
	got, err := read(s, "kept")
	if err != nil || !bytes.Equal(got, kept) {
		return fmt.Errorf("%s: \"kept\" changed (error %s)", property, errString(err))
	}
	return nil
}

// checkConcurrency: parallel writers, listers and readers see only whole
// segments, and one name has one winner.
func checkConcurrency(s segment.Store) error {
	const property = "concurrency"
	const writers, readers = 8, 8

	// Distinct names written while others list and read.
	want := make(map[string][]byte, writers)
	for i := range writers {
		want[fmt.Sprintf("many/%d", i)] = payload(byte(20+i), 300<<10+i*1000)
	}
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		failures []error
		done     = make(chan struct{})
	)
	fail := func(err error) {
		mu.Lock()
		failures = append(failures, err)
		mu.Unlock()
	}
	var readerWG sync.WaitGroup
	for range readers {
		readerWG.Go(func() {
			for {
				stopping := false
				select {
				case <-done:
					stopping = true
				default:
				}
				infos, err := list(s, "many/")
				if err != nil {
					fail(err)
					return
				}
				for _, info := range infos {
					data, err := read(s, info.Name)
					if err != nil {
						fail(fmt.Errorf("a listed segment does not open: %w", err))
						return
					}
					if int64(len(data)) != info.Size || !bytes.Equal(data, want[info.Name]) {
						fail(fmt.Errorf("%q listed with size %d is %d bytes when read, or its bytes differ from what was written", info.Name, info.Size, len(data)))
						return
					}
				}
				if stopping {
					return
				}
			}
		})
	}
	for name, data := range want {
		wg.Go(func() {
			if err := put(s, name, data); err != nil {
				fail(err)
			}
		})
	}
	wg.Wait()
	close(done)
	readerWG.Wait()
	if len(failures) > 0 {
		return fmt.Errorf("%s: parallel Put with List and Open: %w", property, errors.Join(failures...))
	}
	names, err := listNames(s, "many/")
	if err != nil || len(names) != writers {
		return fmt.Errorf("%s: %d segments listed (error %s), want %d", property, len(names), errString(err), writers)
	}

	// One name, many writers: exactly one wins.
	results := make([]error, contenders)
	bodies := make([][]byte, contenders)
	start := make(chan struct{})
	for i := range contenders {
		bodies[i] = payload(byte(40+i), 200<<10+i)
		wg.Go(func() {
			<-start
			_, results[i] = s.Put(bg, raceName, bytes.NewReader(bodies[i]), int64(len(bodies[i])))
		})
	}
	close(start)
	wg.Wait()
	winner := -1
	for i, err := range results {
		switch {
		case err == nil && winner < 0:
			winner = i
		case err == nil:
			return fmt.Errorf("%s: concurrent Puts of one name: writers %d and %d both succeeded, want exactly one", property, winner, i)
		case !errors.Is(err, segment.ErrExists):
			return fmt.Errorf("%s: concurrent Puts of one name: writer %d got %w, want nil or ErrExists", property, i, err)
		}
	}
	if winner < 0 {
		return fmt.Errorf("%s: concurrent Puts of one name: none succeeded, want exactly one", property)
	}
	got, err := read(s, raceName)
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	if !bytes.Equal(got, bodies[winner]) {
		return fmt.Errorf("%s: the stored bytes are not the winning writer's (%d bytes stored, winner wrote %d)", property, len(got), len(bodies[winner]))
	}
	return nil
}

// zeroReader yields n zero bytes without holding them.
type zeroReader struct{ n int64 }

func (r *zeroReader) Read(p []byte) (int, error) {
	if r.n <= 0 {
		return 0, io.EOF
	}
	m := int(min(int64(len(p)), r.n))
	clear(p[:m])
	r.n -= int64(m)
	return m, nil
}

// checkSize: the announced size is checked and MaxSize holds, and a refused
// Put stores nothing and leaves the name free.
func checkSize(s segment.Store, maxSize int64) error {
	const property = "size"
	data := payload(12, 100<<10)

	refused := []struct {
		what string
		r    io.Reader
		size int64
	}{
		{"a reader that yields fewer bytes than announced", bytes.NewReader(data[:len(data)-1]), int64(len(data))},
		{"a reader that yields more bytes than announced", bytes.NewReader(data), int64(len(data) - 1)},
		{"an empty reader announced as one byte", bytes.NewReader(nil), 1},
		{"one byte announced as empty", bytes.NewReader(data[:1]), 0},
	}
	for _, c := range refused {
		if _, err := s.Put(bg, "sized/x", c.r, c.size); err == nil {
			return fmt.Errorf("%s: Put returned nil for %s", property, c.what)
		}
		if err := expectAbsent(property+" ("+c.what+")", s, "sized/x"); err != nil {
			return err
		}
	}
	if err := put(s, "sized/x", data); err != nil {
		return fmt.Errorf("%s: the name cannot be written after refused Puts: %w", property, err)
	}
	if err := s.Delete(bg, "sized/x"); err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}

	// Exactly the limit is accepted, announced and not, and its digest is of
	// what was written.
	for _, size := range []int64{maxSize, -1} {
		info, err := s.Put(bg, "sized/exact", &zeroReader{n: maxSize}, size)
		if err != nil {
			return fmt.Errorf("%s: Put of exactly the limit (%d bytes) announced as %d: %w", property, maxSize, size, err)
		}
		if info.Size != maxSize {
			return fmt.Errorf("%s: Put of exactly the limit reports size %d, want %d", property, info.Size, maxSize)
		}
		if got, err := s.Stat(bg, "sized/exact"); err != nil || got.Size != maxSize || got.SHA256 != info.SHA256 {
			return fmt.Errorf("%s: Stat of the segment of exactly the limit is %+v (error %s), want size %d and the digest Put reported", property, got, errString(err), maxSize)
		}
		if err := s.Delete(bg, "sized/exact"); err != nil {
			return fmt.Errorf("%s: %w", property, err)
		}
	}

	// More than MaxSize bytes, announced and not. The reader is never held in
	// memory, and a store that announces no bound must still stop reading.
	for _, size := range []int64{maxSize + 1, -1} {
		_, err := s.Put(bg, "sized/huge", &zeroReader{n: maxSize + 1}, size)
		if err := expect(property, fmt.Sprintf("Put of MaxSize+1 bytes announced as %d", size), err, segment.ErrTooLarge); err != nil {
			return err
		}
		if err := expectAbsent(property+" (too large)", s, "sized/huge"); err != nil {
			return err
		}
	}
	// A reader that lies about its size to stay under the bound.
	_, err := s.Put(bg, "sized/huge", &zeroReader{n: maxSize + 1}, 10)
	if err == nil {
		return fmt.Errorf("%s: Put returned nil for a reader of MaxSize+1 bytes announced as 10", property)
	}
	return expectAbsent(property+" (a lie about the size)", s, "sized/huge")
}

// checkDigest: Put and Stat report the SHA-256 of the bytes, and an uncertain
// Put is settled by retrying and comparing digests.
func checkDigest(s segment.Store) error {
	const property = "digest"
	cases := [][]byte{nil, {1}, payload(13, 70<<10+5), payload(14, 1<<20)}
	for i, data := range cases {
		name := fmt.Sprintf("sum/%d", i)
		want := sha256.Sum256(data)
		info, err := s.Put(bg, name, bytes.NewReader(data), int64(len(data)))
		if err != nil {
			return fmt.Errorf("%s: Put(%q): %w", property, name, err)
		}
		if info.SHA256 != want {
			return fmt.Errorf("%s: Put(%q) reports SHA256 %x, want %x", property, name, info.SHA256, want)
		}
		stat, err := s.Stat(bg, name)
		if err != nil {
			return fmt.Errorf("%s: Stat(%q): %w", property, name, err)
		}
		if stat.SHA256 != want {
			return fmt.Errorf("%s: Stat(%q) reports SHA256 %x, want %x", property, name, stat.SHA256, want)
		}
	}

	// The retry rule: the result of a Put is lost, the retry gets ErrExists,
	// and Stat settles whether the segment is the writer's own.
	mine, other := payload(15, 5000), payload(16, 5000)
	if _, err := s.Put(bg, "retry/seg", bytes.NewReader(mine), int64(len(mine))); err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	_, err := s.Put(bg, "retry/seg", bytes.NewReader(mine), int64(len(mine)))
	if err := expect(property, "the retry of a Put whose result was lost", err, segment.ErrExists); err != nil {
		return err
	}
	stat, err := s.Stat(bg, "retry/seg")
	if err != nil {
		return fmt.Errorf("%s: %w", property, err)
	}
	if stat.SHA256 != sha256.Sum256(mine) {
		return fmt.Errorf("%s: after the retry Stat reports a digest different from the bytes written", property)
	}
	if stat.SHA256 == sha256.Sum256(other) {
		return fmt.Errorf("%s: Stat reports the digest of other bytes", property)
	}
	return nil
}
