// Package localdir stores segments as files in a directory of the local file
// system. A segment named a/b/c is the file <root>/a/b/c.
//
// # Writing a segment
//
// Put never writes the final file in place. It writes the bytes to a temporary
// file in the same directory as the final name, flushes the file to disk
// (fsync), and then makes the final name a hard link to it. Linking fails when
// the name exists, so the create-if-absent check and the creation are one
// atomic step: two writers of one name cannot both succeed, and an existing
// segment is never replaced (a rename would replace it silently). Put then
// flushes the directory that holds the name and every directory above it up
// to the root, so that the name itself, and any directory Put had to create
// for it, survive a crash; the walk goes all the way up because a concurrent
// Put may have created a directory on the path without having flushed its
// entry yet. Last it removes the temporary name, which leaves the final name
// as the only link. The temporary file lives next to the final name so that
// the link never crosses a file system.
//
// The order (file flushed, then linked, then directories flushed) is not
// observable without a fault-injection seam, so the package keeps the three
// operations behind a small set of hooks, and its internal tests replace them
// with recording stand-ins to check the order and the fault paths.
//
// # Size and digest
//
// Put hashes the bytes (SHA-256) as it copies them, so the digest it reports
// is of what was written. It stops as soon as more than [segment.MaxSize] bytes
// have arrived, or more than the announced size, and never takes the announced
// size as a bound. Stat reads the whole file to compute the digest, which costs
// time in proportion to its size (up to [segment.MaxSize]), so it is not for
// existence checks: List and Open are the cheap ones. Nothing is stored beside
// the segment, so the digest cannot disagree with the bytes. List does not
// compute digests and leaves them zero.
//
// # What a crash leaves
//
// At any point the final name is either absent or a complete, flushed file.
// A crash can leave a temporary file behind: its name starts with a dot, which
// no valid segment name does, so it is never listed, never opened, and never in
// the way of a Put of the same name. A crash between the link and the flush of
// the directories may or may not keep the segment. The writer was not told it
// had succeeded and writes it again; if the link did survive, that retry gets
// [segment.ErrExists]. To tell a survivor of the earlier attempt from some other
// segment of that name, Stat it and compare its SHA256 with the digest of the
// bytes that were being written. Leftover temporary files are not collected by
// this package; they are safe to delete when no Put is running.
//
// A Put whose directory flush fails reports the error. If the name still refers
// to the file this Put wrote, Put removes the name again, so that nothing is
// left visible. If the name has meanwhile been deleted and written afresh by
// someone else, Put leaves it alone and returns an error saying that the
// outcome is unknown; the caller Stats the name and compares SHA256.
//
// # Limits
//
//   - The file system must support hard links; New checks this once, with a
//     probe file in the root, and fails with a clear error if it does not.
//   - One writer per store: Delete checks that a name is a file and then
//     removes it, and a concurrent Put under that name used as a directory
//     could slip in between.
//   - One host: nothing is locked between processes, and a root on a network
//     file system gets whatever that file system gives.
//   - Names are lower case only, so that two names never differ only in case:
//     the default file systems of macOS and Windows would treat them as one.
//   - A directory cannot also be a file, so a store cannot hold both "a" and
//     "a/b". Put refuses the second of such a pair with an error that is
//     neither [segment.ErrExists] nor [segment.ErrInvalidName]; the error is
//     opaque, and callers should treat it as a failure to store the segment.
//   - The root plus the longest name (512 bytes and the temporary name's
//     extra 22 bytes) must fit the operating system's path limit (PATH_MAX,
//     4096 on Linux and 1024 on macOS); a longer path fails with the system's
//     "file name too long" error rather than a package error.
//
// # The root
//
// All access goes through [os.Root], so a name cannot reach outside the root:
// a symbolic link inside the root that points outside it is refused. Whoever
// can write inside the root is still trusted to keep it consistent: a
// symbolic link inside the root that stays inside it makes two names reach one
// file. List does not follow symbolic links and does not list them; Open,
// Stat and Delete refuse a name whose last element is one. A Dir has no Close;
// the root's descriptor is released when the Dir is collected.
package localdir

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"iter"
	"math/rand/v2"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"slices"
	"strings"

	"github.com/lotannauo/toposhift/internal/segment"
)

// chunkSize is how much Put copies between checks of the context.
const chunkSize = 64 << 10

var _ segment.Store = (*Dir)(nil)

// errConflict is returned by Put for a name that would need a file and a
// directory of the same path. It is opaque: it has no exported sentinel.
var errConflict = errors.New("name conflicts with another segment's directory or file")

// errOutcomeUnknown is returned by Put when the directory flush failed and the
// name no longer refers to the file Put wrote.
var errOutcomeUnknown = errors.New("outcome unknown: the name now holds a segment that Put did not verify to be its own; Stat it and compare")

// hooks are the three operations whose order makes a segment durable. The
// internal tests replace them to record the order and to inject faults.
type hooks struct {
	syncFile func(f *os.File) error
	syncDir  func(root *os.Root, dir string) error
	link     func(root *os.Root, oldname, newname string) error
}

func defaultHooks() hooks {
	return hooks{
		syncFile: (*os.File).Sync,
		syncDir:  syncDir,
		link:     (*os.Root).Link,
	}
}

// Dir is a [segment.Store] in a directory of the local file system: a segment
// named a/b/c is the file <root>/a/b/c. A Dir is safe for concurrent use.
type Dir struct {
	root    *os.Root
	hooks   hooks
	maxSize int64
}

// New opens the store at root, creating the directory (0o755) if needed, and
// the directories above it that do not exist, flushing the parents it made so
// that they survive a crash. root must be an absolute path. It fails if the file
// system does not support hard links.
func New(root string) (*Dir, error) { return newDir(root, defaultHooks()) }

func newDir(root string, h hooks) (*Dir, error) {
	if !filepath.IsAbs(root) {
		return nil, fmt.Errorf("localdir: root %q is not an absolute path", root)
	}
	root = filepath.Clean(root)

	// The nearest ancestor that exists already; everything below it is ours to
	// create, and its entry in its parent is ours to flush.
	existing := root
	for {
		if _, err := os.Stat(existing); err == nil {
			break
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			break
		}
		existing = parent
	}
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, fmt.Errorf("localdir: create root: %w", err)
	}
	if existing != root {
		if err := syncCreated(existing, root, h); err != nil {
			return nil, fmt.Errorf("localdir: flush the directories created for the root: %w", err)
		}
	}

	r, err := os.OpenRoot(root)
	if err != nil {
		return nil, fmt.Errorf("localdir: open root: %w", err)
	}
	if err := probeLinks(r, h); err != nil {
		_ = r.Close()
		return nil, fmt.Errorf("localdir: root %q: %w", root, err)
	}
	return &Dir{root: r, hooks: h, maxSize: segment.MaxSize}, nil
}

// syncCreated flushes the parent of every directory from existing (exclusive)
// down to root (inclusive), nearest first, so that each new entry is durable.
func syncCreated(existing, root string, h hooks) error {
	rel, err := filepath.Rel(existing, root)
	if err != nil {
		return err
	}
	er, err := os.OpenRoot(existing)
	if err != nil {
		return err
	}
	defer er.Close()
	for dir := path.Dir(filepath.ToSlash(rel)); ; dir = path.Dir(dir) {
		if err := h.syncDir(er, local(dir)); err != nil {
			return err
		}
		if dir == "." {
			return nil
		}
	}
}

// errNoHardLinks is returned by New for a file system on which a file cannot be
// given a second name.
var errNoHardLinks = errors.New("file system lacks hard links, which Put needs to create a segment only if its name is free")

// probeLinks checks once that the root's file system supports hard links, with
// a hidden file that it removes again. A root it cannot write a file in is not
// probed: Put will report the real error.
func probeLinks(r *os.Root, h hooks) error {
	id := rand.Uint64()
	first, second := fmt.Sprintf(".probe-%016x", id), fmt.Sprintf(".probe-%016x-link", id)
	f, err := r.OpenFile(first, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil
	}
	_ = f.Close()
	defer func() {
		_ = r.Remove(first)
		_ = r.Remove(second)
	}()
	if err := h.link(r, first, second); err != nil {
		return fmt.Errorf("%w: %w", errNoHardLinks, err)
	}
	return nil
}

// local converts a segment name or a slash path to the form [os.Root] takes.
func local(name string) string { return filepath.FromSlash(name) }

// check is the start of every method: the context, then the name.
func check(ctx context.Context, name string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("localdir: %w", err)
	}
	return segment.ValidName(name)
}

// Put implements [segment.Store].
func (d *Dir) Put(ctx context.Context, name string, r io.Reader, size int64) (segment.Info, error) {
	if err := check(ctx, name); err != nil {
		return segment.Info{}, err
	}
	switch {
	case size < -1:
		return segment.Info{}, fmt.Errorf("localdir: put %s: size %d is not a size or -1", name, size)
	case size > d.maxSize:
		return segment.Info{}, fmt.Errorf("localdir: put %s: %d bytes: %w", name, size, segment.ErrTooLarge)
	}
	dir, base := path.Dir(name), path.Base(name)
	if dir != "." {
		if err := d.root.MkdirAll(local(dir), 0o755); err != nil {
			if d.missing(name, err) {
				return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, errConflict)
			}
			return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, err)
		}
	}
	tmp, sum, written, err := d.writeTemp(ctx, dir, base, r, size)
	if err != nil {
		return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, err)
	}
	// Whatever happens, the temporary name must go. After the link it is the
	// segment's second name, and removing it leaves one.
	defer func() { _ = d.root.Remove(local(tmp)) }()

	if err := d.hooks.link(d.root, local(tmp), local(name)); err != nil {
		if errors.Is(err, fs.ErrExist) {
			if info, serr := d.root.Stat(local(name)); serr == nil && info.IsDir() {
				return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, errConflict)
			}
			return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, segment.ErrExists)
		}
		return segment.Info{}, fmt.Errorf("localdir: put %s: %w", name, err)
	}
	if err := d.syncChain(dir); err != nil {
		return segment.Info{}, d.afterFailedFlush(name, tmp, err)
	}
	return segment.Info{Name: name, Size: written, SHA256: sum}, nil
}

// syncChain flushes dir and every directory above it up to the root, nearest
// first.
func (d *Dir) syncChain(dir string) error {
	for {
		if err := d.hooks.syncDir(d.root, local(dir)); err != nil {
			return err
		}
		if dir == "." {
			return nil
		}
		dir = path.Dir(dir)
	}
}

// afterFailedFlush decides what to leave when the directories could not be
// flushed after the link. The name is removed only if it is still the file this
// Put linked; it may have been deleted and written again by someone else.
func (d *Dir) afterFailedFlush(name, tmp string, flushErr error) error {
	final, ferr := d.root.Lstat(local(name))
	mine, merr := d.root.Lstat(local(tmp))
	if ferr == nil && merr == nil && os.SameFile(final, mine) {
		_ = d.root.Remove(local(name))
		return fmt.Errorf("localdir: put %s: %w", name, flushErr)
	}
	return fmt.Errorf("localdir: put %s: %w: %w", name, errOutcomeUnknown, flushErr)
}

// writeTemp copies r into a new temporary file in dir and flushes it. It returns
// the temporary file's slash path, the SHA-256 of what was written and the
// number of bytes. It fails with [segment.ErrTooLarge] as soon as more than the
// limit has been read, whatever size says, and with an error when size is known
// and r yields a different number of bytes. On error it leaves no file behind.
func (d *Dir) writeTemp(ctx context.Context, dir, base string, r io.Reader, size int64) (_ string, sum [32]byte, written int64, err error) {
	var (
		f   *os.File
		tmp string
	)
	for range 16 {
		tmp = path.Join(dir, fmt.Sprintf(".%s.tmp-%016x", base, rand.Uint64()))
		f, err = d.root.OpenFile(local(tmp), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
		if !errors.Is(err, fs.ErrExist) {
			break
		}
	}
	if err != nil {
		return "", sum, 0, err
	}
	defer func() {
		if err != nil {
			_ = f.Close()
			_ = d.root.Remove(local(tmp))
		}
	}()
	// The umask must not decide the mode of a segment.
	if err := f.Chmod(0o644); err != nil {
		return "", sum, 0, err
	}
	hash := sha256.New()
	buf := make([]byte, chunkSize)
	for {
		if err := ctx.Err(); err != nil {
			return "", sum, 0, err
		}
		n, rerr := r.Read(buf)
		if n > 0 {
			// Never trust size: it is checked against what arrives, and the
			// limit holds whatever it says.
			if size >= 0 && written+int64(n) > size {
				return "", sum, 0, fmt.Errorf("reader yields more than the %d bytes announced", size)
			}
			if written+int64(n) > d.maxSize {
				return "", sum, 0, fmt.Errorf("more than %d bytes: %w", d.maxSize, segment.ErrTooLarge)
			}
			if _, err := f.Write(buf[:n]); err != nil {
				return "", sum, 0, err
			}
			hash.Write(buf[:n])
			written += int64(n)
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return "", sum, 0, fmt.Errorf("read: %w", rerr)
		}
	}
	if size >= 0 && written != size {
		return "", sum, 0, fmt.Errorf("reader yields %d bytes, %d announced", written, size)
	}
	// A context that ended while the last chunk was read means the writer no
	// longer wants the segment.
	if err := ctx.Err(); err != nil {
		return "", sum, 0, err
	}
	if err := d.hooks.syncFile(f); err != nil {
		return "", sum, 0, err
	}
	if err := f.Close(); err != nil {
		return "", sum, 0, err
	}
	hash.Sum(sum[:0])
	return tmp, sum, written, nil
}

// syncDir flushes a directory of root so that the names in it survive a crash.
func syncDir(root *os.Root, dir string) error {
	if runtime.GOOS == "windows" {
		// Windows cannot flush a directory handle; its metadata is journaled.
		return nil
	}
	f, err := root.Open(dir)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

// regular reports whether info describes a regular file, the only thing that
// is a segment.
func regular(info fs.FileInfo) bool { return info.Mode().IsRegular() }

// missing reports whether err means that no segment is at name: the path is
// absent, or an element above it is a file rather than a directory.
func (d *Dir) missing(name string, err error) bool {
	if errors.Is(err, fs.ErrNotExist) {
		return true
	}
	// A segment "a" and a lookup of "a/b" fail with ENOTDIR, which is not
	// fs.ErrNotExist. Look for a file where a directory should be, rather than
	// naming the error number, which differs between systems.
	for dir := path.Dir(name); dir != "."; dir = path.Dir(dir) {
		info, serr := d.root.Stat(local(dir))
		if serr != nil {
			continue
		}
		return !info.IsDir()
	}
	return false
}

// Open implements [segment.Store].
func (d *Dir) Open(ctx context.Context, name string) (io.ReadCloser, error) {
	if err := check(ctx, name); err != nil {
		return nil, err
	}
	if _, err := d.stat(name); err != nil {
		return nil, fmt.Errorf("localdir: open %s: %w", name, err)
	}
	f, err := d.root.Open(local(name))
	if err != nil {
		if d.missing(name, err) {
			return nil, fmt.Errorf("localdir: open %s: %w", name, segment.ErrNotFound)
		}
		return nil, fmt.Errorf("localdir: open %s: %w", name, err)
	}
	info, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("localdir: open %s: %w", name, err)
	}
	if !regular(info) {
		_ = f.Close()
		return nil, fmt.Errorf("localdir: open %s: %w", name, segment.ErrNotFound)
	}
	return f, nil
}

// Stat implements [segment.Store]. It reads the whole segment to compute its
// digest: the cost grows with the size, up to [segment.MaxSize], so use List or
// Open to find out whether a segment exists.
func (d *Dir) Stat(ctx context.Context, name string) (segment.Info, error) {
	if err := check(ctx, name); err != nil {
		return segment.Info{}, err
	}
	info, err := d.stat(name)
	if err != nil {
		return segment.Info{}, fmt.Errorf("localdir: stat %s: %w", name, err)
	}
	// The digest is not stored beside the segment: it is read from the bytes,
	// so it cannot disagree with them.
	f, err := d.root.Open(local(name))
	if err != nil {
		if d.missing(name, err) {
			return segment.Info{}, fmt.Errorf("localdir: stat %s: %w", name, segment.ErrNotFound)
		}
		return segment.Info{}, fmt.Errorf("localdir: stat %s: %w", name, err)
	}
	defer f.Close()
	hash := sha256.New()
	buf := make([]byte, chunkSize)
	var n int64
	for {
		if err := ctx.Err(); err != nil {
			return segment.Info{}, fmt.Errorf("localdir: stat %s: %w", name, err)
		}
		m, rerr := f.Read(buf)
		hash.Write(buf[:m])
		n += int64(m)
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return segment.Info{}, fmt.Errorf("localdir: stat %s: %w", name, rerr)
		}
	}
	info.Size = n
	hash.Sum(info.SHA256[:0])
	return info, nil
}

// stat returns segment.ErrNotFound itself, unwrapped, when there is no segment.
// A symbolic link, a directory and anything else that is not a regular file is
// not a segment.
func (d *Dir) stat(name string) (segment.Info, error) {
	info, err := d.root.Lstat(local(name))
	if err != nil {
		if d.missing(name, err) {
			return segment.Info{}, segment.ErrNotFound
		}
		return segment.Info{}, err
	}
	if !regular(info) {
		return segment.Info{}, segment.ErrNotFound
	}
	return segment.Info{Name: name, Size: info.Size()}, nil
}

// Delete implements [segment.Store]. A name that holds no segment is not an
// error, and a directory is not a segment, so Delete does not remove one; this
// holds with one writer per store. It leaves the directories above the segment
// in place, empty or not.
func (d *Dir) Delete(ctx context.Context, name string) error {
	if err := check(ctx, name); err != nil {
		return err
	}
	// Remove also removes an empty directory, which is not a segment.
	if _, err := d.stat(name); err != nil {
		if errors.Is(err, segment.ErrNotFound) {
			return nil
		}
		return fmt.Errorf("localdir: delete %s: %w", name, err)
	}
	if err := d.root.Remove(local(name)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("localdir: delete %s: %w", name, err)
	}
	if err := d.hooks.syncDir(d.root, local(path.Dir(name))); err != nil {
		return fmt.Errorf("localdir: delete %s: %w", name, err)
	}
	return nil
}

// List implements [segment.Store]. It reads the whole listing before it yields
// the first segment, so a segment written or deleted while it runs may or may
// not appear, but every segment it yields was whole when it was read.
func (d *Dir) List(ctx context.Context, prefix string) iter.Seq2[segment.Info, error] {
	return func(yield func(segment.Info, error) bool) {
		if err := ctx.Err(); err != nil {
			yield(segment.Info{}, fmt.Errorf("localdir: list: %w", err))
			return
		}
		if err := validPrefix(prefix); err != nil {
			yield(segment.Info{}, err)
			return
		}
		infos, err := d.collect(ctx, prefix)
		if err != nil {
			yield(segment.Info{}, fmt.Errorf("localdir: list: %w", err))
			return
		}
		for _, info := range infos {
			if err := ctx.Err(); err != nil {
				yield(segment.Info{}, fmt.Errorf("localdir: list: %w", err))
				return
			}
			if !yield(info, nil) {
				return
			}
		}
	}
}

// validPrefix applies the rules of a name to a prefix, which may also be
// empty or end in one slash.
func validPrefix(prefix string) error {
	if prefix == "" {
		return nil
	}
	return segment.ValidName(strings.TrimSuffix(prefix, "/"))
}

// collect returns the segments whose names start with prefix, sorted by name.
func (d *Dir) collect(ctx context.Context, prefix string) ([]segment.Info, error) {
	var infos []segment.Info
	err := fs.WalkDir(d.root.FS(), ".", func(p string, entry fs.DirEntry, err error) error {
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) && p != "." {
				return nil // removed while the walk ran
			}
			return err
		}
		if cerr := ctx.Err(); cerr != nil {
			return cerr
		}
		if p == "." {
			return nil
		}
		hidden := strings.HasPrefix(entry.Name(), ".")
		if entry.IsDir() {
			if hidden {
				return fs.SkipDir
			}
			return nil
		}
		if hidden {
			return nil
		}
		// A file that no valid name leads to is not a segment; it came from
		// somewhere else.
		if segment.ValidName(p) != nil || !strings.HasPrefix(p, prefix) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				return nil
			}
			return err
		}
		if !regular(info) {
			return nil
		}
		infos = append(infos, segment.Info{Name: p, Size: info.Size()})
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(infos, func(a, b segment.Info) int { return cmp.Compare(a.Name, b.Name) })
	return infos, nil
}
