package datadir

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Layout is the layout version this build writes and reads.
const Layout = 1

// dirMode is the mode of every directory this package creates. It is applied
// after creation too, so a umask cannot widen or narrow it.
const dirMode fs.FileMode = 0o700

const (
	fileMode     fs.FileMode = 0o600
	markerPrefix             = "toposhift data directory, layout "
)

// The parts of a data directory, and the files in meta/.
const (
	storeName  = "store"
	busName    = "bus"
	metaName   = "meta"
	formatName = "format"
	lockName   = "lock"
	holderName = "holder"
)

// Dir is an open data directory, held by this process until [Dir.Close].
type Dir struct {
	// Path is the directory, as resolved.
	Path string
	// Store, Bus and Meta are the directories of the parts.
	Store, Bus, Meta string
	// Warnings are sentences to log once at boot. None of them refuses
	// anything.
	Warnings []string

	lock *os.File
}

// holder is the record in meta/holder of the instance that holds the lock.
type holder struct {
	PID     int    `json:"pid"`
	Host    string `json:"host"`
	Started string `json:"started"` // RFC 3339
}

// hooks are what Open asks of the machine, so tests can answer for it.
type hooks struct {
	networkFS func(path string) (string, error)
	tempRoots func() []string
	now       func() time.Time
}

// Open creates the missing directories of r.Dir with mode 0700, checks the
// layout marker, takes the lock that keeps a second instance out, and writes
// the holder record. It never changes the mode or owner of a directory that
// exists.
//
// A directory that has entries and no marker is refused, as is a marker of a
// newer layout. When another instance holds the directory, the error names it.
// A failure to create or lock names the directory, its owner uid and the
// running uid.
func Open(r Result) (*Dir, error) {
	return open(r, hooks{networkFS: networkFS, tempRoots: tempRoots, now: time.Now})
}

func open(r Result, h hooks) (_ *Dir, err error) {
	dir := r.Dir
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("data directory %q is not an absolute path", dir)
	}
	if err := mkdirAll(dir); err != nil {
		return nil, accessError(dir, err)
	}
	if err := checkLayout(dir, false); err != nil {
		return nil, err
	}

	d := &Dir{
		Path:  dir,
		Store: filepath.Join(dir, storeName),
		Bus:   filepath.Join(dir, busName),
		Meta:  filepath.Join(dir, metaName),
	}
	for _, p := range []string{d.Store, d.Bus, d.Meta} {
		if err := mkdirOne(p); err != nil {
			return nil, accessError(dir, err)
		}
	}

	lock, err := os.OpenFile(filepath.Join(d.Meta, lockName), os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return nil, accessError(dir, err)
	}
	defer func() {
		if err != nil {
			_ = lock.Close() // closing releases the lock
		}
	}()
	if err := flock(lock); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, inUseError(dir, d.Meta)
		}
		return nil, accessError(dir, fmt.Errorf("lock %s: %w", lock.Name(), err))
	}
	d.lock = lock

	// Under the lock, an instance that finished initialising a moment ago has
	// written the marker already; otherwise this one writes it, last.
	if err := checkLayout(dir, true); err != nil {
		return nil, err
	}
	rec, err := json.Marshal(holder{PID: os.Getpid(), Host: hostname(), Started: h.now().UTC().Format(time.RFC3339)})
	if err != nil {
		return nil, err
	}
	if err := writeFileAtomic(filepath.Join(d.Meta, holderName), append(rec, '\n')); err != nil {
		return nil, accessError(dir, err)
	}

	d.Warnings = warnings(dir, h)
	return d, nil
}

// Close releases the lock. It is safe to call more than once.
func (d *Dir) Close() error {
	if d == nil || d.lock == nil {
		return nil
	}
	f := d.lock
	d.lock = nil
	unlock(f)
	return f.Close()
}

// checkLayout refuses a directory that is not ours or is of a newer layout.
// With write set, and the directory empty of a marker, it writes the marker.
func checkLayout(dir string, write bool) error {
	layout, found, err := readMarker(dir)
	if err != nil {
		return err
	}
	if found {
		if layout > Layout {
			return fmt.Errorf("data directory %s has layout %d, which is newer than layout %d, the newest this toposhift reads",
				dir, layout, Layout)
		}
		return nil
	}
	blank, err := isBlank(dir)
	if err != nil {
		return accessError(dir, err)
	}
	if !blank {
		return fmt.Errorf("%s is not empty and is not a toposhift data directory", dir)
	}
	if !write {
		return nil
	}
	text := markerPrefix + strconv.Itoa(Layout) + "\n"
	if err := writeFileAtomic(filepath.Join(dir, metaName, formatName), []byte(text)); err != nil {
		return accessError(dir, err)
	}
	return nil
}

// readMarker returns the layout version in meta/format.
func readMarker(dir string) (layout int, found bool, err error) {
	path := filepath.Join(dir, metaName, formatName)
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENOTDIR) {
			return 0, false, nil
		}
		return 0, false, accessError(dir, err)
	}
	s, ok := strings.CutPrefix(strings.TrimSpace(string(b)), markerPrefix)
	n, convErr := strconv.Atoi(s)
	if !ok || convErr != nil || n < 1 {
		return 0, false, fmt.Errorf("%s is not a layout marker of toposhift", path)
	}
	return n, true, nil
}

// isBlank reports whether dir has nothing in it, or only what an initialisation
// that stopped before it wrote the marker leaves: the empty parts and the lock
// files. Anything else belongs to somebody.
func isBlank(dir string) (bool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false, err
	}
	for _, e := range entries {
		switch e.Name() {
		case storeName, busName:
			if !e.IsDir() || !emptyDir(filepath.Join(dir, e.Name())) {
				return false, nil
			}
		case metaName:
			if !e.IsDir() {
				return false, nil
			}
			inner, err := os.ReadDir(filepath.Join(dir, metaName))
			if err != nil {
				return false, err
			}
			for _, f := range inner {
				switch f.Name() {
				case lockName, holderName, holderName + tmpSuffix, formatName + tmpSuffix:
				default:
					return false, nil
				}
			}
		default:
			return false, nil
		}
	}
	return true, nil
}

func emptyDir(path string) bool {
	entries, err := os.ReadDir(path)
	return err == nil && len(entries) == 0
}

const tmpSuffix = ".tmp"

// writeFileAtomic replaces path with data, so a reader sees all of it or none.
func writeFileAtomic(path string, data []byte) error {
	tmp := path + tmpSuffix
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// mkdirAll creates dir and its missing parents, each with mode 0700. A parent
// that exists is left as it is.
func mkdirAll(dir string) error {
	var missing []string
	for p := dir; ; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err == nil {
			if !info.IsDir() {
				return fmt.Errorf("%s is not a directory", p)
			}
			break
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		missing = append(missing, p)
		if filepath.Dir(p) == p {
			break
		}
	}
	for i := len(missing) - 1; i >= 0; i-- {
		if err := mkdirOne(missing[i]); err != nil {
			return err
		}
	}
	return nil
}

// mkdirOne creates p with mode 0700 whatever the umask. A directory that
// exists already is left alone.
func mkdirOne(p string) error {
	err := os.Mkdir(p, dirMode)
	switch {
	case err == nil:
		// Through the directory opened right after it was made, which narrows
		// the window between Mkdir and Chmod; a symlink swapped in at the name
		// in that window would still be followed (no O_NOFOLLOW, for
		// portability). Only a parent writable by other users allows such a
		// swap.
		f, openErr := os.Open(p)
		if openErr != nil {
			return openErr
		}
		chmodErr := f.Chmod(dirMode)
		if closeErr := f.Close(); chmodErr == nil {
			chmodErr = closeErr
		}
		return chmodErr
	case errors.Is(err, fs.ErrExist):
		info, statErr := os.Stat(p)
		if statErr != nil || !info.IsDir() {
			return fmt.Errorf("%s exists and is not a directory", p)
		}
		return nil
	default:
		return err
	}
}

// inUseError names the instance that holds the directory.
func inUseError(dir, meta string) error {
	var h holder
	b, err := os.ReadFile(filepath.Join(meta, holderName))
	if err == nil {
		err = json.Unmarshal(b, &h)
	}
	if err != nil || h.PID == 0 {
		return fmt.Errorf("data directory %s is in use by another instance (its holder record is missing or unreadable)", dir)
	}
	return fmt.Errorf("data directory %s is in use by pid %d on %s since %s", dir, h.PID, h.Host, h.Started)
}

// accessError says what failed on dir and who owns it, because a failure to
// create, lock or write is most often a question of which user runs the
// process. For a directory that does not exist yet, the owner is that of the
// deepest part that does.
func accessError(dir string, err error) error {
	owner := dir
	for {
		if _, statErr := os.Stat(owner); statErr == nil || filepath.Dir(owner) == owner {
			break
		}
		owner = filepath.Dir(owner)
	}
	who := "owner unknown"
	if uid, ok := ownerUID(owner); ok {
		who = fmt.Sprintf("%s is owned by uid %d", owner, uid)
	}
	return fmt.Errorf("data directory %s: %w (%s; running as uid %d)", dir, err, who, os.Getuid())
}

func hostname() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "unknown"
	}
	return h
}

// warnings are the conditions worth saying once at boot that do not stop
// anything.
func warnings(dir string, h hooks) []string {
	var out []string
	real := dir
	if r, err := filepath.EvalSymlinks(dir); err == nil {
		real = r
	}

	for _, root := range h.tempRoots() {
		root = filepath.Clean(root)
		if r, err := filepath.EvalSymlinks(root); err == nil {
			root = r
		}
		if root != "" && root != string(filepath.Separator) && within(real, root) {
			out = append(out, fmt.Sprintf("data directory %s is inside the temp directory %s: data may be lost at reboot", real, root))
			break
		}
	}

	if info, err := os.Stat(dir); err == nil {
		mode := info.Mode()
		if mode.Perm()&0o002 != 0 && mode&fs.ModeSticky == 0 {
			out = append(out, fmt.Sprintf("data directory %s is writable by every user and has no sticky bit: any user can replace what is in it", dir))
		}
	}

	if kind, err := h.networkFS(dir); err == nil && kind != "" {
		out = append(out, fmt.Sprintf("data directory %s is on a network filesystem (%s): file locking and durability on it are not guaranteed", dir, kind))
	}
	return out
}

// within reports whether path is root or below it.
func within(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

// tempRoots are the directories whose contents the system may delete.
func tempRoots() []string {
	roots := []string{os.TempDir(), "/tmp"}
	if runtime.GOOS == "darwin" {
		roots = append(roots, "/private/tmp", "/private/var/folders")
	}
	return roots
}
