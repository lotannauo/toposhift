package keys

import (
	"bytes"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	// FileName is the file of keys in the directory a Store is opened on.
	FileName = "keys.json"
	// MaxKeys is the most keys the file holds.
	MaxKeys = 1000
	// MaxOverlap is the longest overlap a rotation may leave the old key.
	MaxOverlap = 365 * 24 * time.Hour

	lockName    = "keys.lock"
	fileVersion = 1
	maxFileSize = 4 << 20
	// DefaultCheckEvery is how often a Store looks for a changed file.
	DefaultCheckEvery = time.Second
)

var errLockHeld = errors.New("lock held")

// hashEqual compares two hashes in constant time. It is a variable so that a
// test can see that every stored hash is compared on every lookup.
var hashEqual = subtle.ConstantTimeCompare

// Key is what a stored key is known by. It holds neither the key nor its hash.
type Key struct {
	ID      string    `json:"id"`
	Scope   Scope     `json:"scope"`
	Created time.Time `json:"created"`
	// Expires is the instant the key stops working; zero is never.
	Expires time.Time `json:"expires,omitzero"`
	// Replaces is the id of the key a rotation replaced, which may since have
	// been revoked.
	Replaces string `json:"replaces,omitempty"`
}

// ExpiredAt reports whether the key is expired at t. The instant of expiry
// itself is expired.
func (k Key) ExpiredAt(t time.Time) bool {
	return !k.Expires.IsZero() && !t.Before(k.Expires)
}

type record struct {
	Key
	SHA256 string `json:"sha256"`
}

type document struct {
	Version int      `json:"version"`
	Keys    []record `json:"keys"`
}

// entry is a stored key in memory, with its decoded hash.
type entry struct {
	Key
	hash [32]byte
}

// Options configure a Store; the zero value is the production one.
type Options struct {
	// Now is the clock; the default is time.Now.
	Now func() time.Time
	// Rand is where random bytes come from; the default is crypto/rand.
	Rand io.Reader
	// CheckEvery is how often Authenticate looks at the file for a change; the
	// default is DefaultCheckEvery. Changes made through the same Store are
	// seen at once.
	CheckEvery time.Duration
}

// Store is the file of keys in one directory. It is safe for concurrent use,
// and any number of Stores, in any number of processes, may share a directory.
type Store struct {
	dir, path, lockPath string
	now                 func() time.Time
	rand                io.Reader
	checkEvery          time.Duration
	lockWait            time.Duration

	wmu   sync.Mutex // serializes this Store's changes; held while waiting for the file lock
	mu    sync.Mutex // guards state; never held while waiting for the file lock
	state state
}

// state is what the Store last knew of the file.
type state struct {
	loaded  bool
	entries []entry // never modified once published
	info    os.FileInfo
	err     error
	checked time.Time
}

// Open returns the Store of the keys file in dir, which must exist. The file
// itself is read when first needed and is created by the first key.
func Open(dir string, opt Options) (*Store, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("keys: %s is not a directory", dir)
	}
	s := &Store{
		dir:        dir,
		path:       filepath.Join(dir, FileName),
		lockPath:   filepath.Join(dir, lockName),
		now:        opt.Now,
		rand:       opt.Rand,
		checkEvery: opt.CheckEvery,
		lockWait:   10 * time.Second,
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.rand == nil {
		s.rand = rand.Reader
	}
	if s.checkEvery < 0 {
		return nil, errors.New("keys: CheckEvery is negative")
	}
	if s.checkEvery == 0 {
		s.checkEvery = DefaultCheckEvery
	}
	return s, nil
}

// Path is the keys file.
func (s *Store) Path() string { return s.path }

// Authenticate checks a presented key for the scope and returns what it is
// known by. The text is checked for form first ([ErrMalformed], with no file
// read); then its hash is compared with every stored hash; then expiry and
// scope. Every error is a refusal and none holds the key.
func (s *Store) Authenticate(text string, want Scope) (Key, error) {
	if _, err := Parse(text); err != nil {
		return Key{}, err
	}
	entries, err := s.current()
	if err != nil {
		return Key{}, err
	}
	sum := hashOf(text)
	var match *entry
	for i := range entries {
		// No early exit: the time taken is the same whichever key matches, and
		// when none does.
		if hashEqual(sum[:], entries[i].hash[:]) == 1 {
			match = &entries[i]
		}
	}
	if match == nil {
		return Key{}, ErrUnknown
	}
	if match.ExpiredAt(s.now()) {
		return Key{}, ErrExpired
	}
	if match.Scope != want {
		return Key{}, ErrScope
	}
	return match.Key, nil
}

// AuthenticateRequest authenticates the key in the request's Authorization
// header for the scope. A request without a Bearer key is [ErrMalformed]. The
// request's query is never read.
func (s *Store) AuthenticateRequest(r *http.Request, want Scope) (Key, error) {
	tok, ok := BearerToken(r.Header)
	if !ok {
		return Key{}, ErrMalformed
	}
	return s.Authenticate(tok, want)
}

// List returns the stored keys, in the order of the file, read afresh.
func (s *Store) List() ([]Key, error) {
	entries, _, err := load(s.path)
	if err != nil {
		return nil, err
	}
	out := make([]Key, len(entries))
	for i, e := range entries {
		out[i] = e.Key
	}
	return out, nil
}

// CreateOptions are the optional parts of a new key.
type CreateOptions struct {
	// Expires is when the key stops working; zero is never. It must be in the
	// future.
	Expires time.Time
}

// Issued is a new key: its secret, shown once, and what it is known by.
type Issued struct {
	Secret Secret
	Key    Key
}

// Create makes a key of the scope, stores its hash and returns the key.
func (s *Store) Create(scope Scope, opt CreateOptions) (Issued, error) {
	var out Issued
	err := s.mutate(func(entries []entry, now time.Time) ([]entry, error) {
		if _, ok := scope.letter(); !ok {
			return nil, fmt.Errorf("keys: unknown scope %q", scope)
		}
		if err := checkExpiry(opt.Expires, now); err != nil {
			return nil, err
		}
		if len(entries) >= MaxKeys {
			return nil, ErrFull
		}
		issued, e, err := s.mint(entries, scope, opt.Expires, "", now)
		if err != nil {
			return nil, err
		}
		out = issued
		return append(entries, e), nil
	})
	if err != nil {
		return Issued{}, err
	}
	return out, nil
}

// RotateOptions are the parts of a rotation.
type RotateOptions struct {
	// Overlap is how long the old key keeps working: from zero, which ends it
	// now, to MaxOverlap. A key that would expire sooner keeps its expiry; a
	// rotation never lengthens a life.
	Overlap time.Duration
	// Expires is when the new key stops working; zero is never.
	Expires time.Time
}

// Rotation is the result of a rotation.
type Rotation struct {
	Issued
	// Old is the replaced key as it now is, with its new expiry.
	Old Key
}

// Rotate issues a key of the same scope as the key with the id, recording that
// it replaces it, and ends the old key at the end of the overlap. It refuses an
// unknown id ([ErrNotFound]) and a key that is already expired ([ErrExpired]).
func (s *Store) Rotate(id string, opt RotateOptions) (Rotation, error) {
	var out Rotation
	err := s.mutate(func(entries []entry, now time.Time) ([]entry, error) {
		if opt.Overlap < 0 || opt.Overlap > MaxOverlap {
			return nil, fmt.Errorf("keys: the overlap must be between 0 and %s", MaxOverlap)
		}
		if err := checkExpiry(opt.Expires, now); err != nil {
			return nil, err
		}
		i := indexOf(entries, id)
		if i < 0 {
			return nil, ErrNotFound
		}
		old := entries[i]
		if old.ExpiredAt(now) {
			return nil, ErrExpired
		}
		if len(entries) >= MaxKeys {
			return nil, ErrFull
		}
		issued, e, err := s.mint(entries, old.Scope, opt.Expires, old.ID, now)
		if err != nil {
			return nil, err
		}
		end := now.Add(opt.Overlap).UTC()
		if !old.Expires.IsZero() && old.Expires.Before(end) {
			end = old.Expires
		}
		old.Expires = end
		entries[i] = old
		out = Rotation{Issued: issued, Old: old.Key}
		return append(entries, e), nil
	})
	if err != nil {
		return Rotation{}, err
	}
	return out, nil
}

// Revoke removes the key with the id, which stops working at once in this
// Store and within CheckEvery in the others. It returns [ErrNotFound] for an
// unknown id.
func (s *Store) Revoke(id string) error {
	return s.mutate(func(entries []entry, _ time.Time) ([]entry, error) {
		i := indexOf(entries, id)
		if i < 0 {
			return nil, ErrNotFound
		}
		return append(entries[:i], entries[i+1:]...), nil
	})
}

func indexOf(entries []entry, id string) int {
	for i := range entries {
		if entries[i].ID == id {
			return i
		}
	}
	return -1
}

// checkExpiry refuses an expiry that is not in the future or cannot be written
// as a JSON time.
func checkExpiry(t, now time.Time) error {
	if t.IsZero() {
		return nil
	}
	if !t.After(now) {
		return errors.New("keys: the expiry is not in the future")
	}
	if t.Year() > 9999 {
		return errors.New("keys: the expiry is beyond the year 9999")
	}
	return nil
}

// mint makes a key and its entry, with an id and hash that no entry has.
func (s *Store) mint(entries []entry, scope Scope, expires time.Time, replaces string, now time.Time) (Issued, entry, error) {
	for range 8 {
		secret, err := newSecret(scope, s.rand)
		if err != nil {
			return Issued{}, entry{}, err
		}
		var idb [6]byte
		if _, err := io.ReadFull(s.rand, idb[:]); err != nil {
			return Issued{}, entry{}, fmt.Errorf("keys: reading random bytes: %w", err)
		}
		e := entry{
			Key: Key{
				ID: "k_" + hex.EncodeToString(idb[:]), Scope: scope, Created: now.UTC(),
				Replaces: replaces,
			},
			hash: hashOf(string(secret)),
		}
		if !expires.IsZero() {
			e.Expires = expires.UTC()
		}
		if indexOf(entries, e.ID) >= 0 || hasHash(entries, e.hash) {
			continue
		}
		return Issued{Secret: secret, Key: e.Key}, e, nil
	}
	return Issued{}, entry{}, errors.New("keys: no unused id after 8 draws; is the random source broken?")
}

func hasHash(entries []entry, h [32]byte) bool {
	for i := range entries {
		if entries[i].hash == h {
			return true
		}
	}
	return false
}

// current returns the keys, reading the file again when it has changed since
// the last look and the last look was CheckEvery ago or more.
func (s *Store) current() ([]entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	st := &s.state
	if st.loaded && !now.Before(st.checked) && now.Sub(st.checked) < s.checkEvery {
		return st.entries, st.err
	}
	info, err := os.Stat(s.path)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		if !st.loaded || st.info != nil || st.err != nil {
			*st = state{loaded: true}
		}
	case err != nil:
		*st = state{loaded: true, err: fmt.Errorf("%w: %w", ErrUnavailable, err)}
	case !st.loaded || !sameFile(st.info, info):
		entries, linfo, lerr := load(s.path)
		*st = state{loaded: true, entries: entries, info: linfo, err: lerr}
	}
	st.checked = now
	return st.entries, st.err
}

// sameFile reports whether b is the file a was taken of, unchanged as far as
// the file system says: the same file (a rename over it makes a new one), the
// same modification time, size and mode.
func sameFile(a, b os.FileInfo) bool {
	return a != nil && os.SameFile(a, b) && a.ModTime().Equal(b.ModTime()) && a.Size() == b.Size() && a.Mode() == b.Mode()
}

// mutate applies fn to the file's keys under the lock and writes the result.
// fn receives a copy it may change and the time; if it fails nothing is written.
func (s *Store) mutate(fn func(entries []entry, now time.Time) ([]entry, error)) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	unlock, err := s.lockFile()
	if err != nil {
		return err
	}
	defer unlock()

	entries, _, err := load(s.path)
	if err != nil {
		return err
	}
	now := s.now()
	next, err := fn(append([]entry(nil), entries...), now)
	if err != nil {
		return err
	}
	info, err := s.write(next)
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil {
		// Look at the file again at the next check.
		s.state = state{}
		return err
	}
	s.state = state{loaded: true, entries: next, info: info, checked: now}
	return nil
}

// lockFile takes the cross-process lock and returns the function that releases
// it. The lock is on a file of its own, not on keys.json, because a rename
// replaces keys.json and a lock on the replaced file locks nothing.
func (s *Store) lockFile() (func(), error) {
	f, err := os.OpenFile(s.lockPath, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	deadline := time.Now().Add(s.lockWait)
	for {
		err := tryLock(f.Fd())
		if err == nil {
			// Closing the file releases the lock.
			return func() { _ = f.Close() }, nil
		}
		if !errors.Is(err, errLockHeld) {
			_ = f.Close()
			return nil, fmt.Errorf("keys: locking %s: %w", s.lockPath, err)
		}
		if time.Now().After(deadline) {
			_ = f.Close()
			return nil, ErrLocked
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// write replaces the file with the entries, atomically, and returns its info.
func (s *Store) write(entries []entry) (os.FileInfo, error) {
	doc := document{Version: fileVersion, Keys: make([]record, len(entries))}
	for i, e := range entries {
		doc.Keys[i] = record{Key: e.Key, SHA256: hexOf(e.hash)}
	}
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	data = append(data, '\n')

	tmp, err := os.CreateTemp(s.dir, FileName+".tmp-*") // mode 0600
	if err != nil {
		return nil, fmt.Errorf("keys: %w", err)
	}
	name := tmp.Name()
	fail := func(err error) (os.FileInfo, error) {
		_ = tmp.Close()
		_ = os.Remove(name)
		return nil, fmt.Errorf("keys: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return fail(err)
	}
	if err := tmp.Sync(); err != nil {
		return fail(err)
	}
	if err := tmp.Close(); err != nil {
		return fail(err)
	}
	if err := os.Rename(name, s.path); err != nil {
		_ = os.Remove(name)
		return nil, fmt.Errorf("keys: %w", err)
	}
	// Make the rename durable. Not every file system can sync a directory, and
	// the file is already whole, so a failure here is not one of the write.
	if d, err := os.Open(s.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return os.Stat(s.path)
}

// load reads and checks the file. A missing file is no keys. Anything else
// wrong is an error that matches ErrUnavailable. The returned info is that of
// the file as read.
func load(path string) ([]entry, os.FileInfo, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}
	unusable := func(format string, a ...any) ([]entry, os.FileInfo, error) {
		return nil, info, fmt.Errorf("%w: %s: %s", ErrUnavailable, path, fmt.Sprintf(format, a...))
	}
	if !info.Mode().IsRegular() {
		return unusable("not a regular file")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return unusable("mode %04o lets its group or others write it; use 0600", info.Mode().Perm())
	}
	data, err := io.ReadAll(io.LimitReader(f, maxFileSize+1))
	if err != nil {
		return unusable("%v", err)
	}
	if len(data) > maxFileSize {
		return unusable("larger than %d bytes", maxFileSize)
	}
	entries, err := parse(data)
	if err != nil {
		return unusable("%v", err)
	}
	return entries, info, nil
}

// parse reads a file's bytes strictly.
func parse(data []byte) ([]entry, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var doc document
	if err := dec.Decode(&doc); err != nil {
		return nil, fmt.Errorf("not the key file's JSON: %w", err)
	}
	if dec.More() {
		return nil, errors.New("data after the JSON document")
	}
	if doc.Version != fileVersion {
		return nil, fmt.Errorf("version %d, this build reads %d", doc.Version, fileVersion)
	}
	if len(doc.Keys) > MaxKeys {
		return nil, fmt.Errorf("more than %d keys", MaxKeys)
	}
	ids := map[string]bool{}
	hashes := map[[32]byte]bool{}
	entries := make([]entry, 0, len(doc.Keys))
	for i, r := range doc.Keys {
		if !validID(r.ID) {
			return nil, fmt.Errorf("key %d: bad id", i)
		}
		if ids[r.ID] {
			return nil, fmt.Errorf("key %d: id repeated", i)
		}
		ids[r.ID] = true
		if _, ok := r.Scope.letter(); !ok {
			return nil, fmt.Errorf("key %d: unknown scope", i)
		}
		if r.Created.IsZero() {
			return nil, fmt.Errorf("key %d: no creation time", i)
		}
		if r.Replaces != "" && !validID(r.Replaces) {
			return nil, fmt.Errorf("key %d: bad replaces", i)
		}
		var h [32]byte
		if len(r.SHA256) != 2*len(h) {
			return nil, fmt.Errorf("key %d: bad sha256", i)
		}
		if _, err := hex.Decode(h[:], []byte(r.SHA256)); err != nil || hexOf(h) != r.SHA256 {
			return nil, fmt.Errorf("key %d: bad sha256", i)
		}
		if hashes[h] {
			return nil, fmt.Errorf("key %d: hash repeated", i)
		}
		hashes[h] = true
		entries = append(entries, entry{Key: r.Key, hash: h})
	}
	return entries, nil
}

func validID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
