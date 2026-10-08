package segment

import (
	"context"
	"errors"
	"io"
	"iter"
)

// MaxSize is the largest segment a store accepts, in bytes.
const MaxSize = 256 << 20

// Info describes a stored segment.
type Info struct {
	Name string
	Size int64
	// SHA256 is the SHA-256 of the segment's bytes, computed by the store as it
	// wrote them. Put and Stat always set it. List may leave it zero, because
	// an object store cannot list it without a request per segment.
	SHA256 [32]byte
}

// Store holds immutable named segments. Implementations are safe for
// concurrent use; the product has one writer per store.
//
// A note for implementers of an object store: Put and Stat always set SHA256,
// so a store that cannot know the digest before the body is sent (S3-style
// stores) must record it as user metadata before the upload, which means
// buffering the segment or reading it twice (up to MaxSize), unless the service
// accepts a trailing checksum after the body. That is not verified for R2 or B2.
type Store interface {
	// Put stores the bytes read from r under name, whole or not at all, and
	// returns its Info.
	//
	// size is the number of bytes r will yield, or -1 if unknown. A known size
	// that r does not match is an error and stores nothing. More than MaxSize
	// bytes is refused with ErrTooLarge. A name that exists is refused with
	// ErrExists and the existing segment is left unchanged. A read error from
	// r, or a context done before the segment is complete, stores nothing.
	//
	// When Put fails in a way that leaves the outcome uncertain (a lost
	// connection, a crash), retry it: if the retry returns ErrExists, call Stat
	// and compare SHA256 with the digest of the bytes you meant to store. Equal
	// means the earlier Put succeeded; different means another segment holds
	// the name.
	Put(ctx context.Context, name string, r io.Reader, size int64) (Info, error)
	// Open returns the segment's bytes; ErrNotFound if there is none.
	Open(ctx context.Context, name string) (io.ReadCloser, error)
	// Stat returns the segment's Info, SHA256 included; ErrNotFound if there
	// is none.
	Stat(ctx context.Context, name string) (Info, error)
	// List yields every segment whose name starts with prefix (the empty prefix
	// lists all), in ascending byte order of name. SHA256 may be zero. An error
	// ends the sequence.
	List(ctx context.Context, prefix string) iter.Seq2[Info, error]
	// Delete removes the segment. Removing a name that holds no segment is not
	// an error.
	Delete(ctx context.Context, name string) error
}

var (
	// ErrInvalidName is returned for a name or prefix that breaks the rules of
	// [ValidName].
	ErrInvalidName = errors.New("segment: invalid name")
	// ErrExists is returned by Put for a name that already holds a segment.
	ErrExists = errors.New("segment: exists")
	// ErrNotFound is returned for a name that holds no segment.
	ErrNotFound = errors.New("segment: not found")
	// ErrTooLarge is returned by Put for more than [MaxSize] bytes.
	ErrTooLarge = errors.New("segment: too large")
)
