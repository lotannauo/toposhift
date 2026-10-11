//go:build unix

package datadir

import (
	"errors"
	"os"
	"syscall"
)

// flock takes the exclusive lock on f without waiting. A lock held elsewhere
// is an error that matches syscall.EWOULDBLOCK.
func flock(f *os.File) error {
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// unlock releases the lock on f.
func unlock(f *os.File) {
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
}

// ownerUID returns the uid that owns path.
func ownerUID(path string) (uint32, bool) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, false
	}
	return st.Uid, true
}
