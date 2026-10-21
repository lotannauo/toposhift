//go:build unix

package keys

import (
	"errors"
	"syscall"
)

// tryLock takes the exclusive lock on the open file without waiting. A lock
// held elsewhere is reported as errLockHeld.
func tryLock(fd uintptr) error {
	for {
		err := syscall.Flock(int(fd), syscall.LOCK_EX|syscall.LOCK_NB)
		switch {
		case err == nil:
			return nil
		case errors.Is(err, syscall.EINTR):
			continue
		case errors.Is(err, syscall.EWOULDBLOCK):
			return errLockHeld
		}
		return err
	}
}
