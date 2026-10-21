//go:build !unix

package keys

import "errors"

// tryLock fails: key files are supported on Linux and macOS only.
func tryLock(uintptr) error {
	return errors.New("keys: key files are supported on Linux and macOS only")
}
