//go:build !unix

package doltspike

import "errors"

// freeBytes is not known off Unix systems.
func freeBytes(string) (uint64, error) {
	return 0, errors.New("free disk is read on Unix systems only")
}
