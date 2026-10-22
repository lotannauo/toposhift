//go:build !unix

package xtnode

import "errors"

// FreeBytes is not available off Unix.
func FreeBytes(string) (uint64, error) {
	return 0, errors.New("xtnode: free space is read on Unix only")
}
