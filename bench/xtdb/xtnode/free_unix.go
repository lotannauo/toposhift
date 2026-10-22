//go:build unix

package xtnode

import (
	"fmt"
	"syscall"
)

// FreeBytes is the space a process may still write on the file system that holds path.
func FreeBytes(path string) (uint64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return 0, fmt.Errorf("xtnode: free space of %s: %w", path, err)
	}
	return uint64(st.Bavail) * uint64(st.Bsize), nil //nolint:gosec // block counts and sizes are not negative
}
