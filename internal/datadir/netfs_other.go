//go:build !darwin && !linux

package datadir

// networkFS reports no network filesystem: detection exists for Linux and
// macOS only.
func networkFS(string) (string, error) { return "", nil }
