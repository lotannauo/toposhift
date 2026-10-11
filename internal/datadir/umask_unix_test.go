//go:build unix

package datadir

import (
	"syscall"
	"testing"
)

func setUmask(t *testing.T, mask int) {
	t.Helper()
	old := syscall.Umask(mask)
	t.Cleanup(func() { syscall.Umask(old) })
}
