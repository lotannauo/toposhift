//go:build unix

package main

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

// lockDefaultAddress holds, until the test ends, an exclusive lock that every
// test process of this user with the same temp directory shares, so two runs
// that need the default address (two worktrees, two local CI runs) take turns
// instead of racing for it. The lock is flock(2) on an open file, so a process
// that is killed cannot leave it held, and the file is empty and never written
// once created, so the input the go command records for test result reuse does
// not change.
func lockDefaultAddress(t testing.TB) {
	t.Helper()
	path := filepath.Join(os.TempDir(), "toposhift-test-7070.lock")
	f, err := os.OpenFile(path, os.O_RDONLY|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	// Closing the file releases the lock.
	t.Cleanup(func() { _ = f.Close() })

	deadline := time.Now().Add(5 * time.Minute)
	for {
		err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if err == nil {
			return
		}
		if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EINTR) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("another test process has held %s for 5 minutes", path)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
