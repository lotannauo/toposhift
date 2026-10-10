//go:build unix

package runner

import (
	"runtime"
	"syscall"
)

// peakRSS is the largest resident set the process has had, in bytes, or 0 when the
// system does not say. The kernel reports it in bytes on macOS and in kibibytes on
// the other systems.
func peakRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	n := int64(ru.Maxrss)
	if runtime.GOOS != "darwin" {
		n *= 1024
	}
	return n
}
