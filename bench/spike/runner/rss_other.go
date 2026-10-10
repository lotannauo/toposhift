//go:build !unix

package runner

// peakRSS is 0 where the system's resource usage is not read: a build there has no
// peak resident set, and the store gate does not judge one.
func peakRSS() int64 { return 0 }
