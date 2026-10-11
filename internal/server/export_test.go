package server

import "time"

// ShutdownTimeout returns the graceful shutdown deadline.
func ShutdownTimeout() time.Duration { return shutdownTimeout }

// SetShutdownTimeout replaces the graceful shutdown deadline and returns a
// function that restores the previous one. Tests that call it must not run in
// parallel.
func SetShutdownTimeout(d time.Duration) (restore func()) {
	old := shutdownTimeout
	shutdownTimeout = d
	return func() { shutdownTimeout = old }
}
