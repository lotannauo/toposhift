//go:build !unix

package datadir

import (
	"errors"
	"os"
)

// flock fails: data directories are supported on Linux and macOS only.
func flock(*os.File) error {
	return errors.New("data directories are supported on Linux and macOS only")
}

func unlock(*os.File) {}

// ownerUID reports no owner on these platforms.
func ownerUID(string) (uint32, bool) { return 0, false }
