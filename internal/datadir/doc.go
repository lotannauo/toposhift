// Package datadir finds, lays out and locks the directory toposhift keeps its
// data in.
//
// [Resolve] decides which directory that is, without touching the disk (apart
// from reading symlinks). [Open] creates it, checks its layout, takes the lock
// that keeps a second instance out, and returns the paths of the parts.
//
// # Data directory
//
// The directory is the first of these that is set:
//
//  1. the --data-dir flag;
//  2. the TOPOSHIFT_DATA_DIR environment variable;
//  3. $STATE_DIRECTORY, which systemd sets for a unit with StateDirectory=, but
//     only when it is one absolute path. With several paths, separated by
//     colons, or when it is not an absolute path, there is no data directory
//     (it does not fall through to HOME), and the boot output says to name one
//     with the flag;
//  4. $HOME/.local/share/toposhift, only when HOME is set and absolute.
//
// If none is set there is no data directory. toposhift still starts and answers
// health checks, but ingest is off: the receiver answers 503 with a sentence
// naming --data-dir and TOPOSHIFT_DATA_DIR, and boot prints the same sentence.
// The temp directory is never a fallback. The same holds when the directory
// under HOME, which nobody asked for, cannot be opened (HOME is read-only or
// missing, or the directory has other files in it): toposhift boots with ingest
// off and says so. A directory that was asked for, by the flag, the variable or
// $STATE_DIRECTORY, and cannot be opened ends serve with an error.
//
// $XDG_DATA_HOME is not consulted, on macOS as on Linux. When it is set and
// differs from $HOME/.local/share, boot says once that it is not used.
//
// A relative path from the flag or the variable is taken against the working
// directory. A path that starts with "~" is refused, because only a shell
// expands it: a quoted --data-dir=~/x would otherwise name a directory called
// "~" in the working directory. Boot prints the absolute path, and the symlink
// target when it differs.
//
// Missing directories are created with mode 0700, whatever the umask. An
// existing directory is never chmodded or chowned. Inside it are store/, bus/
// and meta/; meta/format holds the layout version, and a layout newer than this
// build reads is refused. A directory that has entries and no marker is not a
// toposhift data directory and is refused, so a mistyped path cannot fill
// somebody's other directory.
//
// One instance owns a directory. meta/lock is held with flock(2) for the life
// of the process, and meta/holder records the pid, host and start time, so a
// second instance names the first in its error. The lock is released when the
// process exits, however it exits.
//
// Boot warns, and does not refuse, when the directory is inside the temp
// directory (data may be lost at reboot), is world-writable without the sticky
// bit, or is on NFS or SMB. A directory owned by another user is not refused
// either: the first write and the lock decide, and a failure names the
// directory, its owner uid and the running uid. toposhift never changes a
// directory's mode or owner to make that work.
//
// # Containers and Kubernetes
//
// The container image is to set TOPOSHIFT_DATA_DIR=/var/lib/toposhift and
// declare it a volume that belongs to the image's non-root user. A volume mounted
// there must be writable by that uid. In Kubernetes, set the pod's fsGroup so
// the mounted volume is group-writable for it; with a read-only root
// filesystem, the data directory must be a mounted volume, because nothing
// else is writable. An emptyDir volume lives only as long as the pod, so its
// data does not survive a reschedule; use a persistent volume for data to keep.
//
// # Backups
//
// A backup that copies the directory while toposhift runs, Time Machine
// included, may copy it in the middle of a write. Stop the instance, or take a
// snapshot of the volume, when the copy has to be consistent.
package datadir
