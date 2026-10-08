// Package segment defines the contract of a segment store: a place that holds
// immutable named blobs.
//
// A segment is an export of the activity log, written once, read many times,
// and deleted when the backups that hold it are retired. This package says
// nothing about what a segment contains or when one is cut. It fixes only the
// [Store] interface and the rules for segment names, so that a local directory
// (package localdir) and an object store can be used interchangeably, and so
// that one suite (package segmenttest) can hold every implementation to the
// same expectations.
//
// # Three guarantees
//
// A segment is never visible half-written: a [Store.Put] that fails, or whose
// reader fails, or whose context ends, leaves nothing under the name. A name
// can never reach outside the store: names are checked by [ValidName] before
// any I/O, and a name that fails the check is refused with [ErrInvalidName]. A
// segment, once written, is never silently replaced: a second Put of a name
// that exists fails with [ErrExists] and leaves the first segment unchanged.
//
// # Names
//
// A name is one or more elements separated by single slashes. See [ValidName]
// for the exact rules. The slash is a separator for grouping and for prefix
// listing; it does not make segments hierarchical, and a store need not support
// a segment and a group of segments under one name (the local directory, for
// one, cannot hold both "a" and "a/b").
package segment
