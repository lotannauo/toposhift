//go:build !unix

package main

import "testing"

// lockDefaultAddress does nothing here: the boot test's own probe of the
// default address still skips it when the address is taken.
func lockDefaultAddress(testing.TB) {}
