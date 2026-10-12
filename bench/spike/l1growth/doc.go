// Package l1growth holds the check that host and node reads stay within the read
// budget when their layer keeps 90 days. The check is a test (see
// l1growth_test.go); it has a package of its own so that its run time does not
// count against the test timeout of the packages it uses.
package l1growth
