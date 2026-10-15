//go:build race

package mcp

// raceEnabled says the tests are built with the race detector, under which the
// properties draw fewer cases and the large scenarios are smaller.
const raceEnabled = true
