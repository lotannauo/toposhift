package runner

import "io"

// What the tests of the diag reader reach: its deliberate faults, and the store gate's
// own formatting of a duration and of megabytes, to compare the reader's figures with
// the text of the gate's rows.

// DiagMutant is a deliberate fault of the reader (see diagMutant).
type DiagMutant = diagMutant

const (
	DiagMutNone           = diagMutNone
	DiagMutQ2Bottom       = diagMutQ2Bottom
	DiagMutQ2FloorRank    = diagMutQ2FloorRank
	DiagMutRIgnoresBounds = diagMutRIgnoresBounds
	DiagMutHeapFromRSS    = diagMutHeapFromRSS
	DiagMutNoCommitCheck  = diagMutNoCommitCheck
)

// DiagFiguresWith is [DiagFigures] with a fault.
func DiagFiguresWith(m *Manifest, metrics io.Reader, mut DiagMutant) (DiagBuild, error) {
	return diagFigures(m, metrics, mut)
}

// ReadDiagWith is [ReadDiag] with a fault.
func ReadDiagWith(root string, o DiagReadOptions, mut DiagMutant) (*DiagReport, error) {
	return readDiag(root, o, mut)
}

// FmtDur and FmtMB are how the store gate prints a duration and a count of bytes.
func FmtDur(ns int64) string { return fmtDur(ns) }
func FmtMB(n int64) string   { return fmtMB(n) }
