package duckengine

import "slices"

// ArmSize is the bytes an arm's own files take.
type ArmSize struct {
	Arm   Arm
	Files []FileInfo
	Bytes int64
	// BytesPerRecord is Bytes over the records the activity file holds, the rows of the
	// record files.
	BytesPerRecord float64
}

// SizeReport is the bytes of a build, by arm and file, and per record held.
type SizeReport struct {
	// Records are the records the activity file holds: every record of the stream the
	// retention keeps.
	Records  int64
	Activity FileInfo
	Arms     []ArmSize
	// Composite is the interval arm's files together with the record files it reads for
	// what an interval cannot answer, if both were built.
	Composite *ArmSize
}

// Sizes reports the bytes of a build from its manifest.
func Sizes(m *Manifest) SizeReport {
	rep := SizeReport{Records: int64(m.Stream.Kept)}
	rep.Activity, _ = m.File(RoleActivity)
	per := func(b int64) float64 {
		if rep.Records == 0 {
			return 0
		}
		return float64(b) / float64(rep.Records)
	}
	for _, a := range Arms {
		if !slices.Contains(m.Arms, a) {
			continue
		}
		s := ArmSize{Arm: a}
		for _, role := range armRoles[a] {
			if f, ok := m.File(role); ok {
				s.Files = append(s.Files, f)
				s.Bytes += f.Bytes
			}
		}
		s.BytesPerRecord = per(s.Bytes)
		rep.Arms = append(rep.Arms, s)
	}
	if m.Has(ArmI) && hasRoles(m, armRoles[ArmA2]) {
		c := ArmSize{Arm: ArmI}
		for _, role := range readRoles(ArmI) {
			f, _ := m.File(role)
			c.Files = append(c.Files, f)
			c.Bytes += f.Bytes
		}
		c.BytesPerRecord = per(c.Bytes)
		rep.Composite = &c
	}
	return rep
}
