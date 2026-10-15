package itbench

import (
	"bufio"
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/lotannauo/toposhift/bench/eval/blastscore"
)

// ResultsFile is the name the results of a score stage on this revision are
// written under, and recorded under in the repository: the dataset, and the
// revision's first eight hex digits.
const ResultsFile = "itbench-lite-d0916b08.json"

// probeHeader is the column line of a probe file.
const probeHeader = "scenario\tcovers\toverlap_minutes\tto_fetch\treason"

// na is a probe value that could not be computed.
const na = "na"

// Coverage is what the probe of a scenario's traces export found: whether the
// export can cover the case's window, and the whole minutes it overlaps it. Nil is
// a value the probe could not compute.
type Coverage struct {
	CoversWindow   *bool
	OverlapMinutes *int
}

// Probe is the coverage of every scenario's traces export, by case id.
type Probe map[string]Coverage

// ReadProbe reads a probe file: lines starting with # are comments, then the
// column line, then one line a scenario, tab-separated:
//
//	scenario  covers  overlap_minutes  to_fetch  reason
//
// covers is true, false or na; overlap_minutes a whole number of minutes or na;
// to_fetch true or false. Only covers and overlap_minutes are kept: the probe
// stored no time, and nothing else of a line goes further. A covers of na has no
// overlap, and an export that does not cover the window overlaps it by zero
// minutes or na. Any other line is refused, and so is a scenario named twice.
func ReadProbe(data []byte) (Probe, error) {
	p := Probe{}
	sc := bufio.NewScanner(bytes.NewReader(data))
	header := false
	for n := 1; sc.Scan(); n++ {
		line := sc.Text()
		switch {
		case strings.HasPrefix(line, "#") && !header:
			continue
		case !header:
			if line != probeHeader {
				return nil, fmt.Errorf("itbench: probe line %d: the column line is not %q", n, probeHeader)
			}
			header = true
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 {
			return nil, fmt.Errorf("itbench: probe line %d: %d fields, want 5", n, len(f))
		}
		id := f[0]
		if !blastscore.CaseIDPattern.MatchString(id) {
			return nil, fmt.Errorf("itbench: probe line %d: %q is not a case id", n, id)
		}
		if _, dup := p[id]; dup {
			return nil, fmt.Errorf("itbench: probe line %d: %s appears twice", n, id)
		}
		var c Coverage
		switch f[1] {
		case "true", "false":
			v := f[1] == "true"
			c.CoversWindow = &v
		case na:
		default:
			return nil, fmt.Errorf("itbench: probe line %d: covers %q is not true, false or na", n, f[1])
		}
		if f[2] != na {
			m, err := strconv.Atoi(f[2])
			if err != nil || m < 0 || strconv.Itoa(m) != f[2] {
				return nil, fmt.Errorf("itbench: probe line %d: overlap_minutes %q is not a whole number of minutes or na", n, f[2])
			}
			c.OverlapMinutes = &m
		}
		switch {
		case c.CoversWindow == nil && c.OverlapMinutes != nil:
			return nil, fmt.Errorf("itbench: probe line %d: an overlap without a coverage", n)
		case c.CoversWindow != nil && !*c.CoversWindow && c.OverlapMinutes != nil && *c.OverlapMinutes != 0:
			return nil, fmt.Errorf("itbench: probe line %d: an export that does not cover the window overlaps it", n)
		}
		if f[3] != "true" && f[3] != "false" {
			return nil, fmt.Errorf("itbench: probe line %d: to_fetch %q is not true or false", n, f[3])
		}
		p[id] = c
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("itbench: probe: %w", err)
	}
	if !header {
		return nil, fmt.Errorf("itbench: probe: no column line %q", probeHeader)
	}
	return p, nil
}

// Check says whether the probe names exactly these case ids: every one, and no
// other, so that no case goes without a coverage and none is read from another
// dataset's probe.
func (p Probe) Check(ids []string) error {
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if _, ok := p[id]; !ok {
			return fmt.Errorf("itbench: the probe has no line for %s", id)
		}
		seen[id] = true
	}
	for id := range p {
		if !seen[id] {
			return fmt.Errorf("itbench: the probe names %s, which is not a scenario of the dataset", id)
		}
	}
	return nil
}

// Apply sets each run's trace coverage from the probe, after checking that the
// probe names the runs' cases exactly ([Probe.Check]).
func (p Probe) Apply(runs []blastscore.CaseRun) error {
	ids := make([]string, len(runs))
	for i, r := range runs {
		ids[i] = r.ID
	}
	if err := p.Check(ids); err != nil {
		return err
	}
	for i := range runs {
		c := p[runs[i].ID]
		runs[i].Diag.TraceCoversWindow, runs[i].Diag.TraceOverlapMinutes = c.CoversWindow, c.OverlapMinutes
	}
	return nil
}
