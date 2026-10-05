package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Pins are the prefixes a family of runs is read at, chosen once and kept: the
// busiest and the median hub prefixes of each class (a node's, a service's, a
// host's: entities that exist from the start of the stream and are never replaced,
// where history grows with churn and refresh). Runs over different windows of
// retained history, or over different lengths of stream, read the same prefixes, so
// what a read costs can be compared between them, and the stream of a run with pins
// is the projection of the full stream on them: only the records that touch a pinned
// entity are written to a store, which is what makes a run over a month of history
// cheap to build.
//
// A pod's own prefix is not pinned: pods are replaced, so one that exists at the
// end of a short stream does not exist in a longer one, and its history is a
// handful of records whatever the window.
type Pins struct {
	Classes []PinnedClass
	// Source is the scenario digest of the spec they were chosen from ([Spec.ScenarioDigest]):
	// a plan with them must be of the same scenario.
	Source string
	// Digest covers the classes and the source, so a plan records which pins it was made
	// with.
	Digest string
}

// PinnedClass is the prefixes of one class.
type PinnedClass struct {
	Class                       workload.Class
	Records, Extensions, Median []identity.Fingerprint
}

// MakePins chooses the pins from the stream of the spec: the hub prefixes that exist
// at its end, ranked by the records a store holds after its last retention.
func MakePins(ctx context.Context, spec Spec) (*Pins, error) {
	spec.Pins = nil
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	an := workload.NewAnalyzer(spec.Workload.Start, spec.Workload.Duration)
	if n := len(spec.Retentions); n > 0 {
		an.SetRetainedFrom(spec.Retentions[n-1].Horizon(spec.Workload.Start))
	}
	if _, err := Drive(ctx, spec, analyzerSink{an}); err != nil {
		return nil, err
	}
	source, err := spec.ScenarioDigest()
	if err != nil {
		return nil, err
	}
	p := &Pins{Source: source}
	for _, t := range targets {
		if !t.hub {
			continue
		}
		picks := an.Pick(t.class, spec.Hot, spec.Median, true)
		pc := PinnedClass{Class: t.class}
		for _, top := range picks.ByRecords {
			pc.Records = append(pc.Records, top.Fingerprint)
		}
		for _, top := range picks.ByExtensions {
			pc.Extensions = append(pc.Extensions, top.Fingerprint)
		}
		for _, top := range picks.Median {
			pc.Median = append(pc.Median, top.Fingerprint)
		}
		if len(pc.Records)+len(pc.Extensions)+len(pc.Median) > 0 {
			p.Classes = append(p.Classes, pc)
		}
	}
	if len(p.Classes) == 0 {
		return nil, fmt.Errorf("runner: the stream has no hub prefix to pin")
	}
	return p, p.seal()
}

// digest is the digest of the classes and the source.
func (p *Pins) digest() (string, error) {
	b, err := json.Marshal(struct {
		Classes []PinnedClass
		Source  string
	}{p.Classes, p.Source})
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}

// seal sets the digest of the pins.
func (p *Pins) seal() (err error) {
	p.Digest, err = p.digest()
	return err
}

// Verify is whether the digest is the pins'. It changes nothing.
func (p *Pins) Verify() error {
	d, err := p.digest()
	if err != nil {
		return err
	}
	if p.Digest != d {
		return fmt.Errorf("runner: the pins say digest %s and hold %s", p.Digest, d)
	}
	return nil
}

// Pick implements [Selector]: the pinned prefixes of the class, whatever is asked.
func (p *Pins) Pick(c workload.Class, _, _ int, _ bool) workload.Picks {
	var out workload.Picks
	for _, pc := range p.Classes {
		if pc.Class != c {
			continue
		}
		top := func(fps []identity.Fingerprint) []workload.PrefixTop {
			var out []workload.PrefixTop
			for _, fp := range fps {
				out = append(out, workload.PrefixTop{Class: c, Fingerprint: fp})
			}
			return out
		}
		out.ByRecords, out.ByExtensions, out.Median = top(pc.Records), top(pc.Extensions), top(pc.Median)
	}
	return out
}

// entities is every entity the pins name.
func (p *Pins) entities() map[identity.Fingerprint]struct{} {
	set := map[identity.Fingerprint]struct{}{}
	for _, pc := range p.Classes {
		for _, list := range [][]identity.Fingerprint{pc.Records, pc.Extensions, pc.Median} {
			for _, fp := range list {
				set[fp] = struct{}{}
			}
		}
	}
	return set
}

// touches is whether the record is about a pinned entity: its subject, or either end
// of its edge. It is the rule of the reference engine's shadow, and the projection
// uses the same one.
func touches(set map[identity.Fingerprint]struct{}, r engine.Record) bool {
	if _, ok := set[r.Subject.A]; ok {
		return true
	}
	if r.Subject.Kind == engine.SubjectEdge {
		_, ok := set[r.Subject.B]
		return ok
	}
	return false
}
