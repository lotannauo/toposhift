package itbench

import (
	"cmp"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	"go.yaml.in/yaml/v3"
)

// GroundTruth is a scenario's labels as the loader reads them. The file comes in
// two shapes with the same fields: flat, with the keys at the top level, and
// wrapped, with an apiVersion and kind and the keys under spec. A file a strict
// parser cannot read is an error, listed by the caller and never repaired.
type GroundTruth struct {
	Fault        []Fault       `yaml:"fault"`
	Groups       []Group       `yaml:"groups"`
	Propagations []Propagation `yaml:"propagations"`
}

// Fault is one labelled fault entry.
type Fault struct {
	Entity    FaultEntity `yaml:"entity"`
	Condition string      `yaml:"condition"`
	Category  string      `yaml:"category"`
	Mechanism string      `yaml:"fault_mechanism"`
}

// FaultEntity is the entity a fault entry names; its name may be a group id or a
// wildcard.
type FaultEntity struct {
	Name    string `yaml:"name"`
	Kind    string `yaml:"kind"`
	GroupID string `yaml:"group_id"`
}

// Group is a named set of cluster objects: those of a kind in a namespace whose
// name is the given name, or fully matches one of the filters.
type Group struct {
	ID        string   `yaml:"id"`
	Kind      string   `yaml:"kind"`
	Name      string   `yaml:"name"`
	Namespace string   `yaml:"namespace"`
	Filter    []string `yaml:"filter"`
	RootCause bool     `yaml:"root_cause"`
}

// Propagation is one labelled step of the chain, from a group id to a group id.
type Propagation struct {
	Source string `yaml:"source"`
	Target string `yaml:"target"`
}

type wrapped struct {
	APIVersion string       `yaml:"apiVersion"`
	Kind       string       `yaml:"kind"`
	Spec       *GroundTruth `yaml:"spec"`
}

// ErrUnreadable wraps the error of a ground-truth file that does not parse.
var ErrUnreadable = errors.New("itbench: unreadable ground truth")

// ReadGroundTruth reads a scenario's ground_truth.yaml, flat or wrapped.
func ReadGroundTruth(path string) (GroundTruth, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return GroundTruth{}, err
	}
	return ParseGroundTruth(b)
}

// ParseGroundTruth parses a ground-truth document. A repeated key or any other
// error of the YAML is an error here: nothing is repaired.
func ParseGroundTruth(b []byte) (GroundTruth, error) {
	// The whole document is read first, into plain values: a repeated key is an error
	// wherever it is, also in a section that is not used.
	var whole any
	if err := yaml.Unmarshal(b, &whole); err != nil {
		return GroundTruth{}, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	var w wrapped
	if err := yaml.Unmarshal(b, &w); err != nil {
		return GroundTruth{}, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	if w.Spec != nil {
		return *w.Spec, nil
	}
	var g GroundTruth
	if err := yaml.Unmarshal(b, &g); err != nil {
		return GroundTruth{}, fmt.Errorf("%w: %w", ErrUnreadable, err)
	}
	return g, nil
}

// Root is the group that marks the root of the chain: exactly one group has
// root_cause set.
func (g GroundTruth) Root() (Group, bool) {
	var root Group
	n := 0
	for _, x := range g.Groups {
		if x.RootCause {
			root, n = x, n+1
		}
	}
	return root, n == 1
}

// Group finds a group by id.
func (g GroundTruth) Group(id string) (Group, bool) {
	for _, x := range g.Groups {
		if x.ID == id {
			return x, true
		}
	}
	return Group{}, false
}

// Chain lists the ids of the groups on the propagation chain, in order of first
// appearance as a source and then a target of each step.
func (g GroundTruth) Chain() []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range g.Propagations {
		for _, id := range []string{p.Source, p.Target} {
			if id != "" && !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	return out
}

// Targets lists the ids of the groups that are the target of a step, in order of
// first appearance.
func (g GroundTruth) Targets() []string {
	var out []string
	seen := map[string]bool{}
	for _, p := range g.Propagations {
		if p.Target != "" && !seen[p.Target] {
			seen[p.Target] = true
			out = append(out, p.Target)
		}
	}
	return out
}

// Matcher says how a group names objects: by an exact name, by the filters the
// label carries, or not at all.
type Matcher struct {
	// Via is "exact name", "label filter" or "" (the group names nothing).
	Via string
	// Why says why the group names nothing: no name or filter, a wildcard name, or
	// a filter that is not a regular expression.
	Why   string
	Match func(name string) bool
}

// Via values.
const (
	ViaName   = "exact-name"
	ViaFilter = "label-filter"
)

// Matcher builds how a group names objects. A group with filters names the objects
// whose whole name matches one of them, each read as a Go (RE2) expression exactly
// as the label writes it. A group with a name and no filter names the object of
// that name, unless the name holds a wildcard, which is no name. A group with
// neither names nothing, and so does one whose filter does not compile: there is
// no prefix or fuzzy match.
func (g Group) Matcher() Matcher {
	if len(g.Filter) > 0 {
		var res []*regexp.Regexp
		for _, f := range g.Filter {
			re, err := regexp.Compile(`^(?:` + f + `)$`)
			if err != nil {
				return Matcher{Why: "filter_not_a_regular_expression"}
			}
			res = append(res, re)
		}
		return Matcher{Via: ViaFilter, Match: func(name string) bool {
			for _, re := range res {
				if re.MatchString(name) {
					return true
				}
			}
			return false
		}}
	}
	switch {
	case g.Name == "":
		return Matcher{Why: "no_name_or_filter"}
	case containsWildcard(g.Name):
		return Matcher{Why: "wildcard_name_without_filter"}
	}
	name := g.Name
	return Matcher{Via: ViaName, Match: func(n string) bool { return n == name }}
}

func containsWildcard(s string) bool {
	for _, c := range s {
		if c == '*' || c == '?' {
			return true
		}
	}
	return false
}

// scenarioDir matches the name of a scenario's directory.
var scenarioDir = regexp.MustCompile(`^Scenario-([0-9]+)$`)

// Scenarios lists the scenario directories under a dataset directory, ordered by
// number.
func Scenarios(dir string) ([]string, error) {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	type sc struct {
		name string
		n    int
	}
	var all []sc
	for _, e := range ents {
		if m := scenarioDir.FindStringSubmatch(e.Name()); m != nil && e.IsDir() {
			n, _ := strconv.Atoi(m[1])
			all = append(all, sc{e.Name(), n})
		}
	}
	slices.SortFunc(all, func(a, b sc) int { return cmp.Compare(a.n, b.n) })
	out := make([]string, len(all))
	for i, s := range all {
		out[i] = filepath.Join(dir, s.name)
	}
	return out, nil
}
