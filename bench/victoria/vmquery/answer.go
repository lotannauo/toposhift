package vmquery

import (
	"encoding/json"
	"fmt"
	"math"
	"slices"
	"strconv"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
)

// Result is one series of an instant query's answer: its labels and the text of its
// value. The metric name is not relied on: a function over a window may drop it, and
// the labels name everything an answer needs [U] (d).
type Result struct {
	Labels map[string]string
	Value  string
}

// present reports whether the series is there. A NaN value is a staleness marker the
// store returned instead of a sample, which is the series having ended [U] (k).
func (r Result) present() bool {
	f, err := strconv.ParseFloat(r.Value, 64)
	return err == nil && !math.IsNaN(f)
}

// response is the JSON of an instant query.
type response struct {
	Status    string `json:"status"`
	ErrorType string `json:"errorType"`
	Error     string `json:"error"`
	IsPartial bool   `json:"isPartial"`
	Data      struct {
		ResultType string `json:"resultType"`
		Result     []struct {
			Metric map[string]string  `json:"metric"`
			Value  [2]json.RawMessage `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

// ParseResponse reads the JSON answer of an instant query as the series of a vector. A
// status other than success, a partial answer or a result of another type is an error:
// an answer the store did not give must not be read as an empty one.
func ParseResponse(body []byte) ([]Result, error) {
	var r response
	if err := json.Unmarshal(body, &r); err != nil {
		return nil, fmt.Errorf("vmquery: reading the response: %w", err)
	}
	switch {
	case r.Status != "success":
		return nil, fmt.Errorf("vmquery: the store answered status %q (%s: %s)", r.Status, r.ErrorType, r.Error)
	case r.IsPartial:
		return nil, fmt.Errorf("vmquery: the store gave a partial answer")
	case r.Data.ResultType != "vector":
		return nil, fmt.Errorf("vmquery: the result is a %q, not a vector", r.Data.ResultType)
	}
	out := make([]Result, 0, len(r.Data.Result))
	for i, s := range r.Data.Result {
		var v string
		if err := json.Unmarshal(s.Value[1], &v); err != nil {
			return nil, fmt.Errorf("vmquery: result %d has no value text: %w", i, err)
		}
		if _, err := strconv.ParseFloat(v, 64); err != nil {
			return nil, fmt.Errorf("vmquery: result %d has the value %q: %w", i, v, err)
		}
		out = append(out, Result{Labels: s.Metric, Value: v})
	}
	return out, nil
}

// edgeOf reads an edge series of an answer and checks that it is on the entity the read
// asked about, at the end the direction names. The peer is the other end.
func edgeOf(r Result, fp identity.Fingerprint, d engine.Direction) (engine.Neighbor, error) {
	near, far := vmmodel.LabelDst, vmmodel.LabelSrc
	if d == engine.Forward {
		near, far = vmmodel.LabelSrc, vmmodel.LabelDst
	}
	if got := r.Labels[near]; got != fp.String() {
		return engine.Neighbor{}, fmt.Errorf("vmquery: an answer for %s has %s=%q: the read was not made on that end", fp, near, got)
	}
	return neighborOf(r, far)
}

func neighborOf(r Result, far string) (engine.Neighbor, error) {
	peer, err := identity.ParseFingerprint(r.Labels[far])
	if err != nil {
		return engine.Neighbor{}, fmt.Errorf("vmquery: label %s of an answer: %w", far, err)
	}
	rel := r.Labels[vmmodel.LabelRel]
	if rel == "" {
		return engine.Neighbor{}, fmt.Errorf("vmquery: an answer for peer %s has no %s label", peer, vmmodel.LabelRel)
	}
	return engine.Neighbor{Peer: peer, Relation: catalog.RelationType(rel)}, nil
}

// Neighbors reads the answer of a neighbors read of fp as the engine returns it: the
// edges that are there through any producer, each once, ordered by peer and relation.
func Neighbors(results []Result, fp identity.Fingerprint, d engine.Direction) ([]engine.Neighbor, error) {
	var out []engine.Neighbor
	for _, r := range results {
		if !r.present() {
			continue
		}
		n, err := edgeOf(r, fp, d)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return sortUnique(out), nil
}

// NeighborsBatch reads the answer of a batched read: the list of each entity of fps, in
// the order of fps. An entity asked about twice has its list twice, and one with no edge
// an empty list, as the contract of the batched read says.
func NeighborsBatch(results []Result, fps []identity.Fingerprint, d engine.Direction) ([][]engine.Neighbor, error) {
	near, far := vmmodel.LabelDst, vmmodel.LabelSrc
	if d == engine.Forward {
		near, far = vmmodel.LabelSrc, vmmodel.LabelDst
	}
	asked := make(map[string]struct{}, len(fps))
	for _, fp := range fps {
		asked[fp.String()] = struct{}{}
	}
	by := map[string][]engine.Neighbor{}
	for _, r := range results {
		if !r.present() {
			continue
		}
		key := r.Labels[near]
		if _, ok := asked[key]; !ok {
			return nil, fmt.Errorf("vmquery: an answer has %s=%q, which no entity of the batch is", near, key)
		}
		n, err := neighborOf(r, far)
		if err != nil {
			return nil, err
		}
		by[key] = append(by[key], n)
	}
	out := make([][]engine.Neighbor, len(fps))
	for i, fp := range fps {
		out[i] = sortUnique(by[fp.String()])
		if out[i] == nil {
			out[i] = []engine.Neighbor{}
		}
	}
	return out, nil
}

// Alive reads the answer of an existence read of fp: the entity is there if any producer's
// series is.
func Alive(results []Result, fp identity.Fingerprint) (bool, error) {
	alive := false
	for _, r := range results {
		if r.Labels[vmmodel.LabelFP] != vmmodel.FPLabel(fp) || r.Labels[vmmodel.LabelType] != string(fp.Type()) {
			return false, fmt.Errorf("vmquery: an answer for %s has type=%q fp=%q", fp, r.Labels[vmmodel.LabelType], r.Labels[vmmodel.LabelFP])
		}
		if r.present() {
			alive = true
		}
	}
	return alive, nil
}

func sortUnique(ns []engine.Neighbor) []engine.Neighbor {
	engine.SortNeighbors(ns)
	return slices.Compact(ns)
}
