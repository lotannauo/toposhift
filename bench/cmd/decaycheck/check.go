package main

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/query/decay"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// A token kind says at which token an instant is read: the store as it was when
// the replay's clock reached the instant, or as it is when the replay ends.
const (
	tokenAsOfInstant = "as_of_instant"
	tokenFull        = "full"
)

// counter is one named count of the sequencer's metrics path.
type counter struct {
	Name string `json:"name"`
	N    int64  `json:"n"`
}

// runResult is what one expiry gave.
type runResult struct {
	Expiry time.Duration `json:"expiry_ns"`
	// X and HalfLife are the policy's extension interval and half-life for a record
	// with the expiry as TTL.
	X        time.Duration `json:"x_ns"`
	HalfLife time.Duration `json:"half_life_ns"`
	// LastSeq is the store's last sequence number when the replay ended, before the
	// sequencer's stop wrote what the coalescer had absorbed.
	LastSeq uint64 `json:"last_seq"`
	// Counters are the metrics path's counters, by name.
	Counters []counter `json:"counters"`
	// Edges and Sightings are the edges the service graph translator wrote and how
	// many sightings it made of them, whole, before the coalescer.
	Edges     int `json:"edges"`
	Sightings int `json:"sightings"`
	// Samples is how many instants were sampled, the first being FirstSample.
	Samples     int           `json:"samples"`
	FirstSample string        `json:"first_sample"`
	Tokens      []tokenResult `json:"tokens"`
	// Failures say what a self-check found wrong; none is expected.
	Failures []string `json:"failures,omitempty"`
}

// tokenResult is the samples of one token kind.
type tokenResult struct {
	Kind string `json:"kind"`
	// Totals are the edges' rows summed, with the maxima taken over them.
	Totals edgeRow `json:"totals"`
	// SilenceBins counts the members by SilentAtLeast in half-lives: zero, then
	// [0,1), [1,2), [2,3), [3,4) and 4 or more, where the confidence is
	// 1, (1/2,1), (1/4,1/2], (1/8,1/4], (1/16,1/8] and at most 1/16.
	SilenceBins [6]int `json:"silence_bins"`
	// UnknownEdges counts edges read that the translator never wrote.
	UnknownEdges int       `json:"unknown_edges"`
	Edges        []edgeRow `json:"edges"`
}

// edgeRow is the samples of one edge (or all of them).
type edgeRow struct {
	Edge string `json:"edge"`
	// Sightings is how many sightings the translator made of the edge.
	Sightings int `json:"sightings"`
	// Members are the samples at which the edge existed at the read's token, and so
	// had evidence. Shown are those with a bound above zero. TrueOverX are those
	// whose true silence is above the extension interval, the silences that can
	// show.
	Members   int `json:"members"`
	Shown     int `json:"shown"`
	TrueOverX int `json:"true_over_x"`
	// The faults, each expected to be zero. Unsound is a bound above the true
	// silence. Loose is a bound short of it by more than the extension interval.
	// NoRecency is a member with no recency derived. NoTruth is a member with a
	// producer the translator never made a sighting for, known at the token.
	// OtherTTL is evidence whose TTL is not the expiry.
	Unsound   int `json:"unsound"`
	Loose     int `json:"loose"`
	NoRecency int `json:"no_recency"`
	NoTruth   int `json:"no_truth"`
	OtherTTL  int `json:"other_ttl"`
	// MaxBound is the largest bound derived, and MaxTrue the largest true silence,
	// MaxShort the largest amount by which a bound fell short of the true silence.
	MaxBound time.Duration `json:"max_bound_ns"`
	MaxTrue  time.Duration `json:"max_true_silence_ns"`
	MaxShort time.Duration `json:"max_shortfall_ns"`
}

func (r *edgeRow) add(o edgeRow) {
	r.Sightings += o.Sightings
	r.Members += o.Members
	r.Shown += o.Shown
	r.TrueOverX += o.TrueOverX
	r.Unsound += o.Unsound
	r.Loose += o.Loose
	r.NoRecency += o.NoRecency
	r.NoTruth += o.NoTruth
	r.OtherTTL += o.OtherTTL
	r.MaxBound = max(r.MaxBound, o.MaxBound)
	r.MaxTrue = max(r.MaxTrue, o.MaxTrue)
	r.MaxShort = max(r.MaxShort, o.MaxShort)
}

func (r edgeRow) faults() int { return r.Unsound + r.Loose + r.NoRecency + r.NoTruth + r.OtherTTL }

// result reads the pipeline's store at every sample, at both token kinds, and
// compares what the derivation says with what the translator's sightings say.
func (p *pipeline) result(ctx context.Context, cfg config) (runResult, error) {
	pol := cfg.Policy
	res := runResult{
		Expiry: p.expiry, X: pol.Extension(p.expiry), HalfLife: pol.HalfLife(p.expiry),
		LastSeq: p.st.LastSeq(), Edges: len(p.truth.edges()), Sightings: p.truth.n,
		Samples: len(p.tokens), FirstSample: p.sample0.Format(time.RFC3339Nano),
	}
	status := p.seq.MetricsStatus()
	for _, name := range slices.Sorted(maps.Keys(status.Counters)) {
		res.Counters = append(res.Counters, counter{Name: name, N: status.Counters[name]})
	}
	// The translator the sequencer owns and the one beside it were given the same
	// requests: they must have made the same number of sightings.
	if got := status.Counters["svcgraph.sightings"]; got != int64(p.truth.n) {
		res.Failures = append(res.Failures, fmt.Sprintf("the sequencer counted %d service graph sightings and the truth holds %d", got, p.truth.n))
	}
	final := p.st.LastSeq()
	for _, kind := range []string{tokenAsOfInstant, tokenFull} {
		tr, err := p.checkKind(ctx, cfg, kind, func(i int) uint64 {
			if kind == tokenFull {
				return final
			}
			return p.tokens[i]
		})
		if err != nil {
			return runResult{}, err
		}
		res.Tokens = append(res.Tokens, tr)
		if n := tr.Totals.faults() + tr.UnknownEdges; n > 0 {
			res.Failures = append(res.Failures, fmt.Sprintf("%s: %d samples contradict the derivation or the truth (unsound %d, loose %d, no recency %d, no truth %d, other TTL %d, unknown edges %d)",
				kind, n, tr.Totals.Unsound, tr.Totals.Loose, tr.Totals.NoRecency, tr.Totals.NoTruth, tr.Totals.OtherTTL, tr.UnknownEdges))
		}
	}
	return res, nil
}

// checkKind reads every sampled instant at the token tokenOf gives it.
func (p *pipeline) checkKind(ctx context.Context, cfg config, kind string, tokenOf func(i int) uint64) (tokenResult, error) {
	pol := cfg.Policy
	edges := p.truth.edges()
	rows := make(map[store.Subject]*edgeRow, len(edges))
	var sources []identity.Fingerprint
	for _, sub := range edges {
		rows[sub] = &edgeRow{Edge: p.label(sub), Sightings: p.truth.sightings(sub)}
		if !slices.Contains(sources, sub.A) {
			sources = append(sources, sub.A)
		}
	}
	out := tokenResult{Kind: kind}
	if len(sources) == 0 {
		return out, nil
	}
	h := pol.HalfLife(p.expiry)
	rd := interval.NewReader(p.st, interval.Options{Evidence: true})
	for i := range p.tokens {
		at := p.sample0.Add(time.Duration(i) * p.step)
		token := tokenOf(i)
		_, els, err := rd.EdgesBatch(ctx, sources, store.Forward, interval.Window{From: at.Add(-cfg.Lookback), To: at.Add(time.Nanosecond), AsOf: token})
		if err != nil {
			return tokenResult{}, fmt.Errorf("%s: the read at %s (token %d): %w", kind, at.Format(time.RFC3339Nano), token, err)
		}
		for _, list := range els {
			for _, el := range list {
				if el.Subject.Kind != store.SubjectEdge || el.Subject.Relation != catalog.DependsOn || len(el.Last) == 0 {
					continue
				}
				row := rows[el.Subject]
				if row == nil {
					out.UnknownEdges++
					continue
				}
				p.observe(row, &out, el, at, token, pol, h)
			}
		}
	}
	for _, sub := range edges {
		out.Totals.add(*rows[sub])
		out.Edges = append(out.Edges, *rows[sub])
	}
	out.Totals.Edge = "all"
	slices.SortStableFunc(out.Edges, func(a, b edgeRow) int {
		switch {
		case a.Edge < b.Edge:
			return -1
		case a.Edge > b.Edge:
			return 1
		}
		return 0
	})
	return out, nil
}

// observe judges one member: an edge with evidence at the instant.
func (p *pipeline) observe(row *edgeRow, out *tokenResult, el interval.Element, at time.Time, token uint64, pol decay.Policy, h time.Duration) {
	row.Members++
	for _, e := range el.Last {
		if e.TTL != p.expiry {
			row.OtherTTL++
		}
	}
	rec, ok := decay.Of(el.Last, at, pol)
	if !ok {
		row.NoRecency++
		return
	}
	// The true silence of the element is its freshest producer's: the time since the
	// producer's latest sighting at or before the instant that the token knows of.
	trueSilence := time.Duration(1<<63 - 1)
	var x time.Duration
	for _, e := range el.Last {
		seen, ok := p.truth.lastBy(tkey{sub: el.Subject, prod: e.Producer}, at, token)
		if !ok {
			row.NoTruth++
			return
		}
		trueSilence = min(trueSilence, at.Sub(seen))
		x = max(x, pol.Extension(e.TTL))
	}
	bound := rec.SilentAtLeast
	if bound > 0 {
		row.Shown++
	}
	if trueSilence > x {
		row.TrueOverX++
	}
	if bound > trueSilence {
		row.Unsound++
	} else {
		short := trueSilence - bound
		row.MaxShort = max(row.MaxShort, short)
		if short > x {
			row.Loose++
		}
	}
	row.MaxBound = max(row.MaxBound, bound)
	row.MaxTrue = max(row.MaxTrue, trueSilence)
	out.SilenceBins[silenceBin(bound, h)]++
}

// silenceBin is the index of a bound in the bins of half-lives.
func silenceBin(bound, h time.Duration) int {
	switch {
	case bound <= 0:
		return 0
	case h <= 0:
		return 5
	}
	return min(1+int(bound/h), 5)
}
