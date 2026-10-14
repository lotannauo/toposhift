package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/activity"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/memstore"
)

// span is a half-open interval of existence.
type span struct{ start, end time.Time }

// truth is the existence of every subject as its raw assertions say it, by the
// lifecycle's rule, computed with no coalescer and no store: a subject exists
// from an assertion to its deadline, and assertions at most one TTL apart run
// together. It is the plain restatement the stored stream is held to.
func truth(raw []rawAssertion) map[store.Subject]*subjectTruth {
	out := make(map[store.Subject]*subjectTruth)
	for _, a := range raw {
		tr := out[a.subject]
		if tr == nil {
			tr = &subjectTruth{}
			out[a.subject] = tr
		}
		if n := len(tr.samples); n > 0 && !a.at.After(tr.samples[n-1].Add(a.ttl)) {
			tr.ivs[len(tr.ivs)-1].end = a.at.Add(a.ttl)
		} else {
			tr.ivs = append(tr.ivs, span{a.at, a.at.Add(a.ttl)})
		}
		tr.samples = append(tr.samples, a.at)
	}
	return out
}

// subjectTruth is the intervals of one subject and the instants it was asserted.
type subjectTruth struct {
	ivs     []span
	samples []time.Time
}

func aliveIn(ivs []span, t time.Time) bool {
	for _, iv := range ivs {
		if !t.Before(iv.start) && t.Before(iv.end) {
			return true
		}
	}
	return false
}

// verifyStream reads an activity file with the activity reader, writes it into
// an in-memory store and asks the store, for every pod, node and edge, at every
// sample time and at each interval's ends, whether it exists. Every answer must
// be the raw assertions'. It also refuses a stream that deletes anything: a
// sampled source has no deletes. It returns how many questions it asked.
func verifyStream(path string, raw []rawAssertion) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return 0, err
	}
	rd, err := activity.NewReader(f, st.Size(), activity.ReaderOptions{})
	if err != nil {
		return 0, err
	}
	defer rd.Close()
	ms, err := memstore.Open(memstore.Options{})
	if err != nil {
		return 0, err
	}
	defer ms.Close()
	ctx := context.Background()
	batch := make([]store.Record, 0, 4096)
	var n int64
	flush := func() error {
		err := ms.Write(ctx, batch)
		batch = batch[:0]
		return err
	}
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, err
		}
		if rec.Kind == lifecycle.Delete {
			return 0, fmt.Errorf("seq %d is a Delete: a sampled source has none, and absence is never a Delete", rec.Seq)
		}
		n++
		batch = append(batch, rec)
		if len(batch) == cap(batch) {
			if err := flush(); err != nil {
				return 0, err
			}
		}
	}
	if err := flush(); err != nil {
		return 0, err
	}
	if info := rd.Info(); info.Records != n {
		return 0, fmt.Errorf("the file says %d records and holds %d", info.Records, n)
	}

	layers := make(map[store.Subject]catalog.Layer)
	for _, a := range raw {
		layers[a.subject] = a.layer
	}
	var asked int64
	var bad []string
	for sub, tr := range truth(raw) {
		ivs := tr.ivs
		times := slices.Clone(tr.samples)
		for _, iv := range ivs {
			times = append(times, iv.start.Add(-1), iv.start, iv.end.Add(-1), iv.end)
		}
		slices.SortFunc(times, func(a, b time.Time) int { return a.Compare(b) })
		times = slices.CompactFunc(times, func(a, b time.Time) bool { return a.Equal(b) })
		for _, t := range times {
			got, err := exists(ctx, ms, sub, layers[sub], t)
			if err != nil {
				return asked, err
			}
			asked++
			if want := aliveIn(ivs, t); got != want && len(bad) < 10 {
				bad = append(bad, fmt.Sprintf("%v at %s: the store says %t, the samples say %t", describe(sub), t.Format(time.RFC3339Nano), got, want))
			}
		}
	}
	if len(bad) > 0 {
		return asked, fmt.Errorf("the stored stream differs from its samples:\n  %s", strings.Join(bad, "\n  "))
	}
	return asked, nil
}

func describe(s store.Subject) string {
	if s.Kind == store.SubjectEntity {
		return s.A.String()
	}
	return fmt.Sprintf("%s -%s-> %s", s.A, s.Relation, s.B)
}

// exists asks the store whether a subject exists at t.
func exists(ctx context.Context, ms *memstore.Store, sub store.Subject, layer catalog.Layer, t time.Time) (bool, error) {
	sc := store.Current(layer)
	if sub.Kind == store.SubjectEntity {
		return ms.Alive(ctx, sub.A, t, sc)
	}
	ns, err := ms.Neighbors(ctx, sub.A, store.Forward, t, sc)
	if err != nil {
		return false, err
	}
	return slices.Contains(ns, store.Neighbor{Peer: sub.B, Relation: sub.Relation}), nil
}
