package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/lotannauo/toposhift/internal/query/decay"
)

// config is what a replay is made of: the parts of the capture, the window, the
// instants sampled, and the expiries, each of which is a pipeline of its own.
type config struct {
	Parts []string
	// From and To are the window: lines whose newest point is at or after To are not
	// sent, and the instants sampled are From, From+Step, and so on, below To. The
	// lines before From are sent: they are the history the translators and the
	// coalescer need.
	From, To time.Time
	Step     time.Duration
	// Lookback is how far before an instant a read of it starts: zero reads the one
	// instant, so every reference comes from its seed.
	Lookback time.Duration
	Expiries []time.Duration
	Policy   decay.Policy
	// Progress, if not nil, gets a line every progressEvery lines.
	Progress io.Writer
}

const progressEvery = 50_000

// lineStats counts the lines of the capture and what became of each.
type lineStats struct {
	Parts int `json:"parts"`
	// Lines are all the lines read, Sent those given to the pipelines, and
	// AtOrAfterTo those whose time is at or after the window's end. NoTime are lines
	// with no data point time (sent at the instant of the newest line before them;
	// one before any line with a time is not sent), and OutOfOrder are lines sent
	// whose time is before an earlier line's.
	Lines       int    `json:"lines"`
	Sent        int    `json:"sent"`
	AtOrAfterTo int    `json:"at_or_after_to"`
	NoTime      int    `json:"no_time"`
	OutOfOrder  int    `json:"out_of_order"`
	First       string `json:"first_line_time"`
	Last        string `json:"newest_line_time"`
}

// replay sends the capture, once, to one pipeline for each expiry, in lockstep, and
// then checks each against the truth it kept. A pipeline is stopped when its result
// is made, or when anything fails.
func replay(ctx context.Context, cfg config) (_ lineStats, _ []runResult, err error) {
	var st lineStats
	st.Parts = len(cfg.Parts)
	var pls []*pipeline
	defer func() {
		if err != nil {
			for _, p := range pls {
				p.abort()
			}
		}
	}()
	var newest time.Time
	err = readLines(cfg.Parts, func(l metricsLine) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		st.Lines++
		if cfg.Progress != nil && st.Lines%progressEvery == 0 {
			_, _ = fmt.Fprintf(cfg.Progress, "decaycheck: %d lines\n", st.Lines)
		}
		at := l.Time
		switch {
		case at.IsZero():
			st.NoTime++
			if newest.IsZero() {
				return nil
			}
			at = newest
		case !at.Before(cfg.To):
			st.AtOrAfterTo++
			return nil
		}
		if pls == nil {
			sampleStart := cfg.From
			if at.After(sampleStart) {
				k := (at.Sub(cfg.From) + cfg.Step - 1) / cfg.Step
				sampleStart = cfg.From.Add(k * cfg.Step)
			}
			for _, e := range cfg.Expiries {
				p, err := newPipeline(e, at, sampleStart, cfg.Step)
				if err != nil {
					return fmt.Errorf("expiry %s: %w", e, err)
				}
				pls = append(pls, p)
			}
			st.First = at.Format(time.RFC3339Nano)
		}
		if at.Before(newest) {
			st.OutOfOrder++
		} else {
			newest = at
		}
		for _, p := range pls {
			if err := p.advance(ctx, at); err != nil {
				return fmt.Errorf("expiry %s: %w", p.expiry, err)
			}
			if err := p.post(ctx, l.Body, at); err != nil {
				return fmt.Errorf("expiry %s: %s line %d: %w", p.expiry, l.Part, l.Number, err)
			}
		}
		st.Sent++
		st.Last = newest.Format(time.RFC3339Nano)
		return nil
	})
	if err != nil {
		return st, nil, err
	}
	if pls == nil {
		return st, nil, errors.New("no line of the capture is before the end of the window")
	}
	var results []runResult
	for _, p := range pls {
		if err := p.advance(ctx, cfg.To.Add(-time.Nanosecond)); err != nil {
			return st, nil, fmt.Errorf("expiry %s: %w", p.expiry, err)
		}
		r, err := p.result(ctx, cfg)
		if err != nil {
			return st, nil, fmt.Errorf("expiry %s: %w", p.expiry, err)
		}
		results = append(results, r)
	}
	// From here the pipelines are stopped one by one, so the deferred abort has
	// none left to stop.
	rest := pls
	pls = nil
	for len(rest) > 0 {
		p := rest[0]
		rest = rest[1:]
		if err := p.finish(); err != nil {
			for _, q := range rest {
				q.abort()
			}
			return st, nil, fmt.Errorf("expiry %s: %w", p.expiry, err)
		}
		_ = p.st.Close()
	}
	return st, results, nil
}
