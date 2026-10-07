package runner

import (
	"context"
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/conformance"
	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/pebblekv"
	"github.com/lotannauo/toposhift/bench/spike/pebblelog"
	"github.com/lotannauo/toposhift/bench/spike/pebblemvcc"
	"github.com/lotannauo/toposhift/bench/spike/workload"
)

// settlingSink is a writer slow enough for the compactions to keep up: it waits for
// them after every batch.
type settlingSink struct{ buildSink }

func (s settlingSink) Write(batch []engine.Record) error {
	if err := s.buildSink.Write(batch); err != nil {
		return err
	}
	return s.e.(engine.Settler).Settle()
}

// tinyBuild is what a build of a plan into tiny tables leaves, as the reads after
// the compaction see it.
type tinyBuild struct {
	tables map[string]int64            // live_table_bytes and tables_l*
	blocks map[string]map[string]int64 // per query: block_loads and cold_block_bytes
	steps  map[string]map[string]int64 // per query: the counters of what it stepped over
	wrong  []string                    // queries answered otherwise than the plan
	seq    uint64                      // the engine's sequence number, reopened
}

// buildTiny writes the plan to a layout opened with tables of a few kilobytes, so
// the data spans many tables. slow waits for the compactions after every batch;
// otherwise the writer runs ahead of them as a build does. canonical rewrites the
// compacted tables.
func buildTiny(t *testing.T, plan *Plan, layout string, slow, canonical bool) tinyBuild {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	fs := vfs.NewMem()
	open := func(rec engine.Recorder, cfg pebblekv.Config) measurable {
		cfg.FS, cfg.Tuning, cfg.DisableReadCompactions = fs, pebblekv.TinyTuning(), true
		var e measurable
		var err error
		switch layout {
		case "L":
			e, err = pebblelog.Open("db", pebblelog.Options{Config: cfg, Recorder: rec, Checkpoints: pebblelog.CheckpointOptions{On: true, KMin: 8, Alpha: 1}})
		case "M":
			cfg.Schema = pebblekv.SchemaCRDB
			e, err = pebblemvcc.Open("db", pebblemvcc.Options{Config: cfg, Recorder: rec})
		}
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open(nil, pebblekv.Config{})
	after := false
	var sink Sink = buildSink{ctx: ctx, e: e, t: &Timing{}, afterRetention: &after}
	if slow {
		sink = settlingSink{sink.(buildSink)}
	}
	if _, err := Drive(ctx, plan.Spec, sink); err != nil {
		t.Fatal(err)
	}
	if err := e.CompactAll(ctx); err != nil {
		t.Fatal(err)
	}
	if canonical {
		if err := e.(engine.Canonicalizer).Canonicalize(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	rec := NewCapture()
	r := open(rec, pebblekv.Config{ReadOnly: true, DisableAutoCompactions: true})
	defer func() { _ = r.Close() }()
	out := tinyBuild{tables: map[string]int64{}, blocks: map[string]map[string]int64{}, steps: map[string]map[string]int64{}, seq: r.LastSeq()}
	for k, v := range r.Stats() {
		if k == "live_table_bytes" || strings.HasPrefix(k, "tables_l") {
			out.tables[k] = v
		}
	}
	for _, q := range plan.Queries {
		rec.Take()
		a, err := Ask(r, q)
		if err != nil {
			t.Fatal(err)
		}
		if a.Digest != q.Expect {
			out.wrong = append(out.wrong, q.Name())
		}
		steps := map[string]int64{}
		for k, v := range rec.Take() {
			if !strings.Contains(k, "block_") && !strings.Contains(k, "separated_value") && !strings.HasSuffix(k, ".value_bytes") {
				steps[k] = v
			}
		}
		out.steps[q.Name()] = steps
		_, cold, err := coldAsk(r, rec, q)
		if err != nil {
			t.Fatal(err)
		}
		out.blocks[q.Name()] = map[string]int64{CounterBlockLoads: cold[CounterBlockLoads], CounterColdBlockBytes: cold[CounterColdBlockBytes]}
	}
	return out
}

// differ counts the queries whose counters differ between two builds.
func differ(a, b map[string]map[string]int64) int {
	n := 0
	for q, c := range a {
		if !maps.Equal(c, b[q]) {
			n++
		}
	}
	return n
}

// Two builds of one plan, one slow enough for the compactions to keep up and one
// that runs ahead of them, leave the same tables and the same blocks for every read
// once canonicalized, while CompactAll alone leaves them different. Either way the
// answers are the plan's, and what each read steps over is the same with and
// without the rewrite, as are the engine's sequence number.
func TestCanonicalBuildsDoNotDependOnTheirSpeed(t *testing.T) {
	t.Parallel()
	conformance.SkipWhenTrimmed(t)

	spec := DefaultSpec(workload.Tiny())
	spec.BatchSize = 64
	spec.Retentions = []Retention{{At: 12 * time.Minute, Keep: 6 * time.Minute}}
	spec.MinNonEmpty = 0
	plan, err := MakePlan(context.Background(), spec, nil)
	if err != nil {
		t.Fatal(err)
	}
	plainDiffers := false
	for _, layout := range []string{"L", "M"} {
		slow, fast := buildTiny(t, plan, layout, true, false), buildTiny(t, plan, layout, false, false)
		cslow, cfast := buildTiny(t, plan, layout, true, true), buildTiny(t, plan, layout, false, true)
		for name, b := range map[string]tinyBuild{"slow": slow, "fast": fast, "slow canonical": cslow, "fast canonical": cfast} {
			if len(b.wrong) > 0 {
				t.Errorf("%s %s: %d answers are not the plan's, first %s", layout, name, len(b.wrong), b.wrong[0])
			}
			if b.seq != plan.Stream.LastSeq {
				t.Errorf("%s %s: opens at seq %d, the plan ends at %d", layout, name, b.seq, plan.Stream.LastSeq)
			}
		}
		if !maps.Equal(cslow.tables, cfast.tables) || differ(cslow.blocks, cfast.blocks) > 0 {
			t.Errorf("%s canonical: tables %v and %v, %d of %d queries load other blocks", layout, cslow.tables, cfast.tables, differ(cslow.blocks, cfast.blocks), len(plan.Queries))
		}
		if cslow.tables["tables_l6"] < 2 {
			t.Errorf("%s canonical: %d tables, the comparison shows little", layout, cslow.tables["tables_l6"])
		}
		for name, b := range map[string]tinyBuild{"fast": fast, "slow canonical": cslow, "fast canonical": cfast} {
			if n := differ(slow.steps, b.steps); n > 0 {
				t.Errorf("%s: %d queries step otherwise in the %s build than in the slow one", layout, n, name)
			}
		}
		d := differ(slow.blocks, fast.blocks)
		t.Logf("%s plain: tables %v and %v, %d of %d queries load other blocks", layout, slow.tables, fast.tables, d, len(plan.Queries))
		t.Logf("%s canonical: tables %v", layout, cslow.tables)
		plainDiffers = plainDiffers || d > 0 || !maps.Equal(slow.tables, fast.tables)
	}
	if !plainDiffers {
		t.Error("CompactAll alone left the same tables at both speeds: the comparison shows nothing")
	}
}
