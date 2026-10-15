package main

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/storetest"
)

// batchStream is a generated workload of about 25,000 records over three hours:
// enough that a batch of 1,000 and a batch of 10,000 cut it into different
// numbers of Writes (so the store decides where its checkpoints go at different
// places), with late records and deletes among them.
func batchStream(t *testing.T) (storetest.Config, []store.Record) {
	t.Helper()
	cfg := storetest.Tiny()
	cfg.Duration = 3 * time.Hour
	// Late by up to twice the checkpoint lag, so that some records delete
	// checkpoints, which a batch of 10,000 has placed at other instants.
	cfg.LateMax = 2 * storeOptions().Checkpoints.Lag
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, g.All()
}

// batchAnswers asks a store a fixed set of questions with the read API and
// returns every answer, errors included, as one line each.
func batchAnswers(ctx context.Context, t *testing.T, st store.Store, g *storetest.Generator, kinds map[string]int) []string {
	t.Helper()
	var fps []identity.Fingerprint
	for i, fp := range g.Entities() {
		if i%3 == 0 {
			fps = append(fps, fp)
		}
	}
	last := st.LastSeq()
	tokens := []uint64{last / 3, 2 * last / 3, last}
	times := []time.Time{g.Start(), g.Start().Add(5 * time.Minute), g.Start().Add(61 * time.Minute), g.Start().Add(119 * time.Minute), g.End()}
	layers := []catalog.Layer{catalog.L0, catalog.L1, catalog.L2, catalog.L3}
	var out []string
	add := func(op string, fp identity.Fingerprint, layer catalog.Layer, token uint64, detail string, v any, err error) {
		line := fmt.Sprintf("%s %s %s asof=%d %s: %v, err=%v", op, fp, layer, token, detail, v, err)
		out = append(out, line)
		if err == nil && !strings.HasPrefix(fmt.Sprint(v), "[]") && fmt.Sprint(v) != "false" {
			kinds[op]++
		}
	}
	for _, fp := range fps {
		for _, layer := range layers {
			for _, token := range tokens {
				sc := store.Scope{Layer: layer, AsOf: token}
				for _, at := range times {
					alive, err := st.Alive(ctx, fp, at, sc)
					add("alive", fp, layer, token, ts(at), alive, err)
					for _, dir := range []store.Direction{store.Forward, store.Reverse} {
						ns, err := st.Neighbors(ctx, fp, dir, at, sc)
						add("neighbors", fp, layer, token, fmt.Sprint(dir, " ", ts(at)), ns, err)
					}
				}
				from, to := times[1], times[3]
				for _, dir := range []store.Direction{store.Forward, store.Reverse} {
					rs, err := st.Window(ctx, fp, dir, from, to, sc)
					add("window", fp, layer, token, fmt.Sprint(dir), rs, err)
				}
				rs, err := st.EntityWindow(ctx, fp, times[0], times[4], sc)
				add("entitywindow", fp, layer, token, "", rs, err)
			}
		}
	}
	return out
}

// TestReplayBatchSizeDoesNotChangeAnswers replays one file, which has late
// records and deletes among about 25,000 records, at a batch of 1,000, at a
// batch of 10,000 and at the default, and asks the three stores the same
// questions. The batch size decides where the store places its checkpoints, not
// what the history says, so every answer and the last sequence number must be
// equal; and the default must be 10,000 records, which is three batches here.
func TestReplayBatchSizeDoesNotChangeAnswers(t *testing.T) {
	cfg, recs := batchStream(t)
	g, err := storetest.NewGenerator(cfg)
	if err != nil {
		t.Fatal(err)
	}
	var late, deletes, pastLag int
	lag := storeOptions().Checkpoints.Lag
	newest := map[store.Subject]time.Time{}
	for i, r := range recs {
		if i > 0 && r.EventTime.Before(recs[i-1].EventTime) {
			late++
		}
		// Behind the newest earlier record of its subject by more than the lag, so
		// behind its prefix's newest by more too: such a record deletes any
		// checkpoint placed after it.
		if n, ok := newest[r.Subject]; ok && r.EventTime.Before(n.Add(-lag)) {
			pastLag++
		}
		if r.EventTime.After(newest[r.Subject]) {
			newest[r.Subject] = r.EventTime
		}
		if r.Kind == lifecycle.Delete {
			deletes++
		}
	}
	if len(recs) < 20000 || len(recs) > 30000 || late == 0 || pastLag == 0 || deletes == 0 {
		t.Fatalf("the stream has %d records, %d out of event-time order (%d by more than the checkpoint lag) and %d deletes; want 20,000 to 30,000, and some of each", len(recs), late, pastLag, deletes)
	}

	dir := t.TempDir()
	file := filepath.Join(dir, "a.parquet")
	writeFile(t, file, recs)

	runs := []struct {
		name        string
		args        []string
		wantBatches int
	}{
		{"batch1000", []string{"--batch", "1000"}, (len(recs) + 999) / 1000},
		{"batch10000", []string{"--batch", "10000"}, (len(recs) + 9999) / 10000},
		{"default", nil, (len(recs) + 9999) / 10000},
	}
	var answers [][]string
	kinds := map[string]int{}
	for _, r := range runs {
		data := filepath.Join(dir, r.name)
		out := mustCLI(t, append([]string{"replay", "activity", file, "--data-dir", data}, r.args...)...)
		want := fmt.Sprintf("replayed %d records in %d batches, seq %d to %d, ", len(recs), r.wantBatches, recs[0].Seq, recs[len(recs)-1].Seq)
		if !strings.HasPrefix(out, want) {
			t.Errorf("%s: summary = %q; want it to start %q", r.name, out, want)
		}
		st, err := openPebble(data, true)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := st.LastSeq(), recs[len(recs)-1].Seq; got != want {
			t.Errorf("%s: last seq = %d, want %d", r.name, got, want)
		}
		answers = append(answers, batchAnswers(t.Context(), t, st, g, kinds))
		if err := st.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if runs[1].wantBatches != 3 || runs[2].wantBatches != 3 {
		t.Fatalf("the stream of %d records is %d and %d batches at 10,000; want 3, so that the default is told from 1,000", len(recs), runs[1].wantBatches, runs[2].wantBatches)
	}

	for i := 1; i < len(runs); i++ {
		if len(answers[i]) != len(answers[0]) {
			t.Fatalf("%s asked %d questions, %s %d", runs[i].name, len(answers[i]), runs[0].name, len(answers[0]))
		}
		wrong := 0
		for q := range answers[0] {
			if answers[i][q] != answers[0][q] {
				if wrong++; wrong <= 3 {
					t.Errorf("%s differs from %s\n got: %s\nwant: %s", runs[i].name, runs[0].name, answers[i][q], answers[0][q])
				}
			}
		}
		if wrong > 0 {
			t.Errorf("%s: %d of %d answers differ from %s", runs[i].name, wrong, len(answers[0]), runs[0].name)
		}
	}
	t.Logf("%d questions per store; answers that were not empty, per operation: %v; %d records, %d late (%d past the lag), %d deletes", len(answers[0]), kinds, len(recs), late, pastLag, deletes)
	for _, k := range []string{"alive", "neighbors", "window", "entitywindow"} {
		if kinds[k] == 0 {
			t.Errorf("no %s question had a non-empty answer: the comparison proved nothing about it", k)
		}
	}
}
