package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/store"
)

func TestCheckProducerLen(t *testing.T) {
	t.Parallel()

	if store.MaxProducerLen >= workload.MaxProducerLen {
		t.Fatalf("store.MaxProducerLen %d is not below workload.MaxProducerLen %d: there is nothing to refuse", store.MaxProducerLen, workload.MaxProducerLen)
	}
	for _, tc := range []struct {
		name  string
		names []string
		n     int
		want  string // a part of the error, or "" for none
	}{
		{"no length", []string{"Lroot/off"}, 0, ""},
		{"the least length", []string{"Lroot/off"}, workload.MinProducerLen, ""},
		{"the limit, for a root candidate", []string{"Lroot/off"}, store.MaxProducerLen, ""},
		{"one over the limit, for a root candidate", []string{"Lroot/off"}, store.MaxProducerLen + 1, "-producer-len 33 is above 32"},
		{"the format bound, for a root candidate", []string{"Lroot/k64a2l1ns"}, workload.MaxProducerLen, "Lroot/k64a2l1ns cannot be built"},
		{"over the limit, for the spike's layouts", []string{"L/off", "L/k64a4", "M/crdb1+filter"}, workload.MaxProducerLen, ""},
		{"over the limit, for a root candidate among others", []string{"L/off", "Lroot/off", "M/default"}, 48, "Lroot/off cannot be built"},
		{"over the limit, for a name that is no candidate", []string{"nonsense"}, 48, ""},
	} {
		err := checkProducerLen(tc.names, tc.n)
		switch {
		case tc.want == "" && err != nil:
			t.Errorf("%s: refused: %v", tc.name, err)
		case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
			t.Errorf("%s: err = %v, want one with %q", tc.name, err, tc.want)
		}
	}
}

// A run that names a root candidate is refused before it plans anything, so that no
// plan is left behind for a length that cannot be built.
func TestRunRefusesALongProducerForARootCandidateBeforePlanning(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "out")
	err := doRun(context.Background(), []string{"-untimed", "-producer-len", "33", "-candidates", "L/off,Lroot/off", "-out", out})
	if err == nil || !strings.Contains(err.Error(), "-producer-len 33 is above 32") {
		t.Fatalf("doRun = %v, want a refusal of -producer-len 33", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, planFile)); statErr == nil {
		t.Error("a plan was made before the refusal")
	}
}

// A build of a root candidate from a plan with a long producer is refused before
// anything is written, and the same plan is not refused for the other layouts.
func TestBuildRefusesAPlanWithALongProducerForARootCandidate(t *testing.T) {
	t.Parallel()

	out := filepath.Join(t.TempDir(), "out")
	ctx := context.Background()
	plan := []string{"-untimed", "-preset", "tiny", "-retain", "12m/6m", "-batch", "200", "-min-non-empty", "0", "-producer-len", "40", "-out", out}
	if err := doPlan(ctx, plan); err != nil {
		t.Fatal(err)
	}
	err := doBuild(ctx, []string{"-untimed", "-candidate", "Lroot/off", "-out", out})
	if err == nil || !strings.Contains(err.Error(), "-producer-len 40 is above 32") {
		t.Fatalf("doBuild = %v, want a refusal of -producer-len 40", err)
	}
	if _, statErr := os.Stat(filepath.Join(out, "Lroot_off")); statErr == nil {
		t.Error("the refused build left a directory")
	}
}
