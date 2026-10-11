package main

import (
	"os"
	"testing"
	"time"
)

func loadWorkload(t *testing.T) *workload {
	t.Helper()
	f, err := os.Open("testdata/workload.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	wl, err := parseWorkload(f)
	if err != nil {
		t.Fatal(err)
	}
	return wl
}

func TestParseWorkload(t *testing.T) {
	wl := loadWorkload(t)
	if len(wl.Runs) != 3 || wl.Runs[0].N != 1 || !wl.Runs[0].HasStart || !wl.Runs[0].HasStop || wl.Runs[0].Lines != 10 || wl.Runs[0].Bytes != 1000 || wl.Runs[0].T0 != 1000 {
		t.Errorf("runs: %+v", wl.Runs)
	}
	if wl.Runs[2].HasStop != true || wl.Runs[1].Start.Format("15:04") != "12:05" {
		t.Errorf("runs: %+v", wl.Runs)
	}
	if len(wl.Stops) != 2 || wl.Stops[0].Name != "S5" || len(wl.Stops[0].Pods) != 2 || wl.Stops[0].Pods[1] != "web-2" || wl.Stops[1].Name != "S12-early" {
		t.Errorf("stops: %+v", wl.Stops)
	}
	if len(wl.API) != 3 {
		t.Fatalf("%d API restarts, want 3", len(wl.API))
	}
	for i, a := range wl.API {
		if a.Label != "API restart 3" || a.Ordinal != i+1 || len(a.Pods) != 1 {
			t.Errorf("API restart %d: %+v (the log says 3 every time; the order tells them apart)", i+1, a)
		}
	}
	if wl.OldUID != "1c427a35-bffd-4c62-90a8-287ebb932e0a" || wl.NewUID != "d43640a7-2ab6-4e47-9765-fe8361ec9020" {
		t.Errorf("agent 2: %q %q", wl.OldUID, wl.NewUID)
	}
	if wl.FirstScaleUp.Format("15:04") != "15:00" || wl.Running400.Format("15:04") != "15:01" || wl.ScaleDown.Format("15:04") != "15:10" || wl.Teardown.Format("15:04") != "15:20" {
		t.Errorf("full-pull phase: %v %v %v %v", wl.FirstScaleUp, wl.Running400, wl.ScaleDown, wl.Teardown)
	}
}

func TestWindowAttributionTellsAPIRestartsApartByTime(t *testing.T) {
	wl := loadWorkload(t)
	ws := wl.windows()
	var names []string
	for _, w := range ws {
		names = append(names, w.Name)
	}
	want := []string{"S5", "S12-early", "API restart 1", "API restart 2", "API restart 3", windowNode, windowFullPull, windowTeardown}
	if len(names) != len(want) {
		t.Fatalf("windows %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("windows %v, want %v", names, want)
		}
	}
	at := func(hm string) time.Time {
		v, err := time.Parse(time.RFC3339, "2026-10-10T"+hm+":00Z")
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	for _, c := range []struct{ when, want string }{
		{"11:59", windowSteady},
		{"12:00", "S5"},
		{"12:19", "S5"}, // 15 minutes after run 2 started at 12:05
		{"12:20", windowSteady},
		{"12:45", "S12-early"},
		{"13:07", windowSteady}, // run 3 started at 12:52
		{"13:41", "API restart 1"},
		{"13:49", "API restart 2"}, // the first window runs to 13:50; the later start wins
		{"13:57", "API restart 3"},
		{"14:03", windowNode}, // the third window runs to 14:06; the node scenario started at 14:00
		{"14:20", windowNode}, // agent 2 was back at 14:09, and the scenario lasts 15 minutes more
		{"14:30", windowSteady},
		{"15:05", windowFullPull},
		{"15:19", windowFullPull},
		{"15:25", windowTeardown},
	} {
		if got := attribute(ws, at(c.when)); got != c.want {
			t.Errorf("%s is in %q, want %q", c.when, got, c.want)
		}
	}
	// A stop window knows which run stopped before it.
	if ws[0].StopOf != 1 || ws[1].StopOf != 2 {
		t.Errorf("stop windows: %+v %+v", ws[0], ws[1])
	}
}
