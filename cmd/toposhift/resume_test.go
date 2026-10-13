package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/resume"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// resumeBody is GET /status's resume object.
type resumeBody struct {
	Resume struct {
		Mode     string           `json:"mode"`
		Counters map[string]int64 `json:"counters"`
	} `json:"resume"`
}

func getResume(t *testing.T, base string) resumeBody {
	t.Helper()
	code, body := getBody(t, base+"/status")
	var r resumeBody
	if err := json.Unmarshal([]byte(body), &r); code != http.StatusOK || err != nil {
		t.Fatalf("GET /status = %d %q (%v)", code, body, err)
	}
	return r
}

// createdPodBody is one pull record of a pod created at created, as OTLP JSON.
func createdPodBody(uid string, created, observed time.Time) []byte {
	return fmt.Appendf(nil, `{"resourceLogs":[{"resource":{},"scopeLogs":[{"logRecords":[{"observedTimeUnixNano":"%d",`+
		`"body":{"kvlistValue":{"values":[{"key":"kind","value":{"stringValue":"Pod"}},{"key":"metadata","value":{"kvlistValue":{"values":[`+
		`{"key":"uid","value":{"stringValue":%q}},{"key":"name","value":{"stringValue":%q}},{"key":"namespace","value":{"stringValue":"default"}},`+
		`{"key":"creationTimestamp","value":{"stringValue":%q}}]}}},`+
		`{"key":"status","value":{"kvlistValue":{"values":[{"key":"phase","value":{"stringValue":"Running"}}]}}}]}},`+
		`"attributes":[{"key":"k8s.resource.name","value":{"stringValue":"pods"}}]}]}]}]}`,
		observed.UnixNano(), uid, uid, created.UTC().Format(time.RFC3339))
}

// The sequencer serve starts resumes from its store: GET /status reports resume
// on, under --retain off and on alike, with every counter; a pod's first sighting
// is new, and after a restart of serve the same pod's first sighting is raised to
// what the store holds.
func TestServeResumesFromItsStore(t *testing.T) {
	dir := t.TempDir()
	created := time.Now().Add(-time.Hour).Truncate(time.Second)
	s := startServe(t, dirEnv(t, dir), "--retain", "off")
	waitIngestOn(t, s.base)
	r := getResume(t, s.base)
	if r.Resume.Mode != "on" {
		t.Fatalf("resume %+v, want on", r.Resume)
	}
	for _, k := range []string{
		"new", "raised", "changed", "changed_gap", "dropped", "unknown", "unknown_exhausted",
		"unknown_baseline_only", "unknown_below_horizon", "unknown_error", "mirror_raised",
	} {
		if _, ok := r.Resume.Counters["sequencer.resume_"+k]; !ok {
			t.Errorf("no counter sequencer.resume_%s in %v", k, r.Resume.Counters)
		}
	}
	postOK(t, s.base, createdPodBody("pod-1", created, time.Now()))
	if n := getResume(t, s.base).Resume.Counters["sequencer.resume_new"]; n != 1 {
		t.Errorf("resume_new %d after a first sighting, want 1", n)
	}
	stopOK(t, s)

	s = startServe(t, dirEnv(t, dir))
	waitIngestOn(t, s.base)
	postOK(t, s.base, createdPodBody("pod-1", created, time.Now()))
	if c := getResume(t, s.base).Resume.Counters; c["sequencer.resume_raised"] != 1 || c["sequencer.resume_new"] != 0 {
		t.Errorf("counters %v after a restart, want the pod raised", c)
	}
	stopOK(t, s)
}

// A sequencer started with no lookup is logged as an error at boot, and GET
// /status says resume is disabled: it is never off silently.
func TestServeSaysResumeIsDisabled(t *testing.T) {
	setSeam(t, &lookupFor, func(*pebblestore.Store) func() (resume.Lookup, error) { return nil })
	s := startServe(t, dirEnv(t, t.TempDir()))
	waitIngestOn(t, s.base)
	if r := getResume(t, s.base); r.Resume.Mode != "disabled" {
		t.Errorf("resume %+v, want disabled", r.Resume)
	}
	stopOK(t, s)
	if !strings.Contains(s.stderr.String(), "resume is disabled") || !strings.Contains(s.stderr.String(), "level=ERROR") {
		t.Errorf("the log does not say resume is disabled, as an error:\n%s", s.stderr)
	}
}

// serve -h documents resume.
func TestServeHelpDocumentsResume(t *testing.T) {
	for _, s := range []string{"Resume", `"resume"`, "disabled", "raised", "unknown"} {
		if !strings.Contains(ingestHelp, s) {
			t.Errorf("the help does not say %q", s)
		}
	}
}
