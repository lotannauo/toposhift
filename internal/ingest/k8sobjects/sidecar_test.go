package k8sobjects_test

import (
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/ingest/k8sobjects"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
)

// A native sidecar is an init container with restartPolicy Always: it runs for the
// life of its pod, and its status is under initContainerStatuses. It is a container
// of the pod. A classic init container is not.

func running(at time.Time) map[string]any {
	return map[string]any{"running": map[string]any{"startedAt": ts(at)}}
}

func terminated(started, finished time.Time) map[string]any {
	return map[string]any{"terminated": map[string]any{"startedAt": ts(started), "finishedAt": ts(finished)}}
}

// sidecarPod is a pod with an app container, a classic init container that has
// finished, and a native sidecar.
func sidecarPod() podSpec {
	return podSpec{
		UID: "p1", Name: "p", Created: t0, Phase: "Running",
		Containers: []map[string]any{
			{"name": "app", "restartCount": 0, "containerID": "containerd://aaa", "state": running(t0.Add(time.Second))},
		},
		InitSpec: []map[string]any{
			{"name": "init"},
			{"name": "proxy", "restartPolicy": "Always"},
		},
		InitStatuses: []map[string]any{
			{"name": "init", "restartCount": 0, "containerID": "containerd://iii", "state": terminated(t0, t0.Add(time.Second))},
			{"name": "proxy", "restartCount": 0, "containerID": "containerd://sss", "state": running(t0.Add(2 * time.Second))},
		},
	}
}

// entities returns the container entities among the records, by subject.
func entities(t testing.TB, rs []store.Record) []store.Subject {
	t.Helper()
	var out []store.Subject
	for _, r := range rs {
		if r.Kind == lifecycle.Observe && r.Subject.Kind == store.SubjectEntity && r.Subject.A.Type() == catalog.Container {
			out = append(out, r.Subject)
		}
	}
	return out
}

// partOfs returns the part_of edges among the records, by subject.
func partOfs(rs []store.Record) []store.Subject {
	var out []store.Subject
	for _, r := range rs {
		if r.Kind == lifecycle.Observe && r.Subject.Kind == store.SubjectEdge && r.Subject.Relation == catalog.PartOf {
			out = append(out, r.Subject)
		}
	}
	return out
}

func podRecord(t testing.TB, r k8sobjects.Result) store.Record {
	t.Helper()
	for _, rec := range r.Records {
		if rec.Subject.Kind == store.SubjectEntity && rec.Subject.A.Type() == catalog.K8sPod {
			return rec
		}
	}
	t.Fatalf("no pod record in %s", joinSubjects(r.Records))
	return store.Record{}
}

// T2: the sidecar is a container entity, part of its pod, and the pod's description
// lists it with the marker. The classic init container is neither.
func TestNativeSidecarIsAContainerOfItsPod(t *testing.T) {
	tr := newTranslator(t, 0)
	r := translate(t, tr, pullRec(t0.Add(time.Minute), sidecarPod().object()))

	if len(r.Records) != 5 {
		t.Fatalf("got %d records (%s), want the pod, two containers and two part_of edges", len(r.Records), joinSubjects(r.Records))
	}
	pod := podFP(t, "p1")
	wantEnts := []store.Subject{store.EntitySubject(containerFP(t, "aaa")), store.EntitySubject(containerFP(t, "sss"))}
	if got := entities(t, r.Records); !sameSet(got, wantEnts) {
		t.Errorf("container entities = %v, want the app and the sidecar", got)
	}
	wantEdges := []store.Subject{
		store.EdgeSubject(containerFP(t, "aaa"), pod, catalog.PartOf),
		store.EdgeSubject(containerFP(t, "sss"), pod, catalog.PartOf),
	}
	if got := partOfs(r.Records); !sameSet(got, wantEdges) {
		t.Errorf("part_of edges = %v, want the app's and the sidecar's", got)
	}
	for _, rec := range r.Records {
		if rec.Subject == store.EntitySubject(containerFP(t, "iii")) {
			t.Errorf("the classic init container is an entity: %+v", rec)
		}
	}

	const want = `{"containers":[` +
		`{"name":"app","restart_count":0,"started_at":"2026-10-09T10:00:01Z","state":"running"},` +
		`{"name":"proxy","native_sidecar":true,"restart_count":0,"started_at":"2026-10-09T10:00:02Z","state":"running"}` +
		`],"name":"p","namespace":"default","phase":"Running"}`
	if got := string(podRecord(t, r).Payload); got != want {
		t.Errorf("pod payload\n got %s\nwant %s", got, want)
	}

	// The container entity's own description is the same for a sidecar as for any
	// container.
	for _, rec := range r.Records {
		if rec.Subject == store.EntitySubject(containerFP(t, "sss")) && string(rec.Payload) != `{"name":"proxy","pod_uid":"p1"}` {
			t.Errorf("sidecar entity payload = %s", rec.Payload)
		}
	}
}

func sameSet(a, b []store.Subject) bool {
	if len(a) != len(b) {
		return false
	}
	for _, s := range a {
		if !slices.Contains(b, s) {
			return false
		}
	}
	return true
}

// T3: a classic init container changes nothing: the pod's description and records
// are those of the same pod without it.
func TestClassicInitContainerIsNotDescribed(t *testing.T) {
	plain := podSpec{
		UID: "p1", Name: "p", Created: t0, Phase: "Running",
		Containers: []map[string]any{
			{"name": "app", "restartCount": 0, "containerID": "containerd://aaa", "state": running(t0.Add(time.Second))},
		},
	}
	withInit := plain
	withInit.InitSpec = []map[string]any{{"name": "init"}}
	withInit.InitStatuses = []map[string]any{
		{"name": "init", "restartCount": 0, "containerID": "containerd://iii", "state": terminated(t0, t0.Add(time.Second))},
	}
	want := translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), plain.object()))
	got := translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), withInit.object()))

	if string(podRecord(t, got).Payload) != string(podRecord(t, want).Payload) {
		t.Errorf("pod payload\n got %s\nwant %s", podRecord(t, got).Payload, podRecord(t, want).Payload)
	}
	if len(got.Records) != len(want.Records) || len(got.Records) != 3 {
		t.Errorf("got %d records (%s), want %d (%s)", len(got.Records), joinSubjects(got.Records), len(want.Records), joinSubjects(want.Records))
	}
	if len(got.Skipped) != 0 {
		t.Errorf("skips = %v, want none", got.Skipped)
	}
}

// T4: only the init container's own restart policy counts. The pod's restart
// policy never does, a spec entry with no status gives nothing, and the policy is
// matched exactly.
func TestOnlyTheInitContainersRestartPolicyCounts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   map[string]any
		status bool
	}{
		{"no policy of its own", map[string]any{"name": "proxy"}, true},
		{"another policy", map[string]any{"name": "proxy", "restartPolicy": "OnFailure"}, true},
		{"policy in another case", map[string]any{"name": "proxy", "restartPolicy": "always"}, true},
		{"always, with no status", map[string]any{"name": "proxy", "restartPolicy": "Always"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := sidecarPod()
			p.InitSpec = []map[string]any{tc.spec}
			p.InitStatuses = nil
			if tc.status {
				p.InitStatuses = []map[string]any{
					{"name": "proxy", "restartCount": 0, "containerID": "containerd://sss", "state": running(t0.Add(2 * time.Second))},
				}
			}
			o := p.object()
			// The pod's own policy, as every pod has.
			o["spec"].(map[string]any)["restartPolicy"] = "Always"
			r := translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), o))

			if len(r.Records) != 3 {
				t.Errorf("got %d records (%s), want the pod, the app and its edge", len(r.Records), joinSubjects(r.Records))
			}
			if len(r.Skipped) != 0 {
				t.Errorf("skips = %v, want none", r.Skipped)
			}
			var m struct {
				Containers []struct{ Name string }
			}
			if err := json.Unmarshal(podRecord(t, r).Payload, &m); err != nil {
				t.Fatal(err)
			}
			if len(m.Containers) != 1 || m.Containers[0].Name != "app" {
				t.Errorf("pod payload %s, want only the app", podRecord(t, r).Payload)
			}
		})
	}
}

// T5: a sidecar with no runtime id is described, not an entity, and counted as a
// skip like any container without one.
func TestSidecarWithoutIDIsSkipped(t *testing.T) {
	p := sidecarPod()
	delete(p.InitStatuses[1], "containerID")
	r := translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), p.object()))

	if len(r.Records) != 3 {
		t.Errorf("got %d records (%s), want the pod, the app and its edge", len(r.Records), joinSubjects(r.Records))
	}
	if got := skips(r)[k8sobjects.SkipContainerNoID]; got != 1 {
		t.Errorf("skipped %d containers without an id, want 1", got)
	}
	if got := len(skips(r)); got != 1 {
		t.Errorf("skips = %v, want only the one", skips(r))
	}
	var m struct {
		Containers []struct {
			Name    string
			Sidecar bool `json:"native_sidecar"`
		}
	}
	if err := json.Unmarshal(podRecord(t, r).Payload, &m); err != nil {
		t.Fatal(err)
	}
	if len(m.Containers) != 2 || m.Containers[1].Name != "proxy" || !m.Containers[1].Sidecar {
		t.Errorf("pod payload %s, want the sidecar described", podRecord(t, r).Payload)
	}

	// A runtime id with no runtime prefix is not trusted, for a sidecar either.
	q := sidecarPod()
	q.InitStatuses[1]["containerID"] = "sss"
	r = translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), q.object()))
	if len(r.Records) != 3 || skips(r)[k8sobjects.SkipBadContainerID] != 1 {
		t.Errorf("an id without a prefix gave %d records, skips %v", len(r.Records), skips(r))
	}
}

// T6: a restart of a sidecar gives it a new id, which ends the old container and
// its part_of edge and begins the new one. The new start is an object-field time.
func TestSidecarRestartReplacesItsContainer(t *testing.T) {
	tr := newTranslator(t, 0)
	translate(t, tr, pullRec(t0.Add(time.Minute), sidecarPod().object()))

	restarted := sidecarPod()
	restartedAt := t0.Add(30 * time.Minute)
	restarted.InitStatuses[1] = map[string]any{
		"name": "proxy", "restartCount": 1, "containerID": "containerd://sss2", "state": running(restartedAt),
	}
	r := translate(t, tr, watchRec(t0.Add(time.Hour), "MODIFIED", restarted.object()))

	var got []string
	for _, rec := range r.Records {
		got = append(got, rec.Kind.String()+" "+string(rec.Subject.Relation)+" "+string(rec.Subject.A.Type()))
	}
	want := []string{"observe  k8s.pod", "delete part_of container", "delete  container", "observe  container", "observe part_of container"}
	if !slices.Equal(got, want) {
		t.Fatalf("restart gave %q, want %q", got, want)
	}
	pod := podFP(t, "p1")
	oldC, newC := containerFP(t, "sss"), containerFP(t, "sss2")
	for i, s := range []store.Subject{
		store.EntitySubject(pod),
		store.EdgeSubject(oldC, pod, catalog.PartOf),
		store.EntitySubject(oldC),
		store.EntitySubject(newC),
		store.EdgeSubject(newC, pod, catalog.PartOf),
	} {
		if r.Records[i].Subject != s {
			t.Errorf("record %d is %+v, want %+v", i, r.Records[i].Subject, s)
		}
	}
	if rec := r.Records[0]; !rec.EventTime.Equal(restartedAt) || rec.EventTimeBasis != store.BasisObjectField {
		t.Errorf("pod event time %s (%s), want the sidecar's new startedAt %s", rec.EventTime, rec.EventTimeBasis, restartedAt)
	}
	if skips(r)[k8sobjects.SkipImplausibleTime] != 0 {
		t.Errorf("skips = %v", r.Skipped)
	}

	// The app is untouched: it was not re-asserted, ended or begun.
	for _, rec := range r.Records {
		if rec.Subject.A == containerFP(t, "aaa") {
			t.Errorf("the app container was touched: %+v", rec)
		}
	}
}

// T7: a pod without init containers is described by exactly these bytes, as before
// native sidecars were read, and so is its container.
func TestPodWithoutInitContainersIsDescribedAsBefore(t *testing.T) {
	p := podSpec{
		UID: "p1", Name: "p", Node: "node-a", Created: t0, Phase: "Running",
		Conds: []map[string]any{cond("Ready", "True", t0.Add(5*time.Second))},
		Containers: []map[string]any{
			{"name": "web", "restartCount": 2, "containerID": "containerd://aaa", "state": running(t0.Add(3 * time.Second))},
			{"name": "log", "restartCount": 0, "containerID": "containerd://bbb", "state": map[string]any{"waiting": map[string]any{"reason": "x"}}},
		},
		Extra: map[string]any{"ownerReferences": []any{map[string]any{"kind": "ReplicaSet", "name": "rs", "controller": true}}},
	}
	o := p.object()
	status := o["status"].(map[string]any)
	status["hostIP"] = "10.0.0.10"
	status["podIPs"] = []any{map[string]any{"ip": "10.0.1.1"}}
	r := translate(t, newTranslator(t, 0), pullRec(t0.Add(time.Minute), o))

	const wantPod = `{"conditions":[{"last_transition_time":"2026-10-09T10:00:05Z","status":"True","type":"Ready"}],` +
		`"containers":[{"name":"log","restart_count":0,"state":"waiting"},` +
		`{"name":"web","restart_count":2,"started_at":"2026-10-09T10:00:03Z","state":"running"}],` +
		`"host_ip":"10.0.0.10","name":"p","namespace":"default","node_name":"node-a",` +
		`"owner":{"kind":"ReplicaSet","name":"rs"},"phase":"Running","pod_ips":["10.0.1.1"]}`
	if got := string(podRecord(t, r).Payload); got != wantPod {
		t.Errorf("pod payload\n got %s\nwant %s", got, wantPod)
	}
	for _, rec := range r.Records {
		if rec.Subject.Kind == store.SubjectEntity && rec.Subject.A.Type() == catalog.Container {
			name := map[store.Subject]string{
				store.EntitySubject(containerFP(t, "aaa")): "web",
				store.EntitySubject(containerFP(t, "bbb")): "log",
			}[rec.Subject]
			if want := `{"name":"` + name + `","pod_uid":"p1"}`; string(rec.Payload) != want {
				t.Errorf("container payload %s, want %s", rec.Payload, want)
			}
		}
	}
}

// T8: the DELETED of a pod this translator has not seen ends its sidecar and the
// sidecar's part_of edge, as it ends its other containers.
func TestDeleteOfAnUnseenPodEndsItsSidecar(t *testing.T) {
	p := sidecarPod()
	got := translate(t, newTranslator(t, 0), watchRec(at(10, 0), "DELETED", p.object()))

	pod := podFP(t, "p1")
	var subjects []store.Subject
	for _, rec := range got.Records {
		if rec.Kind != lifecycle.Delete || !rec.EventTime.Equal(at(10, 0)) || rec.EventTimeBasis != store.BasisObserved {
			t.Errorf("record %+v is not a delete at the observed time", rec)
		}
		subjects = append(subjects, rec.Subject)
	}
	want := []store.Subject{
		store.EdgeSubject(containerFP(t, "aaa"), pod, catalog.PartOf),
		store.EntitySubject(containerFP(t, "aaa")),
		store.EdgeSubject(containerFP(t, "sss"), pod, catalog.PartOf),
		store.EntitySubject(containerFP(t, "sss")),
		store.EntitySubject(pod),
	}
	if !sameSet(subjects, want) {
		t.Errorf("got deletes of %v, want the app, the sidecar, their edges and the pod", subjects)
	}
	if len(got.Skipped) != 0 {
		t.Errorf("skips = %v, want none", got.Skipped)
	}
}

// T9: with a TTL, each pull re-asserts the sidecar and its part_of edge as it does
// any container.
func TestSidecarIsReassertedWithItsPod(t *testing.T) {
	const ttl = 15 * time.Minute
	tr := newTranslator(t, ttl)
	p := sidecarPod()
	first := translate(t, tr, pullRec(t0.Add(time.Minute), p.object()))
	if len(first.Records) != 5 {
		t.Fatalf("first pull gave %d records (%s), want 5", len(first.Records), joinSubjects(first.Records))
	}

	p.Heartbeat = "2"
	refreshAt := t0.Add(10 * time.Minute)
	r := translate(t, tr, pullRec(refreshAt, p.object()))
	if len(r.Records) != 5 {
		t.Fatalf("refresh gave %d records (%s), want the pod, two containers and two edges", len(r.Records), joinSubjects(r.Records))
	}
	for _, rec := range r.Records {
		if rec.Kind != lifecycle.Observe || rec.TTL != ttl || !rec.EventTime.Equal(refreshAt) || rec.EventTimeBasis != store.BasisObserved {
			t.Errorf("refresh record %+v, want an observation at the observed time with the TTL", rec)
		}
	}
	pod, sidecar := podFP(t, "p1"), containerFP(t, "sss")
	if got := entities(t, r.Records); !slices.Contains(got, store.EntitySubject(sidecar)) {
		t.Errorf("the sidecar was not re-asserted: %v", got)
	}
	if got := partOfs(r.Records); !slices.Contains(got, store.EdgeSubject(sidecar, pod, catalog.PartOf)) {
		t.Errorf("the sidecar's part_of was not re-asserted: %v", got)
	}
}
