package k8scluster_test

import (
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
)

func nsEv(uid, name string, at time.Time) ent {
	return ent{Type: "k8s.namespace", IDKey: "k8s.namespace.uid", ID: uid, Time: at, Desc: map[string]string{"k8s.namespace.name": name}}
}

func TestKubeSystemSightings(t *testing.T) {
	// The fixture's kube-system namespace and its sibling.
	in := loadFixture(t, "run1.jsonl")
	_, kube := find(t, in, "of kube-system", func(x input) bool {
		return isEvent(x, "entity.state", "k8s.namespace", "") && descOf(x.Rec, "k8s.namespace.name") == "kube-system"
	})
	_, def := find(t, in, "of default", func(x input) bool {
		return isEvent(x, "entity.state", "k8s.namespace", "") && descOf(x.Rec, "k8s.namespace.name") == "default"
	})
	tr := newTranslator(t)

	r, err := tr.Translate(kube.Res, kube.Rec, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Records) != 0 || len(r.Namespaces) != 1 {
		t.Fatalf("kube-system gave %d records and sightings %+v, want no record and one sighting", len(r.Records), r.Namespaces)
	}
	s := r.Namespaces[0]
	at := instantOf(kube.Rec)
	if s.UID != uidOf(kube.Rec) || s.Deleted || !s.At.Equal(at) || s.Interval != time.Minute {
		t.Errorf("sighting = %+v, want %s at %s with an interval of 1m", s, uidOf(kube.Rec), at)
	}
	if got, want := s.AliveUntil(), at.Add(3*time.Minute); !got.Equal(want) {
		t.Errorf("AliveUntil = %s, want %s", got, want)
	}
	if skipCount(r, k8scluster.SkipTypeNotInCatalog) != 1 {
		t.Errorf("skips = %v: the namespace is still not in the catalog", r.Skipped)
	}

	// The default namespace gives none.
	r, err = tr.Translate(def.Res, def.Rec, time.Time{})
	if err != nil || len(r.Namespaces) != 0 {
		t.Errorf("default gave sightings %+v, %v", r.Namespaces, err)
	}

	// The delete of that UID is a sighting; there is no name in a delete.
	delAt := at.Add(10 * time.Minute)
	r = translate(t, tr, deleteEv("k8s.namespace", uidOf(kube.Rec), delAt))
	if len(r.Records) != 0 || len(r.Namespaces) != 1 {
		t.Fatalf("delete gave %d records and %+v", len(r.Records), r.Namespaces)
	}
	d := r.Namespaces[0]
	if d.UID != uidOf(kube.Rec) || !d.Deleted || !d.At.Equal(delAt) || d.Interval != 0 || !d.AliveUntil().Equal(delAt) {
		t.Errorf("deleted sighting = %+v, AliveUntil %s, want it to end at %s", d, d.AliveUntil(), delAt)
	}

	// The delete of a UID never seen as kube-system is none, whatever it is, and
	// so is the second delete of one that was.
	for _, uid := range []string{"never-seen", uidOf(kube.Rec), uidOf(def.Rec)} {
		r = translate(t, tr, deleteEv("k8s.namespace", uid, delAt.Add(time.Minute)))
		if len(r.Namespaces) != 0 {
			t.Errorf("delete of %s gave sightings %+v", uid, r.Namespaces)
		}
	}
	// A namespace seen as kube-system under two UIDs (a cluster rebuilt) is two
	// sightings, with the interval of each.
	r = translate(t, tr, nsEv("ks-2", "kube-system", t0).interval(120))
	if len(r.Namespaces) != 1 || r.Namespaces[0].Interval != 2*time.Minute || !r.Namespaces[0].AliveUntil().Equal(t0.Add(6*time.Minute)) {
		t.Errorf("interval 120: %+v", r.Namespaces)
	}
	// A state with no interval and no collection interval vouches without an end.
	r = translate(t, tr, nsEv("ks-3", "kube-system", t0).noInterval())
	if len(r.Namespaces) != 1 || !r.Namespaces[0].AliveUntil().IsZero() {
		t.Errorf("no interval: %+v, want no end", r.Namespaces)
	}
	// With a collection interval it takes that, as a TTL does, and ends.
	r = translate(t, newTranslator(t, collection(time.Minute)), nsEv("ks-3", "kube-system", t0).noInterval())
	if len(r.Namespaces) != 1 || r.Namespaces[0].Interval != time.Minute || !r.Namespaces[0].AliveUntil().Equal(t0.Add(3*time.Minute)) {
		t.Errorf("no interval at a collection interval of 1m: %+v, want AliveUntil at +3m", r.Namespaces)
	}
	// An interval of the event wins over the collection interval.
	r = translate(t, newTranslator(t, collection(time.Minute)), nsEv("ks-3", "kube-system", t0).interval(120))
	if len(r.Namespaces) != 1 || !r.Namespaces[0].AliveUntil().Equal(t0.Add(6*time.Minute)) {
		t.Errorf("interval 120 at a collection interval of 1m: %+v", r.Namespaces)
	}
	r = translate(t, tr, nsEv("ks-4", "kube-system", t0).interval(0))
	if len(r.Namespaces) != 1 || !r.Namespaces[0].AliveUntil().IsZero() {
		t.Errorf("interval 0: %+v, want no end", r.Namespaces)
	}
	// The instant is the one the clock rules give.
	r = translateAt(t, tr, nsEv("ks-5", "kube-system", t0.Add(time.Hour)), t0)
	if len(r.Namespaces) != 1 || !r.Namespaces[0].At.Equal(t0) {
		t.Errorf("a clock ahead: %+v, want the receipt time", r.Namespaces)
	}
	// An unusable interval refuses the record.
	r = translate(t, tr, nsEv("ks-6", "kube-system", t0).intervalValue(sv("60")))
	if len(r.Namespaces) != 0 || skipCount(r, k8scluster.SkipBadInterval) != 1 {
		t.Errorf("bad interval: %+v, skips %v", r.Namespaces, r.Skipped)
	}
}
