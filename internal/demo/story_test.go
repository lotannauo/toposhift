package demo

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/identity"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/query/interval"
	"github.com/lotannauo/toposhift/internal/store"
)

// The tests of this file read the store the way the tools do, through the interval
// reader over the whole window, and check that what the script did is in it. The
// store was filled by the sequencer alone, so each of these is the translators'
// answer to the requests of section "The script": nothing is read from the world
// but the ids and the instants.

func reader(b *Built) (*interval.Reader, interval.Window) {
	return interval.NewReader(b.Store, interval.Options{}), interval.Window{From: anchor, To: anchor.Add(Span), AsOf: store.Latest}
}

func fpOfPod(uid string) identity.Fingerprint {
	return fingerprint(catalog.K8sPod, catalog.K8sPodUID, uid)
}

func fpOfNode(uid string) identity.Fingerprint {
	return fingerprint(catalog.K8sNode, catalog.K8sNodeUID, uid)
}

// edgesOf lists the edges of the entity in the direction that have a piece in the
// window, by relation and peer.
func edgesOf(t testing.TB, b *Built, fp identity.Fingerprint, dir store.Direction) map[[2]string]interval.Element {
	t.Helper()
	rd, win := reader(b)
	res, err := rd.Edges(context.Background(), fp, dir, win)
	if err != nil {
		t.Fatalf("Edges of %s: %v", fp, err)
	}
	out := map[[2]string]interval.Element{}
	for _, el := range res.Elements {
		peer := el.Subject.B
		if dir == store.Reverse {
			peer = el.Subject.A
		}
		out[[2]string{string(el.Subject.Relation), peer.String()}] = el
	}
	return out
}

func scheduledOn(t testing.TB, b *Built, uid string) map[string]interval.Element {
	t.Helper()
	out := map[string]interval.Element{}
	for k, el := range edgesOf(t, b, fpOfPod(uid), store.Forward) {
		if k[0] == string(catalog.ScheduledOn) {
			out[k[1]] = el
		}
	}
	return out
}

// Node-2's pods that a Deployment or a StatefulSet keeps are placed on it up to the
// instant the taint's seconds pass and not after: their scheduled_on edges to it
// end inside the minute that follows, by the producer's delete. Each replacement is
// on one of the other nodes, from after its predecessor went.
func TestEvictedPodsLeaveNode2AndReplacementsLandElsewhere(t *testing.T) {
	b := canonicalBuild(t)
	s := b.Story
	failed := s.Failed.ID
	// The instant the taint's seconds pass, written out and not read from the
	// script: three hours, and the five minutes.
	evict := anchor.Add(3*time.Hour + 5*time.Minute)
	if len(s.Evicted) < 6 || len(s.Evicted) != len(s.Replacements) {
		t.Fatalf("%d evicted pods and %d replacements", len(s.Evicted), len(s.Replacements))
	}
	for i, v := range s.Evicted {
		on := scheduledOn(t, b, v.UID)
		el, ok := on[failed]
		if !ok || len(on) != 1 {
			t.Fatalf("%s: scheduled_on edges to %v, want one to node-2", v.Name, keys(on))
		}
		last := el.Pieces[len(el.Pieces)-1]
		if last.End.Before(evict) || !last.End.Before(evict.Add(time.Minute)) {
			t.Errorf("%s: placed on node-2 until %s, want within the minute from %s", v.Name, last.End.Format(time.RFC3339), evict.Format(time.RFC3339))
		}
		if last.EndSource != lifecycle.EndProducer {
			t.Errorf("%s: its placement on node-2 ended by %v, want the producer's delete", v.Name, last.EndSource)
		}

		r := s.Replacements[i]
		on = scheduledOn(t, b, r.UID)
		if len(on) != 1 {
			t.Fatalf("%s: scheduled_on edges to %v, want one", r.Name, keys(on))
		}
		for peer, el := range on {
			if peer == failed {
				t.Errorf("%s is placed on node-2", r.Name)
			}
			known := false
			for _, n := range s.Nodes {
				known = known || n.ID == peer
			}
			if !known {
				t.Errorf("%s is placed on %s, which is not a node of the cluster", r.Name, peer)
			}
			if first := el.Pieces[0].Start; !first.After(v.Deleted) || el.Pieces[0].Before {
				t.Errorf("%s is placed from %s, want after %s went at %s", r.Name, first.Format(time.RFC3339), v.Name, v.Deleted.Format(time.RFC3339))
			}
			if end := el.Pieces[len(el.Pieces)-1].End; !end.Equal(s.End) {
				t.Errorf("%s is placed until %s, want to the end of the window", r.Name, end.Format(time.RFC3339))
			}
		}
	}
	// What a DaemonSet keeps on node-2 stays there for the whole window.
	w := NewWorld(DefaultSeed)
	kept := 0
	for _, p := range w.podsOn(failedNode, anchor) {
		if w.Workloads[p.Workload].Kind != kindDaemonSet {
			continue
		}
		kept++
		el, ok := scheduledOn(t, b, p.UID)[failed]
		if !ok || len(el.Pieces) != 1 || !el.Pieces[0].Start.Equal(anchor) || !el.Pieces[0].End.Equal(s.End) {
			t.Errorf("%s: placement on node-2 %+v, want the whole window in one piece", p.Name, el.Pieces)
		}
	}
	if kept != 2 {
		t.Errorf("%d DaemonSet pods on node-2, want 2", kept)
	}
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

// Nodes that share a machine share a host: node-1 and node-2 run on one, node-3 and
// node-4 on another, from the anchor to the end, and the two hosts differ.
func TestNodesRunOnTheirMachinesHost(t *testing.T) {
	b := canonicalBuild(t)
	w := NewWorld(DefaultSeed)
	hostOf := map[string]string{}
	for i, n := range b.Story.Nodes {
		var hosts []string
		for k, el := range edgesOf(t, b, fpOfNode(w.Nodes[i].UID), store.Forward) {
			if k[0] != string(catalog.RunsOn) {
				continue
			}
			hosts = append(hosts, k[1])
			if len(el.Pieces) != 1 || !el.Pieces[0].Start.Equal(anchor) || !el.Pieces[0].End.Equal(b.Story.End) {
				t.Errorf("%s runs on its host over %+v, want the whole window", n.Name, el.Pieces)
			}
		}
		if len(hosts) != 1 {
			t.Fatalf("%s runs on %v, want one host", n.Name, hosts)
		}
		hostOf[n.Name] = hosts[0]
		want := fingerprint(catalog.Host, catalog.HostID, w.Machines[n.Machine].HostID).String()
		if hosts[0] != want {
			t.Errorf("%s runs on %s, want the host of machine %d, %s", n.Name, hosts[0], n.Machine, want)
		}
	}
	// A host is one entity with one boot: two boots live at once would be a clone
	// collision, which the store quarantines.
	rd, win := reader(b)
	for i, m := range w.Machines {
		host := fingerprint(catalog.Host, catalog.HostID, m.HostID)
		res, err := rd.Entity(context.Background(), host, win)
		if err != nil {
			t.Fatal(err)
		}
		el := res.Elements[0]
		if el.Quarantine != nil || len(el.Pieces) != 1 || !el.Pieces[0].Start.Equal(anchor) || !el.Pieces[0].End.Equal(b.Story.End) {
			t.Errorf("the host of machine %d: quarantine %v, pieces %+v; want one piece over the whole window", i, el.Quarantine, el.Pieces)
		}
	}
	if hostOf["node-1"] != hostOf["node-2"] || hostOf["node-3"] != hostOf["node-4"] || hostOf["node-1"] == hostOf["node-3"] {
		t.Errorf("hosts %v: want node-1 and node-2 on one, node-3 and node-4 on another", hostOf)
	}
}

// The addresses are claims from the objects: every node holds one, every pod that
// runs holds one while it runs, and the claim of a Job's pod ends when the pod
// succeeds, ten minutes before the pod goes.
func TestPodsAndNodesClaimTheirAddresses(t *testing.T) {
	b := canonicalBuild(t)
	w := NewWorld(DefaultSeed)
	claims := func(fp identity.Fingerprint) []interval.Element {
		var out []interval.Element
		for k, el := range edgesOf(t, b, fp, store.Forward) {
			if k[0] == string(catalog.HasAddress) {
				out = append(out, el)
			}
		}
		return out
	}
	for _, n := range w.Nodes {
		if got := claims(fpOfNode(n.UID)); len(got) != 1 {
			t.Errorf("%s claims %d addresses, want 1", n.Name, len(got))
		}
	}
	for _, p := range w.podsOn(failedNode, anchor) {
		got := claims(fpOfPod(p.UID))
		if len(got) != 1 {
			t.Errorf("%s claims %d addresses, want 1", p.Name, len(got))
		}
	}
	jobs := 0
	for _, p := range w.Pods {
		if w.Workloads[p.Workload].Kind != kindJob {
			continue
		}
		jobs++
		got := claims(fpOfPod(p.UID))
		if len(got) != 1 {
			t.Fatalf("%s claims %d addresses, want 1", p.Name, len(got))
		}
		last := got[0].Pieces[len(got[0].Pieces)-1]
		if !last.End.Equal(p.Finished) || last.EndSource != lifecycle.EndProducer {
			t.Errorf("%s: its claim ends at %s by %v, want at %s by the producer", p.Name, last.End.Format(time.RFC3339), last.EndSource, p.Finished.Format(time.RFC3339))
		}
		on := scheduledOn(t, b, p.UID)
		if len(on) != 1 {
			t.Fatalf("%s: placed on %v", p.Name, keys(on))
		}
		for _, el := range on {
			if end := el.Pieces[len(el.Pieces)-1].End; end.Before(p.Deleted) || end.After(p.Deleted.Add(2*time.Second)) {
				t.Errorf("%s: placed until %s, want its deletion at %s", p.Name, end.Format(time.RFC3339), p.Deleted.Format(time.RFC3339))
			}
		}
	}
	if jobs != 6 {
		t.Errorf("%d Job pods, want 6", jobs)
	}
}

// The rollout leaves the three old frontend pods ended inside the window, each a
// second after it was deleted and by the producer, and the new ones alive to the
// end.
func TestTheRolloutEndsTheOldFrontendPods(t *testing.T) {
	b := canonicalBuild(t)
	w := NewWorld(DefaultSeed)
	rd, win := reader(b)
	ended, running := 0, 0
	for _, p := range w.Pods {
		wl := w.Workloads[p.Workload]
		if wl.Name != "frontend" {
			continue
		}
		res, err := rd.Entity(context.Background(), fpOfPod(p.UID), win)
		if err != nil {
			t.Fatal(err)
		}
		pieces := res.Elements[0].Pieces
		last := pieces[len(pieces)-1]
		if p.Owner == ownerOf(wl, wl.ReplicaSet) && p.Created.Before(anchor) {
			ended++
			if last.End.Before(p.Deleted) || last.End.After(p.Deleted.Add(2*time.Second)) || last.EndSource != lifecycle.EndProducer {
				t.Errorf("%s ends at %s by %v, want its deletion at %s", p.Name, last.End.Format(time.RFC3339), last.EndSource, p.Deleted.Format(time.RFC3339))
			}
			continue
		}
		if p.Deleted.IsZero() {
			running++
			if !last.End.Equal(b.Story.End) {
				t.Errorf("%s ends at %s, want the end of the window", p.Name, last.End.Format(time.RFC3339))
			}
		}
	}
	if ended != 3 || running < 3 {
		t.Errorf("%d old frontend pods ended and %d new ones run, want 3 and at least 3", ended, running)
	}
}

// Nothing derived is stored: every record in the store is a producer's assertion,
// and the producers are the three translators of the cluster name.
func TestOnlyTheTranslatorsWrite(t *testing.T) {
	b := canonicalBuild(t)
	want := []lifecycle.Producer{"k8scluster/" + ClusterName, "k8sobjects/" + ClusterName, "nodeagent/" + ClusterName}
	if !slices.Equal(b.Producers, want) {
		t.Errorf("the records are of %v, want %v", b.Producers, want)
	}
}
