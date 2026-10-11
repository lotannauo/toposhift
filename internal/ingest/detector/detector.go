// Package detector tells when two clusters share one cluster name.
//
// The cluster name of a record comes from the collector that sent it, and
// nothing stops two collectors from being given the same one. Their records
// would then be merged into one cluster. The kube-system namespace of a
// cluster has a UID that no other cluster shares, so the namespace events a
// collector reports say which cluster a name stands for. This package
// remembers, per cluster name, the kube-system UIDs that are alive, and says
// when a name has two.
//
// # Rules
//
//   - A [Sighting] is one kube-system namespace event, attributed to the cluster
//     name of the producer that reported it. For each cluster name and UID the
//     sighting with the newest At decides. A delete ends the UID at its At. A
//     state event makes it alive until its AliveUntil, or until At plus
//     [Config.DefaultAlive] when it reports no end. An older sighting never
//     overrides a newer one, and at equal instants a delete wins.
//   - A UID is alive at an instant when the instant is before its end.
//   - Two or more distinct UIDs alive at one instant under one cluster name mean
//     that two clusters share the name, and [Detector.Check] refuses the name.
//     The refusal ends when one of them lapses.
//   - One UID alive at an instant under two or more cluster names means that one
//     cluster is ingested twice. [Detector.Notes] reports it, once for as long
//     as it lasts. It is never a refusal.
//   - The state is rebuilt from the sightings that arrive after a start. Until
//     [Config.Warmup] has passed, nothing is refused or noted, because the
//     sightings that would show the conflict may not have arrived yet; records
//     accepted in that window can already be merged.
//   - A cluster name that was never sighted is never refused. Without a source of
//     namespace events there is no detector.
//
// # Use by the sequencer
//
// For every incoming request, the caller translates it, calls
// [Detector.Observe] for every namespace sighting in it before deciding
// anything, including those of records it is about to refuse, and then calls
// [Detector.Check] with each cluster name in the request and the instant of the
// request. If any cluster is refused, the sink returns a refusal carrying
// [Verdict.Message] and the whole request is answered with a 400. Records of
// that cluster already published to the stream are refused by the sequencer,
// counted and logged. [Detector.Notes] are logged and never refused. The caller
// calls [Detector.Prune] now and then to bound the memory.
//
// The instant passed to Check, Notes and Prune is the sequencer's own clock of
// ingest: the receipt time of the request, or the newest instant it has seen,
// and never earlier than one passed before. It is not a record's event time. A
// late record must not reopen or close a verdict, and Notes reports a case once
// on the assumption that the instants do not go backwards.
//
// Warmup is one report interval and DefaultAlive three; a sequencer whose
// collection interval is not 5 minutes sets both from it.
//
// # Time
//
// The package never reads a clock and does no I/O. Every instant it uses is
// one the caller passes in, so two detectors given the same calls in the same
// order answer alike. A Detector is not safe for concurrent use.
//
// The detector keeps one sighting per cluster name and UID, the newest, so it
// answers for the present of the sightings it has observed. It does not
// reconstruct what was alive at an instant well behind the newest sighting of a
// UID.
package detector

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/lotannauo/toposhift/internal/ingest/k8scluster"
)

const (
	// DefaultWarmup is one default report interval of the cluster receiver.
	DefaultWarmup = 5 * time.Minute
	// DefaultDefaultAlive is three default report intervals.
	DefaultDefaultAlive = 15 * time.Minute

	maxDuration = 366 * 24 * time.Hour
)

// A Sighting is one kube-system namespace event, attributed to the cluster name
// of the producer that reported it.
type Sighting struct {
	// Cluster is the cluster part of the producer name.
	Cluster string
	// UID is the namespace's k8s.namespace.uid.
	UID string
	// Deleted says the event is an entity.delete.
	Deleted bool
	// At is the record's instant.
	At time.Time
	// AliveUntil is when the sighting stops vouching for its UID. The zero time
	// means no end was reported.
	AliveUntil time.Time
}

// FromK8sCluster converts a sighting of the cluster receiver's translator, given
// the cluster name of its producer.
func FromK8sCluster(cluster string, s k8scluster.NamespaceSighting) Sighting {
	return Sighting{
		Cluster:    cluster,
		UID:        s.UID,
		Deleted:    s.Deleted,
		At:         s.At,
		AliveUntil: s.AliveUntil(),
	}
}

// Config sets the two windows. A zero field takes its default.
type Config struct {
	// Warmup is the blind window after the start: nothing is refused or noted
	// before start plus Warmup. The default is five minutes, one default report
	// interval.
	Warmup time.Duration
	// DefaultAlive is how long a state sighting with no end vouches for its UID.
	// The default is fifteen minutes, three default report intervals. It is also
	// the margin [Detector.Prune] keeps.
	DefaultAlive time.Duration
}

// A Verdict says whether the records of a cluster name are refused.
type Verdict struct {
	// Refuse says two or more distinct UIDs are alive under the cluster name.
	Refuse bool
	// Blind says the instant is inside the warmup: nothing is refused or noted.
	Blind bool
	// UIDs are the alive UIDs, sorted, when Refuse.
	UIDs []string
	// Message is the refusal's reason when Refuse, of the form
	// "two clusters under cluster name "X": set k8s.cluster.name in each collector
	// (kube-system UIDs "A", "B")". The name and the UIDs come from the collector,
	// so they are quoted, cut to 64 bytes, and at most four UIDs are listed
	// before "and N more"; UIDs has them all.
	Message string
}

// A Note reports one kube-system UID alive under two or more cluster names:
// one cluster ingested twice.
type Note struct {
	// UID is the kube-system namespace UID.
	UID string
	// Clusters are the cluster names it is alive under, sorted, in full.
	Clusters []string
	// Message is the text to log, with the UID and the names quoted and bounded
	// as in [Verdict.Message].
	Message string
}

// entry is the newest sighting of one UID under one cluster name.
type entry struct {
	at      time.Time
	end     time.Time // the first instant at which the UID is no longer alive
	deleted bool
}

// A Detector remembers the alive kube-system UIDs per cluster name.
type Detector struct {
	cfg      Config
	open     time.Time // start plus Warmup: the first instant that is not blind
	clusters map[string]map[string]entry
	noted    map[string]struct{} // the UIDs the last Notes call reported or kept
}

// New returns a Detector. start is the first instant the caller will pass; it
// must not be the zero time. A negative or over-long window is an error.
func New(c Config, start time.Time) (*Detector, error) {
	if start.IsZero() {
		return nil, errors.New("detector: start must not be the zero time")
	}
	if c.Warmup < 0 || c.Warmup > maxDuration {
		return nil, fmt.Errorf("detector: warmup %s is out of range", c.Warmup)
	}
	if c.DefaultAlive < 0 || c.DefaultAlive > maxDuration {
		return nil, fmt.Errorf("detector: default alive %s is out of range", c.DefaultAlive)
	}
	if c.Warmup == 0 {
		c.Warmup = DefaultWarmup
	}
	if c.DefaultAlive == 0 {
		c.DefaultAlive = DefaultDefaultAlive
	}
	return &Detector{
		cfg:      c,
		open:     start.Add(c.Warmup),
		clusters: map[string]map[string]entry{},
		noted:    map[string]struct{}{},
	}, nil
}

// Observe records a sighting. The caller must call it for every sighting it
// receives, including those in requests or records it is about to refuse, so
// that the detector keeps reading while it refuses.
//
// A sighting with no cluster name or no UID cannot be attributed and is
// ignored.
func (d *Detector) Observe(s Sighting) {
	if s.Cluster == "" || s.UID == "" {
		return
	}
	e := entry{at: s.At, deleted: s.Deleted}
	switch {
	case s.Deleted:
		e.end = s.At
	case !s.AliveUntil.IsZero():
		e.end = s.AliveUntil
	default:
		e.end = s.At.Add(d.cfg.DefaultAlive)
	}
	uids := d.clusters[s.Cluster]
	if uids == nil {
		uids = map[string]entry{}
		d.clusters[s.Cluster] = uids
	}
	if old, ok := uids[s.UID]; ok && !e.supersedes(old) {
		return
	}
	uids[s.UID] = e
}

// supersedes says whether e replaces old for the same cluster name and UID. The
// newer At wins. At equal instants a delete wins, and of two state events the
// one that vouches longer, so that the outcome does not depend on arrival order.
func (e entry) supersedes(old entry) bool {
	switch {
	case e.at.After(old.at):
		return true
	case e.at.Before(old.at):
		return false
	case e.deleted != old.deleted:
		return e.deleted
	}
	return e.end.After(old.end)
}

// aliveAt says whether the UID still vouches at now.
func (e entry) aliveAt(now time.Time) bool { return now.Before(e.end) }

// Check says whether the records of cluster are refused at now.
func (d *Detector) Check(cluster string, now time.Time) Verdict {
	if now.Before(d.open) {
		return Verdict{Blind: true}
	}
	var uids []string
	for uid, e := range d.clusters[cluster] {
		if e.aliveAt(now) {
			uids = append(uids, uid)
		}
	}
	if len(uids) < 2 {
		return Verdict{}
	}
	slices.Sort(uids)
	return Verdict{
		Refuse: true,
		UIDs:   uids,
		Message: fmt.Sprintf("two clusters under cluster name %s: set k8s.cluster.name in each collector (kube-system UIDs %s)",
			quoted(cluster), quotedList(uids)),
	}
}

const (
	maxQuoted = 64 // bytes of a client-chosen value kept in a message
	maxListed = 4  // values listed in a message before "and N more"
)

// quoted returns s as a Go string literal, cut to at most maxQuoted bytes on a
// rune boundary, with "..." after the closing quote when it was cut. Cluster
// names and UIDs come from the collector and end up in messages that reach it,
// so a message shows them quoted and bounded.
func quoted(s string) string {
	if len(s) <= maxQuoted {
		return strconv.Quote(s)
	}
	n := maxQuoted
	for n > 0 && !utf8.RuneStart(s[n]) {
		n--
	}
	return strconv.Quote(s[:n]) + "..."
}

// quotedList quotes the first maxListed of vs, joined with ", ", and says how
// many more there are.
func quotedList(vs []string) string {
	more := 0
	if len(vs) > maxListed {
		more = len(vs) - maxListed
		vs = vs[:maxListed]
	}
	q := make([]string, len(vs))
	for i, v := range vs {
		q[i] = quoted(v)
	}
	out := strings.Join(q, ", ")
	if more > 0 {
		out += ", and " + strconv.Itoa(more) + " more"
	}
	return out
}

// Notes returns, once each, the reverse cases that began since the last call:
// one UID alive under two or more cluster names at now, sorted by UID. A case
// that ends and begins anew is reported again. Inside the warmup it returns
// nothing. It is never a refusal.
func (d *Detector) Notes(now time.Time) []Note {
	if now.Before(d.open) {
		return nil
	}
	names := map[string][]string{}
	for cluster, uids := range d.clusters {
		for uid, e := range uids {
			if e.aliveAt(now) {
				names[uid] = append(names[uid], cluster)
			}
		}
	}
	var out []Note
	still := map[string]struct{}{}
	for uid, clusters := range names {
		if len(clusters) < 2 {
			continue
		}
		still[uid] = struct{}{}
		if _, seen := d.noted[uid]; seen {
			continue
		}
		slices.Sort(clusters)
		out = append(out, Note{
			UID:      uid,
			Clusters: clusters,
			Message: fmt.Sprintf("kube-system UID %s is alive under cluster names %s: one cluster is ingested twice",
				quoted(uid), quotedList(clusters)),
		})
	}
	d.noted = still
	slices.SortFunc(out, func(a, b Note) int { return strings.Compare(a.UID, b.UID) })
	return out
}

// Prune drops the UIDs that ended before now minus DefaultAlive, so that the
// memory stays bounded. It changes no verdict at or after now unless a sighting
// older than a dropped one arrives later and vouches past now; the caller prunes
// with a margin of at least three of the longest report intervals it has seen,
// which DefaultAlive is by default.
func (d *Detector) Prune(now time.Time) {
	cut := now.Add(-d.cfg.DefaultAlive)
	present := map[string]struct{}{}
	for cluster, uids := range d.clusters {
		for uid, e := range uids {
			if e.end.Before(cut) {
				delete(uids, uid)
				continue
			}
			present[uid] = struct{}{}
		}
		if len(uids) == 0 {
			delete(d.clusters, cluster)
		}
	}
	for uid := range d.noted {
		if _, ok := present[uid]; !ok {
			delete(d.noted, uid)
		}
	}
}
