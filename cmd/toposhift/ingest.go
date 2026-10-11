package main

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/lotannauo/toposhift/internal/datadir"
	"github.com/lotannauo/toposhift/internal/ingest/clustername"
	"github.com/lotannauo/toposhift/internal/ingest/otlphttp"
	"github.com/lotannauo/toposhift/internal/ingest/sequencer"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/replay/capture"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// envClusterName is the environment twin of --cluster-name.
const envClusterName = "TOPOSHIFT_CLUSTER_NAME"

// ingestStarting is what the receiver answers while the store opens.
const ingestStarting = "ingest is starting: the store is opening; send the request again later"

// The bounds of a shutdown after the server's own: the sequencer has this long
// to answer what it has claimed and write the coalescer's absorbed refreshes. It
// is an estimate. It is a variable only so tests can lower it.
var sequencerStopTimeout = 5 * time.Second

// The seams of the boot, for tests: how the store is opened, what the sequencer
// writes through, and a hook called with the open store once the cluster name is
// recorded and before the receiver is switched to the sequencer.
var (
	openStore    = pebblestore.Open
	writerFor    = func(st *pebblestore.Store) sequencer.Writer { return st }
	beforeSwitch func(*pebblestore.Store)
)

// ingestConfig is what serve was told about ingest.
type ingestConfig struct {
	// clusterName is the configured cluster name when set; with set false, none
	// is configured.
	clusterName string
	set         bool
	// accept is --accept-new-cluster-name, and acceptGiven says it was given:
	// given and empty accepts that no name is set.
	accept      string
	acceptGiven bool
	// pullInterval is the objects receiver's pull interval.
	pullInterval time.Duration
}

// serveStoreOptions are the options serve opens its store with: those of the
// store every command opens ([storeOptions]), with the authority of the
// producers of the configured cluster name, or of "default" when none is set:
// the objects receiver's descriptions rank above the cluster receiver's. Rank is
// applied when the store folds, so it can change between boots without rewriting
// anything. With no name configured, the producers of the other cluster names a
// client sends are not ranked, and for them the cluster receiver's producer
// wins by the order of the names; that shows only in descriptions, and goes
// when ranking follows the producer's prefix.
//
// Retention is not set here: serve does not retain yet.
func serveStoreOptions(clusterName string, set bool) pebblestore.Options {
	o := storeOptions()
	c := sequencer.DefaultClusterName
	if set {
		c = clusterName
	}
	o.Policy.Rank = map[lifecycle.Producer]int{
		lifecycle.Producer("k8sobjects/" + c): 2,
		lifecycle.Producer("k8scluster/" + c): 1,
	}
	return o
}

// switchSink is the receiver's sink, which boot switches: a gate that answers
// 503 while the store opens or when ingest is off, and then the sequencer.
type switchSink struct{ cur atomic.Pointer[sinkOf] }

type sinkOf struct{ otlphttp.Sink }

func (w *switchSink) set(s otlphttp.Sink) { w.cur.Store(&sinkOf{s}) }

func (w *switchSink) Accept(ctx context.Context, d capture.LogsData) error {
	return w.cur.Load().Accept(ctx, d)
}

func (w *switchSink) AcceptPartial(ctx context.Context, d capture.LogsData) (otlphttp.Partial, error) {
	s := w.cur.Load().Sink
	if p, ok := s.(otlphttp.PartialSink); ok {
		return p.AcceptPartial(ctx, d)
	}
	return otlphttp.Partial{}, s.Accept(ctx, d)
}

// ingest is what boot opened for ingest: the store, and the sequencer with its
// own context once it runs.
type ingest struct {
	st      *pebblestore.Store
	seq     *sequencer.Sequencer
	cancel  context.CancelFunc
	runDone chan error
}

// startIngest opens the store in the data directory, decides from the cluster
// name it records whether ingest may start, records the name when it should,
// starts the sequencer, and switches the receiver to it. It returns what must be
// closed at shutdown, nil when nothing is open. Whatever keeps ingest off is
// logged and answered by the receiver with 503 and Retry-After: 30; the server
// stays up. stopped is closed when the server has ended; boot that finds it
// ended, or ctx done, after the store opened closes the store and starts
// nothing.
func startIngest(ctx context.Context, dir *datadir.Dir, ic ingestConfig, sink *switchSink, log *slog.Logger, stopped <-chan struct{}) *ingest {
	start := time.Now()
	st, err := openStore(dir.Store, serveStoreOptions(ic.clusterName, ic.set))
	if err != nil {
		log.Error("ingest is off: the store could not be opened", "path", dir.Store, "err", err)
		sink.set(gateSink{message: "ingest is off: the store could not be opened: " + err.Error() + "; see the server's log"})
		return nil
	}
	log.Info("store opened", "path", dir.Store, "took", time.Since(start))
	in := &ingest{st: st}
	select {
	case <-ctx.Done():
		return in // stopping: the store is closed with nothing started
	case <-stopped:
		return in
	default:
	}

	name, recordedSet, present, err := st.ClusterName()
	if err != nil {
		log.Error("ingest is off: the store's cluster name could not be read", "err", err)
		sink.set(gateSink{message: "ingest is off: the store's cluster name could not be read: " + err.Error()})
		return in
	}
	d := clustername.Decide(clustername.Input{
		Recorded: name, RecordedSet: recordedSet, Present: present, LastSeq: st.LastSeq(),
		Configured: ic.clusterName, ConfiguredSet: ic.set,
		Accept: ic.accept, AcceptGiven: ic.acceptGiven,
	})
	if d.Warning != "" {
		log.Warn(d.Warning)
	}
	if d.IngestOff {
		log.Error(d.Message)
		sink.set(gateSink{message: d.Message})
		return in
	}
	if d.Message != "" {
		log.Warn(d.Message)
	}
	if d.Record {
		// Synced, and before anything is written under the name.
		if err := st.RecordClusterName(context.Background(), ic.clusterName, ic.set); err != nil {
			log.Error("ingest is off: the cluster name could not be recorded", "err", err)
			sink.set(gateSink{message: "ingest is off: the cluster name could not be recorded: " + err.Error()})
			return in
		}
		log.Info("recorded the cluster name", "cluster_name", showName(ic))
	}

	seq, err := sequencer.New(sequencer.Config{
		Store:          writerFor(st),
		ClusterName:    ic.clusterName,
		ClusterNameSet: ic.set,
		PullInterval:   ic.pullInterval,
		Logger:         log,
	})
	if err != nil {
		log.Error("ingest is off: the sequencer could not be made", "err", err)
		sink.set(gateSink{message: "ingest is off: " + err.Error()})
		return in
	}
	// The sequencer's context is its own: the signal that stops the server must
	// not stop it before the server has answered what it is serving.
	seqCtx, cancel := context.WithCancel(context.Background())
	in.seq, in.cancel, in.runDone = seq, cancel, make(chan error, 1)
	go func() { in.runDone <- seq.Run(seqCtx) }()
	if beforeSwitch != nil {
		beforeSwitch(st)
	}
	sink.set(seq)
	log.Info("ingest is on", "cluster_name", showName(ic), "took", time.Since(start))
	return in
}

func showName(ic ingestConfig) string {
	if !ic.set {
		return "(none set: " + sequencer.DefaultClusterName + ")"
	}
	return ic.clusterName
}

// stop ends ingest after the server has stopped: the sequencer answers what it
// has claimed and writes the coalescer's absorbed refreshes, within
// sequencerStopTimeout; past it, its context is cancelled and Run is waited
// for, so that no Write is in flight when the store closes. Then the store is
// closed. Each step's time is logged. A stop that timed out is not an error:
// the stored data is valid, and only absorbed refreshes may be unwritten.
func (in *ingest) stop(log *slog.Logger) {
	if in == nil {
		return
	}
	if in.seq != nil {
		start := time.Now()
		ctx, cancel := context.WithTimeout(context.Background(), sequencerStopTimeout)
		err := in.seq.Stop(ctx)
		cancel()
		if err != nil {
			log.Warn("the sequencer did not stop in time; it is cancelled, and absorbed refreshes may be unwritten",
				"timeout", sequencerStopTimeout, "err", err)
			in.cancel()
		}
		if err := <-in.runDone; err != nil && !errors.Is(err, context.Canceled) {
			log.Error("the sequencer ended with an error", "err", err)
		}
		in.cancel()
		log.Info("sequencer stopped", "took", time.Since(start))
	}
	start := time.Now()
	if err := in.st.Close(); err != nil {
		log.Error("the store did not close cleanly", "err", err)
	}
	log.Info("store closed", "took", time.Since(start))
}

const ingestHelp = `
Ingest

  With a data directory, serve opens the store in it and takes OpenTelemetry
  logs at POST /v1/logs: the Kubernetes objects receiver's pods and nodes, and
  the cluster receiver's entity events. A request is answered 200 only once
  its records are committed to disk. While the store opens, and whenever
  ingest is off, the receiver answers 503 with Retry-After and says why; health
  is served all the same.

  --cluster-name (or $TOPOSHIFT_CLUSTER_NAME) names the cluster the logs come
  from. With a name, a request whose k8s.cluster.name differs is refused. With
  none, each request's k8s.cluster.name names its cluster, or "default". An
  empty value is no name. The store records the name it was started with.
  When the configured name differs from the recorded one, ingest stays off:
  records under the old name stay, and their references lapse by their TTL.
  To accept the change, start once with
  --accept-new-cluster-name=<the configured name> (empty when no name is set).
  The flag is for one boot, and has no environment twin; a value naming
  another name is ignored with a warning.

  A store that keeps failing every write stays so until serve is restarted:
  the receiver answers 503 and the log says why. On a stop, serve finishes the
  requests in flight, writes what the coalescer holds, and closes the store.
`
