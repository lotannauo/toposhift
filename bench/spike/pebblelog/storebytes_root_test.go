package pebblelog_test

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/workload"
	"github.com/lotannauo/toposhift/internal/store"
	rootkv "github.com/lotannauo/toposhift/internal/store/pebblekv"
	"github.com/lotannauo/toposhift/internal/store/pebblestore"
)

// rootStore lets the driver of the frozen store-bytes test write into the promoted
// store: it converts the spike's records to the store's, field by field, and calls
// the store with a background context.
type rootStore struct{ s *pebblestore.Store }

func (r rootStore) Write(recs []engine.Record) error {
	out := make([]store.Record, len(recs))
	for i, x := range recs {
		out[i] = store.Record{
			Layer: x.Layer,
			Subject: store.Subject{
				Kind: store.SubjectKind(x.Subject.Kind), A: x.Subject.A, B: x.Subject.B, Relation: x.Subject.Relation,
			},
			Producer: x.Producer, EventTime: x.EventTime, Seq: x.Seq, Kind: x.Kind, TTL: x.TTL, Through: x.Through,
			Payload: x.Payload,
		}
	}
	return r.s.Write(context.Background(), out)
}

func (r rootStore) Retain(h time.Time) error { return r.s.Retain(context.Background(), h) }
func (r rootStore) Close() error             { return r.s.Close() }

// openRootStoreBytes opens the promoted store with the fixed options of a case,
// with or without the settling of a retention.
func openRootStoreBytes(settle bool) storeBytesOpener {
	return func(dir string, fs vfs.FS, _ storeBytesVariant, rec engine.Recorder) (storeBytesEngine, error) {
		s, err := pebblestore.Open(dir, pebblestore.Options{
			Config:   rootkv.Config{Tuning: rootkv.TinyTuning(), FS: fs, SettleRetention: settle},
			Recorder: rec,
		})
		if err != nil {
			return nil, err
		}
		return rootStore{s}, nil
	}
}

// The promoted store writes the data keys and values the spike's layout L wrote
// for the same streams, byte for byte: the frozen digests of the checkpoint-off
// cases are the ones it must reproduce, with and without the settling of its
// retentions (which changes no key). The meta keys are outside the digest, and are
// the one thing the promotion was free to change.
func TestStoredBytesAreUnchangedInThePromotedStore(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"config 0 off", "config 5 off", "config 6 off"} {
		var config func() workload.Config
		for _, stream := range storeBytesStreams {
			if name == stream.name+" off" {
				config = stream.config
			}
		}
		if config == nil {
			t.Fatalf("no stream for %q", name)
		}
		for _, settle := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s settle %v", name, settle), func(t *testing.T) {
				t.Parallel()
				run := writeStoreBytes(t, config(), storeBytesVariants[0], "db", vfs.NewMem(), openRootStoreBytes(settle))
				d := run.digest
				if d.Records <= 500 || d.Baselines == 0 || run.retains != len(storeBytesRetains) {
					t.Errorf("the case writes too little to freeze: %d records, %d baselines, %d retentions", d.Records, d.Baselines, run.retains)
				}
				if d.Checkpoints != 0 {
					t.Errorf("the store holds %d checkpoints; it writes none", d.Checkpoints)
				}
				got := storeBytesWant{d.Keys, d.Records, d.Checkpoints, d.Baselines, hex.EncodeToString(d.SHA256[:])}
				t.Logf("%s settle %v: %d keys, %d records, %d baselines, sha256 %s", name, settle, d.Keys, d.Records, d.Baselines, got.sha256)
				if want, ok := storeBytesWants[name]; !ok || got != want {
					t.Errorf("the promoted store holds other data keys and values than the spike's layout L did for this stream.\n"+
						"computed: %q: {%d, %d, %d, %d, %q},\nfrozen:   %+v (present: %v)",
						name, got.keys, got.records, got.checkpoints, got.baselines, got.sha256, want, ok)
				}
			})
		}
	}
}
