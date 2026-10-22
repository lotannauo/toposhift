package vminput

import (
	"time"

	"github.com/lotannauo/toposhift/bench/spike/engine"
	"github.com/lotannauo/toposhift/bench/spike/runner"
	"github.com/lotannauo/toposhift/bench/victoria/vmmodel"
	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
)

// AlibabaProducer is the one producer of the Alibaba placement streams (the reduction's
// name for it).
const AlibabaProducer lifecycle.Producer = "alibaba/placement"

// AlibabaC300TTL is the TTL every record of the placement stream at the five-minute
// cadence carries: three sampling intervals of five minutes. The registry reads the
// producer through it, and the converter refuses a record whose TTL is another.
const AlibabaC300TTL = 15 * time.Minute

// DefaultBatch is the number of records handed to a sink at a time.
const DefaultBatch = 4096

// AlibabaRegistry is the registered table of the Alibaba placement stream: one producer,
// which samples at a fixed cadence and has no watch mode, holds the nodes, the pods and
// the pod-on-node edges, and is read through the TTL its samples carry. The trace has no
// delete and no second producer, so every reading is by this one window.
func AlibabaRegistry(ttl time.Duration) (*vmmodel.Registry, error) {
	h := []vmmodel.Holder{{Producer: AlibabaProducer, Window: ttl}}
	return vmmodel.NewRegistry(
		map[catalog.RelationType][]vmmodel.Holder{catalog.ScheduledOn: h},
		map[catalog.EntityType][]vmmodel.Holder{catalog.K8sNode: h, catalog.K8sPod: h},
	)
}

// Stream is what a read of an activity file amounted to: the content digest from the
// footer (checked against the rows by the read), the records handed to the sinks and the
// last sequence number.
type Stream struct {
	Digest  string
	Records uint64
	LastSeq uint64
}

// Drive reads an activity file into the sinks, as runner.Drive writes a generated stream
// into them: in batches, in the file's ascending Seq order, and with no retention. It
// returns what was read. The sinks share each batch, so one must not keep or change it.
func Drive(path string, sinks ...runner.Sink) (Stream, error) {
	var st Stream
	info, err := ForEach(path, DefaultBatch, func(batch []engine.Record) error {
		for _, s := range sinks {
			if err := s.Write(batch); err != nil {
				return err
			}
		}
		st.Records += uint64(len(batch))
		st.LastSeq = batch[len(batch)-1].Seq
		return nil
	})
	if err != nil {
		return st, err
	}
	st.Digest = info.Digest
	return st, nil
}
