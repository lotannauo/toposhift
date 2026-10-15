package pebblestore

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/lotannauo/toposhift/internal/catalog"
	"github.com/lotannauo/toposhift/internal/lifecycle"
	"github.com/lotannauo/toposhift/internal/store"
	"github.com/lotannauo/toposhift/internal/store/pebblekv"
)

// referenceCellGap works out G, the policy gap of the seed walk's reference cell,
// from the codec: the reverse prefix of a node with 110 live pods, each with one
// scheduled_on reference by the k8sobjects translator, whose records are
// observations and run extensions carrying the translator's edge payload. A
// checkpoint there lists the 110 references; the mean record is one of them.
// G = max(KMin, Alpha x checkpoint bytes / mean record bytes), as the policy's due
// computes it with the default policy.
func referenceCellGap(t *testing.T) (gap, ckptBytes, recordBytes int) {
	t.Helper()
	s := openMem(t, Options{Checkpoints: off()})
	node := fingerprintOf(catalog.K8sNode, 0x10)
	at := time.Date(2026, 10, 15, 8, 0, 0, 0, time.UTC)
	// The payload of k8sobjects' scheduled_on edge: canonical JSON of the node's
	// name and how it was resolved.
	payload, err := json.Marshal(map[string]any{
		"node_name": "ip-10-0-12-34.eu-west-1.compute.internal",
		"resolution": map[string]any{
			"basis":               "node-name-index",
			"node_interval_start": at.Add(123456789 * time.Nanosecond).Format(time.RFC3339Nano),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	var sum int
	const pods = 110
	for i := range pods {
		pod := fingerprintOf(catalog.K8sPod, byte(0x20+i))
		rec := store.Record{
			Layer: catalog.L2, Subject: store.EdgeSubject(pod, node, catalog.ScheduledOn), Producer: "k8sobjects/default",
			EventTime: at.Add(time.Duration(i) * time.Second), Through: at.Add(time.Duration(i)*time.Second + 20*time.Minute),
			Seq: 500_000_000 + uint64(i), Kind: lifecycle.Observe, TTL: 15 * time.Minute,
			EventTimeBasis: store.BasisObjectField, Payload: payload,
		}
		sides, err := s.sidesOf(rec)
		if err != nil {
			t.Fatal(err)
		}
		rev := sides[1] // the node's reverse prefix
		v := pebblekv.FromRecord(rec)
		ns, _ := pebblekv.Locate(rec.EventTime)
		entries = append(entries, entryOf(rev.ref, ns, v))
		sum += len(appendRecordValue(nil, rev.ref, v))
	}
	val, err := appendStamp(nil, stamp{kind: kindCheckpoint, foldVersion: foldVersion, through: 600_000_000, w: 500_000_000 + pods, entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	o := DefaultCheckpoints()
	recordBytes = sum / pods
	gap = max(o.KMin, int(o.Alpha*float64(len(val))/float64(recordBytes)))
	return gap, len(val), recordBytes
}

// The budget constants are the reference cell's gap as the codec gives it: a change
// to the codec, the policy or the cell that moves G fails here, and the budget is
// then changed by a new dated paragraph, not silently.
func TestTheSeedWalkBudgetIsTheReferenceCellsGap(t *testing.T) {
	t.Parallel()
	gap, ckpt, rec := referenceCellGap(t)
	t.Logf("reference cell: checkpoint %d bytes, mean record %d bytes, G = %d", ckpt, rec, gap)
	if seedWalkReferenceGap != gap {
		t.Errorf("seedWalkReferenceGap = %d; the codec gives G = %d (checkpoint %d bytes, mean record %d bytes)", seedWalkReferenceGap, gap, ckpt, rec)
	}
	if seedWalkBudgetRecords != 3*seedWalkReferenceGap || seedWalkBudgetStamps != 1 {
		t.Errorf("SW1 is %d records and %d stamps; want 3 x G = %d and 1", seedWalkBudgetRecords, seedWalkBudgetStamps, 3*gap)
	}
}
