package k8sobjects_test

import "testing"

// The golden files hold the translation of the fixtures cut from the simulated
// cluster captures: one step per log record, with the records, pending edges and
// skips it produced. They are read through by eye when written. Regenerate them only
// with -update and list the files in the commit.
//
// Provenance of the fixtures (every record is a copy of a record of a capture of
// 2026-10-09, with its resource and scope; file:line is the line of the capture
// and the index the record in that line's batch):
//
//	run1.jsonl  1:2 1:4        node pulls (kwok-node-2 f41e8e62, kwok-node-4)
//	            2:1 2:2 2:36   pod pulls at the collector's start (42s2c, 4gjhs, wr85m)
//	            3:0            an entity state event
//	            111 118        db-0, a StatefulSet pod: ADDED, MODIFIED
//	            116            an Event object
//	            203 205 468 544   sbt4s: ADDED, MODIFIED (scheduled), MODIFIED (deletionTimestamp set), DELETED
//	            712            42s2c patched to Failed (Evicted)
//	            729            4gjhs patched to restartCount 3
//	            921            an entity delete event
//	            922            kwok-node-2 DELETED
//	            971 976        wr85m: failed by PodGC, then DELETED
//	            1055 1056      kwok-node-2 re-created under a new UID
//	            1112           kwok-node-4 heartbeat-only MODIFIED
//	run2.jsonl  1:2            the re-created kwok-node-2 in the pull after a restart
//	            2:7            a pod created while the collector was down, first seen in a pull
//	            212            that pod DELETED
func TestGolden(t *testing.T) {
	for _, name := range []string{"run1", "run2"} {
		t.Run(name, func(t *testing.T) {
			in := loadCapture(t, name+".jsonl")
			rs := translateAll(t, newTranslator(t, 0), in)
			checkGolden(t, name+".golden.json", render(t, labels(t, in), in, rs))
		})
	}
}
