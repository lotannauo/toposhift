// Package xtnode is what the XTDB baseline asks of the node as a process, and not as a
// database: whether it has finished the work a load left it (the settle protocol), what it
// is (its image, its options, its configuration), and what it holds on disk. Nothing here
// opens a SQL connection, and nothing here measures a time that it reports.
//
// # The settle protocol
//
// A node that has committed a stream is still working: it indexes the transactions it has
// accepted, it flushes blocks, and its compactor merges the files of each table into larger
// ones for as long as it has jobs. A read, and the size of the directories, taken in the
// middle of that is a measurement of the moment and not of the load. [Settle] waits until
// the node reports, for a whole quiet window, that it has no indexing lag and no compaction
// pending or finishing, and gives up after a cap. What the node reports, found against a
// node of the image the baseline pins and read from its sources (xtdb/healthz.clj, and the
// metrics registered in xtdb/node/impl.clj and the compactor):
//
//   - node_tx_lag_MsgId: the messages of the node's log it has not yet processed. It is 0 when
//     everything submitted has been indexed.
//
//   - compactor_jobs_available: the compaction jobs the compactor can see, computed from the
//     node's catalog of tries, which a finished job changes. 0 when there is nothing to merge.
//
//   - compactor_job_timer_seconds_count: how many jobs the compactor has finished, which a job
//     in flight does not move until it ends. A count that changed in the quiet window means
//     a job was running in it.
//
//   - the X-XTDB-Block-Lag header of /healthz/alive: the blocks the storage has that the node
//     has not caught up with. /healthz/alive answers 503 with "Ingestion error" in its body
//     once the node's ingestion has stopped, which no later wait can undo: that is an error
//     of the settle, not a node that has not settled.
//
//   - node_tx_latestSubmittedMsgId and node_tx_latestProcessedMsgId: the newest message appended to
//     the node's log and the newest the node has processed (the lag is their difference).
//
// # Finishing the block
//
// What the node is filling is a block: rows that have been indexed are in its log, and only a
// block that has been finished is in its storage directory. A stream that does not end on a
// block boundary (and no small one does) would leave a tail in the log that no size of the
// storage directory shows, and a tail that varies with where the stream ended makes two loads
// of one stream differ in bytes. So before anything is sized, [FinishAndSettle] asks the node
// to finish its block, and the quiet window begins only once the node has processed that
// request. The request is POST /system/finish-block on the health port (a GET is answered 405;
// it answers 200 "Block flush message sent successfully."). Found against a node of the pinned
// image: on an empty node the request is processed within seconds and writes the first block
// file (storage/v06/blocks/b00.binpb); its X-XTDB-Message-Id header is the text of a Java object
// and carries no id, so the request is known to have been processed when the node's newest
// processed message reaches the newest that was in its log when the request returned.
//
// A window is quiet while, at every poll, the lag and the pending jobs are 0 and the count of
// finished jobs is what it was when the window began; anything else begins a new window.
// The registered window is 120 seconds and the cap 30 minutes. A node that does not settle
// within the cap is reported as such ([Result].Settled false), and its answers can still be
// checked while its sizes and its timings are not taken.
package xtnode
