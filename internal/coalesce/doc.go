// Package coalesce folds the refreshes of a heartbeating producer into runs, at ingest,
// before a record reaches the store.
//
// # Runs
//
// A producer that watches a subject through a TTL repeats the same observation every
// interval. Stored one by one, those repeats would dwarf everything else the store
// holds. A run is the repeats of one description (TTL, payload, boot and the basis of
// the event time) of one subject by one producer, and ingest keeps it as one record: an
// Observe at the run's first event time, with Through set to the latest refresh and a
// later Seq. The store never
// overwrites, so every extension is a stored record of its own; the refreshes a run
// absorbs, under the extension interval below, are the ones never stored. The basis of
// the event time ([store.EventTimeBasis]) is part of the description because a run
// asserts its first event time: every record the coalescer writes for a run carries the
// basis of the refresh that started it, and a refresh that says its time came from
// elsewhere starts a new run instead of being absorbed into one that says otherwise.
// Ingest must therefore keep a producer's basis stable across its pure refreshes: a
// producer whose basis alternates is never coalesced, every refresh starts a run, and
// storage grows with every refresh.
// [Coalescer.Add] decides, for each record in arrival order, whether it starts a run,
// extends one, closes one, or is already stood for.
//
// # Only this coalescer extends a run
//
// Through is a trust boundary. An extension re-asserts the run at its first event time
// with a later Seq and a later Through, so if two writers could extend one run, an older
// extension arriving after a newer one would overwrite it with a shorter Through and
// shorten the subject's existence. Only a single, sequential coalescer, which sees the
// records in the order they arrive, can tell which extension is the latest. A
// [Coalescer] is therefore not safe for concurrent use, there is one in the product,
// and the producers' own records never carry a Through. A record that does is passed on
// as given and ends the subject's run, as a delete does, so its Through is never
// overwritten or absorbed.
//
// # The three settings
//
// [Config] has three settings, each a trade.
//
// ExtendTTLFraction and ExtendEvery (exclusive) limit how often a run is re-asserted: a
// refresh closer than the extension interval to the last extension is absorbed without
// a record. The run then understates how long its producer was seen by less than the
// extension interval, so existence can end early but never late, and only the end of an
// interval moves. When a run is replaced by another the store is first given what its
// deadline lacks to reach the new run, so an interval has no hole the producer did not
// have. Both zero extends on every refresh, which loses nothing.
//
// Absorption can also hide a clone collision for a while, and shortens a boot's last
// sighting the way it shortens existence. Boot A seen at 0 and again at 60 (absorbed),
// and boot B seen late at 30: the raw stream has A after B first appeared, a collision,
// and the coalesced one does not until A is refreshed again. A boot's last sighting is
// early by less than the extension interval; its ID and first sighting are exact. And
// two clones that report alternating boots under one producer change the description on
// every refresh, so nothing coalesces; the output is still bounded, at two records per
// input (the record that closes the run it replaces, and the new one).
//
// RunMaxAge continues a run that has reached an age, counted from its first event time,
// with a new run asserted at the refresh's own event time, with the same description.
// It does not change how many records a run produces: every extension is a stored
// record, so that number is set by the extension interval (one per ExtendTTLFraction
// times the TTL), not by the bound. The bound swaps an extension for the start of a new
// run, one for one. What it changes is how far back a run's extensions are keyed, which
// is how far a read walks to the oldest live run of a prefix, and which checkpoints an
// extension deletes: an extension is keyed at the run's start, and without the bound
// the extensions of a long-lived run, each written at its start, keep deleting the
// newest checkpoint of the neighbourhood they are keyed in (in the layout measured,
// about 110 staggered live runs on one hot node left it with none).
//
// # The horizon
//
// A store retains each layer back to a horizon and refuses a record before it. An
// extension is keyed at its run's start, so a run that began before its layer's horizon
// can no longer be extended: the coalescer continues it, never extends it, by a new run
// asserted at the refresh's own time. The caller tells the coalescer each horizon with
// [Coalescer.SetHorizon] (or [Coalescer.SetHorizonAll], for a store that retains every
// layer under one). No record it returns for a refresh at or after a layer's horizon is
// before that horizon.
//
// # A restart loses the runs and the horizons
//
// The coalescer keeps its runs and its horizons in memory. After a restart the next
// refresh of each producer starts a new run, and existence can show a gap of up to one
// extension interval where the old run's stored deadline falls short of the new run's
// start. The horizons are lost too: the caller tells the coalescer every horizon the
// store holds ([Coalescer.SetHorizon] or [Coalescer.SetHorizonAll]) before the first
// [Coalescer.Add] after a restart, and, while running, calls SetHorizon in the same
// sequential flow as the store's Retain, before the next Add, so that no Add sees a
// store that has retained further than the coalescer knows.
// [Coalescer.Prune] forgets runs that have ended, so a stream of subjects that come and
// go does not grow the map without bound.
//
// # The measured setting
//
// [DefaultConfig] is the setting the layout was measured with at a 15-minute pod
// heartbeat: an extension at most once per half a TTL, and a run's age bounded at 30
// minutes. For a 5-minute heartbeat the bound measured was 10 minutes. The bound is
// tied to the producers' extension interval, so whoever runs the coalescer records
// [Config.Describe] next to each producer's interval.
package coalesce
