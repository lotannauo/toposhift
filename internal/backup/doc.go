// Package backup backs up a store's committed records as segments, one series of
// files per class, retires each class behind its layer's retention horizon with a
// margin, and restores a backup into an empty store by merging the classes by Seq
// and then setting the horizons, before anything reads the store.
//
// # What is backed up
//
// The records a store committed, in the batches the writer committed them in,
// with their Seqs: the same records, byte for byte, that a restore writes back.
// Nothing derived is backed up (no checkpoint, no baseline): the restoring store
// derives its own. Committed Seqs are dense, so a backup can prove it holds every
// record: Cutter.Add refuses a batch that does not follow the last one with
// ErrGap, and a restore refuses a backup with a hole.
//
// Each record is kept in a class: hosts for an entity record of a host, of any
// kind and with or without a boot id, and otherwise its layer, l0 to l3. Each
// class's records are written as activity files (format version 1, package
// activity), ascending in Seq. A host keeps every record for ever, so a restored
// store folds every boot sighting and every clone collision the live store
// folded, whatever their age.
//
// # Names and the manifest
//
// Under a prefix P (DefaultPrefix when none is chosen) a class's file is
// P/v1/files/<class>/<first>-<last>.parquet and a manifest is
// P/v1/manifests/<generation>.json, with Seqs and generations as 20-digit
// zero-padded decimals, so that a listing sorts by number. Every manifest written,
// by a cut or a retirement, takes the next generation, and the highest generation
// is the backup (Latest). The manifest is JSON, version 1 (see Manifest): it names
// every file with its digest, its Seqs and what rule R needs of it; each class's
// files form a chain (a file's AfterSeq is the last Seq of the file before it, or
// what the class retired); each cut counts its records per class, which sum to
// its span; and it logs the live store's retentions and its meta (StoreMeta).
// testdata/manifest_v1.json pins the format.
//
// # Cuts
//
// A Cutter takes each committed batch (Add) with the ingest clock of its commit,
// and each retention of the live store (Retained). Cut closes the open files,
// stores them, and then stores the next manifest: files first, the manifest last.
//
// # Rule R: retirement
//
// Retire retires, per class l1, l2 and l3 whose layer is not kept, the longest
// prefix P of its files, in Seq order, such that every file's records reach (the
// largest max(EventTime, Through)) and every file's last commit are at least the
// margin M before the layer's horizon h in the last logged retention; that ends at
// a cut boundary for the class; and whose largest reach is at or before the
// EventTime of every Observe left in the class that a record of P could shadow:
// one whose reach is more than M less its TTL after its EventTime. l0 and hosts
// are never retired. A class with an Observe of no TTL, or of a TTL above M less
// the clock bound, is refused for good, with ErrNotRetirable.
//
// Why that keeps a restore's answers equal to the live store's at every instant
// at or after h: a retired record is not live at h, since its deadline is at most
// h - M + TTL; and a kept record that precedes it, in (EventTime, Seq) order, has
// an EventTime below its reach, so if it were live after h its reach would be more
// than M - TTL after its EventTime, which the last condition rules out. Every
// record of one reference is in one class. Retire cuts first, so that the rule
// sees every record the cutter holds. The comment of decide, in retire.go, states
// the assumptions and the lemma in full.
//
// # Restore
//
// Restore is Replay and then SetHorizons. Replay checks the whole backup before
// writing anything (the manifest, the store's meta, that the store is empty, the
// chains, the cut counts, each file's digest and footer), then merges the classes
// by Seq and writes them in batches. SetHorizons retains the store once, at the
// largest instant a logged retention asked for, and checks that every layer's
// horizon is the live store's. Records the live store retired are not there; a
// token between the live store's last retention and the restore's last Seq is
// refused in the retained layers, never answered wrongly.
package backup
