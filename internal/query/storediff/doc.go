// Package storediff holds no code: its tests are the differential check of the
// query packages across stores. They fill the reference store, memstore, and
// pebblestore with the same generated worlds, pebblestore with small checkpoint
// gaps so that the seed walks meet checkpoints, under both retention modes and
// with and without a retention, and require every query to give the same answer
// on both, reads included: package interval's entity, edge and description
// reads, co-residency, blast radius and Around, extraction, graph diff, and the
// subgraph builds. They also hold pebblestore's seed walks at a reference cell
// to the budget pebblestore fixes, and each WindowSince to one iterator.
package storediff
