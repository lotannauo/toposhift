// Package bytesgate measures the size of the wire format against two other
// encodings of the same document, on five fixed fixtures. It is the bytes half of
// the test that decides whether the format stays hand-written or becomes Arrow
// IPC: it reports sizes and ratios and nothing else. It holds no threshold and
// states no verdict: what ratio is enough is set separately, once the numbers are
// in.
//
// # The fixtures
//
// Each is a [wire.Document] built from a seeded memstore by the adapters of
// package subgraph, over a window of one hour, so the same [Scale] always gives
// the same document:
//
//	a  a hot node: BudgetReferencePods pods on one node, replaced at a fixed rate,
//	   a co-residency search from a pod that stays
//	b  extract's anchored reference cell: a host with one node of the same size,
//	   a service with three callers and three callees, an anchored extraction
//	c  extract's exploratory reference cell: forty such hosts, an exploratory
//	   extraction from the service
//	d  a scope of pods whose union is 2000 elements
//	e  the same with a union of 10000 elements
//
// The hash of every entity is drawn from a seeded hash, never counted up, so a
// fingerprint is as incompressible as a real one.
//
// # The encodings
//
// The custom bytes are [wire.Encode]. The JSON twin is [wire.JSON], as it is
// written: indented, because that is the canonical form. The Arrow encoding is
// [Arrow]: a sequence of Arrow IPC streams, one for each of the twelve sections
// of the wire format, each stream with its own schema and one record batch. Its
// body is not compressed by IPC. An entity a section names is its index in the
// entities section, as in the wire format, and Arrow carries what the wire
// format writes and nothing it derives. Times are int64 columns of nanoseconds,
// relative to From as in the wire format, and the absolute ones are int64 too.
// The schemas are listed at [Arrow].
//
// [ArrowDictionaries] is the same with a dictionary(int32, fixed_size_binary(18))
// column for every entity reference, so that each stream carries a dictionary of
// the entities it names. It is measured beside Arrow, as an extra column of the
// report, and is not what the ratio is made of.
//
// # The compression
//
// [Brotli] compresses at quality 11 with a window of 2^22 bytes, with the
// andybalholm/brotli version go.mod pins. Raw sizes are reported and are not what
// the gate is about: a response is compressed once, when it is stored.
//
// # What it reports
//
// For each fixture, the raw and brotli size of each encoding, the bytes of Arrow
// that are metadata, and the ratio of the Arrow bytes to the custom bytes after
// brotli, over the whole response, header included. An encoding of more than the
// brotli window of 2^22 bytes is marked. Over the fixtures, the geometric mean
// and the largest of that ratio.
// Sizes do not depend on the hardware. [Run] and [Report.WriteTable] are the code; the
// test TestReport is the way to run them:
//
//	TOPOSHIFT_BYTES_GATE=1 go test -count=1 -v -timeout 30m -run '^TestReport$' ./internal/wire/bytesgate
//
// # The variants
//
// After the table, [Run]'s report is followed by a block of four sensitivity
// variants, labelled informational. Each is a registered fixture with one thing
// changed, built and measured the same way, with its r printed beside the
// registered fixture's:
//
//	i    churn ten times lower: a pod replaced every 300 s, in a, b and c
//	ii   filler pods mapped to the 16 services besides the cell's seven, 23 services
//	     on a node, the seven keeping their one pod, in b and c
//	iii  1, 2 or 3 containers on each pod (69.7, 24.2 and 6.1 in 100, drawn from a
//	     generator of their own) with a part_of edge each, in d and e
//	iv   scopes fully stable: no pod appears, disappears or flaps, in d and e
//
// They enter no mean and no largest ratio, and nothing is computed over them or
// said of them: they show how far the registered fixtures' ratios move when one
// assumption does. The registered fixtures are the same documents with or without
// them.
//
// The size of the format's own bytes alone, with no Arrow encoding, no JSON and no
// ratio, is read with [RunCustomOnly] and [WriteCustomTable]:
//
//	TOPOSHIFT_BYTES_GATE_CUSTOM_ONLY=1 go test -count=1 -v -timeout 30m -run '^TestReportCustomOnly$' ./internal/wire/bytesgate
//
// Both are skipped without their variable: the full fixtures hold up to ten thousand
// elements and brotli at quality 11 is slow. TestReferenceFixturesBuild, skipped
// the same way, builds them without measuring and logs their shape. The other
// tests run the same code on small fixtures.
package bytesgate
