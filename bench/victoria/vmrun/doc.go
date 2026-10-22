// Package vmrun is what the commands of vmspike do, as a library: convert a stream into
// the samples a metrics store would hold and count them, check that the two streams of an
// input describe the same existence, load the samples into a VictoriaMetrics server and
// check its own counters against the converter's, merge them, read the size of what the
// server holds, and ask the questions of the input of the store and sort the answers
// against the reference.
//
// The commands make the guards that keep a run honest, here and tested apart from any
// server:
//
//   - an output is never written inside a git work tree ([RefuseInWorkTree]): what the
//     commands write is local scratch, not a result;
//   - a plan is used only if the SHA-256 of its file is the one named in advance
//     ([LoadPlanInput]), and the stream it regenerates is the digest the plan records;
//   - a server is used only if its flags are those of flags.conf, its version is the
//     registered one and the container it runs in was started from the registered image
//     ([CollectEnv]);
//   - a load starts on a server that holds none of the baseline's series, and is refused
//     before the first write if the samples fall in more than one UTC month ([CheckOneMonth]):
//     the series count the server reports at /api/v1/series/count counts a series once for
//     each monthly partition it has samples in (a test against the server shows it), so
//     for a stream that crosses a month it is not the converter's;
//   - a load is INVALID, and says so, unless the server's counters (rows received, added
//     and ignored, series created) equal the converter's counts, and a merge is INVALID
//     unless every row is stored afterwards ([Load], [Merge]).
//
// Nothing here prints or records a time. Counts and digests of a run are deterministic
// (a pure function of the input, the registry and the flags); the bytes of a merged
// data directory are not byte-stable and are a figure only on CI hardware, which the
// output says.
package vmrun
