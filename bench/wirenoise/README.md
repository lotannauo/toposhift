# wirenoise

How noisy is the median of a Node decode time on the `ubuntu-24.04-arm` runner? The
wire format's decode gate (item 7 of the bytes gate registration) judges Arrow
against the custom codec by a margin T3, and fixes T3 from this spread. This
directory holds the measurement. It measures noise only: both sides of every
comparison here run the same code, so a true difference between them is zero, and
nothing here says which format decodes faster.

It is not part of the Go build. It has no `.go` file and nothing imports it.

## The design

An **A/A** measurement. In one Node process the same decode workload runs in two
**slots**, 1 and 2, in a counterbalanced order. Each slot has its own, independently
loaded copy of the code (below). The ratio of the slots' medians, `r = m1 / m2`, is
therefore noise of the kind the gate's Arrow-against-custom comparison would suffer.
The spread of `r` over many processes and runners is **S**.

### Groups

A **group** is a workload at a size.

| workload | what one timed iteration does |
|---|---|
| F | Flechette `tableFromIPC` on each of the twelve streams of a fixture, then the full read |
| A | apache-arrow `tableFromIPC` on each of the twelve streams of a fixture, then the same full read |
| C | a stand-in custom decode of the same twelve sections, then the same full read |

C is a hand-written JavaScript reader of the sections in the wire format's style
(`stand_in_a.mjs`, `stand_in_b.mjs`). It is **not the decoder of the decode gate**;
only the noise of code of that kind is used.

| size | elements | covers |
|---|---|---|
| S300 | 280 | fixtures (a) and (b), about 7.5 KB of custom brotli |
| S2k | 2,000 | fixture (d), and the low end of (c) |
| S10k | 10,000 | fixture (e), and the high end of (c) |

Nine groups in all, run in this fixed order in every process: F, A, C, each at S300,
S2k, S10k.

### One process (a trial)

A trial is a fresh `node trial.mjs`. It loads the inputs into memory before any
timing. For each group, in turn:

1. Both slots are made afresh (below).
2. **Warm-ups**, not recorded: 5 per slot, in the order `1 2 2 1 2 1 1 2 1 2`.
3. **Timed iterations**: 30 per slot, as 15 blocks of four calls that alternate
   `1 2 2 1` and `2 1 1 2`, starting with `1 2 2 1` (8 blocks of the first and 7 of
   the second). Counterbalancing keeps a periodic effect from always landing on one
   slot, and alternating the blocks also removes an effect aligned to the block.
4. The timed region is one call of a slot's decode: `process.hrtime.bigint()` before
   and after, in nanoseconds. The checksum the call returns is compared with the
   expected one after the clock has stopped.
5. `m1` is the median of slot 1's 30 times, `m2` of slot 2's, and `r = m1 / m2`. The
   median of an even count is the mean of the two middle values. All 60 raw times and
   the executed order are kept.

**Position is counterbalanced by the trial's parity.** A trial has an index in its job,
1 to `processes`. In an odd trial the slots are made and run as above. In an **even**
trial the mirror image runs: slot 2 is made first, and the numbers 1 and 2 are swapped in
both sequences (warm-ups `2 1 1 2 1 2 2 1 2 1`, blocks `2 1 1 2` and `1 2 2 1`, starting
with `2 1 1 2`). `r` is slot 1 over slot 2 in every trial. A bias that comes from being
made, warmed or timed first then has opposite signs in alternate trials, and the median
of 20 cancels it. Every group record carries its trial index, a `mirror` flag and the
executed order, and `summarize.mjs` accepts the registered strings in an odd trial, their
mirror in an even one, and nothing else.

A job (`run.mjs`) runs `processes` trials one after another, each started with the index
that sets its parity and without `NODE_OPTIONS` and `NODE_COMPILE_CACHE` (a flag or a
compile cache would carry state from one process to the next), and writes `env.json`
(which records both variables as the trials saw them, empty, and as the job had them) and
`results-<job>.json`.

### Independent slots

The two slots of a group share no function, so that inline caches, tier-up and
deoptimisations are independent, as they are between the gate's two codecs:

- each slot is a fresh import of its slot module (`slot_flechette.mjs`,
  `slot_arrow.mjs`, `slot_standin.mjs`) with a query string of its own, which gives it
  its own copy of `fullread.mjs` and, for C, of the stand-in reader;
- Flechette's build file `dist/flechette.cjs` and apache-arrow's single-file build
  `Arrow.esnext.min.js` are evaluated afresh for each slot, by `createRequire` with
  the entry deleted from `require.cache` before and after the load;
- the stand-in exists as two copies of its source, `stand_in_a.mjs` (slot 1) and
  `stand_in_b.mjs` (slot 2), which differ in their first line only;
- the groups do not share instances either: every group makes its two slots anew, after
  the inputs are loaded, so each group starts cold and has its own warm-ups. A decode
  gate bound to this protocol creates both codecs' instances afresh for each group, after
  the inputs are loaded.

These are the two build files a decode gate that uses this spread must time.

## Fixed inputs

`gen.mjs` writes, for each size, into `fixtures/`:

- `s<size>.arrow`: the twelve streams of a fixture, concatenated, each written by one
  call of apache-arrow 21.2.0's `tableToIPC`;
- `s<size>.arrow.offsets.json`: the byte range of each stream, the row counts, and the
  checksums a full read must return;
- `s<size>.tsgf`: the stand-in custom layout of the same values.

Provenance: **the shape of `bytesgate.Arrow` at origin/main 3d92092; the values are
synthetic; no output of bytesgate is used.** No byte of any fixture of the bytes gate
is checked in here, and none of its sizes can be read from these files. Every value
comes from a seeded 32-bit xorshift in `gen.mjs`; there is no `Math.random` and no
clock, and the nine files are the same on every run (`node gen.mjs check` and a test
prove it).

The shape: twelve streams in the order of the format's tags (`header`, `entities`,
`elements`, `keyframes`, `deltas`, `quarantine`, `truncated`, `unattributed`,
`unmapped`, `suppressed`, `pods`, `anchor`), each with its own schema, one record
batch and the end-of-stream marker, with no body compression. The types and nesting
of every column are those of `bytesgate.Arrow` (an entity reference is a `uint32`; an
entity is `fixed_size_binary(18)` with a boundary `bool`; times are `int64`
nanoseconds; flags are `bool`; enumerations are `uint8`; optional values are nulls;
`paths` is `list<list<uint32>>`; `pieces` is a list of structs with `producers` and
`ends` lists inside; `quarantine` nests an element in a struct in a list). A test
holds the written schemas to a table of them, column for column, nullability
included, and a list item is a non-nullable field named `item`.

### What the generator makes

| | rows or values |
|---|---|
| entities | N / 2, rounded up |
| elements | N (280, 2000, 10000) |
| element kind | 40 % entity, 45 % edge, 15 % derived |
| `paths` of an element | 0 for an asserted subject (entity or edge); 1 to 3 for a derived one |
| legs of a path | 1 to 4 |
| `pieces` of an element | 1 to 3 |
| `producers` of a piece | 1 to 2, from a pool of eight names |
| `ends` of a piece | 0 to 2 |
| deltas | round(0.3 N), sorted by time |
| keyframes | 1 + floor(deltas / (N / 2)) |
| `elements` of a keyframe | the elements that have a piece covering the keyframe's time |
| quarantine | one row: round(N / 50) entities (at least 2), round(N / 100) edges (at least 1) |
| truncated | one row: sorted id lists of about round(N / 60) ids (at least 2), three frontier entries, four per-via entries |
| unattributed | round(N / 100) (at least 3) |
| unmapped | round(N / 50) (at least 3) |
| suppressed | round(N / 80) (at least 2) |
| pods | round(N / 70) (at least 3) |
| header, anchor | one row each |

Times are whole milliseconds inside a one-hour window, written as nanoseconds. Every
`int64` and `uint64` is below 2^53, so the stand-in can hold them as numbers, as a custom
decoder would hold relative times; the header's `from` is 10^15 ns, where a real clock
reading would be about 1.7 x 10^18. The libraries read them as BigInt anyway (see the
full read). Every string is
printable ASCII, so a UTF-16 code unit is a byte. A struct column that is nullable
(`collision` in `quarantine`) is null in about 70 % of its rows.

### Splitting

The twelve streams are cut by the ranges `s<size>.arrow.offsets.json` records
(`split.mjs`), before the clock starts. Each range starts at a multiple of 8 and ends
with the end-of-stream marker `0xFFFFFFFF 0x00000000`. A consumer that has only the
concatenation can find the same cuts by walking the IPC messages to each marker. Either
way the cut is outside the timed region, so its method changes no time.

### The stand-in layout

Twelve sections, each a tag byte (1 to 12), a uvarint length and a payload. Inside a
payload: uvarint ids and counts, a presence mask byte before optional fields,
length-prefixed ASCII strings, fixed-width byte strings, sorted id lists
delta-encoded, and times as uvarints relative to the start of the window (an end as
its distance from its start). The reader refuses a truncated buffer, an overlong
uvarint (more than 8 bytes), a section out of order, a section it does not read to
its end, and trailing bytes.

## The full read

The full read is what is done with the decoded tables inside the timed region. It is
**columnar**:

- **Leaf columns only** are materialised: `toArray()` is called on each primitive leaf
  (unsigned and signed integers, `bool`, `fixed_size_binary`, `utf8`, `binary`).
  `toArray()` is never called on a list or a struct, and no row-level object and no
  proxy is made.
- A **list** is walked by its offsets array (a typed array) and then by its child
  column. A nested list is the same, one level down.
- A **struct** is walked by its child columns.
- apache-arrow is reached through `Table.getChildAt`, `Vector.data[0].valueOffsets`,
  `Vector.numChildren`, `Vector.getChildAt` and `Vector.toArray`. Flechette is reached
  through `Table.getChildAt`, `Column.data[0]` (a record batch), the batch's `offsets`
  and `children`, and `Column.toArray` of a column over one leaf batch.
- The stand-in reader produces nodes of the same shape (typed arrays and offsets), and
  the same walk reads them.

A **checksum** is folded over everything read, so that nothing can be optimised away:

- `h` starts at 2166136261. Each node, in depth-first order, with the columns of a table
  in schema order, folds a number `c` into `h` as `h = imul(h ^ c, 16777619) >>> 0`:
  - a **list**: `c` is the sum of its list lengths (a null list has length 0);
  - a **struct**: `c` is its length;
  - a **leaf**: `c` is the sum of its values modulo 2^32, where
    - an unsigned or signed integer adds its value;
    - an `int64` or `uint64` adds its **low 32 bits**, whether the library returns a
      BigInt or a number. Both libraries read int64 as BigInt: apache-arrow 21.2.0 always
      does, and Flechette 2.5.0 is called with `tableFromIPC(view, { useBigInt: true })`.
      Flechette's default (`useBigInt` false) hands numbers and throws above 2^53, and real
      timestamps (int64 nanoseconds, about 1.7 x 10^18) are above it, so a decode of real
      data cannot use the default. The checksum is the same for both representations;
    - a `bool` adds 0 or 1;
    - `fixed_size_binary` and `binary` add **every byte**;
    - `utf8` adds every UTF-16 code unit;
    - a null adds nothing.
- Every null of every node adds 1 to a separate counter `n`. A null slot of a struct is a
  null slot in each of its fields, as the Arrow writers make it.
- The checksum is `(h + n) >>> 0`. The twelve tables of a fixture share one `h` and one
  `n`.
- The generator computes the expected checksum from the values themselves, not from
  any bytes, in code that shares nothing with `fullread.mjs`. Every call of every slot
  is checked against it.

This definition is the model that a decode gate's own columnar scrub and frame
definitions should follow. The decode gate is not bound to it: it registers its own
definitions before any figure exists, including the representation in which each codec
hands int64 values to the client (BigInt, or a number of relative nanoseconds or
milliseconds), the same for all codecs where the client needs the same thing.

## The spread

For each ratio `r`, `d = |ln r|`, so that `m1 / m2` and `m2 / m1` count alike.

- **Per group** (workload and size), over every ratio of every job of every dispatch:
  `S_g` is the 95th percentile of `d` by nearest rank (rank `ceil(0.95 n)`), written
  as a per cent, `(e^d - 1) x 100`.
- **S is the largest `S_g` over the nine groups.** It is not a pooled figure: the gate's
  comparison is of two different codecs, whose variances add, and the noisier side's
  A/A is what bounds that.
- **By CPU part.** A job records its CPU implementer and part. The rule, word for word
  (for the registration):

  > A CPU part is split out of a group only where it has at least 60 ratios in that
  > group. S_g is then the largest of: the figure of each split part; and, if there are
  > ratios of parts below 60, their pooled figure when they number at least 60, and
  > otherwise the pooled figure over every ratio of the group. If no part has 60 ratios,
  > S_g is the pooled figure over every ratio of the group.
- **Reported, never decided on:** `S` by dispatch, by CPU part and by job; the largest
  `S_g` by size; the cross-process A/A, slot 1's medians of two processes of one job
  against each other, every pair; and the independence diagnostics below.

### The check on independence

The gate's ratio is the median of 20 per-process ratios of one job, which is as steady as
an average of independent values only if the processes of a job are independent. The
measurement tests that on its own data. For each job and group take the signed
`l = ln r` of its processes in trial order.

- **`S_med`** is the 95th percentile (nearest rank) of `|median of the 20 l of a job and
  group|` over every job and group of both dispatches (108 values for 12 jobs), written
  as `(e^x - 1) x 100`. It is the gate's own statistic in A/A form.
- **The check** (decisive only in the 7-10 % band of S, see below): if 7 % < S <= 10 %,
  T3 = -0.10 only if `S_med` <= 5.00 %; otherwise T3 = -0.15. `S_med`, the slot bias, the
  position effect, the lag-1 autocorrelation and the intraclass correlation are reported in
  every case and decide nothing else. The threshold, 5.00 %, is `S_MED_LIMIT` in `summarize.mjs`.
- **Reported for every group and over all groups** (`independence.mjs`). With the parity
  mirror, `r` is always slot 1 over slot 2 but the slot that runs first alternates, so the
  pooled mean of `l` measures the slot, not the position:
  - the **slot bias**: the pooled median and mean of `l` as a per cent, and a cluster t, the
    mean of the job means over its standard error across jobs (about 0 and |t| under 2
    under independence);
  - the **position effect**: per job, (the mean of `l` over odd trials minus its mean over
    even trials) / 2; its mean over jobs as a per cent, and its t over jobs (about 0 and
    |t| under 2 when position does not matter). With 12 jobs t has 11 degrees of freedom,
    so about 0.6 of the 9 groups will show |t| > 2 by chance alone;
  - on the series with each job's odd and even means removed separately (the job mean put
    back), so that a position effect does not pass as correlation or as variance: the
    lag-1 autocorrelation within jobs (about -1/20 under independence); a one-way
    random-effects split by job (within-job variance on n - 2 degrees of freedom, the
    between-job variance component, and the intraclass correlation ICC, about 0); and the
    inflation factor `k`, the sd of the job medians over `1.2533 sd_within / sqrt(20)`
    (about 1).

### The bands

The rule registered for T3 is the "keep" form: -0.10 up to 10 %, with the independence
check as the fallback. `summarize.mjs` (`band()`) computes the band and prints T3 and E,
the edge of the band that sets T3, with which a decode gate's void rule compares its own
spread; the registration is the authority.

| S | T3 | E |
|---|---|---|
| under 3 % | -0.05 is allowed (a maintainer chooses -0.05 or -0.10 and records it) | 3 % for -0.05, 7 % for -0.10 |
| 3 % to 7 % | -0.10 | 7 % |
| over 7 % and up to 10 % | -0.10 if `S_med` <= 5.00 %; otherwise -0.15 | 10 % |
| over 10 % | none; the decode gate does not run until a maintainer registers a change | none |

S and `S_med` are compared with the edges after rounding to four decimals of a per cent.
If the S300 groups are over 10 % while S2k and S10k are at or under 10 % (compared the
same way), `summarize.mjs` says so by name (the registered gate, which judges every
fixture and has no floor, cannot decide on fixtures (a) to (c)).

## Running it

Everything runs from this directory, on the Node of the pin.

```sh
npm ci --ignore-scripts --no-audit --no-fund    # the lockfile pins every package
sha256sum --check --strict FILES.sha256          # the files that define the measurement
node gen.mjs check                               # the fixtures, made again, byte for byte
node --test                                      # the tests (node --test <directory> does not work on Node 24)
node run.mjs --job 0 --processes 1 --out /tmp/wirenoise-dry --dry   # one trial with no time in it
```

`summarize.mjs` reads the artifacts of one or more dispatches and takes the registered
values as required arguments:

```sh
node summarize.mjs --dir <artifacts of dispatch 1> --dir <artifacts of dispatch 2> \
  --sha <commit> --files <sha256 of FILES.sha256> --wf <sha256 of wirenoise.yml> \
  --tag gate-freeze-YYYYMMDD --dispatches 2 --processes 20
```

All seven are required: `--tag` is the registered freeze tag, `--dispatches` the number of
dispatches and `--processes` the number of processes of a job (20; an even number, so that
odd and even trials balance). The workflow's own summary passes the dispatch's value. It
refuses (exit status 1, nothing on the standard output) a job that did not run from the tag
given, that ran at another commit, or was dispatched with another, or with other hashes
than those given, on another architecture than `aarch64` or another Node than v24.21.0,
that did not finish, that ran another number of processes than `--processes` or holds
another number of trials, that lacks a group, that did not follow the registered order and
counts for its trial's parity, or whose medians are not those of its
raw times. It groups jobs into dispatches by run id and attempt, and refuses a dispatch
with fewer jobs than it was made for. A "re-run failed jobs" attempt is therefore its own
dispatch holding only the re-run jobs, and is refused as incomplete: a failed dispatch is
run again as a new dispatch.
`FILES.sha256` is rewritten by `node files.mjs write` after any change to a
file it lists; nothing else writes it.

## Pins

| | |
|---|---|
| Node | v24.21.0, `node-v24.21.0-linux-arm64.tar.xz`, sha256 `6ad1325edbdb5649c379b75a237147a666c95d4f9ae8d340fef2d1575d289ad2` (from nodejs.org's `SHASUMS256.txt`), installed by `install-node.sh` |
| apache-arrow | 21.2.0, build file `Arrow.esnext.min.js` |
| @uwdata/flechette | 2.5.0, build file `dist/flechette.cjs`, called with `{ useBigInt: true }` |
| lockfile | `package-lock.json`, made with `npm install --save-exact --ignore-scripts --before 2026-10-01T00:00:00Z`; every package in it was published before that instant |

Renovate does not bump anything here: the pins are registered. The Node of the runner
image is recorded in `env.json` and never used.

## What this does not do

- It does not time Arrow against the custom codec, and prints no such figure.
- It does not use PR 5's decoder, fixtures or full read.
- It does not decide T3. The registered rule does, from the S that `summarize.mjs` prints.
- Two dispatches sample two times of day, not a week.
