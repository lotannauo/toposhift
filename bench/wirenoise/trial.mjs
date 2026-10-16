// One trial: one fresh node process that measures the nine groups once.
//
// A group is a workload (F: Flechette, A: apache-arrow, C: the stand-in custom
// reader) at a size (S300, S2k, S10k). In a group the same decode runs in two slots,
// 1 and 2, each with an independently loaded copy of the code, in a counterbalanced
// order. The true difference between the slots is zero by construction: what the
// group yields, r = m1 / m2, is noise.
//
//   warm-ups, not recorded: the first 2n of 1 2 2 1 2 1 1 2 1 2 (n per slot)
//   timed: blocks of four that alternate 1221 and 2112, starting with 1221, so that
//          30 timed iterations per slot are 15 blocks, 8 of 1221 and 7 of 2112
//
// The position of a slot is counterbalanced by the parity of the trial's index:
// in an odd trial the slots are made and run as above; in an even trial the mirror
// image is run, slot 2 made first and the numbers 1 and 2 swapped in both
// sequences. r is slot 1 over slot 2 in every trial.
//
//   node trial.mjs --index N      a trial: 5 warm-ups and 30 timed iterations per slot,
//                                 one JSON line for each group, with the raw times;
//                                 N is the trial's index in its job (1 if not given)
//   node trial.mjs --dry          1 warm-up and 4 timed iterations per slot, and for
//                                 each group a line that has no time in it at all

import { pathToFileURL } from 'node:url';

import { loadInputs } from './split.mjs';
import { makeSlot } from './slots.mjs';

export const WORKLOADS = ['F', 'A', 'C'];
export const SIZE_NAMES = ['S300', 'S2k', 'S10k'];
/** The groups in the order every process runs them. */
export const GROUPS = WORKLOADS.flatMap((workload) => SIZE_NAMES.map((size) => ({ workload, size })));

/** The registered order of warm-ups: 0 is slot 1, 1 is slot 2. */
export const WARMUP_SEQUENCE = [0, 1, 1, 0, 1, 0, 0, 1, 0, 1];

/** An even trial runs the mirror image of the registered order. */
export const isMirror = (index) => index % 2 === 0;

const flip = (order, mirror) => (mirror ? order.map((x) => 1 - x) : order);

/** The warm-ups for n per slot: the first 2n of the registered sequence, or its mirror. */
export function warmupOrder(n, mirror = false) {
  if (!Number.isInteger(n) || n < 1 || n > 5) throw new Error(`trial: ${n} warm-ups per slot, want 1 to 5`);
  return flip(WARMUP_SEQUENCE.slice(0, 2 * n), mirror);
}

/** The timed order for `iterations` per slot (even): blocks 0110, 1001, 0110, ... or the mirror. */
export function timedOrder(iterations, mirror = false) {
  if (!Number.isInteger(iterations) || iterations < 2 || iterations % 2 !== 0) throw new Error(`trial: ${iterations} timed iterations per slot, want an even number`);
  const out = [];
  for (let b = 0; b < iterations / 2; b++) out.push(...(b % 2 === 0 ? [0, 1, 1, 0] : [1, 0, 0, 1]));
  return flip(out, mirror);
}

/** The median: the middle value, or the mean of the two middle values. */
export function median(xs) {
  const s = [...xs].sort((a, b) => a - b);
  const n = s.length;
  return n % 2 === 1 ? s[(n - 1) / 2] : (s[n / 2 - 1] + s[n / 2]) / 2;
}

const letters = (order) => order.map((s) => s + 1).join('');

/**
 * Runs one group.
 * @param {object} o
 * @param {string} o.workload
 * @param {string} o.size
 * @param {{views: Uint8Array[], tsgf: Uint8Array, expected: number}} o.inputs
 * @param {number} o.warmups per slot
 * @param {number} o.iterations per slot, even
 * @param {number} [o.index] the trial's index in its job; an even one runs the mirror order
 * @param {(workload: string, which: 1|2) => Promise<{decode: Function}>} [o.makeSlot]
 * @param {() => bigint} [o.clock] nanoseconds
 */
export async function runGroup({ workload, size, inputs, warmups, iterations, index = 1, makeSlot: make = makeSlot, clock = process.hrtime.bigint }) {
  const mirror = isMirror(index);
  // the slots are made in the order they run first: slot 2 first in a mirrored trial
  const slots = [];
  for (const which of mirror ? [2, 1] : [1, 2]) slots[which - 1] = await make(workload, which);
  const call = (which) => {
    const t0 = clock();
    const sum = slots[which].decode(inputs);
    const t1 = clock();
    // checked after the clock has stopped
    if (sum !== inputs.expected) throw new Error(`trial: ${workload} ${size} slot ${which + 1} read ${sum}, want ${inputs.expected}`);
    return Number(t1 - t0);
  };
  const warm = warmupOrder(warmups, mirror);
  for (const which of warm) call(which);
  const timed = timedOrder(iterations, mirror);
  const times = [[], []];
  for (const which of timed) times[which].push(call(which));
  const m1 = median(times[0]);
  const m2 = median(times[1]);
  return {
    workload,
    size,
    trial: index,
    mirror,
    m1,
    m2,
    r: m1 / m2,
    order: { warmup: letters(warm), timed: letters(timed) },
    times1: times[0],
    times2: times[1],
  };
}

/** What a dry run writes for a group: it has no time in it. */
export function dryLine(rec) {
  return { workload: rec.workload, size: rec.size, trial: rec.trial, mirror: rec.mirror, order: rec.order, timed1: rec.times1.length, timed2: rec.times2.length, done: true };
}

/**
 * Runs the nine groups in their fixed order. `emit` is called with each group's record.
 */
export async function runTrial({ warmups, iterations, index = 1, emit, groups = GROUPS, inputsFor = loadInputs, make = makeSlot }) {
  const inputs = new Map();
  for (const { size } of groups) if (!inputs.has(size)) inputs.set(size, inputsFor(size)); // before any timing
  for (const { workload, size } of groups) {
    emit(await runGroup({ workload, size, inputs: inputs.get(size), warmups, iterations, index, makeSlot: make }));
  }
}

function parseArgs(argv) {
  const o = { dry: false, warmups: null, iterations: null, index: 1 };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--dry') o.dry = true;
    else if (argv[i] === '--warmups') o.warmups = Number(argv[++i]);
    else if (argv[i] === '--iterations') o.iterations = Number(argv[++i]);
    else if (argv[i] === '--index') o.index = Number(argv[++i]);
    else throw new Error(`trial: unknown argument ${argv[i]}`);
  }
  if (!Number.isInteger(o.index) || o.index < 1) throw new Error('trial: --index is a positive integer');
  o.warmups ??= o.dry ? 1 : 5;
  o.iterations ??= o.dry ? 4 : 30;
  return o;
}

async function main() {
  const o = parseArgs(process.argv.slice(2));
  await runTrial({
    warmups: o.warmups,
    iterations: o.iterations,
    index: o.index,
    emit: (rec) => console.log(JSON.stringify(o.dry ? dryLine(rec) : rec)),
  });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main().catch((err) => {
    console.error(err);
    process.exitCode = 1;
  });
}
