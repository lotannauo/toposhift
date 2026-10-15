// Reads the jobs of one or more dispatches and gives the spread S and its band.
// It is pure: it reads files, prints, and writes only the file --json names.
//
//   node summarize.mjs --dir DIR [--dir DIR ...] --sha SHA --files SHA256 --wf SHA256
//                      --tag gate-freeze-YYYYMMDD --dispatches N --processes K [--json FILE]
//
// --sha, --files and --wf are the registered commit and the registered hashes of
// bench/wirenoise/FILES.sha256 and of .github/workflows/wirenoise.yml; --tag is the
// registered freeze tag, which every job must have run from; --dispatches is the
// number of dispatches the run is made of; --processes is the number of processes of a
// job (20; even, so that odd and even trials balance). All seven are required. A job that
// ran at another commit or with another hash, or that is not valid in another way
// (see check), makes the whole run refuse: nothing is printed on the standard
// output, the reason goes to the standard error, and the exit status is 1.
//
// The statistic (README.md, "The spread"): for each ratio r = m1 / m2 of a group,
// d = |ln r|; the group's spread S_g is the 95th percentile of d, nearest rank,
// written as (e^d - 1) * 100 per cent. S is the largest S_g over the nine groups.

import { readdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { independence } from './independence.mjs';
import { GROUPS, isMirror, median, timedOrder, warmupOrder } from './trial.mjs';

export const NODE_VERSION = 'v24.21.0';
export const WARMUPS = 5;
export const ITERATIONS = 30;
/** A CPU part is split out only where it has at least this many ratios in the group. */
export const MIN_PART_RATIOS = 60;

export class Refusal extends Error {}

const refuse = (msg) => {
  throw new Refusal(msg);
};

// ---------------------------------------------------------------------------
// Reading the jobs.

function walk(dir, out = []) {
  for (const name of readdirSync(dir).sort()) {
    const p = path.join(dir, name);
    if (statSync(p).isDirectory()) walk(p, out);
    else if (name === 'env.json') out.push(p);
  }
  return out;
}

/** Finds each env.json under the directories, with the results file beside it. */
export function readJobs(dirs) {
  const jobs = [];
  for (const dir of dirs) {
    for (const envFile of walk(dir)) {
      const folder = path.dirname(envFile);
      const results = readdirSync(folder).filter((n) => /^results-.*\.json$/.test(n));
      if (results.length !== 1) refuse(`${folder}: ${results.length} results files, want 1`);
      jobs.push({
        where: folder,
        env: JSON.parse(readFileSync(envFile, 'utf8')),
        results: JSON.parse(readFileSync(path.join(folder, results[0]), 'utf8')),
      });
    }
  }
  if (jobs.length === 0) refuse('no job found');
  return jobs;
}

const letters = (order) => order.map((s) => s + 1).join('');
/** The registered order strings of a trial: the registered ones if odd, their mirror if even. */
const expectedOrder = (index) => ({ warmup: letters(warmupOrder(WARMUPS, isMirror(index))), timed: letters(timedOrder(ITERATIONS, isMirror(index))) });

/**
 * Refuses a job that does not count: a ref that is not a freeze tag (or not the
 * registered one, when it is given), another commit or file hash than the
 * registered ones, another architecture or Node version, a job that did not
 * finish, a trial without all nine groups, or a group that did not follow the
 * registered order and counts, or whose figures are not those its raw times give.
 */
export function check(job, reg) {
  const { env, results, where } = job;
  const bad = (msg) => refuse(`${where}: ${msg}`);
  if (!Number.isInteger(reg.processes) || reg.processes < 2 || reg.processes % 2 !== 0) refuse('processes: an even number, so that the parities balance');
  if (env.processes !== reg.processes) bad(`the job ran ${env.processes} processes, want ${reg.processes}`);
  if (!/^refs\/tags\/gate-freeze-[0-9]{8}$/.test(env.github_ref ?? '')) bad('it did not run from a freeze tag');
  if (reg.tag !== undefined && env.github_ref !== `refs/tags/${reg.tag}`) bad('it ran from a tag other than the registered one');
  if (env.github_sha !== reg.sha) bad('it ran at a commit other than the registered one');
  if (env.expected_sha !== reg.sha) bad('it was dispatched with a commit other than the registered one');
  if (env.files_sha256 !== reg.files) bad('the hash of FILES.sha256 is not the registered one');
  if (env.workflow_sha256 !== reg.wf) bad('the hash of wirenoise.yml is not the registered one');
  if (env.uname_m !== 'aarch64') bad(`the architecture is ${env.uname_m}, want aarch64`);
  if (env.node_version !== NODE_VERSION) bad(`Node is ${env.node_version}, want ${NODE_VERSION}`);
  if (results.finished !== true) bad('the job did not finish');
  if (!Number.isInteger(env.processes) || results.trials.length !== env.processes) bad(`${results.trials.length} trials, want ${env.processes}`);
  for (const trial of results.trials) {
    if (trial.groups.length !== GROUPS.length) bad(`trial ${trial.index} wrote ${trial.groups.length} groups, want ${GROUPS.length}`);
    trial.groups.forEach((g, i) => {
      const want = GROUPS[i];
      if (g.workload !== want.workload || g.size !== want.size) bad(`trial ${trial.index}: group ${i} is ${g.workload} ${g.size}, want ${want.workload} ${want.size}`);
      const wantOrder = expectedOrder(trial.index);
      if (g.trial !== trial.index || g.mirror !== isMirror(trial.index)) bad(`trial ${trial.index}: ${g.workload} ${g.size} does not carry its trial's index and parity`);
      if (g.order?.warmup !== wantOrder.warmup || g.order?.timed !== wantOrder.timed) bad(`trial ${trial.index}: ${g.workload} ${g.size} did not run the registered order for its parity`);
      if (g.times1?.length !== ITERATIONS || g.times2?.length !== ITERATIONS) bad(`trial ${trial.index}: ${g.workload} ${g.size} has ${g.times1?.length} and ${g.times2?.length} times, want ${ITERATIONS}`);
      const m1 = median(g.times1);
      const m2 = median(g.times2);
      if (g.m1 !== m1 || g.m2 !== m2 || g.r !== m1 / m2) bad(`trial ${trial.index}: ${g.workload} ${g.size} does not match its raw times`);
      if (!(m1 > 0 && m2 > 0)) bad(`trial ${trial.index}: ${g.workload} ${g.size} has a median that is not positive`);
    });
  }
}

// ---------------------------------------------------------------------------
// The statistic.

/** The 95th percentile by nearest rank, of a list of numbers. */
export function p95(xs) {
  const s = [...xs].sort((a, b) => a - b);
  const rank = Math.floor((95 * s.length + 99) / 100); // ceil(0.95 n) in integers
  return s[rank - 1];
}

/** A log distance as a percentage: (e^d - 1) * 100. */
export const pct = (d) => (Math.exp(d) - 1) * 100;

/**
 * The spread of one group. `entries` are {d, part} for every ratio. Where a CPU
 * part has at least MIN_PART_RATIOS ratios it is split out, and S_g is the largest
 * of the parts' spreads; the ratios of the other parts are pooled, and the pool
 * counts as a part when it has MIN_PART_RATIOS ratios of its own and otherwise
 * stands for itself in the pooled figure over every ratio, which is then one of
 * the figures the largest is taken over. With no part that large, S_g is the pooled
 * figure over every ratio.
 */
export function groupSpread(entries) {
  const byPart = new Map();
  for (const e of entries) {
    if (!byPart.has(e.part)) byPart.set(e.part, []);
    byPart.get(e.part).push(e.d);
  }
  const all = entries.map((e) => e.d);
  const parts = [...byPart].map(([part, ds]) => ({ part, n: ds.length, s: pct(p95(ds)) })).sort((a, b) => (a.part < b.part ? -1 : 1));
  const big = parts.filter((p) => p.n >= MIN_PART_RATIOS);
  if (big.length === 0) return { n: all.length, s: pct(p95(all)), split: false, parts };
  const figures = big.map((p) => p.s);
  const rest = entries.filter((e) => byPart.get(e.part).length < MIN_PART_RATIOS);
  if (rest.length > 0) figures.push(rest.length >= MIN_PART_RATIOS ? pct(p95(rest.map((e) => e.d))) : pct(p95(all)));
  return { n: all.length, s: Math.max(...figures), split: true, parts };
}

/** S and S_med are compared with the registered edges to four decimals of a per cent. */
export const round4 = (x) => Math.round(x * 1e4) / 1e4;

/** The independence check's threshold: S_med at or under this keeps -0.10 in the 7-10 % band. */
export const S_MED_LIMIT = 5.0;

/**
 * The band of S, by the rule registered for T3 (the "keep" form), and E, the edge of
 * the band that sets T3, which a decode gate's void rule compares with.
 *
 *   S under 3 %          T3 = -0.05 is allowed (E = 3 %); -0.10 is also open (E = 7 %)
 *   3 % <= S <= 7 %      T3 = -0.10 (E = 7 %)
 *   7 % < S <= 10 %      T3 = -0.10 (E = 10 %) only if S_med <= 5.00 %; otherwise -0.15 (E = 10 %)
 *   S over 10 %          no T3: the decode gate does not run until a change is registered
 *
 * S_med decides nothing outside the 7-10 % band.
 */
export function band(s, sMedValue) {
  const v = round4(s);
  const m = sMedValue === null || sMedValue === undefined ? null : round4(sMedValue);
  if (v < 3) {
    return { id: 'under-3', t3: -0.05, edges: [{ t3: -0.05, e: 3 }, { t3: -0.1, e: 7 }], fallback: false, text: 'S under 3 %: T3 = -0.05 is allowed, E = 3 % (a maintainer chooses -0.05, E = 3 %, or -0.10, E = 7 %, and records it)' };
  }
  if (v <= 7) return { id: '3-to-7', t3: -0.1, edges: [{ t3: -0.1, e: 7 }], fallback: false, text: 'S from 3 % to 7 %: T3 = -0.10, E = 7 %' };
  if (v <= 10) {
    if (m !== null && m <= S_MED_LIMIT) {
      return { id: '7-to-10', t3: -0.1, edges: [{ t3: -0.1, e: 10 }], fallback: false, text: `S over 7 % and up to 10 %, S_med ${m.toFixed(2)} % <= 5.00 %: T3 = -0.10, E = 10 %` };
    }
    const why = m === null ? 'S_med not available' : `S_med ${m.toFixed(2)} % > 5.00 %`;
    return { id: '7-to-10', t3: -0.15, edges: [{ t3: -0.15, e: 10 }], fallback: true, text: `S over 7 % and up to 10 %, ${why}: the independence check falls back to T3 = -0.15, E = 10 %` };
  }
  return { id: 'over-10', t3: null, edges: [], fallback: false, text: 'S over 10 %: no T3 is set and the decode gate does not run until a maintainer registers a change' };
}

/** The cross-process A/A: slot 1's medians of two processes of one job, every pair. */
function crossSpread(jobs, workload, size) {
  const ds = [];
  for (const job of jobs) {
    const ms = job.results.trials.map((t) => t.groups.find((g) => g.workload === workload && g.size === size).m1);
    for (let i = 0; i < ms.length; i++) for (let j = i + 1; j < ms.length; j++) ds.push(Math.abs(Math.log(ms[i] / ms[j])));
  }
  return ds.length === 0 ? null : pct(p95(ds));
}

function entriesFor(jobs, workload, size) {
  const out = [];
  for (const job of jobs) {
    for (const trial of job.results.trials) {
      const g = trial.groups.find((x) => x.workload === workload && x.size === size);
      out.push({ d: Math.abs(Math.log(g.r)), part: job.env.cpu?.part ?? 'unknown' });
    }
  }
  return out;
}

/** S over the jobs given: the groups' spreads, S, the group it comes from. */
function spreadOf(jobs) {
  const groups = GROUPS.map(({ workload, size }) => {
    const sp = groupSpread(entriesFor(jobs, workload, size));
    return { workload, size, ...sp };
  });
  const worst = groups.reduce((a, b) => (b.s > a.s ? b : a));
  return { groups, s: worst.s, worst: `${worst.workload} ${worst.size}` };
}

/**
 * Checks every job against the registration and computes the report.
 * @param {{where: string, env: object, results: object}[]} jobs
 * @param {{sha: string, files: string, wf: string, processes: number, tag?: string, dispatches?: number}} reg
 */
export function summarize(jobs, reg) {
  for (const job of jobs) check(job, reg);
  const byDispatch = new Map();
  for (const job of jobs) {
    const key = `${job.env.run_id}/${job.env.run_attempt}`;
    if (!byDispatch.has(key)) byDispatch.set(key, []);
    byDispatch.get(key).push(job);
  }
  for (const [key, js] of byDispatch) {
    const want = Number(js[0].env.jobs);
    const ids = new Set(js.map((j) => j.env.job));
    if (ids.size !== js.length || js.length !== want) refuse(`dispatch ${key}: ${js.length} jobs (${ids.size} distinct), want ${want}`);
  }
  if (reg.dispatches !== undefined && byDispatch.size !== reg.dispatches) refuse(`${byDispatch.size} dispatches, want ${reg.dispatches}`);

  const main = spreadOf(jobs);
  const indep = independence(jobs);
  const sMedPct = indep.s_med_pct;
  const s300 = main.groups.filter((g) => g.size === 'S300').map((g) => g.s);
  const rest = main.groups.filter((g) => g.size !== 'S300').map((g) => g.s);
  const s300Only = round4(Math.max(...s300)) > 10 && round4(Math.max(...rest)) <= 10;
  const sizes = {};
  for (const size of ['S300', 'S2k', 'S10k']) sizes[size] = Math.max(...main.groups.filter((g) => g.size === size).map((g) => g.s));

  // Reported, never decided on.
  const perDispatch = [...byDispatch].map(([key, js]) => ({ dispatch: key, jobs: js.length, s: spreadOf(js).s }));
  const partNames = [...new Set(jobs.map((j) => j.env.cpu?.part ?? 'unknown'))].sort();
  const perPart = partNames.map((part) => {
    const js = jobs.filter((j) => (j.env.cpu?.part ?? 'unknown') === part);
    return { part, jobs: js.length, s: spreadOf(js).s };
  });
  const perJob = jobs
    .map((j) => ({ dispatch: `${j.env.run_id}/${j.env.run_attempt}`, job: j.env.job, part: j.env.cpu?.part ?? 'unknown', s: spreadOf([j]).s }))
    .sort((a, b) => (a.dispatch + a.job < b.dispatch + b.job ? -1 : 1));
  const cross = main.groups.map((g) => ({ workload: g.workload, size: g.size, s: crossSpread(jobs, g.workload, g.size) }));

  return {
    jobs: jobs.length,
    dispatches: byDispatch.size,
    ratios_per_group: main.groups[0].n,
    s: main.s,
    worst_group: main.worst,
    s_med_pct: sMedPct,
    band: band(main.s, sMedPct),
    s300_only_over_10: s300Only,
    size_max: sizes,
    groups: main.groups,
    per_dispatch: perDispatch,
    per_cpu_part: perPart,
    per_job: perJob,
    cross_process: cross,
    independence: indep,
    node: NODE_VERSION,
  };
}

const f2 = (x) => (x === null || x === undefined ? 'n/a' : x.toFixed(2));
const f3 = (x) => (x === null || x === undefined ? 'n/a' : x.toFixed(3));

/** The report as text. */
export function render(rep) {
  const lines = [];
  lines.push(`Node decode-noise measurement: ${rep.jobs} jobs, ${rep.dispatches} dispatch(es), ${rep.ratios_per_group} ratios per group, Node ${rep.node}`);
  lines.push('');
  lines.push('group     ratios  S_g %   split by CPU part  parts (part: ratios, S %)');
  for (const g of rep.groups) {
    lines.push(`${(g.workload + ' ' + g.size).padEnd(9)} ${String(g.n).padStart(6)}  ${f2(g.s).padStart(6)}  ${(g.split ? 'yes' : 'no').padEnd(17)}  ${g.parts.map((p) => `${p.part}: ${p.n}, ${f2(p.s)}`).join('; ')}`);
  }
  lines.push('');
  lines.push(`S = ${f2(rep.s)} % (the largest S_g, from ${rep.worst_group})`);
  lines.push(`band: ${rep.band.text}`);
  lines.push(`T3 = ${rep.band.t3 === null ? 'none' : rep.band.t3.toFixed(2)}; E = ${rep.band.edges.length ? rep.band.edges.map((x) => `${x.e} % (for T3 ${x.t3.toFixed(2)})`).join(' or ') : 'none'}`);
  lines.push(`S_med = ${f2(rep.s_med_pct)} % (decisive only in the 7-10 % band; fallback to -0.15: ${rep.band.fallback ? 'yes' : 'no'})`);
  if (rep.s300_only_over_10) lines.push('O6: the S300 groups are over 10 % while S2k and S10k are at or under 10 %: the registered gate (every fixture, no floor) cannot decide on fixtures (a) to (c); a floor, a fixture split or more runs per median must be registered before any decode figure exists');
  lines.push('');
  lines.push('Reported, never decided on:');
  lines.push(`  largest S_g by size: ${Object.entries(rep.size_max).map(([k, v]) => `${k} ${f2(v)} %`).join(', ')}`);
  lines.push(`  S by dispatch: ${rep.per_dispatch.map((d) => `${d.dispatch} (${d.jobs} jobs) ${f2(d.s)} %`).join('; ')}`);
  lines.push(`  S by CPU part: ${rep.per_cpu_part.map((p) => `${p.part} (${p.jobs} jobs) ${f2(p.s)} %`).join('; ')}`);
  lines.push(`  S by job (a few ratios each): ${rep.per_job.map((j) => `${j.dispatch}#${j.job} ${f2(j.s)} %`).join('; ')}`);
  const ov = rep.independence.overall;
  lines.push(`  independence, all groups: lag-1 autocorrelation ${f3(ov.lag1)} (about -1/n under independence), mean intraclass correlation ${f3(ov.icc_mean)}, mean inflation k ${f3(ov.k_mean)}`);
  lines.push(`  slot bias: largest |t| ${f2(ov.max_abs_slot_t)} (${ov.groups_with_abs_slot_t_over_2} groups over 2); position effect: largest |t| ${f2(ov.max_abs_pos_t)} (${ov.groups_with_abs_pos_t_over_2} groups over 2; about 0.6 of 9 by chance alone)`);
  lines.push('  by group (slot bias median %, mean %, t; position effect %, t; lag-1; icc; k):');
  for (const g of rep.independence.groups) lines.push(`    ${(g.workload + ' ' + g.size).padEnd(9)} ${f2(g.slot_bias_median_pct)} ${f2(g.slot_bias_mean_pct)} ${f2(g.slot_bias_t)}  ${f2(g.pos_effect_mean_pct)} ${f2(g.pos_effect_t)}  ${f3(g.lag1)}  ${f3(g.icc)}  ${f3(g.k)}`);
  lines.push(`  cross-process A/A, slot 1 medians of two processes of a job, 95th percentile: ${rep.cross_process.map((c) => `${c.workload} ${c.size} ${f2(c.s)} %`).join('; ')}`);
  return `${lines.join('\n')}\n`;
}

function parseArgs(argv) {
  const o = { dirs: [], sha: null, files: null, wf: null, tag: undefined, dispatches: undefined, processes: undefined, json: null };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--dir') o.dirs.push(argv[++i]);
    else if (a === '--sha') o.sha = argv[++i];
    else if (a === '--files') o.files = argv[++i];
    else if (a === '--wf') o.wf = argv[++i];
    else if (a === '--tag') o.tag = argv[++i];
    else if (a === '--dispatches') o.dispatches = Number(argv[++i]);
    else if (a === '--processes') o.processes = Number(argv[++i]);
    else if (a === '--json') o.json = argv[++i];
    else refuse(`unknown argument ${a}`);
  }
  if (o.dirs.length === 0) refuse('--dir is required');
  if (!/^[0-9a-f]{40}$/.test(o.sha ?? '')) refuse('--sha is 40 lowercase hexadecimal digits');
  if (!/^[0-9a-f]{64}$/.test(o.files ?? '')) refuse('--files is 64 lowercase hexadecimal digits');
  if (!/^[0-9a-f]{64}$/.test(o.wf ?? '')) refuse('--wf is 64 lowercase hexadecimal digits');
  if (!/^gate-freeze-[0-9]{8}$/.test(o.tag ?? '')) refuse('--tag is gate-freeze-YYYYMMDD');
  if (!Number.isInteger(o.dispatches) || o.dispatches < 1) refuse('--dispatches is the number of dispatches, 1 or more');
  if (!Number.isInteger(o.processes) || o.processes < 2 || o.processes % 2 !== 0) refuse('--processes is the number of processes of a job, an even number');
  return o;
}

function main() {
  try {
    const o = parseArgs(process.argv.slice(2));
    const rep = summarize(readJobs(o.dirs), { sha: o.sha, files: o.files, wf: o.wf, tag: o.tag, dispatches: o.dispatches, processes: o.processes });
    if (o.json) writeFileSync(o.json, `${JSON.stringify(rep, null, 2)}\n`);
    process.stdout.write(render(rep));
  } catch (err) {
    if (!(err instanceof Refusal)) throw err;
    process.stderr.write(`summarize: refused: ${err.message}\n`);
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) main();
