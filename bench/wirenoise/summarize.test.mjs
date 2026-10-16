import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { independence, sMed } from './independence.mjs';
import { GROUPS, isMirror, median, runGroup, timedOrder, warmupOrder } from './trial.mjs';
import { xorshift } from './gen.mjs';
import { Refusal, S_MED_LIMIT, band, groupSpread, p95, pct, readJobs, render, summarize } from './summarize.mjs';

const here = import.meta.dirname;
const SHA = 'a'.repeat(40);
const FILES = 'b'.repeat(64);
const WF = 'c'.repeat(64);
const REG = { sha: SHA, files: FILES, wf: WF, processes: 20 };
const letters = (o) => o.map((x) => x + 1).join('');

/** A group record of trial `index` whose two medians are 1000 * r and 1000. */
function groupRecord({ workload, size }, r, index) {
  const times1 = Array(30).fill(1000 * r);
  const times2 = Array(30).fill(1000);
  const m1 = median(times1);
  const m2 = median(times2);
  const mirror = isMirror(index);
  return { workload, size, trial: index, mirror, m1, m2, r: m1 / m2, order: { warmup: letters(warmupOrder(5, mirror)), timed: letters(timedOrder(30, mirror)) }, times1, times2 };
}

/**
 * A job. `ratio(group, trial, job)` gives the ratio of a group in a trial; `jobs` is
 * the number of jobs its dispatch has.
 */
function makeJob({ run = '1', job = '1', jobs = 3, processes = 20, part = '0x41/0xd0c', ratio = () => 1, env = {}, results = {} } = {}) {
  const trials = Array.from({ length: processes }, (_, i) => ({ index: i + 1, groups: GROUPS.map((g) => groupRecord(g, ratio(g, i + 1, job), i + 1)) }));
  return {
    where: `job ${run}/${job}`,
    env: {
      uname_m: 'aarch64',
      node_version: 'v24.21.0',
      cpu: { part },
      github_ref: 'refs/tags/gate-freeze-20261022',
      github_sha: SHA,
      expected_sha: SHA,
      files_sha256: FILES,
      workflow_sha256: WF,
      run_id: run,
      run_attempt: '1',
      jobs: String(jobs),
      job,
      processes,
      ...env,
    },
    results: { job, processes, finished: true, trials, ...results },
  };
}

/** The jobs of one dispatch, every ratio of every group `1 + x / 100`. */
const dispatch = (x, opts = {}) => Array.from({ length: opts.jobs ?? 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: () => 1 + x / 100, ...opts }));

test('S and its band at each edge: 2.99, 3.00, 7.00, 7.01, 10.00 and 10.01 per cent', () => {
  // dispatch(x) gives every ratio 1 + x / 100, so S_med is x too: in the 7-10 band that falls back
  const want = { 2.99: ['under-3', -0.05], 3: ['3-to-7', -0.1], 7: ['3-to-7', -0.1], 7.01: ['7-to-10', -0.15], 10: ['7-to-10', -0.15], 10.01: ['over-10', null] };
  for (const [x, [id, t3]] of Object.entries(want)) {
    const rep = summarize(dispatch(Number(x)), REG);
    assert.ok(Math.abs(rep.s - Number(x)) < 1e-9, `S for ${x} is ${rep.s}`);
    assert.ok(Math.abs(rep.s_med_pct - Number(x)) < 1e-9);
    assert.equal(rep.band.id, id, `${x} %`);
    assert.equal(rep.band.t3, t3, `${x} %`);
  }
});

test('the keep form: -0.10 up to 10 %, with the independence check, and E', () => {
  // outside the 7-10 band S_med decides nothing
  assert.deepEqual([band(2.99, 99).t3, band(3, 99).t3, band(7, 99).t3, band(10.01, 0).t3], [-0.05, -0.1, -0.1, null]);
  assert.deepEqual(band(2.99).edges, [{ t3: -0.05, e: 3 }, { t3: -0.1, e: 7 }]);
  assert.deepEqual(band(3).edges, [{ t3: -0.1, e: 7 }]);
  assert.deepEqual(band(7).edges, [{ t3: -0.1, e: 7 }]);
  assert.deepEqual(band(10.01, 0).edges, []);
  // in the 7-10 band: keep at S_med <= 5.00 %, fall back above it
  assert.equal(S_MED_LIMIT, 5);
  for (const s of [7.01, 8.5, 10]) {
    for (const [m, t3, fallback] of [[0, -0.1, false], [5.0, -0.1, false], [5.00000000002, -0.1, false], [5.01, -0.15, true], [30, -0.15, true]]) {
      const b = band(s, m);
      assert.deepEqual([b.t3, b.fallback, b.edges[0].e], [t3, fallback, 10], `S ${s}, S_med ${m}`);
    }
  }
  assert.equal(band(8, null).t3, -0.15, 'no S_med, no keep');
  // a figure that sits on an edge to within rounding is on the edge
  assert.equal(band(3.00000000002).id, '3-to-7');
  assert.equal(band(2.99999999998).id, '3-to-7');
  assert.equal(band(10.00000000002, 1).id, '7-to-10');
  assert.equal(band(7.00000000002, 1).t3, -0.1);
});

/** Every job has 8 of 20 ratios at 1 + hi / 100 and 12 at 1 + mid / 100: S is hi, the job medians are mid. */
const mixed = (hi, mid) => Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: (g, t) => (t <= 8 ? 1 + hi / 100 : 1 + mid / 100) }));

test('S_med at 5.00 and 5.01 per cent decides the 7-10 band through the whole chain', () => {
  const keep = summarize(mixed(7.01, 5.0), REG);
  assert.ok(Math.abs(keep.s - 7.01) < 1e-9 && Math.abs(keep.s_med_pct - 5.0) < 1e-9, `${keep.s} ${keep.s_med_pct}`);
  assert.deepEqual([keep.band.t3, keep.band.fallback], [-0.1, false]);
  const fall = summarize(mixed(7.01, 5.01), REG);
  assert.deepEqual([fall.band.t3, fall.band.fallback], [-0.15, true]);
  const text = render(fall);
  for (const needle of ['T3 = -0.15; E = 10 % (for T3 -0.15)', 'S_med = 5.01 %', 'fallback to -0.15: yes']) assert.ok(text.includes(needle), needle);
  assert.ok(render(keep).includes('T3 = -0.10; E = 10 %'));
  // S at 10.00 keeps with a quiet S_med, and 10.01 stops whatever S_med is
  assert.equal(summarize(mixed(10, 1), REG).band.t3, -0.1);
  assert.equal(summarize(mixed(10.01, 1), REG).band.t3, null);
});

test('S_med is the 95th percentile of the absolute job medians, over every job and group', () => {
  // twelve jobs, every group of job j at +j % (j = 1..11) and job 12 at -12 %: 108 values, each job nine times
  const ratio = (g, t, job) => (job === '12' ? 0.88 : 1 + Number(job) / 100);
  const jobs = Array.from({ length: 12 }, (_, j) => makeJob({ job: String(j + 1), jobs: 12, ratio }));
  // rank ceil(0.95 * 108) = 103 is in the last nine values, which are job 12's |ln 0.88|
  assert.ok(Math.abs(sMed(jobs) - pct(Math.abs(Math.log(0.88)))) < 1e-9, `${sMed(jobs)}`);
  // the 90th percentile would be job 11's 11 %, and the signed values would put job 12 first
  assert.ok(sMed(jobs) > 13.6);
});

/** A normal draw by Box-Muller from the generator's xorshift. */
function normals(seed) {
  const r = xorshift(seed);
  const u = () => (r.u32() + 1) / 4294967297;
  return () => Math.sqrt(-2 * Math.log(u())) * Math.cos(2 * Math.PI * u());
}

/** 12 jobs of 20 processes, ln r = offset(job) + ar(1) series with innovations sd `sigma`. */
function series12({ seed, sigma, phi = 0, offset = () => 0, trend = () => 0 }) {
  const z = normals(seed);
  const jobs = [];
  for (let j = 1; j <= 12; j++) {
    const ls = [];
    let prev = 0;
    for (let i = 0; i < 20; i++) {
      prev = phi * prev + sigma * z();
      ls.push(offset(j) + trend(i) + prev);
    }
    // the dispatch has 6 jobs: jobs 1-6 are run 1, 7-12 run 2
    const job = makeJob({ run: j <= 6 ? '1' : '2', job: String(j <= 6 ? j : j - 6), jobs: 6, ratio: (g, t) => Math.exp(ls[t - 1]) });
    jobs.push(job);
  }
  return jobs;
}

test('independent data does not fire the check, and its diagnostics are those of independence', () => {
  const jobs = series12({ seed: 11, sigma: 0.04 });
  const rep = summarize(jobs, REG);
  assert.ok(rep.s > 7 && rep.s <= 10, `S ${rep.s}`);
  assert.ok(rep.s_med_pct < 5, `S_med ${rep.s_med_pct}`);
  assert.equal(rep.band.fallback, false);
  assert.equal(rep.band.t3, -0.1);
  const ind = rep.independence;
  assert.ok(Math.abs(ind.overall.lag1 - -0.05) < 0.1, `lag-1 ${ind.overall.lag1}`);
  assert.ok(ind.overall.icc_mean < 0.1, `icc ${ind.overall.icc_mean}`);
  assert.ok(ind.overall.k_mean > 0.5 && ind.overall.k_mean < 1.6, `k ${ind.overall.k_mean}`);
  assert.equal(ind.groups.length, 9);
});

test('a per-job offset fires the check, and shows in the intraclass correlation, k and the bias', () => {
  // the jobs alternate between +6 % and -6 %: no bias overall, but job medians far from zero
  const jobs = series12({ seed: 11, sigma: 0.04, offset: (j) => (j % 2 === 0 ? 0.06 : -0.06) });
  const rep = summarize(jobs, REG);
  assert.ok(rep.s_med_pct > 5, `S_med ${rep.s_med_pct}`);
  assert.ok(rep.independence.overall.icc_mean > 0.5, `icc ${rep.independence.overall.icc_mean}`);
  assert.ok(rep.independence.overall.k_mean > 2, `k ${rep.independence.overall.k_mean}`);
  assert.ok(Math.abs(rep.independence.groups[0].slot_bias_mean_pct) < 5);
  // a constant position bias: every job the same sign
  const biased = summarize(series12({ seed: 11, sigma: 0.04, offset: () => 0.03 }), REG);
  assert.ok(biased.independence.groups.every((g) => g.slot_bias_t > 10), 'the cluster t sees it');
  assert.ok(biased.independence.groups.every((g) => g.slot_bias_mean_pct > 2.5 && g.slot_bias_median_pct > 2.5));
  assert.ok(biased.independence.overall.groups_with_abs_slot_t_over_2 === 9);
  assert.ok(biased.independence.groups.every((g) => Math.abs(g.pos_effect_mean_pct) < 0.5), 'a constant slot offset is no position effect');
  assert.ok(biased.s_med_pct > 3, `a median of 20 does not remove a bias: ${biased.s_med_pct}`);
});

test('an alternating +-b series is a position effect and no slot bias; a constant offset is the reverse; neither moves S_med', () => {
  const b = 0.03;
  // odd trials (index i + 1 odd) +b, even trials -b: whichever slot runs first gains b
  const alt = independence(series12({ seed: 3, sigma: 0.005, trend: (i) => (i % 2 === 0 ? b : -b) }));
  for (const g of alt.groups) {
    assert.ok(Math.abs(g.pos_effect_mean_pct - pct(b)) < 0.3, `position effect ${g.pos_effect_mean_pct}`);
    assert.ok(g.pos_effect_t > 10, `t ${g.pos_effect_t}`);
    assert.ok(Math.abs(g.slot_bias_mean_pct) < 0.3, `slot bias ${g.slot_bias_mean_pct}`);
  }
  assert.equal(alt.overall.groups_with_abs_pos_t_over_2, 9);
  // with the parity means removed it does not pass as correlation or as variance
  assert.ok(alt.overall.lag1 > -0.3, `lag-1 ${alt.overall.lag1}`);
  assert.ok(alt.overall.icc_mean < 0.2 && alt.overall.k_mean < 2, `icc ${alt.overall.icc_mean} k ${alt.overall.k_mean}`);
  // a constant slot offset is a slot bias and no position effect
  const slot = independence(series12({ seed: 3, sigma: 0.005, trend: () => b }));
  for (const g of slot.groups) {
    assert.ok(Math.abs(g.slot_bias_mean_pct - pct(b)) < 0.3, `slot bias ${g.slot_bias_mean_pct}`);
    assert.ok(Math.abs(g.pos_effect_mean_pct) < 0.3, `position effect ${g.pos_effect_mean_pct}`);
  }
  // S_med: the alternation cancels in the median of 20; the offset does not
  assert.ok(sMed(series12({ seed: 3, sigma: 0.005, trend: (i) => (i % 2 === 0 ? b : -b) })) < 1);
  assert.ok(sMed(series12({ seed: 3, sigma: 0.005, trend: () => b })) > 2.5);
  // independent noise shows neither
  const quiet = independence(series12({ seed: 8, sigma: 0.04 }));
  assert.ok(quiet.overall.max_abs_pos_t < 4 && quiet.overall.lag1 < 0.1 && quiet.overall.lag1 > -0.25, `${quiet.overall.max_abs_pos_t} ${quiet.overall.lag1}`);
});

test('the within-job variance is on n - 2 degrees of freedom, after the odd and even means are removed', () => {
  // in each job trial 1 is +d and trial 3 is -d, every other trial 0: the odd mean is 0, the even mean is 0,
  // so the residual sum of squares is 2 d^2 and the variance 2 d^2 / 18
  const d = 0.02;
  const jobs = series12({ seed: 1, sigma: 0, trend: (i) => (i === 0 ? d : i === 2 ? -d : 0) });
  for (const g of independence(jobs).groups) assert.ok(Math.abs(g.within_var - (2 * d * d) / 18) < 1e-12, `${g.within_var}`);
});

test('an autocorrelated series within jobs is flagged by the lag-1 autocorrelation', () => {
  const ar = independence(series12({ seed: 5, sigma: 0.03, phi: 0.8 }));
  assert.ok(ar.overall.lag1 > 0.5, `lag-1 ${ar.overall.lag1}`);
  const iid = independence(series12({ seed: 5, sigma: 0.03, phi: 0 }));
  assert.ok(iid.overall.lag1 < 0.1, `lag-1 ${iid.overall.lag1}`);
  // correlation inside a job makes the median of 20 noisier, which S_med sees
  assert.ok(sMed(series12({ seed: 5, sigma: 0.03, phi: 0.8 })) > sMed(series12({ seed: 5, sigma: 0.03, phi: 0 })));
});

test('d is the log of the ratio, so r and 1/r count alike', () => {
  const half = (g, i) => (i % 2 === 0 ? 1.05 : 1 / 1.05);
  const rep = summarize(Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: half })), REG);
  assert.ok(Math.abs(rep.s - 5) < 1e-9, `S is ${rep.s}`);
  assert.ok(Math.abs(pct(Math.log(1.05)) - 5) < 1e-9);
  // not the distance of the ratio from one: 1/1.05 is 4.76 % away
  assert.ok(Math.abs(pct(Math.abs(Math.log(1 / 1.05))) - 5) < 1e-9);
});

test('the 95th percentile is the nearest rank: ceil(0.95 n)', () => {
  const up = (n) => Array.from({ length: n }, (_, i) => i + 1);
  assert.equal(p95(up(20)), 19);
  assert.equal(p95(up(100)), 95);
  assert.equal(p95(up(21)), 20);
  assert.equal(p95(up(240)), 228);
  assert.equal(p95(up(60)), 57);
  assert.equal(p95([5, 1, 3]), 5);
  assert.equal(p95([7]), 7);
});

test('S is the largest group, not an average, and an S300-only excess is named', () => {
  // F at S300 is 12 %; every other group is 9 %
  const ratio = (g) => (g.workload === 'F' && g.size === 'S300' ? 1.12 : 1.09);
  const rep = summarize(Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio })), REG);
  assert.ok(Math.abs(rep.s - 12) < 1e-9);
  assert.equal(rep.worst_group, 'F S300');
  assert.equal(rep.band.id, 'over-10');
  assert.equal(rep.s300_only_over_10, true);
  assert.match(render(rep), /O6: the S300 groups are over 10 %/);
  // the mean of the groups is about 9.3 %, which would be the 7-10 band
  const mean = rep.groups.reduce((s, g) => s + g.s, 0) / rep.groups.length;
  assert.ok(mean < 10);
  // not S300 alone: a bigger size over 10 % as well
  const both = summarize(Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: (g) => (g.size === 'S10k' || g.size === 'S300' ? 1.12 : 1.09) })), REG);
  assert.equal(both.s300_only_over_10, false);
  assert.doesNotMatch(render(both), /O6:/);
  // S300 a hair over 10 % (rounds to 10.0000) is not over 10 %, as band() reads it
  const hair = summarize(Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: (g) => (g.size === 'S300' ? Math.exp(Math.log(1.1) + 2e-13) : 1.09) })), REG);
  assert.ok(hair.groups.find((g) => g.size === 'S300').s > 10, 'unrounded it is over');
  assert.equal(hair.s300_only_over_10, false);
  assert.equal(hair.band.id, '7-to-10');
  // S300 at 9 % and the rest at 11 %: not the O6 case either
  const other = summarize(Array.from({ length: 3 }, (_, j) => makeJob({ job: String(j + 1), ratio: (g) => (g.size === 'S300' ? 1.09 : 1.11) })), REG);
  assert.equal(other.s300_only_over_10, false);
});

function entries(part, n, d, outliers = 0, outlier = 0) {
  return Array.from({ length: n }, (_, i) => ({ part, d: i < outliers ? outlier : d }));
}

test('a CPU part is split out only with at least 60 ratios in the group', () => {
  const one = Math.log(1.01);
  const thirty = Math.log(1.3);
  // a part of 60 ratios with 4 outliers has its own figure, 30 %, which the pooled 660 do not show
  const split = groupSpread([...entries('x', 600, one), ...entries('y', 60, one, 4, thirty)]);
  assert.equal(split.split, true);
  assert.ok(Math.abs(split.s - 30) < 1e-9, `${split.s}`);
  // the same part with 59 ratios is pooled with the rest, and 4 outliers of 659 decide nothing
  const pooled = groupSpread([...entries('x', 600, one), ...entries('y', 59, one, 4, thirty)]);
  assert.equal(pooled.split, true, 'x is split out');
  assert.ok(Math.abs(pooled.s - 1) < 1e-9, `${pooled.s}`);
  // a 5-ratio part at 30 % does not decide alone
  const tiny = groupSpread([...entries('x', 100, one), ...entries('y', 5, thirty)]);
  assert.ok(Math.abs(tiny.s - 1) < 1e-9, `${tiny.s}`);
  // no part reaches 60: pooled
  const none = groupSpread([...entries('x', 50, one), ...entries('y', 50, thirty)]);
  assert.equal(none.split, false);
  assert.equal(none.parts.length, 2);
  assert.ok(Math.abs(none.s - 30) < 1e-9);
  // two parts of 60 or more: the largest of their figures
  const two = groupSpread([...entries('x', 100, Math.log(1.02)), ...entries('y', 100, Math.log(1.08))]);
  assert.ok(Math.abs(two.s - 8) < 1e-9);
  assert.deepEqual(two.parts.map((p) => p.part), ['x', 'y']);
});

test('refuses a job whose commit, FILES.sha256 hash or workflow hash differs, or that is not valid in another way', () => {
  const ok = dispatch(1);
  assert.doesNotThrow(() => summarize(ok, REG));
  const cases = {
    'another commit': (j) => (j.env.github_sha = 'd'.repeat(40)),
    'another commit named at the dispatch': (j) => (j.env.expected_sha = 'd'.repeat(40)),
    'a branch and not a freeze tag': (j) => (j.env.github_ref = 'refs/heads/main'),
    'a tag that is not a freeze tag': (j) => (j.env.github_ref = 'refs/tags/gate-freeze-2026'),
    'another FILES.sha256 hash': (j) => (j.env.files_sha256 = 'd'.repeat(64)),
    'another workflow hash': (j) => (j.env.workflow_sha256 = 'd'.repeat(64)),
    'another architecture': (j) => (j.env.uname_m = 'x86_64'),
    'another Node': (j) => (j.env.node_version = 'v24.11.0'),
    'an unfinished job': (j) => (j.results.finished = false),
    'a missing trial': (j) => j.results.trials.pop(),
    'a missing group': (j) => j.results.trials[3].groups.pop(),
    'a group out of order': (j) => j.results.trials[3].groups.reverse(),
    'another order of the blocks': (j) => (j.results.trials[3].groups[4].order.timed = j.results.trials[3].groups[4].order.timed.replace('2112', '1221')),
    'another warm-up order': (j) => (j.results.trials[0].groups[0].order.warmup = '1212121212'),
    'the registered order in an even trial': (j) => (j.results.trials[1].groups[2].order = { warmup: letters(warmupOrder(5)), timed: letters(timedOrder(30)) }),
    'the mirrored order in an odd trial': (j) => (j.results.trials[2].groups[2].order = { warmup: letters(warmupOrder(5, true)), timed: letters(timedOrder(30, true)) }),
    'a group that says it was not mirrored in an even trial': (j) => (j.results.trials[1].groups[2].mirror = false),
    'a group of another trial': (j) => (j.results.trials[1].groups[2].trial = 1),
    'a missing time': (j) => j.results.trials[3].groups[4].times1.pop(),
    'a median that is not its times': (j) => (j.results.trials[3].groups[4].m1 *= 1.5),
    'a ratio that is not its medians': (j) => (j.results.trials[3].groups[4].r *= 1.5),
  };
  for (const [what, mutate] of Object.entries(cases)) {
    const jobs = dispatch(1);
    mutate(jobs[1]);
    assert.throws(() => summarize(jobs, REG), Refusal, what);
  }
  // an argument that is not the job's
  assert.throws(() => summarize(ok, { ...REG, sha: 'e'.repeat(40) }), /commit other than the registered/);
  assert.throws(() => summarize(ok, { ...REG, files: 'e'.repeat(64) }), /FILES.sha256/);
  assert.throws(() => summarize(ok, { ...REG, wf: 'e'.repeat(64) }), /wirenoise.yml/);
  assert.doesNotThrow(() => summarize(ok, { ...REG, tag: 'gate-freeze-20261022' }));
  assert.throws(() => summarize(ok, { ...REG, tag: 'gate-freeze-20261023' }), /tag other than the registered/);
});

test('refuses a job or a run that is not the 6 x 20 design: the process count, the trial count, an odd count', () => {
  assert.doesNotThrow(() => summarize(dispatch(1), REG));
  // the dispatch ran 4 processes in each job: a smoke run is not pooled with the design
  assert.throws(() => summarize(dispatch(1, { processes: 4 }), REG), /ran 4 processes, want 20/);
  // one job with another count, its trials and its record agreeing
  const mixed = dispatch(1);
  mixed[1] = makeJob({ job: '2', processes: 10, ratio: () => 1.01 });
  assert.throws(() => summarize(mixed, REG), /ran 10 processes, want 20/);
  // a job that says 20 and holds 19 trials
  const short = dispatch(1);
  short[0].results.trials.pop();
  assert.throws(() => summarize(short, REG), /19 trials, want 20/);
  // an odd count cannot balance the parities, whatever the jobs hold
  assert.throws(() => summarize(dispatch(1, { processes: 5 }), { ...REG, processes: 5 }), /parities balance/);
  assert.throws(() => summarize(dispatch(1), { ...REG, processes: undefined }), /parities balance/);
  assert.doesNotThrow(() => summarize(dispatch(1, { processes: 4 }), { ...REG, processes: 4 }));
});

test('refuses a dispatch with a job missing or repeated, and a number of dispatches that is not the one asked for', () => {
  assert.throws(() => summarize(dispatch(1).slice(1), REG), /jobs/);
  const dup = dispatch(1);
  dup[1].env.job = '1';
  assert.throws(() => summarize(dup, REG), /distinct/);
  assert.doesNotThrow(() => summarize([...dispatch(1), ...dispatch(1, { run: '2' })], { ...REG, dispatches: 2 }));
  assert.throws(() => summarize(dispatch(1), { ...REG, dispatches: 2 }), /1 dispatches, want 2/);
});

test('two dispatches are read together, and the reported figures are there', () => {
  const rep = summarize([...dispatch(2), ...dispatch(4, { run: '2' })], { ...REG, dispatches: 2 });
  assert.equal(rep.dispatches, 2);
  assert.equal(rep.jobs, 6);
  assert.equal(rep.ratios_per_group, 120);
  assert.equal(rep.per_dispatch.length, 2);
  assert.ok(Math.abs(rep.per_dispatch[0].s - 2) < 1e-9);
  assert.ok(Math.abs(rep.per_dispatch[1].s - 4) < 1e-9);
  assert.equal(rep.per_cpu_part.length, 1);
  assert.equal(rep.per_job.length, 6);
  assert.equal(rep.cross_process.length, 9);
  // all A1 medians are equal within a job here, so no process differs from another
  for (const c of rep.cross_process) assert.equal(c.s, 0);
  // the pooled S: half the ratios at 2 %, half at 4 %: the 95th percentile is 4 %
  assert.ok(Math.abs(rep.s - 4) < 1e-9);
  const text = render(rep);
  for (const needle of ['S = 4.00 %', 'band: S from 3 % to 7 %', 'S by dispatch', 'S by CPU part', 'S by job', 'cross-process A/A']) assert.ok(text.includes(needle), needle);
});

test('the command line prints the report for matching jobs, and refuses, with nothing on the standard output, for others', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'wirenoise-sum-'));
  try {
    const put = (name, job) => {
      const d = path.join(dir, name);
      mkdirSync(d, { recursive: true });
      writeFileSync(path.join(d, 'env.json'), JSON.stringify(job.env));
      writeFileSync(path.join(d, `results-${job.env.job}.json`), JSON.stringify(job.results));
    };
    dispatch(5).forEach((j, i) => put(`a${i}`, j));
    const run = (extra = []) => spawnSync(process.execPath, [path.join(here, 'summarize.mjs'), '--dir', dir, '--sha', SHA, '--files', FILES, '--wf', WF, '--tag', 'gate-freeze-20261022', '--dispatches', '1', '--processes', '20', ...extra], { encoding: 'utf8' });
    const ok = run(['--json', path.join(dir, 'summary.json')]);
    assert.equal(ok.status, 0, ok.stderr);
    assert.match(ok.stdout, /S = 5.00 %/);
    assert.match(ok.stdout, /band: S from 3 % to 7 %/);

    const bad = spawnSync(process.execPath, [path.join(here, 'summarize.mjs'), '--dir', dir, '--sha', 'f'.repeat(40), '--files', FILES, '--wf', WF, '--tag', 'gate-freeze-20261022', '--dispatches', '1', '--processes', '20'], { encoding: 'utf8' });
    assert.equal(bad.status, 1);
    assert.equal(bad.stdout, '', 'nothing, and so no S, is printed');
    assert.match(bad.stderr, /refused: .*commit other than the registered/);

    // the tag and the number of dispatches are required
    for (const drop of [['--tag', 'gate-freeze-20261022'], ['--dispatches', '1'], ['--processes', '20']]) {
      const base = ['--dir', dir, '--sha', SHA, '--files', FILES, '--wf', WF, '--tag', 'gate-freeze-20261022', '--dispatches', '1', '--processes', '20'];
      const at = base.indexOf(drop[0]);
      const res = spawnSync(process.execPath, [path.join(here, 'summarize.mjs'), ...base.slice(0, at), ...base.slice(at + 2)], { encoding: 'utf8' });
      assert.equal(res.status, 1, `without ${drop[0]}`);
      assert.equal(res.stdout, '');
      assert.ok(res.stderr.includes(drop[0]), `the refusal names ${drop[0]}: ${res.stderr}`);
    }
    const noArgs = spawnSync(process.execPath, [path.join(here, 'summarize.mjs'), '--dir', dir], { encoding: 'utf8' });
    assert.equal(noArgs.status, 1);
    assert.equal(noArgs.stdout, '');
    assert.throws(() => readJobs([path.join(dir, 'nothing-here-at-all')]), /ENOENT/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('records made by the trial, on a fake clock, are what summarize.mjs reads', async () => {
  let now = 0n;
  let n = 0;
  const make = async () => ({
    decode: (inputs) => {
      n += 1;
      now += BigInt(1000 + (n % 7) * 13);
      return inputs.expected;
    },
  });
  const trialsOf = async () => {
    const trials = [];
    for (let i = 1; i <= 20; i++) {
      const groups = [];
      for (const g of GROUPS) groups.push(await runGroup({ ...g, inputs: { expected: 1 }, warmups: 5, iterations: 30, makeSlot: make, clock: () => now, index: i }));
      trials.push({ index: i, groups });
    }
    return trials;
  };
  const jobs = [];
  for (let j = 1; j <= 3; j++) {
    const job = makeJob({ job: String(j) });
    job.results.trials = await trialsOf();
    jobs.push(job);
  }
  const rep = summarize(jobs, REG);
  assert.equal(rep.ratios_per_group, 60);
  assert.ok(Number.isFinite(rep.s) && rep.s >= 0);
  assert.equal(rep.groups.length, 9);
});
