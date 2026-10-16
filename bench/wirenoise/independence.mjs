// Within-job independence of the per-process ratios, from the measurement's own
// data. The decision rule for the gate's margin treats the median of 20 per-process
// ratios of one job as an average of independent values; these figures test that.
//
// Everything is computed from l = ln r, signed, of the processes of each job in
// trial order, for each group; odd and even trials run mirrored orders. Only S_med
// is ever decisive (README, "The check on independence"); every other figure is
// reported and decides nothing.

import { GROUPS, median } from './trial.mjs';

const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
const sd = (xs) => {
  const m = mean(xs);
  return Math.sqrt(xs.reduce((a, x) => a + (x - m) ** 2, 0) / (xs.length - 1));
};
const pct = (x) => (Math.exp(x) - 1) * 100;
/** The 95th percentile by nearest rank, rank ceil(0.95 n) in integers. */
const p95 = (xs) => [...xs].sort((a, b) => a - b)[Math.floor((95 * xs.length + 99) / 100) - 1];

/** The signed ln r of a group in each trial of a job, in trial order, with the trials' indices. */
export function series(job, workload, size) {
  const trials = [...job.results.trials].sort((a, b) => a.index - b.index);
  return {
    ls: trials.map((t) => Math.log(t.groups.find((g) => g.workload === workload && g.size === size).r)),
    index: trials.map((t) => t.index),
  };
}

/**
 * S_med: for each job and group the median of its per-process ln r; then the 95th
 * percentile, nearest rank, of the absolute values over every job and group, as a
 * per cent. It is the gate's own statistic in A/A form: the spread of a median of
 * the per-process ratios of one job.
 */
export function sMed(jobs) {
  const values = [];
  for (const { workload, size } of GROUPS) for (const job of jobs) values.push(Math.abs(median(series(job, workload, size).ls)));
  return pct(p95(values));
}

/**
 * The figures of one group over the jobs given (balanced: every job has the same,
 * even, number of processes). Percentages are per cent of a ratio, (e^x - 1) * 100.
 *
 * With parity mirroring r is always slot 1 over slot 2 but the slot that runs first
 * alternates, so a mean of ln r measures the slot, not the position:
 *  slot_bias_*     slot identity: the pooled median and mean of ln r, and the mean of the
 *                  job means over its standard error across jobs (t); about 0, |t| < 2
 *  pos_effect_*    position: per job (mean of ln r over odd trials - mean over even
 *                  trials) / 2, its mean over jobs, and t over jobs; about 0, |t| < 2
 * The rest is computed on the series with each job's odd and even means removed
 * separately (and the job's mean put back), so a position effect does not pass as
 * correlation or variance:
 *  lag1            lag-1 autocorrelation within jobs, about -1/n under independence
 *  within_var, between_var, icc   one-way random-effects split by job; icc about 0
 *  k               sd of the job medians over 1.2533 sd_within / sqrt(n); about 1
 */
export function groupFigures(jobs, workload, size) {
  const sers = jobs.map((job) => series(job, workload, size));
  const J = sers.length;
  const n = sers[0].ls.length;
  if (n % 2 !== 0 || n < 4) throw new Error(`independence: ${n} processes in a job, want an even number of at least 4`);
  const raw = sers.map((x) => x.ls);
  const means = raw.map(mean);
  const pos = sers.map(({ ls, index }) => {
    const odd = ls.filter((_, i) => index[i] % 2 === 1);
    const even = ls.filter((_, i) => index[i] % 2 === 0);
    return (mean(odd) - mean(even)) / 2;
  });
  // the series with the parity means removed and the job mean put back
  const adj = sers.map(({ ls, index }, j) => {
    const pm = [0, 1].map((par) => mean(ls.filter((_, i) => index[i] % 2 === par)));
    return ls.map((l, i) => l - pm[index[i] % 2] + means[j]);
  });
  const all = raw.flat();
  let ssw = 0;
  let num = 0;
  for (let j = 0; j < J; j++) {
    for (let i = 0; i < n; i++) {
      ssw += (adj[j][i] - means[j]) ** 2;
      if (i + 1 < n) num += (adj[j][i] - means[j]) * (adj[j][i + 1] - means[j]);
    }
  }
  const tOf = (xs) => {
    const s = sd(xs);
    return s > 0 ? mean(xs) / (s / Math.sqrt(xs.length)) : null;
  };
  const out = {
    workload,
    size,
    jobs: J,
    slot_bias_median_pct: pct(median(all)),
    slot_bias_mean_pct: pct(mean(all)),
    slot_bias_t: null,
    pos_effect_mean_pct: pct(mean(pos)),
    pos_effect_t: null,
    lag1_num: num,
    lag1_den: ssw,
    lag1: ssw > 0 ? num / ssw : null,
    // n - 2 degrees of freedom a job: two parity means are removed
    within_var: ssw / (J * (n - 2)),
    between_var: null,
    icc: null,
    k: null,
  };
  if (J >= 2) {
    out.slot_bias_t = tOf(means);
    out.pos_effect_t = tOf(pos);
    const msw = out.within_var;
    const msb = (n * means.reduce((a, m) => a + (m - mean(means)) ** 2, 0)) / (J - 1);
    out.between_var = Math.max(0, (msb - msw) / n);
    out.icc = out.between_var + msw > 0 ? out.between_var / (out.between_var + msw) : null;
    out.k = msw > 0 ? sd(adj.map(median)) / ((1.2533 * Math.sqrt(msw)) / Math.sqrt(n)) : null;
  }
  return out;
}

/** The figures of every group, and of all together. */
export function independence(jobs) {
  const groups = GROUPS.map(({ workload, size }) => groupFigures(jobs, workload, size));
  const num = groups.reduce((a, g) => a + g.lag1_num, 0);
  const den = groups.reduce((a, g) => a + g.lag1_den, 0);
  const some = (f) => groups.map(f).filter((x) => x !== null && Number.isFinite(x));
  const icc = some((g) => g.icc);
  const k = some((g) => g.k);
  const t = some((g) => g.slot_bias_t);
  const pt = some((g) => g.pos_effect_t);
  const clean = groups.map(({ lag1_num, lag1_den, ...g }) => g);
  return {
    s_med_pct: sMed(jobs),
    groups: clean,
    overall: {
      lag1: den > 0 ? num / den : null,
      icc_mean: icc.length ? mean(icc) : null,
      k_mean: k.length ? mean(k) : null,
      max_abs_slot_t: t.length ? Math.max(...t.map(Math.abs)) : null,
      groups_with_abs_slot_t_over_2: t.filter((x) => Math.abs(x) > 2).length,
      max_abs_pos_t: pt.length ? Math.max(...pt.map(Math.abs)) : null,
      groups_with_abs_pos_t_over_2: pt.filter((x) => Math.abs(x) > 2).length,
    },
  };
}
