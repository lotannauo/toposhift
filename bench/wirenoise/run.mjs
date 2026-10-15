// One job: runs `processes` trials one after another, each a fresh node process, and
// writes the record of the machine (env.json) and the trials (results-<job>.json)
// into the output directory.
//
//   node run.mjs --job 3 --processes 20 --out DIR
//   node run.mjs --job 0 --processes 1 --out DIR --dry     the trial's dry form: no time

import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import { collectEnv, trialEnv } from './env.mjs';
import { GROUPS } from './trial.mjs';

const here = import.meta.dirname;

function parseArgs(argv) {
  const o = { job: null, processes: null, out: null, dry: false };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === '--job') o.job = argv[++i];
    else if (argv[i] === '--processes') o.processes = Number(argv[++i]);
    else if (argv[i] === '--out') o.out = argv[++i];
    else if (argv[i] === '--dry') o.dry = true;
    else throw new Error(`run: unknown argument ${argv[i]}`);
  }
  if (!/^[0-9]+$/.test(o.job ?? '')) throw new Error('run: --job is a number');
  if (!Number.isInteger(o.processes) || o.processes < 1 || o.processes > 20) throw new Error('run: --processes is 1 to 20');
  if (!o.out) throw new Error('run: --out is a directory');
  return o;
}

function main() {
  const o = parseArgs(process.argv.slice(2));
  mkdirSync(o.out, { recursive: true });
  const env = { ...collectEnv(), job: o.job, processes: o.processes };
  writeFileSync(path.join(o.out, 'env.json'), `${JSON.stringify(env, null, 2)}\n`);
  const trials = [];
  for (let i = 1; i <= o.processes; i++) {
    // the trial's index sets its position order (odd: registered, even: mirrored)
    const args = [path.join(here, 'trial.mjs'), '--index', String(i), ...(o.dry ? ['--dry'] : [])];
    const res = spawnSync(process.execPath, args, { encoding: 'utf8', maxBuffer: 1 << 28, env: trialEnv() });
    if (res.status !== 0) {
      process.stderr.write(res.stderr);
      throw new Error(`run: trial ${i} exited with ${res.status ?? res.signal}`);
    }
    const groups = res.stdout.split('\n').filter((l) => l !== '').map((l) => JSON.parse(l));
    // every trial writes all the groups, in the fixed order, and a dry group says it is done
    if (groups.length !== GROUPS.length) throw new Error(`run: trial ${i} wrote ${groups.length} groups, want ${GROUPS.length}`);
    groups.forEach((g, k) => {
      if (g.workload !== GROUPS[k].workload || g.size !== GROUPS[k].size) throw new Error(`run: trial ${i}: group ${k} is ${g.workload} ${g.size}`);
      if (o.dry && g.done !== true) throw new Error(`run: trial ${i}: ${g.workload} ${g.size} did not complete`);
    });
    trials.push({ index: i, groups });
  }
  const results = { schema: 1, job: o.job, processes: o.processes, cpu_part: env.cpu.part, finished: true, trials };
  writeFileSync(path.join(o.out, `results-${o.job}.json`), `${JSON.stringify(results)}\n`);
  console.log(`run: job ${o.job}: ${trials.length} trials written`);
}

main();
