import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import { parseCpuInfo, trialEnv } from './env.mjs';
import { GROUPS, dryLine, isMirror, median, runGroup, runTrial, timedOrder, warmupOrder } from './trial.mjs';

const here = import.meta.dirname;

// The registered order, written out here: five warm-ups per slot in the order
// A1 A2 A2 A1 A2 A1 A1 A2 A1 A2, then fifteen blocks of four that alternate ABBA and
// BAAB, starting with ABBA (8 of ABBA and 7 of BAAB).
const WARMUP = '1221211212';
const TIMED = '12212112'.repeat(7) + '1221';
// An even trial runs the mirror image: the numbers 1 and 2 swapped in both sequences.
const swap = (s) => s.replace(/[12]/g, (c) => (c === '1' ? '2' : '1'));
const MIRROR_WARMUP = swap(WARMUP);
const MIRROR_TIMED = swap(TIMED);

/** Two fake slots on a fake clock: a call of a slot takes the time the plan gives it. */
function fakeWorld(plan, answer) {
  const state = { now: 0n, calls: [], reads: 0 };
  // makeSlot is called with the slot's number, 1 or 2; a call is recorded as 0 or 1
  const made = [];
  const make = async (_workload, number) => ({
    get made() {
      return made;
    },
    decode(inputs) {
      const which = number - 1;
      state.calls.push(which);
      state.now += BigInt(plan(which, state.calls.length));
      return answer ?? inputs.expected;
    },
  });
  const clock = () => {
    state.reads++;
    return state.now;
  };
  const creating = async (workload, number) => {
    made.push(number);
    return make(workload, number);
  };
  return { state, make: creating, made, clock };
}

test('the warm-ups and the timed blocks run in the registered order, and nothing else', async () => {
  const w = fakeWorld(() => 1);
  const rec = await runGroup({ workload: 'F', size: 'S300', inputs: { expected: 7 }, warmups: 5, iterations: 30, makeSlot: w.make, clock: w.clock });
  const executed = w.state.calls.map((x) => x + 1).join('');
  assert.equal(executed, WARMUP + TIMED);
  assert.equal(rec.order.warmup, WARMUP);
  assert.equal(rec.order.timed, TIMED);
  assert.equal(TIMED.length, 60);
  assert.equal(rec.times1.length, 30);
  assert.equal(rec.times2.length, 30);
  const blocks = TIMED.match(/..../g);
  assert.equal(blocks.length, 15);
  assert.equal(blocks.filter((b) => b === '1221').length, 8);
  assert.equal(blocks.filter((b) => b === '2112').length, 7);
  blocks.forEach((b, i) => assert.equal(b, i % 2 === 0 ? '1221' : '2112', 'the blocks alternate, starting with 1221'));
  assert.equal(w.state.reads, 2 * w.state.calls.length, 'the clock is read twice around each call and no more');
});

test('an even trial runs the mirror image: slot 2 made first, both sequences with 1 and 2 swapped', async () => {
  assert.equal(MIRROR_WARMUP, '2112122121');
  assert.deepEqual([isMirror(1), isMirror(2), isMirror(3), isMirror(20)], [false, true, false, true]);
  for (const index of [1, 2, 3, 4]) {
    const w = fakeWorld(() => 1);
    const rec = await runGroup({ workload: 'F', size: 'S300', inputs: { expected: 7 }, warmups: 5, iterations: 30, index, makeSlot: w.make, clock: w.clock });
    const mirror = index % 2 === 0;
    assert.deepEqual(w.made, mirror ? [2, 1] : [1, 2], `trial ${index}: creation order`);
    assert.equal(w.state.calls.map((x) => x + 1).join(''), mirror ? MIRROR_WARMUP + MIRROR_TIMED : WARMUP + TIMED, `trial ${index}: executed order`);
    assert.equal(rec.order.warmup, mirror ? MIRROR_WARMUP : WARMUP);
    assert.equal(rec.order.timed, mirror ? MIRROR_TIMED : TIMED);
    assert.equal(rec.mirror, mirror);
    assert.equal(rec.trial, index);
    assert.equal(rec.times1.length, 30);
    assert.equal(rec.times2.length, 30);
  }
  // r stays slot 1 over slot 2 in a mirrored trial
  const w = fakeWorld((which) => (which === 0 ? 100 : 200));
  const rec = await runGroup({ workload: 'A', size: 'S2k', inputs: { expected: 7 }, warmups: 5, iterations: 30, index: 2, makeSlot: w.make, clock: w.clock });
  assert.equal(rec.r, 0.5);
  // over a pair of trials each slot is first and last as often
  assert.equal(warmupOrder(5, true).map((x) => x + 1).join(''), MIRROR_WARMUP);
  assert.equal(timedOrder(30, true).map((x) => x + 1).join(''), MIRROR_TIMED);
});

test('warm-ups are not in the medians, and r is the ratio of the medians', async () => {
  // a warm-up takes a second; a timed call takes 100 ns in slot 1 and 200 ns in slot 2
  const w = fakeWorld((which, n) => (n <= 10 ? 1e9 : which === 0 ? 100 : 200));
  const rec = await runGroup({ workload: 'A', size: 'S2k', inputs: { expected: 7 }, warmups: 5, iterations: 30, makeSlot: w.make, clock: w.clock });
  assert.equal(rec.m1, 100);
  assert.equal(rec.m2, 200);
  assert.equal(rec.r, 0.5);
  assert.ok(rec.times1.every((t) => t === 100));
});

test('the comparison with the expected checksum happens after the clock has stopped', async () => {
  // reading the expected checksum costs 5000 ns on the fake clock; a timed call takes 100 or 200
  const w = fakeWorld((which) => (which === 0 ? 100 : 200), 7);
  const inputs = {
    get expected() {
      w.state.now += 5000n;
      return 7;
    },
  };
  const rec = await runGroup({ workload: 'F', size: 'S300', inputs, warmups: 1, iterations: 2, makeSlot: w.make, clock: w.clock });
  assert.equal(rec.m1, 100);
  assert.equal(rec.m2, 200);
});

test('a group fails when a slot reads another checksum than the generator\'s', async () => {
  const make = async () => ({ decode: () => 8 });
  await assert.rejects(runGroup({ workload: 'C', size: 'S300', inputs: { expected: 7 }, warmups: 1, iterations: 2, makeSlot: make, clock: () => 0n }), /read 8, want 7/);
});

test('the order of fewer warm-ups is a prefix of the registered one; iterations must be even', () => {
  for (let n = 1; n <= 5; n++) assert.equal(warmupOrder(n).map((x) => x + 1).join(''), WARMUP.slice(0, 2 * n));
  assert.throws(() => warmupOrder(0), /warm-ups/);
  assert.throws(() => warmupOrder(6), /warm-ups/);
  assert.equal(timedOrder(30).map((x) => x + 1).join(''), TIMED);
  assert.equal(timedOrder(4).map((x) => x + 1).join(''), '12212112');
  assert.throws(() => timedOrder(3), /even/);
  // every slot has as many calls as iterations, in every even count
  for (const n of [2, 4, 30]) assert.equal(timedOrder(n).filter((x) => x === 0).length, n);
});

test('the median is the middle value, or the mean of the middle two', () => {
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([4, 1, 3, 2]), 2.5);
  assert.equal(median([5]), 5);
});

test('a dry line has no time in it, and the dry output of a whole trial is nine such lines', () => {
  const rec = { workload: 'F', size: 'S300', m1: 1, m2: 2, r: 0.5, order: { warmup: '12', timed: '1221' }, times1: [1, 2], times2: [3, 4] };
  const line = dryLine(rec);
  assert.deepEqual(Object.keys(line).sort(), ['done', 'mirror', 'order', 'size', 'timed1', 'timed2', 'trial', 'workload']);
  assert.ok(!JSON.stringify(line).includes('times'));

  const res = spawnSync(process.execPath, [path.join(here, 'trial.mjs'), '--dry'], { encoding: 'utf8' });
  assert.equal(res.status, 0, res.stderr);
  const lines = res.stdout.trim().split('\n').map((l) => JSON.parse(l));
  assert.equal(lines.length, 9);
  lines.forEach((l, i) => {
    assert.deepEqual(Object.keys(l), Object.keys(line));
    assert.equal(l.workload, GROUPS[i].workload);
    assert.equal(l.size, GROUPS[i].size);
    assert.equal(l.done, true);
    assert.equal(l.timed1, 4);
    assert.equal(l.order.warmup, '12');
    assert.equal(l.order.timed, '12212112');
    assert.equal(l.trial, 1);
    assert.equal(l.mirror, false);
  });
  const even = spawnSync(process.execPath, [path.join(here, 'trial.mjs'), '--dry', '--index', '2'], { encoding: 'utf8' });
  assert.equal(even.status, 0, even.stderr);
  for (const l of even.stdout.trim().split('\n').map((x) => JSON.parse(x))) {
    assert.deepEqual([l.trial, l.mirror, l.order.warmup, l.order.timed], [2, true, '21', '21121221']);
  }
});

test('a trial loads every input before the first group runs, and runs the nine groups in the fixed order', async () => {
  const log = [];
  const w = fakeWorld(() => 1);
  const make = async (workload, which) => {
    log.push(`make ${workload}${which}`);
    return w.make(workload, which);
  };
  const emitted = [];
  await runTrial({
    warmups: 1,
    iterations: 2,
    emit: (r) => emitted.push(`${r.workload} ${r.size}`),
    inputsFor: (size) => {
      log.push(`load ${size}`);
      return { expected: 1 };
    },
    make,
  });
  const firstMake = log.findIndex((x) => x.startsWith('make'));
  assert.deepEqual(log.slice(0, firstMake), ['load S300', 'load S2k', 'load S10k']);
  assert.deepEqual(emitted, ['F S300', 'F S2k', 'F S10k', 'A S300', 'A S2k', 'A S10k', 'C S300', 'C S2k', 'C S10k']);
  assert.equal(log.filter((x) => x.startsWith('make')).length, 18, 'each group makes its own two slots');
});

test('a job writes the machine and the trials, and a dry job has no time in either', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'wirenoise-run-'));
  try {
    const res = spawnSync(process.execPath, [path.join(here, 'run.mjs'), '--job', '0', '--processes', '2', '--out', dir, '--dry'], {
      encoding: 'utf8',
      env: { ...process.env, GITHUB_SHA: 'a'.repeat(40), EXPECTED_SHA: 'a'.repeat(40), JOBS: '1', NODE_OPTIONS: '--no-warnings', NODE_COMPILE_CACHE: '/nonexistent-cache' },
    });
    assert.equal(res.status, 0, res.stderr);
    const env = JSON.parse(readFileSync(path.join(dir, 'env.json'), 'utf8'));
    for (const k of ['uname_m', 'nproc', 'cpu', 'lscpu_model', 'image_os', 'image_version', 'image_node', 'node_version', 'v8_version', 'github_ref', 'github_sha', 'expected_sha', 'files_sha256', 'workflow_sha256', 'run_id', 'jobs', 'started_at', 'job', 'processes']) {
      assert.ok(k in env, `env.json has ${k}`);
    }
    assert.equal(env.node_version, process.version);
    // the trials run without NODE_OPTIONS and NODE_COMPILE_CACHE, and the record says so
    assert.equal(env.node_options, '');
    assert.equal(env.node_compile_cache, '');
    assert.equal(env.parent_node_options, '--no-warnings');
    assert.equal(env.parent_node_compile_cache, '/nonexistent-cache');
    assert.equal(env.github_sha, 'a'.repeat(40));
    assert.match(env.files_sha256, /^[0-9a-f]{64}$/);
    const text = readFileSync(path.join(dir, 'results-0.json'), 'utf8');
    const results = JSON.parse(text);
    assert.equal(results.finished, true);
    assert.equal(results.trials.length, 2);
    assert.equal(results.trials[0].groups.length, 9);
    assert.deepEqual(results.trials.map((t) => t.groups[0].mirror), [false, true], 'the trial index reaches the trial');
    assert.deepEqual(results.trials.map((t) => t.groups[0].trial), [1, 2]);
    for (const key of ['"times1"', '"times2"', '"m1"', '"m2"', '"r"']) assert.ok(!text.includes(key), `a dry job has no ${key}`);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('the trials a job starts do not load what NODE_OPTIONS names', () => {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'wirenoise-opts-'));
  try {
    // a hook that notes which script it was loaded into
    const hook = path.join(dir, 'hook.cjs');
    const marks = path.join(dir, 'marks.txt');
    writeFileSync(hook, `require('node:fs').appendFileSync(${JSON.stringify(marks)}, require('node:path').basename(process.argv[1] ?? '') + '\\n');`);
    const res = spawnSync(process.execPath, [path.join(here, 'run.mjs'), '--job', '0', '--processes', '1', '--out', path.join(dir, 'out'), '--dry'], {
      encoding: 'utf8',
      env: { ...process.env, NODE_OPTIONS: `--require ${hook}`, JOBS: '1' },
    });
    assert.equal(res.status, 0, res.stderr);
    const loaded = readFileSync(marks, 'utf8').trim().split('\n');
    assert.deepEqual(loaded, ['run.mjs'], 'the hook ran in the job and in no trial');
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('a trial\'s environment has no NODE_OPTIONS and no NODE_COMPILE_CACHE, and keeps the rest', () => {
  const env = trialEnv({ PATH: '/bin', NODE_OPTIONS: '--x', NODE_COMPILE_CACHE: '/c', HOME: '/h' });
  assert.deepEqual(env, { PATH: '/bin', HOME: '/h' });
});

test('the CPU part is read from the implementer and part lines of /proc/cpuinfo', () => {
  const one = 'processor\t: 0\nBogoMIPS\t: 50.00\nCPU implementer\t: 0x41\nCPU architecture: 8\nCPU variant\t: 0x3\nCPU part\t: 0xd0c\nCPU revision\t: 1\n\nprocessor\t: 1\nCPU implementer\t: 0x41\nCPU part\t: 0xd0c\n';
  assert.deepEqual(parseCpuInfo(one), { part: '0x41/0xd0c', distinct: ['0x41/0xd0c'] });
  assert.equal(parseCpuInfo('model name : x\n').part, 'unknown');
  const mixed = `${one}processor\t: 2\nCPU implementer\t: 0x41\nCPU part\t: 0xd40\n`;
  assert.equal(parseCpuInfo(mixed).part, '0x41/0xd0c+0x41/0xd40');
});
