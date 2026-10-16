// Run with `node --allow-natives-syntax natives.probe.mjs [F|A|C]` (slots.test.mjs
// does). For each of three functions of the full read, it forces V8 to optimise the
// copy that slot 1 of a group uses and reports whether slot 2's copy is still
// unoptimised: the two slots must share no JIT state. A function that V8 does not
// keep optimised on this Node, or that both slots had already tiered up, proves
// nothing (conclusive false). Prints one JSON line.

import { loadInputs } from './split.mjs';
import { makeSlot } from './slots.mjs';

const OPTIMIZED = 16 | 32 | 64; // optimized code of any tier: TurboFan, Maglev
const TARGETS = ['fullRead', 'sumLeaf', 'step'];

const workload = process.argv[2] ?? 'F';
const inputs = loadInputs('S300');
const s1 = await makeSlot(workload, 1);
const s2 = await makeSlot(workload, 2);

// both slots decode a few times, so that each has feedback of its own
for (let i = 0; i < 20; i++) {
  s1.decode(inputs);
  s2.decode(inputs);
}

const report = {};
for (const name of TARGETS) {
  const f1 = s1.parts.fullread[name];
  const f2 = s2.parts.fullread[name];
  const b1 = %GetOptimizationStatus(f1);
  const b2 = %GetOptimizationStatus(f2);
  %PrepareFunctionForOptimization(f1);
  s1.decode(inputs);
  %OptimizeFunctionOnNextCall(f1);
  s1.decode(inputs);
  const a1 = %GetOptimizationStatus(f1);
  const a2 = %GetOptimizationStatus(f2);
  report[name] = {
    same_function: f1 === f2,
    status: { before: [b1, b2], after: [a1, a2] },
    // conclusive: V8 optimised slot 1's copy because it was told to, and slot 2's was not optimised before
    conclusive: (b1 & OPTIMIZED) === 0 && (b2 & OPTIMIZED) === 0 && (a1 & OPTIMIZED) !== 0,
    slot2_optimized: (a2 & OPTIMIZED) !== 0,
  };
}
console.log(JSON.stringify(report));
