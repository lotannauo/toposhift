import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import test from 'node:test';

import { SECTION_NAMES, SIZES } from './gen.mjs';
import { makeSlot } from './slots.mjs';
import { loadInputs } from './split.mjs';

const require = createRequire(import.meta.url);
const arrowFile = path.join(path.dirname(require.resolve('apache-arrow')), 'Arrow.esnext.min.js');
const here = import.meta.dirname;

test('F, A and C each read every size to the checksum the generator computed from the values', async () => {
  for (const size of Object.keys(SIZES)) {
    const inputs = loadInputs(size);
    assert.ok(inputs.expected > 0);
    for (const workload of ['F', 'A', 'C']) {
      for (const which of [1, 2]) {
        const slot = await makeSlot(workload, which);
        // the checksum is not handed to the slot: it is read from the bytes
        assert.equal(slot.decode({ views: inputs.views, tsgf: inputs.tsgf }), inputs.expected, `${workload} ${size} slot ${which}`);
      }
    }
  }
});

test('each section alone reads to its own checksum in all three, so one wrong section cannot hide in a sum', async () => {
  const inputs = loadInputs('S300');
  const f = await makeSlot('F', 1);
  const a = await makeSlot('A', 1);
  const c = await makeSlot('C', 1);
  const flechette = f.parts.lib;
  const arrow = a.parts.lib;
  const standIn = c.parts.reader.decode(inputs.tsgf);
  SECTION_NAMES.forEach((name, i) => {
    const want = inputs.sectionChecksums[i];
    assert.equal(f.parts.fullread.fullReadOne(f.parts.adapter, flechette.tableFromIPC(inputs.views[i])), want, `${name} (Flechette)`);
    assert.equal(a.parts.fullread.fullReadOne(a.parts.adapter, arrow.tableFromIPC(inputs.views[i])), want, `${name} (apache-arrow)`);
    assert.equal(c.parts.fullread.fullReadOne(c.parts.adapter, standIn[i]), want, `${name} (stand-in)`);
  });
  assert.equal(new Set(inputs.sectionChecksums).size, 12, 'the twelve sections do not read alike');
});

test('a checksum changes when a value does', async () => {
  const inputs = loadInputs('S300');
  const c = await makeSlot('C', 1);
  assert.equal(c.decode(inputs), inputs.expected);
  // flip one bit inside the first fingerprint of the entities section
  const entities = sectionStarts(inputs.tsgf)[1];
  const bytes = inputs.tsgf.slice();
  bytes[entities.payloadAt + 1 + 5] ^= 0x01;
  assert.notEqual(c.decode({ ...inputs, tsgf: bytes }), inputs.expected);
});

/** Where each section's tag, length and payload start in a .tsgf buffer. */
function sectionStarts(buf) {
  const out = [];
  let pos = 0;
  while (pos < buf.length) {
    const tagAt = pos;
    const lenAt = pos + 1;
    let n = 0;
    let mul = 1;
    let at = lenAt;
    for (;;) {
      const b = buf[at++];
      n += (b & 0x7f) * mul;
      mul *= 128;
      if (b < 0x80) break;
    }
    out.push({ tagAt, lenAt, payloadAt: at, len: n });
    pos = at + n;
  }
  return out;
}

test('the stand-in refuses a truncated buffer, an overlong uvarint, a section out of order and trailing bytes', async () => {
  const inputs = loadInputs('S300');
  for (const which of [1, 2]) {
    const { reader } = await makeSlot('C', which).then((s) => s.parts);
    assert.equal(reader.decode(inputs.tsgf).length, 12);
    for (const cut of [0, 1, 2, 100, inputs.tsgf.length >> 1, inputs.tsgf.length - 1]) {
      assert.throws(() => reader.decode(inputs.tsgf.subarray(0, cut)), /stand-in/, `cut at ${cut}`);
    }
    assert.throws(() => reader.decode(Uint8Array.of(...inputs.tsgf, 0)), /trailing/);
    // the first section's tag, then a uvarint of nine bytes that all continue
    assert.throws(() => reader.decode(Uint8Array.of(1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)), /overlong uvarint/);
    // the same length in eight bytes is not overlong, and then the section is truncated
    assert.throws(() => reader.decode(Uint8Array.of(1, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01)), /truncated/);
    // a section that says it is longer than the buffer
    const where2 = sectionStarts(inputs.tsgf);
    const last2 = where2[where2.length - 1];
    const inflated = inputs.tsgf.slice();
    inflated[last2.lenAt] += 50;
    assert.throws(() => reader.decode(inflated), /^Error: stand-in: truncated section$/, 'a length past the buffer');
    // a field that runs past the end of its section: the entities section one byte short of its last fingerprint
    const ent = where2[1];
    const short = inputs.tsgf.slice();
    assert.ok(short[ent.lenAt + 1] < 0x80, 'the length is two bytes');
    short[ent.lenAt] -= 2;
    assert.throws(() => reader.decode(short), /a field runs past the end of its section/);
    const wrongTag = inputs.tsgf.slice();
    wrongTag[0] = 2;
    assert.throws(() => reader.decode(wrongTag), /has the tag 2/);
    // a payload longer than what its section reads: one byte more in the last section
    const where = sectionStarts(inputs.tsgf);
    const last = where[where.length - 1];
    const longer = Uint8Array.of(...inputs.tsgf, 0);
    assert.ok(inputs.tsgf[last.lenAt] < 127, 'the last section\'s length is one byte');
    longer[last.lenAt] += 1;
    assert.throws(() => reader.decode(longer), /bytes it does not read/);
  }
});

test('the two slots of a group share no function, no library instance and no reader', async () => {
  for (const workload of ['F', 'A', 'C']) {
    const s1 = await makeSlot(workload, 1);
    const s2 = await makeSlot(workload, 2);
    assert.notEqual(s1.decode, s2.decode, `${workload}: decode`);
    assert.notEqual(s1, s2);
    assert.notEqual(s1.parts.fullread, s2.parts.fullread, `${workload}: the full read`);
    assert.notEqual(s1.parts.fullread.fullRead, s2.parts.fullread.fullRead, `${workload}: fullRead`);
    assert.notEqual(s1.parts.fullread.sumLeaf, s2.parts.fullread.sumLeaf, `${workload}: sumLeaf`);
    assert.notEqual(s1.parts.adapter, s2.parts.adapter, `${workload}: adapter`);
    if (workload === 'C') {
      assert.notEqual(s1.parts.reader, s2.parts.reader);
      assert.notEqual(s1.parts.reader.decode, s2.parts.reader.decode, 'C: the readers');
    } else {
      assert.notEqual(s1.parts.lib, s2.parts.lib, `${workload}: the library`);
      assert.notEqual(s1.parts.lib.tableFromIPC, s2.parts.lib.tableFromIPC, `${workload}: tableFromIPC`);
    }
  }
});

test('the same slot of two groups is not the same instance either', async () => {
  for (const workload of ['F', 'A', 'C']) {
    const x = await makeSlot(workload, 1);
    const y = await makeSlot(workload, 1);
    assert.notEqual(x.decode, y.decode);
    assert.notEqual(x.parts.fullread.fullRead, y.parts.fullread.fullRead);
  }
});

test('the two copies of the stand-in reader differ in their first line only', () => {
  const a = readFileSync(path.join(here, 'stand_in_a.mjs'), 'utf8').split('\n');
  const b = readFileSync(path.join(here, 'stand_in_b.mjs'), 'utf8').split('\n');
  assert.notEqual(a[0], b[0]);
  assert.match(a[0], /copy A/);
  assert.match(b[0], /copy B/);
  assert.deepEqual(a.slice(1), b.slice(1));
  assert.ok(!/^import\b/m.test(a.join('\n')), 'a reader imports nothing, so the copies share no function');
});

test('the libraries are the pinned build files, and a cleared require cache leaves no copy behind', async () => {
  const f = await makeSlot('F', 1);
  const a = await makeSlot('A', 1);
  const flechetteFile = require.resolve('@uwdata/flechette');
  assert.ok(flechetteFile.endsWith(path.join('dist', 'flechette.cjs')));
  assert.equal(require.cache[flechetteFile], undefined);
  assert.equal(require.cache[arrowFile], undefined);
  assert.equal(typeof f.parts.lib.tableFromIPC, 'function');
  assert.equal(typeof a.parts.lib.tableFromIPC, 'function');
  // the single-file build is self-contained under require
  const text = readFileSync(arrowFile, 'utf8');
  assert.equal(/\brequire\s*\(/.test(text), false);
});

// A table small enough to read by hand: bool, int64 above 2^32, a nullable uint8, a
// fixed_size_binary, a list and a string, written once and read by both libraries.
function handTable() {
  const A = require(arrowFile);
  const cols = {
    bo: A.vectorFromArray([true, false, true], new A.Bool()),
    i64: A.vectorFromArray([1n, 2n, 4294967297n], new A.Int64()),
    u8n: A.vectorFromArray([1, null, 3], new A.Uint8()),
    fsb: A.vectorFromArray([Uint8Array.of(1, 2), Uint8Array.of(3, 4), Uint8Array.of(5, 6)], new A.FixedSizeBinary(2)),
    lst: A.vectorFromArray([[1], [2, 3], []], new A.List(new A.Field('item', new A.Uint32(), false))),
    str: A.vectorFromArray(['a', 'bc', ''], new A.Utf8()),
  };
  return A.tableToIPC(new A.Table(cols), 'stream');
}

// 2166136261 folded with, in column order: bool 2; int64 1+2+1 = 4; uint8 1+3 = 4; fixed
// bytes 1+2+3+4+5+6 = 21; the list 3 then its items 1+2+3 = 6; the string 97+98+99 = 294;
// and one null.
function handChecksum() {
  const step = (h, c) => Math.imul(h ^ c, 16777619) >>> 0;
  let h = 2166136261;
  for (const c of [2, 4, 4, 21, 3, 6, 294]) h = step(h, c);
  return (h + 1) >>> 0;
}

test('the checksum counts bools, int64 (BigInt or number), bytes, strings, list lengths and nulls as the README says', async () => {
  const bytes = handTable();
  const f = await makeSlot('F', 1);
  const a = await makeSlot('A', 1);
  const want = handChecksum();
  assert.equal(f.parts.fullread.fullRead(f.parts.adapter, [f.parts.lib.tableFromIPC(bytes)]), want, 'Flechette (int64 as number)');
  assert.equal(a.parts.fullread.fullRead(a.parts.adapter, [a.parts.lib.tableFromIPC(bytes)]), want, 'apache-arrow (int64 as BigInt)');
  // and Flechette asked for BigInt gives the same
  const big = f.parts.lib.tableFromIPC(bytes, { useBigInt: true });
  assert.equal(f.parts.fullread.fullRead(f.parts.adapter, [big]), want, 'Flechette (int64 as BigInt)');
});

test('the Flechette slot reads int64 as BigInt (useBigInt: true), the option real timestamps need', async () => {
  const f = await makeSlot('F', 1);
  const seen = [];
  const original = f.parts.lib.tableFromIPC;
  f.parts.lib.tableFromIPC = (view, options) => {
    seen.push(options);
    return original(view, options);
  };
  const inputs = loadInputs('S300');
  assert.equal(f.decode(inputs), inputs.expected);
  f.parts.lib.tableFromIPC = original;
  assert.equal(seen.length, 12);
  for (const o of seen) assert.deepEqual(o, { useBigInt: true });
  // and so an int64 column is a BigInt64Array, as apache-arrow's is
  const header = original(inputs.views[0], { useBigInt: true });
  assert.ok(header.getChild('from').toArray() instanceof BigInt64Array);
  // the default would hand numbers, and refuse a value above 2^53
  assert.ok(!(original(inputs.views[0]).getChild('from').toArray() instanceof BigInt64Array));
});

test('the full read calls toArray() on leaf columns only, in both libraries', async () => {
  for (const workload of ['F', 'A']) {
    const slot = await makeSlot(workload, 1);
    const lib = slot.parts.lib;
    const Proto = workload === 'F' ? lib.Column.prototype : lib.Vector.prototype;
    const T = lib.Type;
    const seen = [];
    const original = Proto.toArray;
    Proto.toArray = function spy(...args) {
      seen.push(this.type.typeId);
      return original.apply(this, args);
    };
    try {
      assert.equal(slot.decode(loadInputs('S300')), loadInputs('S300').expected);
    } finally {
      Proto.toArray = original;
    }
    assert.ok(seen.length > 100, `${workload}: toArray was called ${seen.length} times`);
    assert.equal(seen.filter((id) => id === T.List || id === T.Struct).length, 0, `${workload}: toArray on a list or a struct`);
    assert.ok(seen.every((id) => [T.Int, T.Bool, T.Utf8, T.Binary, T.FixedSizeBinary].includes(id)), `${workload}: leaf types only`);
  }
});

// The optional, stronger check of independent JIT state: under
// --allow-natives-syntax, optimising slot 1's copy of a function leaves slot 2's copy
// unoptimised. V8 does not keep every function optimised on every Node (on the Node
// of this check fullRead of apache-arrow's slot is not kept), so a function where the
// probe is not conclusive proves nothing and is reported; when none is, the test is
// skipped, and the check that the slots share no function stands on its own.
test('optimising slot 1\'s full read leaves slot 2\'s unoptimised (under --allow-natives-syntax)', async (t) => {
  let conclusive = 0;
  for (const workload of ['F', 'A', 'C']) {
    const res = spawnSync(process.execPath, ['--allow-natives-syntax', path.join(here, 'natives.probe.mjs'), workload], { encoding: 'utf8' });
    assert.equal(res.status, 0, res.stderr);
    const report = JSON.parse(res.stdout);
    for (const [name, r] of Object.entries(report)) {
      assert.equal(r.same_function, false, `${workload} ${name}`);
      if (!r.conclusive) {
        t.diagnostic(`${workload} ${name}: not conclusive on this Node`);
        continue;
      }
      conclusive++;
      assert.equal(r.slot2_optimized, false, `${workload} ${name}: slot 2 was optimised with slot 1`);
    }
  }
  if (conclusive === 0) t.skip('V8 on this Node kept no function optimised, so the probe proves nothing here');
});
