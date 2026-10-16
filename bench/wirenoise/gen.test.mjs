import assert from 'node:assert/strict';
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { EOS, loadInputs, splitStreams } from './split.mjs';
import { FIXTURE_DIR, SECTION_NAMES, SIZES, generate, generateAll, makeRows, xorshift } from './gen.mjs';
import { columnText, parseColumn, typeText } from './types.mjs';

const require = createRequire(import.meta.url);
const arrowFile = path.join(path.dirname(require.resolve('apache-arrow')), 'Arrow.esnext.min.js');

// The schemas of the twelve streams of bytesgate.Arrow in internal/wire/bytesgate/arrow.go
// at origin/main 3d92092 (an entity reference is a uint32), written out here by hand
// and not taken from gen.mjs. "?" marks a nullable field; a list item is never nullable.
const ELEMENT = 'a:u32,b:u32?,relation:u16?,derived_kind:u8?,detail:utf8?,subject:u8,class:u8,transitions:u32,paths:list<list<u32>>,pieces:list<struct<start:i64,end:i64,before:bool,end_reason:u8,producers:list<utf8>,ends:list<struct<path:u32,leg:u32,reason:u8>>>>';
const COLLISION = 'struct<stale_boot:utf8,observed_at:i64,newer_boot:utf8,newer_first_seen:i64>';
const TRUNCATION = 'nodes:list<u32>,hosts:list<u32>,members:list<u32>,instances:list<u32>,seed_pods:list<u32>,depth:list<u32>,frontier:list<struct<depth:u32,dropped:list<u32>>>,per_via:list<struct<via:u32,mechanism:u8,dropped:u32>>,results:u32,pairs:u32,members_dropped:u32,suppressed_dropped:u32';
const SUPPRESSED = 'a:u32,b:u32,derived_kind:u8,detail:utf8,reason:u8,coverage_nodes:u32?,coverage_frontier:u32?,class:u8,transitions:u32,spans:list<struct<start:i64,end:i64,before:bool>>';
const BYTESGATE_SCHEMAS = {
  header: 'format_major:u8,format_minor:u8,basis:u8,token:u64,from:i64,to:i64,policy_subgraph:u32,policy_interval:u32,policy_coresidency:u32,policy_extract:u32,policy_blastradius:u32,policy_graphdiff:u32,identity_encoding:u32,catalog_ids:fsb32,rank:bin,query_kind:u8,seeds:list<fsb18>,options:list<struct<name:utf8,value:i64>>,asof:u64',
  entities: 'fingerprint:fsb18,boundary:bool',
  elements: ELEMENT,
  keyframes: 'at:i64,elements:list<u32>',
  deltas: 'at:i64,element:u32,op:u8,reason:u8',
  quarantine: `entities:list<struct<entity:u32,layer:u8,collision:${COLLISION}?>>,edges:list<struct<element:struct<${ELEMENT}>,side:bool,layer:u8,collision:${COLLISION}?>>`,
  truncated: TRUNCATION,
  unattributed: 'seed_pod:u32?,subject:u32,reason:u8,relation:u16?,target:u16?,spans:list<struct<start:i64,end:i64,before:bool>>,candidates:list<u32>',
  unmapped: 'pod:u32,spans:list<struct<start:i64,end:i64,before:bool>>',
  suppressed: SUPPRESSED,
  pods: `seed:u32,truncated:struct<${TRUNCATION}>,quarantined:list<struct<kind:u8,a:u32,b:u32?,relation:u16?>>,suppressed:list<struct<${SUPPRESSED}>>`,
  anchor: 'anchor:u8,judged:bool,frontier_nodes:u32',
};

/** Reads a schema field of either library into the notation. `L` is the library's type-id table. */
function describeField(lib, f) {
  const T = lib.Type;
  const t = f.type;
  let text;
  switch (t.typeId) {
    case T.Int:
      text = `${t.isSigned ?? t.signed ? 'i' : 'u'}${t.bitWidth}`;
      break;
    case T.Bool:
      text = 'bool';
      break;
    case T.Utf8:
      text = 'utf8';
      break;
    case T.Binary:
      text = 'bin';
      break;
    case T.FixedSizeBinary:
      text = `fsb${t.byteWidth ?? t.stride}`;
      break;
    case T.List: {
      const item = t.children[0];
      assert.equal(item.name, 'item', 'a list item is named item');
      assert.equal(item.nullable, false, 'a list item is not nullable');
      text = `list<${describeField(lib, item)}>`;
      break;
    }
    case T.Struct:
      text = `struct<${t.children.map((c) => `${c.name}:${describeField(lib, c)}${c.nullable ? '?' : ''}`).join(',')}>`;
      break;
    default:
      assert.fail(`a type of id ${t.typeId} that bytesgate.Arrow does not write`);
  }
  return text;
}

const columns = (lib, schema) => schema.fields.map((f) => `${f.name}:${describeField(lib, f)}${f.nullable ? '?' : ''}`).join(',');

test('the notation reads and writes a column back', () => {
  for (const spec of Object.values(BYTESGATE_SCHEMAS)) {
    const cols = [];
    // split the top-level commas
    let depth = 0;
    let at = 0;
    for (let i = 0; i < spec.length; i++) {
      if (spec[i] === '<') depth++;
      else if (spec[i] === '>') depth--;
      else if (spec[i] === ',' && depth === 0) {
        cols.push(spec.slice(at, i));
        at = i + 1;
      }
    }
    cols.push(spec.slice(at));
    for (const c of cols) assert.equal(columnText(parseColumn(c)), c);
  }
  assert.equal(typeText(parseColumn('x:fsb18').type), 'fsb18');
  assert.throws(() => parseColumn('x:u128'), /unknown type/);
  assert.throws(() => parseColumn('x:list<u8'), /expected/);
});

test('the generator is deterministic: nine files, the same bytes twice', () => {
  const one = generateAll();
  const two = generateAll();
  assert.equal(one.size, 9);
  for (const [name, bytes] of one) assert.ok(Buffer.from(bytes).equals(Buffer.from(two.get(name))), name);
});

test('the checked-in fixtures are the generator\'s bytes', () => {
  for (const [name, bytes] of generateAll()) {
    assert.ok(Buffer.from(bytes).equals(readFileSync(path.join(FIXTURE_DIR, name))), `${name} is not what gen.mjs makes`);
  }
});

test('the xorshift is the 32-bit one, and the same for the same seed', () => {
  const a = xorshift(1);
  assert.deepEqual([a.u32(), a.u32(), a.u32()], [270369, 67634689, 2647435461]);
  const b = xorshift(0x5eed0300);
  const c = xorshift(0x5eed0300);
  assert.deepEqual([b.below(1000), b.below(1000)], [c.below(1000), c.below(1000)]);
});

test('the sizes have the shape the guide gives', () => {
  const rows = makeRows('S300');
  assert.equal(rows.elements.length, SIZES.S300.elements);
  assert.ok(SIZES.S300.elements >= 250 && SIZES.S300.elements <= 300);
  assert.equal(SIZES.S2k.elements, 2000);
  assert.equal(SIZES.S10k.elements, 10000);
  for (const size of Object.keys(SIZES)) {
    const r = makeRows(size);
    const n = r.elements.length;
    assert.ok(Math.abs(n / r.entities.length - 2) < 0.1, 'about two elements to an entity');
    assert.equal(r.deltas.length, Math.round(0.3 * n));
    const times = r.deltas.map((d) => d.at);
    assert.deepEqual(times, [...times].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)), 'delta times are sorted');
    for (const el of r.elements) {
      assert.ok(el.paths.length === 0 || el.subject === 1, 'only derived subjects have paths');
      assert.ok(el.pieces.length >= 1 && el.pieces.length <= 3);
      for (const p of el.paths) assert.ok(p.length >= 1 && p.length <= 4);
      for (const p of el.pieces) {
        assert.ok(p.producers.length >= 1 && p.producers.length <= 2);
        assert.ok(p.ends.length <= 2);
      }
    }
  }
});

test('every fixture has the twelve streams of bytesgate.Arrow, column for column, in both libraries', () => {
  const flechette = require('@uwdata/flechette');
  const arrow = require(arrowFile);
  for (const size of Object.keys(SIZES)) {
    const inputs = loadInputs(size);
    assert.equal(inputs.views.length, 12);
    inputs.views.forEach((view, i) => {
      const name = SECTION_NAMES[i];
      assert.equal(columns(flechette, flechette.tableFromIPC(view).schema), BYTESGATE_SCHEMAS[name], `${size} ${name} (Flechette)`);
      assert.equal(columns(arrow, arrow.tableFromIPC(view).schema), BYTESGATE_SCHEMAS[name], `${size} ${name} (apache-arrow)`);
    });
  }
});

test('a stream is one record batch, and the header, quarantine, truncated and anchor streams are one row', () => {
  const flechette = require('@uwdata/flechette');
  const arrow = require(arrowFile);
  const inputs = loadInputs('S300');
  inputs.views.forEach((view, i) => {
    const t = flechette.tableFromIPC(view);
    assert.equal(t.getChildAt(0).data.length, 1, SECTION_NAMES[i]);
    assert.equal(arrow.tableFromIPC(view).batches.length, 1, SECTION_NAMES[i]);
    if (['header', 'quarantine', 'truncated', 'anchor'].includes(SECTION_NAMES[i])) assert.equal(t.numRows, 1);
  });
});

test('split yields twelve views that each decode alone and end with the marker', () => {
  const flechette = require('@uwdata/flechette');
  const arrow = require(arrowFile);
  for (const size of Object.keys(SIZES)) {
    const g = generate(size);
    const views = splitStreams(g.arrow, g.offsets.sections);
    assert.equal(views.length, 12);
    assert.equal(views.reduce((s, v) => s + v.length, 0), g.arrow.length, 'the views tile the buffer');
    views.forEach((v, i) => {
      assert.deepEqual([...v.subarray(v.length - 8)], [...EOS], `${size} ${SECTION_NAMES[i]} ends with the marker`);
      assert.equal(v.byteOffset % 8, 0);
      assert.equal(flechette.tableFromIPC(v).numRows, g.offsets.sections[i].rows);
      assert.equal(arrow.tableFromIPC(v).numRows, g.offsets.sections[i].rows);
    });
  }
});

test('split refuses ranges that do not follow one another, or a stream without its marker', () => {
  const g = generate('S300');
  const s = g.offsets.sections;
  assert.throws(() => splitStreams(g.arrow, s.slice(1)), /12/);
  assert.throws(() => splitStreams(g.arrow, s.map((x, i) => (i === 3 ? { ...x, start: x.start + 8 } : x))), /does not follow/);
  assert.throws(() => splitStreams(g.arrow, s.map((x, i) => (i === 3 ? { ...x, end: x.end - 8 } : x))), /does not follow|marker/);
  assert.throws(() => splitStreams(g.arrow, s.map((x, i) => (i === 0 ? { ...x, name: 'x' } : x))), /want header/);
  // the last range ends where the buffer does
  assert.throws(() => splitStreams(Uint8Array.of(...g.arrow, 0, 0, 0, 0, 0, 0, 0, 0), s), /last range ends at/);
  assert.equal(splitStreams(g.arrow, s).length, 12);
});

test('no value in a fixture is outside what every library reads exactly', () => {
  for (const size of Object.keys(SIZES)) {
    const rows = makeRows(size);
    const big = [];
    const visit = (v) => {
      if (typeof v === 'bigint') big.push(v);
      else if (Array.isArray(v)) v.forEach(visit);
      else if (v && typeof v === 'object' && !ArrayBuffer.isView(v)) Object.values(v).forEach(visit);
    };
    visit(rows);
    assert.ok(big.length > 0);
    for (const v of big) assert.ok(v >= 0n && v < 2n ** 53n, `${v} must be a safe integer`);
  }
  // strings are printable ASCII, so a UTF-16 code unit is a byte
  const rows = makeRows('S300');
  const strings = [];
  const visit = (v) => {
    if (typeof v === 'string') strings.push(v);
    else if (Array.isArray(v)) v.forEach(visit);
    else if (v && typeof v === 'object' && !ArrayBuffer.isView(v)) Object.values(v).forEach(visit);
  };
  visit(rows);
  assert.ok(strings.length > 0);
  for (const s of strings) assert.match(s, /^[\x20-\x7e]*$/);
});
