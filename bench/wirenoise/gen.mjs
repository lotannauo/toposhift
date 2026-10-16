// The fixed inputs of the Node decode-noise measurement, made by a seeded
// generator, byte for byte the same on every run.
//
// Provenance: the shape of bytesgate.Arrow in internal/wire/bytesgate/arrow.go at
// origin/main 3d92092 (twelve Arrow IPC streams, one for each section of the wire
// format, in the order of the format's tags, each with its own schema, one record
// batch and the end-of-stream marker; an entity reference is a uint32; no body
// compression). The values are synthetic. No output of bytesgate is used, and no
// byte of any fixture of the bytes gate is reproduced here.
//
// For each of three sizes it writes, into fixtures/:
//
//   s<size>.arrow               the twelve streams, concatenated, each written by one
//                               call of apache-arrow's tableToIPC
//   s<size>.arrow.offsets.json  the twelve byte ranges of the streams, the section
//                               names, and the checksums a full read must return
//   s<size>.tsgf                the stand-in custom layout of the same values: twelve
//                               sections, each a tag byte, a uvarint length and a payload
//
//   node gen.mjs write   makes the nine files
//   node gen.mjs check   makes them again in memory and fails unless they are the files
//
// No Math.random and no clock: the one source of values is the 32-bit xorshift below.

import { createRequire } from 'node:module';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath, pathToFileURL } from 'node:url';
import path from 'node:path';

import { parseColumn } from './types.mjs';

const here = path.dirname(fileURLToPath(import.meta.url));
export const FIXTURE_DIR = path.join(here, 'fixtures');

/** The three sizes. `elements` is the count of rows of the elements section. */
export const SIZES = {
  S300: { id: 's300', elements: 280, seed: 0x5eed0300 },
  S2k: { id: 's2k', elements: 2000, seed: 0x5eed2000 },
  S10k: { id: 's10k', elements: 10000, seed: 0x5eed1000 },
};

// ---------------------------------------------------------------------------
// The twelve sections, as the schemas of bytesgate.Arrow writes them (entity
// references as uint32). Each column is "name:type", with a "?" when nullable.

const SPAN = 'struct<start:i64,end:i64,before:bool>';
const COLLISION = 'struct<stale_boot:utf8,observed_at:i64,newer_boot:utf8,newer_first_seen:i64>';
const ELEMENT = [
  'a:u32',
  'b:u32?',
  'relation:u16?',
  'derived_kind:u8?',
  'detail:utf8?',
  'subject:u8',
  'class:u8',
  'transitions:u32',
  'paths:list<list<u32>>',
  'pieces:list<struct<start:i64,end:i64,before:bool,end_reason:u8,producers:list<utf8>,ends:list<struct<path:u32,leg:u32,reason:u8>>>>',
];
const TRUNCATION = [
  'nodes:list<u32>',
  'hosts:list<u32>',
  'members:list<u32>',
  'instances:list<u32>',
  'seed_pods:list<u32>',
  'depth:list<u32>',
  'frontier:list<struct<depth:u32,dropped:list<u32>>>',
  'per_via:list<struct<via:u32,mechanism:u8,dropped:u32>>',
  'results:u32',
  'pairs:u32',
  'members_dropped:u32',
  'suppressed_dropped:u32',
];
const SUPPRESSED = [
  'a:u32',
  'b:u32',
  'derived_kind:u8',
  'detail:utf8',
  'reason:u8',
  'coverage_nodes:u32?',
  'coverage_frontier:u32?',
  'class:u8',
  'transitions:u32',
  `spans:list<${SPAN}>`,
];

/** The sections in the order of the format's tags. */
export const SECTION_NAMES = [
  'header',
  'entities',
  'elements',
  'keyframes',
  'deltas',
  'quarantine',
  'truncated',
  'unattributed',
  'unmapped',
  'suppressed',
  'pods',
  'anchor',
];

export const SECTION_COLUMNS = {
  header: [
    'format_major:u8',
    'format_minor:u8',
    'basis:u8',
    'token:u64',
    'from:i64',
    'to:i64',
    'policy_subgraph:u32',
    'policy_interval:u32',
    'policy_coresidency:u32',
    'policy_extract:u32',
    'policy_blastradius:u32',
    'policy_graphdiff:u32',
    'identity_encoding:u32',
    'catalog_ids:fsb32',
    'rank:bin',
    'query_kind:u8',
    'seeds:list<fsb18>',
    'options:list<struct<name:utf8,value:i64>>',
    'asof:u64',
  ],
  entities: ['fingerprint:fsb18', 'boundary:bool'],
  elements: ELEMENT,
  keyframes: ['at:i64', 'elements:list<u32>'],
  deltas: ['at:i64', 'element:u32', 'op:u8', 'reason:u8'],
  quarantine: [
    `entities:list<struct<entity:u32,layer:u8,collision:${COLLISION}?>>`,
    `edges:list<struct<element:struct<${ELEMENT.join(',')}>,side:bool,layer:u8,collision:${COLLISION}?>>`,
  ],
  truncated: TRUNCATION,
  unattributed: [
    'seed_pod:u32?',
    'subject:u32',
    'reason:u8',
    'relation:u16?',
    'target:u16?',
    `spans:list<${SPAN}>`,
    'candidates:list<u32>',
  ],
  unmapped: ['pod:u32', `spans:list<${SPAN}>`],
  suppressed: SUPPRESSED,
  pods: [
    'seed:u32',
    `truncated:struct<${TRUNCATION.join(',')}>`,
    'quarantined:list<struct<kind:u8,a:u32,b:u32?,relation:u16?>>',
    `suppressed:list<struct<${SUPPRESSED.join(',')}>>`,
  ],
  anchor: ['anchor:u8', 'judged:bool', 'frontier_nodes:u32'],
};

// ---------------------------------------------------------------------------
// The generator's one source of values.

/** A 32-bit xorshift (13, 17, 5). */
export function xorshift(seed) {
  let x = seed | 0 || 1;
  const u32 = () => {
    x ^= x << 13;
    x ^= x >>> 17;
    x ^= x << 5;
    return x >>> 0;
  };
  const below = (n) => Math.floor((u32() / 4294967296) * n);
  return {
    u32,
    below,
    range: (lo, hi) => lo + below(hi - lo + 1),
    chance: (p) => u32() / 4294967296 < p,
    pick: (list) => list[below(list.length)],
    bytes: (n) => Uint8Array.from({ length: n }, () => u32() & 0xff),
  };
}

const PRODUCERS = ['k8s-informer', 'kwok-capture', 'otel-host', 'cilium-flow', 'aws-config', 'manual', 'pod-heartbeat', 'node-agent'];
const WINDOW_MS = 3_600_000;
const NS = 1_000_000n;
const FROM = 1_000_000_000_000_000n; // below 2^53: a library that reads int64 as a number loses nothing
const ms = (n) => BigInt(n) * NS;

/** A fingerprint: two bytes of entity type, then sixteen of hash. */
function fingerprint(r) {
  const out = r.bytes(18);
  out[0] = 0;
  out[1] = 1 + r.below(12);
  return out;
}

/** A sorted list of distinct ids below `bound`, of about `count`. */
function sortedIds(r, bound, count) {
  const set = new Set();
  for (let i = 0; i < count; i++) set.add(r.below(bound));
  return [...set].sort((a, b) => a - b);
}

function spans(r, lo, hi) {
  return Array.from({ length: r.range(lo, hi) }, () => {
    const start = r.below(WINDOW_MS);
    return { start: ms(start), end: ms(start + r.below(WINDOW_MS + 1 - start)), before: r.chance(0.1) };
  });
}

function collision(r) {
  if (!r.chance(0.3)) return null;
  return {
    stale_boot: `boot-${r.below(1000000)}`,
    observed_at: ms(r.below(WINDOW_MS)),
    newer_boot: `boot-${r.below(1000000)}`,
    newer_first_seen: ms(r.below(WINDOW_MS)),
  };
}

/** One element: an entity, an edge or a derived subject. */
function element(r, n, e) {
  const roll = r.below(100);
  const kind = roll < 40 ? 'entity' : roll < 85 ? 'edge' : 'derived';
  const el = {
    a: r.below(e),
    b: kind === 'entity' ? null : r.below(e),
    relation: kind === 'edge' ? 1 + r.below(40) : null,
    derived_kind: kind === 'derived' ? 1 + r.below(4) : null,
    detail: kind === 'derived' ? `via-${r.below(1000)}` : null,
    subject: kind === 'derived' ? 1 : 0,
    class: r.below(4),
    transitions: r.below(9),
    paths: [],
    pieces: [],
  };
  // paths: 0 for an asserted subject, 1 to 3 for a derived one; 1 to 4 legs each
  const npaths = kind === 'derived' ? r.range(1, 3) : 0;
  for (let i = 0; i < npaths; i++) el.paths.push(Array.from({ length: r.range(1, 4) }, () => r.below(n)));
  // pieces: 1 to 3; producers 1 to 2; ends 0 to 2
  for (let i = r.range(1, 3); i > 0; i--) {
    const start = r.below(WINDOW_MS);
    el.pieces.push({
      start: ms(start),
      end: ms(start + r.below(WINDOW_MS + 1 - start)),
      before: r.chance(0.1),
      end_reason: r.below(4),
      producers: Array.from({ length: r.range(1, 2) }, () => r.pick(PRODUCERS)),
      ends: Array.from({ length: r.range(0, 2) }, () => ({
        path: npaths > 0 ? r.below(npaths) : 0,
        leg: r.range(1, 4),
        reason: r.below(4),
      })),
    });
  }
  return el;
}

function truncation(r, e, count) {
  return {
    nodes: sortedIds(r, e, count),
    hosts: sortedIds(r, e, Math.max(1, count >> 2)),
    members: sortedIds(r, e, count),
    instances: sortedIds(r, e, count),
    seed_pods: sortedIds(r, e, Math.max(1, count >> 1)),
    depth: sortedIds(r, e, count),
    frontier: Array.from({ length: 3 }, (_, i) => ({ depth: i + 1, dropped: sortedIds(r, e, count) })),
    per_via: Array.from({ length: 4 }, () => ({ via: r.below(e), mechanism: r.below(3), dropped: r.below(50) })),
    results: r.below(5000),
    pairs: r.below(5000),
    members_dropped: r.below(100),
    suppressed_dropped: r.below(100),
  };
}

function suppressed(r, e) {
  const ubiquitous = r.chance(0.5);
  return {
    a: r.below(e),
    b: r.below(e),
    derived_kind: 1 + r.below(4),
    detail: `rule-${r.below(500)}`,
    reason: ubiquitous ? 1 : 2,
    coverage_nodes: ubiquitous ? r.below(1000) : null,
    coverage_frontier: ubiquitous ? r.below(100) : null,
    class: r.below(4),
    transitions: r.below(9),
    spans: spans(r, 1, 3),
  };
}

/**
 * The rows of every section of one fixture. The proportions follow the registered
 * fixtures: about two elements to an entity, deltas about 0.3 times the elements
 * with sorted times, a keyframe for about every half of the elements in deltas, and
 * small sections for the rest.
 */
export function makeRows(size) {
  const { elements: n, seed } = SIZES[size];
  const r = xorshift(seed);
  const e = Math.ceil(n / 2);
  const small = (div, min) => Math.max(min, Math.round(n / div));
  const rows = {};

  rows.header = [
    {
      format_major: 1,
      format_minor: 0,
      basis: 2,
      token: BigInt(n) * 1000003n,
      from: FROM,
      to: FROM + ms(WINDOW_MS),
      policy_subgraph: 1,
      policy_interval: 1,
      policy_coresidency: 1,
      policy_extract: 2,
      policy_blastradius: 1,
      policy_graphdiff: 1,
      identity_encoding: 1,
      catalog_ids: r.bytes(32),
      rank: r.bytes(24),
      query_kind: 2,
      seeds: [fingerprint(r), fingerprint(r)],
      options: [
        { name: 'depth', value: 3n },
        { name: 'limit', value: 10000n },
        { name: 'window', value: 3600n },
        { name: 'cap', value: 64n },
      ],
      asof: 42n,
    },
  ];
  rows.entities = Array.from({ length: e }, () => ({ fingerprint: fingerprint(r), boundary: r.chance(0.05) }));
  rows.elements = Array.from({ length: n }, () => element(r, n, e));

  const ndeltas = Math.round(0.3 * n);
  const nk = 1 + Math.floor(ndeltas / (n / 2));
  rows.keyframes = Array.from({ length: nk }, (_, k) => {
    const at = Math.floor(((k + 1) * WINDOW_MS) / (nk + 1));
    const present = [];
    rows.elements.forEach((el, i) => {
      if (el.pieces.some((p) => p.start <= ms(at) && ms(at) < p.end)) present.push(i);
    });
    return { at: ms(at), elements: present };
  });
  const times = Array.from({ length: ndeltas }, () => r.below(WINDOW_MS)).sort((a, b) => a - b);
  rows.deltas = times.map((t) => ({ at: ms(t), element: r.below(n), op: 1 + r.below(2), reason: r.below(6) }));

  rows.quarantine = [
    {
      entities: Array.from({ length: small(50, 2) }, () => ({ entity: r.below(e), layer: r.below(3), collision: collision(r) })),
      edges: Array.from({ length: small(100, 1) }, () => ({
        element: element(r, n, e),
        side: r.chance(0.5),
        layer: r.below(3),
        collision: collision(r),
      })),
    },
  ];
  rows.truncated = [truncation(r, e, small(60, 2))];
  rows.unattributed = Array.from({ length: small(100, 3) }, () => {
    const claims = r.chance(0.5);
    return {
      seed_pod: r.chance(0.7) ? r.below(e) : null,
      subject: r.below(e),
      reason: claims ? 2 : 1,
      relation: claims ? 1 + r.below(40) : null,
      target: claims ? 1 + r.below(12) : null,
      spans: spans(r, 1, 2),
      candidates: sortedIds(r, e, r.range(1, 3)),
    };
  });
  rows.unmapped = Array.from({ length: small(50, 3) }, () => ({ pod: r.below(e), spans: spans(r, 1, 3) }));
  rows.suppressed = Array.from({ length: small(80, 2) }, () => suppressed(r, e));
  rows.pods = Array.from({ length: small(70, 3) }, () => ({
    seed: r.below(e),
    truncated: truncation(r, e, 3),
    quarantined: Array.from({ length: r.range(0, 2) }, () => {
      const edge = r.chance(0.5);
      return { kind: edge ? 2 : 1, a: r.below(e), b: edge ? r.below(e) : null, relation: edge ? 1 + r.below(40) : null };
    }),
    suppressed: Array.from({ length: r.range(0, 2) }, () => suppressed(r, e)),
  }));
  rows.anchor = [{ anchor: 1, judged: true, frontier_nodes: r.below(500) }];
  return rows;
}

// ---------------------------------------------------------------------------
// The Arrow streams.

const requireCjs = createRequire(import.meta.url);

function loadArrow() {
  // The single-file build that the measurement times.
  const dir = path.dirname(requireCjs.resolve('apache-arrow'));
  return requireCjs(path.join(dir, 'Arrow.esnext.min.js'));
}

function arrowType(A, t) {
  switch (t.k) {
    case 'u8':
      return new A.Uint8();
    case 'u16':
      return new A.Uint16();
    case 'u32':
      return new A.Uint32();
    case 'u64':
      return new A.Uint64();
    case 'i64':
      return new A.Int64();
    case 'bool':
      return new A.Bool();
    case 'utf8':
      return new A.Utf8();
    case 'bin':
      return new A.Binary();
    case 'fsb':
      return new A.FixedSizeBinary(t.n);
    case 'list':
      return new A.List(new A.Field('item', arrowType(A, t.item), false));
    case 'struct':
      return new A.Struct(t.fields.map((c) => new A.Field(c.name, arrowType(A, c.type), c.nullable)));
    default:
      throw new Error(`gen: no Arrow type for ${t.k}`);
  }
}

/** One stream: the schema, one record batch of the rows, the end-of-stream marker. */
function writeStream(A, columns, rows) {
  const cols = columns.map(parseColumn);
  const fields = cols.map((c) => new A.Field(c.name, arrowType(A, c.type), c.nullable));
  const vectors = cols.map((c, i) =>
    A.vectorFromArray(
      rows.map((row) => row[c.name]),
      fields[i].type,
    ),
  );
  const schema = new A.Schema(fields);
  const data = A.makeData({
    type: new A.Struct(fields),
    length: rows.length,
    nullCount: 0,
    children: vectors.map((v) => v.data[0]),
  });
  const batch = new A.RecordBatch(schema, data);
  return A.tableToIPC(new A.Table(schema, batch), 'stream');
}

// ---------------------------------------------------------------------------
// The stand-in layout: tag, uvarint length, payload; uvarint ids; sorted lists
// delta-encoded; times as uvarints relative to From. Integers are written as
// numbers, so every int64 of a fixture is below 2^53.

class Writer {
  constructor() {
    this.chunks = [];
    this.cur = [];
  }
  u8(b) {
    this.cur.push(b & 0xff);
  }
  uv(v) {
    let n = Number(v);
    if (!Number.isSafeInteger(n) || n < 0) throw new Error(`gen: not a uvarint: ${v}`);
    while (n >= 128) {
      this.cur.push((n % 128) | 128);
      n = Math.floor(n / 128);
    }
    this.cur.push(n);
  }
  raw(bytes) {
    for (const b of bytes) this.cur.push(b);
  }
  str(s) {
    if (!/^[\x20-\x7e]*$/.test(s)) throw new Error(`gen: a string that is not printable ASCII: ${s}`);
    this.uv(s.length);
    for (let i = 0; i < s.length; i++) this.cur.push(s.charCodeAt(i));
  }
  bytes() {
    return Uint8Array.from(this.cur);
  }
}

function sortedList(w, ids) {
  w.uv(ids.length);
  let prev = 0;
  for (const id of ids) {
    w.uv(id - prev);
    prev = id;
  }
}

function spanList(w, list) {
  w.uv(list.length);
  for (const s of list) {
    w.uv(s.start);
    w.uv(s.end - s.start);
    w.u8(s.before ? 1 : 0);
  }
}

function elementRow(w, el) {
  w.uv(el.a);
  w.u8((el.b !== null ? 1 : 0) | (el.relation !== null ? 2 : 0) | (el.derived_kind !== null ? 4 : 0) | (el.detail !== null ? 8 : 0));
  if (el.b !== null) w.uv(el.b);
  if (el.relation !== null) w.uv(el.relation);
  if (el.derived_kind !== null) w.u8(el.derived_kind);
  if (el.detail !== null) w.str(el.detail);
  w.u8(el.subject);
  w.u8(el.class);
  w.uv(el.transitions);
  w.uv(el.paths.length);
  for (const p of el.paths) {
    w.uv(p.length);
    for (const leg of p) w.uv(leg);
  }
  w.uv(el.pieces.length);
  for (const p of el.pieces) {
    w.uv(p.start);
    w.uv(p.end - p.start);
    w.u8(p.before ? 1 : 0);
    w.u8(p.end_reason);
    w.uv(p.producers.length);
    for (const s of p.producers) w.str(s);
    w.uv(p.ends.length);
    for (const x of p.ends) {
      w.uv(x.path);
      w.uv(x.leg);
      w.u8(x.reason);
    }
  }
}

function collisionRow(w, c) {
  if (c === null) {
    w.u8(0);
    return;
  }
  w.u8(1);
  w.str(c.stale_boot);
  w.uv(c.observed_at);
  w.str(c.newer_boot);
  w.uv(c.newer_first_seen);
}

function truncationRow(w, t) {
  for (const k of ['nodes', 'hosts', 'members', 'instances', 'seed_pods', 'depth']) sortedList(w, t[k]);
  w.uv(t.frontier.length);
  for (const f of t.frontier) {
    w.uv(f.depth);
    sortedList(w, f.dropped);
  }
  w.uv(t.per_via.length);
  for (const v of t.per_via) {
    w.uv(v.via);
    w.u8(v.mechanism);
    w.uv(v.dropped);
  }
  for (const k of ['results', 'pairs', 'members_dropped', 'suppressed_dropped']) w.uv(t[k]);
}

function suppressedRow(w, s) {
  w.uv(s.a);
  w.uv(s.b);
  w.u8(s.derived_kind);
  w.str(s.detail);
  w.u8(s.reason);
  w.u8((s.coverage_nodes !== null ? 1 : 0) | (s.coverage_frontier !== null ? 2 : 0));
  if (s.coverage_nodes !== null) w.uv(s.coverage_nodes);
  if (s.coverage_frontier !== null) w.uv(s.coverage_frontier);
  w.u8(s.class);
  w.uv(s.transitions);
  spanList(w, s.spans);
}

const SECTION_WRITERS = {
  header(w, rows) {
    for (const h of rows) {
      w.u8(h.format_major);
      w.u8(h.format_minor);
      w.u8(h.basis);
      w.uv(h.token);
      w.uv(h.from);
      w.uv(h.to);
      for (const k of ['policy_subgraph', 'policy_interval', 'policy_coresidency', 'policy_extract', 'policy_blastradius', 'policy_graphdiff', 'identity_encoding']) w.uv(h[k]);
      w.raw(h.catalog_ids);
      w.uv(h.rank.length);
      w.raw(h.rank);
      w.u8(h.query_kind);
      w.uv(h.seeds.length);
      for (const s of h.seeds) w.raw(s);
      w.uv(h.options.length);
      for (const o of h.options) {
        w.str(o.name);
        w.uv(o.value);
      }
      w.uv(h.asof);
    }
  },
  entities(w, rows) {
    for (const x of rows) {
      w.raw(x.fingerprint);
      w.u8(x.boundary ? 1 : 0);
    }
  },
  elements(w, rows) {
    for (const el of rows) elementRow(w, el);
  },
  keyframes(w, rows) {
    let prev = 0n;
    for (const k of rows) {
      w.uv(k.at - prev);
      prev = k.at;
      sortedList(w, k.elements);
    }
  },
  deltas(w, rows) {
    let prev = 0n;
    for (const d of rows) {
      w.uv(d.at - prev);
      prev = d.at;
      w.uv(d.element);
      w.u8(d.op);
      w.u8(d.reason);
    }
  },
  quarantine(w, rows) {
    for (const q of rows) {
      w.uv(q.entities.length);
      for (const x of q.entities) {
        w.uv(x.entity);
        w.u8(x.layer);
        collisionRow(w, x.collision);
      }
      w.uv(q.edges.length);
      for (const x of q.edges) {
        elementRow(w, x.element);
        w.u8(x.side ? 1 : 0);
        w.u8(x.layer);
        collisionRow(w, x.collision);
      }
    }
  },
  truncated(w, rows) {
    for (const t of rows) truncationRow(w, t);
  },
  unattributed(w, rows) {
    for (const u of rows) {
      w.u8((u.seed_pod !== null ? 1 : 0) | (u.relation !== null ? 2 : 0) | (u.target !== null ? 4 : 0));
      if (u.seed_pod !== null) w.uv(u.seed_pod);
      w.uv(u.subject);
      w.u8(u.reason);
      if (u.relation !== null) w.uv(u.relation);
      if (u.target !== null) w.uv(u.target);
      spanList(w, u.spans);
      sortedList(w, u.candidates);
    }
  },
  unmapped(w, rows) {
    for (const u of rows) {
      w.uv(u.pod);
      spanList(w, u.spans);
    }
  },
  suppressed(w, rows) {
    for (const s of rows) suppressedRow(w, s);
  },
  pods(w, rows) {
    for (const p of rows) {
      w.uv(p.seed);
      truncationRow(w, p.truncated);
      w.uv(p.quarantined.length);
      for (const k of p.quarantined) {
        w.u8(k.kind);
        w.uv(k.a);
        w.u8((k.b !== null ? 1 : 0) | (k.relation !== null ? 2 : 0));
        if (k.b !== null) w.uv(k.b);
        if (k.relation !== null) w.uv(k.relation);
      }
      w.uv(p.suppressed.length);
      for (const s of p.suppressed) suppressedRow(w, s);
    }
  },
  anchor(w, rows) {
    for (const a of rows) {
      w.u8(a.anchor);
      w.u8(a.judged ? 1 : 0);
      w.uv(a.frontier_nodes);
    }
  },
};

/** The stand-in layout of all twelve sections: tag (1 to 12), uvarint length, payload. */
function writeTsgf(rows) {
  const out = [];
  SECTION_NAMES.forEach((name, i) => {
    const w = new Writer();
    w.uv(rows[name].length);
    SECTION_WRITERS[name](w, rows[name]);
    const payload = w.bytes();
    const head = new Writer();
    head.u8(i + 1);
    head.uv(payload.length);
    out.push(head.bytes(), payload);
  });
  const total = out.reduce((s, b) => s + b.length, 0);
  const buf = new Uint8Array(total);
  let at = 0;
  for (const b of out) {
    buf.set(b, at);
    at += b.length;
  }
  return buf;
}

// ---------------------------------------------------------------------------
// The checksum a full read must return, computed from the values themselves and
// not from any bytes. It follows README.md's definition; it deliberately shares no
// code with fullread.mjs.

const SEED = 2166136261;
const step = (h, c) => Math.imul(h ^ c, 16777619) >>> 0;
const low32 = (v) => Number(BigInt.asUintN(32, BigInt(v)));

function leafSum(t, vals) {
  let s = 0;
  for (const v of vals) {
    if (v === null) continue;
    switch (t.k) {
      case 'u8':
      case 'u16':
      case 'u32':
        s += v;
        break;
      case 'u64':
      case 'i64':
        s += low32(v);
        break;
      case 'bool':
        s += v ? 1 : 0;
        break;
      case 'utf8':
        for (let i = 0; i < v.length; i++) s += v.charCodeAt(i);
        break;
      default: // bin, fsb
        for (const b of v) s += b;
    }
    s %= 4294967296;
  }
  return s;
}

function foldNode(t, vals, st) {
  for (const v of vals) if (v === null) st.nulls++;
  if (t.k === 'list') {
    let total = 0;
    const items = [];
    for (const v of vals) {
      if (v === null) continue;
      total += v.length;
      for (const x of v) items.push(x);
    }
    st.h = step(st.h, total % 4294967296);
    foldNode(t.item, items, st);
  } else if (t.k === 'struct') {
    st.h = step(st.h, vals.length);
    for (const f of t.fields) foldNode(f.type, vals.map((v) => (v === null ? null : v[f.name])), st);
  } else {
    st.h = step(st.h, leafSum(t, vals));
  }
}

/** Folds one section into st, or into a fresh state, and returns it. */
function foldSection(name, rows, st = { h: SEED, nulls: 0 }) {
  for (const c of SECTION_COLUMNS[name].map(parseColumn)) {
    foldNode(c.type, rows.map((row) => row[c.name]), st);
  }
  return st;
}

export const finish = (st) => (st.h + st.nulls) >>> 0;

/** The checksums of a fixture: the whole, and each section alone. */
export function expectedChecksums(rows) {
  const all = { h: SEED, nulls: 0 };
  const sections = SECTION_NAMES.map((name) => {
    foldSection(name, rows[name], all);
    return finish(foldSection(name, rows[name]));
  });
  return { checksum: finish(all), sections };
}

// ---------------------------------------------------------------------------

/**
 * Everything of one fixture, in memory.
 * @param {string} size S300, S2k or S10k
 */
export function generate(size, A = loadArrow()) {
  const rows = makeRows(size);
  const streams = SECTION_NAMES.map((name) => writeStream(A, SECTION_COLUMNS[name], rows[name]));
  const total = streams.reduce((s, b) => s + b.length, 0);
  const arrow = new Uint8Array(total);
  const ranges = [];
  let at = 0;
  streams.forEach((b, i) => {
    arrow.set(b, at);
    ranges.push({ name: SECTION_NAMES[i], start: at, end: at + b.length, rows: rows[SECTION_NAMES[i]].length });
    at += b.length;
  });
  const sums = expectedChecksums(rows);
  const offsets = {
    provenance: 'shape of bytesgate.Arrow at origin/main 3d92092; values synthetic; no output of bytesgate is used',
    size,
    sections: ranges,
    checksum: sums.checksum,
    section_checksums: sums.sections,
  };
  return { rows, arrow, offsets, tsgf: writeTsgf(rows) };
}

/** The nine files of the three fixtures, by name. */
export function generateAll() {
  const A = loadArrow();
  const files = new Map();
  for (const [size, { id }] of Object.entries(SIZES)) {
    const g = generate(size, A);
    files.set(`${id}.arrow`, g.arrow);
    files.set(`${id}.arrow.offsets.json`, new TextEncoder().encode(`${JSON.stringify(g.offsets, null, 2)}\n`));
    files.set(`${id}.tsgf`, g.tsgf);
  }
  return files;
}

function main(mode) {
  const files = generateAll();
  if (mode === 'write') {
    mkdirSync(FIXTURE_DIR, { recursive: true });
    for (const [name, bytes] of files) writeFileSync(path.join(FIXTURE_DIR, name), bytes);
    console.log(`gen: wrote ${files.size} files into fixtures/`);
    return 0;
  }
  if (mode === 'check') {
    const bad = [];
    for (const [name, bytes] of files) {
      const file = path.join(FIXTURE_DIR, name);
      if (!existsSync(file) || !Buffer.from(bytes).equals(readFileSync(file))) bad.push(name);
    }
    if (bad.length > 0) {
      console.error(`gen: not the checked-in bytes: ${bad.join(', ')}`);
      return 1;
    }
    console.log(`gen: ${files.size} files regenerated byte for byte`);
    return 0;
  }
  console.error('usage: node gen.mjs write|check');
  return 2;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv[2]);
}
