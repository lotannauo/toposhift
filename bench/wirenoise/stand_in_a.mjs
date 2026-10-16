// Stand-in custom reader, copy A. Copy B is this file with this line changed.
//
// A hand-written reader of the twelve sections of a fixture's .tsgf layout in the
// style of the wire format: a tag byte, a uvarint length and a payload; uvarint
// ids; sorted lists delta-encoded; times as uvarints relative to the window's
// start. It decodes into typed arrays and offset arrays, and gives the nodes that
// fullread.mjs walks. It is NOT the decoder of the decode gate: only the noise of
// code of this kind is used, never its speed.
//
// The file imports nothing, so that the two copies share no function.

const KIND_LEAF = 0;
const KIND_LIST = 1;
const KIND_STRUCT = 2;

// ---------------------------------------------------------------------------
// The builders. Each gives a node {kind, length, nullCount, values | offsets |
// children} when it is done. A null slot of a struct is a null slot in each of its
// children, as the Arrow writers make it.

class Leaf {
  constructor(Ctor) {
    this.a = new Ctor(64);
    this.n = 0;
    this.nulls = 0;
  }
  push(v) {
    if (this.n === this.a.length) {
      const bigger = new this.a.constructor(this.a.length * 2);
      bigger.set(this.a);
      this.a = bigger;
    }
    this.a[this.n++] = v;
  }
  null() {
    this.nulls++;
    this.push(0);
  }
  node() {
    return { kind: KIND_LEAF, length: this.n, nullCount: this.nulls, values: this.a.subarray(0, this.n) };
  }
}

/** Bytes of fixed width (fixed_size_binary) or of variable length (utf8, binary). */
class Bytes {
  constructor(width) {
    this.width = width;
    this.a = new Uint8Array(256);
    this.n = 0;
    this.rows = 0;
    this.nulls = 0;
    this.offs = width === 0 ? new Leaf(Int32Array) : null;
    if (this.offs) this.offs.push(0);
  }
  room(extra) {
    if (this.n + extra > this.a.length) {
      const bigger = new Uint8Array(Math.max(this.a.length * 2, this.n + extra));
      bigger.set(this.a);
      this.a = bigger;
    }
  }
  put(src, pos, len) {
    this.room(len);
    this.a.set(src.subarray(pos, pos + len), this.n);
    this.n += len;
    this.rows++;
    if (this.offs) this.offs.push(this.n);
  }
  null() {
    this.nulls++;
    this.rows++;
    if (this.offs) this.offs.push(this.n);
    else {
      this.room(this.width);
      this.n += this.width;
    }
  }
  node() {
    return { kind: KIND_LEAF, length: this.rows, nullCount: this.nulls, values: this.a.subarray(0, this.n) };
  }
}

class List {
  constructor(child) {
    this.child = child;
    this.o = new Leaf(Int32Array);
    this.o.push(0);
    this.total = 0;
    this.nulls = 0;
  }
  /** Ends a list of `count` items, which the caller has pushed into the child. */
  add(count) {
    this.total += count;
    this.o.push(this.total);
  }
  null() {
    this.nulls++;
    this.o.push(this.total);
  }
  node() {
    return {
      kind: KIND_LIST,
      length: this.o.n - 1,
      nullCount: this.nulls,
      offsets: this.o.a.subarray(0, this.o.n),
      children: [this.child.node()],
    };
  }
}

class Struct {
  constructor(children) {
    this.children = children;
    this.len = 0;
    this.nulls = 0;
  }
  ok() {
    this.len++;
  }
  null() {
    this.len++;
    this.nulls++;
    for (const c of this.children) c.null();
  }
  node() {
    return { kind: KIND_STRUCT, length: this.len, nullCount: this.nulls, children: this.children.map((c) => c.node()) };
  }
}

const u8c = () => new Leaf(Uint8Array);
const u16c = () => new Leaf(Uint16Array);
const u32c = () => new Leaf(Uint32Array);
const f64c = () => new Leaf(Float64Array);

// ---------------------------------------------------------------------------
// The byte reader.

class Reader {
  constructor(buf, pos, end) {
    this.buf = buf;
    this.pos = pos;
    this.end = end;
  }
  u8() {
    if (this.pos >= this.end) throw new Error('stand-in: truncated section');
    return this.buf[this.pos++];
  }
  /** A uvarint of at most 8 bytes, so at most 56 bits. */
  uv() {
    let r = 0;
    let mul = 1;
    for (let i = 0; i < 8; i++) {
      const b = this.u8();
      r += (b & 0x7f) * mul;
      if (b < 0x80) return r;
      mul *= 128;
    }
    throw new Error('stand-in: overlong uvarint');
  }
  /** Takes n bytes and gives where they start. */
  take(n) {
    if (n < 0 || this.pos + n > this.end) throw new Error('stand-in: a field runs past the end of its section');
    const at = this.pos;
    this.pos += n;
    return at;
  }
  /** Pushes a length-prefixed string into a Bytes builder. */
  str(into) {
    const n = this.uv();
    into.put(this.buf, this.take(n), n);
  }
}

// ---------------------------------------------------------------------------
// The shapes that more than one section has.

function sortedInto(r, leaf) {
  const n = r.uv();
  let prev = 0;
  for (let i = 0; i < n; i++) {
    prev += r.uv();
    leaf.push(prev);
  }
  return n;
}

/** A list<u32> column read from a sorted, delta-encoded list. */
function sortedColumn() {
  const items = u32c();
  const list = new List(items);
  return { list, read: (r) => list.add(sortedInto(r, items)) };
}

/** list<struct<start:i64,end:i64,before:bool>> */
function spanColumn() {
  const start = f64c();
  const end = f64c();
  const before = u8c();
  const st = new Struct([start, end, before]);
  const list = new List(st);
  return {
    list,
    read(r) {
      const n = r.uv();
      for (let i = 0; i < n; i++) {
        const s = r.uv();
        start.push(s);
        end.push(s + r.uv());
        before.push(r.u8());
        st.ok();
      }
      list.add(n);
    },
  };
}

/** The ten columns of an element. */
function elementColumns() {
  const c = {
    a: u32c(),
    b: u32c(),
    relation: u16c(),
    derived_kind: u8c(),
    detail: new Bytes(0),
    subject: u8c(),
    klass: u8c(),
    transitions: u32c(),
  };
  const legs = u32c();
  const path = new List(legs);
  c.legs = legs;
  c.path = path;
  c.paths = new List(path);
  const pStart = f64c();
  const pEnd = f64c();
  const pBefore = u8c();
  const pReason = u8c();
  const prodBytes = new Bytes(0);
  const producers = new List(prodBytes);
  const ePath = u32c();
  const eLeg = u32c();
  const eReason = u8c();
  const endSt = new Struct([ePath, eLeg, eReason]);
  const ends = new List(endSt);
  const pieceSt = new Struct([pStart, pEnd, pBefore, pReason, producers, ends]);
  Object.assign(c, { pStart, pEnd, pBefore, pReason, prodBytes, producers, ePath, eLeg, eReason, endSt, ends, pieceSt });
  c.pieces = new List(pieceSt);
  c.all = [c.a, c.b, c.relation, c.derived_kind, c.detail, c.subject, c.klass, c.transitions, c.paths, c.pieces];
  return c;
}

function readElement(r, c) {
  c.a.push(r.uv());
  const mask = r.u8();
  if (mask & 1) c.b.push(r.uv());
  else c.b.null();
  if (mask & 2) c.relation.push(r.uv());
  else c.relation.null();
  if (mask & 4) c.derived_kind.push(r.u8());
  else c.derived_kind.null();
  if (mask & 8) r.str(c.detail);
  else c.detail.null();
  c.subject.push(r.u8());
  c.klass.push(r.u8());
  c.transitions.push(r.uv());
  const np = r.uv();
  for (let i = 0; i < np; i++) {
    const nl = r.uv();
    for (let j = 0; j < nl; j++) c.legs.push(r.uv());
    c.path.add(nl);
  }
  c.paths.add(np);
  const npieces = r.uv();
  for (let i = 0; i < npieces; i++) {
    const s = r.uv();
    c.pStart.push(s);
    c.pEnd.push(s + r.uv());
    c.pBefore.push(r.u8());
    c.pReason.push(r.u8());
    const nprod = r.uv();
    for (let j = 0; j < nprod; j++) r.str(c.prodBytes);
    c.producers.add(nprod);
    const nends = r.uv();
    for (let j = 0; j < nends; j++) {
      c.ePath.push(r.uv());
      c.eLeg.push(r.uv());
      c.eReason.push(r.u8());
      c.endSt.ok();
    }
    c.ends.add(nends);
    c.pieceSt.ok();
  }
  c.pieces.add(npieces);
}

/** struct<stale_boot:utf8,observed_at:i64,newer_boot:utf8,newer_first_seen:i64>, nullable. */
function collisionColumn() {
  const stale = new Bytes(0);
  const seen = f64c();
  const newer = new Bytes(0);
  const first = f64c();
  const st = new Struct([stale, seen, newer, first]);
  return {
    st,
    read(r) {
      if (r.u8() === 0) {
        st.null();
        return;
      }
      r.str(stale);
      seen.push(r.uv());
      r.str(newer);
      first.push(r.uv());
      st.ok();
    },
  };
}

/** The twelve columns of a truncation, as the nodes of a table or the fields of a struct. */
function truncationColumns() {
  const lists = ['nodes', 'hosts', 'members', 'instances', 'seed_pods', 'depth'].map(sortedColumn);
  const fDepth = u32c();
  const fDropped = sortedColumn();
  const fSt = new Struct([fDepth, fDropped.list]);
  const frontier = new List(fSt);
  const vVia = u32c();
  const vMech = u8c();
  const vDropped = u32c();
  const vSt = new Struct([vVia, vMech, vDropped]);
  const perVia = new List(vSt);
  const counts = [u32c(), u32c(), u32c(), u32c()];
  return {
    all: [...lists.map((l) => l.list), frontier, perVia, ...counts],
    read(r) {
      for (const l of lists) l.read(r);
      const nf = r.uv();
      for (let i = 0; i < nf; i++) {
        fDepth.push(r.uv());
        fDropped.read(r);
        fSt.ok();
      }
      frontier.add(nf);
      const nv = r.uv();
      for (let i = 0; i < nv; i++) {
        vVia.push(r.uv());
        vMech.push(r.u8());
        vDropped.push(r.uv());
        vSt.ok();
      }
      perVia.add(nv);
      for (const c of counts) c.push(r.uv());
    },
  };
}

/** The ten columns of a suppressed entry. */
function suppressedColumns() {
  const c = {
    a: u32c(),
    b: u32c(),
    dk: u8c(),
    detail: new Bytes(0),
    reason: u8c(),
    covNodes: u32c(),
    covFrontier: u32c(),
    klass: u8c(),
    transitions: u32c(),
    spans: spanColumn(),
  };
  return {
    all: [c.a, c.b, c.dk, c.detail, c.reason, c.covNodes, c.covFrontier, c.klass, c.transitions, c.spans.list],
    read(r) {
      c.a.push(r.uv());
      c.b.push(r.uv());
      c.dk.push(r.u8());
      r.str(c.detail);
      c.reason.push(r.u8());
      const mask = r.u8();
      if (mask & 1) c.covNodes.push(r.uv());
      else c.covNodes.null();
      if (mask & 2) c.covFrontier.push(r.uv());
      else c.covFrontier.null();
      c.klass.push(r.u8());
      c.transitions.push(r.uv());
      c.spans.read(r);
    },
  };
}

// ---------------------------------------------------------------------------
// The twelve sections. Each reads its payload and gives the nodes of its columns
// in schema order.

function readHeader(r) {
  const fm = u8c();
  const fn = u8c();
  const basis = u8c();
  const token = f64c();
  const from = f64c();
  const to = f64c();
  const policy = Array.from({ length: 7 }, u32c);
  const catalog = new Bytes(32);
  const rank = new Bytes(0);
  const kind = u8c();
  const seedBytes = new Bytes(18);
  const seeds = new List(seedBytes);
  const optName = new Bytes(0);
  const optValue = f64c();
  const optSt = new Struct([optName, optValue]);
  const options = new List(optSt);
  const asof = f64c();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    fm.push(r.u8());
    fn.push(r.u8());
    basis.push(r.u8());
    token.push(r.uv());
    from.push(r.uv());
    to.push(r.uv());
    for (const p of policy) p.push(r.uv());
    catalog.put(r.buf, r.take(32), 32);
    const nr = r.uv();
    rank.put(r.buf, r.take(nr), nr);
    kind.push(r.u8());
    const ns = r.uv();
    for (let j = 0; j < ns; j++) seedBytes.put(r.buf, r.take(18), 18);
    seeds.add(ns);
    const no = r.uv();
    for (let j = 0; j < no; j++) {
      r.str(optName);
      optValue.push(r.uv());
      optSt.ok();
    }
    options.add(no);
    asof.push(r.uv());
  }
  return [fm, fn, basis, token, from, to, ...policy, catalog, rank, kind, seeds, options, asof].map((b) => b.node());
}

function readEntities(r) {
  const fp = new Bytes(18);
  const boundary = u8c();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    fp.put(r.buf, r.take(18), 18);
    boundary.push(r.u8());
  }
  return [fp, boundary].map((b) => b.node());
}

function readElements(r) {
  const c = elementColumns();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) readElement(r, c);
  return c.all.map((b) => b.node());
}

function readKeyframes(r) {
  const at = f64c();
  const els = sortedColumn();
  const rows = r.uv();
  let prev = 0;
  for (let i = 0; i < rows; i++) {
    prev += r.uv();
    at.push(prev);
    els.read(r);
  }
  return [at, els.list].map((b) => b.node());
}

function readDeltas(r) {
  const at = f64c();
  const element = u32c();
  const op = u8c();
  const reason = u8c();
  const rows = r.uv();
  let prev = 0;
  for (let i = 0; i < rows; i++) {
    prev += r.uv();
    at.push(prev);
    element.push(r.uv());
    op.push(r.u8());
    reason.push(r.u8());
  }
  return [at, element, op, reason].map((b) => b.node());
}

function readQuarantine(r) {
  const entity = u32c();
  const layer = u8c();
  const coll = collisionColumn();
  const entSt = new Struct([entity, layer, coll.st]);
  const entities = new List(entSt);
  const el = elementColumns();
  const elSt = new Struct(el.all);
  const side = u8c();
  const eLayer = u8c();
  const eColl = collisionColumn();
  const edgeSt = new Struct([elSt, side, eLayer, eColl.st]);
  const edges = new List(edgeSt);
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    const ne = r.uv();
    for (let j = 0; j < ne; j++) {
      entity.push(r.uv());
      layer.push(r.u8());
      coll.read(r);
      entSt.ok();
    }
    entities.add(ne);
    const nedges = r.uv();
    for (let j = 0; j < nedges; j++) {
      readElement(r, el);
      elSt.ok();
      side.push(r.u8());
      eLayer.push(r.u8());
      eColl.read(r);
      edgeSt.ok();
    }
    edges.add(nedges);
  }
  return [entities, edges].map((b) => b.node());
}

function readTruncated(r) {
  const t = truncationColumns();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) t.read(r);
  return t.all.map((b) => b.node());
}

function readUnattributed(r) {
  const seedPod = u32c();
  const subject = u32c();
  const reason = u8c();
  const relation = u16c();
  const target = u16c();
  const spans = spanColumn();
  const candidates = sortedColumn();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    const mask = r.u8();
    if (mask & 1) seedPod.push(r.uv());
    else seedPod.null();
    subject.push(r.uv());
    reason.push(r.u8());
    if (mask & 2) relation.push(r.uv());
    else relation.null();
    if (mask & 4) target.push(r.uv());
    else target.null();
    spans.read(r);
    candidates.read(r);
  }
  return [seedPod, subject, reason, relation, target, spans.list, candidates.list].map((b) => b.node());
}

function readUnmapped(r) {
  const pod = u32c();
  const spans = spanColumn();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    pod.push(r.uv());
    spans.read(r);
  }
  return [pod, spans.list].map((b) => b.node());
}

function readSuppressed(r) {
  const s = suppressedColumns();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) s.read(r);
  return s.all.map((b) => b.node());
}

function readPods(r) {
  const seed = u32c();
  const t = truncationColumns();
  const tSt = new Struct(t.all);
  const kind = u8c();
  const a = u32c();
  const b = u32c();
  const relation = u16c();
  const qSt = new Struct([kind, a, b, relation]);
  const quarantined = new List(qSt);
  const s = suppressedColumns();
  const sSt = new Struct(s.all);
  const suppressed = new List(sSt);
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    seed.push(r.uv());
    t.read(r);
    tSt.ok();
    const nq = r.uv();
    for (let j = 0; j < nq; j++) {
      kind.push(r.u8());
      a.push(r.uv());
      const mask = r.u8();
      if (mask & 1) b.push(r.uv());
      else b.null();
      if (mask & 2) relation.push(r.uv());
      else relation.null();
      qSt.ok();
    }
    quarantined.add(nq);
    const ns = r.uv();
    for (let j = 0; j < ns; j++) {
      s.read(r);
      sSt.ok();
    }
    suppressed.add(ns);
  }
  return [seed, tSt, quarantined, suppressed].map((x) => x.node());
}

function readAnchor(r) {
  const anchor = u8c();
  const judged = u8c();
  const frontier = u32c();
  const rows = r.uv();
  for (let i = 0; i < rows; i++) {
    anchor.push(r.u8());
    judged.push(r.u8());
    frontier.push(r.uv());
  }
  return [anchor, judged, frontier].map((b) => b.node());
}

const SECTIONS = [
  readHeader,
  readEntities,
  readElements,
  readKeyframes,
  readDeltas,
  readQuarantine,
  readTruncated,
  readUnattributed,
  readUnmapped,
  readSuppressed,
  readPods,
  readAnchor,
];

/**
 * Decodes a whole fixture. Gives one table (an array of column nodes) for each of
 * the twelve sections. It throws on a truncated buffer, an overlong uvarint, a
 * section out of order, a section that is not read to its end, and trailing bytes.
 * @param {Uint8Array} buf
 */
export function decode(buf) {
  const tables = [];
  let pos = 0;
  for (let i = 0; i < SECTIONS.length; i++) {
    const head = new Reader(buf, pos, buf.length);
    const tag = head.u8();
    if (tag !== i + 1) throw new Error(`stand-in: section ${i + 1} has the tag ${tag}`);
    const len = head.uv();
    const start = head.pos;
    if (start + len > buf.length) throw new Error('stand-in: truncated section');
    const r = new Reader(buf, start, start + len);
    tables.push(SECTIONS[i](r));
    if (r.pos !== r.end) throw new Error(`stand-in: section ${i + 1} has ${r.end - r.pos} bytes it does not read`);
    pos = start + len;
  }
  if (pos !== buf.length) throw new Error('stand-in: trailing bytes after the last section');
  return tables;
}
