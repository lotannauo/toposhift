// The full read: what is done with a decoded table inside the timed region.
// README.md defines it; this file is that definition.
//
// A table is walked column by column. Only the leaf columns are materialised, with
// toArray() on each primitive leaf (unsigned and signed integers, bool,
// fixed_size_binary, utf8, binary). A list column is walked by its offsets array and
// then by its child column; a struct column by its child columns. toArray() is
// never called on a list or a struct, and no row-level object or proxy is made.
//
// A checksum is folded over everything read, so that nothing can be optimised away:
//
//   h starts at 2166136261. Each node, in depth-first order with the columns of a
//   table in schema order, folds a number c into h as h = imul(h ^ c, 16777619) >>> 0:
//     a list      c = the sum of its list lengths (a null list has length 0)
//     a struct    c = its length
//     a leaf      c = the sum of its values, modulo 2^32, where
//                   integers add their value, an int64 or uint64 its low 32 bits
//                   (whether the library gives a BigInt or a number),
//                   bool adds 0 or 1, fixed_size_binary and binary add every byte,
//                   utf8 adds every UTF-16 code unit (the fixtures hold printable
//                   ASCII only, so a code unit is a byte),
//                   a null adds nothing
//   Every null of every node adds 1 to a separate counter n. The checksum of a table
//   is (h + n) >>> 0. The twelve tables of a fixture share one h and one n.
//
// The slot that imports this file gets its own copy of it (the import carries a
// query string), so that the two slots of the measurement share no function.

export const SEED = 2166136261;
const M32 = 4294967296;

export const LEAF = 0;
export const LIST = 1;
export const STRUCT = 2;

export const step = (h, c) => Math.imul(h ^ c, 16777619) >>> 0;

/** Sum of the values of a leaf, as toArray() returns it, modulo 2^32. */
export function sumLeaf(arr) {
  let s = 0;
  if (ArrayBuffer.isView(arr)) {
    const n = arr.length;
    if (arr instanceof BigInt64Array || arr instanceof BigUint64Array) {
      for (let i = 0; i < n; i++) s += Number(BigInt.asUintN(32, arr[i]));
    } else if (arr instanceof Float64Array) {
      for (let i = 0; i < n; i++) s += arr[i] % M32;
    } else {
      for (let i = 0; i < n; i++) s += arr[i];
    }
    return s % M32;
  }
  const n = arr.length;
  for (let i = 0; i < n; i++) {
    const v = arr[i];
    if (v === null) continue;
    switch (typeof v) {
      case 'number':
        s += v % M32;
        break;
      case 'bigint':
        s += Number(BigInt.asUintN(32, v));
        break;
      case 'boolean':
        if (v) s += 1;
        break;
      case 'string':
        for (let j = 0; j < v.length; j++) s += v.charCodeAt(j);
        break;
      default:
        for (let j = 0; j < v.length; j++) s += v[j];
    }
  }
  return s % M32;
}

function walk(ad, node, st) {
  st.nulls += ad.nullCount(node);
  switch (ad.kind(node)) {
    case LIST: {
      const off = ad.offsets(node);
      const n = ad.length(node);
      let total = 0;
      for (let i = 0; i < n; i++) total += off[i + 1] - off[i];
      st.h = step(st.h, total % M32);
      walk(ad, ad.children(node)[0], st);
      break;
    }
    case STRUCT: {
      st.h = step(st.h, ad.length(node));
      const kids = ad.children(node);
      for (let i = 0; i < kids.length; i++) walk(ad, kids[i], st);
      break;
    }
    default:
      st.h = step(st.h, sumLeaf(ad.leaf(node)));
  }
}

/** Walks one table into st. */
export function readTable(ad, table, st) {
  const cols = ad.columns(table);
  for (let i = 0; i < cols.length; i++) walk(ad, cols[i], st);
}

/** The checksum of a list of tables read together (the twelve sections of a fixture). */
export function fullRead(ad, tables) {
  const st = { h: SEED, nulls: 0 };
  for (let i = 0; i < tables.length; i++) readTable(ad, tables[i], st);
  return (st.h + st.nulls) >>> 0;
}

/** The checksum of one table alone. */
export function fullReadOne(ad, table) {
  return fullRead(ad, [table]);
}

// ---------------------------------------------------------------------------
// The adapters: how each library's public API gives the columns, the offsets of a
// list, the children of a list or a struct, and a leaf's array.

/** apache-arrow: a Table gives its columns as Vectors. */
export function arrowAdapter(lib) {
  const { List, Struct } = lib.Type;
  return {
    columns(table) {
      const out = new Array(table.numCols);
      for (let i = 0; i < out.length; i++) out[i] = table.getChildAt(i);
      return out;
    },
    kind(v) {
      const id = v.type.typeId;
      return id === List ? LIST : id === Struct ? STRUCT : LEAF;
    },
    length: (v) => v.length,
    nullCount: (v) => v.nullCount,
    offsets(v) {
      if (v.data.length !== 1) throw new Error('fullread: a list of more than one chunk');
      return v.data[0].valueOffsets;
    },
    children(v) {
      const n = v.numChildren;
      const out = new Array(n);
      for (let i = 0; i < n; i++) out[i] = v.getChildAt(i);
      return out;
    },
    leaf: (v) => v.toArray(),
  };
}

/**
 * Flechette: a Table gives its columns, a Column its record batches, and a batch its
 * offsets and its child batches. A leaf batch is read with toArray() of a Column
 * over that one batch.
 */
export function flechetteAdapter(lib) {
  const { List, Struct } = lib.Type;
  const { Column } = lib;
  return {
    columns(table) {
      const out = new Array(table.numCols);
      for (let i = 0; i < out.length; i++) {
        const col = table.getChildAt(i);
        if (col.data.length !== 1) throw new Error('fullread: a column of more than one batch');
        out[i] = col.data[0];
      }
      return out;
    },
    kind(b) {
      const id = b.type.typeId;
      return id === List ? LIST : id === Struct ? STRUCT : LEAF;
    },
    length: (b) => b.length,
    nullCount: (b) => b.nullCount,
    offsets: (b) => b.offsets,
    children: (b) => b.children,
    leaf: (b) => new Column([b]).toArray(),
  };
}

/** The stand-in reader's nodes already have the shape the walk wants. */
export function standInAdapter() {
  return {
    columns: (table) => table,
    kind: (n) => n.kind,
    length: (n) => n.length,
    nullCount: (n) => n.nullCount,
    offsets: (n) => n.offsets,
    children: (n) => n.children,
    leaf: (n) => n.values,
  };
}
