// A small notation for the Arrow types of the twelve sections, shared by the
// generator, which builds the streams from it, and by the tests, which read the
// schemas of the written streams back into the same notation.
//
//   column := name ":" type [ "?" ]            a "?" marks a nullable field
//   type   := u8 | u16 | u32 | u64 | i64 | bool | utf8 | bin | fsb<N>
//           | list<type>                        the item is never nullable
//           | struct<column, column, ...>
//
// A list item is a field named "item" that is not nullable, as the writer of
// bytesgate.Arrow gives it (arrow.ListOfNonNullable).

const LEAVES = new Set(['u8', 'u16', 'u32', 'u64', 'i64', 'bool', 'utf8', 'bin']);

/**
 * Parses one column, "name:type" or "name:type?".
 * @param {string} text
 * @returns {{name: string, type: object, nullable: boolean}}
 */
export function parseColumn(text) {
  const p = { s: text, i: 0 };
  const col = column(p);
  if (p.i !== p.s.length) throw new Error(`types: trailing text at ${p.i} in "${text}"`);
  return col;
}

function column(p) {
  const colon = p.s.indexOf(':', p.i);
  if (colon < 0) throw new Error(`types: no ":" in "${p.s.slice(p.i)}"`);
  const name = p.s.slice(p.i, colon);
  if (!/^[a-z_][a-z0-9_]*$/.test(name)) throw new Error(`types: bad column name "${name}"`);
  p.i = colon + 1;
  const type = parseType(p);
  let nullable = false;
  if (p.s[p.i] === '?') {
    nullable = true;
    p.i++;
  }
  return { name, type, nullable };
}

function parseType(p) {
  const rest = p.s.slice(p.i);
  if (rest.startsWith('list<')) {
    p.i += 5;
    const item = parseType(p);
    expect(p, '>');
    return { k: 'list', item };
  }
  if (rest.startsWith('struct<')) {
    p.i += 7;
    const fields = [column(p)];
    while (p.s[p.i] === ',') {
      p.i++;
      fields.push(column(p));
    }
    expect(p, '>');
    return { k: 'struct', fields };
  }
  const m = /^(fsb[0-9]+|[a-z0-9]+)/.exec(rest);
  if (!m) throw new Error(`types: no type at "${rest}"`);
  p.i += m[0].length;
  if (m[0].startsWith('fsb')) return { k: 'fsb', n: Number(m[0].slice(3)) };
  if (!LEAVES.has(m[0])) throw new Error(`types: unknown type "${m[0]}"`);
  return { k: m[0] };
}

function expect(p, ch) {
  if (p.s[p.i] !== ch) throw new Error(`types: expected "${ch}" at ${p.i} in "${p.s}"`);
  p.i++;
}

/** Writes a type back in the notation. */
export function typeText(t) {
  switch (t.k) {
    case 'list':
      return `list<${typeText(t.item)}>`;
    case 'struct':
      return `struct<${t.fields.map(columnText).join(',')}>`;
    case 'fsb':
      return `fsb${t.n}`;
    default:
      return t.k;
  }
}

/** Writes a column back in the notation. */
export function columnText(c) {
  return `${c.name}:${typeText(c.type)}${c.nullable ? '?' : ''}`;
}
