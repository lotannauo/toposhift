// One slot of workload F. The slot module is imported once for each slot and group
// with a query string of its own, so every import is a fresh instance of this file,
// of fullread.mjs (imported below with the same query) and, by a cleared require
// cache, of the library's build file. No function is shared with another slot.

import { createRequire } from 'node:module';
import path from 'node:path';

const query = new URL(import.meta.url).search;
const require = createRequire(path.join(import.meta.dirname, 'x.js'));

/** Evaluates a CommonJS file afresh and leaves no trace of it in the cache. */
function fresh(file) {
  delete require.cache[file];
  const lib = require(file);
  delete require.cache[file];
  return lib;
}

const file = require.resolve('@uwdata/flechette');
if (!file.endsWith(path.join('dist', 'flechette.cjs'))) throw new Error(`slot: flechette resolves to ${file}`);
const lib = fresh(file);
const fr = await import(`./fullread.mjs${query}`);
const adapter = fr.flechetteAdapter(lib);

export const parts = { lib, fullread: fr, adapter };

/**
 * Decodes the twelve streams, then reads them in full. Gives the checksum. int64 is
 * read as BigInt (useBigInt: true): real timestamps are int64 nanoseconds above 2^53,
 * which the default (numbers) refuses, so this is the option a decode of real data
 * must use.
 */
export function decode(inputs) {
  const views = inputs.views;
  const tables = new Array(views.length);
  for (let i = 0; i < views.length; i++) tables[i] = lib.tableFromIPC(views[i], { useBigInt: true });
  return fr.fullRead(adapter, tables);
}
