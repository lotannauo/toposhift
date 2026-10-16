// One slot of workload A. See slot_flechette.mjs for how a slot is kept apart from
// the other one.

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

// The single-file build of the package, next to its entry point.
const file = path.join(path.dirname(require.resolve('apache-arrow')), 'Arrow.esnext.min.js');
const lib = fresh(file);
const fr = await import(`./fullread.mjs${query}`);
const adapter = fr.arrowAdapter(lib);

export const parts = { lib, fullread: fr, adapter };

/** Decodes the twelve streams, then reads them in full. Gives the checksum. */
export function decode(inputs) {
  const views = inputs.views;
  const tables = new Array(views.length);
  for (let i = 0; i < views.length; i++) tables[i] = lib.tableFromIPC(views[i]);
  return fr.fullRead(adapter, tables);
}
