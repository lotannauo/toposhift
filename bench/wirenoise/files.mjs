// FILES.sha256: the sha256 of every file that defines the measurement, in the
// format of sha256sum, so that `sha256sum -c FILES.sha256` run in this directory
// checks it. The hash of FILES.sha256 itself and of the workflow file are what the
// registration names, and what env.json records.
//
//   node files.mjs write   writes FILES.sha256
//   node files.mjs check   fails unless FILES.sha256 is what the files give
//
// Tests are not in the list: they decide nothing about a figure.

import { createHash } from 'node:crypto';
import { readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';

import { SIZES } from './gen.mjs';

const here = import.meta.dirname;
export const MANIFEST = path.join(here, 'FILES.sha256');

const FIXTURES = Object.values(SIZES).flatMap(({ id }) => [`fixtures/${id}.arrow`, `fixtures/${id}.arrow.offsets.json`, `fixtures/${id}.tsgf`]);

/** The files of the measurement, relative to this directory, in order. */
export const MEASURING_FILES = [
  'README.md',
  'package.json',
  'package-lock.json',
  'install-node.sh',
  'types.mjs',
  'gen.mjs',
  'split.mjs',
  'fullread.mjs',
  'stand_in_a.mjs',
  'stand_in_b.mjs',
  'slots.mjs',
  'slot_flechette.mjs',
  'slot_arrow.mjs',
  'slot_standin.mjs',
  'trial.mjs',
  'env.mjs',
  'run.mjs',
  'independence.mjs',
  'summarize.mjs',
  'files.mjs',
  ...FIXTURES,
];

/** The text FILES.sha256 must have. */
export function manifestText(dir = here) {
  return MEASURING_FILES.map((f) => `${createHash('sha256').update(readFileSync(path.join(dir, f))).digest('hex')}  ${f}\n`).join('');
}

function main(mode) {
  if (mode === 'write') {
    writeFileSync(MANIFEST, manifestText());
    console.log(`files: wrote ${MEASURING_FILES.length} lines into FILES.sha256`);
    return 0;
  }
  if (mode === 'check') {
    if (readFileSync(MANIFEST, 'utf8') !== manifestText()) {
      console.error('files: FILES.sha256 is not what the files give');
      return 1;
    }
    console.log(`files: FILES.sha256 matches ${MEASURING_FILES.length} files`);
    return 0;
  }
  console.error('usage: node files.mjs write|check');
  return 2;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv[2]);
}
