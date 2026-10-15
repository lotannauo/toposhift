import assert from 'node:assert/strict';
import { readdirSync, readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';

import { MANIFEST, MEASURING_FILES, manifestText } from './files.mjs';

const here = import.meta.dirname;
const json = (f) => JSON.parse(readFileSync(path.join(here, f), 'utf8'));

test('FILES.sha256 is what the files give, in the format of sha256sum', () => {
  const text = readFileSync(MANIFEST, 'utf8');
  assert.equal(text, manifestText());
  const lines = text.trim().split('\n');
  assert.equal(lines.length, MEASURING_FILES.length);
  for (const line of lines) assert.match(line, /^[0-9a-f]{64} {2}\S+$/);
});

test('every file of the directory that is not a test is in FILES.sha256, and nothing else is', () => {
  const skip = new Set(['node_modules', 'FILES.sha256', '.gitignore', '.gitattributes', 'natives.probe.mjs', 'out']);
  const found = [];
  const walk = (dir, rel) => {
    for (const name of readdirSync(dir).sort()) {
      if (rel === '' && skip.has(name)) continue;
      const p = path.join(dir, name);
      const r = rel === '' ? name : `${rel}/${name}`;
      if (statSync(p).isDirectory()) walk(p, r);
      else if (!name.endsWith('.test.mjs')) found.push(r);
    }
  };
  walk(here, '');
  assert.deepEqual(found.sort(), [...MEASURING_FILES].sort());
});

test('the pins: exact versions in package.json, the lockfile agrees, every package is resolved from the registry with an integrity', () => {
  const pkg = json('package.json');
  assert.deepEqual(pkg.dependencies, { '@uwdata/flechette': '2.5.0', 'apache-arrow': '21.2.0' });
  assert.equal(pkg.private, true);
  assert.equal(pkg.type, 'module');
  const lock = json('package-lock.json');
  assert.equal(lock.lockfileVersion, 3);
  assert.equal(lock.packages['node_modules/@uwdata/flechette'].version, '2.5.0');
  assert.equal(lock.packages['node_modules/apache-arrow'].version, '21.2.0');
  const names = Object.keys(lock.packages).filter((k) => k !== '').sort();
  assert.deepEqual(names, [
    'node_modules/@types/node',
    'node_modules/@uwdata/flechette',
    'node_modules/apache-arrow',
    'node_modules/flatbuffers',
    'node_modules/json-with-bigint',
    'node_modules/tslib',
    'node_modules/undici-types',
  ]);
  for (const k of names) {
    const p = lock.packages[k];
    assert.match(p.resolved, /^https:\/\/registry\.npmjs\.org\//, k);
    assert.match(p.integrity, /^sha512-/, k);
    assert.ok(!p.hasInstallScript, `${k} has an install script`);
  }
});

test('every installed package has a licence on the allow-list', () => {
  const allowed = new Set(['MIT', 'ISC', 'BSD-2-Clause', 'BSD-3-Clause', 'Apache-2.0', '0BSD']);
  const lock = json('package-lock.json');
  for (const [k, p] of Object.entries(lock.packages)) {
    if (k === '') continue;
    assert.ok(allowed.has(p.license), `${k}: ${p.license}`);
  }
});
