import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import test from 'node:test';

const here = import.meta.dirname;
const workflows = path.join(here, '..', '..', '.github', 'workflows');
const read = (name) => readFileSync(path.join(workflows, name), 'utf8');
const wirenoise = read('wirenoise.yml');

/** The `uses:` lines of a workflow, as {action, ref, comment}. */
function usesOf(text) {
  return [...text.matchAll(/^\s*(?:- )?uses:\s*(\S+)@(\S+)\s*(?:#\s*(.*))?$/gm)].map((m) => ({ action: m[1], ref: m[2], comment: m[3] ?? '', line: m[0].trim() }));
}

test('every action is pinned to a full commit hash that the repository\'s other workflows already use', () => {
  const mine = usesOf(wirenoise);
  assert.ok(mine.length >= 10);
  const known = new Set([...usesOf(read('diag.yml')), ...usesOf(read('ci.yml'))].map((u) => `${u.action}@${u.ref}`));
  for (const u of mine) {
    assert.match(u.ref, /^[0-9a-f]{40}$/, `${u.action} is pinned to a tag or a branch`);
    assert.match(u.comment, /^v\d+\.\d+\.\d+$/, `${u.action} has its tag in a comment`);
    assert.ok(known.has(`${u.action}@${u.ref}`), `${u.action}@${u.ref} is not a pin that diag.yml or ci.yml uses`);
  }
  const names = new Set(mine.map((u) => u.action));
  assert.deepEqual([...names].sort(), ['actions/checkout', 'actions/download-artifact', 'actions/upload-artifact', 'step-security/harden-runner']);
});

test('no expression is written into a script: values reach `run:` only through env', () => {
  const lines = wirenoise.split('\n');
  for (let i = 0; i < lines.length; i++) {
    const m = /^(\s*)(?:- )?run:/.exec(lines[i]);
    if (!m) continue;
    const indent = m[1].length;
    assert.ok(!lines[i].includes('${{'), `line ${i + 1}`);
    for (let j = i + 1; j < lines.length && (lines[j].trim() === '' || lines[j].length - lines[j].trimStart().length > indent); j++) {
      assert.ok(!lines[j].includes('${{'), `an expression in a run script at line ${j + 1}`);
    }
  }
});

test('permissions are none at the top and contents: read in each job; checkouts keep no credentials', () => {
  assert.match(wirenoise, /^permissions: \{\}$/m);
  const jobs = wirenoise.slice(wirenoise.indexOf('\njobs:'));
  const names = [...jobs.matchAll(/^ {2}([a-z-]+):$/gm)].map((m) => m[1]);
  assert.deepEqual(names, ['setup', 'measure', 'summary', 'dry-run']);
  assert.equal([...jobs.matchAll(/^ {4}permissions:\n {6}contents: read$/gm)].length, 4);
  const checkouts = usesOf(wirenoise).filter((u) => u.action === 'actions/checkout').length;
  assert.equal([...wirenoise.matchAll(/persist-credentials: false/g)].length, checkouts);
  assert.ok(!/\bcache\b.*:/.test(usesOf(wirenoise).map((u) => u.action).join('\n')), 'no cache action');
});

test('jobs are named wirenoise ..., the measured ones wait for approval, the dry run does not', () => {
  for (const name of ['setup', 'measure', 'summary', 'dry run']) assert.match(wirenoise, new RegExp(`name: wirenoise ${name}\\n`));
  const setup = wirenoise.slice(wirenoise.indexOf('  setup:'), wirenoise.indexOf('  measure:'));
  assert.match(setup, /environment: bench-approval/);
  assert.match(setup, /if: github\.event_name == 'workflow_dispatch'/);
  const dry = wirenoise.slice(wirenoise.indexOf('  dry-run:'));
  assert.match(dry, /if: github\.event_name == 'pull_request'/);
  assert.ok(!dry.includes('environment: bench-approval'));
  assert.ok(!dry.includes('upload-artifact'), 'the dry run keeps nothing');
  // every job that runs the pinned Node runs it on the arm64 runner; the setup job runs no Node
  for (const job of ['measure', 'summary', 'dry-run']) {
    const from = wirenoise.indexOf(`  ${job}:\n`);
    assert.ok(from > 0);
    assert.match(wirenoise.slice(from, from + 400), /runs-on: ubuntu-24\.04-arm\n/, job);
  }
  assert.ok(!wirenoise.slice(wirenoise.indexOf('  measure:')).includes('environment:'), 'only setup waits for approval');
});

test('the dispatch is bound to a freeze tag and to the commit it names, and its inputs are validated', () => {
  assert.ok(wirenoise.includes("re='^refs/tags/gate-freeze-[0-9]{8}$'"));
  assert.ok(wirenoise.includes('[[ $REF =~ $re ]] || fail'), 'the ref is matched against the tag expression');
  assert.ok(wirenoise.includes('[[ $EXPECTED_SHA =~ $hex40 ]] || fail'));
  assert.ok(wirenoise.includes('[[ $SHA =~ $hex40 ]] || fail'));
  assert.ok(wirenoise.includes('[[ $JOBS =~ $re ]] || fail'));
  assert.ok(wirenoise.includes('[[ $PROCESSES =~ $re ]] || fail'));
  assert.ok(wirenoise.includes('[ "$EXPECTED_SHA" = "$SHA" ] || fail'));
  assert.ok(wirenoise.includes('git merge-base --is-ancestor "$SHA" origin/main 2>/dev/null || fail'));
  assert.ok(wirenoise.includes("re='^[1-8]$'"));
  assert.ok(wirenoise.includes("re='^([1-9]|1[0-9]|20)$'"));
  // the summary job reads the same tag back
  assert.ok(wirenoise.includes('TAG: ${{ github.ref_name }}'));
  assert.ok(wirenoise.includes('--tag "$TAG" --dispatches 1 --processes "$PROCESSES"'));
  assert.ok(wirenoise.includes('PROCESSES: ${{ needs.setup.outputs.processes }}'));
  assert.match(wirenoise, /fetch-depth: 0/);
  assert.match(wirenoise, /max-parallel: 8/);
  assert.match(wirenoise, /fail-fast: false/);
  assert.match(wirenoise, /cancel-in-progress: \$\{\{ github\.event_name == 'pull_request' \}\}/);
});

test('install-node.sh downloads the pinned tarball and checks the published hash', () => {
  const sh = readFileSync(path.join(here, 'install-node.sh'), 'utf8');
  assert.match(sh, /version="v24\.21\.0"/);
  assert.match(sh, /tarball="node-\$\{version\}-linux-arm64\.tar\.xz"/);
  assert.match(sh, /sha256="6ad1325edbdb5649c379b75a237147a666c95d4f9ae8d340fef2d1575d289ad2"/);
  assert.match(sh, /sha256sum --check --strict -/);
  assert.match(sh, /curl --fail --silent --show-error --location --retry 3/);
  assert.match(sh, /^set -euo pipefail$/m);
  assert.ok(!/\bnvm\b|setup-node/.test(sh + wirenoise), 'the image\'s Node and nvm are not used');
  assert.match(wirenoise, /npm ci --ignore-scripts --no-audit --no-fund/);
});
