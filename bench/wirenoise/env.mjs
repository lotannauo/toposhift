// The record of the machine and the code a job ran on, written to env.json. Its
// fields are reported, and judged only where summarize.mjs says: the commit, the
// two file hashes, the architecture and the Node version.

import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, readFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';

const here = import.meta.dirname;
export const FILES_PATH = path.join(here, 'FILES.sha256');
export const WORKFLOW_PATH = path.join(here, '..', '..', '.github', 'workflows', 'wirenoise.yml');

/**
 * The environment a trial runs in: the job's, without the variables that change a
 * node process's code or its flags (a compile cache or injected options would
 * carry JIT state or a flag from one process to the next).
 */
export function trialEnv(env = process.env) {
  const out = { ...env };
  delete out.NODE_OPTIONS;
  delete out.NODE_COMPILE_CACHE;
  return out;
}

export const sha256File = (file) => createHash('sha256').update(readFileSync(file)).digest('hex');

/** The CPU implementer and part of each core in /proc/cpuinfo text. */
export function parseCpuInfo(text) {
  const pairs = new Set();
  let implementer = null;
  for (const line of text.split('\n')) {
    const m = /^(CPU implementer|CPU part)\s*:\s*(\S+)/.exec(line);
    if (!m) continue;
    if (m[1] === 'CPU implementer') implementer = m[2];
    else if (implementer !== null) {
      pairs.add(`${implementer}/${m[2]}`);
      implementer = null;
    }
  }
  const distinct = [...pairs].sort();
  return { part: distinct.length === 1 ? distinct[0] : distinct.length === 0 ? 'unknown' : distinct.join('+'), distinct };
}

function tryRun(cmd, args) {
  try {
    return execFileSync(cmd, args, { encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] });
  } catch {
    return '';
  }
}

/**
 * Collects the record. Everything outside the process is read through `src`, so that
 * a test can give it a machine.
 */
export function collectEnv(env = process.env, src = {}) {
  const cpuinfo = src.cpuinfo ?? (existsSync('/proc/cpuinfo') ? readFileSync('/proc/cpuinfo', 'utf8') : '');
  const lscpu = src.lscpu ?? tryRun('lscpu', []);
  const model = /^Model name:\s*(.+)$/m.exec(lscpu);
  const cpu = parseCpuInfo(cpuinfo);
  return {
    schema: 1,
    uname_m: (src.unameM ?? tryRun('uname', ['-m'])).trim(),
    nproc: src.nproc ?? os.availableParallelism(),
    cpu,
    lscpu_model: model ? model[1].trim() : '',
    image_os: env.ImageOS ?? '',
    image_version: env.ImageVersion ?? '',
    image_node: env.IMAGE_NODE ?? '',
    // what a trial sees (always empty: see trialEnv), and what the job's own process had
    node_options: '',
    node_compile_cache: '',
    parent_node_options: env.NODE_OPTIONS ?? '',
    parent_node_compile_cache: env.NODE_COMPILE_CACHE ?? '',
    node_version: process.version,
    v8_version: process.versions.v8,
    github_ref: env.GITHUB_REF ?? '',
    github_sha: env.GITHUB_SHA ?? '',
    expected_sha: env.EXPECTED_SHA ?? '',
    files_sha256: sha256File(src.filesPath ?? FILES_PATH),
    workflow_sha256: sha256File(src.workflowPath ?? WORKFLOW_PATH),
    run_id: env.GITHUB_RUN_ID ?? '',
    run_attempt: env.GITHUB_RUN_ATTEMPT ?? '',
    jobs: env.JOBS ?? '',
    started_at: (src.now ?? new Date()).toISOString(),
  };
}
