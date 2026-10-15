// Loads a fixture and cuts its Arrow bytes into the twelve streams, outside the
// timed region. The cuts are the byte ranges the generator recorded; a consumer
// that does not have them can find the same cuts by walking the IPC messages to
// each end-of-stream marker (0xFFFFFFFF 0x00000000). However the cuts are found,
// they are made before the clock starts, so the method does not affect any time.

import { readFileSync } from 'node:fs';
import path from 'node:path';

import { FIXTURE_DIR, SECTION_NAMES, SIZES } from './gen.mjs';

/** The end-of-stream marker that closes every stream. */
export const EOS = Uint8Array.of(255, 255, 255, 255, 0, 0, 0, 0);

/** A copy of a file in memory whose first byte is 8-aligned, as typed-array views of IPC buffers want. */
function readAligned(file) {
  const bytes = readFileSync(file);
  const out = new Uint8Array(bytes.length);
  out.set(bytes);
  return out;
}

/**
 * Cuts the concatenated streams into views, by the recorded ranges. The ranges
 * must be the twelve sections in order, contiguous from 0 to the end of the
 * buffer (the last range ends where the buffer does), each starting at a multiple of 8, each ending with the end-of-stream
 * marker.
 * @param {Uint8Array} arrow
 * @param {{name: string, start: number, end: number}[]} sections
 * @returns {Uint8Array[]}
 */
export function splitStreams(arrow, sections) {
  if (sections.length !== SECTION_NAMES.length) throw new Error(`split: ${sections.length} streams, want ${SECTION_NAMES.length}`);
  let at = 0;
  const views = sections.map((s, i) => {
    if (s.name !== SECTION_NAMES[i]) throw new Error(`split: stream ${i} is ${s.name}, want ${SECTION_NAMES[i]}`);
    if (s.start !== at || s.end <= s.start || s.end > arrow.length) throw new Error(`split: the range of ${s.name} does not follow the one before`);
    if (s.start % 8 !== 0) throw new Error(`split: ${s.name} starts at ${s.start}, not a multiple of 8`);
    for (let k = 0; k < EOS.length; k++) {
      if (arrow[s.end - EOS.length + k] !== EOS[k]) throw new Error(`split: ${s.name} does not end with the end-of-stream marker`);
    }
    at = s.end;
    return arrow.subarray(s.start, s.end);
  });
  if (at !== arrow.length) throw new Error(`split: the last range ends at ${at}, the buffer at ${arrow.length}`);
  return views;
}

/**
 * Loads one size: the twelve views, the stand-in bytes and the checksum a full
 * read of them must return.
 * @param {string} size S300, S2k or S10k
 */
export function loadInputs(size, dir = FIXTURE_DIR) {
  const id = SIZES[size].id;
  const arrow = readAligned(path.join(dir, `${id}.arrow`));
  const tsgf = readAligned(path.join(dir, `${id}.tsgf`));
  const meta = JSON.parse(readFileSync(path.join(dir, `${id}.arrow.offsets.json`), 'utf8'));
  return { size, views: splitStreams(arrow, meta.sections), tsgf, expected: meta.checksum, sectionChecksums: meta.section_checksums };
}
