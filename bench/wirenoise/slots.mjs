// Makes the two slots of a group. Each slot is a fresh import of a slot module with
// a query string of its own, so that no function, no library instance and no
// stand-in reader is shared between slots or between groups (see slot_flechette.mjs).

const MODULE = { F: 'slot_flechette.mjs', A: 'slot_arrow.mjs', C: 'slot_standin.mjs' };

let serial = 0;

/**
 * @param {'F'|'A'|'C'} workload
 * @param {1|2} which the slot
 * @returns {Promise<{decode: (inputs: object) => number, parts: object}>}
 */
export async function makeSlot(workload, which) {
  const file = MODULE[workload];
  if (!file) throw new Error(`slots: no workload ${workload}`);
  if (which !== 1 && which !== 2) throw new Error(`slots: no slot ${which}`);
  serial += 1;
  return import(`./${file}?which=${which}&id=${serial}`);
}
