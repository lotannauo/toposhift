// One slot of workload C. Slot 1 reads with copy A of the stand-in reader and slot 2
// with copy B; see slot_flechette.mjs for how a slot is kept apart from the other.

const url = new URL(import.meta.url);
const query = url.search;
const which = url.searchParams.get('which');
if (which !== '1' && which !== '2') throw new Error(`slot: which is ${which}, want 1 or 2`);

const reader = await import(`./stand_in_${which === '1' ? 'a' : 'b'}.mjs${query}`);
const fr = await import(`./fullread.mjs${query}`);
const adapter = fr.standInAdapter();

export const parts = { reader, fullread: fr, adapter };

/** Decodes the twelve sections, then reads them in full. Gives the checksum. */
export function decode(inputs) {
  return fr.fullRead(adapter, reader.decode(inputs.tsgf));
}
