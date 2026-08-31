import { describe, expect, it } from 'bun:test';
import { ENTRY_TYPE_NAMES } from '../systemSchema.js';

/**
 * Frozen entry-type ABI.
 *
 * These numbers are not an implementation detail: they are written into every
 * persisted trace row and into the SQLite sink, and `lmao-core/build.rs`
 * generates the Rust `EntryType` discriminants by INDEX from this same array.
 * A variant that Rust names is protected by the compiler — deleting one fails
 * the build with `no variant named ...`. A variant's NUMBER is not: inserting
 * or removing an entry mid-table renumbers everything after it, every existing
 * trace silently reads as a different event, and nothing complains.
 *
 * So this is a deliberate second copy, and disagreeing with the table is its
 * entire job. Appending is free; changing a line here is an ABI change and
 * should be as loud in review as it is in consequence.
 */
const FROZEN_ENTRY_TYPES: readonly (readonly [number, string])[] = [
  [1, 'span-start'],
  [2, 'span-ok'],
  [3, 'span-err'],
  [4, 'span-exception'],
  [5, 'span-retry'],
  [6, 'trace'],
  [7, 'debug'],
  [8, 'info'],
  [9, 'warn'],
  [10, 'error'],
  [11, 'ff-access'],
  [12, 'ff-usage'],
  [13, 'period-start'],
  [14, 'op-invocations'],
  [15, 'op-errors'],
  [16, 'op-exceptions'],
  [17, 'op-duration-total'],
  [18, 'op-duration-ok'],
  [19, 'op-duration-err'],
  [20, 'op-duration-min'],
  [21, 'op-duration-max'],
  [22, 'buffer-writes'],
  [23, 'buffer-spans'],
  [24, 'buffer-capacity'],
];

describe('entry-type ABI', () => {
  it('keeps slot 0 unused', () => {
    // build.rs asserts this too: the enum starts at 1 so 0 is never a valid row.
    expect(ENTRY_TYPE_NAMES[0]).toBe('');
  });

  for (const [tag, name] of FROZEN_ENTRY_TYPES) {
    it(`keeps ${name} at ${tag}`, () => {
      // Widened deliberately: the table is a const tuple, so its element type is
      // a union of the very literals under test and `toBe` would only accept a
      // member of it — which would make the assertion vacuous.
      const actual: string | undefined = ENTRY_TYPE_NAMES[tag];
      expect(actual).toBe(name);
    });
  }

  it('declares no entry beyond the frozen set without extending it', () => {
    // Appending is allowed; this fails only when the pin was not extended too.
    const declared: number = ENTRY_TYPE_NAMES.length;
    expect(declared).toBe(FROZEN_ENTRY_TYPES.length + 1);
  });
});
