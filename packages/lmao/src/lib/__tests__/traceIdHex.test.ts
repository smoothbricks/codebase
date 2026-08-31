/**
 * Trace-id hex encoding parity.
 *
 * A 256-entry lookup table is exactly the shape where one wrong entry corrupts
 * one value in 256 and no ordinary test notices: a sampled check passes, ids
 * look plausible, and one byte in 256 renders wrong forever. Every stored trace
 * and every external correlation keys on this string, so the table is verified
 * over the whole 0x00-0xFF domain in every one of the sixteen positions, against
 * the `toString(16).padStart(2, '0')` encoding that shipped before it.
 */

import { describe, expect, it } from 'bun:test';

import { generateTraceId } from '../traceId.js';

/** The encoder the LUT replaced, kept here as the parity oracle. */
function referenceHex(bytes: Uint8Array): string {
  let hex = '';
  for (let i = 0; i < bytes.length; i++) {
    hex += bytes[i].toString(16).padStart(2, '0');
  }
  return hex;
}

/**
 * Run `generateTraceId` against chosen bytes by substituting the source it
 * actually reads. This is the only seam: the byte source is not a parameter.
 */
function traceIdForBytes(bytes: Uint8Array): string {
  const original = crypto.getRandomValues;
  Object.defineProperty(crypto, 'getRandomValues', {
    configurable: true,
    value: (target: Uint8Array) => {
      target.set(bytes);
      return target;
    },
  });
  try {
    return generateTraceId();
  } finally {
    Object.defineProperty(crypto, 'getRandomValues', { configurable: true, value: original });
  }
}

describe('trace-id hex encoding', () => {
  it('is byte-identical to the toString(16) encoder for every byte value in every position', () => {
    // Uniform fills sweep all 256 values through all 16 positions at once, so a
    // single wrong table entry cannot hide in a position the test never reached.
    for (let value = 0; value <= 0xff; value++) {
      const bytes = new Uint8Array(16).fill(value);
      const actual = traceIdForBytes(bytes);
      expect(actual).toBe(referenceHex(bytes));
    }
  });

  it('places each byte value at each of the sixteen positions independently', () => {
    // Uniform fills would still pass if the table were indexed by position
    // rather than by value. Vary one position at a time against a fixed rest.
    for (let position = 0; position < 16; position++) {
      for (let value = 0; value <= 0xff; value++) {
        const bytes = new Uint8Array(16).fill(0x5a);
        bytes[position] = value;
        expect(traceIdForBytes(bytes)).toBe(referenceHex(bytes));
      }
    }
  });

  it('emits exactly 32 lowercase hex characters for every byte value', () => {
    for (let value = 0; value <= 0xff; value++) {
      const id = traceIdForBytes(new Uint8Array(16).fill(value));
      expect(id).toHaveLength(32);
      expect(id).toMatch(/^[0-9a-f]{32}$/);
      // Case matters: an uppercase table would still round-trip through
      // parseInt and still look like a trace id, while breaking every stored
      // id's string equality.
      expect(id).toBe(id.toLowerCase());
    }
  });

  it('keeps the W3C shape for real random bytes', () => {
    for (let i = 0; i < 64; i++) {
      expect(generateTraceId()).toMatch(/^[0-9a-f]{32}$/);
    }
  });
});
