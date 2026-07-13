import { afterEach, describe, expect, it, spyOn } from 'bun:test';

import { _resetThreadId, copyThreadIdTo, getThreadId } from '../threadId.js';

const THREAD_ID_BYTES = Uint8Array.of(0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef);
const THREAD_ID_BIGINT = 0xefcdab8967452301n;

afterEach(() => {
  _resetThreadId();
});

describe('thread ID initialization', () => {
  it('initializes once and copies the singleton bytes without aliasing', () => {
    const getRandomValues = spyOn(crypto, 'getRandomValues').mockImplementation((array: Uint8Array<ArrayBuffer>) => {
      array.set(THREAD_ID_BYTES);
      return array;
    });
    _resetThreadId();

    try {
      const firstCopy = new Uint8Array(12).fill(0xaa);

      copyThreadIdTo(firstCopy, 2);
      firstCopy[2] = 0;

      const secondCopy = new Uint8Array(THREAD_ID_BYTES.length);
      copyThreadIdTo(secondCopy, 0);

      expect(getRandomValues).toHaveBeenCalledTimes(1);
      expect(Array.from(firstCopy)).toEqual([0xaa, 0xaa, 0, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xaa, 0xaa]);
      expect(secondCopy).toEqual(THREAD_ID_BYTES);
      expect(getThreadId()).toBe(THREAD_ID_BIGINT);
    } finally {
      getRandomValues.mockRestore();
    }
  });

  it('retries entropy initialization after a provider failure instead of caching zeroes', () => {
    let attempts = 0;
    const getRandomValues = spyOn(crypto, 'getRandomValues').mockImplementation((array: Uint8Array<ArrayBuffer>) => {
      attempts++;
      if (attempts === 1) throw new Error('entropy temporarily unavailable');
      array.set(THREAD_ID_BYTES);
      return array;
    });
    _resetThreadId();

    try {
      expect(() => getThreadId()).toThrow('entropy temporarily unavailable');

      const copied = new Uint8Array(THREAD_ID_BYTES.length);
      copyThreadIdTo(copied, 0);

      expect(getRandomValues).toHaveBeenCalledTimes(2);
      expect(copied).toEqual(THREAD_ID_BYTES);
      expect(getThreadId()).toBe(THREAD_ID_BIGINT);
      expect(getThreadId()).not.toBe(0n);
    } finally {
      getRandomValues.mockRestore();
    }
  });
});
