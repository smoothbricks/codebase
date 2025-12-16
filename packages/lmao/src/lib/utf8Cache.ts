/**
 * SIEVE-based cache for UTF-8 string encoding
 *
 * Per string storage design specs:
 * - Category/text columns store raw JS strings in hot path (zero cost)
 * - UTF-8 encoding happens in cold path (Arrow conversion)
 * - Frequent strings benefit from caching to avoid re-encoding
 *
 * Uses SIEVE algorithm (NSDI'24) which is simpler AND better than LRU
 * for web workloads. SIEVE achieves ~9% lower miss ratio than LRU-K,
 * ARC, and 2Q while being much simpler to implement.
 *
 * @see https://junchengyang.com/publication/nsdi24-SIEVE.pdf
 */

import { SieveCache } from '@neophi/sieve-cache';

/**
 * Default cache size for UTF-8 encoding
 *
 * 4096 entries is a reasonable default that:
 * - Handles typical category cardinality (user IDs, session IDs, etc.)
 * - Bounds memory usage (~4KB keys + encoded values)
 * - Provides good hit rate for repetitive strings
 */
const DEFAULT_UTF8_CACHE_SIZE = 4096;

/**
 * UTF-8 encoding cache using SIEVE eviction algorithm
 *
 * This cache is used during Arrow conversion to avoid re-encoding
 * strings that appear multiple times across buffers or conversions.
 *
 * Thread-safety: JavaScript is single-threaded, so no locks needed.
 * The cache is safe to use in async contexts since JS awaits don't
 * allow concurrent access to the same cache instance.
 */
export class Utf8Cache {
  private readonly cache: SieveCache<string, Uint8Array>;
  private readonly encoder = new TextEncoder();

  // Stats for debugging/monitoring
  private hits = 0;
  private misses = 0;

  constructor(maxSize = DEFAULT_UTF8_CACHE_SIZE) {
    this.cache = new SieveCache<string, Uint8Array>(maxSize);
  }

  /**
   * Get UTF-8 encoded bytes for a string, using cache if available
   *
   * @param str - String to encode
   * @returns UTF-8 encoded bytes
   */
  encode(str: string): Uint8Array {
    const cached = this.cache.get(str);
    if (cached !== undefined) {
      this.hits++;
      return cached;
    }

    this.misses++;
    const encoded = this.encoder.encode(str);
    this.cache.set(str, encoded);
    return encoded;
  }

  /**
   * Encode multiple strings, returning concatenated bytes and offsets
   *
   * This is optimized for Arrow dictionary construction where we need
   * both the concatenated bytes and the offset array.
   *
   * @param strings - Array of strings to encode
   * @returns Concatenated UTF-8 bytes and Arrow-format offsets
   */
  encodeMany(strings: readonly string[]): { data: Uint8Array; offsets: Int32Array } {
    // First pass: encode all strings and calculate total size
    const encoded: Uint8Array[] = new Array(strings.length);
    let totalSize = 0;

    for (let i = 0; i < strings.length; i++) {
      encoded[i] = this.encode(strings[i]);
      totalSize += encoded[i].length;
    }

    // Build offsets array (Arrow format: length = strings.length + 1)
    const offsets = new Int32Array(strings.length + 1);
    offsets[0] = 0;
    for (let i = 0; i < encoded.length; i++) {
      offsets[i + 1] = offsets[i] + encoded[i].length;
    }

    // Concatenate all encoded bytes
    const data = new Uint8Array(totalSize);
    let offset = 0;
    for (const bytes of encoded) {
      data.set(bytes, offset);
      offset += bytes.length;
    }

    return { data, offsets };
  }

  /**
   * Get cache statistics
   */
  get stats(): { hits: number; misses: number; hitRate: number; size: number } {
    const total = this.hits + this.misses;
    return {
      hits: this.hits,
      misses: this.misses,
      hitRate: total > 0 ? this.hits / total : 0,
      size: this.cache.size,
    };
  }

  /**
   * Reset statistics (useful for per-conversion tracking)
   */
  resetStats(): void {
    this.hits = 0;
    this.misses = 0;
  }

  /**
   * Clear the cache entirely
   */
  clear(): void {
    this.cache.clear();
    this.hits = 0;
    this.misses = 0;
  }
}

/**
 * Global UTF-8 cache instance
 *
 * Using a global cache provides cross-conversion caching benefits:
 * - Strings that appear in multiple spans/buffers are cached
 * - Warm cache improves performance over time
 *
 * The SIEVE eviction ensures bounded memory even with unbounded inputs.
 */
export const globalUtf8Cache = new Utf8Cache();

/**
 * Create a new UTF-8 cache with custom size
 *
 * Use this when you want a dedicated cache instance (e.g., for testing
 * or for isolation between different conversion contexts).
 *
 * @param maxSize - Maximum number of entries to cache
 * @returns New Utf8Cache instance
 */
export function createUtf8Cache(maxSize?: number): Utf8Cache {
  return new Utf8Cache(maxSize);
}
