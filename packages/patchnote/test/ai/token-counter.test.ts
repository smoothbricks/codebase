/**
 * Tests for token counting utilities
 */

import { describe, expect, test } from 'bun:test';
import { countTokens, exceedsTokenLimit } from '../../src/ai/token-counter.js';

describe('token-counter', () => {
  describe('countTokens', () => {
    test('should count tokens for simple text', () => {
      const text = 'Hello, world!';
      const tokens = countTokens(text);

      // Should return a reasonable number (actual count will depend on tokenizer)
      expect(tokens).toBeGreaterThan(0);
      expect(tokens).toBeLessThan(text.length); // Tokens should be fewer than characters
    });

    test('should count tokens for longer text', () => {
      const text =
        'The quick brown fox jumps over the lazy dog. This is a longer sentence that should produce more tokens.';
      const tokens = countTokens(text);

      expect(tokens).toBeGreaterThan(10);
      expect(tokens).toBeLessThan(50); // Reasonable upper bound
    });

    test('should handle empty string', () => {
      expect(countTokens('')).toBe(0);
    });

    test('should handle whitespace only', () => {
      const tokens = countTokens('   \n\t  ');
      expect(tokens).toBeGreaterThanOrEqual(0);
    });

    test('should handle code snippets', () => {
      const code = `function hello() {
  return "world";
}`;
      const tokens = countTokens(code);

      expect(tokens).toBeGreaterThan(5);
    });

    test('should handle special characters', () => {
      const special = '!@#$%^&*()_+-=[]{}|;:,.<>?';
      const tokens = countTokens(special);

      expect(tokens).toBeGreaterThan(0);
    });

    test('should handle unicode characters', () => {
      const unicode = 'こんにちは世界 🌍 مرحبا';
      const tokens = countTokens(unicode);

      expect(tokens).toBeGreaterThan(0);
    });
  });

  describe('exceedsTokenLimit', () => {
    test('should return true when content exceeds limit', () => {
      const longText = 'word '.repeat(1000);
      expect(exceedsTokenLimit(longText, 100)).toBe(true);
    });

    test('should return false when content is within limit', () => {
      const shortText = 'Hello';
      expect(exceedsTokenLimit(shortText, 100)).toBe(false);
    });

    test('should return false when content equals limit', () => {
      // Find exact token count
      const text = 'test';
      const exactLimit = countTokens(text);
      expect(exceedsTokenLimit(text, exactLimit)).toBe(false);
    });

    test('should return true when content exceeds limit by one', () => {
      const text = 'test test';
      const tokenCount = countTokens(text);
      expect(exceedsTokenLimit(text, tokenCount - 1)).toBe(true);
    });

    test('should handle zero limit', () => {
      expect(exceedsTokenLimit('any text', 0)).toBe(true);
    });

    test('should handle empty text with any limit', () => {
      expect(exceedsTokenLimit('', 0)).toBe(false);
      expect(exceedsTokenLimit('', 100)).toBe(false);
    });
  });

  describe('token counting accuracy', () => {
    test('should be more accurate than character-based estimation', () => {
      // GPT tokenizer is roughly 3-4 characters per token for English
      const text = 'The quick brown fox jumps over the lazy dog';
      const tokens = countTokens(text);
      const characterEstimate = Math.ceil(text.length / 4);

      // Token count should be close to character-based estimate but not identical
      expect(Math.abs(tokens - characterEstimate)).toBeLessThan(10);
    });
  });
});
