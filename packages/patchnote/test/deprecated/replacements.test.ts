/**
 * Unit tests for deprecated package replacement lookup
 * Tests Renovate replacement mapping
 */

import { describe, expect, test } from 'bun:test';
import { findReplacement } from '../../src/deprecated/replacements.js';

describe('findReplacement', () => {
  test('returns suggestion for known package (babel-eslint)', () => {
    const result = findReplacement('babel-eslint');
    expect(result).not.toBeNull();
    expect(result!.replacementName).toBe('@babel/eslint-parser');
    expect(result!.replacementVersion).toBe('7.11.0');
    expect(result!.description).toContain('babel-eslint');
  });

  test('returns null for unknown package', () => {
    const result = findReplacement('nonexistent-package-xyz');
    expect(result).toBeNull();
  });

  test('returns suggestion for scoped package (@hapi/joi)', () => {
    const result = findReplacement('@hapi/joi');
    expect(result).not.toBeNull();
    expect(result!.replacementName).toBeTruthy();
  });
});
