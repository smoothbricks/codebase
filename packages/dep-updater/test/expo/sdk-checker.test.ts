import { describe, expect, test } from 'bun:test';
import { compareVersions } from '../../src/expo/sdk-checker.js';

describe('Expo SDK Checker', () => {
  describe('compareVersions', () => {
    test('returns -1 when first version is lower', () => {
      expect(compareVersions('51.0.0', '52.0.0')).toBe(-1);
      expect(compareVersions('52.0.0', '52.1.0')).toBe(-1);
      expect(compareVersions('52.1.0', '52.1.1')).toBe(-1);
    });

    test('returns 0 when versions are equal', () => {
      expect(compareVersions('52.0.0', '52.0.0')).toBe(0);
      expect(compareVersions('52.1.5', '52.1.5')).toBe(0);
    });

    test('returns 1 when first version is higher', () => {
      expect(compareVersions('53.0.0', '52.0.0')).toBe(1);
      expect(compareVersions('52.2.0', '52.1.0')).toBe(1);
      expect(compareVersions('52.1.2', '52.1.1')).toBe(1);
    });

    test('handles versions with different part counts', () => {
      expect(compareVersions('52.0', '52.0.0')).toBe(0);
      expect(compareVersions('52', '52.0.0')).toBe(0);
      expect(compareVersions('52.1', '52.0.5')).toBe(1);
    });
  });
});
