/**
 * Unit tests for filterUpdates()
 * Tests exclude/include filtering with micromatch glob patterns
 */

import { describe, expect, test } from 'bun:test';
import { filterUpdates } from '../src/filters.js';
import type { Logger } from '../src/logger.js';
import type { FilterConfig, PackageUpdate } from '../src/types.js';

/** Helper: create a minimal PackageUpdate fixture */
function makeUpdate(name: string, overrides?: Partial<PackageUpdate>): PackageUpdate {
  return {
    name,
    fromVersion: '1.0.0',
    toVersion: '1.1.0',
    updateType: 'minor',
    ecosystem: 'npm',
    ...overrides,
  };
}

/** Helper: create a mock logger that captures messages */
function createMockLogger(): { logger: Logger; messages: string[] } {
  const messages: string[] = [];
  const logger: Logger = {
    debug: (msg: string) => messages.push(`debug: ${msg}`),
    info: (msg: string) => messages.push(`info: ${msg}`),
    warn: (msg: string) => messages.push(`warn: ${msg}`),
    error: (msg: string) => messages.push(`error: ${msg}`),
  };
  return { logger, messages };
}

const sampleUpdates: PackageUpdate[] = [
  makeUpdate('react', { fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major' }),
  makeUpdate('react-dom', { fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major' }),
  makeUpdate('vite', { fromVersion: '5.0.0', toVersion: '5.1.0', updateType: 'minor' }),
  makeUpdate('vitest', { fromVersion: '1.0.0', toVersion: '1.1.0', updateType: 'minor' }),
  makeUpdate('@types/react', { fromVersion: '18.0.0', toVersion: '19.0.0', updateType: 'major' }),
  makeUpdate('@types/node', { fromVersion: '20.0.0', toVersion: '22.0.0', updateType: 'major' }),
  makeUpdate('@biomejs/biome', { fromVersion: '1.0.0', toVersion: '2.0.0', updateType: 'major' }),
  makeUpdate('typescript', { fromVersion: '5.0.0', toVersion: '5.3.0', updateType: 'minor' }),
];

describe('filterUpdates', () => {
  describe('empty/undefined filters', () => {
    test('returns all updates when filters is undefined', () => {
      const result = filterUpdates(sampleUpdates, undefined);
      expect(result).toEqual(sampleUpdates);
    });

    test('returns all updates when both exclude and include are empty', () => {
      const result = filterUpdates(sampleUpdates, { exclude: [], include: [] });
      expect(result).toEqual(sampleUpdates);
    });

    test('returns all updates when filter config has no arrays', () => {
      const result = filterUpdates(sampleUpdates, {});
      expect(result).toEqual(sampleUpdates);
    });
  });

  describe('exclude patterns', () => {
    test('excludes exact package name', () => {
      const result = filterUpdates(sampleUpdates, { exclude: ['react'] });
      expect(result.map((u) => u.name)).not.toContain('react');
      expect(result).toHaveLength(sampleUpdates.length - 1);
    });

    test('excludes scoped packages with glob', () => {
      const result = filterUpdates(sampleUpdates, { exclude: ['@types/*'] });
      const names = result.map((u) => u.name);
      expect(names).not.toContain('@types/react');
      expect(names).not.toContain('@types/node');
      expect(result).toHaveLength(sampleUpdates.length - 2);
    });

    test('excludes multiple exact names', () => {
      const result = filterUpdates(sampleUpdates, { exclude: ['react', 'react-dom', 'typescript'] });
      const names = result.map((u) => u.name);
      expect(names).not.toContain('react');
      expect(names).not.toContain('react-dom');
      expect(names).not.toContain('typescript');
      expect(result).toHaveLength(sampleUpdates.length - 3);
    });

    test('brace expansion removes multiple packages', () => {
      const result = filterUpdates(sampleUpdates, { exclude: ['{react,react-dom}'] });
      const names = result.map((u) => u.name);
      expect(names).not.toContain('react');
      expect(names).not.toContain('react-dom');
      expect(result).toHaveLength(sampleUpdates.length - 2);
    });
  });

  describe('include patterns', () => {
    test('keeps only matching packages', () => {
      const result = filterUpdates(sampleUpdates, { include: ['vite', 'vitest'] });
      expect(result).toHaveLength(2);
      expect(result.map((u) => u.name)).toEqual(['vite', 'vitest']);
    });

    test('keeps scoped packages with glob', () => {
      const result = filterUpdates(sampleUpdates, { include: ['@biomejs/*'] });
      expect(result).toHaveLength(1);
      expect(result[0]!.name).toBe('@biomejs/biome');
    });
  });

  describe('precedence: exclude wins over include', () => {
    test('package matching both exclude and include is excluded', () => {
      const result = filterUpdates(sampleUpdates, {
        exclude: ['vite'],
        include: ['vite', 'vitest'],
      });
      const names = result.map((u) => u.name);
      expect(names).not.toContain('vite');
      expect(names).toContain('vitest');
      expect(result).toHaveLength(1);
    });
  });

  describe('logging', () => {
    test('logs excluded packages with reason', () => {
      const { logger, messages } = createMockLogger();
      filterUpdates(sampleUpdates, { exclude: ['react'] }, logger);
      expect(messages).toContain('info: Filtered out (excluded): react');
    });

    test('logs not-in-include-list packages with reason', () => {
      const { logger, messages } = createMockLogger();
      filterUpdates(sampleUpdates, { include: ['vite'] }, logger);
      // All packages except vite should be logged as "not in include list"
      expect(messages.some((m) => m.includes('not in include list'))).toBe(true);
      expect(messages.some((m) => m.includes('react'))).toBe(true);
    });

    test('does not log when no logger provided', () => {
      // Should not throw even without logger
      const result = filterUpdates(sampleUpdates, { exclude: ['react'] });
      expect(result).toHaveLength(sampleUpdates.length - 1);
    });
  });

  describe('downgrades filtering', () => {
    test('filters downgrades array the same way as updates', () => {
      const downgrades: PackageUpdate[] = [
        makeUpdate('react', { fromVersion: '19.0.0', toVersion: '18.0.0' }),
        makeUpdate('vite', { fromVersion: '5.1.0', toVersion: '5.0.0' }),
      ];

      const result = filterUpdates(downgrades, { exclude: ['react'] });
      expect(result).toHaveLength(1);
      expect(result[0]!.name).toBe('vite');
    });
  });
});
