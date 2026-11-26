import { describe, expect, it } from 'bun:test';
import { overlappingTsconfigFiles, refuseOverlappingTsconfigFiles, TsconfigOverlapError } from './tsconfig-overlap.js';

describe('overlappingTsconfigFiles', () => {
  it('returns an empty list when either set is empty or the sets are disjoint', () => {
    expect(overlappingTsconfigFiles([], ['/ws/packages/a/src/a.test.ts'])).toEqual([]);
    expect(overlappingTsconfigFiles(['/ws/packages/a/src/index.ts'], [])).toEqual([]);
    expect(
      overlappingTsconfigFiles(
        ['/ws/packages/a/src/index.ts', '/ws/packages/a/src/util.ts'],
        ['/ws/packages/a/src/index.test.ts', '/ws/packages/a/src/test-suite-tracer.ts'],
      ),
    ).toEqual([]);
  });

  it('returns every file included by both configs, deduplicated and sorted', () => {
    expect(
      overlappingTsconfigFiles(
        [
          '/ws/packages/a/src/z.ts',
          '/ws/packages/a/src/test-suite-tracer.ts',
          '/ws/packages/a/src/a.ts',
          '/ws/packages/a/src/a.ts',
        ],
        ['/ws/packages/a/src/index.test.ts', '/ws/packages/a/src/test-suite-tracer.ts', '/ws/packages/a/src/a.ts'],
      ),
    ).toEqual(['/ws/packages/a/src/a.ts', '/ws/packages/a/src/test-suite-tracer.ts']);
  });
});

describe('refuseOverlappingTsconfigFiles', () => {
  it('throws a typed error naming the project, each shared file, both configs and the fix', () => {
    let caught: unknown;
    try {
      refuseOverlappingTsconfigFiles({
        projectName: 'example',
        projectRoot: '/ws/packages/example',
        libConfig: 'tsconfig.lib.json',
        testConfig: 'tsconfig.test.json',
        libFiles: ['/ws/packages/example/src/index.ts', '/ws/packages/example/src/test-suite-tracer.ts'],
        testFiles: ['/ws/packages/example/src/index.test.ts', '/ws/packages/example/src/test-suite-tracer.ts'],
      });
    } catch (error) {
      caught = error;
    }

    expect(caught).toBeInstanceOf(TsconfigOverlapError);
    if (!(caught instanceof TsconfigOverlapError)) {
      throw new Error(`expected TsconfigOverlapError, got ${String(caught)}`);
    }
    expect(caught.projectName).toBe('example');
    expect(caught.libConfig).toBe('tsconfig.lib.json');
    expect(caught.testConfig).toBe('tsconfig.test.json');
    expect(caught.files).toEqual(['src/test-suite-tracer.ts']);
    expect(caught.message).toContain('example');
    expect(caught.message).toContain('src/test-suite-tracer.ts');
    expect(caught.message).toContain('tsconfig.lib.json');
    expect(caught.message).toContain('tsconfig.test.json');
    expect(caught.message).toMatch(/exclude/i);
  });

  it('does not throw when lib and test file sets are disjoint', () => {
    expect(() =>
      refuseOverlappingTsconfigFiles({
        projectName: 'example',
        projectRoot: '/ws/packages/example',
        libConfig: 'tsconfig.lib.json',
        testConfig: 'tsconfig.test.json',
        libFiles: ['/ws/packages/example/src/index.ts'],
        testFiles: ['/ws/packages/example/src/index.test.ts'],
      }),
    ).not.toThrow();
  });
});
