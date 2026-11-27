import { describe, expect, it } from 'bun:test';
import { overlappingTsconfigFiles, tsconfigOverlap } from './tsconfig-overlap.js';

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

describe('tsconfigOverlap', () => {
  it('names the project, each shared file, both configs and the fix', () => {
    const overlap = tsconfigOverlap({
      projectName: 'example',
      projectRoot: '/ws/packages/example',
      libConfig: 'tsconfig.lib.json',
      testConfig: 'tsconfig.test.json',
      libFiles: ['/ws/packages/example/src/index.ts', '/ws/packages/example/src/test-suite-tracer.ts'],
      testFiles: ['/ws/packages/example/src/index.test.ts', '/ws/packages/example/src/test-suite-tracer.ts'],
    });

    expect(overlap?.projectName).toBe('example');
    expect(overlap?.libConfig).toBe('tsconfig.lib.json');
    expect(overlap?.testConfig).toBe('tsconfig.test.json');
    expect(overlap?.files).toEqual(['src/test-suite-tracer.ts']);
    expect(overlap?.message).toContain('example');
    expect(overlap?.message).toContain('src/test-suite-tracer.ts');
    expect(overlap?.message).toContain('tsconfig.lib.json');
    expect(overlap?.message).toContain('tsconfig.test.json');
    expect(overlap?.message).toMatch(/exclude/i);
  });

  it('is undefined when lib and test file sets are disjoint', () => {
    expect(
      tsconfigOverlap({
        projectName: 'example',
        projectRoot: '/ws/packages/example',
        libConfig: 'tsconfig.lib.json',
        testConfig: 'tsconfig.test.json',
        libFiles: ['/ws/packages/example/src/index.ts'],
        testFiles: ['/ws/packages/example/src/index.test.ts'],
      }),
    ).toBeUndefined();
  });
});
