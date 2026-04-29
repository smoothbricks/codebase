import { describe, expect, it } from 'bun:test';
import type { MonorepoPack } from './index.js';
import { runValidatePacks } from './index.js';

const ctx = { root: '/workspace', syncRuntime: false };

describe('monorepo validation pack phases', () => {
  it('runs fix mode in pre-fix, build, post-fix, then validation order', async () => {
    const events: string[] = [];
    const packs: MonorepoPack[] = [
      {
        name: 'pre',
        fixPreBuild() {
          events.push('pre-fix');
        },
        validatePreBuild() {
          events.push('pre-validate');
          return 0;
        },
      },
      {
        name: 'post',
        fixPostBuild() {
          events.push('post-fix');
        },
        validatePostBuild() {
          events.push('post-validate');
          return 0;
        },
      },
    ];

    const failures = await runValidatePacks(
      ctx,
      { fix: true },
      {
        packs,
        runBuild() {
          events.push('build');
          return 0;
        },
      },
    );

    expect(failures).toBe(0);
    expect(events).toEqual(['pre-fix', 'build', 'post-fix', 'pre-validate', 'post-validate']);
  });

  it('runs normal mode in pre-validation, build, then post-validation order', async () => {
    const events: string[] = [];
    const packs: MonorepoPack[] = [
      {
        name: 'pre',
        validatePreBuild() {
          events.push('pre-validate');
          return 0;
        },
      },
      {
        name: 'post',
        validatePostBuild() {
          events.push('post-validate');
          return 0;
        },
      },
    ];

    const failures = await runValidatePacks(
      ctx,
      {},
      {
        packs,
        runBuild() {
          events.push('build');
          return 0;
        },
      },
    );

    expect(failures).toBe(0);
    expect(events).toEqual(['pre-validate', 'build', 'post-validate']);
  });

  it('does not build or run post-build validation after a fail-fast pre-build failure', async () => {
    const events: string[] = [];
    const packs: MonorepoPack[] = [
      {
        name: 'pre',
        validatePreBuild() {
          events.push('pre-validate');
          return 2;
        },
      },
      {
        name: 'post',
        validatePostBuild() {
          events.push('post-validate');
          return 0;
        },
      },
    ];

    const failures = await runValidatePacks(
      ctx,
      { failFast: true },
      {
        packs,
        runBuild() {
          events.push('build');
          return 0;
        },
      },
    );

    expect(failures).toBe(2);
    expect(events).toEqual(['pre-validate']);
  });

  it('does not run post-build fixers or validation after a build failure', async () => {
    const events: string[] = [];
    const packs: MonorepoPack[] = [
      {
        name: 'pre',
        fixPreBuild() {
          events.push('pre-fix');
        },
        validatePreBuild() {
          events.push('pre-validate');
          return 0;
        },
      },
      {
        name: 'post',
        fixPostBuild() {
          events.push('post-fix');
        },
        validatePostBuild() {
          events.push('post-validate');
          return 0;
        },
      },
    ];

    const failures = await runValidatePacks(
      ctx,
      { fix: true },
      {
        packs,
        runBuild() {
          events.push('build');
          return 1;
        },
      },
    );

    expect(failures).toBe(1);
    expect(events).toEqual(['pre-fix', 'build']);
  });
});
