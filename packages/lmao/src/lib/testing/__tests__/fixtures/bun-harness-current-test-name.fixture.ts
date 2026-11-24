import * as bunTest from 'bun:test';
import { expect } from 'bun:test';
import { defineOpContext } from '../../../defineOpContext.js';
import { defineLogSchema } from '../../../schema/defineLogSchema.js';
import { createBunTestMock, currentTestName, initTraceTestRun } from '../../bun-harness.js';

/**
 * A traced Bun run on the global singleton `_als` path (`initTraceTestRun` + `createBunTestMock`), run without a
 * bunfig preload so `_activeSuiteTracer` stays null. Checks `currentTestName()` before init, outside a test, at top
 * level, and inside nested describes.
 */
const beforeInit = currentTestName();

initTraceTestRun(defineOpContext({ logSchema: defineLogSchema({}) }));
const { describe, it } = createBunTestMock(bunTest);
const outsideTest = currentTestName();

it('global path top level', () => {
  expect(beforeInit).toBeUndefined();
  expect(outsideTest).toBeUndefined();
  expect(currentTestName()).toBe('global path top level');
});

describe('global outer', () => {
  describe('global inner', () => {
    it('global leaf', async () => {
      expect(currentTestName()).toBe('global outer > global inner > global leaf');
      await Promise.resolve();
      expect(currentTestName()).toBe('global outer > global inner > global leaf');
    });
  });
});
