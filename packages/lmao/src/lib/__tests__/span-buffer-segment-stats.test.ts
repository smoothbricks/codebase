import { beforeEach, describe, expect, it } from 'bun:test';
import fc from 'fast-check';
import { defineOpContext } from '../defineOpContext.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import { getSpanBufferClass } from '../spanBuffer.js';
import { TestTracer } from '../tracers/TestTracer.js';
import type { AnySpanBuffer } from '../types.js';

import { createTestTracerOptions } from './test-helpers.js';

const opContext = defineOpContext({
  logSchema: new LogSchema({
    testField: S.category(),
  }),
});

function requireBuffer(buffer: AnySpanBuffer | undefined, label: string): AnySpanBuffer {
  if (!buffer) {
    throw new Error(`Expected ${label} buffer`);
  }
  return buffer;
}

function writeIndices(first: AnySpanBuffer): number[] {
  const indices: number[] = [];
  let segment: AnySpanBuffer | undefined = first;
  while (segment) {
    indices.push(segment._writeIndex);
    segment = segment._overflow;
  }
  return indices;
}

function accountedRows(first: AnySpanBuffer): number {
  let rows = Math.max(0, first._writeIndex - 2);
  let segment = first._overflow;
  while (segment) {
    rows += segment._writeIndex;
    segment = segment._overflow;
  }
  return rows;
}

function fullSegmentRows(capacities: readonly number[], segmentIndex: number): number {
  return capacities[segmentIndex] - (segmentIndex === 0 ? 2 : 0);
}

function totalRowsFor(capacities: readonly number[], tailRows: number): number {
  let rows = tailRows;
  for (let segmentIndex = 0; segmentIndex < capacities.length - 1; segmentIndex++) {
    rows += fullSegmentRows(capacities, segmentIndex);
  }
  return rows;
}

describe('SpanBuffer physical-segment statistics accounting', () => {
  const SpanBufferClass = getSpanBufferClass(opContext.logBinding.logSchema);
  const stats = SpanBufferClass.stats;

  beforeEach(() => {
    stats.capacity = 8;
    stats.totalWrites = 0;
    stats.spansCreated = 0;
  });

  it('accounts full overflow segments immediately and the final segment exactly once on completion', () => {
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    tracer.trace('three-segment-root', (ctx) => {
      for (let row = 0; row < 20; row++) {
        ctx.log.info(`row ${row}`);
      }

      // The first two physical segments are sealed; the six-row tail is still active.
      expect(stats.totalWrites).toBe(14);
      return ctx.ok(null);
    });

    const root = requireBuffer(tracer.rootBuffers[0], 'root');
    expect(writeIndices(root)).toEqual([8, 8, 6]);
    expect(accountedRows(root)).toBe(20);
    expect(stats.totalWrites).toBe(accountedRows(root));
  });

  it('snapshots the just-sealed segment before overflow-triggered reset and tuning', () => {
    const tracer = new TestTracer(opContext, createTestTracerOptions());

    // Put the next overflow above the tuning threshold. The six newly sealed rows
    // must be visible to the snapshot before tuning resets the counters.
    stats.spansCreated = 10;
    stats.totalWrites = 100;

    tracer.trace('snapshot-order', (ctx) => {
      for (let row = 0; row < 7; row++) {
        ctx.log.info(`row ${row}`);
      }
      return ctx.ok(null);
    });

    expect(tracer.statsSnapshots.map((snapshot) => snapshot.totalWrites)).toEqual([106]);
    // Tuning reset the sealed prefix; terminal completion then accounts the one-row tail.
    expect(stats.totalWrites).toBe(1);
  });

  it('accounts root and child overflow chains without lifecycle rows or overlap double-counting', async () => {
    const tracer = new TestTracer(opContext, createTestTracerOptions());
    let root: AnySpanBuffer | undefined;
    let child: AnySpanBuffer | undefined;

    await tracer.trace('root', async (ctx) => {
      root = ctx.buffer;
      for (let row = 0; row < 7; row++) {
        ctx.log.info(`root-before-child ${row}`);
      }
      expect(stats.totalWrites).toBe(6);

      await ctx.span('child', (childCtx) => {
        child = childCtx.buffer;
        for (let row = 0; row < 15; row++) {
          childCtx.log.info(`child ${row}`);
        }
        return childCtx.ok(null);
      });

      // The root's active one-row tail is not complete yet; the child chain is.
      expect(stats.totalWrites).toBe(21);

      for (let row = 0; row < 6; row++) {
        ctx.log.info(`root-after-child ${row}`);
      }
      return ctx.ok(null);
    });

    const completedRoot = requireBuffer(root, 'root');
    const completedChild = requireBuffer(child, 'child');
    expect(writeIndices(completedRoot)).toEqual([8, 7]);
    expect(writeIndices(completedChild)).toEqual([8, 8, 1]);

    const expectedRows = accountedRows(completedRoot) + accountedRows(completedChild);
    expect(expectedRows).toBe(28);
    expect(stats.totalWrites).toBe(expectedRows);
  });

  it('accounts randomized writes across no-overflow, overflow, child, and mixed-capacity segments', async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.oneof(
          fc.tuple(fc.integer({ min: 1, max: 4 }).map((units) => units * 8)),
          fc.uniqueArray(
            fc.integer({ min: 1, max: 4 }).map((units) => units * 8),
            {
              minLength: 2,
              maxLength: 4,
            },
          ),
        ),
        fc.boolean(),
        fc.integer({ min: 0, max: 31 }),
        async (capacities, useChild, tailSeed) => {
          const lastCapacity = capacities[capacities.length - 1];
          if (lastCapacity === undefined) {
            throw new Error('Expected at least one generated capacity');
          }
          const tailRows = capacities.length === 1 ? tailSeed % (lastCapacity - 1) : 1 + (tailSeed % lastCapacity);
          const rowsToWrite = totalRowsFor(capacities, tailRows);

          stats.capacity = capacities[0];
          stats.totalWrites = 0;
          stats.spansCreated = 0;
          const tracer = new TestTracer(opContext, createTestTracerOptions());
          let completed: AnySpanBuffer | undefined;

          await tracer.trace('property-root', async (ctx) => {
            const writeRows = (spanCtx: typeof ctx) => {
              completed = spanCtx.buffer;
              let row = 0;
              for (let segmentIndex = 0; segmentIndex < capacities.length; segmentIndex++) {
                if (segmentIndex > 0) {
                  stats.capacity = capacities[segmentIndex];
                }
                const rowsInSegment =
                  segmentIndex === capacities.length - 1 ? tailRows : fullSegmentRows(capacities, segmentIndex);
                for (let segmentRow = 0; segmentRow < rowsInSegment; segmentRow++) {
                  spanCtx.log.info(`row ${row++}`);
                }
              }
            };

            if (useChild) {
              await ctx.span('property-child', (childCtx) => {
                writeRows(childCtx);
                return childCtx.ok(null);
              });
            } else {
              writeRows(ctx);
            }
            return ctx.ok(null);
          });

          const first = requireBuffer(completed, useChild ? 'child' : 'root');
          const expectedIndices = capacities.map((capacity, segmentIndex) =>
            segmentIndex === capacities.length - 1 ? tailRows + (segmentIndex === 0 ? 2 : 0) : capacity,
          );
          expect(writeIndices(first)).toEqual(expectedIndices);
          expect(accountedRows(first)).toBe(rowsToWrite);
          expect(stats.totalWrites).toBe(rowsToWrite);

          const sealedPrefixTotals: number[] = [];
          let sealedRows = 0;
          for (let segmentIndex = 0; segmentIndex < capacities.length - 1; segmentIndex++) {
            sealedRows += fullSegmentRows(capacities, segmentIndex);
            sealedPrefixTotals.push(sealedRows);
          }
          expect(tracer.statsSnapshots.map((snapshot) => snapshot.totalWrites)).toEqual(sealedPrefixTotals);
        },
      ),
      { numRuns: 80 },
    );
  });
});
