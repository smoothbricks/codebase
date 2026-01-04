import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { shouldTuneCapacity } from '../capacityTuning.js';
import { createSpanLogger } from '../codegen/spanLoggerGenerator.js';
import { DEFAULT_METADATA } from '../opContext/defineOp.js';
import { S } from '../schema/builder.js';
import { LogSchema } from '../schema/LogSchema.js';
import { mergeWithSystemSchema } from '../schema/systemSchema.js';
import { createSpanBuffer, getSpanBufferClass } from '../spanBuffer.js';
import type { SpanBufferStats } from '../spanBufferStats.js';

import { createTestTraceRoot, createTestTracerOptions } from './test-helpers.js';

/**
 * Create mock SpanBufferStats for testing capacity tuning.
 *
 * Per agent-todo/opgroup-refactor.md lines 58-70, 525-547:
 * Stats are on SpanBufferClass.stats (static property), NOT on LogBinding.
 */
function createMockStats(overrides: Partial<SpanBufferStats> = {}): SpanBufferStats {
  return {
    capacity: overrides.capacity ?? DEFAULT_BUFFER_CAPACITY,
    totalWrites: overrides.totalWrites ?? 0,
    overflowWrites: overrides.overflowWrites ?? 0,
    totalCreated: overrides.totalCreated ?? 0,
    overflows: overrides.overflows ?? 0,
  };
}

describe('Capacity Tuning Algorithm', () => {
  let stats: SpanBufferStats;

  beforeEach(() => {
    stats = createMockStats();
  });

  describe('shouldTuneCapacity - Success Cases', () => {
    it('should increase capacity when overflow ratio > 15%', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20; // 20% overflow
      // Start with default capacity
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity * 2); // Doubled
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.overflowWrites).toBe(0); // Reset
    });

    it('should decrease capacity when overflow ratio < 5% with many buffers', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 3; // 3% overflow
      stats.totalCreated = 15; // Many buffers
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4; // Start at 4x default so we can halve

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Halved
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.overflowWrites).toBe(0); // Reset
    });

    it('should double capacity from default to 2x', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 16; // 16% overflow

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should double capacity from 128 to 256', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 16;
      stats.capacity = 128;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(256);
    });

    it('should halve capacity from 4x to 2x default', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should halve capacity from 32 to 16', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;
      stats.capacity = 32;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(16);
    });
  });

  describe('shouldTuneCapacity - Edge Cases', () => {
    it('should not tune with insufficient samples (< 100)', () => {
      stats.totalWrites = 99;
      stats.overflowWrites = 50; // 50% overflow, but not enough samples
      const initialCapacity = stats.capacity;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // Unchanged
    });

    it('should not tune at exactly 100 writes with low overflow', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 10; // 10% overflow (between 5% and 15%)
      const initialCapacity = stats.capacity;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity);
    });

    it('should cap capacity at 1024', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20; // 20% overflow
      stats.capacity = 1024;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024); // Cannot increase beyond 1024
    });

    it('should cap capacity at minimum when decreasing', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY); // Cannot decrease below default
    });

    it('should not decrease without enough buffers (< 10)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalCreated = 9; // Not enough buffers
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);
    });

    it('should handle exactly 15% overflow (boundary)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 15; // Exactly 15%
      const initialCapacity = stats.capacity;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // > 15%, not >= 15%
    });

    it('should handle exactly 5% overflow (boundary)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 5; // Exactly 5%
      stats.totalCreated = 10;
      const initialCapacity = stats.capacity;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // < 5%, not <= 5%
    });

    it('should tune at 15.1% overflow', () => {
      stats.totalWrites = 1000;
      stats.overflowWrites = 151; // 15.1% overflow

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should tune at 4.9% overflow with sufficient buffers', () => {
      stats.totalWrites = 1000;
      stats.overflowWrites = 49; // 4.9% overflow
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4; // Need room to decrease

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should handle zero overflow writes', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 0; // 0% overflow
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4; // Need room to decrease

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Should decrease
    });

    it('should handle all writes overflowing (100%)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 100; // 100% overflow

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Should increase
    });
  });

  describe('shouldTuneCapacity - Capacity Bounds', () => {
    it('should not increase beyond 1024 even with high overflow', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 99; // 99% overflow
      stats.capacity = 1024;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024);
    });

    it('should cap at 1024 when doubling from 512', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      stats.capacity = 512;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024); // Capped at 1024 (max capacity)
    });

    it('should not decrease below minimum', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 0;
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should cap at minimum when halving from 2x', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 2;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });
  });

  describe('Stats Reset After Tuning', () => {
    it('should reset stats after increasing capacity', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      stats.totalCreated = 5;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.overflowWrites).toBe(0);
      expect(stats.totalCreated).toBe(0);
    });

    it('should reset stats after decreasing capacity', () => {
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 2; // Need room to decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.overflowWrites).toBe(0);
      expect(stats.totalCreated).toBe(0);
    });

    it('should not reset stats when no tuning occurs', () => {
      stats.totalWrites = 50; // Not enough samples
      stats.overflowWrites = 10;
      stats.totalCreated = 5;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(50); // Unchanged
      expect(stats.overflowWrites).toBe(10); // Unchanged
      expect(stats.totalCreated).toBe(5); // Unchanged
    });
  });

  describe('Multiple Tuning Cycles', () => {
    it('should handle multiple increases correctly', () => {
      // Start at default
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // First increase
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Second increase (stats were reset after first tuning)
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);

      // Third increase
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 8);
    });

    it('should handle multiple decreases correctly', () => {
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 8; // Start high
      stats.totalCreated = 10;

      // First decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);

      // Second decrease (stats were reset, need to set totalCreated again)
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Third decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should handle increase then decrease', () => {
      // Start at default
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // Increase due to high overflow
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Decrease due to low overflow (stats were reset after tuning)
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalCreated = 10;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });
  });

  describe('Property-Based Testing for Self-Tuning Formulas', () => {
    it('should verify all mathematical invariants for arbitrary inputs', () => {
      fc.assert(
        fc.property(
          fc.record({
            totalWrites: fc.integer({ min: 0, max: 10000 }),
            overflowWrites: fc.integer({ min: 0, max: 5000 }),
            totalCreated: fc.integer({ min: 0, max: 1000 }),
            initialCapacity: fc.constantFrom(8, 16, 32, 64, 128, 256, 512, 1024),
          }),
          ({ totalWrites, overflowWrites, totalCreated, initialCapacity }) => {
            // Set up stats state
            stats.totalWrites = totalWrites;
            stats.overflowWrites = overflowWrites;
            stats.totalCreated = totalCreated;
            stats.capacity = initialCapacity;

            const beforeCapacity = stats.capacity;
            shouldTuneCapacity(stats);
            const afterCapacity = stats.capacity;

            // Core invariants that must always hold
            expect(afterCapacity).toBeGreaterThanOrEqual(8);
            expect(afterCapacity).toBeLessThanOrEqual(1024);
            expect(afterCapacity & (afterCapacity - 1)).toBe(0); // Power of 2

            // Verify exact behavior matches specification
            const overflowRatio = totalWrites > 0 ? overflowWrites / totalWrites : 0;
            const hasEnoughSamples = totalWrites >= 100;
            const hasManyBuffers = totalCreated >= 10;

            if (hasEnoughSamples) {
              if (overflowRatio > 0.15 && beforeCapacity < 1024) {
                expect(afterCapacity).toBe(Math.min(beforeCapacity * 2, 1024));
              } else if (overflowRatio < 0.05 && hasManyBuffers && beforeCapacity > 8) {
                expect(afterCapacity).toBe(Math.max(8, beforeCapacity / 2));
              } else {
                expect(afterCapacity).toBe(beforeCapacity);
              }
            } else {
              expect(afterCapacity).toBe(beforeCapacity);
            }

            // Stats reset invariant: if capacity changed, stats are reset
            if (afterCapacity !== beforeCapacity) {
              expect(stats.totalWrites).toBe(0);
              expect(stats.overflowWrites).toBe(0);
              expect(stats.totalCreated).toBe(0);
            }
          },
        ),
        { numRuns: 1000 },
      );
    });

    it('should verify the specific failing case from property testing', () => {
      // Reproduce the exact counterexample that exposed the analysis bug
      stats.totalWrites = 104; // 13 spans * 8 entries each
      stats.overflowWrites = 0; // No overflow
      stats.totalCreated = 18; // 5 initial + 13 spans created
      stats.capacity = 64; // Start capacity

      // Verify conditions before tuning
      const overflowRatio = stats.totalWrites > 0 ? stats.overflowWrites / stats.totalWrites : 0;
      const hasEnoughSamples = stats.totalWrites >= 100;
      const hasManyBuffers = stats.totalCreated >= 10;

      const beforeCapacity = stats.capacity;
      shouldTuneCapacity(stats);
      const afterCapacity = stats.capacity;

      console.log('Failing case analysis:');
      console.log('  totalWrites:', stats.totalWrites);
      console.log('  overflowWrites:', stats.overflowWrites);
      console.log('  overflowRatio:', overflowRatio);
      console.log('  totalCreated:', stats.totalCreated);
      console.log('  hasEnoughSamples:', hasEnoughSamples);
      console.log('  hasManyBuffers:', hasManyBuffers);
      console.log('  beforeCapacity:', beforeCapacity);
      console.log('  afterCapacity:', afterCapacity);

      // All conditions for shrinking are met:
      expect(hasEnoughSamples).toBe(true); // 104 >= 100
      expect(overflowRatio < 0.05).toBe(true); // 0 < 0.05
      expect(hasManyBuffers).toBe(true); // 18 >= 10
      expect(beforeCapacity > 8).toBe(true); // 64 > 8

      // Therefore, capacity SHOULD shrink from 64 to 32
      expect(afterCapacity).toBe(32); // Implementation IS correct
      // Stats should be reset after tuning
      expect(stats.totalWrites).toBe(0);
      expect(stats.overflowWrites).toBe(0);
      expect(stats.totalCreated).toBe(0);
    });

    it('should verify boundary conditions with precision', () => {
      fc.assert(
        fc.property(
          fc.record({
            totalWrites: fc.integer({ min: 95, max: 105 }), // Around sample boundary
            overflowRatio: fc.double({ min: -0.1, max: 0.3 }).filter((n) => !Number.isNaN(n)),
            totalCreated: fc.integer({ min: 8, max: 12 }), // Around buffer boundary
            initialCapacity: fc.constantFrom(8, 16, 32, 64, 128, 256, 512, 1024),
          }),
          ({ totalWrites, overflowRatio, totalCreated, initialCapacity }) => {
            const overflowWrites = Math.max(0, Math.floor(totalWrites * Math.max(0, overflowRatio)));

            stats.totalWrites = totalWrites;
            stats.overflowWrites = overflowWrites;
            stats.totalCreated = totalCreated;
            stats.capacity = initialCapacity;

            shouldTuneCapacity(stats);

            // Verify capacity respects boundaries
            expect(stats.capacity).toBeGreaterThanOrEqual(8);
            expect(stats.capacity).toBeLessThanOrEqual(1024);
            expect(stats.capacity & (stats.capacity - 1)).toBe(0);
          },
        ),
        { numRuns: 500 },
      );
    });

    it('should handle extreme workload patterns', () => {
      fc.assert(
        fc.property(
          fc.array(
            fc.record({
              writes: fc.integer({ min: 50, max: 1000 }),
              overflows: fc.integer({ min: 0, max: 500 }),
              buffers: fc.integer({ min: 1, max: 50 }),
            }),
            { minLength: 3, maxLength: 10 },
          ),
          (periods) => {
            // Reset stats
            stats.capacity = DEFAULT_BUFFER_CAPACITY;
            stats.totalWrites = 0;
            stats.overflowWrites = 0;
            stats.totalCreated = 0;

            const capacityChanges: number[] = [];

            for (const period of periods) {
              stats.totalWrites += period.writes;
              stats.overflowWrites += period.overflows;
              stats.totalCreated += period.buffers;

              const before = stats.capacity;
              shouldTuneCapacity(stats);
              const after = stats.capacity;

              if (before !== after) {
                capacityChanges.push(after);
                // Verify power-of-2 changes
                expect([0.5, 2]).toContain(after / before);
              }
            }

            // Final state should be reasonable
            const totalWrites = periods.reduce((sum, p) => sum + p.writes, 0);
            const totalOverflows = periods.reduce((sum, p) => sum + p.overflows, 0);
            const overallRatio = totalWrites > 0 ? totalOverflows / totalWrites : 0;

            if (overallRatio > 0.15) {
              expect(stats.capacity).toBeGreaterThanOrEqual(DEFAULT_BUFFER_CAPACITY);
            } else if (overallRatio < 0.05) {
              expect(stats.capacity).toBeLessThanOrEqual(DEFAULT_BUFFER_CAPACITY);
            }

            // Should maintain power-of-2 invariant
            expect(stats.capacity & (stats.capacity - 1)).toBe(0);
          },
        ),
        { numRuns: 100 },
      );
    });
  });

  describe('Integration with SpanLogger overflow', () => {
    /**
     * These tests verify that trackOverflowAndTune is actually called
     * when SpanLogger triggers buffer overflow via _getNextBuffer().
     *
     * The SpanLogger's generated code calls:
     *   helpers.trackOverflowAndTune(oldBuffer.constructor.stats)
     *
     * We verify this by checking that stats.overflows and stats.overflowWrites
     * are incremented when we trigger overflow.
     */

    const integrationSchema = new LogSchema(
      mergeWithSystemSchema({
        testField: S.category(),
      }),
    );

    it('should call trackOverflowAndTune when buffer overflows', () => {
      // Get SpanBufferClass and reset stats
      const SpanBufferClass = getSpanBufferClass(integrationSchema);
      const stats = SpanBufferClass.stats;

      // Set small capacity to trigger overflow quickly
      stats.capacity = 8;
      stats.overflows = 0;
      stats.overflowWrites = 0;
      stats.totalWrites = 0;
      stats.totalCreated = 0;

      // Create buffer and logger
      const buffer = createSpanBuffer(
        integrationSchema,
        'test-span',
        createTestTraceRoot('test-trace'),
        DEFAULT_METADATA,
        undefined,
      );
      const logger = createSpanLogger(integrationSchema, buffer);

      // SpanLogger reserves rows 0-1, so capacity 8 means 6 entries before overflow
      // Write enough entries to trigger overflow
      const entriesToWrite = 10; // More than capacity - 2

      for (let i = 0; i < entriesToWrite; i++) {
        logger.info(`message ${i}`);
      }

      // Verify overflow was triggered and stats updated
      expect(stats.overflows).toBeGreaterThan(0);
      expect(stats.overflowWrites).toBeGreaterThan(0);
      expect(stats.totalWrites).toBe(entriesToWrite);
    });

    it('should tune capacity after enough overflow samples', () => {
      // Get SpanBufferClass and reset stats
      const SpanBufferClass = getSpanBufferClass(integrationSchema);
      const stats = SpanBufferClass.stats;

      // Set small capacity to trigger frequent overflows
      const initialCapacity = 8;
      stats.capacity = initialCapacity;
      stats.overflows = 0;
      stats.overflowWrites = 0;
      stats.totalWrites = 0;
      stats.totalCreated = 0;

      // Write many entries to accumulate stats and trigger tuning
      // Need 100+ totalWrites and >15% overflow ratio to trigger capacity increase
      const buffer = createSpanBuffer(
        integrationSchema,
        'test-span',
        createTestTraceRoot('test-trace'),
        DEFAULT_METADATA,
        undefined,
      );
      const logger = createSpanLogger(integrationSchema, buffer);

      // With capacity 8 and 2 reserved rows, each buffer holds 6 entries
      // Write 120 entries to accumulate enough samples to trigger tuning
      for (let i = 0; i < 120; i++) {
        logger.info(`message ${i}`);
      }

      // Capacity tuning should have happened:
      // - With capacity 8, we overflow every 6 entries (high overflow ratio ~16%)
      // - After 100 samples with >15% overflow, capacity doubles to 16
      // - Stats are reset after tuning, so totalWrites will be low
      // The key assertion is that capacity increased from the initial value
      expect(stats.capacity).toBeGreaterThan(initialCapacity);

      // Verify it's still a power of 2
      expect(stats.capacity & (stats.capacity - 1)).toBe(0);
    });

    it('should call onStatsWillResetFor before stats reset during overflow', async () => {
      // Import TestTracer and defineOpContext for proper integration test
      const { defineOpContext } = await import('../defineOpContext.js');
      const { TestTracer } = await import('../tracers/TestTracer.js');

      // Create op context with small capacity
      const ctx = defineOpContext({
        logSchema: integrationSchema,
      });

      // Create TestTracer to capture stats snapshots
      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      // Set small capacity to trigger overflow quickly
      const SpanBufferClass = getSpanBufferClass(integrationSchema);
      const stats = SpanBufferClass.stats;
      stats.capacity = 8;
      stats.overflows = 0;
      stats.overflowWrites = 0;
      stats.totalWrites = 0;
      stats.totalCreated = 0;

      // Execute trace with enough log entries to trigger overflow and capacity tuning
      // With capacity 8 and 2 reserved rows, we need 100+ writes with >15% overflow
      trace('test-trace', (ctx) => {
        for (let i = 0; i < 120; i++) {
          ctx.log.info(`entry ${i}`);
        }
      });

      // Verify the tracer captured stats snapshots before reset
      expect(tracer.statsSnapshots.length).toBeGreaterThan(0);

      // Verify captured stats show non-zero writes before reset
      const firstSnapshot = tracer.statsSnapshots[0];
      expect(firstSnapshot.totalWrites).toBeGreaterThan(0);
      expect(firstSnapshot.capacity).toBeGreaterThanOrEqual(8);

      // Verify buffer reference is present
      expect(firstSnapshot.buffer).toBeDefined();
      expect(firstSnapshot.buffer._spanName).toBeDefined();
    });
  });
});
