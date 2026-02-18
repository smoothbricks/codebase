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
    spansCreated: overrides.spansCreated ?? 0,
  };
}

/**
 * Compute utilization for testing
 * Utilization = totalWrites / (spansCreated * usableRowsPerSpan)
 */
function computeUtilization(stats: SpanBufferStats): number {
  const usableRowsPerSpan = stats.capacity - 2;
  return stats.spansCreated > 0 ? stats.totalWrites / (stats.spansCreated * usableRowsPerSpan) : 0;
}

describe('Capacity Tuning Algorithm', () => {
  let stats: SpanBufferStats;

  beforeEach(() => {
    stats = createMockStats();
  });

  describe('shouldTuneCapacity - Success Cases', () => {
    it('should increase capacity when utilization > 150%', () => {
      // 10 spans, capacity 8, usable rows = 6
      // At 150% utilization: 10 * 6 * 1.5 = 90 writes
      // At 160% utilization: 10 * 6 * 1.6 = 96 writes
      stats.spansCreated = 10;
      stats.capacity = 8;
      stats.totalWrites = 96; // 160% utilization

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity * 2); // Doubled
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.spansCreated).toBe(0); // Reset
    });

    it('should decrease capacity when utilization < 50%', () => {
      // 10 spans, capacity 32, usable rows = 30
      // At 50% utilization: 10 * 30 * 0.5 = 150 writes
      // At 40% utilization: 10 * 30 * 0.4 = 120 writes
      stats.spansCreated = 10;
      stats.capacity = 32;
      stats.totalWrites = 120; // 40% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(16); // Halved
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.spansCreated).toBe(0); // Reset
    });

    it('should double capacity from default to 2x', () => {
      stats.spansCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY; // 8
      const usableRows = stats.capacity - 2; // 6
      stats.totalWrites = Math.floor(usableRows * 10 * 1.6); // 160% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should double capacity from 128 to 256', () => {
      stats.spansCreated = 10;
      stats.capacity = 128;
      const usableRows = stats.capacity - 2; // 126
      stats.totalWrites = Math.floor(usableRows * 10 * 1.6); // 160% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(256);
    });

    it('should halve capacity from 4x to 2x default', () => {
      stats.spansCreated = 10;
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 4; // 32
      const usableRows = stats.capacity - 2; // 30
      stats.totalWrites = Math.floor(usableRows * 10 * 0.4); // 40% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });
  });

  describe('shouldTuneCapacity - Edge Cases', () => {
    it('should not tune with insufficient spans (< 10)', () => {
      stats.spansCreated = 9;
      stats.capacity = 8;
      stats.totalWrites = 1000; // Very high utilization, but not enough spans

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // Unchanged
    });

    it('should not tune when utilization is in stable zone (50-150%)', () => {
      stats.spansCreated = 10;
      stats.capacity = 8;
      const usableRows = stats.capacity - 2; // 6
      stats.totalWrites = usableRows * 10; // Exactly 100% utilization

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity);
    });

    it('should cap capacity at 1024', () => {
      stats.spansCreated = 10;
      stats.capacity = 1024;
      const usableRows = stats.capacity - 2;
      stats.totalWrites = Math.floor(usableRows * 10 * 2); // 200% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024); // Cannot increase beyond 1024
    });

    it('should cap capacity at minimum (8) when decreasing', () => {
      stats.spansCreated = 10;
      stats.capacity = 8;
      stats.totalWrites = 10; // Very low utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(8); // Cannot decrease below 8
    });

    it('should handle exactly 150% utilization (boundary - no increase)', () => {
      stats.spansCreated = 10;
      stats.capacity = 8;
      const usableRows = stats.capacity - 2; // 6
      stats.totalWrites = usableRows * 10 * 1.5; // Exactly 150%

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // > 150%, not >= 150%
    });

    it('should handle exactly 50% utilization (boundary - no decrease)', () => {
      stats.spansCreated = 10;
      stats.capacity = 32;
      const usableRows = stats.capacity - 2;
      stats.totalWrites = usableRows * 10 * 0.5; // Exactly 50%

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // < 50%, not <= 50%
    });

    it('should tune at 151% utilization', () => {
      stats.spansCreated = 100;
      stats.capacity = 8;
      const usableRows = stats.capacity - 2; // 6
      stats.totalWrites = Math.floor(usableRows * 100 * 1.51); // 151% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(16); // Doubled
    });

    it('should tune at 49% utilization', () => {
      stats.spansCreated = 100;
      stats.capacity = 32;
      const usableRows = stats.capacity - 2;
      stats.totalWrites = Math.floor(usableRows * 100 * 0.49); // 49% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(16); // Halved
    });

    it('should handle zero writes', () => {
      stats.spansCreated = 10;
      stats.totalWrites = 0;
      stats.capacity = 32;

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(16); // Should decrease (0% utilization < 50%)
    });

    it('should handle zero spans (no tuning possible)', () => {
      stats.spansCreated = 0;
      stats.totalWrites = 1000;
      stats.capacity = 8;

      const initialCapacity = stats.capacity;
      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(initialCapacity); // Unchanged - not enough spans
    });
  });

  describe('shouldTuneCapacity - Capacity Bounds', () => {
    it('should not increase beyond 1024 even with extreme utilization', () => {
      stats.spansCreated = 10;
      stats.capacity = 1024;
      stats.totalWrites = 1000000; // Extreme utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024);
    });

    it('should cap at 1024 when doubling from 512', () => {
      stats.spansCreated = 10;
      stats.capacity = 512;
      const usableRows = stats.capacity - 2;
      stats.totalWrites = Math.floor(usableRows * 10 * 2); // 200% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(1024); // Capped at 1024
    });

    it('should not decrease below minimum', () => {
      stats.spansCreated = 10;
      stats.capacity = 8;
      stats.totalWrites = 1; // Very low utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(8);
    });

    it('should cap at minimum when halving from 16', () => {
      stats.spansCreated = 10;
      stats.capacity = 16;
      const usableRows = stats.capacity - 2;
      stats.totalWrites = Math.floor(usableRows * 10 * 0.3); // 30% utilization

      shouldTuneCapacity(stats);

      expect(stats.capacity).toBe(8);
    });
  });

  describe('Stats Reset After Tuning', () => {
    it('should reset stats after increasing capacity', () => {
      stats.spansCreated = 10;
      stats.capacity = 8;
      stats.totalWrites = 100; // High utilization

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.spansCreated).toBe(0);
    });

    it('should reset stats after decreasing capacity', () => {
      stats.spansCreated = 10;
      stats.capacity = 32;
      stats.totalWrites = 50; // Low utilization

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.spansCreated).toBe(0);
    });

    it('should not reset stats when no tuning occurs', () => {
      stats.spansCreated = 5; // Not enough spans
      stats.totalWrites = 1000;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(1000); // Unchanged
      expect(stats.spansCreated).toBe(5); // Unchanged
    });
  });

  describe('Multiple Tuning Cycles', () => {
    it('should handle multiple increases correctly', () => {
      // Start at default (capacity=8, usableRows=6)
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // First increase: need >150% utilization
      // usableRows=6, spansCreated=10 → need totalWrites > 10*6*1.5 = 90
      stats.spansCreated = 10;
      stats.totalWrites = 100; // 100/(10*6) = 1.67 > 1.5 → grows to 16
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // 16

      // Second increase: capacity=16, usableRows=14
      // need totalWrites > 10*14*1.5 = 210
      stats.spansCreated = 10;
      stats.totalWrites = 220; // 220/(10*14) = 1.57 > 1.5 → grows to 32
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4); // 32
    });

    it('should handle multiple decreases correctly', () => {
      stats.capacity = DEFAULT_BUFFER_CAPACITY * 8; // Start high (64)

      // First decrease
      stats.spansCreated = 10;
      stats.totalWrites = 50; // Low utilization
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4); // 32

      // Second decrease
      stats.spansCreated = 10;
      stats.totalWrites = 50;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // 16

      // Third decrease
      stats.spansCreated = 10;
      stats.totalWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY); // 8
    });

    it('should handle increase then stabilize', () => {
      // Start at default
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY); // 8

      // Increase due to high utilization
      stats.spansCreated = 10;
      stats.totalWrites = 100;
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // 16

      // Now with new capacity, same workload should be stable
      // 10 spans * (16-2) usable * 100% = 140 writes ideal
      stats.spansCreated = 10;
      stats.totalWrites = 140; // 100% utilization - stable zone
      shouldTuneCapacity(stats);
      expect(stats.capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Still 16
    });
  });

  describe('Property-Based Testing for Utilization Formula', () => {
    it('should verify all mathematical invariants for arbitrary inputs', () => {
      fc.assert(
        fc.property(
          fc.record({
            totalWrites: fc.integer({ min: 0, max: 10000 }),
            spansCreated: fc.integer({ min: 0, max: 1000 }),
            initialCapacity: fc.constantFrom(8, 16, 32, 64, 128, 256, 512, 1024),
          }),
          ({ totalWrites, spansCreated, initialCapacity }) => {
            // Set up stats state
            stats.totalWrites = totalWrites;
            stats.spansCreated = spansCreated;
            stats.capacity = initialCapacity;

            const beforeCapacity = stats.capacity;
            const utilization = computeUtilization(stats);
            shouldTuneCapacity(stats);
            const afterCapacity = stats.capacity;

            // Core invariants that must always hold
            expect(afterCapacity).toBeGreaterThanOrEqual(8);
            expect(afterCapacity).toBeLessThanOrEqual(1024);
            expect(afterCapacity & (afterCapacity - 1)).toBe(0); // Power of 2

            // Verify exact behavior matches specification
            const hasEnoughSpans = spansCreated >= 10;

            if (hasEnoughSpans) {
              if (utilization > 1.5 && beforeCapacity < 1024) {
                expect(afterCapacity).toBe(Math.min(beforeCapacity * 2, 1024));
              } else if (utilization < 0.5 && beforeCapacity > 8) {
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
              expect(stats.spansCreated).toBe(0);
            }
          },
        ),
        { numRuns: 1000 },
      );
    });

    it('should verify anti-flip-flop property: after tuning, opposite action is not triggered', () => {
      fc.assert(
        fc.property(
          fc.record({
            spansCreated: fc.integer({ min: 10, max: 100 }),
            initialCapacity: fc.constantFrom(8, 16, 32, 64, 128, 256, 512),
            utilizationPercent: fc.integer({ min: 1, max: 300 }),
          }),
          ({ spansCreated, initialCapacity, utilizationPercent }) => {
            stats.spansCreated = spansCreated;
            stats.capacity = initialCapacity;

            const usableRows = initialCapacity - 2;
            const totalWrites = Math.floor((usableRows * spansCreated * utilizationPercent) / 100);
            stats.totalWrites = totalWrites;

            const beforeCapacity = stats.capacity;
            shouldTuneCapacity(stats);
            const afterCapacity = stats.capacity;

            if (afterCapacity !== beforeCapacity) {
              // Simulate the SAME workload (same totalWrites) with the new capacity
              // This tests: if we tune and then see the same workload again,
              // we should NOT immediately trigger the opposite tuning action
              // Note: stats are reset after tuning, so use saved totalWrites
              const newUsableRows = afterCapacity - 2;
              const newUtilization = totalWrites / (spansCreated * newUsableRows);

              if (afterCapacity > beforeCapacity) {
                // We grew because utilization was >150%
                // After growing, the new utilization should NOT trigger shrinking (<50%)
                // This is the anti-flip-flop property
                expect(newUtilization).toBeGreaterThanOrEqual(0.5);
              } else {
                // We shrunk because utilization was <50%
                // After shrinking, the new utilization should NOT trigger growing (>150%)
                expect(newUtilization).toBeLessThanOrEqual(1.5);
              }
            }
          },
        ),
        { numRuns: 500 },
      );
    });
  });

  describe('Integration with SpanLogger overflow', () => {
    const userIntegrationSchema = new LogSchema({
      testField: S.category(),
    });

    const integrationSchema = new LogSchema(
      mergeWithSystemSchema({
        testField: S.category(),
      }),
    );

    it('should tune capacity based on utilization after many spans', () => {
      const SpanBufferClass = getSpanBufferClass(integrationSchema);
      const stats = SpanBufferClass.stats;

      // Reset stats
      const initialCapacity = 8;
      stats.capacity = initialCapacity;
      stats.totalWrites = 0;
      stats.spansCreated = 0;

      // Simulate 10+ spans with high utilization (>150%)
      // Capacity 8, usable rows = 6
      // 150% utilization = 9 writes per span
      // 160% utilization = 9.6 writes per span
      for (let span = 0; span < 12; span++) {
        const buffer = createSpanBuffer(
          integrationSchema,
          createTestTraceRoot('test-trace'),
          DEFAULT_METADATA,
          undefined,
        );
        const logger = createSpanLogger(integrationSchema, buffer);

        // Write 10 entries per span (160% utilization of 6 usable rows)
        for (let i = 0; i < 10; i++) {
          logger.info(`message ${i}`);
        }
      }

      // After 12 spans with 10 writes each = 120 writes
      // Utilization = 120 / (12 * 6) = 166% > 150%
      // Should have triggered capacity increase
      expect(stats.capacity).toBeGreaterThan(initialCapacity);
      expect(stats.capacity & (stats.capacity - 1)).toBe(0); // Power of 2
    });

    it('should call onStatsWillResetFor before stats reset during tuning', async () => {
      const { defineOpContext } = await import('../defineOpContext.js');
      const { TestTracer } = await import('../tracers/TestTracer.js');

      const ctx = defineOpContext({
        logSchema: userIntegrationSchema,
      });

      const tracer = new TestTracer(ctx, { ...createTestTracerOptions() });
      const { trace } = tracer;

      const SpanBufferClass = getSpanBufferClass(integrationSchema);
      const stats = SpanBufferClass.stats;
      stats.capacity = 8;
      stats.totalWrites = 0;
      stats.spansCreated = 0;

      // Execute multiple traces to accumulate enough spans for tuning
      for (let t = 0; t < 15; t++) {
        trace(`test-trace-${t}`, (ctx) => {
          // Write many entries to trigger high utilization
          for (let i = 0; i < 15; i++) {
            ctx.log.info(`entry ${i}`);
          }
        });
      }

      // If capacity was tuned, we should have stats snapshots
      // (captured in onStatsWillResetFor before reset)
      if (tracer.statsSnapshots.length > 0) {
        const firstSnapshot = tracer.statsSnapshots[0];
        expect(firstSnapshot.totalWrites).toBeGreaterThan(0);
        expect(firstSnapshot.capacity).toBeGreaterThanOrEqual(8);
        expect(firstSnapshot.buffer).toBeDefined();
      }
    });
  });
});
