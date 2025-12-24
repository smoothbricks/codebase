import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import fc from 'fast-check';
import { shouldTuneCapacity } from '../capacityTuning.js';
import type { LogBinding } from '../logBinding.js';
import { LogSchema } from '../schema/LogSchema.js';

/**
 * Create a mock LogBinding for testing capacity tuning.
 * Uses sb_* properties as per LogBinding interface.
 */
function createMockLogBinding(): LogBinding {
  return {
    logSchema: new LogSchema({}),
    remappedViewClass: undefined,
    sb_capacity: DEFAULT_BUFFER_CAPACITY,
    sb_totalWrites: 0,
    sb_overflowWrites: 0,
    sb_totalCreated: 0,
    sb_overflows: 0,
  };
}

describe('Capacity Tuning Algorithm', () => {
  let module: LogBinding;

  beforeEach(() => {
    module = createMockLogBinding();
  });

  describe('shouldTuneCapacity - Success Cases', () => {
    it('should increase capacity when overflow ratio > 15%', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20; // 20% overflow
      // Start with default capacity
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      const initialCapacity = module.sb_capacity;
      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(initialCapacity * 2); // Doubled
      expect(module.sb_totalWrites).toBe(0); // Reset
      expect(module.sb_overflowWrites).toBe(0); // Reset
    });

    it('should decrease capacity when overflow ratio < 5% with many buffers', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 3; // 3% overflow
      module.sb_totalCreated = 15; // Many buffers
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4; // Start at 4x default so we can halve

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Halved
      expect(module.sb_totalWrites).toBe(0); // Reset
      expect(module.sb_overflowWrites).toBe(0); // Reset
    });

    it('should double capacity from default to 2x', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 16; // 16% overflow

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should double capacity from 128 to 256', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 16;
      module.sb_capacity = 128;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(256);
    });

    it('should halve capacity from 4x to 2x default', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4; // 4% overflow
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should halve capacity from 32 to 16', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10;
      module.sb_capacity = 32;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(16);
    });
  });

  describe('shouldTuneCapacity - Edge Cases', () => {
    it('should not tune with insufficient samples (< 100)', () => {
      module.sb_totalWrites = 99;
      module.sb_overflowWrites = 50; // 50% overflow, but not enough samples
      const initialCapacity = module.sb_capacity;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(initialCapacity); // Unchanged
    });

    it('should not tune at exactly 100 writes with low overflow', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 10; // 10% overflow (between 5% and 15%)
      const initialCapacity = module.sb_capacity;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(initialCapacity);
    });

    it('should cap capacity at 1024', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20; // 20% overflow
      module.sb_capacity = 1024;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(1024); // Cannot increase beyond 1024
    });

    it('should cap capacity at minimum when decreasing', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4; // 4% overflow
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY); // Cannot decrease below default
    });

    it('should not decrease without enough buffers (< 10)', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4; // 4% overflow
      module.sb_totalCreated = 9; // Not enough buffers
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);
    });

    it('should handle exactly 15% overflow (boundary)', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 15; // Exactly 15%
      const initialCapacity = module.sb_capacity;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(initialCapacity); // > 15%, not >= 15%
    });

    it('should handle exactly 5% overflow (boundary)', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 5; // Exactly 5%
      module.sb_totalCreated = 10;
      const initialCapacity = module.sb_capacity;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(initialCapacity); // < 5%, not <= 5%
    });

    it('should tune at 15.1% overflow', () => {
      module.sb_totalWrites = 1000;
      module.sb_overflowWrites = 151; // 15.1% overflow

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should tune at 4.9% overflow with sufficient buffers', () => {
      module.sb_totalWrites = 1000;
      module.sb_overflowWrites = 49; // 4.9% overflow
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4; // Need room to decrease

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);
    });

    it('should handle zero overflow writes', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 0; // 0% overflow
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 4; // Need room to decrease

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Should decrease
    });

    it('should handle all writes overflowing (100%)', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 100; // 100% overflow

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2); // Should increase
    });
  });

  describe('shouldTuneCapacity - Capacity Bounds', () => {
    it('should not increase beyond 1024 even with high overflow', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 99; // 99% overflow
      module.sb_capacity = 1024;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(1024);
    });

    it('should cap at 1024 when doubling from 512', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      module.sb_capacity = 512;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(1024); // Capped at 1024 (max capacity)
    });

    it('should not decrease below minimum', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 0;
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should cap at minimum when halving from 2x', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10;
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 2;

      shouldTuneCapacity(module);

      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });
  });

  describe('Stats Reset After Tuning', () => {
    it('should reset stats after increasing capacity', () => {
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      module.sb_totalCreated = 5;

      shouldTuneCapacity(module);

      expect(module.sb_totalWrites).toBe(0);
      expect(module.sb_overflowWrites).toBe(0);
      expect(module.sb_totalCreated).toBe(0);
    });

    it('should reset stats after decreasing capacity', () => {
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 2; // Need room to decrease
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10;

      shouldTuneCapacity(module);

      expect(module.sb_totalWrites).toBe(0);
      expect(module.sb_overflowWrites).toBe(0);
      expect(module.sb_totalCreated).toBe(0);
    });

    it('should not reset stats when no tuning occurs', () => {
      module.sb_totalWrites = 50; // Not enough samples
      module.sb_overflowWrites = 10;
      module.sb_totalCreated = 5;

      shouldTuneCapacity(module);

      expect(module.sb_totalWrites).toBe(50); // Unchanged
      expect(module.sb_overflowWrites).toBe(10); // Unchanged
      expect(module.sb_totalCreated).toBe(5); // Unchanged
    });
  });

  describe('Multiple Tuning Cycles', () => {
    it('should handle multiple increases correctly', () => {
      // Start at default
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // First increase
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Second increase
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);

      // Third increase
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 8);
    });

    it('should handle multiple decreases correctly', () => {
      module.sb_capacity = DEFAULT_BUFFER_CAPACITY * 8; // Start high
      module.sb_totalCreated = 10;

      // First decrease
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 4);

      // Second decrease
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10; // Need to set again after reset
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Third decrease
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);
    });

    it('should handle increase then decrease', () => {
      // Start at default
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);

      // Increase due to high overflow
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 20;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY * 2);

      // Decrease due to low overflow
      module.sb_totalWrites = 100;
      module.sb_overflowWrites = 4;
      module.sb_totalCreated = 10;
      shouldTuneCapacity(module);
      expect(module.sb_capacity).toBe(DEFAULT_BUFFER_CAPACITY);
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
            // Set up module state
            module.sb_totalWrites = totalWrites;
            module.sb_overflowWrites = overflowWrites;
            module.sb_totalCreated = totalCreated;
            module.sb_capacity = initialCapacity;

            const beforeCapacity = module.sb_capacity;
            shouldTuneCapacity(module);
            const afterCapacity = module.sb_capacity;

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

            // Stats reset invariant
            if (afterCapacity !== beforeCapacity) {
              expect(module.sb_totalWrites).toBe(0);
              expect(module.sb_overflowWrites).toBe(0);
              expect(module.sb_totalCreated).toBe(0);
            }
          },
        ),
        { numRuns: 1000 },
      );
    });

    it('should verify the specific failing case from property testing', () => {
      // Reproduce the exact counterexample that exposed the analysis bug
      module.sb_totalWrites = 104; // 13 spans * 8 entries each
      module.sb_overflowWrites = 0; // No overflow
      module.sb_totalCreated = 18; // 5 initial + 13 spans created
      module.sb_capacity = 64; // Start capacity

      // Verify conditions before tuning
      const overflowRatio = module.sb_totalWrites > 0 ? module.sb_overflowWrites / module.sb_totalWrites : 0;
      const hasEnoughSamples = module.sb_totalWrites >= 100;
      const hasManyBuffers = module.sb_totalCreated >= 10;

      const beforeCapacity = module.sb_capacity;
      shouldTuneCapacity(module);
      const afterCapacity = module.sb_capacity;

      console.log('Failing case analysis:');
      console.log('  totalWrites:', module.sb_totalWrites);
      console.log('  overflowWrites:', module.sb_overflowWrites);
      console.log('  overflowRatio:', overflowRatio);
      console.log('  totalCreated:', module.sb_totalCreated);
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
      expect(module.sb_totalWrites).toBe(0); // Stats should reset
      expect(module.sb_overflowWrites).toBe(0);
      expect(module.sb_totalCreated).toBe(0);
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

            module.sb_totalWrites = totalWrites;
            module.sb_overflowWrites = overflowWrites;
            module.sb_totalCreated = totalCreated;
            module.sb_capacity = initialCapacity;

            shouldTuneCapacity(module);

            // Verify capacity respects boundaries
            expect(module.sb_capacity).toBeGreaterThanOrEqual(8);
            expect(module.sb_capacity).toBeLessThanOrEqual(1024);
            expect(module.sb_capacity & (module.sb_capacity - 1)).toBe(0);
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
            // Reset module
            module.sb_capacity = DEFAULT_BUFFER_CAPACITY;
            module.sb_totalWrites = 0;
            module.sb_overflowWrites = 0;
            module.sb_totalCreated = 0;

            const capacityChanges: number[] = [];

            for (const period of periods) {
              module.sb_totalWrites += period.writes;
              module.sb_overflowWrites += period.overflows;
              module.sb_totalCreated += period.buffers;

              const before = module.sb_capacity;
              shouldTuneCapacity(module);
              const after = module.sb_capacity;

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
              expect(module.sb_capacity).toBeGreaterThanOrEqual(DEFAULT_BUFFER_CAPACITY);
            } else if (overallRatio < 0.05) {
              expect(module.sb_capacity).toBeLessThanOrEqual(DEFAULT_BUFFER_CAPACITY);
            }

            // Should maintain power-of-2 invariant
            expect(module.sb_capacity & (module.sb_capacity - 1)).toBe(0);
          },
        ),
        { numRuns: 100 },
      );
    });
  });
});
