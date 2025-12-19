import { beforeEach, describe, expect, it } from 'bun:test';
import { DEFAULT_BUFFER_CAPACITY } from '@smoothbricks/arrow-builder';
import { shouldTuneCapacity } from '../defineModule.js';
import { ModuleContext } from '../moduleContext.js';

/**
 * Mock ModuleContext for testing capacity tuning
 * Uses sb_* properties as per ModuleContext design
 */
class MockModuleContext extends ModuleContext {
  constructor() {
    super('test-sha', 'test-package', 'test-path', {});
  }
}

describe('Capacity Tuning Algorithm', () => {
  let module: MockModuleContext;

  beforeEach(() => {
    module = new MockModuleContext();
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
});
