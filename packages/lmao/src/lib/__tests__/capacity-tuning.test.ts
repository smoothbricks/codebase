import { describe, it, expect, beforeEach } from 'bun:test';
import type { BufferCapacityStats } from '@smoothbricks/arrow-builder';

/**
 * Mock implementation of shouldTuneCapacity for testing
 * This mirrors the actual implementation in lmao.ts
 */
function shouldTuneCapacity(stats: BufferCapacityStats): boolean {
  const minSamples = 100;
  if (stats.totalWrites < minSamples) return false;
  
  const overflowRatio = stats.overflowWrites / stats.totalWrites;
  
  // Increase if >15% writes overflow
  if (overflowRatio > 0.15 && stats.currentCapacity < 1024) {
    const newCapacity = Math.min(stats.currentCapacity * 2, 1024);
    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }
  
  // Decrease if <5% writes overflow and we have many buffers
  if (overflowRatio < 0.05 && stats.totalBuffersCreated >= 10 && stats.currentCapacity > 8) {
    const newCapacity = Math.max(8, stats.currentCapacity / 2);
    stats.currentCapacity = newCapacity;
    resetStats(stats);
    return true;
  }
  
  return false;
}

function resetStats(stats: BufferCapacityStats): void {
  stats.totalWrites = 0;
  stats.overflowWrites = 0;
  stats.totalBuffersCreated = 0;
}

describe('Capacity Tuning Algorithm', () => {
  let stats: BufferCapacityStats;

  beforeEach(() => {
    stats = {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    };
  });

  describe('shouldTuneCapacity - Success Cases', () => {
    it('should increase capacity when overflow ratio > 15%', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20; // 20% overflow
      stats.currentCapacity = 64;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true);
      expect(stats.currentCapacity).toBe(128); // Doubled
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.overflowWrites).toBe(0); // Reset
    });

    it('should decrease capacity when overflow ratio < 5% with many buffers', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 3; // 3% overflow
      stats.totalBuffersCreated = 15; // Many buffers
      stats.currentCapacity = 64;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true);
      expect(stats.currentCapacity).toBe(32); // Halved
      expect(stats.totalWrites).toBe(0); // Reset
      expect(stats.overflowWrites).toBe(0); // Reset
    });

    it('should double capacity from 64 to 128', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 16; // 16% overflow
      stats.currentCapacity = 64;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(128);
    });

    it('should double capacity from 128 to 256', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 16;
      stats.currentCapacity = 128;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(256);
    });

    it('should halve capacity from 64 to 32', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalBuffersCreated = 10;
      stats.currentCapacity = 64;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(32);
    });

    it('should halve capacity from 32 to 16', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10;
      stats.currentCapacity = 32;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(16);
    });
  });

  describe('shouldTuneCapacity - Edge Cases', () => {
    it('should not tune with insufficient samples (< 100)', () => {
      stats.totalWrites = 99;
      stats.overflowWrites = 50; // 50% overflow, but not enough samples

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false);
      expect(stats.currentCapacity).toBe(64); // Unchanged
    });

    it('should not tune at exactly 100 writes with low overflow', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 10; // 10% overflow (between 5% and 15%)

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false);
      expect(stats.currentCapacity).toBe(64);
    });

    it('should cap capacity at 1024', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20; // 20% overflow
      stats.currentCapacity = 1024;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false); // Cannot increase beyond 1024
      expect(stats.currentCapacity).toBe(1024);
    });

    it('should cap capacity at 8 when decreasing', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalBuffersCreated = 10;
      stats.currentCapacity = 8;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false); // Cannot decrease below 8
      expect(stats.currentCapacity).toBe(8);
    });

    it('should not decrease without enough buffers (< 10)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4; // 4% overflow
      stats.totalBuffersCreated = 9; // Not enough buffers
      stats.currentCapacity = 64;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false);
      expect(stats.currentCapacity).toBe(64);
    });

    it('should handle exactly 15% overflow (boundary)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 15; // Exactly 15%

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false); // > 15%, not >= 15%
      expect(stats.currentCapacity).toBe(64);
    });

    it('should handle exactly 5% overflow (boundary)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 5; // Exactly 5%
      stats.totalBuffersCreated = 10;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false); // < 5%, not <= 5%
      expect(stats.currentCapacity).toBe(64);
    });

    it('should tune at 15.1% overflow', () => {
      stats.totalWrites = 1000;
      stats.overflowWrites = 151; // 15.1% overflow

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true);
      expect(stats.currentCapacity).toBe(128);
    });

    it('should tune at 4.9% overflow with sufficient buffers', () => {
      stats.totalWrites = 1000;
      stats.overflowWrites = 49; // 4.9% overflow
      stats.totalBuffersCreated = 10;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true);
      expect(stats.currentCapacity).toBe(32);
    });

    it('should handle zero overflow writes', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 0; // 0% overflow
      stats.totalBuffersCreated = 10;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true); // Should decrease
      expect(stats.currentCapacity).toBe(32);
    });

    it('should handle all writes overflowing (100%)', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 100; // 100% overflow

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(true); // Should increase
      expect(stats.currentCapacity).toBe(128);
    });
  });

  describe('shouldTuneCapacity - Capacity Bounds', () => {
    it('should not increase beyond 1024 even with high overflow', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 99; // 99% overflow
      stats.currentCapacity = 1024;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false);
      expect(stats.currentCapacity).toBe(1024);
    });

    it('should cap at 1024 when doubling from 512', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      stats.currentCapacity = 512;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(1024); // Not 1024 (capped)
    });

    it('should not decrease below 8', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 0;
      stats.totalBuffersCreated = 10;
      stats.currentCapacity = 8;

      const tuned = shouldTuneCapacity(stats);

      expect(tuned).toBe(false);
      expect(stats.currentCapacity).toBe(8);
    });

    it('should cap at 8 when halving from 16', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10;
      stats.currentCapacity = 16;

      shouldTuneCapacity(stats);

      expect(stats.currentCapacity).toBe(8);
    });
  });

  describe('Stats Reset After Tuning', () => {
    it('should reset stats after increasing capacity', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      stats.totalBuffersCreated = 5;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.overflowWrites).toBe(0);
      expect(stats.totalBuffersCreated).toBe(0);
    });

    it('should reset stats after decreasing capacity', () => {
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(0);
      expect(stats.overflowWrites).toBe(0);
      expect(stats.totalBuffersCreated).toBe(0);
    });

    it('should not reset stats when no tuning occurs', () => {
      stats.totalWrites = 50; // Not enough samples
      stats.overflowWrites = 10;
      stats.totalBuffersCreated = 5;

      shouldTuneCapacity(stats);

      expect(stats.totalWrites).toBe(50); // Unchanged
      expect(stats.overflowWrites).toBe(10); // Unchanged
      expect(stats.totalBuffersCreated).toBe(5); // Unchanged
    });
  });

  describe('Multiple Tuning Cycles', () => {
    it('should handle multiple increases correctly', () => {
      stats.currentCapacity = 64;

      // First increase
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(128);

      // Second increase
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(256);

      // Third increase
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(512);
    });

    it('should handle multiple decreases correctly', () => {
      stats.currentCapacity = 64;
      stats.totalBuffersCreated = 10;

      // First decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(32);

      // Second decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10; // Need to set again after reset
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(16);

      // Third decrease
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(8);
    });

    it('should handle increase then decrease', () => {
      stats.currentCapacity = 64;

      // Increase due to high overflow
      stats.totalWrites = 100;
      stats.overflowWrites = 20;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(128);

      // Decrease due to low overflow
      stats.totalWrites = 100;
      stats.overflowWrites = 4;
      stats.totalBuffersCreated = 10;
      shouldTuneCapacity(stats);
      expect(stats.currentCapacity).toBe(64);
    });
  });
});
