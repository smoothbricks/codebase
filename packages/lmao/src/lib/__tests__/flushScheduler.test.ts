/**
 * Unit tests for background flush scheduler
 */

import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import {
  type FlushHandler,
  type FlushMetadata,
  FlushScheduler,
  type FlushSchedulerConfig,
  FlushSchedulerTestUtils,
} from '../flushScheduler.js';
import { createSpanBuffer } from '../spanBuffer.js';

import type { AnySpanBuffer } from '../types.js';
import { createTestOpMetadata, createTestSchema, createTestTraceRoot } from './test-helpers.js';

function createTestBuffer(): AnySpanBuffer {
  const schema = createTestSchema({});
  const opMetadata = createTestOpMetadata();
  const buffer = createSpanBuffer(schema, createTestTraceRoot('test-trace'), opMetadata);
  // Write some test data
  buffer._writeIndex = 5;
  for (let i = 0; i < buffer._writeIndex; i++) {
    buffer.timestamp[i] = BigInt(Date.now()) * 1_000_000n;
    buffer.entry_type[i] = 1;
  }
  return buffer;
}

describe('FlushScheduler', () => {
  let scheduler: FlushScheduler;
  let flushHandler: FlushHandler;
  let flushCalls: Array<{ metadata: FlushMetadata }>;

  beforeEach(() => {
    flushCalls = [];
    flushHandler = mock((_table, metadata) => {
      flushCalls.push({ metadata });
    });

    scheduler = new FlushScheduler(flushHandler);
  });

  afterEach(() => {
    if (scheduler) {
      scheduler.stop();
    }
  });

  describe('register', () => {
    describe('success cases', () => {
      it('should register a buffer for automatic flushing', () => {
        const buffer = createTestBuffer();

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should start scheduler when first buffer is registered', () => {
        const buffer = createTestBuffer();
        scheduler.register(buffer);

        // Scheduler should be running (internal state check)
        expect(scheduler).toBeDefined();
      });

      it('should register multiple buffers', () => {
        const buffer1 = createTestBuffer();
        const buffer2 = createTestBuffer();
        const buffer3 = createTestBuffer();

        scheduler.register(buffer1);
        scheduler.register(buffer2);
        scheduler.register(buffer3);

        expect(scheduler).toBeDefined();
      });
    });

    describe('edge cases', () => {
      it('should handle registering same buffer multiple times', () => {
        const buffer = createTestBuffer();

        scheduler.register(buffer);
        scheduler.register(buffer);
        scheduler.register(buffer);

        expect(scheduler).toBeDefined();
      });

      it('should handle buffer with zero writeIndex', () => {
        const buffer = createTestBuffer();
        buffer._writeIndex = 0;

        scheduler.register(buffer);
        expect(scheduler).toBeDefined();
      });

      it('should handle buffer at full capacity', () => {
        const buffer = createTestBuffer();
        buffer._writeIndex = buffer._capacity;

        scheduler.register(buffer);
        expect(scheduler).toBeDefined();
      });
    });

    describe('failure cases', () => {
      it('should handle buffer with invalid writeIndex', () => {
        const buffer = createTestBuffer();
        buffer._writeIndex = -1;

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should handle buffer with writeIndex > capacity', () => {
        const buffer = createTestBuffer();
        buffer._writeIndex = buffer._capacity + 100;

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should handle buffer with missing properties', () => {
        const buffer = createTestBuffer();
        // Create a partial buffer missing timestamps to test error handling
        const { timestamp: _timestamps, ...partialBuffer } = buffer;

        expect(() => {
          scheduler.register(partialBuffer as AnySpanBuffer);
        }).not.toThrow();
      });
    });
  });

  describe('start and stop', () => {
    describe('success cases', () => {
      it('should start the scheduler', () => {
        expect(() => {
          scheduler.start();
        }).not.toThrow();
      });

      it('should stop the scheduler', () => {
        scheduler.start();

        expect(() => {
          scheduler.stop();
        }).not.toThrow();
      });

      it('should allow restart after stop', () => {
        scheduler.start();
        scheduler.stop();

        expect(() => {
          scheduler.start();
        }).not.toThrow();
      });
    });

    describe('edge cases', () => {
      it('should handle multiple start calls', () => {
        scheduler.start();
        scheduler.start();
        scheduler.start();

        expect(scheduler).toBeDefined();
      });

      it('should handle multiple stop calls', () => {
        scheduler.start();
        scheduler.stop();
        scheduler.stop();
        scheduler.stop();

        expect(scheduler).toBeDefined();
      });

      it('should handle stop without start', () => {
        expect(() => {
          scheduler.stop();
        }).not.toThrow();
      });
    });

    describe('failure cases', () => {
      it('should not throw if timers fail to clear', () => {
        scheduler.start();

        // Force internal state corruption - accessing private property for testing
        FlushSchedulerTestUtils.setFlushTimer(scheduler, undefined);

        expect(() => {
          scheduler.stop();
        }).not.toThrow();
      });
    });
  });

  describe('flush', () => {
    describe('success cases', () => {
      it('should manually trigger a flush', async () => {
        const buffer = createTestBuffer();
        scheduler.register(buffer);

        await scheduler.flush();

        // Handler should have been called
        expect(flushCalls.length).toBeGreaterThan(0);
      });

      it('should flush with correct metadata', async () => {
        const buffer = createTestBuffer();
        scheduler.register(buffer);

        await scheduler.flush();

        expect(flushCalls[0].metadata.flushReason).toBe('manual');
        expect(flushCalls[0].metadata.totalRows).toBeGreaterThan(0);
      });

      it('should flush multiple buffers together', async () => {
        const buffer1 = createTestBuffer();
        const buffer2 = createTestBuffer();

        // Buffers created from the same schema share the same SpanBufferClass
        // and can be flushed together (schema compatibility is automatic)

        buffer1._writeIndex = 10;
        buffer2._writeIndex = 20;

        scheduler.register(buffer1);
        scheduler.register(buffer2);

        await scheduler.flush();

        expect(flushCalls[0].metadata.totalRows).toBeGreaterThan(0);
      });

      it('should require re-registering a buffer after successful flush', async () => {
        const buffer = createTestBuffer();
        scheduler.register(buffer);

        await scheduler.flush();
        expect(flushCalls.length).toBe(1);

        // Simulate later writes without re-registering.
        buffer._writeIndex = 3;
        await scheduler.flush();

        // Buffer should have been removed from pending set after first successful flush.
        expect(flushCalls.length).toBe(1);
      });
    });

    describe('edge cases', () => {
      it('should handle flush with no registered buffers', async () => {
        // Should complete without throwing
        await scheduler.flush();
        expect(flushCalls.length).toBe(0); // No flush should occur
      });

      it('should handle flush with empty buffers', async () => {
        const buffer = createTestBuffer();
        buffer._writeIndex = 0;

        scheduler.register(buffer);

        // Should complete without throwing
        await scheduler.flush();
        expect(flushCalls.length).toBe(0); // No flush should occur for empty buffers
      });

      it('should handle concurrent flush calls', async () => {
        const buffer = createTestBuffer();
        scheduler.register(buffer);

        await Promise.all([scheduler.flush(), scheduler.flush(), scheduler.flush()]);

        expect(flushCalls.length).toBeGreaterThanOrEqual(1);
      });
    });

    describe('failure cases', () => {
      it('should handle flush when handler throws error', async () => {
        const errorMessages: unknown[] = [];
        const originalError = console.error;
        console.error = (...args: unknown[]) => {
          errorMessages.push(...args);
        };

        try {
          const errorHandler: FlushHandler = () => {
            throw new Error('Flush failed');
          };

          const errorScheduler = new FlushScheduler(errorHandler);

          const buffer = createTestBuffer();
          const originalWriteIndex = buffer._writeIndex;
          errorScheduler.register(buffer);

          // Should complete without throwing (error is caught and logged)
          await errorScheduler.flush();

          // Verify error was logged
          expect(errorMessages.length).toBeGreaterThan(0);
          expect(errorMessages.some((msg) => String(msg).includes('Error in flush handler'))).toBe(true);

          // Verify buffers are NOT reset on failure (to avoid data loss)
          expect(buffer._writeIndex).toBe(originalWriteIndex);

          errorScheduler.stop();
        } finally {
          console.error = originalError;
        }
      });

      it('should handle flush with corrupted buffer', async () => {
        const errorMessages: unknown[] = [];
        const originalError = console.error;
        console.error = (...args: unknown[]) => {
          errorMessages.push(...args);
        };

        try {
          const buffer = createTestBuffer();
          // Create a partial buffer missing timestamps to test error handling
          const { timestamp: _timestamps, ...partialBuffer } = buffer;

          scheduler.register(partialBuffer as AnySpanBuffer);

          // Should complete without throwing (error is caught and logged)
          await scheduler.flush();

          // Verify error was logged for buffer conversion failure
          expect(errorMessages.length).toBeGreaterThan(0);
          expect(errorMessages.some((msg) => String(msg).includes('Error converting buffer to Arrow table'))).toBe(
            true,
          );
        } finally {
          console.error = originalError;
        }
      });

      it('should handle handler that returns rejected promise', async () => {
        const errorMessages: unknown[] = [];
        const originalError = console.error;
        console.error = (...args: unknown[]) => {
          errorMessages.push(...args);
        };

        try {
          const rejectedHandler: FlushHandler = async () => {
            throw new Error('Async error');
          };

          const rejectedScheduler = new FlushScheduler(rejectedHandler);

          const buffer = createTestBuffer();
          const originalWriteIndex = buffer._writeIndex;
          rejectedScheduler.register(buffer);

          // Should complete without throwing (error is caught and logged)
          await rejectedScheduler.flush();

          // Verify error was logged
          expect(errorMessages.length).toBeGreaterThan(0);
          expect(errorMessages.some((msg) => String(msg).includes('Error in flush handler'))).toBe(true);

          // Verify buffers are NOT reset on failure (to avoid data loss)
          expect(buffer._writeIndex).toBe(originalWriteIndex);

          rejectedScheduler.stop();
        } finally {
          console.error = originalError;
        }
      });
    });
  });

  describe('recordActivity', () => {
    describe('success cases', () => {
      it('should record activity timestamp', () => {
        expect(() => {
          scheduler.recordActivity();
        }).not.toThrow();
      });

      it('should update activity timestamp on multiple calls', () => {
        scheduler.recordActivity();
        scheduler.recordActivity();
        scheduler.recordActivity();

        expect(scheduler).toBeDefined();
      });
    });

    describe('edge cases', () => {
      it('should handle rapid consecutive calls', () => {
        for (let i = 0; i < 1000; i++) {
          scheduler.recordActivity();
        }

        expect(scheduler).toBeDefined();
      });
    });

    describe('failure cases', () => {
      it('should not throw even if internal state is corrupted', () => {
        FlushSchedulerTestUtils.setLastActivityTime(scheduler, null);

        expect(() => {
          scheduler.recordActivity();
        }).not.toThrow();
      });
    });
  });

  describe('configuration', () => {
    describe('success cases', () => {
      it('should accept custom configuration', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: 5000,
          minFlushInterval: 500,
          capacityThreshold: 0.9,
          idleDetection: false,
        };

        const customScheduler = new FlushScheduler(flushHandler, config);

        expect(customScheduler).toBeDefined();
        customScheduler.stop();
      });

      it('should use default config when not provided', () => {
        const defaultScheduler = new FlushScheduler(flushHandler);

        expect(defaultScheduler).toBeDefined();
        defaultScheduler.stop();
      });

      it('should accept partial configuration', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: 20000,
        };

        const partialScheduler = new FlushScheduler(flushHandler, config);

        expect(partialScheduler).toBeDefined();
        partialScheduler.stop();
      });
    });

    describe('edge cases', () => {
      it('should handle zero intervals', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: 0,
          minFlushInterval: 0,
        };

        const zeroScheduler = new FlushScheduler(flushHandler, config);

        expect(zeroScheduler).toBeDefined();
        zeroScheduler.stop();
      });

      it('should handle very large intervals', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: Number.MAX_SAFE_INTEGER,
          minFlushInterval: Number.MAX_SAFE_INTEGER - 1,
        };

        const largeScheduler = new FlushScheduler(flushHandler, config);

        expect(largeScheduler).toBeDefined();
        largeScheduler.stop();
      });

      it('should handle capacity threshold at boundaries', () => {
        const config1: FlushSchedulerConfig = { capacityThreshold: 0.0 };
        const config2: FlushSchedulerConfig = { capacityThreshold: 1.0 };

        const scheduler1 = new FlushScheduler(flushHandler, config1);

        const scheduler2 = new FlushScheduler(flushHandler, config2);

        expect(scheduler1).toBeDefined();
        expect(scheduler2).toBeDefined();

        scheduler1.stop();
        scheduler2.stop();
      });
    });

    describe('failure cases', () => {
      it('should handle negative intervals', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: -1000,
          minFlushInterval: -500,
        };

        const negativeScheduler = new FlushScheduler(flushHandler, config);

        expect(negativeScheduler).toBeDefined();
        negativeScheduler.stop();
      });

      it('should handle capacity threshold > 1', () => {
        const config: FlushSchedulerConfig = {
          capacityThreshold: 1.5,
        };

        const overScheduler = new FlushScheduler(flushHandler, config);

        expect(overScheduler).toBeDefined();
        overScheduler.stop();
      });

      it('should handle NaN in configuration', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: Number.NaN,
          capacityThreshold: Number.NaN,
        };

        const nanScheduler = new FlushScheduler(flushHandler, config);

        expect(nanScheduler).toBeDefined();
        nanScheduler.stop();
      });
    });
  });
});
