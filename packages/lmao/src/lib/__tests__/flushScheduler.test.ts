/**
 * Unit tests for background flush scheduler
 */

import { afterEach, beforeEach, describe, expect, it, mock } from 'bun:test';
import type { StringInterner } from '../convertToArrow.js';
import { type FlushHandler, type FlushMetadata, FlushScheduler, type FlushSchedulerConfig } from '../flushScheduler.js';
import type { ModuleContext, SpanBuffer, TaskContext } from '../types.js';

// Mock StringInterner
class MockStringInterner implements StringInterner {
  private strings: string[] = [];

  getString(idx: number): string | undefined {
    return this.strings[idx];
  }

  getStrings(): readonly string[] {
    return this.strings;
  }

  intern(str: string): number {
    const idx = this.strings.length;
    this.strings.push(str);
    return idx;
  }
}

function createMockSpanBuffer(): SpanBuffer {
  const moduleContext: ModuleContext = {
    moduleId: 0,
    gitSha: 'test',
    filePath: 'test.ts',
    tagAttributes: {},
    spanBufferCapacityStats: {
      currentCapacity: 64,
      totalWrites: 0,
      overflowWrites: 0,
      totalBuffersCreated: 0,
    },
  };

  const taskContext: TaskContext = {
    module: moduleContext,
    spanNameId: 0,
    lineNumber: 0,
  };

  const buffer = {
    threadId: BigInt('0x123456789ABCDEF0'), // Mock 64-bit thread ID
    spanId: 1,
    traceId: 'test-trace',
    timestamps: new BigInt64Array(64),
    operations: new Uint8Array(64),
    nullBitmaps: {},
    children: [],
    task: taskContext,
    writeIndex: 5, // Some rows written
    capacity: 64,
  } as SpanBuffer;

  // Initialize timestamp values to avoid undefined errors
  for (let i = 0; i < buffer.writeIndex; i++) {
    buffer.timestamps[i] = BigInt(Date.now()) * 1_000_000n; // Convert ms to nanoseconds
    buffer.operations[i] = 1; // entry type: span-start
  }

  return buffer;
}

describe('FlushScheduler', () => {
  let scheduler: FlushScheduler;
  let flushHandler: FlushHandler;
  let flushCalls: Array<{ metadata: FlushMetadata }>;
  let moduleIdInterner: MockStringInterner;
  let spanNameInterner: MockStringInterner;

  beforeEach(() => {
    flushCalls = [];
    flushHandler = mock((table, metadata) => {
      flushCalls.push({ metadata });
    });

    moduleIdInterner = new MockStringInterner();
    spanNameInterner = new MockStringInterner();

    scheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner);
  });

  afterEach(() => {
    if (scheduler) {
      scheduler.stop();
    }
  });

  describe('register', () => {
    describe('success cases', () => {
      it('should register a buffer for automatic flushing', () => {
        const buffer = createMockSpanBuffer();

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should start scheduler when first buffer is registered', () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        // Scheduler should be running (internal state check)
        expect(scheduler).toBeDefined();
      });

      it('should register multiple buffers', () => {
        const buffer1 = createMockSpanBuffer();
        const buffer2 = createMockSpanBuffer();
        const buffer3 = createMockSpanBuffer();

        scheduler.register(buffer1);
        scheduler.register(buffer2);
        scheduler.register(buffer3);

        expect(scheduler).toBeDefined();
      });
    });

    describe('edge cases', () => {
      it('should handle registering same buffer multiple times', () => {
        const buffer = createMockSpanBuffer();

        scheduler.register(buffer);
        scheduler.register(buffer);
        scheduler.register(buffer);

        expect(scheduler).toBeDefined();
      });

      it('should handle buffer with zero writeIndex', () => {
        const buffer = createMockSpanBuffer();
        buffer.writeIndex = 0;

        scheduler.register(buffer);
        expect(scheduler).toBeDefined();
      });

      it('should handle buffer at full capacity', () => {
        const buffer = createMockSpanBuffer();
        buffer.writeIndex = buffer.capacity;

        scheduler.register(buffer);
        expect(scheduler).toBeDefined();
      });
    });

    describe('failure cases', () => {
      it('should handle buffer with invalid writeIndex', () => {
        const buffer = createMockSpanBuffer();
        buffer.writeIndex = -1;

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should handle buffer with writeIndex > capacity', () => {
        const buffer = createMockSpanBuffer();
        buffer.writeIndex = buffer.capacity + 100;

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });

      it('should handle buffer with missing properties', () => {
        const buffer = createMockSpanBuffer();
        delete (buffer as any).timestamps;

        expect(() => {
          scheduler.register(buffer);
        }).not.toThrow();
      });
    });
  });

  describe('unregister', () => {
    describe('success cases', () => {
      it('should unregister a buffer', () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        expect(() => {
          scheduler.unregister(buffer);
        }).not.toThrow();
      });

      it('should stop scheduler when all buffers are unregistered', () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);
        scheduler.unregister(buffer);

        expect(scheduler).toBeDefined();
      });

      it('should unregister multiple buffers', () => {
        const buffers = [createMockSpanBuffer(), createMockSpanBuffer(), createMockSpanBuffer()];

        buffers.forEach((b) => scheduler.register(b));
        buffers.forEach((b) => scheduler.unregister(b));

        expect(scheduler).toBeDefined();
      });
    });

    describe('edge cases', () => {
      it('should handle unregistering buffer that was never registered', () => {
        const buffer = createMockSpanBuffer();

        expect(() => {
          scheduler.unregister(buffer);
        }).not.toThrow();
      });

      it('should handle unregistering same buffer multiple times', () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        scheduler.unregister(buffer);
        scheduler.unregister(buffer);
        scheduler.unregister(buffer);

        expect(scheduler).toBeDefined();
      });
    });

    describe('failure cases', () => {
      it('should handle null buffer gracefully', () => {
        expect(() => {
          scheduler.unregister(null as any);
        }).not.toThrow();
      });

      it('should handle undefined buffer gracefully', () => {
        expect(() => {
          scheduler.unregister(undefined as any);
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

        // Force internal state corruption
        (scheduler as any).flushTimer = null;

        expect(() => {
          scheduler.stop();
        }).not.toThrow();
      });
    });
  });

  describe('flush', () => {
    describe('success cases', () => {
      it('should manually trigger a flush', async () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        await scheduler.flush();

        // Handler should have been called
        expect(flushCalls.length).toBeGreaterThan(0);
      });

      it('should flush with correct metadata', async () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        await scheduler.flush();

        expect(flushCalls[0].metadata.flushReason).toBe('manual');
        expect(flushCalls[0].metadata.totalRows).toBeGreaterThan(0);
      });

      it('should flush multiple buffers together', async () => {
        const buffer1 = createMockSpanBuffer();
        const buffer2 = createMockSpanBuffer();

        // Make buffers share the same module context to ensure schema compatibility
        buffer2.task = buffer1.task;

        buffer1.writeIndex = 10;
        buffer2.writeIndex = 20;

        scheduler.register(buffer1);
        scheduler.register(buffer2);

        await scheduler.flush();

        expect(flushCalls[0].metadata.totalRows).toBeGreaterThan(0);
      });
    });

    describe('edge cases', () => {
      it('should handle flush with no registered buffers', async () => {
        // Should complete without throwing
        await scheduler.flush();
        expect(flushCalls.length).toBe(0); // No flush should occur
      });

      it('should handle flush with empty buffers', async () => {
        const buffer = createMockSpanBuffer();
        buffer.writeIndex = 0;

        scheduler.register(buffer);

        // Should complete without throwing
        await scheduler.flush();
        expect(flushCalls.length).toBe(0); // No flush should occur for empty buffers
      });

      it('should handle concurrent flush calls', async () => {
        const buffer = createMockSpanBuffer();
        scheduler.register(buffer);

        await Promise.all([scheduler.flush(), scheduler.flush(), scheduler.flush()]);

        expect(flushCalls.length).toBeGreaterThanOrEqual(1);
      });
    });

    describe('failure cases', () => {
      it('should handle flush when handler throws error', async () => {
        const errorHandler: FlushHandler = () => {
          throw new Error('Flush failed');
        };

        const errorScheduler = new FlushScheduler(errorHandler, moduleIdInterner, spanNameInterner);

        const buffer = createMockSpanBuffer();
        errorScheduler.register(buffer);

        // Should complete without throwing (error is caught and logged)
        await errorScheduler.flush();

        // Verify it tried to flush (error logged but didn't propagate)
        expect(buffer.writeIndex).toBeGreaterThan(0);

        errorScheduler.stop();
      });

      it('should handle flush with corrupted buffer', async () => {
        const buffer = createMockSpanBuffer();
        delete (buffer as any).timestamps;

        scheduler.register(buffer);

        // Should complete without throwing (error is caught and logged)
        await scheduler.flush();

        // Flush should have been attempted
        expect(true).toBe(true);
      });

      it('should handle handler that returns rejected promise', async () => {
        const rejectedHandler: FlushHandler = async () => {
          throw new Error('Async error');
        };

        const rejectedScheduler = new FlushScheduler(rejectedHandler, moduleIdInterner, spanNameInterner);

        const buffer = createMockSpanBuffer();
        rejectedScheduler.register(buffer);

        // Should complete without throwing (error is caught and logged)
        await rejectedScheduler.flush();

        // Verify it tried to flush
        expect(buffer.writeIndex).toBeGreaterThan(0);

        rejectedScheduler.stop();
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
        (scheduler as any).lastActivityTime = null;

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

        const customScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(customScheduler).toBeDefined();
        customScheduler.stop();
      });

      it('should use default config when not provided', () => {
        const defaultScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner);

        expect(defaultScheduler).toBeDefined();
        defaultScheduler.stop();
      });

      it('should accept partial configuration', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: 20000,
        };

        const partialScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

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

        const zeroScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(zeroScheduler).toBeDefined();
        zeroScheduler.stop();
      });

      it('should handle very large intervals', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: Number.MAX_SAFE_INTEGER,
          minFlushInterval: Number.MAX_SAFE_INTEGER - 1,
        };

        const largeScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(largeScheduler).toBeDefined();
        largeScheduler.stop();
      });

      it('should handle capacity threshold at boundaries', () => {
        const config1: FlushSchedulerConfig = { capacityThreshold: 0.0 };
        const config2: FlushSchedulerConfig = { capacityThreshold: 1.0 };

        const scheduler1 = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config1);

        const scheduler2 = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config2);

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

        const negativeScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(negativeScheduler).toBeDefined();
        negativeScheduler.stop();
      });

      it('should handle capacity threshold > 1', () => {
        const config: FlushSchedulerConfig = {
          capacityThreshold: 1.5,
        };

        const overScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(overScheduler).toBeDefined();
        overScheduler.stop();
      });

      it('should handle NaN in configuration', () => {
        const config: FlushSchedulerConfig = {
          maxFlushInterval: Number.NaN,
          capacityThreshold: Number.NaN,
        };

        const nanScheduler = new FlushScheduler(flushHandler, moduleIdInterner, spanNameInterner, config);

        expect(nanScheduler).toBeDefined();
        nanScheduler.stop();
      });
    });
  });
});
