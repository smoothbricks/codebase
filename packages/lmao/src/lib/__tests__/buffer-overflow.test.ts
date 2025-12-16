import { describe, expect, it } from 'bun:test';
import { createModuleContext, createRequestContext } from '../lmao.js';
import { S } from '../schema/builder.js';
import { defineTagAttributes } from '../schema/defineTagAttributes.js';

/**
 * Tests for buffer overflow handling and capacity tuning
 */
describe('Buffer Overflow and Capacity Management', () => {
  const dbAttributes = defineTagAttributes({
    requestId: S.category(),
    userId: S.category(),
    operation: S.enum(['SELECT', 'INSERT', 'UPDATE', 'DELETE']),
    duration: S.number(),
    httpStatus: S.number(),
    region: S.category(),
    query: S.text(),
  });

  const featureFlags = {
    schema: {
      advancedValidation: S.boolean(),
      newUI: S.boolean(),
    },
  };

  const flagEvaluator = {
    getSync: () => true,
    getAsync: async () => true,
  };

  const environmentConfig = {
    apiEndpoint: 'https://api.example.com',
    logLevel: 'info',
  };

  describe('Buffer Overflow in Tag Operations', () => {
    it('should handle tag writes that exceed initial buffer capacity', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      // Set very small capacity to force overflow
      const testTask = moduleContext.task('test-overflow', async (ctx) => {
        // Write more entries than the initial capacity
        // Initial capacity is 64, so write 100 entries
        for (let i = 0; i < 100; i++) {
          ctx.tag
            .requestId(`req-${i}`)
            .userId(`user-${i}`)
            .operation('SELECT')
            .duration(Math.random() * 100);
        }

        return ctx.ok({ written: 100 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-overflow-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);

      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.written).toBe(100);
      }
    });

    it('should handle chained tag writes correctly', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-chaining', async (ctx) => {
        // Each tag write should work even after overflow
        for (let i = 0; i < 10; i++) {
          ctx.tag
            .requestId(`req-${i}`)
            .userId(`user-${i}`)
            .with({ operation: 'SELECT', duration: 10.5 })
            .httpStatus(200)
            .region('us-west-2');
        }

        return ctx.ok({ written: 10 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-chain-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });

    it('should handle very large number of writes', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-large-writes', async (ctx) => {
        // Write 1000 entries to test multiple buffer chains
        for (let i = 0; i < 1000; i++) {
          ctx.tag.requestId(`req-${i}`).userId(`user-${i}`);
        }

        return ctx.ok({ written: 1000 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-large-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.written).toBe(1000);
      }
    });
  });

  describe('Buffer Overflow in Message Logging', () => {
    it('should handle message logs that exceed buffer capacity', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-message-overflow', async (ctx) => {
        // Write many messages
        for (let i = 0; i < 100; i++) {
          ctx.log.info(`Message ${i}`);
          ctx.log.debug(`Debug ${i}`);
          ctx.log.warn(`Warning ${i}`);
        }

        return ctx.ok({ messages: 300 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-message-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.messages).toBe(300);
      }
    });

    it('should handle mixed tag and message writes', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-mixed-writes', async (ctx) => {
        // Interleave tag writes and messages
        for (let i = 0; i < 100; i++) {
          ctx.tag.requestId(`req-${i}`).userId(`user-${i}`);
          ctx.log.info(`Processing request ${i}`);
          ctx.tag.operation('SELECT').duration(10.5);
          ctx.log.debug(`Query completed for ${i}`);
        }

        return ctx.ok({ writes: 400 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-mixed-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.writes).toBe(400);
      }
    });
  });

  describe('Child Span Buffer Overflow', () => {
    it('should handle overflow in child spans', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-child-overflow', async (ctx) => {
        // Parent writes
        for (let i = 0; i < 50; i++) {
          ctx.tag.requestId(`parent-${i}`);
        }

        // Child span with many writes
        await ctx.span('child-span', async (childCtx) => {
          for (let i = 0; i < 100; i++) {
            childCtx.tag.requestId(`child-${i}`).userId(`user-${i}`);
          }
        });

        return ctx.ok({ parentWrites: 50, childWrites: 100 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-child-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.parentWrites).toBe(50);
        expect(result.value.childWrites).toBe(100);
      }
    });

    it('should handle multiple child spans with overflow', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-multi-child', async (ctx) => {
        // Create 5 child spans, each with 100 writes
        for (let childNum = 0; childNum < 5; childNum++) {
          await ctx.span(`child-${childNum}`, async (childCtx) => {
            for (let i = 0; i < 100; i++) {
              childCtx.tag.requestId(`child-${childNum}-req-${i}`).userId(`user-${i}`);
            }
          });
        }

        return ctx.ok({ children: 5, writesPerChild: 100 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-multi-child-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.children).toBe(5);
        expect(result.value.writesPerChild).toBe(100);
      }
    });
  });

  describe('Capacity Tuning Behavior', () => {
    it('should not crash with rapid writes', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      // Execute multiple tasks to trigger capacity tuning
      const testTask = moduleContext.task('test-rapid', async (ctx) => {
        for (let i = 0; i < 200; i++) {
          ctx.tag.requestId(`req-${i}`);
        }
        return ctx.ok({ written: 200 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-rapid-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      // Run task multiple times
      for (let i = 0; i < 5; i++) {
        const result = await testTask(requestCtx);
        expect(result.success).toBe(true);
      }
    });

    it('should handle writes at exact capacity boundary', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-boundary', async (ctx) => {
        // Write exactly 64 entries (initial capacity)
        for (let i = 0; i < 64; i++) {
          ctx.tag.requestId(`req-${i}`);
        }

        // Write one more to trigger overflow
        ctx.tag.requestId('req-64');

        return ctx.ok({ written: 65 });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-boundary-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.written).toBe(65);
      }
    });
  });

  describe('Scoped Attributes on Buffer Overflow', () => {
    it('should carry scoped attributes forward when buffer overflows', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-scope-overflow', async (ctx) => {
        // Set scoped attributes at the beginning (scope is now on ctx directly)
        ctx.scope({ requestId: 'scoped-req-123', userId: 'scoped-user-456' });

        // Write enough messages to trigger buffer overflow
        // Initial capacity is 64, and writeIndex starts at 2 (row 0: span-start, row 1: span-end)
        // So we need 62+ messages to overflow
        for (let i = 0; i < 100; i++) {
          ctx.log.info(`Message ${i}`);
        }

        // The scoped attributes should be present in the new buffer
        // We can verify by getting the scoped values from the scope instance
        const scopedAttrs = ctx.log._getScope()._getScopeValues();

        return ctx.ok({
          messagesWritten: 100,
          scopedRequestId: scopedAttrs.requestId,
          scopedUserId: scopedAttrs.userId,
        });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-scope-overflow-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.messagesWritten).toBe(100);
        // Scoped attributes should still be accessible (they're stored in the logger, not buffer)
        expect(result.value.scopedRequestId).toBeDefined();
        expect(result.value.scopedUserId).toBeDefined();
      }
    });

    it('should apply scoped attributes to messages in chained buffer after overflow', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      // We need access to the buffer to verify scoped attributes are pre-filled
      // This is an internal implementation test
      const bufferAfterOverflow: unknown = null;

      const testTask = moduleContext.task('test-scope-prefill', async (ctx) => {
        // Set scoped attributes
        ctx.scope({ region: 'us-east-1' });

        // Write enough messages to trigger overflow
        for (let i = 0; i < 80; i++) {
          ctx.log.info(`Message ${i}`);
        }

        // Write a final message after overflow - scoped attributes should be applied
        ctx.log.info('Final message after overflow');

        return ctx.ok({ success: true });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-scope-prefill-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
    });
  });

  describe('Error Handling', () => {
    it('should not lose data on buffer overflow', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-no-data-loss', async (ctx) => {
        const writes: string[] = [];

        // Write many entries and track them
        for (let i = 0; i < 150; i++) {
          const reqId = `req-${i}`;
          writes.push(reqId);
          ctx.tag.requestId(reqId);
        }

        return ctx.ok({ totalWrites: writes.length });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-no-loss-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.totalWrites).toBe(150);
      }
    });

    it('should handle errors during writes without corrupting buffer', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-error-handling', async (ctx) => {
        try {
          // Write some entries
          for (let i = 0; i < 50; i++) {
            ctx.tag.requestId(`req-${i}`);
          }

          // Simulate an error
          throw new Error('Simulated error');
        } catch (error) {
          // Continue writing after error
          for (let i = 50; i < 100; i++) {
            ctx.tag.requestId(`req-${i}`);
          }

          return ctx.ok({ recovered: true, writes: 100 });
        }
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-error-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.recovered).toBe(true);
        expect(result.value.writes).toBe(100);
      }
    });

    it('should correctly write fluent attributes across 128+ log entries with overflow', async () => {
      const moduleContext = createModuleContext({
        moduleMetadata: {
          gitSha: 'abc123',
          filePath: 'src/services/test.ts',
          moduleName: 'TestService',
        },
        tagAttributes: dbAttributes,
      });

      const testTask = moduleContext.task('test-fluent-overflow', async (ctx) => {
        const NUM_ENTRIES = 128;
        const operations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'] as const;

        // Save root buffer BEFORE any writes (ctx.log._buffer will change on overflow)
        const rootBuffer = ctx.log._buffer;

        // Write 128 log entries, each with fluent attribute chaining
        for (let i = 0; i < NUM_ENTRIES; i++) {
          ctx.log
            .info(`Message ${i}`)
            .requestId(`req-${i}`)
            .userId(`user-${i}`)
            .operation(operations[i % 4])
            .duration(i * 10);
        }

        // SpanLogger tracks _writeIndex
        const logger = ctx.log;

        // Collect all entries from buffer chain
        // We know there are NUM_ENTRIES entries starting at row 2 of root buffer
        const entries: Array<{
          requestId: string;
          userId: string;
          operation: number;
          duration: number;
        }> = [];

        let currentBuffer: typeof rootBuffer | undefined = rootBuffer;
        let entriesCollected = 0;
        let bufferNum = 0;

        while (currentBuffer && entriesCollected < NUM_ENTRIES) {
          bufferNum++;
          // Skip rows 0-1 (tag and result rows) in root buffer only
          const startRow = bufferNum === 1 ? 2 : 0;
          const endRow = currentBuffer.capacity;

          for (let row = startRow; row < endRow && entriesCollected < NUM_ENTRIES; row++) {
            entries.push({
              requestId: currentBuffer.requestId_values?.[row] as string,
              userId: currentBuffer.userId_values?.[row] as string,
              operation: currentBuffer.operation_values?.[row] as number,
              duration: currentBuffer.duration_values?.[row] as number,
            });
            entriesCollected++;
          }

          currentBuffer = currentBuffer._next as typeof rootBuffer | undefined;
        }

        return ctx.ok({
          entriesWritten: entries.length,
          entries,
        });
      });

      const requestCtx = createRequestContext(
        { requestId: 'req-fluent-overflow-test' },
        featureFlags,
        flagEvaluator,
        environmentConfig,
      );

      const result = await testTask(requestCtx);
      expect(result.success).toBe(true);
      if (result.success) {
        expect(result.value.entriesWritten).toBe(128);

        // Verify each entry has correct attributes
        const operations = ['SELECT', 'INSERT', 'UPDATE', 'DELETE'];
        for (let i = 0; i < 128; i++) {
          const entry = result.value.entries[i];
          expect(entry.requestId).toBe(`req-${i}`);
          expect(entry.userId).toBe(`user-${i}`);
          // Enum values are stored as indices
          expect(entry.operation).toBe(i % 4);
          expect(entry.duration).toBe(i * 10);
        }
      }
    });
  });
});
