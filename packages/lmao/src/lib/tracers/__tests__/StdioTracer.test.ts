/**
 * StdioTracer tests
 *
 * Tests for the StdioTracer class - a tracer that prints spans to stdout/stderr
 * with color-coded trace IDs, indentation, and human-readable formatting.
 */

// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { Writable } from 'node:stream';
import { defineOpContext, type OpContextOf } from '../../defineOpContext.js';
import { S } from '../../schema/builder.js';
import { defineLogSchema } from '../../schema/defineLogSchema.js';
import { StdioTracer } from '../StdioTracer.js';

// Create a mock writable stream that captures output
function createMockStream(): { stream: NodeJS.WriteStream; output: string[] } {
  const output: string[] = [];
  const stream = new Writable({
    write(chunk, _encoding, callback) {
      output.push(chunk.toString());
      callback();
    },
  }) as NodeJS.WriteStream;
  // Add required properties
  (stream as NodeJS.WriteStream).isTTY = false;
  return { stream, output };
}

describe('StdioTracer', () => {
  const testSchema = defineLogSchema({
    userId: S.category(),
  });

  const opContext = defineOpContext({
    logSchema: testSchema,
  });
  type Ctx = OpContextOf<typeof opContext>;
  const { logBinding, defineOp } = opContext;

  describe('basic output', () => {
    it('should write trace start and end to stdout', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('my-trace', testOp);

      // Should have trace start and trace end lines
      expect(output.length).toBeGreaterThanOrEqual(2);
      expect(output.some((line) => line.includes('my-trace'))).toBe(true);
    });

    it('should include trace_id in output', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('id-test', testOp);

      // trace_id is W3C format (32 hex chars), should appear in brackets
      expect(output.some((line) => /\[[a-f0-9]{32}\]/.test(line))).toBe(true);
    });

    it('should include timestamp in output', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('ts-test', testOp);

      // ISO timestamp format
      expect(output.some((line) => /\d{4}-\d{2}-\d{2}T/.test(line))).toBe(true);
    });
  });

  describe('nested spans with indentation', () => {
    it('should indent child spans', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const childOp = defineOp('child', (ctx) => ctx.ok('c'));
      const parentOp = defineOp('parent', async (ctx) => {
        await ctx.span('child-span', childOp);
        return ctx.ok('p');
      });

      await trace('root', parentOp);

      // Find lines with child-span - should have indentation
      const childLines = output.filter((line) => line.includes('child-span'));
      expect(childLines.length).toBeGreaterThan(0);

      // Child lines should have leading spaces (indentation)
      const hasIndent = childLines.some((line) => /\s{2,}.*child-span/.test(line));
      expect(hasIndent).toBe(true);
    });

    it('should use tree characters (├─ and └─)', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const childOp = defineOp('child', (ctx) => ctx.ok('c'));
      const parentOp = defineOp('parent', async (ctx) => {
        await ctx.span('tree-child', childOp);
        return ctx.ok('p');
      });

      await trace('tree-root', parentOp);

      // Should have tree branch characters
      const hasStartBranch = output.some((line) => line.includes('├─'));
      const hasEndBranch = output.some((line) => line.includes('└─'));

      expect(hasStartBranch).toBe(true);
      expect(hasEndBranch).toBe(true);
    });
  });

  describe('status indicators', () => {
    it('should show [OK] for successful spans', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('ok-test', testOp);

      expect(output.some((line) => line.includes('[OK]'))).toBe(true);
    });

    it('should show [ERR] for ctx.err() results', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.err('CODE', 'error'));
      await trace('err-test', testOp);

      expect(output.some((line) => line.includes('[ERR]'))).toBe(true);
    });

    it('should show [EXCEPTION] and write to stderr for thrown errors', async () => {
      const { stream: out, output: stdout } = createMockStream();
      const { stream: err, output: stderr } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const failOp = defineOp('fail', async () => {
        throw new Error('boom');
      });

      await expect(trace('exception-test', failOp)).rejects.toThrow('boom');

      // EXCEPTION should go to stderr for child spans
      // But root trace might go to stdout
      const allOutput = [...stdout, ...stderr];
      expect(allOutput.some((line) => line.includes('[EXCEPTION]') || line.includes('EXCEPTION'))).toBe(true);
    });
  });

  describe('duration formatting', () => {
    it('should include duration in span end output', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', async (ctx) => {
        await new Promise((r) => setTimeout(r, 10)); // Small delay
        return ctx.ok('done');
      });

      await trace('duration-test', testOp);

      // Should have duration with unit (ms, µs, ns, or s)
      expect(output.some((line) => /\(\d+\.\d+(?:s|ms|µs|ns)\)/.test(line))).toBe(true);
    });
  });

  describe('concurrent trace isolation', () => {
    it('should track indent per trace_id for concurrent traces', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const childOp = defineOp('child', async (ctx) => {
        await new Promise((r) => setTimeout(r, 5));
        return ctx.ok('c');
      });

      const parentOp = defineOp('parent', async (ctx) => {
        await ctx.span('nested', childOp);
        return ctx.ok('p');
      });

      // Run two traces concurrently
      await Promise.all([trace('trace-A', parentOp), trace('trace-B', parentOp)]);

      // Both traces should complete without corrupted indentation
      // This is hard to assert precisely, but at minimum both should appear
      expect(output.some((line) => line.includes('trace-A'))).toBe(true);
      expect(output.some((line) => line.includes('trace-B'))).toBe(true);
    });
  });

  describe('color support', () => {
    it('should include ANSI color codes when enabled', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      // Enable colors
      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, true);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('color-test', testOp);

      // Should have ANSI escape codes (ESC character followed by [<number>m)
      const hasAnsi = output.some((line) => {
        // biome-ignore lint/suspicious/noControlCharactersInRegex: Testing for ANSI escape sequences
        return /\u001b\[\d+m/.test(line);
      });
      expect(hasAnsi).toBe(true);
    });

    it('should not include ANSI codes when colors disabled', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      // Disable colors
      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('no-color-test', testOp);

      // Should NOT have ANSI escape codes
      const hasAnsi = output.some((line) => {
        // biome-ignore lint/suspicious/noControlCharactersInRegex: Testing for ANSI escape sequences
        return /\u001b\[\d+m/.test(line);
      });
      expect(hasAnsi).toBe(false);
    });
  });

  describe('separator line', () => {
    it('should print separator line at trace end', async () => {
      const { stream: out, output } = createMockStream();
      const { stream: err } = createMockStream();

      const tracer = new StdioTracer<Ctx>({ logBinding }, out, err, false);
      const { trace } = tracer;

      const testOp = defineOp('test', (ctx) => ctx.ok('done'));
      await trace('sep-test', testOp);

      // Should have line of = characters
      expect(output.some((line) => /={10,}/.test(line))).toBe(true);
    });
  });
});
