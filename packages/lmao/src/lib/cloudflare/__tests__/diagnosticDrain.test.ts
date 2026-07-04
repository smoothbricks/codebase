// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineLogSchema, defineOpContext, S } from '../../defineOpContext.js';
import {
  DiagnosticDrainTracer,
  PipelinesStreamTransport,
  QueuesFallbackTransport,
  type TraceChunkQueueMessage,
} from '../diagnosticDrain.js';
import type { TraceRow } from '../traceRows.js';

const testSchema = defineLogSchema({
  userId: S.category(),
});

const opContext = defineOpContext({ logSchema: testSchema });
const { defineOp } = opContext;

function makeRows(count: number): TraceRow[] {
  const rows: TraceRow[] = [];
  for (let i = 0; i < count; i++) {
    rows.push({
      trace_id: 'trace-x',
      span_id: 1,
      parent_span_id: 0,
      row_index: i,
      entry_type: 1,
      timestamp_ns: (i + 1) * 1_000_000,
      message: null,
    });
  }
  return rows;
}

describe('DiagnosticDrainTracer', () => {
  it('flush drains completed roots and sends them as rows through the Pipelines-shaped binding', async () => {
    const sentBatches: TraceRow[][] = [];
    // env.TRACE_STREAM.send-shaped fn — array of JSON-serializable records, resolves on ingest.
    const transport = new PipelinesStreamTransport(async (rows) => {
      sentBatches.push(rows);
    });
    const tracer = new DiagnosticDrainTracer(opContext, { ...createTestTracerOptions(), transport });
    const { trace, flush } = tracer;

    const op = defineOp('work', (ctx) => {
      ctx.tag.userId('user-1');
      return ctx.ok('done');
    });
    await trace('first', op);
    await trace('second', op);

    expect(tracer.queue).toHaveLength(2);
    await flush();

    // One send per completed root buffer, queue fully drained.
    expect(tracer.queue).toHaveLength(0);
    expect(sentBatches).toHaveLength(2);

    const firstRows = sentBatches[0];
    expect(firstRows.length).toBeGreaterThan(0);
    expect(firstRows[0].trace_id).toBeString();
    expect(firstRows[0].message).toBe('first');
    expect(firstRows[0].entry_type).toBeNumber();
    expect(firstRows[0].timestamp_ns).toBeNumber();
    // User schema columns ride flat on the row.
    expect(firstRows.some((row) => row.userId === 'user-1')).toBe(true);
    // Rows must be JSON-serializable records.
    expect(JSON.parse(JSON.stringify(firstRows))).toEqual(firstRows);
  });

  it('flush never rejects on transport failure — errors surface via onSendError only', async () => {
    const errors: { error: unknown; rows: readonly TraceRow[] }[] = [];
    const tracer = new DiagnosticDrainTracer(opContext, {
      ...createTestTracerOptions(),
      transport: {
        send: () => Promise.reject(new Error('pipeline unavailable')),
      },
      onSendError: (error, rows) => {
        errors.push({ error, rows });
      },
    });
    const { trace, flush } = tracer;

    const op = defineOp('work', (ctx) => ctx.ok('done'));
    await trace('doomed', op);

    await flush();

    expect(errors).toHaveLength(1);
    expect(errors[0].error).toBeInstanceOf(Error);
    expect(errors[0].rows.length).toBeGreaterThan(0);
    // Buffers are consumed even on failure — no unbounded isolate accumulation.
    expect(tracer.queue).toHaveLength(0);
  });

  it('flush with an empty queue is a no-op', async () => {
    const sentBatches: TraceRow[][] = [];
    const tracer = new DiagnosticDrainTracer(opContext, {
      ...createTestTracerOptions(),
      transport: new PipelinesStreamTransport(async (rows) => {
        sentBatches.push(rows);
      }),
    });

    await tracer.flush();
    expect(sentBatches).toHaveLength(0);
  });
});

describe('QueuesFallbackTransport', () => {
  it('ships batched chunk-envelope messages, never per-span messages', async () => {
    const messages: TraceChunkQueueMessage[] = [];
    const transport = new QueuesFallbackTransport({
      fileRef: 'queue://test-worker',
      maxRowsPerMessage: 2,
      sendMessage: async (message) => {
        messages.push(message);
      },
    });

    await transport.send(makeRows(5));

    expect(messages).toHaveLength(3);
    expect(messages.map((m) => m.rows.length)).toEqual([2, 2, 1]);
    for (const message of messages) {
      expect(message.envelope.chunk_id).toStartWith('chunk_');
      expect(message.envelope.file_ref).toBe('queue://test-worker');
      expect(message.envelope.row_count).toBe(message.rows.length);
      expect(message.envelope.started_at_ms).toBeLessThanOrEqual(message.envelope.ended_at_ms);
    }
  });

  it('chunk_id is deterministic for the same batch — redelivery dedupes by chunk_id', async () => {
    const chunkIds: string[] = [];
    const transport = new QueuesFallbackTransport({
      fileRef: 'queue://test-worker',
      maxRowsPerMessage: 10,
      sendMessage: async (message) => {
        chunkIds.push(message.envelope.chunk_id);
      },
    });

    const batch = makeRows(3);
    await transport.send(batch);
    await transport.send(batch);
    await transport.send(makeRows(4));

    expect(chunkIds[0]).toBe(chunkIds[1]);
    expect(chunkIds[2]).not.toBe(chunkIds[0]);
  });

  it('rejects a non-positive batch size at construction', () => {
    expect(
      () =>
        new QueuesFallbackTransport({
          fileRef: 'queue://test-worker',
          maxRowsPerMessage: 0,
          sendMessage: async () => {},
        }),
    ).toThrow('maxRowsPerMessage');
  });
});
