// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { createTestTracerOptions } from '../../__tests__/test-helpers.js';
import { defineLogSchema, defineOpContext, S } from '../../defineOpContext.js';
import { Err, Ok } from '../../result.js';
import { ClassSplitTracer, type DeliveryClassifier } from '../classSplit.js';
import { type CollectorSendFailure, FakeCollectorClient } from '../collectorClient.js';
import type { TraceRow } from '../traceRows.js';

const testSchema = defineLogSchema({
  lane: S.category(),
});

const opContext = defineOpContext({ logSchema: testSchema });
const { defineOp } = opContext;

// Injected classifier seam — here: a row is billing-grade iff it carries the billing lane tag.
const classifyByLane: DeliveryClassifier = (row) => (row.lane === 'billing' ? 'billing-grade' : 'diagnostic');

const billableOp = defineOp('billable', (ctx) => {
  ctx.tag.lane('billing');
  return ctx.ok('charged');
});

const diagnosticOp = defineOp('diagnostic', (ctx) => ctx.ok('observed'));

interface Harness {
  tracer: ClassSplitTracer<typeof opContext>;
  collector: FakeCollectorClient;
  diagnosticBatches: (readonly TraceRow[])[];
  diagnosticErrors: unknown[];
  lateFailures: CollectorSendFailure[];
}

function makeHarness(options?: { diagnosticsFail?: boolean }): Harness {
  const collector = new FakeCollectorClient();
  const diagnosticBatches: (readonly TraceRow[])[] = [];
  const diagnosticErrors: unknown[] = [];
  const lateFailures: CollectorSendFailure[] = [];
  const tracer = new ClassSplitTracer(opContext, {
    ...createTestTracerOptions(),
    collector,
    diagnostics: {
      send: async (rows) => {
        if (options?.diagnosticsFail) throw new Error('diagnostic lane down');
        diagnosticBatches.push(rows);
      },
    },
    classify: classifyByLane,
    onDiagnosticSendError: (error) => {
      diagnosticErrors.push(error);
    },
    onLateBillableSendFailure: (failure) => {
      lateFailures.push(failure);
    },
  });
  return { tracer, collector, diagnosticBatches, diagnosticErrors, lateFailures };
}

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe('ClassSplitTracer', () => {
  describe('settleBillable (awaited within the request)', () => {
    it('returns Ok(null) when the request produced no billing-grade rows', async () => {
      const { tracer, collector, diagnosticBatches } = makeHarness();
      await tracer.trace('req', diagnosticOp);

      const settled = await tracer.settleBillable();
      expect(settled).toBeInstanceOf(Ok);
      if (settled instanceof Ok) expect(settled.value).toBeNull();
      expect(collector.acked).toHaveLength(0);
      // Diagnostic rows are staged, not sent yet — that is flush()'s lazy job.
      expect(diagnosticBatches).toHaveLength(0);
    });

    it('sends billing-grade rows to the collector and resolves only on its ack', async () => {
      const { tracer, collector } = makeHarness();
      collector.holdSends();
      await tracer.trace('req', billableOp);

      let settledFlag = false;
      const pending = tracer.settleBillable().then((result) => {
        settledFlag = true;
        return result;
      });

      await tick();
      // Send-before-response: the request must still be blocked here.
      expect(settledFlag).toBe(false);
      expect(collector.pendingCount).toBe(1);

      collector.ackNext();
      const settled = await pending;
      expect(settled).toBeInstanceOf(Ok);
      expect(collector.acked).toHaveLength(1);
      expect(collector.acked[0].every((row) => row.lane === 'billing')).toBe(true);
      // No billing-grade rows may remain buffered in the isolate.
      expect(tracer.queue).toHaveLength(0);
    });

    it('returns Err when the collector is unavailable — the billable request must fail', async () => {
      const { tracer, collector } = makeHarness();
      collector.failNext({ code: 'collector-unavailable', message: 'shard down' });
      await tracer.trace('req', billableOp);

      const settled = await tracer.settleBillable();
      expect(settled).toBeInstanceOf(Err);
      if (settled instanceof Err) expect(settled.error.code).toBe('collector-unavailable');
    });

    it('routes rows of one trace to both lanes per the injected classifier', async () => {
      const { tracer, collector, diagnosticBatches } = makeHarness();
      await tracer.trace('req', billableOp);
      await tracer.trace('req', diagnosticOp);

      const settled = await tracer.settleBillable();
      expect(settled).toBeInstanceOf(Ok);
      expect(collector.acked).toHaveLength(1);

      await tracer.flush();
      expect(diagnosticBatches).toHaveLength(1);
      const diagnosticRows = diagnosticBatches[0];
      expect(diagnosticRows.length).toBeGreaterThan(0);
      expect(diagnosticRows.every((row) => row.lane !== 'billing')).toBe(true);
    });
  });

  describe('flush (lazy, waitUntil-schedulable)', () => {
    it('sends staged + newly drained diagnostic rows and never rejects on transport failure', async () => {
      const { tracer, diagnosticErrors } = makeHarness({ diagnosticsFail: true });
      await tracer.trace('req', diagnosticOp);

      await tracer.flush();
      expect(diagnosticErrors).toHaveLength(1);
      expect(diagnosticErrors[0]).toBeInstanceOf(Error);
    });

    it('still delivers billing rows found at flush time (settle skipped), awaited', async () => {
      const { tracer, collector, lateFailures } = makeHarness();
      await tracer.trace('req', billableOp);

      await tracer.flush();
      expect(collector.acked).toHaveLength(1);
      expect(lateFailures).toHaveLength(0);
    });

    it('escalates late billable send failures via onLateBillableSendFailure without rejecting', async () => {
      const { tracer, collector, lateFailures } = makeHarness();
      collector.failNext({ code: 'collector-rejected', message: 'commit failed' });
      await tracer.trace('req', billableOp);

      await tracer.flush();
      expect(lateFailures).toHaveLength(1);
      expect(lateFailures[0].code).toBe('collector-rejected');
    });

    it('does not resend diagnostic rows on a second flush', async () => {
      const { tracer, diagnosticBatches } = makeHarness();
      await tracer.trace('req', diagnosticOp);

      await tracer.settleBillable();
      await tracer.flush();
      await tracer.flush();
      expect(diagnosticBatches).toHaveLength(1);
    });
  });
});
