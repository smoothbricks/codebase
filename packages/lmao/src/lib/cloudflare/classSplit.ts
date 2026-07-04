/**
 * Class-split flush adapter — routes each completed root buffer's rows by delivery class.
 *
 * Per specs/lmao/01u_cloudflare_trace_segments.md §Delivery Classes every trace row belongs to
 * exactly one class:
 * - billing-grade rows go to the CollectorClient, AWAITED within the request via
 *   `settleBillable()` before/alongside response completion — an Err fails the billable request
 *   (backpressure ownership: never silently queue billing-grade rows in the isolate)
 * - diagnostic rows go to the drain transport lazily, on the waitUntil-scheduled `flush()`
 *
 * The classifier is an injected function: the spec fixes the CLASSES, not the classification
 * mechanism, so the mapping from row to class stays a seam (constructor-injected like the
 * transports).
 *
 * Canonical request flow (01s wrapper seam):
 *   const settled = await tracer.settleBillable();   // before returning the response
 *   if (settled instanceof Err) return failBillableRequest(settled.error);
 *   ctx.waitUntil(tracer.flush());                    // diagnostics, fire-and-forget
 */

import type { OpContextBinding } from '../opContext/types.js';
import { Err, Ok, type Result } from '../result.js';
import type { TracerOptions } from '../tracer.js';
import { ArrayQueueTracer } from '../tracers/ArrayQueueTracer.js';
import type { CollectorAck, CollectorClient, CollectorSendFailure } from './collectorClient.js';
import type { DiagnosticTransport } from './diagnosticDrain.js';
import { spanBufferToTraceRows, type TraceRow } from './traceRows.js';

export type DeliveryClass = 'billing-grade' | 'diagnostic';

/** Injected classification seam — decides which lane one row rides. */
export type DeliveryClassifier = (row: TraceRow) => DeliveryClass;

/** Singleton for the common "nothing billable in this request" settle outcome. */
const SETTLED_NOTHING_BILLABLE: Ok<null> = new Ok(null);

export interface ClassSplitOptions<
  T extends import('../schema/LogSchema.js').LogSchema = import('../schema/LogSchema.js').LogSchema,
> extends TracerOptions<T> {
  collector: CollectorClient;
  diagnostics: DiagnosticTransport;
  classify: DeliveryClassifier;
  /** Diagnostic-lane failure hook: emit metrics; drop is within the class's loss budget. */
  onDiagnosticSendError?: (error: unknown, rows: readonly TraceRow[]) => void;
  /**
   * Billing rows found only at flush() time (the wrapper skipped settleBillable) whose late
   * awaited send failed. This is a broken-integration alarm — page on it; the rows are lost to
   * this isolate and only replay/reconciliation (01u §Zero-Loss Sources) can heal the gap.
   */
  onLateBillableSendFailure?: (failure: CollectorSendFailure, rows: readonly TraceRow[]) => void;
}

interface PartitionedRows {
  billing: TraceRow[];
  diagnostic: TraceRow[];
}

/**
 * Tracer tying the two lanes together over onTraceEnd (queue) + settleBillable/flush (drain).
 *
 * Diagnostic rows discovered during settleBillable are held for the same request's flush() —
 * request-scoped staging only, never cross-request accumulation of billing-grade data.
 */
export class ClassSplitTracer<B extends OpContextBinding = OpContextBinding> extends ArrayQueueTracer<B> {
  private readonly collector: CollectorClient;
  private readonly diagnostics: DiagnosticTransport;
  private readonly classify: DeliveryClassifier;
  private readonly onDiagnosticSendError: ((error: unknown, rows: readonly TraceRow[]) => void) | undefined;
  private readonly onLateBillableSendFailure:
    | ((failure: CollectorSendFailure, rows: readonly TraceRow[]) => void)
    | undefined;
  private pendingDiagnosticRows: TraceRow[] = [];

  constructor(binding: B, options: ClassSplitOptions<B['logBinding']['logSchema']>) {
    super(binding, options);
    this.collector = options.collector;
    this.diagnostics = options.diagnostics;
    this.classify = options.classify;
    this.onDiagnosticSendError = options.onDiagnosticSendError;
    this.onLateBillableSendFailure = options.onLateBillableSendFailure;
  }

  private partitionDrained(): PartitionedRows {
    const billing: TraceRow[] = [];
    const diagnostic: TraceRow[] = [];
    for (const buffer of this.drain()) {
      let rows: TraceRow[];
      try {
        rows = spanBufferToTraceRows(buffer);
      } finally {
        this.bufferStrategy.releaseBuffer(buffer);
      }
      for (const row of rows) {
        (this.classify(row) === 'billing-grade' ? billing : diagnostic).push(row);
      }
    }
    return { billing, diagnostic };
  }

  /**
   * Drain completed traces and durably deliver the billing-grade rows — awaited within the
   * request. Ok(null) means no billing rows were produced; Err means the collector could not
   * make the revenue records durable and the billable request must fail. Diagnostic rows are
   * staged for this request's flush().
   */
  async settleBillable(): Promise<Result<CollectorAck | null, CollectorSendFailure>> {
    const { billing, diagnostic } = this.partitionDrained();
    this.pendingDiagnosticRows.push(...diagnostic);
    if (billing.length === 0) return SETTLED_NOTHING_BILLABLE;
    return this.collector.send(billing);
  }

  /**
   * Lazy, waitUntil-schedulable drain: never rejects. Diagnostic rows (staged + newly drained)
   * go to the drain transport; failures hit onDiagnosticSendError per the class's loss budget.
   * Billing rows found here mean settleBillable was skipped — they are still sent awaited (never
   * silently dropped or left buffered), with failures escalated via onLateBillableSendFailure.
   */
  override async flush(): Promise<void> {
    const { billing, diagnostic } = this.partitionDrained();
    const diagnosticRows = this.pendingDiagnosticRows.concat(diagnostic);
    this.pendingDiagnosticRows = [];

    if (billing.length > 0) {
      const result = await this.collector.send(billing);
      if (result instanceof Err) {
        this.onLateBillableSendFailure?.(result.error, billing);
      }
    }

    if (diagnosticRows.length > 0) {
      try {
        await this.diagnostics.send(diagnosticRows);
      } catch (error) {
        this.onDiagnosticSendError?.(error, diagnosticRows);
      }
    }
  }
}
