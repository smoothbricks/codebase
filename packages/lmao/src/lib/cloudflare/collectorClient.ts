/**
 * Billing-grade collector client.
 *
 * Per specs/lmao/01u_cloudflare_trace_segments.md §Delivery Classes / §Zero-Loss Sources:
 * `send(rows)` resolves Ok only when the collector has ACKED — and the ack means the collector
 * DO's transactional-storage commit completed, which IS the durability point. From that moment
 * the rows are durable and replay-owned.
 *
 * Send-before-response contract: the caller awaits `send` within the request; an Err fails the
 * billable request (same principle as any payment-gateway outage). Implementations and callers
 * must NEVER buffer billing-grade rows in the isolate across requests — an isolate eviction
 * between requests silently destroys revenue records.
 *
 * The collector DO itself is not deployed yet; this module ships the interface plus an in-memory
 * fake with ack/fail/latency injection so lane composition is testable today.
 */

import { Err, Ok, type Result } from '../result.js';
import type { TraceRow } from './traceRows.js';

/** Proof of collector durability: the DO transactional-storage commit completed. */
export interface CollectorAck {
  readonly durable: true;
}

/** Singleton ack — the ack carries no data, so never allocate it per send. */
export const COLLECTOR_ACK: CollectorAck = { durable: true };

/** Operational send failure — the caller must fail the billable request on this. */
export interface CollectorSendFailure {
  readonly code: 'collector-unavailable' | 'collector-rejected';
  readonly message: string;
}

/**
 * Transport to the billing-grade collector shard.
 *
 * `send` resolves Ok(CollectorAck) only after the collector's DO transactional-storage commit;
 * operational failures come back as Err (never throw) so the request path can fail deliberately.
 */
export interface CollectorClient {
  send(rows: readonly TraceRow[]): Promise<Result<CollectorAck, CollectorSendFailure>>;
}

interface HeldSend {
  readonly rows: readonly TraceRow[];
  readonly resolve: (result: Result<CollectorAck, CollectorSendFailure>) => void;
}

/**
 * In-memory CollectorClient fake with ack/fail/latency injection.
 *
 * Modes:
 * - default: every send acks immediately (after optional injected latency)
 * - `failNext(failure)`: queue an Err for upcoming sends
 * - `holdSends()`: sends stay pending until the test calls `ackNext()`/`rejectNext()` — this is
 *   how contract tests prove "resolves only when the collector acks"
 */
export class FakeCollectorClient implements CollectorClient {
  /** Row batches the fake has durably acked, in ack order. */
  readonly acked: (readonly TraceRow[])[] = [];

  private readonly failures: CollectorSendFailure[] = [];
  private readonly held: HeldSend[] = [];
  private holding = false;
  private latencyMs = 0;

  /** Queue an Err result for the next send (FIFO across multiple calls). */
  failNext(failure: CollectorSendFailure): void {
    this.failures.push(failure);
  }

  /** Inject artificial ack latency for subsequent sends. */
  setLatency(ms: number): void {
    this.latencyMs = ms;
  }

  /** Switch to manual mode: sends stay pending until ackNext()/rejectNext(). */
  holdSends(): void {
    this.holding = true;
  }

  /** Number of sends currently awaiting a manual ack. */
  get pendingCount(): number {
    return this.held.length;
  }

  /** Ack the oldest held send (DO commit happened). */
  ackNext(): void {
    const pending = this.held.shift();
    if (!pending) throw new Error('FakeCollectorClient.ackNext: no held send');
    this.acked.push(pending.rows);
    pending.resolve(new Ok(COLLECTOR_ACK));
  }

  /** Fail the oldest held send with an operational failure. */
  rejectNext(failure: CollectorSendFailure): void {
    const pending = this.held.shift();
    if (!pending) throw new Error('FakeCollectorClient.rejectNext: no held send');
    pending.resolve(new Err(failure));
  }

  async send(rows: readonly TraceRow[]): Promise<Result<CollectorAck, CollectorSendFailure>> {
    if (this.latencyMs > 0) {
      await new Promise((resolve) => setTimeout(resolve, this.latencyMs));
    }
    if (this.holding) {
      return new Promise((resolve) => {
        this.held.push({ rows, resolve });
      });
    }
    const failure = this.failures.shift();
    if (failure) return new Err(failure);
    this.acked.push(rows);
    return new Ok(COLLECTOR_ACK);
  }
}
