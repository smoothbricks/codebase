// Configure Node.js timestamp implementation - MUST be first import
import '../../__tests__/test-helpers.js';

import { describe, expect, it } from 'bun:test';
import { Err, Ok } from '../../result.js';
import { COLLECTOR_ACK, FakeCollectorClient } from '../collectorClient.js';
import type { TraceRow } from '../traceRows.js';

const rows: TraceRow[] = [
  {
    trace_id: 't1',
    span_id: 1,
    parent_span_id: 0,
    row_index: 0,
    entry_type: 1,
    timestamp_ns: 1_000_000,
    message: 'op',
  },
];

function tick(): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

describe('CollectorClient contract (via FakeCollectorClient)', () => {
  describe('ack semantics', () => {
    it('resolves Ok with the durability ack and records the batch', async () => {
      const client = new FakeCollectorClient();
      const result = await client.send(rows);

      expect(result).toBeInstanceOf(Ok);
      if (result instanceof Ok) expect(result.value).toBe(COLLECTOR_ACK);
      expect(client.acked).toEqual([rows]);
    });

    it('send resolves ONLY when the collector acks (durability point)', async () => {
      const client = new FakeCollectorClient();
      client.holdSends();

      let settled = false;
      const pending = client.send(rows).then((result) => {
        settled = true;
        return result;
      });

      await tick();
      // No ack yet — the send-before-response contract means the caller is still blocked here.
      expect(settled).toBe(false);
      expect(client.pendingCount).toBe(1);
      expect(client.acked).toHaveLength(0);

      client.ackNext();
      const result = await pending;
      expect(settled).toBe(true);
      expect(result).toBeInstanceOf(Ok);
      expect(client.acked).toEqual([rows]);
    });

    it('supports injected ack latency', async () => {
      const client = new FakeCollectorClient();
      client.setLatency(20);

      const start = Date.now();
      const result = await client.send(rows);
      expect(result).toBeInstanceOf(Ok);
      expect(Date.now() - start).toBeGreaterThanOrEqual(15);
    });
  });

  describe('failure semantics', () => {
    it('returns Err (never throws) on injected failure — the billable request must fail', async () => {
      const client = new FakeCollectorClient();
      client.failNext({ code: 'collector-unavailable', message: 'shard down' });

      const result = await client.send(rows);
      expect(result).toBeInstanceOf(Err);
      if (result instanceof Err) expect(result.error.code).toBe('collector-unavailable');
      // A failed send is NOT durable — nothing may be recorded as acked.
      expect(client.acked).toHaveLength(0);
    });

    it('held sends can be rejected with an operational failure', async () => {
      const client = new FakeCollectorClient();
      client.holdSends();

      const pending = client.send(rows);
      client.rejectNext({ code: 'collector-rejected', message: 'storage commit failed' });

      const result = await pending;
      expect(result).toBeInstanceOf(Err);
      if (result instanceof Err) expect(result.error.code).toBe('collector-rejected');
      expect(client.acked).toHaveLength(0);
    });

    it('recovers after a failure: subsequent sends ack normally', async () => {
      const client = new FakeCollectorClient();
      client.failNext({ code: 'collector-unavailable', message: 'blip' });

      expect(await client.send(rows)).toBeInstanceOf(Err);
      expect(await client.send(rows)).toBeInstanceOf(Ok);
      expect(client.acked).toEqual([rows]);
    });
  });
});
