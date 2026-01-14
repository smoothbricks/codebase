/**
 * Tests for strongly-typed trace facts.
 *
 * These tests verify both runtime behavior AND compile-time type safety.
 * Invalid facts should be caught by TypeScript before tests even run.
 */

import { describe, expect, it } from 'bun:test';
import {
  createFactArray,
  type FactArray,
  ffFact,
  isLogFact,
  isSpanFact,
  isTagFact,
  logError,
  logFact,
  logInfo,
  metricFact,
  parseFact,
  type SpanFact,
  scopeFact,
  spanErr,
  spanOk,
  spanStarted,
  type TraceFact,
  tagFact,
} from '../facts.js';

describe('facts', () => {
  describe('SpanFact', () => {
    it('creates typed span:started facts', () => {
      const fact = spanStarted('fetch-user');

      // Type is exactly: "span:fetch-user: started"
      fact satisfies 'span:fetch-user: started';

      expect(fact).toBe('span:fetch-user: started');
      expect(isSpanFact(fact)).toBe(true);
    });

    it('creates typed span:ok facts', () => {
      const fact = spanOk('fetch-user');

      // Type is exactly: "span:fetch-user: ok"
      fact satisfies 'span:fetch-user: ok';

      expect(fact).toBe('span:fetch-user: ok');
    });

    it('creates typed span:err facts', () => {
      const fact = spanErr('fetch-user', 'NOT_FOUND');

      // Type is exactly: "span:fetch-user: err(NOT_FOUND)"
      fact satisfies 'span:fetch-user: err(NOT_FOUND)';

      expect(fact).toBe('span:fetch-user: err(NOT_FOUND)');
    });

    it('allows span names with various characters', () => {
      // Names can include dots, dashes, etc.
      const fact1 = spanStarted('http.fetch');
      const fact2 = spanStarted('db:query:users');
      const fact3 = spanOk('op/reserveInventory');

      expect(fact1).toBe('span:http.fetch: started');
      expect(fact2).toBe('span:db:query:users: started');
      expect(fact3).toBe('span:op/reserveInventory: ok');
    });
  });

  describe('LogFact', () => {
    it('creates typed log facts', () => {
      const fact = logFact('info', 'Processing order');

      // Type is exactly: "log:info: Processing order"
      fact satisfies 'log:info: Processing order';

      expect(fact).toBe('log:info: Processing order');
      expect(isLogFact(fact)).toBe(true);
    });

    it('provides convenience functions', () => {
      expect(logInfo('hello')).toBe('log:info: hello');
      expect(logError('oops')).toBe('log:error: oops');
    });
  });

  describe('TagFact', () => {
    it('creates typed tag facts', () => {
      const fact = tagFact('userId', '123');

      // Type is exactly: "tag:userId: 123"
      fact satisfies 'tag:userId: 123';

      expect(fact).toBe('tag:userId: 123');
      expect(isTagFact(fact)).toBe(true);
    });

    it('stringifies numbers and booleans', () => {
      const numFact = tagFact('count', 42);
      const boolFact = tagFact('enabled', true);

      // Types preserve the literal value
      numFact satisfies 'tag:count: 42';
      boolFact satisfies 'tag:enabled: true';

      expect(numFact).toBe('tag:count: 42');
      expect(boolFact).toBe('tag:enabled: true');
    });
  });

  describe('ScopeFact', () => {
    it('creates typed scope facts', () => {
      const fact = scopeFact('requestId', 'req-abc');
      expect(fact).toBe('scope:requestId: req-abc');
    });
  });

  describe('FFFact', () => {
    it('creates typed feature flag facts', () => {
      const fact = ffFact('darkMode', true);
      expect(fact).toBe('ff:darkMode: true');
    });
  });

  describe('MetricFact', () => {
    it('creates typed metric facts', () => {
      const fact = metricFact('duration_ms', 42);
      expect(fact).toBe('metric:duration_ms: 42');
    });
  });

  describe('parseFact', () => {
    it('parses span facts', () => {
      const parsed = parseFact(spanStarted('fetch-user'));
      expect(parsed).toEqual({
        namespace: 'span',
        name: 'fetch-user',
        content: 'started',
        raw: 'span:fetch-user: started',
      });
    });

    it('parses tag facts', () => {
      const parsed = parseFact(tagFact('userId', '123'));
      expect(parsed).toEqual({
        namespace: 'tag',
        name: 'userId',
        content: '123',
        raw: 'tag:userId: 123',
      });
    });

    it('parses facts with colons in content', () => {
      const parsed = parseFact(spanErr('fetch', 'ERROR:TIMEOUT'));
      expect(parsed).toEqual({
        namespace: 'span',
        name: 'fetch',
        content: 'err(ERROR:TIMEOUT)',
        raw: 'span:fetch: err(ERROR:TIMEOUT)',
      });
    });
  });

  describe('FactArray', () => {
    const facts: FactArray = createFactArray([
      spanStarted('validate'),
      spanStarted('fetch-user'),
      tagFact('userId', '123'),
      logInfo('Fetching user'),
      spanOk('fetch-user'),
      spanStarted('fetch-orders'),
      logError('Order not found'),
      spanErr('fetch-orders', 'NOT_FOUND'),
      spanOk('validate'),
    ]);

    it('checks exact fact presence', () => {
      expect(facts.has(spanStarted('validate'))).toBe(true);
      expect(facts.has(spanOk('validate'))).toBe(true);
      expect(facts.has(spanErr('validate', 'FAIL'))).toBe(false);
    });

    it('matches patterns with wildcards', () => {
      // Any span that started
      expect(facts.hasMatch('span:*: started')).toBe(true);

      // Any span that completed ok
      expect(facts.hasMatch('span:*: ok')).toBe(true);

      // Specific span, any state
      expect(facts.hasMatch('span:fetch-user: *')).toBe(true);

      // Any error log
      expect(facts.hasMatch('log:error: *')).toBe(true);

      // Non-existent patterns
      expect(facts.hasMatch('span:unknown: *')).toBe(false);
      expect(facts.hasMatch('log:debug: *')).toBe(false);
    });

    it('filters by namespace', () => {
      const spans = facts.byNamespace('span');
      expect(spans.length).toBe(6); // 3 started + 2 ok + 1 err

      const logs = facts.byNamespace('log');
      expect(logs.length).toBe(2);

      const tags = facts.byNamespace('tag');
      expect(tags.length).toBe(1);
    });

    it('provides typed namespace filters', () => {
      const spans = facts.spans();
      expect(spans.every(isSpanFact)).toBe(true);

      const logs = facts.logs();
      expect(logs.every(isLogFact)).toBe(true);

      const tags = facts.tags();
      expect(tags.every(isTagFact)).toBe(true);
    });

    it('checks facts in order', () => {
      // These facts appear in this order
      expect(
        facts.hasInOrder([
          spanStarted('validate'),
          spanStarted('fetch-user'),
          spanOk('fetch-user'),
          spanOk('validate'),
        ]),
      ).toBe(true);

      // Wrong order - validate:ok comes after fetch-user:ok
      expect(facts.hasInOrder([spanOk('validate'), spanOk('fetch-user')])).toBe(false);

      // Missing fact
      expect(facts.hasInOrder([spanStarted('unknown')])).toBe(false);
    });

    it('matches and returns filtered array', () => {
      const okSpans = facts.match('span:*: ok');
      expect(okSpans.length).toBe(2);
      expect(okSpans.has(spanOk('fetch-user'))).toBe(true);
      expect(okSpans.has(spanOk('validate'))).toBe(true);
    });
  });

  describe('type safety', () => {
    it('TraceFact union accepts all fact types', () => {
      // All these should type-check as TraceFact
      const facts: TraceFact[] = [
        spanStarted('test'),
        spanOk('test'),
        spanErr('test', 'ERR'),
        logFact('info', 'msg'),
        tagFact('key', 'value'),
        scopeFact('key', 'value'),
        ffFact('flag', true),
        metricFact('name', 123),
      ];

      expect(facts.length).toBe(8);
    });

    it('SpanFact is narrower than TraceFact', () => {
      const spanFact: SpanFact<'test', 'ok'> = spanOk('test');

      // SpanFact is assignable to TraceFact
      const traceFact: TraceFact = spanFact;

      expect(traceFact).toBe('span:test: ok');
    });

    // These would be compile errors if uncommented:
    // const invalid1: TraceFact = 'not a valid fact';
    // const invalid2: SpanFact = 'span:test: invalid_state';
    // const invalid3: LogFact = 'log:invalid_level: msg';
  });
});

describe('trace-testing example', () => {
  it('demonstrates the trace-testing pattern', () => {
    // Simulate facts collected from a trace
    const facts = createFactArray([
      spanStarted('execute-loop'),
      spanStarted('reduce'),
      tagFact('event_count', 5),
      spanOk('reduce'),
      spanStarted('decide'),
      spanStarted('op:reserveInventory'),
      tagFact('sku', 'SKU-A'),
      tagFact('quantity', 2),
      spanOk('op:reserveInventory'),
      spanStarted('op:chargePayment'),
      logInfo('Charging $49.99'),
      spanOk('op:chargePayment'),
      spanOk('decide'),
      spanOk('execute-loop'),
    ]);

    // Assert on WHAT happened, not HOW
    // This test doesn't care about implementation details

    // The execution completed successfully
    expect(facts.has(spanOk('execute-loop'))).toBe(true);

    // Reduce happened before decide
    expect(facts.hasInOrder([spanOk('reduce'), spanStarted('decide')])).toBe(true);

    // Inventory was reserved before payment was charged
    expect(facts.hasInOrder([spanOk('op:reserveInventory'), spanStarted('op:chargePayment')])).toBe(true);

    // The right tags were set
    expect(facts.has(tagFact('sku', 'SKU-A'))).toBe(true);
    expect(facts.has(tagFact('quantity', 2))).toBe(true);

    // No errors occurred
    expect(facts.hasMatch('span:*: err(*)')).toBe(false);
    expect(facts.hasMatch('log:error: *')).toBe(false);
  });
});
