import { afterEach, describe, expect, it } from 'bun:test';
import type { LoadRequest } from '@smoothbricks/statebus-data-loader';
import { onlineManager, QueryObserver } from '@tanstack/query-core';
import type { QueryExecutionContext } from '../index.js';
import { fixture, type Items } from './fixture.js';

const cleanups: (() => void)[] = [];
afterEach(() => {
  for (const cleanup of cleanups.splice(0)) cleanup();
});
function setup(options: Parameters<typeof fixture>[0]) {
  const test = fixture(options);
  cleanups.push(() => test.dispose());
  return test;
}

function accepted(test: ReturnType<typeof fixture>, id: string | number): LoadRequest {
  const state = test.state(id);
  if (state.kind !== 'loading') throw new Error('Expected an admitted load.');
  return state.request;
}

describe('StateBus -> real QueryClient integration', () => {
  it('deduplicates same-resource interest and cancels only after final zero plus the grace period', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    const first = test.interest('home');
    const second = test.interest('home');
    await test.tick();
    expect(executions.length).toBe(1);
    first();
    await test.tick();
    test.timer.advance(1000);
    expect(executions[0]?.signal.aborted).toBe(false);
    second();
    await test.tick();
    test.timer.advance(249);
    expect(executions[0]?.signal.aborted).toBe(false);
    test.timer.advance(1);
    await test.tick();
    expect(executions[0]?.signal.aborted).toBe(true);
    expect(test.state('home').kind).toBe('cancelled');
    pending.resolve(['late']);
    await test.tick();
    expect(test.state('home').kind).toBe('cancelled');
  });

  it('cancels an unobserved offline request even before its transport consumes the signal', async () => {
    const wasOnline = onlineManager.isOnline();
    onlineManager.setOnline(false);
    try {
      let executions = 0;
      const test = setup({
        execute: async () => {
          executions += 1;
          return [];
        },
      });
      const release = test.interest('home');
      await test.tick();
      expect(test.queryClient.getQueryState(['loader', 'string', 'home'])?.fetchStatus).toBe('paused');
      release();
      await test.tick();
      test.timer.advance(250);
      await test.tick();
      expect(test.queryClient.getQueryState(['loader', 'string', 'home'])?.fetchStatus).toBe('idle');
      expect(test.state('home').kind).toBe('cancelled');
      expect(executions).toBe(0);
    } finally {
      onlineManager.setOnline(wasOnline);
    }
  });

  it('coalesces subscribe/unsubscribe in the same wave without issuing a request', async () => {
    let executions = 0;
    const test = setup({
      execute: async () => {
        executions += 1;
        return [];
      },
    });
    const release = test.interest('home');
    release();
    await test.tick();
    expect(executions).toBe(0);
    expect(test.state('home').kind).toBe('not-requested');
  });

  it('reuses the in-flight request after reinterest during grace', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    const release = test.interest('home');
    await test.tick();
    const original = accepted(test, 'home');
    release();
    await test.tick();
    test.timer.advance(200);
    test.interest('home');
    await test.tick();
    test.timer.advance(100);
    expect(executions.length).toBe(1);
    expect(executions[0]?.signal.aborted).toBe(false);
    expect(accepted(test, 'home')).toEqual(original);
    pending.resolve(['ready']);
    await test.tick();
    expect(test.state('home').kind).toBe('ready');
    expect(test.timer.pending).toBe(0);
  });

  it('keeps numeric and string IDs separate while sharing a matching query key safely', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      queryKey: () => ['shared'],
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    const number = test.interest(7);
    test.interest('7');
    await test.tick();
    expect(executions.length).toBe(1);
    number();
    await test.tick();
    test.timer.advance(250);
    await test.tick();
    expect(test.state(7).kind).toBe('cancelled');
    expect(test.state('7').kind).toBe('loading');
    expect(executions[0]?.signal.aborted).toBe(false);
    pending.resolve(['shared result']);
    await test.tick();
    expect(test.state('7').kind).toBe('ready');
    expect(test.state(7).kind).toBe('cancelled');
  });

  it('does not cancel a shared query still owned by an external observer', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    const external = new QueryObserver<Items>(test.queryClient, {
      queryKey: ['loader', 'string', 'home'],
      enabled: false,
    });
    const stopExternal = external.subscribe(() => {});
    cleanups.push(stopExternal);
    const release = test.interest('home');
    await test.tick();
    release();
    await test.tick();
    test.timer.advance(250);
    await test.tick();
    expect(executions[0]?.signal.aborted).toBe(false);
    pending.resolve(['external cache']);
    await test.tick();
    expect(test.queryClient.getQueryData<Items>(['loader', 'string', 'home'])).toEqual(['external cache']);
    expect(test.state('home').kind).toBe('cancelled');
  });

  it('fulfills interest from the cache through the same lifecycle events, without invoking the transport', async () => {
    let executions = 0;
    const test = setup({
      staleTime: Number.POSITIVE_INFINITY,
      execute: async () => {
        executions += 1;
        return [];
      },
    });
    test.queryClient.setQueryData(['loader', 'string', 'home'], ['cached']);
    test.interest('home');
    await test.tick();
    expect(executions).toBe(0);
    expect(test.events.map((event) => event.type)).toEqual(['loadRequested', 'loadSucceeded']);
    const state = test.state('home');
    expect(state.kind === 'ready' ? state.data.value : undefined).toEqual(['cached']);
  });

  it('starts queries for interest that predates provider installation', async () => {
    let executions = 0;
    const test = setup({
      autoInstall: false,
      execute: async () => {
        executions += 1;
        return ['ready'];
      },
    });
    test.interest('home');
    await test.tick();
    expect(executions).toBe(0);
    test.install();
    await test.tick();
    expect(executions).toBe(1);
    expect(test.state('home').kind).toBe('ready');
  });

  it('respects reducer admission for duplicate commands in the same dispatch wave', async () => {
    const pending = Promise.withResolvers<Items>();
    let executions = 0;
    const test = setup({
      automaticDemand: false,
      execute: async () => {
        executions += 1;
        return pending.promise;
      },
    });
    test.interest('home');
    await test.tick();
    const base: LoadRequest = {
      interest: { key: 'loaderTest.resources', id: 'home' },
      requestId: 'manual',
      fingerprint: 'home-load',
      at: 0,
      reason: 'refresh',
      policy: 'drop-duplicate',
    };
    test.channel.publish({ type: 'loadRequested', request: base });
    test.channel.publish({ type: 'loadRequested', request: { ...base, requestId: 'duplicate' } });
    await test.tick();
    expect(executions).toBe(1);
    expect(accepted(test, 'home').requestId).toBe('manual');
  });

  it('does not cancel an admitted request when the reducer refuses a conflicting cancellation', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    test.interest('home');
    await test.tick();
    const request = accepted(test, 'home');
    test.channel.publish({
      type: 'loadCancelled',
      request: { ...request, fingerprint: 'wrong-operation' },
      reason: 'cancelled',
    });
    await test.tick();
    expect(test.state('home').kind).toBe('loading');
    expect(executions[0]?.signal.aborted).toBe(false);
    pending.resolve(['ready']);
    await test.tick();
    expect(test.state('home').kind).toBe('ready');
  });

  it('supersedes an older query and refuses its late outcome', async () => {
    const first = Promise.withResolvers<Items>();
    const second = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      queryKey: (request) => [request.requestId],
      execute: async (context) => {
        executions.push(context);
        return executions.length === 1 ? first.promise : second.promise;
      },
    });
    test.interest('home');
    await test.tick();
    const previous = accepted(test, 'home');
    test.channel.publish({
      type: 'loadRequested',
      request: { ...previous, requestId: 'refresh', fingerprint: 'v2', reason: 'refresh' },
    });
    await test.tick();
    expect(executions.length).toBe(2);
    expect(executions[0]?.signal.aborted).toBe(true);
    second.resolve(['new']);
    await test.tick();
    first.resolve(['old']);
    await test.tick();
    const state = test.state('home');
    expect(state.kind === 'ready' ? state.data.value : undefined).toEqual(['new']);
  });

  it('emits periodic real byte measurements, resets them per retry, and retains the request ID', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const retried = Promise.withResolvers<void>();
    const test = setup({
      retry: 1,
      execute: async (context) => {
        executions.push(context);
        context.reportBytes({ direction: 'download', transferred: context.attempt === 1 ? 10 : 3 });
        if (context.attempt === 1) throw new Error('transient');
        retried.resolve();
        return pending.promise;
      },
    });
    test.interest('home');
    await test.tick();
    await retried.promise;
    await test.tick();
    expect(executions.map((context) => context.attempt)).toEqual([1, 2]);
    expect(new Set(executions.map((context) => context.request.requestId)).size).toBe(1);
    test.timer.advance(100);
    await test.tick();
    const state = test.state('home');
    expect(state.kind === 'loading' ? state.progress : undefined).toEqual({ attempt: 2, download: { transferred: 3 } });
    executions[1]?.reportBytes({ direction: 'download', transferred: 9, total: 9 });
    pending.resolve(['ready']);
    await test.tick();
    const progress = test.events.filter((event) => event.type === 'loadProgressed');
    expect(progress.at(-1)?.sample).toEqual({ direction: 'download', transferred: 9, total: 9 });
    expect(test.events.at(-1)?.type).toBe('loadSucceeded');
    expect(test.timer.pending).toBe(0);
  });

  it('normalizes rejected requests to typed failures instead of leaving loading state', async () => {
    const test = setup({
      execute: async () => {
        throw new Error('offline');
      },
    });
    test.interest('home');
    await test.tick();
    const state = test.state('home');
    expect(state.kind === 'failed' ? state.error : undefined).toEqual({ code: 'query-failed', message: 'offline' });
  });

  it('disposes observers/timers and suppresses late publications', async () => {
    const pending = Promise.withResolvers<Items>();
    const executions: QueryExecutionContext[] = [];
    const test = setup({
      execute: async (context) => {
        executions.push(context);
        return pending.promise;
      },
    });
    test.interest('home');
    await test.tick();
    const count = test.events.length;
    test.dispose();
    test.dispose();
    expect(executions[0]?.signal.aborted).toBe(true);
    executions[0]?.reportBytes({ direction: 'download', transferred: 100 });
    pending.resolve(['late']);
    test.timer.advance(1000);
    await test.tick();
    expect(test.events.length).toBe(count);
    expect(test.timer.pending).toBe(0);
  });
});
