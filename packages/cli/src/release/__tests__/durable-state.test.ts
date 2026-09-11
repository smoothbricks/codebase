import { describe, expect, it } from 'bun:test';
import { createServer } from 'node:net';
import { requireDurableState, transportFailureDetail } from '../durable-state.js';

describe('transport failure classification', () => {
  it('classifies a real reset from a real fetch, whatever the runtime calls it', async () => {
    // Not a hand-built error: a server that accepts the connection and then
    // resets it. The runtime's own words for that are worth reading -- Bun
    // rejects with "The socket connection was closed unexpectedly" and puts
    // the only usable signal, ECONNRESET, on a property, so a classifier that
    // reads the message alone declares a production reset "not transport" and
    // rethrows it as a bug.
    const server = createServer((socket) => {
      socket.on('data', () => socket.resetAndDestroy());
    });
    await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
    const address = server.address();
    if (address === null || typeof address === 'string') {
      throw new Error('loopback server did not report a port');
    }

    const rejection = await fetch(`http://127.0.0.1:${address.port}/releases/tags/pkg@1.0.0`).then(
      () => null,
      (error: unknown) => error,
    );
    server.close();

    expect(transportFailureDetail(rejection)).toContain('ECONNRESET');
  });

  it('reads the reset out of a fetch rejection cause chain', () => {
    // What `fetch` actually rejects with: the top-level message is three words
    // and the reason is one level down. Classifying on the message alone either
    // retries every bug or retries no reset.
    const rejection = new TypeError('fetch failed', { cause: new Error('read ECONNRESET') });

    expect(transportFailureDetail(rejection)).toContain('ECONNRESET');
  });

  it('reads the reason out of an aggregate of failed connection attempts', () => {
    const rejection = new AggregateError(
      [new Error('connect ECONNREFUSED 127.0.0.1:3000'), new Error('connect ECONNREFUSED ::1:3000')],
      'all connection attempts failed',
    );

    expect(transportFailureDetail(rejection)).toContain('ECONNREFUSED');
  });

  it('refuses to classify a programmer error as transport', () => {
    // Retrying this three times would hide a bug behind a network story.
    expect(transportFailureDetail(new TypeError('Invalid URL'))).toBeNull();
    expect(transportFailureDetail(new Error('401 Unauthorized'))).toBeNull();
  });
});

describe('undetermined durable state', () => {
  it('answers exists and absent as the booleans the release plans from', () => {
    expect(requireDurableState({ kind: 'exists' }, 'pkg@1.0.0', 'whether it is published')).toBe(true);
    expect(requireDurableState({ kind: 'absent' }, 'pkg@1.0.0', 'whether it is published')).toBe(false);
  });

  it('refuses a non-answer with the subject, the attempt count and the transport detail', () => {
    const refusal = () =>
      requireDurableState(
        { kind: 'undetermined', attempts: 3, detail: 'read ECONNRESET' },
        'pkg@1.0.0',
        'whether it is published',
      );

    expect(refusal).toThrow('pkg@1.0.0');
    expect(refusal).toThrow('3 attempts');
    expect(refusal).toThrow('read ECONNRESET');
    // The operator's next move is in the message: re-dispatch, do not
    // investigate the release.
    expect(refusal).toThrow('network failure');
    expect(refusal).toThrow('re-dispatching this run is safe');
  });
});
