import { describe, expect, it } from 'bun:test';
import { Readable } from 'node:stream';
import { parseAdmission, parseHandle, parseRetirement, readAdmission } from './owned-subtree.js';

const INCARNATION = '0123456789abcdef0123456789abcdef';
const HANDLE = { jobId: 1, index: 2, incarnation: INCARNATION };
const ADMITTED_LINE = `{"kind":"admitted","handle":{"jobId":1,"index":2,"incarnation":"${INCARNATION}"}}`;
const REFUSAL_REASONS = ['foldUnavailable', 'notMember', 'foreignHandle', 'jobEnded', 'spentHandle'] as const;

const LOST = '[{"kind":"eventsLost"},{"kind":"unobservedExit","pid":7}]';
const OUTLIVED = '[{"pid":4101,"image":"/bin/sleep","signal":15},{"pid":4102,"signal":9}]';
const RETIRED = `{"kind":"retired","retirement":{"containment":{"kind":"observed","lost":${LOST}},"outlived":${OUTLIVED}}}`;
const RETIRED_INCOMPLETE = `{"kind":"retired","retirement":{"containment":{"kind":"observed","lost":${LOST}},"outlived":${OUTLIVED},"outlivedIncomplete":true}}`;

describe('parseAdmission', () => {
  it('reads an admitted line with its trailing newline as the handle', () => {
    expect(parseAdmission(`${ADMITTED_LINE}\n`)).toEqual({ kind: 'admitted', handle: HANDLE });
  });

  for (const reason of REFUSAL_REASONS) {
    it(`reads the refusal ${reason}`, () => {
      expect(parseAdmission(`{"kind":"refused","reason":"${reason}"}\n`)).toEqual({ kind: 'refused', reason });
    });
  }

  it('reports an unknown refusal reason as unread, quoting the text', () => {
    const text = '{"kind":"refused","reason":"tired"}';
    expect(parseAdmission(text)).toMatchObject({ kind: 'unread', detail: expect.stringContaining(text) });
  });

  it('reports text that is not JSON as unread', () => {
    expect(parseAdmission('garbage')).toMatchObject({ kind: 'unread', detail: expect.stringContaining('garbage') });
  });

  it('reports an admitted line without a handle as unread', () => {
    expect(parseAdmission('{"kind":"admitted"}')).toMatchObject({ kind: 'unread' });
  });
});

describe('parseHandle', () => {
  it('returns a JSON object as the handle', () => {
    expect(parseHandle(`{"jobId":1,"index":2,"incarnation":"${INCARNATION}"}`)).toEqual(HANDLE);
  });

  it('returns null for a torn write', () => {
    expect(parseHandle('[')).toBeNull();
  });

  it('returns null for a JSON array', () => {
    expect(parseHandle('[1]')).toBeNull();
  });

  it('returns null for a JSON string', () => {
    expect(parseHandle('"x"')).toBeNull();
  });
});

describe('parseRetirement', () => {
  it('reads outlived rows, with and without an image, and the coverage gaps', () => {
    expect(parseRetirement(RETIRED)).toEqual({
      kind: 'retired',
      signalled: [
        { pid: 4101, image: '/bin/sleep', signal: 15 },
        { pid: 4102, image: null, signal: 9 },
      ],
      signalledIncomplete: false,
      gaps: [
        { kind: 'eventsLost', pid: null },
        { kind: 'unobservedExit', pid: 7 },
      ],
    });
  });

  it('reports outlivedIncomplete as signalledIncomplete', () => {
    expect(parseRetirement(RETIRED_INCOMPLETE)).toEqual({
      kind: 'retired',
      signalled: [
        { pid: 4101, image: '/bin/sleep', signal: 15 },
        { pid: 4102, image: null, signal: 9 },
      ],
      signalledIncomplete: true,
      gaps: [
        { kind: 'eventsLost', pid: null },
        { kind: 'unobservedExit', pid: 7 },
      ],
    });
  });

  it('reads an empty retirement as empty arrays', () => {
    expect(
      parseRetirement('{"kind":"retired","retirement":{"containment":{"kind":"observed","lost":[]},"outlived":[]}}'),
    ).toEqual({ kind: 'retired', signalled: [], signalledIncomplete: false, gaps: [] });
  });

  it('reads a refusal as refused', () => {
    expect(parseRetirement('{"kind":"refused","reason":"spentHandle"}')).toEqual({
      kind: 'refused',
      reason: 'spentHandle',
    });
  });

  it('fails an outlived row missing its signal', () => {
    expect(
      parseRetirement(
        '{"kind":"retired","retirement":{"containment":{"kind":"observed","lost":[]},"outlived":[{"pid":4101,"image":"/bin/sleep"}]}}',
      ),
    ).toMatchObject({ kind: 'failed', detail: expect.stringContaining('cannot read') });
  });

  it('reads a kernel-proven containment as a receipt with no coverage gaps', () => {
    expect(
      parseRetirement(
        '{"kind":"retired","retirement":{"containment":{"kind":"kernel"},"outlived":[{"pid":7,"signal":15}]}}',
      ),
    ).toEqual({
      kind: 'retired',
      signalled: [{ pid: 7, image: null, signal: 15 }],
      signalledIncomplete: false,
      gaps: [],
    });
  });

  it('fails a containment this client does not know', () => {
    expect(
      parseRetirement('{"kind":"retired","retirement":{"containment":{"kind":"guessed"},"outlived":[]}}'),
    ).toMatchObject({ kind: 'failed', detail: expect.stringContaining('does not know') });
  });

  it('fails text that is not JSON', () => {
    expect(parseRetirement('retired: yes')).toMatchObject({
      kind: 'failed',
      detail: expect.stringContaining('printed no JSON'),
    });
  });
});

describe('readAdmission', () => {
  it('is unread when the command was started without its handle descriptor', async () => {
    expect(await readAdmission(null)).toMatchObject({
      kind: 'unread',
      detail: expect.stringContaining('without its handle descriptor'),
    });
  });

  it('reads a refusal line from the descriptor', async () => {
    const stream = Readable.from([Buffer.from('{"kind":"refused","reason":"notMember"}\n')]);
    expect(await readAdmission(stream)).toEqual({ kind: 'refused', reason: 'notMember' });
  });

  it('joins an admitted line split across two chunks', async () => {
    const stream = Readable.from([
      Buffer.from('{"kind":"admitted","handle":{"jobId":1,'),
      Buffer.from(`"index":2,"incarnation":"${INCARNATION}"}}\n`),
    ]);
    expect(await readAdmission(stream)).toEqual({ kind: 'admitted', handle: HANDLE });
  });

  it('is unread when the descriptor closes without an admission', async () => {
    expect(await readAdmission(Readable.from([]))).toMatchObject({
      kind: 'unread',
      detail: expect.stringContaining('without an admission'),
    });
  });
});
