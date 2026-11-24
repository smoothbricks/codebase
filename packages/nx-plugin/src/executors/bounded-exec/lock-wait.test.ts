import { describe, expect, it } from 'bun:test';
import type { LockHolders } from '@smoothbricks/procinfo';
import fc from 'fast-check';
import { describeLockWait, lockCandidates, lockWaits, type OpenFile, parseLsofTable } from './lock-wait.js';

/** What the kernel's lock record says of each path, as procinfo's `lockHolders` answers it. */
const records = (entries: Record<string, LockHolders>): ReadonlyMap<string, LockHolders> =>
  new Map(Object.entries(entries));
const lock = (pid: number | null, kind: string | null, waiting = false) => ({ pid, kind, exclusive: true, waiting });

// The rows of the 2026-10-21 cargo case: `cargo --frozen test` blocked on the package cache while another cargo held
// it open. The cwd and txt rows are never lock candidates; the numbered REG row is the only one.
const INCIDENT_HEADER = 'COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME';
const INCIDENT_CWD = 'cargo 94769 dev cwd DIR 1,330 2624 2 /private/var/folders/cowshed/502/mnt/repo/shed';
const INCIDENT_TXT = 'cargo 94769 dev txt REG 1,20 6268080 1152921500312225049 /usr/lib/libobjc.A.dylib';
const INCIDENT_CACHE = 'cargo 94769 dev 5u REG 1,20 0 123 /Users/dev/.cargo/.package-cache';
const INCIDENT_TABLE = [INCIDENT_HEADER, INCIDENT_CWD, INCIDENT_TXT, INCIDENT_CACHE].join('\n');
const CACHE = '/Users/dev/.cargo/.package-cache';
const CWD = '/private/var/folders/cowshed/502/mnt/repo/shed';

const incidentRows: OpenFile[] = [
  { pid: 94769, fd: 'cwd', type: 'DIR', path: CWD },
  { pid: 94769, fd: 'txt', type: 'REG', path: '/usr/lib/libobjc.A.dylib' },
  { pid: 94769, fd: '5u', type: 'REG', path: CACHE },
];

describe('parseLsofTable', () => {
  it('reads the incident rows and skips the header', () => {
    expect(parseLsofTable(INCIDENT_TABLE)).toEqual(incidentRows);
  });

  it('skips any row it cannot parse, and keeps spaces inside NAME', () => {
    const text = [
      INCIDENT_HEADER,
      'garbage',
      '',
      'cargo 94769 dev 5u',
      'cargo 1 dev 3u REG 1,2 0 9 /Users/dev/My Cache/a file',
    ].join('\n');
    expect(parseLsofTable(text)).toEqual([{ pid: 1, fd: '3u', type: 'REG', path: '/Users/dev/My Cache/a file' }]);
  });
});

describe('lockCandidates', () => {
  it('keeps only the numbered REG row of the incident', () => {
    expect(lockCandidates(incidentRows)).toEqual([CACHE]);
  });

  it('keeps numbered regular files and nothing else, sorted and distinct', () => {
    const row = (fd: string, type: string, path: string): OpenFile => ({ pid: 7, fd, type, path });
    expect(
      lockCandidates([
        row('255r', 'REG', '/b'),
        row('txt', 'REG', '/unnumbered-txt'),
        row('mem', 'REG', '/unnumbered-mem'),
        row('5u', 'DIR', '/a-dir'),
        row('3r', 'CHR', '/dev/null'),
        row('10w', 'REG', '/a'),
        row('11w', 'REG', '/a'),
      ]),
    ).toEqual(['/a', '/b']);
  });
});

describe('lockWaits', () => {
  it('names the 2026-10-21 holder outside the run from the Linux lock record, and the run waiting for it', () => {
    const record = records({
      [CACHE]: { state: 'locked', locks: [lock(51234, 'flock'), lock(94769, 'flock', true)] },
    });
    expect(lockWaits(incidentRows, record)).toEqual([
      { path: CACHE, inTree: [94769], waiting: [94769], holders: [51234] },
    ]);
  });

  it('names no wait on Linux where no process of the tree waits on the record, however many hold the file', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94770, fd: '4u', type: 'REG', path: CACHE }];
    const record = records({ [CACHE]: { state: 'locked', locks: [lock(94769, 'flock')] } });
    expect(lockWaits(tree, record)).toEqual([]);
  });

  it('names a Darwin holder outside the tree, and one Darwin does not name, as a wait with its holder', () => {
    for (const holder of [51234, null]) {
      const record = records({ [CACHE]: { state: 'locked', locks: [lock(holder, null)] } });
      expect(lockWaits(incidentRows, record)).toEqual([
        { path: CACHE, inTree: [94769], waiting: [], holders: [holder] },
      ]);
    }
  });

  it('names a Darwin lock two processes of the tree hold open, its holder one of them', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94770, fd: '4u', type: 'REG', path: CACHE }];
    const record = records({ [CACHE]: { state: 'locked', locks: [lock(94770, null)] } });
    expect(lockWaits(tree, record)).toEqual([{ path: CACHE, inTree: [94769, 94770], waiting: [], holders: [94770] }]);
    const own = records({ [CACHE]: { state: 'locked', locks: [lock(94769, null)] } });
    expect(lockWaits(incidentRows, own)).toEqual([]);
  });

  it('names no wait for an unlocked file, one whose locks were not read, or one held through an unnumbered descriptor', () => {
    const tree: OpenFile[] = [...incidentRows, { pid: 94770, fd: '4u', type: 'REG', path: CACHE }];
    expect(lockWaits(tree, records({ [CACHE]: { state: 'unlocked' } }))).toEqual([]);
    expect(lockWaits(tree, records({ [CACHE]: { state: 'unread', reason: 'not read within 250ms' } }))).toEqual([]);
    const library = records({ '/usr/lib/libobjc.A.dylib': { state: 'locked', locks: [lock(51234, null)] } });
    expect(lockWaits(incidentRows, library)).toEqual([]);
  });
});

describe('describeLockWait', () => {
  it('names the holder the lock record names, and says so where it names none', () => {
    expect(describeLockWait({ path: CACHE, inTree: [94769], waiting: [94769], holders: [51234] })).toBe(
      `${CACHE}, open in the run by pid 94769, which waits for it; held by pid 51234`,
    );
    expect(describeLockWait({ path: CACHE, inTree: [94769], waiting: [], holders: [null] })).toBe(
      `${CACHE}, open in the run by pid 94769; locked, and the holder is not named by the kernel's lock record on this platform`,
    );
  });
});

// Generators shaped like `lsof -nP -p` rows: COMMAND PID USER FD TYPE DEVICE SIZE/OFF NODE NAME.
const paths = fc.stringMatching(/^\/[A-Za-z0-9._ -]{0,30}[A-Za-z0-9._-]$/);
const fds = fc.stringMatching(/^(\d{1,3}[rwu]|cwd|txt|mem|rtd)$/);
const types = fc.constantFrom('REG', 'DIR', 'CHR', 'IPv4', 'PIPE');

function renderTable(rows: readonly OpenFile[]): string {
  return rows.map((row) => `cmd ${row.pid} dev ${row.fd} ${row.type} 1,20 0 1 ${row.path}`).join('\n');
}

describe('lsof reading properties', () => {
  it('reads back every row that renders as lsof prints it', () => {
    const rows = fc.array(fc.record({ pid: fc.integer({ min: 1, max: 99_999 }), fd: fds, type: types, path: paths }), {
      maxLength: 20,
    });
    fc.assert(fc.property(rows, (generated) => expect(parseLsofTable(renderTable(generated))).toEqual(generated)));
  });

  it('names a wait only for a locked lockable path the run holds, never a holder the record does not hold', () => {
    const samplePaths = ['/a/x', '/a/y', '/b z'];
    const row = fc.record({
      pid: fc.integer({ min: 1, max: 12 }),
      fd: fds,
      type: types,
      path: fc.constantFrom(...samplePaths),
    });
    const held = fc.record({
      pid: fc.option(fc.integer({ min: 1, max: 20 }), { nil: null }),
      kind: fc.constantFrom('flock', 'posix', null),
      exclusive: fc.boolean(),
      waiting: fc.boolean(),
    });
    const record = fc.oneof(
      fc.constant<LockHolders>({ state: 'unlocked' }),
      fc.array(held, { minLength: 1, maxLength: 4 }).map((locks): LockHolders => ({ state: 'locked', locks })),
    );
    fc.assert(
      fc.property(
        fc.array(row, { maxLength: 12 }),
        fc.dictionary(fc.constantFrom(...samplePaths), record),
        (tree, kept) => {
          const read = records(kept);
          const waits = lockWaits(tree, read);
          const names = waits.map((wait) => wait.path);
          expect(names).toEqual([...names].sort());
          expect(new Set(names).size).toBe(names.length);
          const candidates = lockCandidates(tree);
          for (const wait of waits) {
            expect(candidates).toContain(wait.path);
            const locked = read.get(wait.path);
            expect(locked?.state).toBe('locked');
            const pids = locked?.state === 'locked' ? locked.locks.map((entry) => entry.pid) : [];
            for (const holder of wait.holders) expect(pids).toContain(holder);
            for (const pid of wait.waiting) expect(wait.inTree).toContain(pid);
          }
        },
      ),
    );
  });
});
